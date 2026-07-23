# lb (Leaderboard) Datum Class — Design

Date: 2026-07-23

## Overview & Goals

lb is a structured ranked-set datum class: per-board score standings
with covering display data, updated under a declared rule, queried and
mutated through single-round-trip ops that never require follow-up
fetches (no 1+N pattern — a top-N display must not need N player-datum
reads to render).

The driving requirements:

- A player does not have one score — they have a score **per
  (leaderboard, area configuration)**. There are many boards, and a
  player appears on many of them. A board, not a player field, is the
  unit of ranking.
- Board entries must **cover** display: `(rank score, player_id,
  display string)` live together in the ranked structure so top/bottom
  lists render with zero additional fetches.
- Score updates follow a **declared rule** (max / min / replace —
  elo deferred, see below), not caller whim.
- The update op returns everything a post-submit UI needs in one round
  trip: the top list, the player's exact rank (with the board total, so
  a client renders exact placement inside the top-N and a percentile
  beyond it), a neighbors-around-the-player window, and ranks for a
  supplied list of friend player ids.

## Classification: a Datum Class, Not an Index

Per the taxonomy in `2026-07-21-datum-type-system-design.md` (tk
section): sk/rk are derived indexes — rebuildable from a datum scan,
which licenses index-grade durability relaxations. lb fails that test:
under max/min rules the board entry *is* the authoritative "best score"
(nothing else stores it), and future rules like elo are path-dependent
and unrecomputable in principle. lb is therefore **primary data with
datum-grade durability requirements**, like tk — the third class riding
the `ResidentIndex` rail:

1. Derived indexes (sk, rk) — framework-maintained via diff →
   `IndexUpdate`; rebuildable.
2. Decomposed field storage (tk) — framework-maintained; primary data.
3. Structured datum classes (lb) — **solution-called ops**; primary
   data.

The distinction from rk is also mechanical: an lb write is not a side
effect of writing a player datum (there is no single score field to
diff — the score is per-board), so lb does not ride `TypedOpContext`'s
change detection at all. The write *is* the operation, arriving as an
explicit `update_lb` call. rk remains unchanged as the general derived
rank index over a declared numeric field.

## Framework Extension: the `execute` Rail

`ResidentIndex` (`seisin-node/src/index_handler.rs`) gains a third
method alongside `apply` (framework-diff writes) and `query`
(read-only):

```rust
/// A solution-called, mutating op against the resident structure,
/// returning result data — payload and result bytes are opaque to the
/// framework, exactly like `query`. Default: unsupported.
fn execute(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
  let _ = payload;
  Err("this index kind supports no execute ops".to_string())
}
```

Plumbing mirrors the existing query path exactly:
`WorkerMessage::IndexExecute { target, index_kind, payload, reply }`
and `WorkerHandle`/`WorkerPool::run_index_execute`, dispatched
synchronously to the owning thread with no collation and no op record —
a board op touches exactly one datum, and the owning thread's serial
message processing makes each read-modify-write-respond atomic. This is
the same mechanism tk's correction-upserts and `as_of` queries will use
(Part 4), built here first.

## Class Declaration & Board Identity

A leaderboard class is declared standalone — not attached to a
`DatumTypeDef` field:

```rust
pub enum LbRule {
  Max,     // keep the better (higher) score
  Min,     // keep the better (lower) score
  Replace, // last write wins
}

pub struct LbClassDef {
  pub name: String,          // e.g. "racing"
  pub score_type: LbScoreType, // I64 | F64 — fixes rank-key encoding
  pub display_len: u16,      // fixed covering-display width, bytes
  pub rule: LbRule,
}
```

Registration binds one registry kind string per class —
`register_lb_class(registry, def, data_dir)` registers kind
`lb:{name}` — because `IndexKind::open` receives only a `DatumId` and
must learn the class's rule/display width from the registered kind
itself (the same reason rk's kind carries `data_dir`).

Board identity: `DatumId::from_name` over
`lb:{class}:{leaderboard_id}:{area_config_id}`. Every board is an
independent datum placed by the ordinary ring — boards distribute
across threads/nodes for free (tk's structural advantage, not rk's
single-datum funnel). **A season or reset is a new `leaderboard_id`** —
no reset machinery; old boards simply age out. Board files:
`{data_dir}/lb_<board-datum-id-hex>.btree`.

## Resident Board State

Per board, on its owning thread:

- A counted B+Tree (`seisin-storage`): key = 8-byte order-preserving
  rank key (same I64 sign-flip / F64 total_cmp transforms as rk) ++
  16-byte `player_id` tiebreaker — tied scores coexist, ordered
  arbitrarily-but-deterministically by player id (upgrading tie policy,
  e.g. earliest-submission-wins via a timestamp in the key, is a
  documented later migration, not v1). Value = the display string,
  padded/truncated to the class's `display_len` (see "Fixed Width &
  the TOAST Question" below).
Board-level attributes are **normalized up into board identity, never
repeated per entry**: `class`, `leaderboard_id`, and `area_config_id`
exist only in the derived board datum id (and thus the file), and the
rule/display_len only in the class registration. An entry is exactly
`rank_key(8) ++ player_id(16) → display(display_len)` — nothing
board-constant is stored per tuple.

- An in-memory `HashMap<player_id, current rank key>`, rebuilt by one
  O(n) full scan on cold open. It is derivable from the tree, so it is
  never persisted; it is what makes self-rank and friend-rank lookups
  O(log n) instead of scans.

New `seisin-storage` primitives this needs:

- `rank_of_key(&mut self, key: &[u8]) -> Result<Option<u64>>` — the
  mirror of `entry_at_rank`: a counted descent accumulating the sizes
  of subtrees to the left, returning the key's 0-based rank (None if
  absent).
- `scan_from_rank(&mut self, rank: u64, n: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>>`
  — descend once to the leaf holding `rank`, then walk sibling links;
  backs the neighbors-around-player window without per-entry descents.

## Ops & Wire Contract

Wire-level scores are the 8-byte rank keys; the paired client library
knows the class's `score_type` and the transforms are bijective
(`seisin-types` gains `decode_rank_key` alongside `encode_rank_key`).
Codecs live in `seisin-protocol` as standalone functions (the rk
precedent: defined once, used by both server routing and the lb
implementation).

- **`update_lb`** — via `execute`. Payload: `{player_id, display,
  rank_key, friend_ids: Vec<DatumId>, top: u32, window: u32}`. The
  owning thread applies the class rule against the player's current
  entry (map lookup): keep the existing entry, or remove-old +
  insert-new + map update. Response, assembled from the same resident
  state in the same message handling:

  ```
  {
    total: u64,                 // board population
    player_rank: u64,           // exact, 0-based
    top: Vec<LbEntry>,          // `top` entries, best first
    around: Vec<LbEntry>,       // `window` entries centered on the player
    friends: Vec<(DatumId, u64, [u8;8], display)>, // rank + score + display
  }
  ```

  `LbEntry = { rank_key: [u8;8], player_id: DatumId, display: Vec<u8> }`
  (padding trimmed). Friends not present on the board are omitted.
  Exact-rank-in-top-N vs percentile-beyond is client presentation:
  `player_rank`/`total` carries both.

- **`remove_lb`** — via `execute`. Payload `{player_id}`; removes the
  entry and map record (deleted/banned player). Board wipe is
  deliberately not an op — that's what new board ids are for.

- **`LbQuery`** — via the read-only `query` method. Payload: `{top:
  u32, bottom: u32, around_player: Option<DatumId>, window: u32,
  friend_ids: Vec<DatumId>}` → same bundle shape plus a bottom list.
  Spectator/refresh views use this; the update response deliberately
  excludes bottom lists (a display concern, not a post-submit one).

Wire variants: `Request::LbExecute { board_id, class, op }` /
`Request::LbQuery { board_id, class, query }` and
`Response::LbResult { .. }`, routed exactly like `RkQuery` (redirect if
the receiving node isn't native for `board_id`). The `class` field
exists only to form the registry kind string `lb:{class}` —
`seisin-node` stays semantics-agnostic.

## Client Oversampling Contract

Cached top/bottom windows decay as scores change, and the churn is
asymmetric: under a ratchet rule (max), a top list only gains or
reshuffles entries, while a bottom list *erodes* — players improve out
of it and (new entrants aside) nobody arrives. Clients therefore fetch
`n + k` (k chosen per end; bottom generally wants a larger k), display
n, and consume the k slack locally as entries depart; when the slack is
spent, they simply re-issue the query. Server-side this costs nothing
(top/bottom are bounded sibling-link scans — k more entries off the
same walk) and requires nothing: no cursors, no sessions, no change
notification. Every response carries `total`, which doubles as a
staleness hint for percentile displays. Push/subscription ("tell me
when my window decays") is explicitly out of scope — the n+k-then-
reload pattern exists precisely to avoid it.

## Fixed Width & the TOAST Question

lb display strings are stored fixed-width (`display_len`,
padded/truncated). For display names this is semantically fine — they
are lossy presentation data, and a declared cap is standard product
behavior — and it keeps the B+Tree's fixed-size-value engine untouched.

The general problem it sidesteps is real, though, and is hereby a
**named Storage Tier requirement**: datum content at the disk layer
needs a fixed inline area plus out-of-line overflow for large/variable
tails (the PostgreSQL TOAST shape). In `seisin-storage` terms: value
slots become a fixed-size descriptor — inline bytes for small values,
`(overflow_page_id, total_len)` chain head for large ones. Two design
consequences to carry into that work: (1) the engine has **no
free-list** — removing an entry with an overflow chain would leak those
pages until a `rebuild_from` compaction, so overflow forces the
free-list/reclamation question; (2) the inline threshold interacts with
the page-size tuning already deferred from the Index Storage Engine
sub-project. Once overflow exists, lb's `display_len` cap can be raised
or removed by migrating a class's boards (`rebuild_from` already
provides the mechanical rebuild path).

## Durability Note

lb boards are primary data: index-grade relaxations (no fsync before
ack, rebuild-from-scan) do **not** apply in principle. Today nothing in
the system fsyncs (Storage Tier is unbuilt), so lb's practical
durability equals everything else's — but when Storage Tier lands, lb
files belong in the datum-grade class (write-through-before-ack), not
the index class. There is deliberately no `rebuild_from`-style recovery
story for lb: lost board state is lost.

## Testing Strategy

- `seisin-storage`: known-answer tests for `rank_of_key` (present,
  absent, first/last, multi-level trees) and `scan_from_rank`
  (mid-leaf start, crossing sibling links, rank past end); a
  model-based test confirming both stay correct after interleaved
  inserts and removes (extending the existing LCG/BTreeMap pattern).
- lb kind unit tests: each rule (Max keeps better / replaces worse,
  Min inverted, Replace unconditional), tie handling (two players at
  one score both present, deterministic order), remove, cold-reopen
  rebuilding the player map from the file, display padding/truncation
  round-trip, malformed payloads as errors not panics.
- Codec round-trips for every op/result shape, including empty
  friends/around lists.
- Integration (`seisin-types` tests, following
  `integration_rk_leaderboard.rs`'s bootstrap): a real node, two
  independent boards of one class; players submit over the wire;
  verify the update bundle (top order and covering display, exact
  rank, friend ranks, a worse score under Max changing nothing),
  `LbQuery`'s bottom list, board independence, and a removed player
  vanishing. Stress 10x; standing 20x re-run of the wound-wait/
  collation suites (worker.rs's loop gains an arm).

## Deferred, Explicitly

- **Elo** (and any rule needing opponent context or cross-entry
  updates) — additive to `LbRule` and the `execute` payload later.
- **Tie-policy upgrades** (earliest-submission-wins timestamps in the
  key) — a per-class board rebuild when needed.
- **Bottom lists in the update response** — LbQuery covers it.
- **Board wipe op** — new board ids cover it.
- **Push/change notification** — the oversampling contract covers it.
- **Variable-length display / TOAST overflow** — Storage Tier (see
  above).
