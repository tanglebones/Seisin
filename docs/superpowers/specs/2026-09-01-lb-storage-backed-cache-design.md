# lb: storage-backed boards with a bounded compute-side cache

## Motivation

Today an `lb` board (`LbResidentBoard` in `seisin-types/src/lb_kind.rs`)
is a `BPlusTree` file fully resident on whichever compute node's ring
bucket owns the board's `DatumId` — never replicated, never durable
beyond that one node's local disk (per `CLAUDE.md`'s "indexes are
rebuildable derived state — no WAL," which lb is grandfathered into as
"index-grade" pending this work). Two problems follow directly: losing
that one compute node loses the board, and a board's full working set
must fit in that node's memory regardless of how large it grows.

This moves board data into the storage tier (replicated, durable,
survives a compute node loss) and replaces the compute-side full-tree
residency with a bounded cache that answers reads locally when it can
and falls back to targeted storage queries — top-N, bottom-M,
stochastic sample, and point/"±N around a key" lookups — when it can't,
evicting least-needed entries to stay under a size limit.

This generalizes to a storage-tier primitive (an ordered, content-
agnostic collection with rank-based operations) rather than an
lb-specific mechanism, since `rk` and `tk` are already tracked in
`PROGRESS.md` as needing the same durability treatment and can become
the primitive's next consumers without another protocol change.

## Decisions carried in from the design discussion

- **Storage nodes are closer to databases than blob stores now**: the
  new collection operations replicate as *logical ops* (`insert`,
  `remove`) fanned out to all N replicas, each applying the op to its
  own local `BPlusTree` copy independently — not as byte-diffs of a
  whole file. This is simpler than forcing the redesign through the
  existing `Patch`/`diff` byte-delta path (which assumes the *compute*
  side holds old-and-new full bytes to diff, no longer true once
  storage owns the canonical copy), and keeps every replica byte-
  identical by construction (deterministic replay of the same op
  sequence) rather than by depending on diff quality.
- **Rank-based operations on ordered fields stay content-agnostic.**
  Storage learns nothing about "score" or "player" — `scan_forward`,
  `sample_by_rank`, `rank_of_key`, etc. operate on raw ordered byte
  keys, exactly the posture the `BPlusTree` engine already has today
  (it's already used this way, resident-side, for both `rk` and `lb`).
- **lb operations stay pinned and uncollated — already true, and a
  hard invariant this redesign must not break.** `Request::LbExecute`/
  `LbQuery` are their own wire variants, dispatched as
  `WorkerMessage::IndexExecute`/`IndexQuery` — per the existing doc
  comment in `worker.rs`, these run synchronously on the target's
  *owning thread* with "no collation, no op record: single-datum
  atomicity comes from serial message processing on the owning
  thread." They never enter the `Acquire`/`Recall`/wound-wait machinery
  generic multi-datum `Op`s use, and a board's owning thread is fixed
  by the compute ring's placement of its `DatumId` — not renegotiated
  per op. Nothing in this design changes that dispatch path; it only
  changes what `LbIndexKind`/the resident structure does once a
  request reaches the owning thread. Worth calling out because it's
  precisely what makes a bounded, stateful cache viable at all — if
  board authority could move mid-stream the way collated multi-datum
  ops move, the cache would be invalidated constantly.

## Non-goals

- No changes to the client-facing lb API (`Request::LbExecute`/
  `LbQuery`, `LbExecuteOp`, `LbQueryReq`, `LbResult`) — this is a
  backend swap under an unchanged contract. The `leaderboard-http`
  example crate's spec needs exactly one line changed (its "Non-goals"
  bullet claiming lb isn't storage-backed) and nothing else.
- No change to `rk`/`tk` themselves — the primitive is built generic,
  but migrating `rk`/`tk` onto it is separate, future work per
  `PROGRESS.md`.
- No copy/insert (xdelta-style) deltas — irrelevant here since this
  design replicates logical ops, not byte diffs, which is what made
  that PROGRESS.md item necessary for the *blob* `Patch` path, not this
  one.

## The storage-side ordered-collection primitive

### Wire protocol (`seisin-protocol::store_wire`)

New `StoreRequest`/`StoreResponse` variants, each scoped by
`collection_id: DatumId` (identifies which `BPlusTree` file — analogous
to how `Put`/`Get` are scoped by a datum id today):

```rust
// StoreRequest additions
CollectionCreate { collection_id: DatumId, key_size: u32, value_size: u32 },
CollectionInsert { collection_id: DatumId, key: Vec<u8>, value: Vec<u8> },
CollectionRemove { collection_id: DatumId, key: Vec<u8> },
CollectionGet { collection_id: DatumId, key: Vec<u8> },
CollectionScanForward { collection_id: DatumId, limit: u32 },
CollectionScanBackward { collection_id: DatumId, limit: u32 },
CollectionSample { collection_id: DatumId, k: u32 },
CollectionRankOfKey { collection_id: DatumId, key: Vec<u8> },
CollectionScanFromRank { collection_id: DatumId, rank: u64, n: usize },

// StoreResponse additions
CollectionEntry { value: Option<Vec<u8>> },        // Get
CollectionEntries { entries: Vec<(Vec<u8>, Vec<u8>)> }, // Scan*/Sample
CollectionRank { rank: Option<u64> },              // RankOfKey
```
(`Ack`/`Error` are reused for `Create`/`Insert`/`Remove`.) `CollectionCreate`
is idempotent — a repeat on an already-created collection is a no-op
`Ack`, since ownership handoff and replica catch-up both need to be able
to call it without first checking existence.

Bumps `STORE_PROTOCOL_VERSION` to 4. Pre-first-release, so per house
policy this drops the old decoder rather than keeping it — no n±1
compatibility burden yet.

### Storage-side implementation

Each collection is one `BPlusTree` file (`collection_{hex(collection_id)}.btree`,
mirroring today's `lb_{hex}.btree` naming) living in the storage node's
data directory, opened lazily on first use and kept resident in a
per-connection (or per-storage-node, behind a mutex — matching how
`StoreNode`/`DatumLog` are already shared today) map keyed by
`collection_id`. `CollectionInsert`/`Remove` call straight into the
already-existing `BPlusTree::insert`/`remove`, fsynced before ack —
same durability contract as `Put`. The four read ops call straight into
the already-existing `scan_forward_bounded`/`scan_backward_bounded`/
`sample_by_rank`/`rank_of_key`/`scan_from_rank` — no new tree logic
anywhere, just wiring.

### Replication

A new `RemoteCollectionStore` (sibling to `RemoteStore`, reusing its
`serving_replicas`/`mark_stale`/`halt_total_loss` shape wholesale — same
ring lookup, same "write to every alive non-stale replica, ≥1 must ack,
total loss halts the cluster" policy) fans `CollectionInsert`/`Remove`/
`Create` out to every replica in the board's storage-ring placement.
Reads (`Get`/`Scan*`/`Sample`/`RankOfKey`) go to the primary replica
with failover to the next, exactly like `RemoteStore::get` does today.
Board replication factor is a fixed constant for now (matching how
`cluster_test_node`'s `put2`/`get2` hardcode `REPL: u16 = 2`) — no
per-board configuration surface yet.

## lb on top of the primitive: two collections per board

- **`rank`** — key `rank_key(8) ++ player_id(16)` (24 bytes, matching
  today's `composite_key`), value = today's length-prefixed padded
  display encoding. Backs `ScanForward`/`ScanBackward` (top/bottom),
  `Sample` (stochastic sampling), and `RankOfKey`+`ScanFromRank`
  (windowed "±N around a given key" — the friend-rank and around-player
  queries).
- **`by_player`** — key `player_id` (16 bytes), value `rank_key` (8
  bytes). Replaces today's in-memory `by_player: HashMap<DatumId,
  [u8; 8]>` side index, moved to storage so it's bounded on compute too.
  A "fetch score for player 123, then ±5" request is: point `Get` on
  `by_player` → `rank_key` → `RankOfKey`+`ScanFromRank(rank±5)` on
  `rank`.

`LbExecuteOp::Update` becomes: `Get` old `rank_key` from `by_player` (to
know whether/what to remove from `rank` under the class's Max/Min/
Replace rule), `Insert`/`Remove` on `rank` as needed, `Insert` the new
`rank_key` into `by_player`. `LbExecuteOp::Remove` removes from both.
Both collections for a board are created together (`CollectionCreate`
on first write) since a board never has one without the other.

## Compute-side cache (`LbResidentBoard` → `LbCache`)

### Per-board cache configuration, resolved on first access

Cache sizing (pinned top-K, pinned bottom-M, LRU cap) is **per specific
board**, not per class — a "global season" leaderboard and a "friends
this week" leaderboard under the same class may warrant very different
sizes. Since the set of actual boards isn't fixed or enumerable up
front (leaderboards get created dynamically, e.g. per event), this
can't be a static table supplied at registration time. Instead,
`register_lb_class` takes a resolver callback, invoked once per board
the first time this compute node opens it (naturally already the
"first access" point — `resident_indexes.entry(target)`'s `Vacant`
branch in `worker.rs`, which only calls `IndexKind::open` the first
time a given board id is touched on that thread):

```rust
pub struct LbCacheConfig {
  pub pinned_top: usize,
  pub pinned_bottom: usize,
  pub max_cached_entries: usize, // includes the pinned windows
}

pub fn register_lb_class(
  registry: &mut IndexKindRegistry,
  def: LbClassDef,
  data_dir: PathBuf,
  cache_config: impl Fn(DatumId) -> LbCacheConfig + Send + Sync + 'static,
);
```

The callback receives only the board's opaque `DatumId` — the wire
protocol deliberately never carries a board's `leaderboard_id`/
`area_config_id` past initial `DatumId` derivation (`lb_board_key`'s
doc comment: "never repeated per entry"), and this design doesn't
change that. A solution that needs the callback to make a genuinely
per-leaderboard decision is expected to maintain its own mapping from
`DatumId` to that leaderboard's settings (it already computed the id
itself via `lb_board_key` when the leaderboard was created, so it can
key its own config store the same way) — the callback is where that
lookup runs. A solution with no such per-board tuning need can just
return a constant.

Replaces the fully-resident `BPlusTree` with:
- **Pinned windows**: the current top-K and bottom-M entries (K/M come
  from this board's resolved `LbCacheConfig`), kept live — every write that
  touches the pinned range patches the cache in place instead of
  invalidating it, since these are read far more than anything else.
- **An LRU for everything else** — entries pulled in by a point/around-
  player/friend/sample query, evicted least-recently-used first when
  the cache's total entry count exceeds its configured limit. Since the
  pinned top/bottom windows are excluded from the LRU by construction,
  "evict the middle first" falls out naturally: the LRU only ever holds
  middle-of-the-pack entries in the first place.
- **Writes are write-through, not write-back**: every `Update`/`Remove`
  commits to storage synchronously before replying (same durability
  posture as today — a client doesn't get an ack for data that could
  still vanish), then the cache is patched. No dirty/flush state to
  reason about.

`LbIndexKind::open` no longer seeds from `cache.get(target)` (the
framework's generic per-datum blob cache, which held the whole file's
bytes before this change) — it starts a board cold (empty pinned
windows, empty LRU) and the first `execute`/`query` against it
populates the pinned windows from storage on demand.

## Ownership handoff

A compute ring reweight or failover moves a board's *serving thread*
(unchanged from today), but board *data* now lives in the storage tier
regardless of which compute node currently owns it. The new owner opens
an `LbCache` cold and repopulates its pinned windows lazily on first
access — strictly cheaper than today's full local-file rebuild on
takeover, and a genuine improvement, not just a side effect.

## Testing

- `seisin-storage`: unit tests for the new `Collection*` request/
  response encode/decode round trips (mirroring existing store-wire
  tests) plus a storage-side handler test exercising create/insert/
  remove/scan/sample/rank-of-key against a real file (no mocking).
- `seisin-types`: rewrite `integration_lb_boards.rs` to start both a
  real storage node and a real compute node (same shape as the existing
  cluster integration tests), proving the wire-level `LbExecute`/
  `LbQuery` contract is byte-for-byte unchanged from the client's view.
- A dedicated cache-eviction test: insert more players than the cache's
  configured limit, assert the pinned top/bottom stay resident while a
  point query for a middle player triggers exactly one storage round
  trip (instrumented via a call counter on a test `RemoteCollectionStore`
  substitute, or by asserting on storage-node request logs) — proving
  eviction actually bounds memory rather than just existing in name.
- A replica-failover test for `RemoteCollectionStore`, mirroring
  `RemoteStore`'s existing stale-marking/failover tests.

## Open questions

- `sample_by_rank`'s existing resident-side semantics (uniform-at-random
  by rank position) carry over unchanged; not re-litigated here.

## Follow-up

Once this is approved and implemented, update
`2026-09-01-leaderboard-example-design.md`'s "Non-goals" section — the
bullet stating lb boards aren't storage-backed becomes false.
