# rk Index (Ranked/Sampled) — Design

## Overview & Goals

Wires the rk (Ranked/Sampled — Leaderboards) index kind, per the datum
type system design doc's "rk Index" section, on top of the `seisin-storage`
counted B+Tree engine (`docs/superpowers/specs/2026-07-22-index-storage-engine-design.md`).
This is Datum Type System Part 3.

**Scope, explicit.** This sub-project delivers a *minimal viable* rk:
unconditional replace-on-change updates (matches sk's existing semantics —
no violations, no conditional apply), and a query surface
(`top_n`/`bottom_n`/`percentile_sample`) callable independently of any
write. Deliberately deferred to a separate follow-on plan: conditional/
ratchet update semantics (e.g. "best score wins"), rich update results
carrying rank-after-update plus advisory context (neighbor entries, top-N
bundled into the write response), node-function/placement wiring (which
node's disk holds a given rk index's file — assumed here to be whatever
node/thread currently natively owns that index datum, using the same
per-thread resident-cache mechanism already used for sk), and page-size
auto-detection/benchmarking.

## Framework Change: Removing `IndexHandlerRegistry`

The existing `IndexHandlerRegistry` (`seisin-node/src/index_handler.rs`,
built in Datum Type System Part 2 revised) is a generic
`Fn(Option<&[u8]>, &[u8]) -> (Vec<u8>, Option<String>)` bytes-in/bytes-out
contract. It fits sk (a small entry-list blob, cheap to fully decode/
re-encode per update) but cannot fit rk: rk's resident state is a live,
multi-page `seisin-storage::BPlusTree` file handle, and forcing it through
"pass current bytes, get new bytes" would mean serializing the entire file
on every update — defeating the point of a disk-backed, bounded-I/O
structure.

Rather than build a second parallel registry, `IndexHandlerRegistry` is
removed entirely. `seisin-node`'s `worker.rs` gains hardcoded logic per
index kind:

```rust
match index_kind.as_str() {
  "sk" => { /* sk logic, moved in from seisin-types::sk_index */ }
  "rk" => { /* rk logic, using seisin-storage against rk_cache */ }
  other => { /* reply with a "no such index kind" violation */ }
}
```

This is a deliberate loosening of the framework/solution-logic separation
this project has otherwise maintained (`seisin-node` previously knew
nothing about what "sk"/"rk" meant). sk's apply logic (`SkIndexOp`,
`encode_sk_index_op`/`decode_sk_index_op`, `apply_sk_index_update`)
migrates from `crates/seisin-types/src/sk_index.rs` into `seisin-node`
(new module `seisin-node/src/sk_apply.rs`), since `seisin-node` cannot
depend on `seisin-types` (one-directional dependency, established in Part
2 revised) — the logic must physically live in `seisin-node` to be called
from `worker.rs` directly. `seisin-types::sk_index` keeps only the
payload-*building* side (`sk_key`, `encode_sk_index_op`,
`decode_sk_index_op` re-exported or duplicated as needed for
`TypedOpContext` to construct what it schedules) — `register_sk_index_handler`
and the registry-facing wrapper are deleted.

`WorkerHandle::spawn`/`WorkerPool::spawn` currently take a trailing
`index_handlers: Arc<IndexHandlerRegistry>` parameter (threaded through 13
call sites across the workspace in Part 2 revised). This parameter is
replaced with `data_dir: Arc<String>` (see below) — same call-site
position, so the same 13 sites get a small, mechanical update rather than
a structural one.

## `seisin-storage` Change: Adding `remove`

rk needs to move an existing entry to a new rank_key when its tracked
field changes (per the already-written Part 2 revised spec's rk section:
"remove the old rank_key entry if one existed, insert the new one"). The
B+Tree built in the Index Storage Engine sub-project is insert-only — no
delete existed. A new primitive is added:

```rust
impl BPlusTree {
  /// Removes `key` if present, returning whether it was found. Fixes
  /// every ancestor internal node's subtree count on the way down, so
  /// rank-based descent stays correct — does NOT merge or rebalance an
  /// underfull leaf/internal page with a sibling after removal. Pages
  /// can become sparse over a long delete-heavy history: a real but
  /// bounded inefficiency (never incorrect, just eventually less
  /// space-efficient), documented as a known limitation and revisited
  /// only if it proves to matter for a real workload.
  pub fn remove(&mut self, key: &[u8]) -> Result<bool> { ... }
}
```

A rank_key change is realized as `remove(old_key)` followed by
`insert(new_key, value)` — both existing/new primitives used directly by
rk's apply logic in `seisin-node`, not wrapped in a new storage-engine
"replace" method (keeping the engine's public surface minimal).

## Key Composition (Fixes a Tie/Collision Bug)

The B+Tree's key cannot be the rank_key alone: two entities tied at the
same score would collide under upsert semantics, silently overwriting one
with the other (real data loss under a common case for a leaderboard —
ties are expected, not exceptional). The key is therefore a fixed 24-byte
composite:

```
key   = encode_rank_key(field_value)  (8 bytes, order-preserving)
        ++ pk_id.as_bytes()            (16 bytes)
value = pk_id.as_bytes()               (16 bytes, redundant with the
                                         key's suffix — lets scan/sample
                                         consumers read (rank_key_bytes,
                                         pk_id) pairs without decoding
                                         the key)
```

Sorting is therefore primarily by rank_key, with `pk_id` as an arbitrary
(but deterministic) tiebreaker — good enough for "who's currently ranked
where," not claiming any particular tie-breaking *policy* (e.g. "earliest
submission wins ties" is not implemented; that's product logic for a
later pass if ever needed).

## Rank Key Encoding

`rk`'s declared field must be `FieldType::I64` or `FieldType::F64` (any
other type is a schema-declaration-time error — enforced when `IndexDef::Rk`
is validated, mirroring how sk already restricts to primitive types).
Encoding must make raw byte-lexicographic comparison (what the B+Tree
uses internally) match numeric total order:

```rust
// crates/seisin-types/src/rk_index.rs
pub fn encode_rank_key(value: &FieldValue) -> Result<[u8; 8]> {
  match value {
    FieldValue::I64(v) => {
      // Two's-complement order doesn't match unsigned byte order —
      // flipping the sign bit does: negative numbers (high bit set)
      // become the smaller unsigned range, positive numbers the larger.
      Ok(((*v as u64) ^ 0x8000_0000_0000_0000).to_be_bytes())
    }
    FieldValue::F64(v) => {
      // Matches core::f64::total_cmp's own bit transform exactly, so
      // byte-lexicographic order equals total_cmp order (NaN included,
      // orderable rather than rejected).
      let bits = v.to_bits();
      let mask = ((bits as i64) >> 63) as u64 | 0x8000_0000_0000_0000;
      Ok((bits ^ mask).to_be_bytes())
    }
    other => bail!("rk rank_key must be I64 or F64, got {other:?}"),
  }
}
```

These live in `seisin-types` (they interpret `FieldValue`, a domain
concept) — `seisin-node`'s rk apply logic only ever sees the already-
encoded 8-byte rank_key plus the 16-byte pk_id, never a `FieldValue`.

## Write Path — Reuses the Existing Op Lifecycle Unchanged

`schema.rs` gains a new `IndexDef` variant:

```rust
pub enum IndexDef {
  Sk { field: String, unique: Option<ConflictOp> },
  Rk { field: String },
}
```

`TypedOpContext`'s `Drop` impl (`crates/seisin-types/src/typed_context.rs`)
currently pattern-matches `IndexDef::Sk` irrefutably (a single-variant
enum). It gains an `Rk` arm: on a tracked field change (old value !=
new value, same diffing already in place), it computes
`encode_rank_key` for whichever of old/new exist and calls
`ctx.schedule_index_update(rk_key_datum_id, "rk", payload)` — the exact
same `OpContext` method sk already uses. `rk_key_datum_id` is
`DatumId::from_name(&derived_id_namespace(), format!("rk:{type_name}.{field_name}").as_bytes())`
— the same namespace-derivation helper `sk_key` already uses in
`seisin-types::sk_index`, promoted from private to `pub(crate)` (it's
currently a private fn in that module) so a new sibling `rk_index.rs`
module can call it too. One single derived id per declared rk index (no
value-based partitioning, unlike sk's per-distinct-value keys), matching
the design doc's `rk:{type_name}.{field_name}` key naming.

The payload carries enough to let `seisin-node`'s hardcoded `"rk"` apply
logic do a remove-then-insert:

```rust
pub struct RkIndexOp {
  pub pk_id: DatumId,
  pub old_rank_key: Option<[u8; 8]>,  // None on first insert (create)
  pub new_rank_key: Option<[u8; 8]>,  // None on delete (datum removed)
}
```

This flows through the *exact same* three-phase op lifecycle already
built in Part 2 revised (execute → dispatch `IndexUpdate` → wait →
commit-or-fail) — no changes needed to `try_run_if_ready`/
`dispatch_index_update`/`IndexUpdateReplied`. `worker.rs`'s `rk_cache:
HashMap<DatumId, BPlusTree>` (parallel to the existing `index_cache:
HashMap<DatumId, Vec<u8>>` for sk) holds the open file handle for
whichever rk indexes this thread has touched, opened lazily
(`BPlusTree::open` if the file exists, else `BPlusTree::create`) on
first access per index. `worker.rs`'s `"rk"` match arm: look up (or
open/create) the tree in `rk_cache`, `remove(old_key ++ pk_id)` if
`old_rank_key.is_some()`, `insert(new_key ++ pk_id, pk_id_bytes)` if
`new_rank_key.is_some()`, always succeeds (no violation possible in this
minimal version — unconditional replace).

**Correction from the brainstorm discussion:** bundling "the new rank"
into the update's own result was floated and rejected. `TypedOpContext`'s
`Drop` runs *after* the op handler has already returned, so the handler
can't synchronously observe a rank that hasn't been computed at that
point, and `IndexUpdateReplied`'s existing plumbing only carries a
pass/fail violation, not a data payload, back into the op's result.
Getting your own rank after a write is a follow-up query call (below).
Bundling it into the write response is real additional plumbing,
deferred to the richer-semantics follow-on alongside conditional-apply
and advisory neighbor/top-N data.

## Read Path — Dedicated Wire Messages, Bypassing the Op Lifecycle

Querying an index only ever touches one datum (the index's own derived
id) — it needs no multi-datum collation, so it reuses the same
direct-dispatch-to-owning-thread mechanism `IndexUpdate` already has,
without going through `OpRegistry`/`OpContext`/collation at all. Per the
brainstorm discussion, this gets its own **type-specific** wire pair
(not a generic `Request::IndexQuery`), since different index kinds return
structurally different things:

```rust
// crates/seisin-protocol/src/lib.rs — new Request/Response variants
pub enum Request {
  // ...existing variants unchanged...
  RkQuery {
    index_datum_id: DatumId,
    query: RkQueryKind,
  },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RkQueryKind {
  TopN(u32),
  BottomN(u32),
  PercentileSample(u32),
}

pub enum Response {
  // ...existing variants unchanged...
  RkQueryResult {
    entries: Vec<(Vec<u8>, DatumId)>,  // (rank_key bytes, pk_id), in the
                                        // order the query implies
  },
}
```

Unlike `IndexUpdate` (node-to-node only, never sent by a client),
`RkQuery` is client-facing — a solution's client calls it directly, the
same way it calls `Request::Op`. Its routing therefore follows
`Request::Op`'s existing client-facing pattern end to end: `server.rs`
redirects via `Response::Redirect` if the receiving node isn't native for
`index_datum_id` (same `ring`-based native-node check `Op` already uses),
and once on the right node, `WorkerPool` gets a new method,
`run_rk_query(index_datum_id, query) -> Result<Vec<(Vec<u8>, DatumId)>, String>`,
directly parallel to the existing `WorkerPool::run_op`/
`WorkerHandle::run_op` (picks the destination thread via `ring.native()`,
sends a message, blocks on the same kind of reply channel `run_op` already
uses) — but skipping collation/`op_records` entirely, since a query only
ever touches one datum and is answered synchronously and immediately by
whichever thread owns it, straight from `rk_cache`. `worker.rs` hardcodes
`RkQueryKind::TopN(n)` → `scan_backward_bounded(n)`,
`RkQueryKind::BottomN(n)` → `scan_forward_bounded(n)`,
`RkQueryKind::PercentileSample(k)` → `sample_by_rank(k)`, decoding each
returned `(Vec<u8>, Vec<u8>)` pair (24-byte-key-derived rank_key bytes
aren't separately needed here — the tree's *value* is already the
16-byte pk_id, so results are read directly as `(key[0..8].to_vec(),
DatumId::from_bytes(value.try_into().unwrap()))`).

## Data Directory

`NodeConfig` (`crates/seisin-node/src/config.rs`) gains a new field:

```rust
pub struct NodeConfig {
  pub self_node_id: u64,
  pub members: Vec<MemberConfig>,
  pub data_dir: String,
}
```

rk index files live at `{data_dir}/rk_{type_name}.{field_name}.btree`.
This is the minimum real decision needed to make rk functional at all —
not a stand-in for Storage Tier's eventual placement/replication/
multi-node concerns, just "where do this node's own files go." Tests use
a `tempfile::TempDir` for `data_dir`, matching how every other test in
this workspace avoids touching the real filesystem outside a temp
sandbox.

## Testing Strategy

- Unit tests for `seisin-storage::BPlusTree::remove`: removes an
  existing key (count/rank-descent stays correct afterward), a no-op on
  a missing key (returns `false`), correctness across a multi-level tree
  (using the same small-key-size trick from the Index Storage Engine
  plan to force splits cheaply), and a property-style test confirming
  `sample_by_rank`/`scan_forward_bounded`/`scan_backward_bounded` remain
  correct after a mix of inserts and removes.
- Unit tests for `seisin-types::rk_index::encode_rank_key`: I64 round-trip
  ordering (a set of positive/negative/zero values, sorted by encoded
  bytes, matches their numeric order), F64 ordering matches
  `f64::total_cmp` exactly for a set including negative/positive/zero/
  NaN/infinity values, rejects non-numeric `FieldValue`s.
- Unit tests for the moved sk apply logic in `seisin-node` (same test
  cases already existing in `seisin-types::sk_index`, moved along with
  the code, confirming the migration didn't change behavior).
- Unit tests for `worker.rs`'s `"rk"` `IndexUpdate` handling: a fresh
  insert (`old_rank_key: None`), a rank_key change (remove old +
  insert new), a pure removal (`new_rank_key: None`, entity deleted) —
  each verified via the resident `rk_cache`'s tree state directly.
- Integration test (`seisin-node` or `seisin-types`, following the
  existing `integration_automatic_index_maintenance.rs` pattern): a real
  node, a solution op writing a scored entity via `TypedOpContext`,
  followed by a `Request::RkQuery` call confirming `TopN`/`BottomN`/
  `PercentileSample` all return correct results — proven end-to-end
  through the real wire protocol, not a shortcut. Stress-tested 10x per
  this project's established concurrency-testing discipline, since this
  touches `worker.rs`'s core message-handling loop.
- Full workspace `cargo test`, `cargo fmt --check`, `cargo clippy
  --workspace --all-targets -- -D warnings` clean; re-run the existing
  concurrency-sensitive integration tests (`integration_wound_wait`,
  `integration_cross_node_wound_wait`, `integration_op_collation`) 20x
  per this project's established discipline, since `worker.rs`'s
  `WorkerHandle::spawn`/`WorkerPool::spawn` signatures change again
  (13 call sites, same ripple pattern as Part 2 revised's Task 5).

## Open Questions Carried Forward

- **Conditional/ratchet update semantics** (e.g. "only replace if the
  new score is better") — deferred to a follow-on plan; this version's
  update is always unconditional replace-on-change.
- **Rich update results** (rank-after-update, advisory neighbor/top-N
  context bundled into the write's own response) — deferred; requires
  real plumbing changes to how `IndexUpdateReplied` carries data back
  into an op's result, not just a pass/fail violation.
- **Node-function/placement wiring** — still deferred from the Index
  Storage Engine sub-project; this plan assumes rk's file lives wherever
  the index datum's native thread currently is, via the same per-thread
  resident-cache mechanism sk already uses.
- **Page-size auto-detection and operator benchmark tool** — still
  deferred from the Index Storage Engine sub-project.
- **`remove`'s lack of page merge/rebalance** — accepted as a documented
  limitation (see above); revisit only if a real delete-heavy rk
  workload demonstrates it matters.
- **rk index sharding** (a single rk index is one global bottleneck
  datum under high write volume) — already noted as a known limitation
  in the original datum type system spec, unchanged by this plan.
