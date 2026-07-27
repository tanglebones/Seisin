# Storage Replication (Storage Tier Part C-2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline execution — this project never uses subagents) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Per-datum-type replication — an opted-in type is persisted to N distinct storage nodes so a storage-node crash fails over to a surviving replica instead of halting — per `docs/superpowers/specs/2026-07-27-storage-replication-design.md`.

**Architecture:** N is a per-type schema property (default 1), carried on each write and stored per datum so the type-blind migration driver stays uniform. The ring gains `replicas(id, N)` (salted re-hash; rank 0 == `native`). `RemoteStore` writes to all alive, non-stale replicas (≥1 required) and reads the primary with failover; the coordinated whole-cluster halt moves from the membership path to a point-of-use trip in `RemoteStore` when a datum's every replica is gone. Recovery is the C-1 driver generalized to replica sets. N=1 is byte-for-byte today's single-copy fail-stop behavior throughout.

**Tech Stack:** Rust workspace, `anyhow`, blocking TCP + length-prefixed frames, jump-consistent-hash `Ring`, the C-1 transfer engine + `seisin-migrate` driver.

## Global Constraints

- **N=1 stays provably unchanged.** Every existing single-copy test must stay green untouched except where a signature ripple forces a mechanical edit. Replicated behavior layers on top; the default path is today's path.
- House style: compressed per-task blocks (concrete `Produces:` signatures, Red/Green/Commit), matching Parts A/B/C-1. TDD per task: failing test first, then minimal code.
- Crate versions stay `0.1.0` (pre-first-release).
- Gates per task: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings` clean, full `cargo test --workspace` green.
- `seisin-node/src/lib.rs` has `#![deny(warnings)]`.
- **Wire versioning:** pre-first-release, so each bump drops the old decoder (note it). Bumps here: `STORE_PROTOCOL_VERSION 2→3`, `DatumLog FORMAT_VERSION 2→3`. (Client/admin `PROTOCOL_VERSION` and `GOSSIP_PROTOCOL_VERSION` are unchanged — no client-facing or gossip payload changes.)
- Commit per task directly on `main`, trailer `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- Stress: 10× each new integration suite; the standing 20× wound-wait / cross-node-wound-wait / op-collation loop **is** required here — `worker.rs`/`OpContext` changes in Task 4.

## File / responsibility map

- `crates/seisin-ring/src/ring.rs` — `replicas(id, n)`.
- `crates/seisin-storage/src/datum_log.rs` — per-record N (header/record + index + `list_ids`).
- `crates/seisin-protocol/src/store_wire.rs` — `Put`/`Patch` carry N; `IdList` → `(id, N)` pairs.
- `crates/seisin-core/src/store.rs` — `Store` trait N params; `InMemoryStore`.
- `crates/seisin-*` OpContext (the ctx ops call) — `put`/`get`/`delete` default N=1; add `*_replicated`.
- `crates/seisin-node/src/store_server.rs` + `transfer.rs` — persist/return N; copy preserves N.
- `crates/seisin-node/src/remote_store.rs` — multi-replica write, read failover, point-of-use halt.
- `crates/seisin-node/src/gossip_state.rs` — `storage_alive`/`storage_stale`; death → sets, no membership halt.
- `crates/seisin-node/src/server.rs` — `InstallStorageRing` clears stale.
- `crates/seisin-migrate/src/lib.rs` — `plan_moves` over replica sets; `recover`.
- `crates/seisin-types` — per-type `replication_factor`; typed write/read thread N.

---

### Task 1: `Ring::replicas(id, n)` (seisin-ring)

**Files:** `crates/seisin-ring/src/ring.rs`.

**Produces:** `pub fn replicas(&self, id: DatumId, n: usize) -> Vec<NodeId>` — rank 0 is `native(id).0`; rank k≥1 hashes a salted key derived from `(hash_key(id), k)` into the slot count, advancing the salt on a collision with an already-chosen node, until `n` distinct nodes are collected or the ring's distinct-node count is reached. Reuses `JumpBackHasher`; empty ring returns `vec![]`.

- [ ] **Red:** `replicas_rank0_is_native` (`replicas(id,1) == vec![native(id).0]` for many ids); `replicas_returns_n_distinct_nodes` (with ≥N nodes, len==N, all distinct); `replicas_caps_at_node_count` (N > nodes → all distinct nodes, no dupes/panic); `replicas_is_deterministic`; weight-bias sanity (heavier node appears at rank 0 more often).
- [ ] **Green:** implement; `cargo test -p seisin-ring`.
- [ ] **Commit:** `feat: Ring::replicas — N distinct nodes, rank 0 == native (Storage C-2 Task 1)`.

---

### Task 2: `DatumLog` stores N per record (seisin-storage)

**Files:** `crates/seisin-storage/src/datum_log.rs`.

**Produces:** `FORMAT_VERSION = 3`. Each Full/Delta record's payload gains a `u16 n` immediately after the id (`[kind][id][n][body]`); tombstones write `n=0`. `LogRef` gains `n: u16`; recovery reads it back. Signatures:
```rust
pub fn put_full(&mut self, id: [u8; 16], bytes: &[u8], n: u16) -> Result<()>;
pub fn put_delta(&mut self, id: [u8; 16], delta: &Delta, n: u16) -> Result<PatchOutcome>;
pub fn list_ids(&self, after: Option<[u8; 16]>, limit: usize) -> Vec<([u8; 16], u16)>;
pub fn get(&mut self, id: [u8; 16]) -> Result<Option<Vec<u8>>>;   // unchanged
pub fn n_of(&self, id: [u8; 16]) -> Option<u16>;                   // the stored factor
```
`read_record_at` returns the `n` too (extend `RawRecord`). The self-rebase (`put_delta` consolidating into a Full) preserves the id's `n`.

**Notes:** `n_of` (and `list_ids`' pairs) is how the transfer/driver recover N without knowing the type. `HEADER_LEN` is unchanged (N is per-record, not in the file header). Bumping `FORMAT_VERSION` rejects v2 logs on open — acceptable pre-first-release; note it. Every in-crate `put_full`/`put_delta` test call gains an `n` arg (use `1`).

- [ ] **Red:** `n_round_trips_through_a_reopen` (put with n=3, reopen, `n_of` == 3, `list_ids` pair == 3); `list_ids_returns_id_n_pairs`; delta write keeps the id's n; existing round-trip/recovery/tear tests updated for the new arg + record layout (the tear-offset test recomputes its byte offset).
- [ ] **Green:** widen the record, thread `n`; `cargo test -p seisin-storage`.
- [ ] **Commit:** `feat: datum log stores a replication factor per record (Storage C-2 Task 2)`.

---

### Task 3: Store wire v3 — N on writes, `(id, N)` on list (seisin-protocol)

**Files:** `crates/seisin-protocol/src/store_wire.rs`.

**Produces:** `STORE_PROTOCOL_VERSION = 3`. `StoreRequest::Put { id, bytes, n: u16 }`, `StoreRequest::Patch { id, delta, n: u16 }`. `StoreResponse::IdList { ids: Vec<(DatumId, u16)>, done }`. `ListIds`/`Transfer`/`Retire`/`Identify`/`TransferStatus`/`FinishTransfer` request shapes unchanged. Codecs updated; v2 decoder dropped (note it).

- [ ] **Red:** extend the round-trip tests — `Put`/`Patch` with `n` (0 and non-zero), `IdList` with `(id, n)` pairs (empty and non-empty). `store_call` echo test unchanged.
- [ ] **Green:** implement; `cargo test -p seisin-protocol`.
- [ ] **Commit:** `feat: store wire v3 — replication factor on writes and list-ids (Storage C-2 Task 3)`.

---

### Task 4: `Store` trait N + OpContext + store server + transfer (seisin-core, seisin-node)

**Files:** `crates/seisin-core/src/store.rs`, the OpContext type the ops use, `crates/seisin-node/src/store_server.rs`, `crates/seisin-node/src/transfer.rs`, `crates/seisin-node/src/remote_store.rs`, plus every `Store`/OpContext call site (worker, tests).

**Produces:**
```rust
// Store trait — every method gains the datum's replication factor.
fn get(&self, id: DatumId, n: u16) -> Option<Vec<u8>>;
fn put(&self, id: DatumId, content: Vec<u8>, n: u16);
fn delete(&self, id: DatumId, n: u16);
fn put_with_previous(&self, id: DatumId, content: Vec<u8>, previous: Option<&[u8]>, n: u16);

// OpContext — existing methods keep their signatures and pass n = 1;
// the typed layer uses the _replicated variants (Task 8).
impl OpContext {
  pub fn put(&self, id, bytes) { self.store.put(id, bytes, 1) }        // unchanged callers
  pub fn get(&self, id) -> Option<Vec<u8>> { self.store.get(id, 1) }
  pub fn delete(&self, id) { self.store.delete(id, 1) }
  pub fn put_replicated(&self, id, bytes, n: u16);
  pub fn get_replicated(&self, id, n: u16) -> Option<Vec<u8>>;
  pub fn delete_replicated(&self, id, n: u16);
}
```
**Behavior this task:** `InMemoryStore` ignores `n` (one copy). The store server persists `n` (`Put`/`Patch` → `log.put_full/​put_delta(.., n)`); `ListIds` returns `(id, n)` pairs from the log; the transfer copy reads `(bytes, n)` (via `get` + `n_of`) and re-`Put`s with `n`. **`RemoteStore` takes the new signatures but still behaves single-copy** — writes/reads `native(id).0` only, ignoring extra replicas — so behavior is byte-unchanged. All existing tests stay green (N=1 everywhere).

**Notes:** this is the wide mechanical ripple; do it in one task so the workspace compiles. `worker.rs`/OpContext changes → run the standing 20× loop at the end.

- [ ] **Red:** `in_memory_store_ignores_n` (put with n=3 reads back with any n); store-server `list_ids_reports_stored_n`; transfer test asserts the copied datum keeps its `n` at the destination (extend the Task-4-C1 transfer test).
- [ ] **Green:** thread `n` through the trait + impls + server + transfer; OpContext default-1 wrappers; ripple call sites. `cargo test -p seisin-core -p seisin-node`; standing 20× loop.
- [ ] **Commit:** `feat: replication factor threads through the Store trait and store server (Storage C-2 Task 4)`.

---

### Task 5: `storage_alive` / `storage_stale` sets (seisin-node)

**Files:** `crates/seisin-node/src/gossip_state.rs`, `crates/seisin-node/src/main.rs`, and every `ClusterState { .. }` literal (tests).

**Produces:** `ClusterState` gains `pub storage_alive: Arc<RwLock<HashSet<NodeId>>>` and `pub storage_stale: Arc<RwLock<HashSet<NodeId>>>`. `apply_ready_mutations` **additionally** (membership halt untouched for now): on a storage Join → insert into `storage_alive`; on a storage Leave → remove from `storage_alive`, insert into `storage_stale`. Seeded at construction with the initial ring members (main.rs + `ClusterState::compute_only` seeds both empty).

**Notes:** additive — the storage-Leave still halts here, so every existing test stays green. Task 6 removes the membership halt and starts consuming these sets.

- [ ] **Red:** `a_storage_join_adds_to_alive`; `a_storage_leave_removes_from_alive_and_marks_stale`; a returned node stays in `storage_stale` (a second Join does not clear it).
- [ ] **Green:** add the sets + maintenance + seeds; ripple `ClusterState` literals. `cargo test -p seisin-node`.
- [ ] **Commit:** `feat: track alive/stale storage nodes on the compute side (Storage C-2 Task 5)`.

---

### Task 6: `RemoteStore` multi-replica + point-of-use halt (seisin-node)

**Files:** `crates/seisin-node/src/remote_store.rs`, `crates/seisin-node/src/gossip_state.rs`, `crates/seisin-node/src/main.rs`, `crates/seisin-node/tests/integration_storage_halt.rs`, and the two gossip_state halt tests.

**Produces:** `RemoteStore::new` gains `storage_alive`, `storage_stale`, and `Arc<HaltState>`. Internals:
```rust
fn serving_replicas(&self, id, n) -> Vec<NodeId>   // replicas(id,n) ∩ alive \ stale, rank order
fn try_call(&self, node, id, req) -> Result<StoreResponse, String>  // Err on IO (for failover)
```
- **write** (`put`/`put_with_previous`): `targets = serving_replicas(id, n)`; if empty → `halt.halt(reason)` + panic (fail-stop); else write+fsync to every target (delta path per-target, `NeedFull` → full `Put`); ack when all done.
- **read** (`get`): try each `serving_replicas(id, n)` in order; first success wins; all fail → `halt.halt` + panic.
- **delete:** delete on every serving replica.
- **`apply_ready_mutations`**: the storage-Leave branch **no longer calls `halt.halt`** — only the alive/stale update from Task 5 remains. The halt is now point-of-use.

**Notes:** `RemoteStore::call` is refactored to a `try_call` returning `Result`; the panic/halt moves up to the serving methods. `main.rs` + the C-1 `store_pair`/integration `remote_store(..)` helpers pass the alive/stale/halt handles. `integration_storage_halt` updates: after the storage death, assert the halt trips on the **first client op** (not preemptively), still returning "cluster halted". The gossip_state `a_storage_leave_engages_the_halt...` test becomes `...updates alive/stale without halting`.

- [ ] **Red:** in-crate integration `replicated_write_reaches_two_nodes_and_reads_back` (N=2 over two in-process store nodes; both hold it); `read_fails_over_when_the_primary_is_down`; `a_degraded_write_acks_to_the_survivor`; `total_loss_trips_the_point_of_use_halt`; the updated halt/gossip tests.
- [ ] **Green:** implement multi-replica + failover + point-of-use halt; remove the membership halt; rewire callers. `cargo test -p seisin-node`; 10× the new suite; standing 20× loop.
- [ ] **Commit:** `feat: RemoteStore replicates writes, fails over reads, halts on total loss (Storage C-2 Task 6)`.

---

### Task 7: migration driver over replica sets + `recover` (seisin-migrate, seisin-node)

**Files:** `crates/seisin-migrate/src/lib.rs`, `crates/seisin-migrate/src/main.rs`, `crates/seisin-node/src/server.rs`.

**Produces:** `plan_moves` generalizes to replica sets:
```rust
// For each (id, n): move to every node in replicas_new(id,n) \ replicas_old(id,n).
pub fn plan_moves(old: &Ring, new: &Ring, ids: &[(DatumId, u16)]) -> Vec<Move>;
```
The driver's id enumeration uses `ListIds`' `(id, n)` pairs. `handle_install_storage_ring` (server.rs) additionally **clears its member set from `storage_stale`** (re-admission). New CLI verb `seisin-migrate recover <config.ron>`: propose a ring without the dead node(s), restoring N onto survivors/replacements, then run the standard copy→pause→tail→flip→resume→retire (the flip re-admits via the stale-clear).

**Notes:** `Move` unchanged (per-id source→dest); a datum with 2 new replicas yields 2 `Move`s. `migrate`/`recover` share the same engine; `recover` just computes the proposed ring for the operator (drop dead, keep N).

- [ ] **Red:** `plan_moves` unit tests over replica sets — add (a new replica node gains a copy of the right subset), remove (each of the removed node's replicas re-homes to the next distinct node), reweight, N=1 (identical to C-1's single-owner result), identical ring → no moves.
- [ ] **Green:** generalize `plan_moves`, thread `(id,n)`, stale-clear on install, `recover` verb. `cargo test -p seisin-migrate -p seisin-node`.
- [ ] **Commit:** `feat: migration driver moves replica sets; recover restores N (Storage C-2 Task 7)`.

---

### Task 8: per-type replication factor in the schema (seisin-types)

**Files:** `crates/seisin-types` (schema declaration + the typed write/read drivers).

**Produces:** a datum type declares a `replication_factor: u16` (default 1) in its schema. The typed write path calls `ctx.put_replicated(pk, bytes, n)` / the typed read path `ctx.get_replicated(pk, n)` with the type's factor; index datums (sk/rk/tk/lb/partition) stay N=1 (`ctx.put`/`ctx.get`). Wire the factor from the schema through to the `Store` calls.

**Notes:** exact seam depends on the seisin-types schema/context shape — follow the existing pattern for how a type's declared attributes reach its write/read drivers. Only typed *content* (pk datums) replicates.

- [ ] **Red:** a typed-layer test: a type declared with `replication_factor = 2` writes its datum to two storage nodes (drive through a 2-node in-process storage set-up like Task 6's); a default type writes to one.
- [ ] **Green:** add the schema field + thread it; `cargo test -p seisin-types`.
- [ ] **Commit:** `feat: per-datum-type replication factor in the schema (Storage C-2 Task 8)`.

---

### Task 9: integration, stress, docs

**Files:** `crates/seisin-node/tests/integration_storage_replication.rs` (new), `docs/superpowers/PROGRESS.md`, `CLAUDE.md`.

**Harness:** extend the C-1 migration harness (in-process compute + N storage nodes, driving `seisin_migrate`) to a replication-factor-aware setup. Cover spec §9:
- **Replicated write + read-one**: N=2 corpus; every datum on two nodes; reads succeed.
- **Read failover**: drop the primary of a shard (remove from alive); reads still succeed from the secondary; no halt.
- **Degraded write**: one replica down; writes ack; the down node ends stale.
- **Total-loss halt (point-of-use)**: both replicas of a shard down; the first op touching it trips the whole-cluster halt.
- **N=1 unchanged**: a single-copy datum on a dead node trips the halt on access (mirrors C-1).
- **Driver re-replication**: kill a replica, run `recover`; N restored; the re-admitted node serves; stale cleared.
- **Stale not served**: a node that missed writes and returned is never read until re-replicated.

- [ ] **Red/Green:** write the suite; iterate to green.
- [ ] **Stress:** `integration_storage_replication` 10×; standing 20× wound-wait/cross-node-wound-wait/op-collation. Full gates.
- [ ] **Docs:** PROGRESS.md — Part C-2 "Done" entry + updated Part C remainder (compaction, tk/lb datum-grade durability, group commit, incremental catch-up, rack-awareness, read load-balancing). Refresh the CLAUDE.md snapshot. Commit `feat: per-type storage replication with failover and driver recovery (Storage Tier Part C-2)`; push.

---

## Deferred (spec §Not-in-scope, restated)

Incremental catch-up of a returned replica (v1 does full driver resync); rack/zone-aware placement; read load-balancing across replicas; changing a type's N on already-written data; whole-disk / block-device replication (external tooling, by design).

## Self-review against the spec

- §1 per-type factor default 1 → Task 8; N=1 unchanged → Global Constraints + Tasks 4/6. ✓
- §2 N carried + stored per datum → Tasks 2/3/4; `ListIds` `(id,N)` → Tasks 2/3. ✓
- §3 `Ring::replicas` salted re-hash → Task 1. ✓
- §4 all-alive/≥1 write → Task 6. ✓
- §5 read-one + failover → Task 6. ✓
- §6 alive/stale sets + point-of-use halt + no membership halt → Tasks 5/6. ✓
- §7 driver over replica sets + `recover` + stale-clear on install → Task 7. ✓
- §8 wire/API additions → Tasks 1–8 as listed. ✓
- §9 testing → Tasks 1–8 units + Task 9 suite. ✓
