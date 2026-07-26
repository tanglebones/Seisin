# Storage Tier Part B Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline execution — this project never uses subagents) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Role-tagged storage membership on the existing gossip fabric plus the coordinated fail-stop halt, per `docs/superpowers/specs/2026-07-26-storage-tier-part-b-design.md`.

**Architecture:** `MemberUpdate` grows role/weight/store_address (gossip wire v2); mutation application becomes role-aware via a `ClusterState` bundle threading the storage ring, the now-shared store-address book, and the `HaltState` through the three `apply_ready_mutations` call sites; storage nodes run an ack-only gossip responder.

## Global Constraints

House rules as prior plans. Gossip wire version bumps to 2 with the pre-first-release note from the spec. `Store` signatures stay infallible.

---

### Task 1: role-tagged membership + gossip wire v2

**Files:** `crates/seisin-gossip/src/membership.rs`, `crates/seisin-gossip/src/wire.rs`, plus mechanical `MemberUpdate` literal updates across `seisin-node` (main.rs seeding, gossip_state tests) and `seisin-gossip` tests.

**Produces:** `pub enum MemberRole { Compute, Storage }` (derives Debug/Clone/Copy/PartialEq/Eq); `MemberUpdate` gains `pub role: MemberRole, pub capacity_weight: u32, pub store_address: String`; wire codec appends `role: u8` + `capacity_weight: u32 LE` + `store_address` string; `GOSSIP_PROTOCOL_VERSION = 2`.

- [ ] Red: membership/wire round-trip tests with a Storage member (role, weight, address survive); version constant test updated. Green (compiler-guided literal ripple — every existing `MemberUpdate { ... }` gains `role: MemberRole::Compute, capacity_weight: 0, store_address: String::new()`). Commit `feat: role-tag gossip membership (wire v2)`.

### Task 2: HaltState + role-aware mutation routing

**Files:** `crates/seisin-node/src/halt.rs` (new), `crates/seisin-node/src/gossip_state.rs`, `crates/seisin-node/src/remote_store.rs` (addresses behind `RwLock`), `crates/seisin-node/src/gossip_server.rs`, `crates/seisin-node/src/gossip_client.rs`, `crates/seisin-node/src/server.rs`, `crates/seisin-node/src/main.rs`.

**Produces:**

```rust
// halt.rs
pub struct HaltState { halted: AtomicBool, reason: Mutex<Option<String>> }
impl HaltState { pub fn new() -> Self; pub fn halt(&self, reason: String); pub fn is_halted(&self) -> bool; pub fn reason(&self) -> Option<String>; }

// gossip_state.rs
pub struct ClusterState {
  pub compute_ring: Arc<RwLock<Ring>>,
  pub storage_ring: Arc<RwLock<Ring>>,
  pub store_addresses: Arc<RwLock<HashMap<NodeId, String>>>,
  pub halt: Arc<HaltState>,
}
pub fn apply_ready_mutations(gossip, cluster: &ClusterState, self_node_id, pool);
```

Routing per ready mutation: look up the node's role in the member table (default Compute when absent — pre-Part-B behavior). Compute → existing paths. Storage Join → `storage_ring.apply_join(node, update.capacity_weight.max(1))` + `store_addresses.write().insert(node, update.store_address)`. Storage Leave → `halt.halt(format!("cluster halted: storage node {node:?} confirmed dead — fail-stop (no replication in v1)"))`. `RemoteStore::new` takes `Arc<RwLock<HashMap<..>>>`; its lookups read-lock. `server.rs::handle_connection` gains an `Arc<HaltState>` param checked before dispatch (`OpError { message: reason }`); `serve` signature grows accordingly; `main.rs` builds one `ClusterState` (storage ring/address book from config even when empty) and threads it + halt everywhere.

- [ ] Red: gossip_state unit tests — storage join updates storage ring + address book; storage leave halts with the node named and leaves both rings untouched; compute leave still evicts/releases (existing test adapted). halt.rs unit test. Green; workspace build (3 call sites + server/serve signature ripple + integration tests' `serve(...)` calls gain a fresh `HaltState`). Commit `feat: role-aware mutation routing with coordinated fail-stop halt`.

### Task 3: storage gossip responder + main wiring

**Files:** `crates/seisin-node/src/gossip_server.rs` (add `serve_gossip_storage(listener, gossip: Arc<GossipState>)` — merge incoming, ack with piggyback, never applies mutations), `crates/seisin-node/src/main.rs` (storage branch also binds `gossip_address` and runs the responder; both branches seed the member table with roles/weights/store addresses from config).

- [ ] Green-by-construction (responder is 20 lines sharing the existing handler shape); `cargo build`; unit coverage arrives via Task 4's integration. Commit `feat: storage nodes join the gossip fabric as ack-only responders`.

### Task 4: integration, stress, docs

**Files:** `crates/seisin-node/tests/integration_storage_halt.rs`, `docs/superpowers/PROGRESS.md`.

Mirror `integration_gossip_failure_detection.rs`'s bootstrap: one compute node (fast probe/suspicion timeouts, real `run_gossip_loop` in a thread) + one storage member (delta-log store server + `serve_gossip_storage` on its gossip address), member table seeded with both. Assert: (1) an op writes through storage and reads back; (2) drop the storage node's gossip listener (and store listener); (3) within the suspicion window the compute node's detector confirms it dead and the halt engages — a client op now gets `OpError` containing "cluster halted" and the storage node's id; (4) the compute ring is untouched (its own node still owns its datums). Stress 10x + standing 20x suites; full gates; PROGRESS entry; push.

## Deferred (spec)
Part C migration/reweighting/compaction/durability; auto-resume; storage-side self-halt.
