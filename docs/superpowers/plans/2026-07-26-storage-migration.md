# Storage Migration & Reweighting (Storage Tier Part C-1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline execution — this project never uses subagents) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One unified client-side migration driver for storage node add / planned remove / capacity reweight (drain-live → pause → tail → flip → resume), plus its operational companions — log identity, a resumable pause flavor on the halt gate, and storage-side self-halt — per `docs/superpowers/specs/2026-07-26-storage-migration-design.md`.

**Architecture:** The storage node grows a stamped log identity, a paged id enumeration, a transfer engine (snapshot-copy with dirty-set tail), an `Identify` reply, and a self-halt heartbeat gate. The store wire (v2) and client/admin wire (v2) grow the request/response surface. The compute node's `HaltState` gains a resumable *pause* alongside the permanent *halt*, its client server grows an admin control plane (`GetClusterConfig`/`Pause`/`Resume`/`ClearHalt`/`InstallStorageRing`) that bypasses the op gate, and `apply_ready_mutations` stops auto-extending the storage ring on a storage Join (join now records availability only; a drained node's later Leave is ignored because it is no longer in the ring). A new `seisin-migrate` crate is the admin driver: it does all ring math and hands storage explicit id lists.

**Tech Stack:** Rust workspace, `anyhow`, blocking TCP + length-prefixed frames (`seisin_protocol::{read_frame,write_frame}`), jump-consistent-hash `Ring`, UUIDv7 (`uuid`), `tempfile` for test logs.

## Global Constraints

- **House style over the skill template.** This repo's plans (Parts A/B) use compressed per-task blocks — concrete `Produces:` signatures, test intent, one Red/Green/Commit checkbox — not the writing-plans skill's line-by-line 2-to-5-minute steps. This plan matches the repo (per GUIDELINES "match existing style first"). Executing it still means TDD per task: write the failing test first, then the minimal code.
- Crate versions stay `0.1.0` (deliberate pre-first-release deviation).
- Gates per task: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings` clean, full `cargo test --workspace` green.
- `seisin-node/src/lib.rs` has `#![deny(warnings)]` — new modules must be warning-clean.
- **Wire versioning:** the n±1 keep-old-decoder policy binds from the first deployed release; there have been none. So each version bump here *drops* the old decoder rather than preserving it — bump the constant, drop nothing to keep, and note the pre-first-release break in the module comment (same treatment as gossip wire v2 in Part B). Bumps in this plan: `STORE_PROTOCOL_VERSION 1→2`, `PROTOCOL_VERSION 1→2`, `GOSSIP_PROTOCOL_VERSION 2→3`, `DatumLog` `FORMAT_VERSION 1→2`.
- `Store` trait signatures stay infallible (unchanged by this part).
- Commit per task directly on `main`, trailer `Co-Authored-By: <model> <noreply@anthropic.com>` per house workflow. **(Open: the house trailer names "Claude Fable 5"; this session runs Opus 4.8 — confirm which name to stamp before the first commit.)**
- Stress discipline: 10× each new integration suite; whenever `seisin-node/src/worker.rs` changes, also the standing 20× loop over `integration_wound_wait`, `integration_cross_node_wound_wait`, `integration_op_collation`. (This plan does not touch `worker.rs`, so only the 10× new-suite runs are required — but re-check before assuming.)

---

## File / responsibility map

- `crates/seisin-storage/src/datum_log.rs` — grows a stamped 16-byte log id in the header + `log_id()` + paged `list_ids()`.
- `crates/seisin-storage/Cargo.toml` — add `uuid` (v7 generation for the log id).
- `crates/seisin-protocol/src/store_wire.rs` — store wire v2: new request/response variants + a `store_call` round-trip helper.
- `crates/seisin-protocol/src/lib.rs` — client/admin wire v2: admin requests + responses + `StorageMember`.
- `crates/seisin-node/src/store_server.rs` — `StoreNode` state bundle; `Identify`; self-halt gate; transfer/finish/retire dispatch.
- `crates/seisin-node/src/transfer.rs` (new) — `TransferManager` + snapshot-copy worker + dirty-set.
- `crates/seisin-node/src/heartbeat.rs` (new) — `Heartbeat` (last-heard) + pure `is_stale`.
- `crates/seisin-node/src/halt.rs` — resumable pause flavor + `clear_halt`.
- `crates/seisin-node/src/gossip_state.rs` — `ClusterState` gains `identity_book`; storage Join = availability-only; Leave halts only if in ring; identity reconcile.
- `crates/seisin-node/src/gossip_server.rs` — storage responder feeds the heartbeat; compute responder reconciles the identity book.
- `crates/seisin-node/src/server.rs` — admin control plane; op gate becomes halt-then-pause; admin requests bypass the gate.
- `crates/seisin-gossip/src/membership.rs` + `wire.rs` — `MemberUpdate` gains `log_id`; gossip wire v3.
- `crates/seisin-ring/src/ring.rs` — `contains`, `weights`, `node_ids` for diff/report math.
- `crates/seisin-migrate/` (new crate) — the admin driver lib + `main.rs` CLI.
- `crates/seisin-node/main.rs` — composition-root rewiring for every new bundle/param.

---

### Task 1: log identity + paged id enumeration (seisin-storage)

**Files:** `crates/seisin-storage/src/datum_log.rs`, `crates/seisin-storage/Cargo.toml`.

**Interfaces — Produces:**
- Header layout: `MAGIC(4) ++ FORMAT_VERSION:u16 LE ++ log_id:[u8;16]`; `FORMAT_VERSION = 2`; `HEADER_LEN = 22`. On create (empty file) a fresh `Uuid::now_v7()` is written and fsynced; on open the 16 bytes are read back into `self.log_id`.
- `pub fn log_id(&self) -> [u8; 16]`.
- `pub fn list_ids(&self, after: Option<[u8; 16]>, limit: usize) -> Vec<[u8; 16]>` — ids currently present (index keys), sorted ascending by raw bytes, strictly greater than `after`, capped at `limit`. Caller detects "done" when it gets back fewer than `limit`.

**Notes:** `uuid` is already a workspace-transitive dep (seisin-core uses it); add it to seisin-storage's `Cargo.toml` directly — the log id is opaque 16 bytes, so this does not couple the crate to `DatumId`. All record-offset math already keys off `HEADER_LEN`, so widening the header is a one-constant change. Bumping `FORMAT_VERSION` makes any v1 log fail to open — acceptable pre-first-release; note it in the module comment beside the version constant.

- [ ] **Red:** `log_id_is_stamped_at_creation_and_stable_across_reopen` (open → capture `log_id()` → drop → reopen same path → equal; and a second fresh path differs); `list_ids_pages_in_ascending_order` (insert several, page with `limit=2` walking `after`, assert full ascending set with no dupes/gaps, and a tombstoned id drops out). Keep the existing `wrong_magic_and_wrong_version_are_loud_open_errors` passing (it asserts a bad/`99` version errors — still true).
- [ ] **Green:** widen header, generate+persist+read the id, add the two accessors. `cargo test -p seisin-storage`.
- [ ] **Commit:** `feat: datum log stamps a log id and enumerates ids (Storage C-1 Task 1)`.

---

### Task 2: store wire v2 + round-trip client helper (seisin-protocol)

**Files:** `crates/seisin-protocol/src/store_wire.rs`.

**Interfaces — Produces:** `STORE_PROTOCOL_VERSION = 2`. New `StoreRequest` variants:
```rust
ListIds { after: Option<DatumId>, limit: u32 },
Transfer { transfer_id: DatumId, ids: Vec<DatumId>, dest_address: String },
TransferStatus { transfer_id: DatumId },
FinishTransfer { transfer_id: DatumId },
Retire { transfer_id: DatumId },
Identify,
```
New `StoreResponse` variants:
```rust
IdList { ids: Vec<DatumId>, done: bool },
TransferProgress { copied: u64, dirty: u64, done: bool },
Identity { node_id: NodeId, log_id: DatumId },
Error { message: String },
```
`Ack` continues to answer `Transfer`/`FinishTransfer`/`Retire`. New helper:
```rust
pub fn store_call(address: &str, request: &StoreRequest) -> anyhow::Result<StoreResponse>;
```
(connect a fresh `TcpStream`, `write_frame(encode_store_request)`, `read_frame`, `decode_store_response` — the storage→storage and driver→storage round-trip primitive.)

**Notes:** `store_wire.rs` already imports `DatumId`; add `use seisin_core::authority::NodeId;` for `Identity`. Follow the existing tag-byte + `put_bytes`/`take_*`-style codec already used in `lib.rs` (mirror those local helpers here, or inline length-prefix reads). Drop the v1 decoder (pre-first-release) and update the module-comment version note.

- [ ] **Red:** extend `round_trips_every_request_variant` / `round_trips_every_response_variant` to cover every new variant (incl. `ListIds { after: None }` and `after: Some(..)`, empty and non-empty `ids`, `done` both ways); a `store_call_round_trips_against_a_tiny_echo_listener` test (spawn a `TcpListener` that reads one frame and writes back `encode_store_response(&Ack)`; assert `store_call` returns `Ack`).
- [ ] **Green:** implement encode/decode + `store_call`. `cargo test -p seisin-protocol`.
- [ ] **Commit:** `feat: store wire v2 — transfer/identify/list-ids surface + store_call (Storage C-1 Task 2)`.

---

### Task 3: store server state bundle + Identify + self-halt heartbeat (seisin-node)

**Files:** `crates/seisin-node/src/heartbeat.rs` (new), `crates/seisin-node/src/store_server.rs`, `crates/seisin-node/src/lib.rs` (module decls), `crates/seisin-node/src/main.rs`, `crates/seisin-node/src/gossip_server.rs`, and every `serve_store(...)` caller (`remote_store.rs` tests, `integration_storage_tier.rs`, `integration_storage_halt.rs`).

**Interfaces — Produces:**
```rust
// heartbeat.rs
pub struct Heartbeat { last: Mutex<Instant> }
impl Heartbeat {
  pub fn new() -> Self;                       // last = Instant::now() — fresh boot counts as "just heard"
  pub fn record(&self);                       // last = Instant::now()
  pub fn is_stale(&self, threshold: Duration) -> bool; // now - last > threshold
}
pub fn stale(last: Instant, now: Instant, threshold: Duration) -> bool; // pure — the unit-tested seam

// store_server.rs
pub struct StoreNode {
  pub log: Arc<Mutex<DatumLog>>,
  pub node_id: NodeId,
  pub transfers: Arc<TransferManager>,        // introduced empty here, driven in Task 4
  pub heartbeat: Arc<Heartbeat>,
  pub self_halt_threshold: Duration,
}
pub fn serve_store(listener: TcpListener, node: Arc<StoreNode>);
```

**Behavior:** before serving any request, `if node.heartbeat.is_stale(node.self_halt_threshold)` reply `StoreResponse::Error { message: "storage node <id> self-halted: no gossip contact within <ms>ms" }` instead of touching the log (fail-stop symmetry — a partitioned storage node stops acking writes). `Identify` → `StoreResponse::Identity { node_id, log_id: DatumId::from_bytes(log.lock().log_id()) }`. Existing Put/Get/Delete/Patch dispatch unchanged. `serve_gossip_storage` gains an `Arc<Heartbeat>` param and calls `heartbeat.record()` on every accepted message. `main.rs` storage branch builds one `Arc<Heartbeat>`, threads it into both `serve_gossip_storage` and the `StoreNode`, sets `self_halt_threshold` from `SUSPICION_TIMEOUT_MILLIS` (reuse `seisin_gossip::failure_detector::SUSPICION_TIMEOUT_MILLIS`), and passes `node_id`.

**Notes:** `TransferManager` is declared here (Task 4 fills its behavior) so the `StoreNode` shape is final now and Task 4 needs no further signature churn — declare it as a `Default` empty manager. Existing `serve_store` callers pass a fresh `Arc<StoreNode>` with a large `self_halt_threshold` (e.g. `Duration::from_secs(3600)`) so unrelated tests never self-halt.

- [ ] **Red:** `heartbeat.rs` unit tests over `stale(...)` (fresh → not stale; last well before now-threshold → stale; exactly-at-threshold boundary). In `store_server.rs`: `identify_returns_node_id_and_log_id` (spin up a `StoreNode` on a tempdir, `store_call(addr, &Identify)`, assert `node_id` + `log_id` matches the log's); `a_stale_heartbeat_answers_error` (threshold `0`ms, no `record`, a `Get` returns `StoreResponse::Error`); `a_fresh_heartbeat_serves` (record, then `Get` works).
- [ ] **Green:** add `heartbeat` module, `StoreNode`, the gate, `Identify`; rewire `serve_store` signature and every caller + `main.rs`. `cargo test -p seisin-node` (+ the two storage integration suites still green).
- [ ] **Commit:** `feat: store server state bundle, Identify, and self-halt heartbeat (Storage C-1 Task 3)`.

---

### Task 4: transfer engine — snapshot copy + dirty tail + retire (seisin-node)

**Files:** `crates/seisin-node/src/transfer.rs` (new), `crates/seisin-node/src/store_server.rs`, `crates/seisin-node/src/lib.rs`.

**Interfaces — Produces:**
```rust
// transfer.rs
#[derive(Default)]
pub struct TransferManager { inner: Mutex<HashMap<DatumId, TransferState>> }
impl TransferManager {
  pub fn start(&self, transfer_id: DatumId, ids: Vec<DatumId>, dest: String);
  pub fn note_write(&self, id: DatumId);              // any active transfer holding `id` marks it dirty
  pub fn dest(&self, transfer_id: DatumId) -> Option<String>;
  pub fn ids(&self, transfer_id: DatumId) -> Vec<DatumId>;
  pub fn bump_copied(&self, transfer_id: DatumId, n: u64);
  pub fn mark_done(&self, transfer_id: DatumId);
  pub fn status(&self, transfer_id: DatumId) -> Option<(u64, u64, bool)>; // (copied, dirty, done)
  pub fn take_dirty(&self, transfer_id: DatumId) -> Vec<DatumId>;
  pub fn retire(&self, transfer_id: DatumId) -> Vec<DatumId>;             // remove transfer, return its ids
}
```

**Behavior in `store_server.rs` dispatch:**
- `Transfer { transfer_id, ids, dest_address }` → `transfers.start(...)`, spawn a worker thread that, for each id, reads `log.get(id)` and (if `Some`) `store_call(dest, &Put { id, bytes })`, `bump_copied(1)`; `mark_done()` at the end. Reply `Ack` immediately (copy is async).
- `Put`/`Patch`/`Delete` handlers additionally call `transfers.note_write(id)` after a successful apply (client writes keep flowing; any write to a transfer id lands in the dirty set → re-sent in the tail).
- `TransferStatus { transfer_id }` → `TransferProgress { copied, dirty, done }` (or `Error` if unknown).
- `FinishTransfer { transfer_id }` → for each `take_dirty()` id, re-`store_call(dest, &Put(current value))`; reply `Ack`.
- `Retire { transfer_id }` → for each `retire()` id, `log.delete(id)` (tombstone); reply `Ack`.

**Notes:** the worker thread holds `Arc<StoreNode>` (clone before spawn). Dirty tracking is deliberately over-inclusive (a write to a transfer id *before* that id's snapshot copy still re-sends it) — correct, and simpler than tracking per-id copy timestamps; note this in a `//` comment. `note_write` is O(active transfers) — fine for the single-migration-at-a-time model.

- [ ] **Red:** in `transfer.rs`, unit tests over the manager (start→ids/dest; note_write only dirties ids in the set; status counts; take_dirty drains; retire returns+removes). In `store_server.rs`, an end-to-end `transfer_copies_then_tails_a_dirty_write` with **two** in-process `StoreNode`s: seed source with 3 ids; `Transfer` to dest; poll `TransferStatus` until `done`; write a 4th value to one transferred id on the source; `FinishTransfer`; assert dest now `Get`s every id including the tailed new value; `Retire`; assert source `Get`s return `None` for the transferred ids.
- [ ] **Green:** implement the manager + dispatch + worker. `cargo test -p seisin-node`.
- [ ] **Commit:** `feat: storage transfer engine — snapshot copy, dirty tail, retire (Storage C-1 Task 4)`.

---

### Task 5: resumable pause flavor on HaltState (seisin-node)

**Files:** `crates/seisin-node/src/halt.rs`.

**Interfaces — Produces:** `HaltState` gains `paused: AtomicBool`, `pause_reason: Mutex<Option<String>>`, and:
```rust
pub fn pause(&self, reason: String);   // set paused + reason (last writer wins; driver-owned)
pub fn resume(&self);                  // clears pause only — never clears a halt
pub fn is_paused(&self) -> bool;
pub fn pause_reason(&self) -> Option<String>;
pub fn clear_halt(&self);              // clears the permanent halt — driver-only, post identity-verify
/// The single gate answer: halt wins over pause; None = serve.
pub fn gate(&self) -> Option<String>;  // Some("cluster halted: ..") | Some("cluster paused: ..") | None
```
`gate()`: if halted → `Some(reason.unwrap_or("cluster halted"))`; else if paused → `Some(format!("cluster paused: {}", pause_reason.unwrap_or_default()))`; else `None`. The distinct `"cluster paused"` vs `"cluster halted"` prefix is how clients tell "retry shortly" from "cluster is down".

**Notes:** `halt()` keeps first-reason-wins; `clear_halt()` resets both the flag and the stored reason so a subsequent genuine halt can re-arm. Halt precedence is enforced purely in `gate()`.

- [ ] **Red:** `pause_then_resume_round_trips` (pause → `is_paused` + gate says "cluster paused" with the reason; resume → not paused, gate `None`); `halt_beats_pause_in_the_gate` (pause + halt set → gate returns the halt reason; resume does not clear the halt); `clear_halt_lets_a_later_halt_rearm`.
- [ ] **Green:** add the fields + methods. `cargo test -p seisin-node halt`.
- [ ] **Commit:** `feat: resumable pause flavor + clear_halt on the halt gate (Storage C-1 Task 5)`.

---

### Task 6: client/admin wire v2 (seisin-protocol)

**Files:** `crates/seisin-protocol/src/lib.rs`.

**Interfaces — Produces:** `PROTOCOL_VERSION = 2`. New:
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMember { pub node_id: NodeId, pub weight: u32, pub store_address: String, pub log_id: DatumId }

// Request variants:
GetClusterConfig,
Pause { reason: String },
Resume,
ClearHalt,
InstallStorageRing { members: Vec<StorageMember> },

// Response variants:
ClusterConfig { members: Vec<StorageMember> },   // ring members + store addresses + log ids, one shot
Ack,                                             // for Pause/Resume/ClearHalt/InstallStorageRing
```
Plus `pub fn encode_storage_member` / `decode_storage_member` building blocks (reuse the file's `put_id`/`take_id`/`put_bytes`/`take_bytes`/`take_u32`). Assign fresh opcode/response tag bytes after the existing ones (`OP_*` up to 14, `RESP_*` up to 12 today).

**Notes:** drop the v1 decoder (pre-first-release) and update the `PROTOCOL_VERSION` doc-comment note. The `ClusterConfig.members` list is the driver's whole planning input — ring membership + weights + addresses + log ids in one reply.

- [ ] **Red:** round-trip every new request + response variant (incl. empty and multi-member `InstallStorageRing`/`ClusterConfig`, and `Pause` with a reason string); a `storage_member_round_trips` unit test.
- [ ] **Green:** implement encode/decode. `cargo test -p seisin-protocol`.
- [ ] **Commit:** `feat: client/admin wire v2 — cluster config, pause/resume, install-ring (Storage C-1 Task 6)`.

---

### Task 7: MemberUpdate log_id + identity book + ring helpers (gossip, ring, node)

**Files:** `crates/seisin-gossip/src/membership.rs`, `crates/seisin-gossip/src/wire.rs`, `crates/seisin-ring/src/ring.rs`, `crates/seisin-node/src/gossip_state.rs`, `crates/seisin-node/src/gossip_server.rs`, `crates/seisin-node/src/main.rs`, and every `MemberUpdate { .. }` / `ClusterState { .. }` literal (gossip tests, `integration_storage_halt.rs`, gossip_state tests).

**Interfaces — Produces:**
- `MemberUpdate` gains `pub log_id: [u8; 16]` (all-zeros for compute/unknown). Gossip wire appends 16 raw bytes; `GOSSIP_PROTOCOL_VERSION = 3` (pre-first-release drop-old note).
- `seisin-ring`: `pub fn contains(&self, node_id: NodeId) -> bool`; `pub fn weights(&self) -> Vec<(NodeId, u32)>` (slot count per node, **first-appearance order** — so a rebuild via `from_members(&weights)` reproduces placement); `pub fn node_ids(&self) -> Vec<NodeId>` (unique, first-appearance order).
- `ClusterState` gains `pub identity_book: Arc<RwLock<HashMap<NodeId, DatumId>>>` (log id per storage member).
- `pub fn reconcile_identity_book(gossip: &GossipState, cluster: &ClusterState)` — for every `MemberRole::Storage` member in the table whose `log_id` is non-zero, insert `node_id → DatumId::from_bytes(log_id)` into `identity_book`. Called from `serve_gossip` (compute) after `apply_ready_mutations`.

**Notes:** the storage node's `main.rs` self-seed sets `log_id` from its own `log.lock().log_id()`; every other `MemberUpdate` literal gets `log_id: [0u8; 16]`. Compute nodes learn each storage node's real log id when that node's self-update piggybacks in via gossip, then `reconcile_identity_book` copies it into the book — this populates initial config-seeded storage members without needing a Join mutation. `weights()` first-appearance order is load-bearing for deterministic ring rebuild — assert it in a ring test.

- [ ] **Red:** gossip wire round-trips a `log_id`; ring `contains`/`weights`/`node_ids` tests (incl. `weights` order + a multi-slot node counted once with its slot count); `reconcile_identity_book_copies_storage_log_ids` (seed a storage member with a non-zero log id → book gains it; a zero log id is skipped; a compute member is skipped).
- [ ] **Green:** add the field + wire bytes + ring helpers + book + reconcile; ripple every literal (compiler-guided). `cargo test -p seisin-gossip -p seisin-ring -p seisin-node`.
- [ ] **Commit:** `feat: gossip carries log identity; identity book + ring diff helpers (Storage C-1 Task 7)`.

---

### Task 8: storage Join = availability-only; Leave halts only if in ring (seisin-node)

**Files:** `crates/seisin-node/src/gossip_state.rs`.

**Behavior change in `apply_ready_mutations`:**
- **Storage `Join`** no longer calls `storage_ring.apply_join`. It records availability only: insert `store_address` into `store_addresses` and (if the update's `log_id` is non-zero) `node_id → log_id` into `identity_book`. The node enters the ring *only* via a driver `InstallStorageRing` (Task 9). Rationale: extending a jump-hash ring re-homes existing keys whose bytes never moved — a latent read-miss bug once real data exists.
- **Storage `Leave`** halts *only if* `storage_ring.read().contains(node_id)`. A drained node was removed from the ring at flip time, so its later Leave finds it absent and is ignored (planned removal avoids the halt for free). A still-present node's Leave halts exactly as before.

**Notes:** this rewrites the two Part-B storage branches and the two Part-B tests that asserted the old auto-extend behavior. Keep the compute Join/Leave branches and the cache-eviction/lock-release tail untouched.

- [ ] **Red:** rewrite `a_storage_join_extends_the_storage_ring_and_address_book` → `a_storage_join_records_availability_without_touching_the_ring` (after apply: `store_addresses` learned the address, `identity_book` learned the log id, but `storage_ring` is unchanged — still routes to whatever it did before, and `contains(joined)` is false). Add `a_storage_leave_is_ignored_when_the_node_is_not_in_the_ring` (empty/absent ring → no halt). Keep `a_storage_leave_engages_the_halt_and_touches_no_ring` but ensure the node **is** in the ring first (seed a one-member storage ring for that node).
- [ ] **Green:** rewrite the two storage branches. `cargo test -p seisin-node gossip_state`.
- [ ] **Commit:** `feat: storage Join records availability only; drained Leave no longer halts (Storage C-1 Task 8)`.

---

### Task 9: compute admin control plane (seisin-node)

**Files:** `crates/seisin-node/src/server.rs`, `crates/seisin-node/src/main.rs`, and every `serve(...)` caller (`integration_storage_halt.rs`, `integration_multi_node_routing.rs`, any other integration suite that calls `serve`).

**Interfaces — Produces:** `serve` signature becomes
```rust
pub fn serve(
  listener: TcpListener,
  self_node_id: NodeId,
  cluster: Arc<ClusterState>,               // compute_ring/storage_ring/store_addresses/identity_book/halt
  address_book: Arc<HashMap<NodeId, String>>, // compute client addresses (unchanged role)
  pool: Arc<WorkerPool>,
);
```
(`compute_ring` and `halt` now come from `cluster`; drop the standalone `ring`/`halt` params.)

**Behavior in `handle_connection`:**
- **Gate restructure:** admin requests (`GetClusterConfig`, `Pause`, `Resume`, `ClearHalt`, `InstallStorageRing`) are dispatched *before/around* the op gate — they must be served while halted or paused (read-only control plane + the flip itself). Op-shaped requests (everything else) consult `cluster.halt.gate()`: `Some(message)` → `Response::OpError { message }` (covers both halt and pause), `None` → dispatch as today.
- `GetClusterConfig` → build `ClusterConfig { members }` from `storage_ring.weights()` joined with `store_addresses` and `identity_book` (one `StorageMember` per ring node, in `weights()` order; missing address → empty string, missing log id → zero id).
- `Pause { reason }` → `cluster.halt.pause(reason)` → `Ack`. `Resume` → `cluster.halt.resume()` → `Ack`. `ClearHalt` → `cluster.halt.clear_halt()` → `Ack`.
- `InstallStorageRing { members }` → rebuild the shared storage ring in place: `*storage_ring.write() = Ring::from_members(&members.iter().map(|m| (m.node_id, m.weight)).collect::<Vec<_>>())` (**wire order preserved** — matches the driver's proposed-ring math); replace `store_addresses` contents with `members`' addresses; replace `identity_book` contents with `members`' log ids. `Ack`.

**Notes:** `Ring::from_members` takes `&[(NodeId, u32)]`. Because the shared `Arc<RwLock<Ring>>` is the same object `RemoteStore` reads, the flip is a live swap with no restart (Part B's shared-structure design). `main.rs` passes the single `cluster` it already builds; the storage-node branch never calls `serve`. Integration tests that call `serve` now build a `ClusterState` (most already do for the halt test) and pass it.

- [ ] **Red:** `pause_rejects_ops_but_serves_cluster_config` (in-process compute node: `Pause` → an `Op` returns `OpError` containing "cluster paused"; `GetClusterConfig` still returns `ClusterConfig`; `Resume` → the `Op` succeeds); `install_storage_ring_swaps_placement` (start with a 1-node storage ring, `GetClusterConfig` shows it; `InstallStorageRing` with a different single member; `GetClusterConfig` now reports the new member and `storage_ring.native(id).0` changed). Prefer driving these over the wire via `seisin_client::call` against a real `serve` thread.
- [ ] **Green:** restructure the gate, add the five handlers, ripple `serve` callers + `main.rs`. `cargo test -p seisin-node`.
- [ ] **Commit:** `feat: compute admin control plane — cluster config, pause/resume, install-ring flip (Storage C-1 Task 9)`.

---

### Task 10: migration driver crate (seisin-migrate)

**Files:** `crates/seisin-migrate/Cargo.toml` (new), `crates/seisin-migrate/src/lib.rs` (new), `crates/seisin-migrate/src/main.rs` (new), workspace `Cargo.toml` members list.

**Dependencies:** `seisin-protocol`, `seisin-ring`, `seisin-core`, `seisin-client`, `anyhow`. (No dep on `seisin-node` — the driver is pure client-side.)

**Interfaces — Produces (lib):**
```rust
/// The moved set: for the id corpus, every id whose owning node differs
/// between `old` and `new`, grouped by (source_node → dest_node).
pub fn plan_moves(old: &Ring, new: &Ring, ids: &[DatumId]) -> Vec<Move>;
pub struct Move { pub id: DatumId, pub source: NodeId, pub dest: NodeId }

/// Full driver: compute → GetClusterConfig, enumerate ids per source
/// (ListIds paging), plan, and (when apply) bulk-copy → pause → tail →
/// flip → resume → retire. Prints the plan; only mutates when `apply`.
pub fn migrate(compute_addr: &str, proposed: &[StorageMember], apply: bool) -> anyhow::Result<Report>;

/// Resume-after-halt: Identify each named node, verify (node_id, log_id)
/// against the compute's identity book (GetClusterConfig, readable while
/// halted); all match → ClearHalt on every compute node; any mismatch →
/// refuse (impostor), halt stands.
pub fn resume(compute_addrs: &[String]) -> anyhow::Result<()>;
```

**Driver flow (matches spec §2), all storage I/O via `seisin_protocol::store_call`, all compute I/O via `seisin_client::call`:**
1. **Plan:** `GetClusterConfig` from `compute_addr` → current `StorageMember`s → build `old = Ring::from_members(current)`, `new = Ring::from_members(proposed)`. For each current member, `ListIds` (page with `after`/`limit` until a short page) to enumerate its ids. `plan_moves(old, new, all_ids)`. Print per-(source→dest) counts. If `!apply`, stop here (dry run — the default; `--apply` required to mutate).
2. **Bulk copy:** per source, one fresh `transfer_id = DatumId::new()`, `Transfer { transfer_id, ids, dest_address }`; poll `TransferStatus` until `done`.
3. **Pause:** `Pause { reason: "migrating" }` to every compute node (the driver is given, or reads from config, all compute client addresses).
4. **Tail:** `FinishTransfer { transfer_id }` to every source.
5. **Flip:** `InstallStorageRing { members: proposed }` to every compute node; hold the pause until every install `Ack`s.
6. **Resume:** `Resume` to every compute node; then `Retire { transfer_id }` to every source.

**Crash safety / idempotency (spec §2):** a crashed driver leaves the cluster on the old ring with inert extra dest copies (unreachable until a flip names them owner). Re-running re-copies under a fresh `transfer_id` (Put is last-write-wins) and re-flips under the pause. `Retire` is the only destructive step and is gated behind every compute node's flip `Ack`. Any command error aborts the driver, leaving the old ring live.

**CLI (`main.rs`):** reads a RON file naming the compute client addresses + the proposed `StorageMember` set (add = current+new node, remove = current−node, reweight = new weights); `--apply` executes, absent = dry-run plan print; a `resume` subcommand runs `resume(...)`. Two-phase plan→execute per GUIDELINES (dry-run default, explicit `--apply`).

**Notes:** `plan_moves` is pure ring math and the crate's testable core; the wire orchestration is covered by Task 11's integration suites (which drive the real driver against in-process nodes). Keep `migrate`/`resume` thin over `plan_moves` + `store_call` + `call` so the integration tests exercise them directly.

- [ ] **Red:** `plan_moves` unit tests — add (1→2 storage nodes: some ids move to the new node, the rest stay, sources correct); remove (2→1: every id on the removed node moves to the survivor, none move off the survivor); reweight ((1,1)→(1,3): a non-empty subset moves toward the heavier node); empty diff (identical rings → no moves). Build them from `Ring::from_members` with fixed corpora of `DatumId::new()`.
- [ ] **Green:** create the crate, implement `plan_moves`, `migrate`, `resume`, `main.rs`. Add to workspace members. `cargo test -p seisin-migrate`; `cargo build -p seisin-migrate`.
- [ ] **Commit:** `feat: storage migration driver crate — plan/copy/pause/tail/flip/resume (Storage C-1 Task 10)`.

---

### Task 11: integration, stress, docs

**Files:** `crates/seisin-node/tests/integration_storage_migration.rs` (new), `crates/seisin-node/Cargo.toml` (dev-dep `seisin-migrate`), `docs/superpowers/PROGRESS.md`, `CLAUDE.md` (resume snapshot).

**Harness:** mirror `integration_storage_halt.rs`'s bootstrap — in-process storage `StoreNode`s (store server + `serve_gossip_storage` + heartbeat) and compute node(s) (`serve` + `serve_gossip` + `run_gossip_loop`, fast 20/20/40ms timeouts), member table seeded with roles/weights/addresses/log ids. Drive the real `seisin_migrate::{migrate,resume}` against them. Cover spec §8:

- **Live add:** 2 storage nodes, write a corpus of byte datums through compute; driver admits a third (proposed = 3 members); every datum reads back; post-flip `native(id).0` matches the new ring.
- **Planned remove:** drain a node (proposed = survivors); assert its subsequent gossip `Leave` does **not** halt (it left the ring at flip); all data readable from survivors.
- **Reweight:** change a weight; a non-empty subset moves; corpus fully readable.
- **Concurrent writes:** issue writes to soon-to-move ids *during* the bulk-copy phase; after the flip they read back the latest value (dirty-set tail proven end-to-end).
- **Halt + resume:** kill a storage node (halt engages via the detector, as in the Part B test), restart a `StoreNode` on the **same** tempdir (same log id), `resume(...)` clears the halt, every previously-acked write reads back.
- **Impostor:** restart on an **empty** tempdir (fresh log id); `resume(...)` refuses (mismatch) and the halt stands; a client op still gets `OpError` containing "cluster halted".

- [ ] **Red/Green:** write the suite; iterate until green.
- [ ] **Stress:** `integration_storage_migration` 10× green (loop it). Full gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`.
- [ ] **Docs:** PROGRESS.md — move Storage Tier Part C-1 to "Done" (migration + reweighting + log identity + pause + self-halt), note what Part C still defers (log compaction, tk/lb datum-grade durability, group commit, copy/insert deltas, chunk-aware wire, hot-value LRU, replication). Refresh the CLAUDE.md resume snapshot. Commit `feat: live storage migration/reweight/add-remove + resume end-to-end (Storage Tier Part C-1)`; push.

---

## Deferred (spec §Not-in-scope, restated)

Replication (crashes still halt; recovery is log-dir restore + resume); log compaction; tk/lb B+Tree datum-grade durability; group commit; copy/insert deltas; chunk-aware wire; hot-value LRU. Each is its own later part.

## Self-review against the spec

- §1 ring-change/moved-set → Task 10 (`plan_moves`); Join-no-longer-auto-extends + drained-Leave-ignored → Task 8. ✓
- §2 driver protocol (plan→copy→pause→tail→flip→resume) + crash safety + `--apply` → Task 10; wire pieces it calls → Tasks 2/4/6/9. ✓
- §3 pause flavor + precedence → Task 5 (state) + Task 9 (gate). ✓
- §4 log id + `Identify` + identity book + resume + impostor + `ClearHalt` → Tasks 1/3/7/9/10. ✓
- §5 self-halt heartbeat + `StoreResponse::Error` → Task 3 (`RemoteStore` already panics on a non-`Ack`/`Value`, which the detector converts to the halt — verified against `remote_store.rs`). ✓
- §6 wire additions (store v2 / client v2) → Tasks 2/6. ✓
- §7 tk/lb resident files rebuild — no code (migration moves only log content); asserted implicitly by the corpus round-trips. ✓
- §8 testing (unit + all six integration scenarios + stress/gates) → Tasks 1–10 units + Task 11. ✓
