# Deployment & Cluster Tests (Sub-project 5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline execution — this project never uses subagents) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A real-process, real-socket cluster test harness under `cargo test`, plus a cross-node correctness suite exercising routing, wound-wait, crash reclaim/halt, migration, and replication across genuine `seisin-node` processes — per `docs/superpowers/specs/2026-07-27-deployment-cluster-tests-design.md`.

**Architecture:** Failure-detection timeouts become optional `NodeConfig` fields (default = today's constants) so the harness can run fast. `main.rs`'s composition root is extracted to a reusable `seisin_node::node::run(config, ops, index_kinds)` so both the bare binary and a test-only `cluster_test_node` binary (byte ops) share it. A `ClusterHarness` in the integration test generates configs, spawns node processes (storage-first) over localhost, drives them via `seisin-client` and the `seisin_migrate` library, kills them (SIGKILL) for crash scenarios, and reaps them on drop.

**Tech Stack:** Rust, `std::process::Command`, `tempfile`, RON config, `seisin-client` / `seisin-migrate` (dev-deps), `CARGO_BIN_EXE_cluster_test_node`.

## Global Constraints

- House style: compressed per-task blocks; TDD where a unit boundary exists (config parsing), scenario-as-test for the harness.
- Crate versions stay `0.1.0`.
- Gates per task: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings` clean, full `cargo test --workspace` green.
- `seisin-node/src/lib.rs` has `#![deny(warnings)]`.
- Commit per task directly on `main`, trailer `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- No `worker.rs` change here, so the standing 20× wound-wait loop is not triggered; stress is 10× the new cluster suite.
- Production behavior must be unchanged: omitting the new config fields yields exactly today's timeouts, and the bare `seisin-node` binary stays op-less.

## File / responsibility map

- `crates/seisin-node/src/config.rs` — optional timeout fields + defaulting accessors.
- `crates/seisin-node/src/node.rs` (new) — `run(config, ops, index_kinds)`, extracted from `main.rs`.
- `crates/seisin-node/src/main.rs` — thin wrapper over `node::run` (bare, op-less).
- `crates/seisin-node/src/bin/cluster_test_node.rs` (new) — `node::run` with byte ops.
- `crates/seisin-node/tests/integration_cluster.rs` (new) — `ClusterHarness` + the six scenarios.
- `docs/superpowers/PROGRESS.md`, `CLAUDE.md` — sub-project 5 done entry + snapshot.

---

### Task 1: configurable failure-detection timeouts (config + main)

**Files:** `crates/seisin-node/src/config.rs`, `crates/seisin-node/src/main.rs`.

**Produces:** `NodeConfig` gains four `#[serde(default)] Option<u64>` fields — `probe_interval_millis`, `probe_timeout_millis`, `suspicion_timeout_millis`, `self_halt_threshold_millis` — plus defaulting accessors:
```rust
impl NodeConfig {
  pub fn probe_interval_millis(&self) -> u64 { self.probe_interval_millis.unwrap_or(seisin_gossip::failure_detector::PROBE_TIMEOUT_MILLIS) }
  pub fn probe_timeout_millis(&self) -> u64  { self.probe_timeout_millis.unwrap_or(seisin_gossip::failure_detector::PROBE_TIMEOUT_MILLIS) }
  pub fn suspicion_timeout_millis(&self) -> u64 { self.suspicion_timeout_millis.unwrap_or(seisin_gossip::failure_detector::SUSPICION_TIMEOUT_MILLIS) }
  pub fn self_halt_threshold_millis(&self) -> u64 { self.self_halt_threshold_millis.unwrap_or(seisin_gossip::failure_detector::SUSPICION_TIMEOUT_MILLIS) }
}
```
`main.rs` uses these accessors for `run_gossip_loop`'s three timeout args and the storage `StoreNode.self_halt_threshold` (`Duration::from_millis(config.self_halt_threshold_millis())`).

**Notes:** `config.rs` already depends on nothing preventing the `seisin_gossip` import (it's a workspace dep of `seisin-node`). The `SAMPLE` config test constant omits the new fields — proving the absent→default path.

- [ ] **Red:** `timeouts_default_to_the_production_constants` (parse `SAMPLE` → accessors return 1000/1000/5000/5000); `timeouts_are_read_when_present` (a RON with the four fields set → accessors return them).
- [ ] **Green:** add fields + accessors; wire `main.rs`. `cargo test -p seisin-node config`.
- [ ] **Commit:** `feat: configurable failure-detection timeouts in node config (SP5 Task 1)`.

---

### Task 2: extract `node::run`, add `cluster_test_node`, and the ClusterHarness + routing scenario

**Files:** `crates/seisin-node/src/node.rs` (new), `crates/seisin-node/src/lib.rs` (module decl), `crates/seisin-node/src/main.rs`, `crates/seisin-node/src/bin/cluster_test_node.rs` (new), `crates/seisin-node/tests/integration_cluster.rs` (new), `crates/seisin-node/Cargo.toml` (ensure `seisin-migrate` dev-dep — already present from C-1).

**Produces:**
```rust
// node.rs — the whole of main.rs's body, verbatim, parameterized by the op/index registries.
pub fn run(
  config: crate::config::NodeConfig,
  ops: seisin_ops::registry::OpRegistry,
  index_kinds: crate::index_handler::IndexKindRegistry,
) -> anyhow::Result<()>;
```
`main.rs` becomes `node::run(NodeConfig::load(&path)?, OpRegistry::new(), IndexKindRegistry::new())` (bare, op-less — unchanged behavior). `cluster_test_node.rs` builds an `OpRegistry` with byte ops `put1`/`get1` (`ctx.put`/`ctx.get`) and `put2`/`get2` (`ctx.put_replicated`/`get_replicated` at factor 2), then calls `node::run`.

`ClusterHarness` (in `integration_cluster.rs`):
```rust
enum Role { Compute, Storage }
struct NodeSpec { id: u64, role: Role, thread_count: u32, weight: u32 }
struct ClusterHarness { /* tempdir, children, per-node addrs */ }
impl ClusterHarness {
  fn start(specs: &[NodeSpec]) -> Self;      // alloc ports, write RON per node (fast timeouts), spawn storage-first, barrier on client/store ports
  fn compute_addr(&self, id: u64) -> String;
  fn store_addr(&self, id: u64) -> String;
  fn kill(&mut self, id: u64);               // Child::kill (SIGKILL)
  fn op(&self, compute_id: u64, name: &str, ids: Vec<DatumId>, payload: &[u8]) -> anyhow::Result<Response>;  // via seisin-client
}
impl Drop for ClusterHarness { /* kill every child */ }
```
Port allocation: bind `127.0.0.1:0`, record, drop (existing reserve pattern). Barrier: poll-connect each node's client (and store) port until it accepts, with a bounded deadline. Configs share one `members` list; each node gets a `data_dir` under the tempdir and the fast timeouts (20/20/40/40 ms).

- [ ] **Red/Green:** the routing scenario `a_client_op_is_served_across_two_compute_nodes` — two compute nodes; `put1` a datum, `get1` it back through *either* node (the redirect resolves over real sockets). Green-by-construction for `run`/`cluster_test_node` (compiler-guided extraction). `cargo test -p seisin-node --test integration_cluster`.
- [ ] **Verify:** `cargo build -p seisin-node --bin seisin-node --bin cluster_test_node` (both binaries build); run the scenario 3× (spawn stability).
- [ ] **Commit:** `feat: node::run extraction, cluster_test_node binary, and process cluster harness (SP5 Task 2)`.

---

### Task 3: crash and wound-wait scenarios

**Files:** `crates/seisin-node/tests/integration_cluster.rs`.

**Produces** three scenarios on the real harness:
- `a_killed_compute_node_is_reclaimed_and_ops_keep_succeeding` — 2 compute nodes; write keys; `kill` one; sleep past the fast suspicion window; assert every subsequent client op to the survivor succeeds (ring converged, keys re-homed, cache reload from storage). Uses one storage node so reads survive the compute kill.
- `a_cross_node_op_completes_under_contention` — 2 compute nodes; an op naming two datums native to different nodes completes with the expected result (foreign pull + release over the real peer link).
- `a_killed_storage_node_halts_client_traffic` — 2 storage + 1 compute, N=1 corpus; `kill` a storage node; poll until a client op returns the "cluster halted" reason (point-of-use halt over real sockets).

**Notes:** all timing keys off the harness's fast timeouts; the halt poll mirrors the in-process pattern (fail-stop worker → gated retries). Reuse the corpus/ids helpers from Task 2.

- [ ] **Red/Green:** write the three scenarios; iterate to green.
- [ ] **Commit:** `feat: cluster crash-reclaim, cross-node, and storage-halt scenarios (SP5 Task 3)`.

---

### Task 4: migration + replication scenarios, stress, docs

**Files:** `crates/seisin-node/tests/integration_cluster.rs`, `docs/superpowers/PROGRESS.md`, `CLAUDE.md`.

**Produces** two scenarios driving the real `seisin_migrate` library against the spawned cluster:
- `a_live_migration_admits_a_third_storage_node` — 2 storage + 1 compute; write an N=1 corpus via `put1`; build the proposed 3-member `StorageMember` set (store addresses from the harness, log id zero → driver resolves via `Identify`); `seisin_migrate::migrate(&[compute_addr], &proposed, true)`; assert every datum reads back and at least one now lives on the new node (per `Ring::replicas`/`native` of the new ring).
- `replication_survives_a_replica_kill_and_recover_restores_it` — 3 storage + 1 compute; write a corpus via `put2` (factor 2); `kill` one storage node; assert `get2` reads still succeed (failover, no halt); `seisin_migrate::recover(&[compute_addr], true)`; assert the corpus is fully readable and re-replicated onto the survivors.

**Notes:** the migrate/recover calls are the real driver as a library — its TCP hits the spawned nodes. The compute node must know the initial storage set (config) and, for the add, the new node must be reachable and gossiped in (start all three storage nodes up front; the driver's `InstallStorageRing` flips the ring). Confirm the harness starts the to-be-added node so `Identify` reaches it.

- [ ] **Red/Green:** write both scenarios; iterate to green.
- [ ] **Stress:** `integration_cluster` 10× green. Full gates.
- [ ] **Docs:** PROGRESS.md — Sub-project 5 "Done" entry (harness + configurable timeouts + six scenarios); note the deployment-management system and Docker remain out. Refresh the CLAUDE.md snapshot. Commit `feat: live migration + replication cluster scenarios; SP5 harness complete (Sub-project 5)`; push.

---

## Deferred (spec §Not-in-scope, restated)

Deployment *management* system (n→n+1 rollout orchestration, uniform-version enforcement, update ordering); Docker/container orchestration (a container variant reusing these scenarios is a later option); graceful-leave signal handling on the node (clean exit currently == crash to the ring, which is the intended equivalence).

## Self-review against the spec

- §1 configurable timeouts → Task 1. ✓
- §2 harness (ports, config gen, spawn storage-first, barrier, kill, drop) + `cluster_test_node` + `node::run` extraction + migrate-as-library → Task 2 (+ used in 3/4). ✓
- §3 six scenarios: routing (T2), wound-wait + compute-kill + storage-halt (T3), migration add + replication failover/recover (T4). ✓
- §4 gating/determinism (default suite, startup barrier, 10× stress) → Tasks 2/4. ✓
- §5 testing (config unit test + scenarios + stress + gates) → all tasks. ✓
