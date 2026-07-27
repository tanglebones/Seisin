# Deployment & Cluster Tests (Sub-project 5) — Design

A real-process, real-socket cluster test harness that runs under plain
`cargo test`, plus a cross-node correctness suite that exercises the
storage-tier and routing machinery end-to-end across genuine
`seisin-node` OS processes talking over localhost TCP — validating the
wire protocols, gossip, migration, and replication over actual sockets
rather than the in-process simulations the existing integration tests
use.

**Not in scope**: the deployment *management* system (central n→n+1
rollout orchestration, uniform-version enforcement, storage→compute→
client update ordering) — a separate, still-undesigned concern (see the
main design doc's Open Questions). Docker/container orchestration is also
out; spawned OS processes cover the real-socket path, and a container
variant reusing the same scenarios is a possible later addition.

## 1. Enabling change: configurable failure-detection timeouts

The spawned node binary currently hardcodes the `failure_detector`
constants (probe 1000 ms, suspicion 5000 ms), which would make every
crash-detection scenario wait 5 s+. `NodeConfig` (RON) gains four
**optional** fields, each defaulting to today's constant when omitted:

- `probe_interval_millis` (default = `PROBE_TIMEOUT_MILLIS` = 1000)
- `probe_timeout_millis` (default = `PROBE_TIMEOUT_MILLIS` = 1000)
- `suspicion_timeout_millis` (default = `SUSPICION_TIMEOUT_MILLIS` = 5000)
- `self_halt_threshold_millis` (default = `SUSPICION_TIMEOUT_MILLIS` = 5000)

`main.rs` reads them for `run_gossip_loop`'s three timeout arguments and
for the storage node's `StoreNode.self_halt_threshold`. Production
behavior is unchanged (omit → same constants); the harness sets fast
values (≈20/20/40/40 ms) so a killed node converges to dead in ≈1 s.
This is also a genuine improvement — these are operational knobs that
should be tunable per deployment without a recompile.

## 2. The harness (`seisin-node/tests/`)

Lives under `seisin-node/tests/` specifically so Cargo provides
`env!("CARGO_BIN_EXE_<bin>")` — the path to a freshly-built binary in the
`seisin-node` package — to the integration test.

**Test solution binary.** The bare `main.rs` composition root registers
empty op/index registries (a real solution supplies those in its own
binary), so a spawned bare node can serve no client ops. Sub-project 5
adds a small test-only binary `crates/seisin-node/src/bin/
cluster_test_node.rs`: the same composition root as `main.rs` but with an
`OpRegistry` carrying a handful of byte ops — `put1`/`get1` (single copy)
and `put2`/`get2` (`ctx.put_replicated`/`get_replicated` at factor 2) —
mirroring what the in-process integration tests register. The harness
spawns *this* binary (`CARGO_BIN_EXE_cluster_test_node`). Using byte ops
(not a typed `seisin-types` schema) keeps the test node dependency-light
while still exercising single-copy and replicated storage paths
end-to-end.

**Driving migration.** The nodes are the only spawned processes; the
migration/recover scenarios call the `seisin_migrate` **library**
(`migrate`/`recover`, already a dev-dependency) directly from the test —
it is a client, and its store/admin-wire calls hit the real spawned
nodes over TCP. (Spawning the `seisin-migrate` binary is unnecessary and
its path isn't provided to another package's tests anyway.)

A `ClusterHarness` support type (in a shared `mod cluster_harness`):

- **Port allocation**: bind `127.0.0.1:0`, record the port, drop the
  listener (the existing reserve-a-port pattern). Each node needs three
  ports (client, gossip, peer-link) plus a store port for storage nodes.
- **Config generation**: write one RON `NodeConfig` per node to a
  `tempfile::TempDir` — a shared `members` list (every node's ids,
  addresses, roles, weights), a distinct `self_node_id`, a per-node
  `data_dir` under the tempdir, and the fast timeout fields.
- **Spawn**: `Command::new(env!("CARGO_BIN_EXE_seisin-node"))
  .env("SEISIN_NODE_CONFIG", <path>).spawn()`, **storage nodes first**
  (the design doc's storage→compute deploy order), then a startup
  barrier that polls each node's client (and store) port until it
  accepts a connection before returning.
- **Drive**: helpers to run a client op via `seisin-client` against a
  node's client address, and to invoke `seisin_migrate::{migrate,
  recover}` (library) against the spawned cluster's compute addresses.
- **Fault injection**: `kill(node)` sends SIGKILL (`Child::kill`) — the
  crash case. The node has no graceful-leave handler, so a clean exit is
  indistinguishable from a crash to the ring (both vanish and are
  detected dead), which is exactly the equivalence the design wants.
- **Teardown**: a `Drop` impl kills every spawned child, so a panicking
  test never leaks node processes.

## 3. Scenario suite (`integration_cluster.rs`)

Each scenario stands up a fresh cluster, drives it over real sockets,
and tears it down. Covering the design doc's Testing Strategy plus the
C-1/C-2 additions:

1. **Routing / redirect** — two compute nodes; a client op for a datum
   native to the *other* node is redirected and served correctly over
   real sockets.
2. **Cross-node wound-wait** — an op spanning datums native to two
   different compute nodes completes under contention (foreign pull +
   release across the real peer link).
3. **Compute kill → lazy reclaim** — kill a compute node; the ring
   converges under the fast timeouts, its keys re-home, and client ops
   keep succeeding (reclaim + cheap eviction across a real membership
   change).
4. **Storage kill → point-of-use halt** — an N=1 corpus across two
   storage nodes; kill one; a client op touching a now-lost shard comes
   back with the "cluster halted" reason.
5. **Live migration add** — two storage nodes + a written corpus; call
   `seisin_migrate::migrate` to admit a third; every datum still reads
   back and placement follows the new ring.
6. **Replication failover + recover** — a `replicated(2)` type; kill one
   replica; reads still succeed with no halt; `seisin-migrate recover`
   restores the replication factor; the corpus is fully readable.

All six drive the `cluster_test_node` binary's byte ops (§2); the
replication scenario uses the `put2`/`get2` (factor-2) ops.

## 4. Gating and determinism

With the fast timeouts each scenario runs in ≈1–3 s. They run in the
default `cargo test`; a note in the harness records that if total suite
time becomes a problem they can be `#[ignore]`-tagged for a dedicated
slow CI job. The startup barrier (poll-until-accept) removes spawn
races; real ports/processes carry more nondeterminism than in-process
tests, so the suite is stress-run 10×. The harness spawns the real
binary and changes no `worker.rs` code, so the standing 20× wound-wait
loop is not triggered by this sub-project.

## 5. Testing

The scenarios are the tests. Additionally:
- Unit: config parsing of the new optional timeout fields (present →
  used; absent → production defaults).
- Stress: the cluster suite 10×; full workspace gates
  (`cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D
  warnings`, `cargo test --workspace`).
