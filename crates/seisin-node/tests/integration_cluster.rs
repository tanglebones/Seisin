//! Real-process, real-socket cluster tests (Sub-project 5): a
//! `ClusterHarness` generates a RON config per node, spawns real
//! `cluster_test_node` processes over localhost (storage-first), drives
//! them via `seisin-client` and the `seisin_migrate` library, and kills
//! them (SIGKILL) for crash scenarios — reaping every child on drop.
//!
//! Timeouts are turned down via config so crash detection converges in
//! ~1s. Killing a process is the crash case; the node has no
//! graceful-leave handler, so a clean exit is indistinguishable from a
//! crash to the ring (both vanish and are detected dead).

use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

/// Cluster scenarios each spawn several node processes; running them in
/// parallel (cargo's default) starves gossip convergence and collides
/// ports. This global lock serializes them — one live cluster at a time.
static CLUSTER_LOCK: Mutex<()> = Mutex::new(());

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_protocol::{Request, Response, StorageMember};
use seisin_ring::ring::Ring;

const PROBE_INTERVAL_MS: u64 = 20;
const PROBE_TIMEOUT_MS: u64 = 20;
const SUSPICION_TIMEOUT_MS: u64 = 40;
// Generous vs the probe cadence: storage self-halt is not what these
// scenarios exercise, and a tight threshold would spuriously fire before
// a real-process gossip probe reaches the storage node.
const SELF_HALT_MS: u64 = 2000;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
  Compute,
  Storage,
}

struct NodeSpec {
  id: u64,
  role: Role,
  thread_count: u32,
  weight: u32,
}

impl NodeSpec {
  fn compute(id: u64, thread_count: u32) -> Self {
    Self {
      id,
      role: Role::Compute,
      thread_count,
      weight: 0,
    }
  }
  fn storage(id: u64, weight: u32) -> Self {
    Self {
      id,
      role: Role::Storage,
      thread_count: 1,
      weight,
    }
  }
}

/// A running node's addresses.
struct NodeAddrs {
  id: u64,
  role: Role,
  client: String,
  gossip: String,
  peer_link: String,
  store: String,
}

struct ClusterHarness {
  _dir: tempfile::TempDir,
  nodes: Vec<NodeAddrs>,
  children: Vec<(u64, Child)>,
  _guard: MutexGuard<'static, ()>,
}

/// Binds an ephemeral localhost port, records it, and drops the listener
/// (the standard reserve-a-port pattern used across the test suite).
fn free_port() -> u16 {
  TcpListener::bind("127.0.0.1:0")
    .unwrap()
    .local_addr()
    .unwrap()
    .port()
}

fn addr() -> String {
  format!("127.0.0.1:{}", free_port())
}

impl ClusterHarness {
  fn start(specs: &[NodeSpec]) -> Self {
    // Held for the cluster's lifetime — only one cluster runs at a time
    // (recover from a poisoned lock so one panicking scenario doesn't
    // wedge the rest).
    let guard = CLUSTER_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let nodes: Vec<NodeAddrs> = specs
      .iter()
      .map(|s| NodeAddrs {
        id: s.id,
        role: s.role,
        client: addr(),
        gossip: addr(),
        peer_link: addr(),
        store: addr(),
      })
      .collect();

    // One shared members list (RON), reused in every node's config.
    let members: String = specs
      .iter()
      .zip(&nodes)
      .map(|(spec, n)| match spec.role {
        Role::Compute => format!(
          "(node_id: {}, address: \"{}\", gossip_address: \"{}\", peer_link_address: \"{}\", thread_count: {}),",
          n.id, n.client, n.gossip, n.peer_link, spec.thread_count
        ),
        Role::Storage => format!(
          "(node_id: {}, address: \"{}\", gossip_address: \"{}\", peer_link_address: \"{}\", thread_count: 1, role: Storage, store_address: Some(\"{}\"), capacity_weight: Some({})),",
          n.id, n.client, n.gossip, n.peer_link, n.store, spec.weight
        ),
      })
      .collect();

    let mut children = Vec::new();
    // Storage nodes first (the deploy order), then compute.
    let order = specs
      .iter()
      .filter(|s| s.role == Role::Storage)
      .chain(specs.iter().filter(|s| s.role == Role::Compute));
    for spec in order {
      let data_dir = dir.path().join(format!("node{}", spec.id));
      let config = format!(
        "(\n  self_node_id: {},\n  members: [{}],\n  data_dir: {:?},\n  probe_interval_millis: Some({}),\n  probe_timeout_millis: Some({}),\n  suspicion_timeout_millis: Some({}),\n  self_halt_threshold_millis: Some({}),\n)",
        spec.id,
        members,
        data_dir.to_str().unwrap(),
        PROBE_INTERVAL_MS,
        PROBE_TIMEOUT_MS,
        SUSPICION_TIMEOUT_MS,
        SELF_HALT_MS,
      );
      let config_path = dir.path().join(format!("node{}.ron", spec.id));
      std::fs::write(&config_path, config).unwrap();
      let child = Command::new(env!("CARGO_BIN_EXE_cluster_test_node"))
        .env("SEISIN_NODE_CONFIG", &config_path)
        .spawn()
        .expect("failed to spawn cluster_test_node");
      children.push((spec.id, child));
    }

    let harness = ClusterHarness {
      _dir: dir,
      nodes,
      children,
      _guard: guard,
    };
    harness.await_ready();
    harness
  }

  /// Polls each node's primary listener (client for compute, store for
  /// storage) until it accepts, so a scenario never races the spawn.
  fn await_ready(&self) {
    let deadline = Instant::now() + Duration::from_secs(10);
    for n in &self.nodes {
      let target = match n.role {
        Role::Compute => &n.client,
        Role::Storage => &n.store,
      };
      loop {
        if TcpStream::connect(target).is_ok() {
          break;
        }
        assert!(
          Instant::now() < deadline,
          "node {} never came up on {target}",
          n.id
        );
        std::thread::sleep(Duration::from_millis(20));
      }
    }
  }

  fn node(&self, id: u64) -> &NodeAddrs {
    self.nodes.iter().find(|n| n.id == id).unwrap()
  }

  fn compute_ids(&self) -> Vec<u64> {
    self
      .nodes
      .iter()
      .filter(|n| n.role == Role::Compute)
      .map(|n| n.id)
      .collect()
  }

  fn compute_addr(&self, id: u64) -> String {
    self.node(id).client.clone()
  }

  #[allow(dead_code)] // used by later scenarios (Tasks 3-4)
  fn store_addr(&self, id: u64) -> String {
    self.node(id).store.clone()
  }

  /// A client op against compute node `id`, returning its `Response`.
  fn op(&self, id: u64, name: &str, datum_ids: Vec<DatumId>, payload: &[u8]) -> Response {
    self.try_op(id, name, datum_ids, payload).unwrap()
  }

  /// Like `op`, but surfaces the transport error (a redirect to a
  /// just-killed node refuses the connection) instead of panicking — so
  /// crash scenarios can poll through the convergence window.
  fn try_op(
    &self,
    id: u64,
    name: &str,
    datum_ids: Vec<DatumId>,
    payload: &[u8],
  ) -> anyhow::Result<Response> {
    seisin_client::call(
      &self.compute_addr(id),
      Request::Op {
        op_id: DatumId::new(),
        op_name: name.to_string(),
        datum_ids,
        payload: payload.to_vec(),
      },
    )
  }

  #[allow(dead_code)] // used by the crash scenarios (Task 3)
  fn kill(&mut self, id: u64) {
    if let Some((_, child)) = self.children.iter_mut().find(|(cid, _)| *cid == id) {
      let _ = child.kill();
      let _ = child.wait();
    }
  }
}

impl Drop for ClusterHarness {
  fn drop(&mut self) {
    for (_, child) in &mut self.children {
      let _ = child.kill();
      let _ = child.wait();
    }
  }
}

/// Convenience: the OpResult payload, panicking on any error response.
fn ok(resp: Response) -> Vec<u8> {
  match resp {
    Response::OpResult { payload } => payload,
    other => panic!("expected OpResult, got {other:?}"),
  }
}

#[test]
fn a_client_op_is_served_across_two_compute_nodes() {
  // Two compute nodes over real sockets; a put on one node and a get on
  // the *other* both resolve to the datum's owner via redirect.
  let h = ClusterHarness::start(&[
    NodeSpec::compute(1, 2),
    NodeSpec::compute(2, 2),
    NodeSpec::storage(10, 1),
  ]);
  // Write and read many ids through whichever node the client happens to
  // hit — every one must round-trip regardless of which node owns it.
  for i in 0..20u32 {
    let id = DatumId::new();
    let via_a = 1;
    let via_b = 2;
    assert!(ok(h.op(via_a, "put1", vec![id], format!("v{i}").as_bytes())).is_empty());
    // Read back through the other compute node — the redirect (or local
    // service) must return the same value over real sockets.
    assert_eq!(
      ok(h.op(via_b, "get1", vec![id], &[])),
      format!("v{i}").into_bytes()
    );
  }
  assert_eq!(h.compute_ids().len(), 2);
}

#[test]
fn a_killed_compute_node_is_reclaimed_and_ops_keep_succeeding() {
  // Two compute nodes over one storage node; kill a compute node and the
  // ring converges so the survivor owns every key and serves it (reload
  // from storage), no longer redirecting to the dead node.
  let mut h = ClusterHarness::start(&[
    NodeSpec::compute(1, 2),
    NodeSpec::compute(2, 2),
    NodeSpec::storage(10, 1),
  ]);
  // Some keys written while both nodes are up.
  for i in 0..10u32 {
    h.op(1, "put1", vec![DatumId::new()], format!("v{i}").as_bytes());
  }

  h.kill(2);

  // A round-trip that succeeds without a redirect to the dead node.
  let round_trips = |id: DatumId| -> bool {
    matches!(
      h.try_op(1, "put1", vec![id], b"v"),
      Ok(Response::OpResult { .. })
    ) && matches!(
      h.try_op(1, "get1", vec![id], b""),
      Ok(Response::OpResult { payload }) if payload == b"v".to_vec()
    )
  };

  // Poll until the survivor's detector has fully converged node 2 dead
  // (ring shrunk to just node 1): a *fixed batch* of ids must all
  // round-trip — a single fresh id could merely happen to be node-1-
  // native while node-2-native ids still redirect to the dead node.
  let batch: Vec<DatumId> = (0..15).map(|_| DatumId::new()).collect();
  let deadline = Instant::now() + Duration::from_secs(10);
  loop {
    if batch.iter().all(|id| round_trips(*id)) {
      break;
    }
    assert!(
      Instant::now() < deadline,
      "cluster never reclaimed after the compute-node kill"
    );
    thread::sleep(Duration::from_millis(50));
  }

  // Reclaimed: every op now serves on the survivor.
  for i in 0..20u32 {
    let id = DatumId::new();
    assert!(ok(h.op(1, "put1", vec![id], format!("k{i}").as_bytes())).is_empty());
    assert_eq!(
      ok(h.op(1, "get1", vec![id], &[])),
      format!("k{i}").into_bytes()
    );
  }
}

#[test]
fn cross_node_ops_complete_under_contention() {
  // A two-datum op whose ids may be native to different compute nodes
  // completes over the real peer link (foreign pull + release). Random
  // pairs cover both the same-node and cross-node dispatch paths.
  let h = ClusterHarness::start(&[
    NodeSpec::compute(1, 2),
    NodeSpec::compute(2, 2),
    NodeSpec::storage(10, 1),
  ]);
  for _ in 0..30 {
    let a = DatumId::new();
    let b = DatumId::new();
    // Route via either node; the op touches both ids and must complete.
    assert_eq!(
      h.op(1, "touch_both", vec![a, b], b"x"),
      Response::OpResult { payload: vec![] }
    );
    assert_eq!(ok(h.op(2, "get1", vec![a], &[])), b"x".to_vec());
    assert_eq!(ok(h.op(2, "get1", vec![b], &[])), b"x".to_vec());
  }
}

#[test]
fn a_killed_storage_node_halts_client_traffic() {
  // N=1 corpus across two storage nodes; kill one, and an op touching a
  // now-lost shard trips the point-of-use cluster halt over real sockets.
  let mut h = ClusterHarness::start(&[
    NodeSpec::compute(1, 2),
    NodeSpec::storage(10, 1),
    NodeSpec::storage(20, 1),
  ]);
  for i in 0..20u32 {
    h.op(1, "put1", vec![DatumId::new()], format!("v{i}").as_bytes());
  }

  h.kill(20);

  // Some op touching a datum on the killed node halts the whole cluster;
  // poll fresh reads until the halt reason appears.
  let deadline = Instant::now() + Duration::from_secs(8);
  let mut halted = false;
  while Instant::now() < deadline {
    if let Ok(Response::OpError { message }) = h.try_op(1, "get1", vec![DatumId::new()], b"") {
      if message.contains("cluster halted") {
        halted = true;
        break;
      }
    }
    thread::sleep(Duration::from_millis(20));
  }
  assert!(halted, "killing a storage node did not halt client traffic");
}

impl ClusterHarness {
  /// The proposed storage ring as `StorageMember`s (log ids zero — the
  /// driver resolves them via `Identify`), addresses from the harness.
  fn proposed(&self, ids_weights: &[(u64, u32)]) -> Vec<StorageMember> {
    ids_weights
      .iter()
      .map(|(id, weight)| StorageMember {
        node_id: NodeId(*id),
        weight: *weight,
        store_address: self.store_addr(*id),
        log_id: DatumId::from_bytes([0u8; 16]),
      })
      .collect()
  }
}

#[test]
fn a_live_reweight_moves_data_and_keeps_the_corpus_readable() {
  // Three storage nodes; write a corpus, then run the real seisin_migrate
  // driver (library) to reweight one node heavier — the copy -> pause ->
  // flip -> resume path runs over real sockets, and every datum stays
  // readable.
  let h = ClusterHarness::start(&[
    NodeSpec::compute(1, 2),
    NodeSpec::storage(10, 1),
    NodeSpec::storage(20, 1),
    NodeSpec::storage(30, 1),
  ]);
  let ids: Vec<DatumId> = (0..40).map(|_| DatumId::new()).collect();
  for (i, id) in ids.iter().enumerate() {
    h.op(1, "put1", vec![*id], format!("v{i}").as_bytes());
  }

  let report = seisin_migrate::migrate(
    &[h.compute_addr(1)],
    &h.proposed(&[(10, 1), (20, 1), (30, 4)]),
    true,
  )
  .unwrap();
  assert!(
    report.applied && report.total_moves > 0,
    "reweight moved nothing"
  );

  // Every datum still reads back...
  for (i, id) in ids.iter().enumerate() {
    assert_eq!(
      ok(h.op(1, "get1", vec![*id], &[])),
      format!("v{i}").into_bytes()
    );
  }
  // ...and placement followed the new ring (node 30 owns a share now).
  let new_ring = Ring::from_members(&[(NodeId(10), 1), (NodeId(20), 1), (NodeId(30), 4)]);
  assert!(ids.iter().any(|id| new_ring.native(*id).0 == NodeId(30)));
}

#[test]
fn replication_survives_a_replica_kill_and_recover_restores_it() {
  // Three storage nodes, a replication-factor-2 corpus; kill one storage
  // node and reads still succeed (failover, no halt); recover drops the
  // dead node and restores replication onto the survivors.
  let mut h = ClusterHarness::start(&[
    NodeSpec::compute(1, 2),
    NodeSpec::storage(10, 1),
    NodeSpec::storage(20, 1),
    NodeSpec::storage(30, 1),
  ]);
  let ids: Vec<DatumId> = (0..30).map(|_| DatumId::new()).collect();
  for (i, id) in ids.iter().enumerate() {
    h.op(1, "put2", vec![*id], format!("v{i}").as_bytes());
  }

  h.kill(30);

  // Reads still succeed by failing over to the surviving replica — poll
  // through the detection window (a datum whose primary was node 30
  // fails over once the compute marks it stale).
  let read_all = |h: &ClusterHarness| -> bool {
    ids.iter().enumerate().all(|(i, id)| {
      matches!(
        h.try_op(1, "get2", vec![*id], b""),
        Ok(Response::OpResult { payload }) if payload == format!("v{i}").into_bytes()
      )
    })
  };
  let deadline = Instant::now() + Duration::from_secs(8);
  while !read_all(&h) {
    assert!(
      Instant::now() < deadline,
      "replicated reads did not fail over after the replica kill"
    );
    thread::sleep(Duration::from_millis(50));
  }
  assert!(!matches!(
    h.try_op(1, "get2", vec![DatumId::new()], b""),
    Ok(Response::OpError { message }) if message.contains("cluster halted")
  ));

  // Recover: drop the dead node and restore replication onto 10 & 20.
  let report = seisin_migrate::recover(&[h.compute_addr(1)], true).unwrap();
  assert!(report.applied);

  // The corpus is fully readable after recovery.
  for (i, id) in ids.iter().enumerate() {
    assert_eq!(
      ok(h.op(1, "get2", vec![*id], &[])),
      format!("v{i}").into_bytes()
    );
  }
}
