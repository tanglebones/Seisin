//! End-to-end storage migration: live add, planned remove, reweight,
//! concurrent writes (dirty tail), halt+resume, and impostor refusal —
//! driving the real `seisin_migrate` driver against in-process compute
//! and storage nodes.
//!
//! Membership events (a drained/dead node's Leave) are injected through
//! the real `apply_ready_mutations` path rather than by waiting on the
//! gossip failure detector — the detector→halt timing is covered by
//! `integration_gossip_failure_detection` / `integration_storage_halt`;
//! here the concern is the migration mechanism and the halt/resume
//! control flow, which want a deterministic trigger.

use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_gossip::membership::{Incarnation, MemberRole, MemberStatus, MemberUpdate};
use seisin_gossip::sequencer::RingMutation;
use seisin_node::gossip_state::{apply_ready_mutations, ClusterState, GossipState};
use seisin_node::halt::HaltState;
use seisin_node::heartbeat::Heartbeat;
use seisin_node::index_handler::IndexKindRegistry;
use seisin_node::pool::WorkerPool;
use seisin_node::remote_store::RemoteStore;
use seisin_node::server::serve;
use seisin_node::store_server::{serve_store, StoreNode};
use seisin_node::transfer::TransferManager;
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{Request, Response, StorageMember};
use seisin_ring::ring::Ring;
use seisin_storage::datum_log::DatumLog;

const COMPUTE_ID: NodeId = NodeId(1);

/// A live in-process storage node.
struct Storage {
  node_id: NodeId,
  weight: u32,
  store_addr: String,
  log_id: DatumId,
  _dir: tempfile::TempDir,
  _node: Arc<StoreNode>,
}

fn start_storage(node_id: NodeId, weight: u32) -> Storage {
  let dir = tempfile::tempdir().unwrap();
  let log = Arc::new(Mutex::new(
    DatumLog::open(&dir.path().join("datum_log.dlog")).unwrap(),
  ));
  let log_id = DatumId::from_bytes(log.lock().unwrap().log_id());
  let node = Arc::new(StoreNode {
    log,
    node_id,
    heartbeat: Arc::new(Heartbeat::new()),
    // No gossip loop drives the heartbeat here, so a huge threshold
    // keeps the node serving for the test's duration.
    self_halt_threshold: Duration::from_secs(3600),
    transfers: Arc::new(TransferManager::default()),
    data_dir: dir.path().to_path_buf(),
    collections: Mutex::new(HashMap::new()),
  });
  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let store_addr = listener.local_addr().unwrap().to_string();
  {
    let node = Arc::clone(&node);
    thread::spawn(move || serve_store(listener, node));
  }
  Storage {
    node_id,
    weight,
    store_addr,
    log_id,
    _dir: dir,
    _node: node,
  }
}

/// The compute node plus the handles a test needs to drive membership
/// events and inspect state.
struct Cluster {
  compute_addr: String,
  gossip: Arc<GossipState>,
  cluster: Arc<ClusterState>,
  pool: Arc<WorkerPool>,
  storages: Vec<Storage>,
}

/// Builds a cluster: every node in `storages` is reachable (address +
/// identity known to the compute), but only those whose id is in
/// `initial_ring` start in the storage ring.
fn build(storages: Vec<Storage>, initial_ring: &[NodeId]) -> Cluster {
  let compute_ring = Arc::new(RwLock::new(Ring::from_members(&[(COMPUTE_ID, 2)])));
  let ring_members: Vec<(NodeId, u32)> = storages
    .iter()
    .filter(|s| initial_ring.contains(&s.node_id))
    .map(|s| (s.node_id, s.weight))
    .collect();
  let storage_ring = Arc::new(RwLock::new(Ring::from_members(&ring_members)));
  let store_addresses = Arc::new(RwLock::new(
    storages
      .iter()
      .map(|s| (s.node_id, s.store_addr.clone()))
      .collect::<HashMap<_, _>>(),
  ));
  let identity_book = Arc::new(RwLock::new(
    storages
      .iter()
      .map(|s| (s.node_id, s.log_id))
      .collect::<HashMap<_, _>>(),
  ));
  let halt = Arc::new(HaltState::new());
  let storage_alive: std::collections::HashSet<NodeId> =
    storages.iter().map(|s| s.node_id).collect();
  let cluster = Arc::new(ClusterState {
    compute_ring: Arc::clone(&compute_ring),
    storage_ring: Arc::clone(&storage_ring),
    store_addresses: Arc::clone(&store_addresses),
    identity_book,
    storage_alive: Arc::new(RwLock::new(storage_alive)),
    storage_stale: Arc::new(RwLock::new(std::collections::HashSet::new())),
    halt: Arc::clone(&halt),
  });

  // Gossip member table — roles for apply_ready_mutations routing.
  let gossip = Arc::new(GossipState::new());
  {
    let mut table = gossip.member_table.lock().unwrap();
    for s in &storages {
      table.merge_update(MemberUpdate {
        node_id: s.node_id,
        incarnation: Incarnation(0),
        status: MemberStatus::Alive,
        client_address: String::new(),
        gossip_address: String::new(),
        thread_count: 1,
        role: MemberRole::Storage,
        capacity_weight: s.weight,
        store_address: s.store_addr.clone(),
        log_id: s.log_id.as_bytes(),
      });
    }
  }

  let mut ops = OpRegistry::new();
  ops.register(
    "put",
    Box::new(|ctx, ids, payload| {
      ctx.put(ids[0], payload.to_vec());
      vec![]
    }),
  );
  ops.register(
    "get",
    Box::new(|ctx, ids, _payload| ctx.get(ids[0]).unwrap_or_default()),
  );
  let store = Arc::new(RemoteStore::new(Arc::clone(&cluster)));
  let peer_link = TcpListener::bind("127.0.0.1:0").unwrap();
  let pool = Arc::new(WorkerPool::spawn(
    store,
    2,
    Arc::new(ops),
    Arc::clone(&compute_ring),
    COMPUTE_ID,
    peer_link,
    Arc::new(HashMap::new()),
    Arc::new(IndexKindRegistry::new()),
  ));

  let client_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let compute_addr = client_listener.local_addr().unwrap().to_string();
  {
    let cluster = Arc::clone(&cluster);
    let pool = Arc::clone(&pool);
    let address_book = Arc::new(HashMap::from([(COMPUTE_ID, compute_addr.clone())]));
    thread::spawn(move || serve(client_listener, COMPUTE_ID, cluster, address_book, pool));
  }

  Cluster {
    compute_addr,
    gossip,
    cluster,
    pool,
    storages,
  }
}

impl Cluster {
  fn proposed(&self, ids_and_weights: &[(NodeId, u32)]) -> Vec<StorageMember> {
    ids_and_weights
      .iter()
      .map(|(id, weight)| {
        let s = self.storages.iter().find(|s| s.node_id == *id).unwrap();
        StorageMember {
          node_id: *id,
          weight: *weight,
          store_address: s.store_addr.clone(),
          // Resolved by the driver via Identify.
          log_id: DatumId::from_bytes([0u8; 16]),
        }
      })
      .collect()
  }

  fn put(&self, id: DatumId, value: &[u8]) {
    let resp = seisin_client::call(
      &self.compute_addr,
      Request::Op {
        op_id: DatumId::new(),
        op_name: "put".to_string(),
        datum_ids: vec![id],
        payload: value.to_vec(),
      },
    )
    .unwrap();
    assert_eq!(resp, Response::OpResult { payload: vec![] });
  }

  /// Reads `id` from storage (evicting the compute cache first so the
  /// value must come from whatever storage node currently owns it).
  fn get_from_storage(&self, id: DatumId) -> Vec<u8> {
    self.pool.evict_non_native(Arc::new(|_| false));
    match seisin_client::call(
      &self.compute_addr,
      Request::Op {
        op_id: DatumId::new(),
        op_name: "get".to_string(),
        datum_ids: vec![id],
        payload: vec![],
      },
    )
    .unwrap()
    {
      Response::OpResult { payload } => payload,
      other => panic!("expected OpResult, got {other:?}"),
    }
  }

  /// Injects a confirmed-dead Leave for `node_id` through the real
  /// mutation-apply path (what the gossip server would do on a detector
  /// verdict): the node drops from the alive serving set and is marked
  /// stale. The coordinated halt is point-of-use, so this does not halt
  /// on its own — a client op touching a now-lost shard does (see
  /// `wait_until_halted`).
  fn inject_leave(&self, node_id: NodeId) {
    self
      .gossip
      .record_mutation(1, RingMutation::Leave { node_id });
    apply_ready_mutations(&self.gossip, &self.cluster, COMPUTE_ID, &self.pool);
  }

  /// Fires reads of `id` until the cluster is observed halted (the first
  /// op touching a fully-lost shard trips the point-of-use halt and
  /// fail-stops its worker; subsequent ops are cleanly gated). Panics if
  /// no halt appears.
  fn wait_until_halted(&self, id: DatumId) {
    for _ in 0..50 {
      if let Ok(Response::OpError { message }) = seisin_client::call(
        &self.compute_addr,
        Request::Op {
          op_id: DatumId::new(),
          op_name: "get".to_string(),
          datum_ids: vec![id],
          payload: vec![],
        },
      ) {
        if message.contains("cluster halted") {
          return;
        }
      }
      thread::sleep(Duration::from_millis(20));
    }
    panic!("cluster did not halt after storage loss");
  }
}

fn corpus(n: usize) -> Vec<DatumId> {
  (0..n).map(|_| DatumId::new()).collect()
}

#[test]
fn live_add_moves_a_subset_and_keeps_every_datum_readable() {
  let a = start_storage(NodeId(10), 1);
  let b = start_storage(NodeId(20), 1);
  let c = start_storage(NodeId(30), 1);
  let c_id = c.node_id;
  let cluster = build(vec![a, b, c], &[NodeId(10), NodeId(20)]); // c available, not in ring

  let ids = corpus(60);
  for (i, id) in ids.iter().enumerate() {
    cluster.put(*id, format!("v{i}").as_bytes());
  }

  let report = seisin_migrate::migrate(
    std::slice::from_ref(&cluster.compute_addr),
    &cluster.proposed(&[(NodeId(10), 1), (NodeId(20), 1), (NodeId(30), 1)]),
    true,
  )
  .unwrap();
  assert!(report.applied);
  // Some datums moved onto the newly-admitted node.
  assert!(report.total_moves > 0);

  // Every datum still reads back its value...
  for (i, id) in ids.iter().enumerate() {
    assert_eq!(cluster.get_from_storage(*id), format!("v{i}").into_bytes());
  }
  // ...and at least one now lives on the new node (placement followed
  // the new ring).
  let new_ring = Ring::from_members(&[(NodeId(10), 1), (NodeId(20), 1), (NodeId(30), 1)]);
  assert!(ids.iter().any(|id| new_ring.native(*id).0 == c_id));
}

#[test]
fn planned_remove_drains_a_node_and_its_later_leave_does_not_halt() {
  let a = start_storage(NodeId(10), 1);
  let b = start_storage(NodeId(20), 1);
  let b_id = b.node_id;
  let cluster = build(vec![a, b], &[NodeId(10), NodeId(20)]);

  let ids = corpus(60);
  for (i, id) in ids.iter().enumerate() {
    cluster.put(*id, format!("v{i}").as_bytes());
  }

  // Drain node 20 out of the ring.
  seisin_migrate::migrate(
    std::slice::from_ref(&cluster.compute_addr),
    &cluster.proposed(&[(NodeId(10), 1)]),
    true,
  )
  .unwrap();

  // Its subsequent Leave (operator shuts the drained node down) must NOT
  // halt — it is no longer in the ring.
  cluster.inject_leave(b_id);
  assert!(
    !cluster.cluster.halt.is_halted(),
    "a drained node's Leave should not halt the cluster"
  );

  // All data readable from the survivor.
  for (i, id) in ids.iter().enumerate() {
    assert_eq!(cluster.get_from_storage(*id), format!("v{i}").into_bytes());
  }
}

#[test]
fn reweight_moves_data_and_keeps_the_corpus_readable() {
  let a = start_storage(NodeId(10), 1);
  let b = start_storage(NodeId(20), 1);
  let cluster = build(vec![a, b], &[NodeId(10), NodeId(20)]);

  let ids = corpus(80);
  for (i, id) in ids.iter().enumerate() {
    cluster.put(*id, format!("v{i}").as_bytes());
  }

  let report = seisin_migrate::migrate(
    std::slice::from_ref(&cluster.compute_addr),
    &cluster.proposed(&[(NodeId(10), 1), (NodeId(20), 3)]), // node 20 gets heavier
    true,
  )
  .unwrap();
  assert!(
    report.applied && report.total_moves > 0,
    "reweight moved nothing"
  );

  for (i, id) in ids.iter().enumerate() {
    assert_eq!(cluster.get_from_storage(*id), format!("v{i}").into_bytes());
  }
}

#[test]
fn concurrent_writes_during_migration_are_all_present_after_the_flip() {
  let a = start_storage(NodeId(10), 1);
  let b = start_storage(NodeId(20), 1);
  let c = start_storage(NodeId(30), 1);
  let cluster = Arc::new(build(vec![a, b, c], &[NodeId(10), NodeId(20)]));

  let ids = corpus(60);
  for id in &ids {
    cluster.put(*id, b"v0");
  }

  // A background writer updates every datum to v1 while the migration
  // runs, retrying past the pause window.
  let writer = {
    let cluster = Arc::clone(&cluster);
    let ids = ids.clone();
    thread::spawn(move || {
      for id in ids {
        put_with_retry(&cluster.compute_addr, id, b"v1");
      }
    })
  };

  seisin_migrate::migrate(
    std::slice::from_ref(&cluster.compute_addr),
    &cluster.proposed(&[(NodeId(10), 1), (NodeId(20), 1), (NodeId(30), 1)]),
    true,
  )
  .unwrap();
  writer.join().unwrap();

  // Every datum reads back the latest value — copied, tailed as dirty,
  // or re-applied post-resume, depending on when its write landed.
  for id in &ids {
    assert_eq!(cluster.get_from_storage(*id), b"v1".to_vec());
  }
}

/// Writes `value` to `id`, retrying while the cluster is paused for a
/// migration (a distinct retryable error). Deadline-based (not a fixed
/// iteration count) so a pause window stretched by parallel-test load
/// doesn't spuriously exhaust the budget — this drives real TCP, so wall
/// time is the right bound here.
fn put_with_retry(compute_addr: &str, id: DatumId, value: &[u8]) {
  let deadline = std::time::Instant::now() + Duration::from_secs(60);
  while std::time::Instant::now() < deadline {
    let resp = seisin_client::call(
      compute_addr,
      Request::Op {
        op_id: DatumId::new(),
        op_name: "put".to_string(),
        datum_ids: vec![id],
        payload: value.to_vec(),
      },
    )
    .unwrap();
    match resp {
      Response::OpResult { .. } => return,
      Response::OpError { message } if message.contains("cluster paused") => {
        thread::sleep(Duration::from_millis(5));
      }
      other => panic!("unexpected write response: {other:?}"),
    }
  }
  panic!("write never succeeded within the deadline");
}

#[test]
fn halt_then_resume_verifies_identity_and_restores_service() {
  let a = start_storage(NodeId(10), 1);
  let cluster = build(vec![a], &[NodeId(10)]);

  let id = DatumId::new();
  cluster.put(id, b"durable");

  // The storage node is confirmed dead (dropped from serving); a client
  // op touching its now-lost single-copy shard trips the point-of-use
  // halt.
  cluster.inject_leave(NodeId(10));
  cluster.wait_until_halted(id);

  // The node is back (same log id — the identity book still matches).
  // resume verifies identity, clears the halt, and re-admits the node.
  seisin_migrate::resume(std::slice::from_ref(&cluster.compute_addr)).unwrap();
  assert!(!cluster.cluster.halt.is_halted());

  // Every previously-acked write reads back.
  assert_eq!(cluster.get_from_storage(id), b"durable".to_vec());
}

#[test]
fn resume_refuses_an_impostor_and_the_halt_stands() {
  let a = start_storage(NodeId(10), 1);
  let cluster = build(vec![a], &[NodeId(10)]);

  let id = DatumId::new();
  cluster.put(id, b"x");
  cluster.inject_leave(NodeId(10));
  cluster.wait_until_halted(id);

  // Simulate an impostor: the identity book expects a different log id
  // than the node now reports (a blank/wrong disk at the same address).
  cluster
    .cluster
    .identity_book
    .write()
    .unwrap()
    .insert(NodeId(10), DatumId::from_bytes([0xAB; 16]));

  let err = seisin_migrate::resume(std::slice::from_ref(&cluster.compute_addr)).unwrap_err();
  assert!(
    err.to_string().contains("impostor"),
    "expected an impostor refusal, got: {err}"
  );
  // The halt stands.
  assert!(cluster.cluster.halt.is_halted());
}
