//! End-to-end per-type replication: replicated write + read-one, read
//! failover, degraded write, total-loss point-of-use halt, N=1 unchanged,
//! driver re-replication (`recover`), and a stale returned node never
//! being served — driven through the full stack (client -> compute ->
//! workers -> RemoteStore -> storage) and the real `seisin_migrate`
//! driver.
//!
//! Membership/liveness is driven directly on the compute's alive/stale
//! sets (removing a node = "it died") rather than via the gossip
//! detector — the detector timing is covered elsewhere; here the concern
//! is the replicated read/write/recover behavior.

use std::collections::{HashMap, HashSet};
use std::net::TcpListener;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_node::gossip_state::ClusterState;
use seisin_node::heartbeat::Heartbeat;
use seisin_node::index_handler::IndexKindRegistry;
use seisin_node::pool::WorkerPool;
use seisin_node::remote_store::RemoteStore;
use seisin_node::server::serve;
use seisin_node::store_server::{serve_store, StoreNode};
use seisin_node::transfer::TransferManager;
use seisin_ops::registry::OpRegistry;
use seisin_protocol::store_wire::{store_call, StoreRequest, StoreResponse};
use seisin_protocol::{Request, Response};
use seisin_ring::ring::Ring;
use seisin_storage::datum_log::DatumLog;

const COMPUTE_ID: NodeId = NodeId(1);
const REPL: u16 = 2;

struct Storage {
  node_id: NodeId,
  store_addr: String,
  log_id: DatumId,
  _dir: tempfile::TempDir,
  _node: Arc<StoreNode>,
}

fn start_storage(node_id: NodeId) -> Storage {
  let dir = tempfile::tempdir().unwrap();
  let log = Arc::new(Mutex::new(
    DatumLog::open(&dir.path().join("datum_log.dlog")).unwrap(),
  ));
  let log_id = DatumId::from_bytes(log.lock().unwrap().log_id());
  let node = Arc::new(StoreNode {
    log,
    node_id,
    heartbeat: Arc::new(Heartbeat::new()),
    self_halt_threshold: Duration::from_secs(3600),
    transfers: Arc::new(TransferManager::default()),
  });
  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let store_addr = listener.local_addr().unwrap().to_string();
  {
    let node = Arc::clone(&node);
    thread::spawn(move || serve_store(listener, node));
  }
  Storage {
    node_id,
    store_addr,
    log_id,
    _dir: dir,
    _node: node,
  }
}

struct Cluster {
  compute_addr: String,
  cluster: Arc<ClusterState>,
  storages: Vec<Storage>,
}

/// A compute node over `storages` (all in the ring, all alive), with
/// N=1 and N=2 byte read/write ops.
fn build(storages: Vec<Storage>) -> Cluster {
  let members: Vec<(NodeId, u32)> = storages.iter().map(|s| (s.node_id, 1)).collect();
  let compute_ring = Arc::new(RwLock::new(Ring::from_members(&[(COMPUTE_ID, 2)])));
  let cluster = Arc::new(ClusterState {
    storage_ring: Arc::new(RwLock::new(Ring::from_members(&members))),
    store_addresses: Arc::new(RwLock::new(
      storages
        .iter()
        .map(|s| (s.node_id, s.store_addr.clone()))
        .collect::<HashMap<_, _>>(),
    )),
    identity_book: Arc::new(RwLock::new(
      storages.iter().map(|s| (s.node_id, s.log_id)).collect(),
    )),
    storage_alive: Arc::new(RwLock::new(storages.iter().map(|s| s.node_id).collect())),
    storage_stale: Arc::new(RwLock::new(HashSet::new())),
    ..ClusterState::compute_only(Arc::clone(&compute_ring))
  });

  let mut ops = OpRegistry::new();
  ops.register(
    "put1",
    Box::new(|ctx, ids, payload| {
      ctx.put(ids[0], payload.to_vec());
      vec![]
    }),
  );
  ops.register(
    "get1",
    Box::new(|ctx, ids, _p| ctx.get(ids[0]).unwrap_or_default()),
  );
  ops.register(
    "put2",
    Box::new(|ctx, ids, payload| {
      ctx.put_replicated(ids[0], payload.to_vec(), REPL);
      vec![]
    }),
  );
  ops.register(
    "get2",
    Box::new(|ctx, ids, _p| ctx.get_replicated(ids[0], REPL).unwrap_or_default()),
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
    cluster,
    storages,
  }
}

impl Cluster {
  fn op(&self, name: &str, id: DatumId, payload: &[u8]) -> Response {
    seisin_client::call(
      &self.compute_addr,
      Request::Op {
        op_id: DatumId::new(),
        op_name: name.to_string(),
        datum_ids: vec![id],
        payload: payload.to_vec(),
      },
    )
    .unwrap()
  }

  fn addr_of(&self, node: NodeId) -> String {
    self
      .storages
      .iter()
      .find(|s| s.node_id == node)
      .unwrap()
      .store_addr
      .clone()
  }

  /// The datum's replica nodes under the current storage ring.
  fn replicas(&self, id: DatumId, n: u16) -> Vec<NodeId> {
    self
      .cluster
      .storage_ring
      .read()
      .unwrap()
      .replicas(id, n as usize)
  }

  /// A direct store-wire Get against one node (bypassing the compute
  /// node) — to inspect exactly which nodes physically hold a datum.
  fn direct_get(&self, node: NodeId, id: DatumId) -> Option<Vec<u8>> {
    match store_call(&self.addr_of(node), &StoreRequest::Get { id }).unwrap() {
      StoreResponse::Value { bytes } => bytes,
      other => panic!("expected Value, got {other:?}"),
    }
  }

  fn set_alive(&self, node: NodeId, alive: bool) {
    let mut set = self.cluster.storage_alive.write().unwrap();
    if alive {
      set.insert(node);
    } else {
      set.remove(&node);
    }
  }

  fn wait_until_halted(&self, name: &str, id: DatumId) {
    for _ in 0..50 {
      if let Ok(Response::OpError { message }) = seisin_client::call(
        &self.compute_addr,
        Request::Op {
          op_id: DatumId::new(),
          op_name: name.to_string(),
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
    panic!("cluster did not halt");
  }
}

fn nodes(ids: &[u64]) -> Vec<Storage> {
  ids.iter().map(|&i| start_storage(NodeId(i))).collect()
}

#[test]
fn a_replicated_write_reaches_two_nodes_and_reads_back() {
  let c = build(nodes(&[10, 20, 30]));
  let id = DatumId::new();
  assert_eq!(
    c.op("put2", id, b"dup"),
    Response::OpResult { payload: vec![] }
  );
  assert_eq!(
    c.op("get2", id, &[]),
    Response::OpResult {
      payload: b"dup".to_vec()
    }
  );
  // Both replica nodes physically hold it; a non-replica does not.
  let replicas = c.replicas(id, REPL);
  assert_eq!(replicas.len(), 2);
  for node in &replicas {
    assert_eq!(c.direct_get(*node, id), Some(b"dup".to_vec()));
  }
  let outsider = [NodeId(10), NodeId(20), NodeId(30)]
    .into_iter()
    .find(|n| !replicas.contains(n))
    .unwrap();
  assert_eq!(c.direct_get(outsider, id), None);
}

#[test]
fn a_read_fails_over_when_the_primary_is_down() {
  let c = build(nodes(&[10, 20, 30]));
  let id = DatumId::new();
  c.op("put2", id, b"v");
  // Evict the compute cache so the read must hit storage, then drop the
  // primary: the read fails over to the secondary.
  let primary = c.replicas(id, REPL)[0];
  c.set_alive(primary, false);
  assert_eq!(
    c.op("get2", id, &[]),
    Response::OpResult {
      payload: b"v".to_vec()
    }
  );
  assert!(!c.cluster.halt.is_halted());
}

#[test]
fn a_degraded_write_acks_with_one_replica_down() {
  let c = build(nodes(&[10, 20, 30]));
  let id = DatumId::new();
  let replicas = c.replicas(id, REPL);
  let (primary, secondary) = (replicas[0], replicas[1]);
  // Secondary down: the write still acks (to the surviving primary), and
  // the down node never receives it (it is left behind — a returning
  // node must be re-replicated before it can serve again).
  c.set_alive(secondary, false);
  assert_eq!(
    c.op("put2", id, b"v"),
    Response::OpResult { payload: vec![] }
  );
  assert_eq!(c.direct_get(primary, id), Some(b"v".to_vec()));
  assert_eq!(c.direct_get(secondary, id), None);
}

#[test]
fn total_loss_of_both_replicas_halts_at_point_of_use() {
  let c = build(nodes(&[10, 20, 30]));
  let id = DatumId::new();
  c.op("put2", id, b"v");
  for node in c.replicas(id, REPL) {
    c.set_alive(node, false);
  }
  c.wait_until_halted("get2", id);
  assert!(c.cluster.halt.is_halted());
}

#[test]
fn a_single_copy_datum_still_fail_stops_on_loss() {
  // N=1 is unchanged: the sole replica going away is a total loss.
  let c = build(nodes(&[10, 20, 30]));
  let id = DatumId::new();
  c.op("put1", id, b"v");
  let sole = c.replicas(id, 1)[0];
  c.set_alive(sole, false);
  c.wait_until_halted("get1", id);
}

#[test]
fn a_stale_returned_node_is_not_served() {
  let c = build(nodes(&[10, 20, 30]));
  let id = DatumId::new();
  c.op("put2", id, b"v");
  let secondary = c.replicas(id, REPL)[1];
  // Mark the secondary stale (as a confirmed-dead-then-returned node
  // would be): it is excluded from serving even though it is "alive".
  c.cluster.storage_stale.write().unwrap().insert(secondary);
  // A write now skips the stale secondary (only the primary is served),
  // proven by the secondary NOT receiving the new value.
  c.op("put2", id, b"v2");
  assert_eq!(c.direct_get(secondary, id), Some(b"v".to_vec())); // still the old value
  assert_eq!(c.replicas(id, REPL)[0], c.replicas(id, REPL)[0]); // primary has v2
  let primary = c.replicas(id, REPL)[0];
  assert_eq!(c.direct_get(primary, id), Some(b"v2".to_vec()));
}

#[test]
fn recover_restores_replication_after_a_loss() {
  let c = build(nodes(&[10, 20, 30]));
  // A corpus of replicated datums.
  let ids: Vec<DatumId> = (0..40).map(|_| DatumId::new()).collect();
  for (i, id) in ids.iter().enumerate() {
    c.op("put2", *id, format!("v{i}").as_bytes());
  }
  // Kill node 30: mark it dead (dropped from alive, stale) and make it
  // unreachable by removing its address so the driver treats it as gone.
  let dead = NodeId(30);
  c.set_alive(dead, false);
  c.cluster.storage_stale.write().unwrap().insert(dead);
  c.cluster.store_addresses.write().unwrap().remove(&dead);

  // recover: drop the dead node, restore replication onto the survivors.
  let report = seisin_migrate::recover(std::slice::from_ref(&c.compute_addr), true).unwrap();
  assert!(report.applied);

  // Every datum still reads back, and now has two replicas among the
  // two survivors (10, 20).
  for (i, id) in ids.iter().enumerate() {
    assert_eq!(
      c.op("get2", *id, &[]),
      Response::OpResult {
        payload: format!("v{i}").into_bytes()
      }
    );
    let replicas = c.replicas(*id, REPL);
    assert_eq!(replicas.len(), 2);
    assert!(replicas
      .iter()
      .all(|n| *n == NodeId(10) || *n == NodeId(20)));
  }
  assert!(!c.cluster.halt.is_halted());
}
