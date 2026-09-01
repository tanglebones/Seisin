//! `CollectionStore`: the compute-side interface to the storage tier's
//! ordered-collection primitive — `RemoteCollectionStore` is the real
//! networked implementation; a solution's `IndexKind`s (lb, and later
//! rk/tk) depend on this trait, not on `RemoteCollectionStore` directly,
//! the same way compute code already depends on `Store` rather than
//! `RemoteStore`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::Arc;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_protocol::store_wire::{
  decode_store_response, encode_store_request, StoreRequest, StoreResponse,
};
use seisin_protocol::{read_frame, write_frame};

use crate::gossip_state::ClusterState;
use crate::replica_resolver::ReplicaResolver;

pub trait CollectionStore: Send + Sync {
  /// Idempotent: creates the collection if it doesn't already exist.
  fn create(&self, collection_id: DatumId, key_size: u32, value_size: u32, n: u16);
  fn insert(&self, collection_id: DatumId, key: Vec<u8>, value: Vec<u8>, n: u16);
  fn remove(&self, collection_id: DatumId, key: Vec<u8>, n: u16);
  fn get(&self, collection_id: DatumId, key: Vec<u8>, n: u16) -> Option<Vec<u8>>;
  /// Best-first bounded scan.
  fn scan_forward(&self, collection_id: DatumId, limit: u32, n: u16) -> Vec<(Vec<u8>, Vec<u8>)>;
  /// Worst-first bounded scan.
  fn scan_backward(&self, collection_id: DatumId, limit: u32, n: u16) -> Vec<(Vec<u8>, Vec<u8>)>;
  fn sample(&self, collection_id: DatumId, k: u32, n: u16) -> Vec<(Vec<u8>, Vec<u8>)>;
  /// Ascending rank (0 = worst) of `key`, if present.
  fn rank_of_key(&self, collection_id: DatumId, key: Vec<u8>, n: u16) -> Option<u64>;
  fn scan_from_rank(
    &self,
    collection_id: DatumId,
    rank: u64,
    limit: u32,
    n: u16,
  ) -> Vec<(Vec<u8>, Vec<u8>)>;
  fn count(&self, collection_id: DatumId, n: u16) -> u64;
}

pub struct RemoteCollectionStore {
  resolver: ReplicaResolver,
}

thread_local! {
  static CONNECTIONS: RefCell<HashMap<u64, TcpStream>> = RefCell::new(HashMap::new());
}

impl RemoteCollectionStore {
  pub fn new(cluster: Arc<ClusterState>) -> Self {
    Self {
      resolver: ReplicaResolver::new(cluster),
    }
  }

  /// One request/response round trip on this thread's connection to
  /// `node`'s store address, reconnecting once on an IO error.
  fn try_call(&self, node: NodeId, request: &StoreRequest) -> Result<StoreResponse, String> {
    let address = self
      .resolver
      .cluster()
      .store_addresses
      .read()
      .unwrap()
      .get(&node)
      .cloned()
      .ok_or_else(|| format!("no store address configured for storage node {node:?}"))?;
    let encoded = encode_store_request(request);
    for attempt in 0..2 {
      let result = CONNECTIONS.with(|conns| {
        let mut conns = conns.borrow_mut();
        let stream = match conns.entry(node.0) {
          std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
          std::collections::hash_map::Entry::Vacant(v) => match TcpStream::connect(&address) {
            Ok(stream) => v.insert(stream),
            Err(e) => return Err(e.to_string()),
          },
        };
        if let Err(e) = write_frame(stream, &encoded) {
          conns.remove(&node.0);
          return Err(e.to_string());
        }
        match read_frame(stream) {
          Ok(payload) => Ok(payload),
          Err(e) => {
            conns.remove(&node.0);
            Err(e.to_string())
          }
        }
      });
      match result {
        Ok(payload) => return decode_store_response(&payload).map_err(|e| e.to_string()),
        Err(_) if attempt == 0 => continue,
        Err(e) => return Err(e),
      }
    }
    unreachable!("both attempts return")
  }

  /// Sends `request` to every serving replica of `collection_id`; a
  /// node that fails is marked stale; total failure fail-stops. Used
  /// for `create`/`insert`/`remove` (write ops — logical-op
  /// replication, not byte diffs, since storage itself performs the
  /// mutation here rather than receiving precomputed bytes to diff).
  fn write_all(&self, collection_id: DatumId, n: u16, request: &StoreRequest) {
    let targets = self.resolver.serving_replicas(collection_id, n);
    if targets.is_empty() {
      self.resolver.halt_total_loss(collection_id);
    }
    let mut acked = 0;
    for node in targets {
      match self.try_call(node, request) {
        Ok(StoreResponse::Ack) => acked += 1,
        _ => self.resolver.mark_stale(node),
      }
    }
    if acked == 0 {
      self.resolver.halt_total_loss(collection_id);
    }
  }

  /// Reads from the primary replica, failing over to the next on error
  /// — mirrors `RemoteStore::get`'s failover shape.
  fn read_one(&self, collection_id: DatumId, n: u16, request: &StoreRequest) -> StoreResponse {
    let targets = self.resolver.serving_replicas(collection_id, n);
    if targets.is_empty() {
      self.resolver.halt_total_loss(collection_id);
    }
    for node in &targets {
      match self.try_call(*node, request) {
        Ok(response) => return response,
        Err(_) => self.resolver.mark_stale(*node),
      }
    }
    self.resolver.halt_total_loss(collection_id);
  }
}

impl CollectionStore for RemoteCollectionStore {
  fn create(&self, collection_id: DatumId, key_size: u32, value_size: u32, n: u16) {
    self.write_all(
      collection_id,
      n,
      &StoreRequest::CollectionCreate {
        collection_id,
        key_size,
        value_size,
      },
    );
  }

  fn insert(&self, collection_id: DatumId, key: Vec<u8>, value: Vec<u8>, n: u16) {
    self.write_all(
      collection_id,
      n,
      &StoreRequest::CollectionInsert {
        collection_id,
        key,
        value,
      },
    );
  }

  fn remove(&self, collection_id: DatumId, key: Vec<u8>, n: u16) {
    self.write_all(
      collection_id,
      n,
      &StoreRequest::CollectionRemove { collection_id, key },
    );
  }

  fn get(&self, collection_id: DatumId, key: Vec<u8>, n: u16) -> Option<Vec<u8>> {
    match self.read_one(
      collection_id,
      n,
      &StoreRequest::CollectionGet { collection_id, key },
    ) {
      StoreResponse::CollectionEntry { value } => value,
      other => panic!("unexpected reply to CollectionGet: {other:?}"),
    }
  }

  fn scan_forward(&self, collection_id: DatumId, limit: u32, n: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
    match self.read_one(
      collection_id,
      n,
      &StoreRequest::CollectionScanForward {
        collection_id,
        limit,
      },
    ) {
      StoreResponse::CollectionEntries { entries } => entries,
      other => panic!("unexpected reply to CollectionScanForward: {other:?}"),
    }
  }

  fn scan_backward(&self, collection_id: DatumId, limit: u32, n: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
    match self.read_one(
      collection_id,
      n,
      &StoreRequest::CollectionScanBackward {
        collection_id,
        limit,
      },
    ) {
      StoreResponse::CollectionEntries { entries } => entries,
      other => panic!("unexpected reply to CollectionScanBackward: {other:?}"),
    }
  }

  fn sample(&self, collection_id: DatumId, k: u32, n: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
    match self.read_one(
      collection_id,
      n,
      &StoreRequest::CollectionSample { collection_id, k },
    ) {
      StoreResponse::CollectionEntries { entries } => entries,
      other => panic!("unexpected reply to CollectionSample: {other:?}"),
    }
  }

  fn rank_of_key(&self, collection_id: DatumId, key: Vec<u8>, n: u16) -> Option<u64> {
    match self.read_one(
      collection_id,
      n,
      &StoreRequest::CollectionRankOfKey { collection_id, key },
    ) {
      StoreResponse::CollectionRank { rank } => rank,
      other => panic!("unexpected reply to CollectionRankOfKey: {other:?}"),
    }
  }

  fn scan_from_rank(
    &self,
    collection_id: DatumId,
    rank: u64,
    limit: u32,
    n: u16,
  ) -> Vec<(Vec<u8>, Vec<u8>)> {
    match self.read_one(
      collection_id,
      n,
      &StoreRequest::CollectionScanFromRank {
        collection_id,
        rank,
        limit,
      },
    ) {
      StoreResponse::CollectionEntries { entries } => entries,
      other => panic!("unexpected reply to CollectionScanFromRank: {other:?}"),
    }
  }

  fn count(&self, collection_id: DatumId, n: u16) -> u64 {
    match self.read_one(
      collection_id,
      n,
      &StoreRequest::CollectionCount { collection_id },
    ) {
      StoreResponse::CollectionCount { total } => total,
      other => panic!("unexpected reply to CollectionCount: {other:?}"),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::collections::HashSet;
  use std::net::TcpListener;
  use std::sync::{Mutex, RwLock};

  use seisin_ring::ring::Ring;
  use seisin_storage::datum_log::DatumLog;

  use crate::store_server::{serve_store, StoreNode};

  /// Boots an in-process store server on a tempdir log at `node_id`,
  /// returning its address and the kept-alive tempdir — same shape as
  /// `remote_store.rs`'s own `start_storage` test helper.
  fn start_storage(node_id: NodeId) -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let log = Arc::new(Mutex::new(
      DatumLog::open(&dir.path().join("datum_log.dlog")).unwrap(),
    ));
    let node = Arc::new(StoreNode {
      log,
      node_id,
      heartbeat: Arc::new(crate::heartbeat::Heartbeat::new()),
      self_halt_threshold: std::time::Duration::from_secs(3600),
      transfers: Arc::new(crate::transfer::TransferManager::default()),
      data_dir: dir.path().to_path_buf(),
      collections: Mutex::new(HashMap::new()),
    });
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || serve_store(listener, node));
    (addr, dir)
  }

  /// A three-node cluster; a replication-factor-2 collection lands on
  /// two nodes, and an insert still succeeds (and a read still works)
  /// after one replica is dropped from the alive set.
  fn three_node_cluster() -> (
    RemoteCollectionStore,
    Arc<ClusterState>,
    Vec<tempfile::TempDir>,
  ) {
    let mut addrs = HashMap::new();
    let mut dirs = Vec::new();
    for id in [NodeId(1), NodeId(2), NodeId(3)] {
      let (addr, dir) = start_storage(id);
      addrs.insert(id, addr);
      dirs.push(dir);
    }
    let cluster = Arc::new(ClusterState {
      storage_ring: Arc::new(RwLock::new(Ring::from_members(&[
        (NodeId(1), 1),
        (NodeId(2), 1),
        (NodeId(3), 1),
      ]))),
      store_addresses: Arc::new(RwLock::new(addrs)),
      storage_alive: Arc::new(RwLock::new(HashSet::from([
        NodeId(1),
        NodeId(2),
        NodeId(3),
      ]))),
      ..ClusterState::compute_only(Arc::new(RwLock::new(Ring::from_members(&[(NodeId(9), 1)]))))
    });
    (
      RemoteCollectionStore::new(Arc::clone(&cluster)),
      cluster,
      dirs,
    )
  }

  #[test]
  fn create_insert_and_get_round_trip_and_survive_one_replica_down() {
    let (store, cluster, _dirs) = three_node_cluster();
    let collection_id = DatumId::new();
    let replicas = cluster
      .storage_ring
      .read()
      .unwrap()
      .replicas(collection_id, 2);
    assert_eq!(replicas.len(), 2);

    store.create(collection_id, 4, 4, 2);
    store.insert(collection_id, vec![1, 2, 3, 4], vec![5, 6, 7, 8], 2);
    assert_eq!(
      store.get(collection_id, vec![1, 2, 3, 4], 2),
      Some(vec![5, 6, 7, 8])
    );

    // Drop the primary from the alive set: reads/writes fail over to
    // the secondary and still succeed, and the primary gets marked
    // stale rather than the cluster halting.
    cluster.storage_alive.write().unwrap().remove(&replicas[0]);
    assert_eq!(
      store.get(collection_id, vec![1, 2, 3, 4], 2),
      Some(vec![5, 6, 7, 8])
    );
    store.insert(collection_id, vec![9, 9, 9, 9], vec![1, 1, 1, 1], 2);
    assert_eq!(store.count(collection_id, 2), 2);
    assert!(!cluster.halt.is_halted());
  }

  #[test]
  fn total_loss_trips_the_point_of_use_halt() {
    let (store, cluster, _dirs) = three_node_cluster();
    let collection_id = DatumId::new();
    for node in cluster
      .storage_ring
      .read()
      .unwrap()
      .replicas(collection_id, 2)
    {
      cluster.storage_alive.write().unwrap().remove(&node);
    }
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
      store.create(collection_id, 4, 4, 2);
    }))
    .is_err();
    assert!(panicked, "a total-loss write should fail-stop the worker");
    assert!(cluster.halt.is_halted());
    assert!(cluster.halt.reason().unwrap().contains("total shard loss"));
  }
}
