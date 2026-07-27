//! `RemoteStore`: the networked `Store` implementation. Resolves a
//! datum's ordered replica set from the storage ring, then writes to
//! every alive, non-stale replica (≥1 required to ack) and reads the
//! primary with failover to the next replica.
//!
//! **Failure policy (Storage Tier Part C-2)**: a single replica that
//! fails a call is marked stale and excluded (recovered later by a
//! driver re-replication) — the write/read proceeds on the survivors.
//! Only when a datum's *every* replica is unreachable does the cluster
//! fail-stop: `RemoteStore` engages the coordinated `HaltState`
//! (whole-cluster, point-of-use) and panics the worker. At replication
//! factor 1 (the default — index datums, untyped datums, and types that
//! did not opt in) the sole replica being gone is exactly that total
//! loss, so single-copy data fail-stops precisely as before.

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::Arc;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::Store;
use seisin_protocol::store_wire::{
  decode_store_response, encode_store_request, StoreRequest, StoreResponse,
};
use seisin_protocol::{read_frame, write_frame};
use seisin_storage::delta::diff;

use crate::gossip_state::ClusterState;

pub struct RemoteStore {
  cluster: Arc<ClusterState>,
}

thread_local! {
  static CONNECTIONS: RefCell<HashMap<u64, TcpStream>> = RefCell::new(HashMap::new());
}

impl RemoteStore {
  pub fn new(cluster: Arc<ClusterState>) -> Self {
    Self { cluster }
  }

  /// The datum's replica set restricted to nodes that can actually serve
  /// it right now — in the ring, alive, and not stale — in rank order
  /// (rank 0, the primary, first).
  fn serving_replicas(&self, id: DatumId, n: u16) -> Vec<NodeId> {
    let replicas = self
      .cluster
      .storage_ring
      .read()
      .unwrap()
      .replicas(id, n as usize);
    let alive = self.cluster.storage_alive.read().unwrap();
    let stale = self.cluster.storage_stale.read().unwrap();
    replicas
      .into_iter()
      .filter(|node| alive.contains(node) && !stale.contains(node))
      .collect()
  }

  /// Excludes `node` from future serving until a driver re-replication
  /// re-admits it — used when a call to it fails mid-operation.
  fn mark_stale(&self, node: NodeId) {
    self.cluster.storage_stale.write().unwrap().insert(node);
  }

  /// Engages the coordinated whole-cluster halt for a datum whose every
  /// replica is gone, then fail-stops this worker.
  fn halt_total_loss(&self, id: DatumId) -> ! {
    let reason =
      format!("cluster halted: every replica of datum {id:?} is unreachable — total shard loss");
    self.cluster.halt.halt(reason.clone());
    panic!("{reason}");
  }

  /// One request/response round trip on this thread's connection to
  /// `node`, reconnecting once on an IO error before returning `Err`
  /// (so the caller can fail over / mark the node stale rather than
  /// bring the whole cluster down for a single-replica hiccup).
  fn try_call(&self, node: NodeId, request: &StoreRequest) -> Result<StoreResponse, String> {
    let address = self
      .cluster
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
        // One reconnect attempt covers a storage-side idle close.
        Err(_) if attempt == 0 => continue,
        Err(e) => return Err(e),
      }
    }
    unreachable!("both attempts return")
  }

  /// Writes `id` to `node`, using a byte delta when one is supplied and
  /// worthwhile (falling back to a full `Put` on `NeedFull`). Returns
  /// `Err` if the node is unreachable or answers unexpectedly.
  fn write_one(
    &self,
    node: NodeId,
    id: DatumId,
    content: &[u8],
    delta: Option<&seisin_storage::delta::Delta>,
    n: u16,
  ) -> Result<(), String> {
    let full = || StoreRequest::Put {
      id,
      bytes: content.to_vec(),
      n,
    };
    match delta {
      Some(delta) => match self.try_call(
        node,
        &StoreRequest::Patch {
          id,
          delta: delta.clone(),
          n,
        },
      )? {
        StoreResponse::Ack => Ok(()),
        StoreResponse::NeedFull => match self.try_call(node, &full())? {
          StoreResponse::Ack => Ok(()),
          other => Err(format!("unexpected reply to Put: {other:?}")),
        },
        other => Err(format!("unexpected reply to Patch: {other:?}")),
      },
      None => match self.try_call(node, &full())? {
        StoreResponse::Ack => Ok(()),
        other => Err(format!("unexpected reply to Put: {other:?}")),
      },
    }
  }

  /// Writes to every serving replica (≥1 required), marking any that
  /// fails stale; total failure fail-stops.
  fn write_all(&self, id: DatumId, content: Vec<u8>, previous: Option<&[u8]>, n: u16) {
    let targets = self.serving_replicas(id, n);
    if targets.is_empty() {
      self.halt_total_loss(id);
    }
    let delta = previous.and_then(|prev| {
      let d = diff(prev, &content);
      (d.encoded_len() < content.len() / 2).then_some(d)
    });
    let mut acked = 0;
    for node in targets {
      match self.write_one(node, id, &content, delta.as_ref(), n) {
        Ok(()) => acked += 1,
        Err(_) => self.mark_stale(node),
      }
    }
    if acked == 0 {
      self.halt_total_loss(id);
    }
  }
}

impl Store for RemoteStore {
  fn get_replicated(&self, id: DatumId, n: u16) -> Option<Vec<u8>> {
    let targets = self.serving_replicas(id, n);
    if targets.is_empty() {
      self.halt_total_loss(id);
    }
    let mut reached_any = false;
    for node in targets {
      match self.try_call(node, &StoreRequest::Get { id }) {
        Ok(StoreResponse::Value { bytes: Some(value) }) => return Some(value),
        Ok(StoreResponse::Value { bytes: None }) => reached_any = true,
        Ok(other) => {
          // A serving replica that answers a Get with a non-Value is
          // broken — exclude it and fail over.
          let _ = other;
          self.mark_stale(node);
        }
        Err(_) => self.mark_stale(node),
      }
    }
    if reached_any {
      None // every reachable replica agrees the datum is absent
    } else {
      self.halt_total_loss(id) // no replica was reachable — total loss
    }
  }

  fn put_replicated(&self, id: DatumId, content: Vec<u8>, n: u16) {
    self.write_all(id, content, None, n);
  }

  fn delete_replicated(&self, id: DatumId, n: u16) {
    let targets = self.serving_replicas(id, n);
    if targets.is_empty() {
      self.halt_total_loss(id);
    }
    let mut acked = 0;
    for node in targets {
      match self.try_call(node, &StoreRequest::Delete { id }) {
        Ok(StoreResponse::Ack) => acked += 1,
        Ok(_) | Err(_) => self.mark_stale(node),
      }
    }
    if acked == 0 {
      self.halt_total_loss(id);
    }
  }

  fn put_with_previous_replicated(
    &self,
    id: DatumId,
    content: Vec<u8>,
    previous: Option<&[u8]>,
    n: u16,
  ) {
    self.write_all(id, content, previous, n);
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
  /// returning its address and the kept-alive tempdir.
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
    });
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || serve_store(listener, node));
    (addr, dir)
  }

  /// A single-node (N=1) RemoteStore over one in-process storage node —
  /// the shape the pre-replication tests used.
  fn store_pair() -> (RemoteStore, tempfile::TempDir) {
    let (addr, dir) = start_storage(NodeId(1));
    let cluster = Arc::new(ClusterState {
      storage_ring: Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)]))),
      store_addresses: Arc::new(RwLock::new(HashMap::from([(NodeId(1), addr)]))),
      storage_alive: Arc::new(RwLock::new(HashSet::from([NodeId(1)]))),
      ..ClusterState::compute_only(Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)]))))
    });
    (RemoteStore::new(cluster), dir)
  }

  #[test]
  fn put_get_delete_round_trip_over_the_wire() {
    let (store, _dir) = store_pair();
    let id = DatumId::new();
    assert_eq!(store.get(id), None);
    store.put(id, b"hello".to_vec());
    assert_eq!(store.get(id), Some(b"hello".to_vec()));
    store.delete(id);
    assert_eq!(store.get(id), None);
  }

  #[test]
  fn put_with_previous_ships_a_patch_and_reads_back_exactly() {
    let (store, _dir) = store_pair();
    let id = DatumId::new();
    let v1 = vec![7u8; 4096];
    store.put(id, v1.clone());
    let mut v2 = v1.clone();
    v2[1000] = 9;
    store.put_with_previous(id, v2.clone(), Some(&v1));
    assert_eq!(store.get(id), Some(v2));
  }

  #[test]
  fn a_patch_for_an_unknown_id_falls_back_to_a_full_put() {
    let (store, _dir) = store_pair();
    let id = DatumId::new();
    let old = vec![1u8; 4096];
    let mut new = old.clone();
    new[10] = 2;
    store.put_with_previous(id, new.clone(), Some(&old));
    assert_eq!(store.get(id), Some(new));
  }

  #[test]
  fn a_poor_delta_goes_straight_to_a_full_put() {
    let (store, _dir) = store_pair();
    let id = DatumId::new();
    store.put(id, b"abc".to_vec());
    store.put_with_previous(id, b"xyz!".to_vec(), Some(b"abc"));
    assert_eq!(store.get(id), Some(b"xyz!".to_vec()));
  }

  /// A three-node cluster; a replication-factor-2 datum lands on two
  /// nodes, and a read still succeeds after its primary is dropped.
  fn three_node_cluster() -> (RemoteStore, Arc<ClusterState>, Vec<tempfile::TempDir>) {
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
    (RemoteStore::new(Arc::clone(&cluster)), cluster, dirs)
  }

  #[test]
  fn a_replicated_write_reaches_two_nodes_and_reads_back() {
    let (store, cluster, _dirs) = three_node_cluster();
    // Find an id whose two replicas we can inspect directly.
    let id = DatumId::new();
    let replicas = cluster.storage_ring.read().unwrap().replicas(id, 2);
    assert_eq!(replicas.len(), 2);
    store.put_replicated(id, b"dup".to_vec(), 2);
    assert_eq!(store.get_replicated(id, 2), Some(b"dup".to_vec()));

    // Drop the primary from the alive set: the read fails over to the
    // secondary and still returns the value, no halt.
    cluster.storage_alive.write().unwrap().remove(&replicas[0]);
    assert_eq!(store.get_replicated(id, 2), Some(b"dup".to_vec()));
    assert!(!cluster.halt.is_halted());
  }

  #[test]
  fn total_loss_trips_the_point_of_use_halt() {
    let (store, cluster, _dirs) = three_node_cluster();
    let id = DatumId::new();
    // Drop every replica of this datum from the alive set.
    for node in cluster.storage_ring.read().unwrap().replicas(id, 2) {
      cluster.storage_alive.write().unwrap().remove(&node);
    }
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
      store.get_replicated(id, 2);
    }))
    .is_err();
    assert!(panicked, "a total-loss read should fail-stop the worker");
    assert!(cluster.halt.is_halted());
    assert!(cluster.halt.reason().unwrap().contains("total shard loss"));
  }
}
