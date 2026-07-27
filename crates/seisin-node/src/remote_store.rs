//! `RemoteStore`: the networked `Store` implementation — hashes each
//! id into the static storage ring and round-trips to the owning
//! storage node over one plain blocking TCP connection per compute
//! worker thread (thread-local; `Store` is synchronous, so no
//! multiplexing is needed).
//!
//! **Failure policy (Part A fail-stop)**: any storage round-trip
//! failure — connect refused after one reconnect attempt, disconnect
//! mid-call, malformed or unexpected reply — panics the calling worker
//! with the storage node and datum named. This is v1 of "the cluster
//! halts rather than serve from a partially-lost dataset"; coordinated
//! cluster-wide halt arrives with Part B's storage-pool membership.

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::{Arc, RwLock};

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::Store;
use seisin_protocol::store_wire::{
  decode_store_response, encode_store_request, StoreRequest, StoreResponse,
};
use seisin_protocol::{read_frame, write_frame};
use seisin_ring::ring::Ring;
use seisin_storage::delta::diff;

pub struct RemoteStore {
  /// The storage ring — `Ring` reused with capacity weights in place
  /// of thread counts; the thread half of `native()` is ignored.
  storage_ring: Arc<RwLock<Ring>>,
  /// RwLock so gossip can extend the book when a storage member joins
  /// (Part B) — lookups read-lock.
  addresses: Arc<RwLock<HashMap<NodeId, String>>>,
}

thread_local! {
  static CONNECTIONS: RefCell<HashMap<u64, TcpStream>> = RefCell::new(HashMap::new());
}

impl RemoteStore {
  pub fn new(
    storage_ring: Arc<RwLock<Ring>>,
    addresses: Arc<RwLock<HashMap<NodeId, String>>>,
  ) -> Self {
    Self {
      storage_ring,
      addresses,
    }
  }

  fn storage_node_for(&self, id: DatumId) -> NodeId {
    self.storage_ring.read().unwrap().native(id).0
  }

  /// One request/response round trip on this thread's connection to
  /// `node`, reconnecting once on an IO error before giving up.
  fn call(&self, node: NodeId, id: DatumId, request: &StoreRequest) -> StoreResponse {
    let address = self
      .addresses
      .read()
      .unwrap()
      .get(&node)
      .unwrap_or_else(|| panic!("no store address configured for storage node {node:?}"))
      .clone();
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
        Ok(payload) => match decode_store_response(&payload) {
          Ok(response) => return response,
          Err(e) => panic!(
            "storage node {node:?} sent a malformed reply for datum {id:?}: {e} — halting (fail-stop)"
          ),
        },
        Err(e) if attempt == 0 => {
          // One reconnect attempt covers a storage-side idle close.
          let _ = e;
          continue;
        }
        Err(e) => panic!(
          "storage node {node:?} unreachable for datum {id:?}: {e} — halting (fail-stop)"
        ),
      }
    }
    unreachable!("both attempts return or panic")
  }
}

impl Store for RemoteStore {
  fn get(&self, id: DatumId) -> Option<Vec<u8>> {
    let node = self.storage_node_for(id);
    match self.call(node, id, &StoreRequest::Get { id }) {
      StoreResponse::Value { bytes } => bytes,
      other => panic!(
        "storage node {node:?} answered a Get for {id:?} with {other:?} — halting (fail-stop)"
      ),
    }
  }

  fn put(&self, id: DatumId, content: Vec<u8>) {
    let node = self.storage_node_for(id);
    match self.call(node, id, &StoreRequest::Put { id, bytes: content }) {
      StoreResponse::Ack => {}
      other => panic!(
        "storage node {node:?} answered a Put for {id:?} with {other:?} — halting (fail-stop)"
      ),
    }
  }

  fn delete(&self, id: DatumId) {
    let node = self.storage_node_for(id);
    match self.call(node, id, &StoreRequest::Delete { id }) {
      StoreResponse::Ack => {}
      other => panic!(
        "storage node {node:?} answered a Delete for {id:?} with {other:?} — halting (fail-stop)"
      ),
    }
  }

  /// The delta path: with a previous value in hand and a worthwhile
  /// trim, ship a Patch; on `NeedFull` (the log has no base — e.g. the
  /// cache believed in a value the log never saw) fall back to a full
  /// Put. Cold caches, new datums, and poor deltas go straight to Put.
  fn put_with_previous(&self, id: DatumId, content: Vec<u8>, previous: Option<&[u8]>) {
    let Some(previous) = previous else {
      return self.put(id, content);
    };
    let delta = diff(previous, &content);
    if delta.encoded_len() >= content.len() / 2 {
      return self.put(id, content);
    }
    let node = self.storage_node_for(id);
    match self.call(node, id, &StoreRequest::Patch { id, delta }) {
      StoreResponse::Ack => {}
      StoreResponse::NeedFull => self.put(id, content),
      other => panic!(
        "storage node {node:?} answered a Patch for {id:?} with {other:?} — halting (fail-stop)"
      ),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::net::TcpListener;
  use std::sync::Mutex;

  use seisin_storage::datum_log::DatumLog;

  /// Boots an in-process store server on a tempdir log, returning the
  /// RemoteStore wired to it (single storage node, weight 1) and the
  /// tempdir (kept alive).
  fn store_pair() -> (RemoteStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let log = Arc::new(Mutex::new(
      DatumLog::open(&dir.path().join("datum_log.dlog")).unwrap(),
    ));
    let node = Arc::new(crate::store_server::StoreNode {
      log,
      node_id: NodeId(1),
      heartbeat: Arc::new(crate::heartbeat::Heartbeat::new()),
      self_halt_threshold: std::time::Duration::from_secs(3600),
    });
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || crate::store_server::serve_store(listener, node));
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let mut addresses = HashMap::new();
    addresses.insert(NodeId(1), addr);
    (
      RemoteStore::new(ring, Arc::new(RwLock::new(addresses))),
      dir,
    )
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
    // The store has never seen this id, but the caller believes it has
    // a previous value (e.g. a cache/log divergence): NeedFull -> Put.
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
    // Completely different content: delta >= half — full put path.
    store.put_with_previous(id, b"xyz!".to_vec(), Some(b"abc"));
    assert_eq!(store.get(id), Some(b"xyz!".to_vec()));
  }
}
