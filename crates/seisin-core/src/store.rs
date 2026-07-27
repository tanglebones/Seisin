//! The durable-source-of-truth abstraction. `InMemoryStore` stands in for
//! the real sharded storage tier (a later sub-project) — for a
//! single-node deployment, storage and compute share a process, but the
//! `Store` trait boundary is what later lets a networked storage tier
//! slot in without touching `Cache` or the worker.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::datum::DatumId;

pub trait Store: Send + Sync {
  /// Reads `id`; `n` is its replication factor — a networked store uses
  /// it to bound the replica set it reads/fails over across.
  fn get_replicated(&self, id: DatumId, n: u16) -> Option<Vec<u8>>;
  /// Writes `id` with replication factor `n` — a networked store
  /// persists `n` per datum and (once replicated) writes to `n` nodes.
  fn put_replicated(&self, id: DatumId, content: Vec<u8>, n: u16);
  fn delete_replicated(&self, id: DatumId, n: u16);

  /// A put that also carries the caller's previous value for `id`, if
  /// it holds one — a networked store uses it to ship a byte delta
  /// instead of the full content (see the Storage Tier design doc).
  /// The default ignores `previous`; in-memory stores need nothing
  /// more.
  fn put_with_previous_replicated(
    &self,
    id: DatumId,
    content: Vec<u8>,
    previous: Option<&[u8]>,
    n: u16,
  ) {
    let _ = previous;
    self.put_replicated(id, content, n);
  }

  // --- N=1 back-compat wrappers: the single-copy default path, so every
  // existing caller (and test) stays byte-for-byte unchanged. ---
  fn get(&self, id: DatumId) -> Option<Vec<u8>> {
    self.get_replicated(id, 1)
  }
  fn put(&self, id: DatumId, content: Vec<u8>) {
    self.put_replicated(id, content, 1);
  }
  fn delete(&self, id: DatumId) {
    self.delete_replicated(id, 1);
  }
  fn put_with_previous(&self, id: DatumId, content: Vec<u8>, previous: Option<&[u8]>) {
    self.put_with_previous_replicated(id, content, previous, 1);
  }
}

#[derive(Default)]
pub struct InMemoryStore {
  data: Mutex<HashMap<DatumId, Vec<u8>>>,
}

impl InMemoryStore {
  pub fn new() -> Self {
    Self::default()
  }
}

impl Store for InMemoryStore {
  // In-memory storage is a single process — replication has no meaning,
  // so `n` is ignored (one copy).
  fn get_replicated(&self, id: DatumId, _n: u16) -> Option<Vec<u8>> {
    self.data.lock().unwrap().get(&id).cloned()
  }

  fn put_replicated(&self, id: DatumId, content: Vec<u8>, _n: u16) {
    self.data.lock().unwrap().insert(id, content);
  }

  fn delete_replicated(&self, id: DatumId, _n: u16) {
    self.data.lock().unwrap().remove(&id);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn put_then_get_returns_content() {
    let store = InMemoryStore::new();
    let id = DatumId::new();
    store.put(id, b"hello".to_vec());
    assert_eq!(store.get(id), Some(b"hello".to_vec()));
  }

  #[test]
  fn get_on_missing_id_returns_none() {
    let store = InMemoryStore::new();
    assert_eq!(store.get(DatumId::new()), None);
  }

  #[test]
  fn delete_removes_content() {
    let store = InMemoryStore::new();
    let id = DatumId::new();
    store.put(id, b"hello".to_vec());
    store.delete(id);
    assert_eq!(store.get(id), None);
  }

  #[test]
  fn delete_on_missing_id_is_a_no_op() {
    let store = InMemoryStore::new();
    store.delete(DatumId::new());
  }

  #[test]
  fn in_memory_store_ignores_the_replication_factor() {
    let store = InMemoryStore::new();
    let id = DatumId::new();
    store.put_replicated(id, b"v".to_vec(), 3);
    // Readable at any factor — in-memory keeps one copy regardless.
    assert_eq!(store.get_replicated(id, 1), Some(b"v".to_vec()));
    assert_eq!(store.get(id), Some(b"v".to_vec()));
  }
}
