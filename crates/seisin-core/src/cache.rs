//! The owning-thread's in-memory working-copy cache. Every mutation is
//! written through to the backing `Store` *before* this returns, so an
//! acknowledged write is never only-in-memory — this is what makes lazy
//! crash recovery safe in later sub-projects (see the design doc's
//! "Collation & Op Execution" section: write-before-ack).

use std::collections::HashMap;
use std::sync::Arc;

use crate::datum::DatumId;
use crate::store::Store;

pub struct Cache {
  store: Arc<dyn Store>,
  entries: HashMap<DatumId, Vec<u8>>,
}

impl Cache {
  pub fn new(store: Arc<dyn Store>) -> Self {
    Self {
      store,
      entries: HashMap::new(),
    }
  }

  /// Returns the datum's content, serving from the local cache on a hit
  /// and loading through to the store on a miss.
  pub fn get(&mut self, id: DatumId) -> Option<Vec<u8>> {
    if let Some(content) = self.entries.get(&id) {
      return Some(content.clone());
    }
    let loaded = self.store.get(id)?;
    self.entries.insert(id, loaded.clone());
    Some(loaded)
  }

  /// Writes through to the store, then updates the local cache. Returns
  /// only after the store write completes.
  pub fn put(&mut self, id: DatumId, content: Vec<u8>) {
    self.store.put(id, content.clone());
    self.entries.insert(id, content);
  }

  /// Deletes from the store, then evicts the local cache entry.
  pub fn delete(&mut self, id: DatumId) {
    self.store.delete(id);
    self.entries.remove(&id);
  }

  /// Evicts a cache entry without touching the store — used when a
  /// datum's ownership moves away from this thread.
  pub fn invalidate(&mut self, id: DatumId) {
    self.entries.remove(&id);
  }

  /// Evicts every cached entry for which `is_native(datum_id)` returns
  /// false — called after a ring mutation to drop entries this node no
  /// longer natively owns. This guarantees that if this node later
  /// regains ownership of one of them, `get` is a hard miss and reloads
  /// from the store, rather than serving a value that might predate
  /// another node's writes in the interim (see the design doc's "Cache
  /// Invalidation on Ring Membership Change" section).
  pub fn evict_non_native(&mut self, mut is_native: impl FnMut(DatumId) -> bool) {
    self.entries.retain(|&id, _| is_native(id));
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::store::InMemoryStore;

  #[test]
  fn get_on_empty_cache_and_store_returns_none() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    assert_eq!(cache.get(DatumId::new()), None);
  }

  #[test]
  fn put_is_readable_directly_from_the_store() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let mut cache = Cache::new(Arc::clone(&store));
    let id = DatumId::new();
    cache.put(id, b"hello".to_vec());
    assert_eq!(store.get(id), Some(b"hello".to_vec()));
  }

  #[test]
  fn get_after_put_returns_the_written_content() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let id = DatumId::new();
    cache.put(id, b"hello".to_vec());
    assert_eq!(cache.get(id), Some(b"hello".to_vec()));
  }

  #[test]
  fn invalidate_forces_a_reload_from_the_store() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let mut cache = Cache::new(Arc::clone(&store));
    let id = DatumId::new();
    cache.put(id, b"original".to_vec());

    // Mutate storage directly, bypassing the cache, to simulate another
    // node having written through while this thread held a now-stale
    // cache entry.
    store.put(id, b"updated".to_vec());
    cache.invalidate(id);

    assert_eq!(cache.get(id), Some(b"updated".to_vec()));
  }

  #[test]
  fn delete_removes_from_both_cache_and_store() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let mut cache = Cache::new(Arc::clone(&store));
    let id = DatumId::new();
    cache.put(id, b"hello".to_vec());
    cache.delete(id);
    assert_eq!(cache.get(id), None);
    assert_eq!(store.get(id), None);
  }

  #[test]
  fn evict_non_native_removes_entries_the_predicate_rejects() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let mut cache = Cache::new(Arc::clone(&store));
    let kept = DatumId::new();
    let evicted = DatumId::new();
    cache.put(kept, b"kept".to_vec());
    cache.put(evicted, b"evicted".to_vec());

    cache.evict_non_native(|id| id == kept);

    // The evicted entry must reload from the store rather than serve a
    // stale cached value: mutate storage directly, then confirm get
    // picks up the new value.
    store.put(evicted, b"updated".to_vec());
    assert_eq!(cache.get(evicted), Some(b"updated".to_vec()));

    // The kept entry is unaffected.
    assert_eq!(cache.get(kept), Some(b"kept".to_vec()));
  }
}
