//! The durable-source-of-truth abstraction. `InMemoryStore` stands in for
//! the real sharded storage tier (a later sub-project) — for a
//! single-node deployment, storage and compute share a process, but the
//! `Store` trait boundary is what later lets a networked storage tier
//! slot in without touching `Cache` or the worker.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::datum::DatumId;

pub trait Store: Send + Sync {
    fn get(&self, id: DatumId) -> Option<Vec<u8>>;
    fn put(&self, id: DatumId, content: Vec<u8>);
    fn delete(&self, id: DatumId);
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
    fn get(&self, id: DatumId) -> Option<Vec<u8>> {
        self.data.lock().unwrap().get(&id).cloned()
    }

    fn put(&self, id: DatumId, content: Vec<u8>) {
        self.data.lock().unwrap().insert(id, content);
    }

    fn delete(&self, id: DatumId) {
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
}
