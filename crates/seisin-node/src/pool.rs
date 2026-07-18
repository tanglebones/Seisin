//! A pool of owning threads (one `WorkerHandle` per `ThreadId`) sharing
//! one backing store — this node's share of the compute ring's slots.
//! Each thread's `Cache` is independent, but since all threads on this
//! node share the same `Store`, a write on one thread's cache is visible
//! (via a store fallback on cache miss) from another thread on the same
//! node — ownership isolation across nodes is enforced by the ring/relay
//! layer in `server.rs`, not by this type.

use std::sync::Arc;

use seisin_core::authority::ThreadId;
use seisin_core::datum::DatumId;
use seisin_core::store::Store;
use seisin_protocol::{Request, Response};

use crate::worker::WorkerHandle;

pub struct WorkerPool {
  handles: Vec<WorkerHandle>,
}

impl WorkerPool {
  /// Spawns `thread_count` worker threads, each with its own `Cache`
  /// over the same shared `store`.
  pub fn spawn(store: Arc<dyn Store>, thread_count: u32) -> Self {
    let handles = (0..thread_count)
      .map(|_| WorkerHandle::spawn(Arc::clone(&store)))
      .collect();
    Self { handles }
  }

  /// Submits a request to the given thread and blocks for its response.
  ///
  /// # Panics
  /// Panics if `thread_id` is out of range for this pool — callers are
  /// expected to only submit for thread ids the ring actually assigned
  /// to this node.
  pub fn submit(&self, thread_id: ThreadId, request: Request) -> Response {
    self.handles[thread_id.0 as usize].submit(request)
  }

  /// Asks every worker in the pool to evict cache entries `is_native`
  /// rejects — see `WorkerHandle::evict_non_native`.
  pub fn evict_non_native(&self, is_native: Arc<dyn Fn(DatumId) -> bool + Send + Sync>) {
    for handle in &self.handles {
      handle.evict_non_native(Arc::clone(&is_native));
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use seisin_core::datum::DatumId;
  use seisin_core::store::InMemoryStore;

  #[test]
  fn each_thread_id_indexes_a_distinct_worker() {
    let pool = WorkerPool::spawn(Arc::new(InMemoryStore::new()), 2);
    assert_eq!(
      pool.submit(ThreadId(0), Request::Get { id: DatumId::new() }),
      Response::NotFound
    );
    assert_eq!(
      pool.submit(ThreadId(1), Request::Get { id: DatumId::new() }),
      Response::NotFound
    );
  }

  #[test]
  fn writes_on_one_thread_are_visible_via_the_shared_store_from_another() {
    let pool = WorkerPool::spawn(Arc::new(InMemoryStore::new()), 2);
    let id = DatumId::new();
    pool.submit(
      ThreadId(0),
      Request::Put {
        id,
        content: b"hello".to_vec(),
      },
    );
    match pool.submit(ThreadId(1), Request::Get { id }) {
      Response::Value { content, .. } => assert_eq!(content, b"hello"),
      other => panic!("expected Value, got {other:?}"),
    }
  }
}
