//! The single owning thread for this sub-project's one authority slot.
//! Later sub-projects add a ring of these, one per (node, thread); for
//! now there is exactly one, so every response reports `AuthorityIdx::Native`.

use std::sync::mpsc::{self, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use seisin_core::authority::AuthorityIdx;
use seisin_core::cache::Cache;
use seisin_core::datum::DatumId;
use seisin_core::store::Store;
use seisin_protocol::{Request, Response};

enum WorkerMessage {
  Request(Request, Sender<Response>),
  EvictNonNative(Arc<dyn Fn(DatumId) -> bool + Send + Sync>),
}

pub struct WorkerHandle {
  sender: Sender<WorkerMessage>,
  _join: JoinHandle<()>,
}

impl WorkerHandle {
  pub fn spawn(store: Arc<dyn Store>) -> Self {
    let (sender, receiver) = mpsc::channel::<WorkerMessage>();
    let join = thread::spawn(move || {
      let mut cache = Cache::new(store);
      for message in receiver {
        match message {
          WorkerMessage::Request(request, reply) => {
            let response = handle_request(&mut cache, request);
            let _ = reply.send(response);
          }
          WorkerMessage::EvictNonNative(is_native) => {
            cache.evict_non_native(|id| is_native(id));
          }
        }
      }
    });
    Self {
      sender,
      _join: join,
    }
  }

  /// Submits a request to the owning thread and blocks for its response.
  pub fn submit(&self, request: Request) -> Response {
    let (reply_tx, reply_rx) = mpsc::channel();
    self
      .sender
      .send(WorkerMessage::Request(request, reply_tx))
      .expect("worker thread exited unexpectedly");
    reply_rx.recv().expect("worker dropped the reply channel")
  }

  /// Asks the owning thread to evict any cache entry `is_native` rejects
  /// — fire-and-forget, no reply, but guaranteed to be processed before
  /// any `submit` call made after this one returns (the worker's inbox
  /// is a single ordered queue).
  pub fn evict_non_native(&self, is_native: Arc<dyn Fn(DatumId) -> bool + Send + Sync>) {
    let _ = self.sender.send(WorkerMessage::EvictNonNative(is_native));
  }
}

fn handle_request(cache: &mut Cache, request: Request) -> Response {
  match request {
    Request::Get { id } => match cache.get(id) {
      Some(content) => Response::Value {
        content,
        authority: AuthorityIdx::Native,
      },
      None => Response::NotFound,
    },
    Request::Put { id, content } => {
      cache.put(id, content);
      Response::Ok
    }
    Request::Delete { id } => {
      cache.delete(id);
      Response::Ok
    }
    Request::Op { .. } => Response::OpError {
      message: "Request::Op must be dispatched via run_op, not submit".to_string(),
    },
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use seisin_core::datum::DatumId;
  use seisin_core::store::InMemoryStore;

  #[test]
  fn put_then_get_returns_the_stored_content() {
    let worker = WorkerHandle::spawn(Arc::new(InMemoryStore::new()));
    let id = DatumId::new();

    assert_eq!(
      worker.submit(Request::Put {
        id,
        content: b"hello".to_vec()
      }),
      Response::Ok
    );
    assert_eq!(
      worker.submit(Request::Get { id }),
      Response::Value {
        content: b"hello".to_vec(),
        authority: AuthorityIdx::Native
      }
    );
  }

  #[test]
  fn get_on_missing_datum_returns_not_found() {
    let worker = WorkerHandle::spawn(Arc::new(InMemoryStore::new()));
    assert_eq!(
      worker.submit(Request::Get { id: DatumId::new() }),
      Response::NotFound
    );
  }

  #[test]
  fn delete_then_get_returns_not_found() {
    let worker = WorkerHandle::spawn(Arc::new(InMemoryStore::new()));
    let id = DatumId::new();
    worker.submit(Request::Put {
      id,
      content: b"hello".to_vec(),
    });
    assert_eq!(worker.submit(Request::Delete { id }), Response::Ok);
    assert_eq!(worker.submit(Request::Get { id }), Response::NotFound);
  }

  #[test]
  fn evict_non_native_removes_rejected_entries_before_a_later_submit_sees_them() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let worker = WorkerHandle::spawn(Arc::clone(&store));
    let id = DatumId::new();
    worker.submit(Request::Put {
      id,
      content: b"original".to_vec(),
    });

    // Mutate storage directly (simulating another node's write-through)
    // and evict, then confirm the very next Get reloads the new value.
    store.put(id, b"updated".to_vec());
    worker.evict_non_native(Arc::new(|_| false));

    match worker.submit(Request::Get { id }) {
      Response::Value { content, .. } => assert_eq!(content, b"updated"),
      other => panic!("expected Value, got {other:?}"),
    }
  }
}
