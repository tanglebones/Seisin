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
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{Request, Response};

enum WorkerMessage {
  Request(Request, Sender<Response>),
  EvictNonNative(Arc<dyn Fn(DatumId) -> bool + Send + Sync>),
  Evict(DatumId),
  RunOp {
    op_name: String,
    datum_ids: Vec<DatumId>,
    payload: Vec<u8>,
    reply: Sender<Result<Vec<u8>, String>>,
  },
}

pub struct WorkerHandle {
  sender: Sender<WorkerMessage>,
  _join: JoinHandle<()>,
}

impl WorkerHandle {
  pub fn spawn(store: Arc<dyn Store>, ops: Arc<OpRegistry>) -> Self {
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
          WorkerMessage::Evict(id) => {
            cache.invalidate(id);
          }
          WorkerMessage::RunOp {
            op_name,
            datum_ids,
            payload,
            reply,
          } => {
            let mut ctx = seisin_ops::context::OpContext::new(&mut cache);
            let result = ops.invoke(&op_name, &mut ctx, &datum_ids, &payload);
            let _ = reply.send(result);
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

  /// Asks the owning thread to evict a single cache entry — fire-and-
  /// forget, same ordering guarantee as `evict_non_native`.
  pub fn evict(&self, id: DatumId) {
    let _ = self.sender.send(WorkerMessage::Evict(id));
  }

  /// Runs a registered op on the owning thread and blocks for its result.
  pub fn run_op(
    &self,
    op_name: String,
    datum_ids: Vec<DatumId>,
    payload: Vec<u8>,
  ) -> Result<Vec<u8>, String> {
    let (reply_tx, reply_rx) = mpsc::channel();
    self
      .sender
      .send(WorkerMessage::RunOp {
        op_name,
        datum_ids,
        payload,
        reply: reply_tx,
      })
      .expect("worker thread exited unexpectedly");
    reply_rx.recv().expect("worker dropped the reply channel")
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
    let worker = WorkerHandle::spawn(Arc::new(InMemoryStore::new()), Arc::new(OpRegistry::new()));
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
    let worker = WorkerHandle::spawn(Arc::new(InMemoryStore::new()), Arc::new(OpRegistry::new()));
    assert_eq!(
      worker.submit(Request::Get { id: DatumId::new() }),
      Response::NotFound
    );
  }

  #[test]
  fn delete_then_get_returns_not_found() {
    let worker = WorkerHandle::spawn(Arc::new(InMemoryStore::new()), Arc::new(OpRegistry::new()));
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
    let worker = WorkerHandle::spawn(Arc::clone(&store), Arc::new(OpRegistry::new()));
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

  #[test]
  fn evict_removes_one_entry_without_touching_others() {
    let worker = WorkerHandle::spawn(Arc::new(InMemoryStore::new()), Arc::new(OpRegistry::new()));
    let kept = DatumId::new();
    let evicted = DatumId::new();
    worker.submit(Request::Put {
      id: kept,
      content: b"kept".to_vec(),
    });
    worker.submit(Request::Put {
      id: evicted,
      content: b"evicted".to_vec(),
    });

    worker.evict(evicted);

    match worker.submit(Request::Get { id: kept }) {
      Response::Value { content, .. } => assert_eq!(content, b"kept"),
      other => panic!("expected Value, got {other:?}"),
    }
  }

  #[test]
  fn run_op_invokes_the_registered_handler() {
    let mut ops = OpRegistry::new();
    ops.register(
      "put_first",
      Box::new(|ctx, ids, payload| {
        ctx.put(ids[0], payload.to_vec());
        payload.to_vec()
      }),
    );
    let worker = WorkerHandle::spawn(Arc::new(InMemoryStore::new()), Arc::new(ops));
    let id = DatumId::new();
    let result = worker.run_op("put_first".to_string(), vec![id], b"hi".to_vec());
    assert_eq!(result, Ok(b"hi".to_vec()));

    match worker.submit(Request::Get { id }) {
      Response::Value { content, .. } => assert_eq!(content, b"hi"),
      other => panic!("expected Value, got {other:?}"),
    }
  }

  #[test]
  fn run_op_on_unknown_name_returns_an_error() {
    let worker = WorkerHandle::spawn(Arc::new(InMemoryStore::new()), Arc::new(OpRegistry::new()));
    assert!(worker.run_op("nope".to_string(), vec![], vec![]).is_err());
  }
}
