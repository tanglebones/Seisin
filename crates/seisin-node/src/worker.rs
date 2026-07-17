//! The single owning thread for this sub-project's one authority slot.
//! Later sub-projects add a ring of these, one per (node, thread); for
//! now there is exactly one, so every response reports `AuthorityIdx::Native`.

use std::sync::mpsc::{self, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use seisin_core::authority::AuthorityIdx;
use seisin_core::cache::Cache;
use seisin_core::store::Store;
use seisin_protocol::{Request, Response};

pub struct WorkerHandle {
    sender: Sender<(Request, Sender<Response>)>,
    _join: JoinHandle<()>,
}

impl WorkerHandle {
    pub fn spawn(store: Arc<dyn Store>) -> Self {
        let (sender, receiver) = mpsc::channel::<(Request, Sender<Response>)>();
        let join = thread::spawn(move || {
            let mut cache = Cache::new(store);
            for (request, reply) in receiver {
                let response = handle_request(&mut cache, request);
                let _ = reply.send(response);
            }
        });
        Self { sender, _join: join }
    }

    /// Submits a request to the owning thread and blocks for its response.
    pub fn submit(&self, request: Request) -> Response {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender
            .send((request, reply_tx))
            .expect("worker thread exited unexpectedly");
        reply_rx.recv().expect("worker dropped the reply channel")
    }
}

fn handle_request(cache: &mut Cache, request: Request) -> Response {
    match request {
        Request::Get { id } => match cache.get(id) {
            Some(content) => Response::Value { content, authority: AuthorityIdx::Native },
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

        assert_eq!(worker.submit(Request::Put { id, content: b"hello".to_vec() }), Response::Ok);
        assert_eq!(
            worker.submit(Request::Get { id }),
            Response::Value { content: b"hello".to_vec(), authority: AuthorityIdx::Native }
        );
    }

    #[test]
    fn get_on_missing_datum_returns_not_found() {
        let worker = WorkerHandle::spawn(Arc::new(InMemoryStore::new()));
        assert_eq!(worker.submit(Request::Get { id: DatumId::new() }), Response::NotFound);
    }

    #[test]
    fn delete_then_get_returns_not_found() {
        let worker = WorkerHandle::spawn(Arc::new(InMemoryStore::new()));
        let id = DatumId::new();
        worker.submit(Request::Put { id, content: b"hello".to_vec() });
        assert_eq!(worker.submit(Request::Delete { id }), Response::Ok);
        assert_eq!(worker.submit(Request::Get { id }), Response::NotFound);
    }
}
