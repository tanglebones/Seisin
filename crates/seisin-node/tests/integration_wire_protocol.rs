use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use seisin_core::authority::{AuthorityIdx, NodeId};
use seisin_core::datum::DatumId;
use seisin_core::sk::{decode_sk_entries, encode_sk_entries};
use seisin_core::store::InMemoryStore;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_protocol::{
  decode_response, encode_request, read_frame, write_frame, Request, Response,
};
use seisin_ring::ring::Ring;

fn start_test_server() -> SocketAddr {
  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap();
  let node_id = NodeId(1);
  let ring = Arc::new(std::sync::RwLock::new(Ring::from_members(&[(node_id, 1)])));
  let address_book = Arc::new(HashMap::new());
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    1,
    Arc::new(seisin_ops::registry::OpRegistry::new()),
  ));
  thread::spawn(move || serve(listener, node_id, ring, address_book, pool));
  addr
}

fn round_trip(stream: &mut TcpStream, request: Request) -> Response {
  write_frame(stream, &encode_request(&request)).unwrap();
  let payload = read_frame(stream).unwrap();
  decode_response(&payload).unwrap()
}

#[test]
fn put_then_get_returns_stored_content() {
  let addr = start_test_server();
  let mut stream = TcpStream::connect(addr).unwrap();
  let id = DatumId::new();

  assert_eq!(
    round_trip(
      &mut stream,
      Request::Put {
        id,
        content: b"hello".to_vec()
      }
    ),
    Response::Ok
  );
  assert_eq!(
    round_trip(&mut stream, Request::Get { id }),
    Response::Value {
      content: b"hello".to_vec(),
      authority: AuthorityIdx::Native
    }
  );
}

#[test]
fn get_on_missing_datum_returns_not_found() {
  let addr = start_test_server();
  let mut stream = TcpStream::connect(addr).unwrap();
  assert_eq!(
    round_trip(&mut stream, Request::Get { id: DatumId::new() }),
    Response::NotFound
  );
}

#[test]
fn delete_removes_datum() {
  let addr = start_test_server();
  let mut stream = TcpStream::connect(addr).unwrap();
  let id = DatumId::new();

  round_trip(
    &mut stream,
    Request::Put {
      id,
      content: b"data".to_vec(),
    },
  );
  assert_eq!(
    round_trip(&mut stream, Request::Delete { id }),
    Response::Ok
  );
  assert_eq!(
    round_trip(&mut stream, Request::Get { id }),
    Response::NotFound
  );
}

#[test]
fn secondary_key_datum_round_trips_as_a_regular_datum() {
  let addr = start_test_server();
  let mut stream = TcpStream::connect(addr).unwrap();
  let sk_id = DatumId::new();
  let entries = vec![(DatumId::new(), AuthorityIdx::Native)];
  let content = encode_sk_entries(&entries);

  round_trip(
    &mut stream,
    Request::Put {
      id: sk_id,
      content: content.clone(),
    },
  );
  match round_trip(&mut stream, Request::Get { id: sk_id }) {
    Response::Value { content: got, .. } => assert_eq!(decode_sk_entries(&got).unwrap(), entries),
    other => panic!("expected Value, got {other:?}"),
  }
}
