use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::sk::{decode_sk_entries, encode_sk_entries};
use seisin_core::store::InMemoryStore;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{
  decode_response, encode_request, read_frame, write_frame, Request, Response,
};
use seisin_ring::ring::Ring;

/// A not-found sentinel this test's `get` op returns when the datum is
/// absent, since `ctx.get` returns `Option` but an op's return type is a
/// plain `Vec<u8>` — the framework itself never interprets an op's
/// payload, so this convention is entirely test-local.
const NOT_FOUND_SENTINEL: &[u8] = b"__NOT_FOUND__";

fn build_registry() -> OpRegistry {
  let mut ops = OpRegistry::new();
  ops.register(
    "put",
    Box::new(|ctx, ids, payload| {
      ctx.put(ids[0], payload.to_vec());
      vec![]
    }),
  );
  ops.register(
    "get",
    Box::new(|ctx, ids, _payload| {
      ctx
        .get(ids[0])
        .unwrap_or_else(|| NOT_FOUND_SENTINEL.to_vec())
    }),
  );
  ops.register(
    "delete",
    Box::new(|ctx, ids, _payload| {
      ctx.delete(ids[0]);
      vec![]
    }),
  );
  ops
}

fn start_test_server() -> SocketAddr {
  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap();
  let node_id = NodeId(1);
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_id, 1)])));
  let address_book = Arc::new(HashMap::new());
  let peer_link_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    1,
    Arc::new(build_registry()),
    Arc::clone(&ring),
    node_id,
    peer_link_listener,
    Arc::new(HashMap::new()),
  ));
  thread::spawn(move || serve(listener, node_id, ring, address_book, pool));
  addr
}

fn round_trip(stream: &mut TcpStream, request: Request) -> Response {
  write_frame(stream, &encode_request(&request)).unwrap();
  let payload = read_frame(stream).unwrap();
  decode_response(&payload).unwrap()
}

fn op_result(response: Response) -> Vec<u8> {
  match response {
    Response::OpResult { payload } => payload,
    other => panic!("expected OpResult, got {other:?}"),
  }
}

#[test]
fn put_then_get_returns_stored_content() {
  let addr = start_test_server();
  let mut stream = TcpStream::connect(addr).unwrap();
  let id = DatumId::new();

  assert_eq!(
    op_result(round_trip(
      &mut stream,
      Request::Op {
        op_id: DatumId::new(),
        op_name: "put".to_string(),
        datum_ids: vec![id],
        payload: b"hello".to_vec(),
      }
    )),
    Vec::<u8>::new()
  );
  assert_eq!(
    op_result(round_trip(
      &mut stream,
      Request::Op {
        op_id: DatumId::new(),
        op_name: "get".to_string(),
        datum_ids: vec![id],
        payload: vec![],
      }
    )),
    b"hello".to_vec()
  );
}

#[test]
fn get_on_missing_datum_returns_the_not_found_sentinel() {
  let addr = start_test_server();
  let mut stream = TcpStream::connect(addr).unwrap();
  assert_eq!(
    op_result(round_trip(
      &mut stream,
      Request::Op {
        op_id: DatumId::new(),
        op_name: "get".to_string(),
        datum_ids: vec![DatumId::new()],
        payload: vec![],
      }
    )),
    NOT_FOUND_SENTINEL.to_vec()
  );
}

#[test]
fn delete_removes_datum() {
  let addr = start_test_server();
  let mut stream = TcpStream::connect(addr).unwrap();
  let id = DatumId::new();

  round_trip(
    &mut stream,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "put".to_string(),
      datum_ids: vec![id],
      payload: b"data".to_vec(),
    },
  );
  round_trip(
    &mut stream,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "delete".to_string(),
      datum_ids: vec![id],
      payload: vec![],
    },
  );
  assert_eq!(
    op_result(round_trip(
      &mut stream,
      Request::Op {
        op_id: DatumId::new(),
        op_name: "get".to_string(),
        datum_ids: vec![id],
        payload: vec![],
      }
    )),
    NOT_FOUND_SENTINEL.to_vec()
  );
}

#[test]
fn secondary_key_datum_round_trips_as_a_regular_datum() {
  let addr = start_test_server();
  let mut stream = TcpStream::connect(addr).unwrap();
  let sk_id = DatumId::new();
  let entries = vec![(DatumId::new(), seisin_core::authority::AuthorityIdx::Native)];
  let content = encode_sk_entries(&entries);

  round_trip(
    &mut stream,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "put".to_string(),
      datum_ids: vec![sk_id],
      payload: content,
    },
  );
  let got = op_result(round_trip(
    &mut stream,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "get".to_string(),
      datum_ids: vec![sk_id],
      payload: vec![],
    },
  ));
  assert_eq!(decode_sk_entries(&got).unwrap(), entries);
}
