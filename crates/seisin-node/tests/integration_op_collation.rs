use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::InMemoryStore;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{Request, Response};
use seisin_ring::ring::Ring;

/// Encodes a "transfer" op payload as two `u64` balances aren't stored
/// yet (there's no typed layer) — this test's op just moves the literal
/// bytes from one datum to the other, which is enough to prove
/// collation without needing a real typed value format.
fn start_single_node_server() -> String {
  let node_id = NodeId(1);
  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap().to_string();

  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_id, 4)])));
  let address_book = Arc::new(HashMap::new());

  let mut ops = OpRegistry::new();
  ops.register(
    "move_content",
    Box::new(|ctx, ids, _payload| {
      let from = ids[0];
      let to = ids[1];
      let content = ctx.get(from).unwrap_or_default();
      ctx.delete(from);
      ctx.put(to, content.clone());
      content
    }),
  );
  ops.register(
    "put",
    Box::new(|ctx, ids, payload| {
      ctx.put(ids[0], payload.to_vec());
      vec![]
    }),
  );
  ops.register(
    "get",
    Box::new(|ctx, ids, _payload| ctx.get(ids[0]).unwrap_or_default()),
  );

  let peer_link_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    4,
    Arc::new(ops),
    Arc::clone(&ring),
    node_id,
    peer_link_listener,
    Arc::new(HashMap::new()),
    Arc::new(seisin_node::index_handler::IndexHandlerRegistry::new()),
  ));

  thread::spawn(move || serve(listener, node_id, ring, address_book, pool));
  addr
}

#[test]
fn an_op_collates_datums_natively_owned_by_different_local_threads() {
  let addr = start_single_node_server();

  let from = DatumId::new();
  let to = DatumId::new();

  seisin_client::call(
    &addr,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "put".to_string(),
      datum_ids: vec![from],
      payload: b"payload".to_vec(),
    },
  )
  .unwrap();

  let response = seisin_client::call(
    &addr,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "move_content".to_string(),
      datum_ids: vec![from, to],
      payload: vec![],
    },
  )
  .unwrap();
  assert_eq!(
    response,
    Response::OpResult {
      payload: b"payload".to_vec()
    }
  );

  // The source datum was deleted by the op; the destination now holds
  // the moved content — both writes are durable (write-through-before-
  // ack already proven in earlier sub-projects; this confirms the op
  // path exercises the same Cache/Store underneath).
  assert_eq!(
    seisin_client::call(
      &addr,
      Request::Op {
        op_id: DatumId::new(),
        op_name: "get".to_string(),
        datum_ids: vec![from],
        payload: vec![],
      }
    )
    .unwrap(),
    Response::OpResult { payload: vec![] }
  );
  match seisin_client::call(
    &addr,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "get".to_string(),
      datum_ids: vec![to],
      payload: vec![],
    },
  )
  .unwrap()
  {
    Response::OpResult { payload } => assert_eq!(payload, b"payload"),
    other => panic!("expected OpResult, got {other:?}"),
  }
}

#[test]
fn an_unknown_op_name_returns_an_op_error() {
  let addr = start_single_node_server();
  let response = seisin_client::call(
    &addr,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "does_not_exist".to_string(),
      datum_ids: vec![],
      payload: vec![],
    },
  )
  .unwrap();
  match response {
    Response::OpError { message } => assert!(message.contains("unknown op")),
    other => panic!("expected OpError, got {other:?}"),
  }
}
