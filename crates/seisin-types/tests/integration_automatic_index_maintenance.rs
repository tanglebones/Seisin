use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::InMemoryStore;
use seisin_node::index_handler::IndexHandlerRegistry;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ops::context::OpContext;
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{Request, Response};
use seisin_ring::ring::Ring;
use seisin_types::field::{FieldType, FieldValue};
use seisin_types::schema::{ConflictOp, DatumTypeDef, IndexDef};
use seisin_types::sk_index::register_sk_index_handler;
use seisin_types::typed_context::TypedOpContext;
use seisin_types::{decode_datum, encode_datum};

fn user_type() -> DatumTypeDef {
  DatumTypeDef::new("user")
    .field("email", FieldType::String)
    .index(IndexDef::Sk {
      field: "email".to_string(),
      unique: Some(ConflictOp("resolve_duplicate_email".to_string())),
    })
}

fn start_node() -> String {
  let def = user_type();
  let mut ops = OpRegistry::new();
  let write_def = def.clone();
  ops.register(
    "write_user",
    Box::new(move |ctx: &mut OpContext, ids, payload| {
      let values = decode_datum(&write_def, payload).unwrap();
      let mut tctx = TypedOpContext::new(ctx);
      tctx.get(ids[0], &write_def);
      tctx.set(ids[0], &write_def, values);
      vec![]
    }),
  );

  let mut index_handlers = IndexHandlerRegistry::new();
  register_sk_index_handler(&mut index_handlers);

  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  let node_id = NodeId(1);
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_id, 1)])));
  let peer_link_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    1,
    Arc::new(ops),
    Arc::clone(&ring),
    node_id,
    peer_link_listener,
    Arc::new(std::collections::HashMap::new()),
    Arc::new(index_handlers),
  ));
  let address_book = Arc::new(std::collections::HashMap::new());
  thread::spawn(move || serve(listener, node_id, ring, address_book, pool));
  thread::sleep(std::time::Duration::from_millis(100));
  addr
}

#[test]
fn a_second_write_of_the_same_unique_value_fails_the_whole_op_automatically() {
  let addr = start_node();
  let def = user_type();

  let first_pk = DatumId::new();
  let values = vec![FieldValue::String("a@example.com".to_string())];
  let first_response = seisin_client::call(
    &addr,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "write_user".to_string(),
      datum_ids: vec![first_pk],
      payload: encode_datum(&def, &values).unwrap(),
    },
  )
  .unwrap();
  assert_eq!(first_response, Response::OpResult { payload: vec![] });

  let second_pk = DatumId::new();
  let second_response = seisin_client::call(
    &addr,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "write_user".to_string(),
      datum_ids: vec![second_pk],
      payload: encode_datum(&def, &values).unwrap(),
    },
  )
  .unwrap();
  match second_response {
    Response::OpError { message } => assert_eq!(message, "resolve_duplicate_email"),
    other => panic!("expected OpError, got {other:?}"),
  }
}
