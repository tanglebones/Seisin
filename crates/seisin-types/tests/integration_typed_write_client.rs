use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::InMemoryStore;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ops::context::OpContext;
use seisin_ops::registry::OpRegistry;
use seisin_ring::ring::Ring;
use seisin_types::field::FieldType;
use seisin_types::field::FieldValue;
use seisin_types::schema::{ConflictOp, DatumTypeDef, IndexDef};
use seisin_types::typed_write::{encode_write_result, write_typed_datum};
use seisin_types::decode_datum;

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

  ops.register(
    "read_user",
    Box::new(move |ctx: &mut OpContext, ids, _payload| ctx.get(ids[0]).unwrap_or_default()),
  );

  let write_def = def.clone();
  ops.register(
    "write_user",
    Box::new(move |ctx: &mut OpContext, ids, payload| {
      let values = decode_datum(&write_def, payload).unwrap();
      let result = write_typed_datum(ctx, &write_def, ids[0], &values).unwrap();
      encode_write_result(&result)
    }),
  );

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
  ));
  let address_book = Arc::new(std::collections::HashMap::new());
  thread::spawn(move || serve(listener, node_id, ring, address_book, pool));
  thread::sleep(std::time::Duration::from_millis(100));
  addr
}

#[test]
fn a_second_write_of_the_same_unique_value_is_reported_for_follow_up() {
  let addr = start_node();
  let def = user_type();

  let first_pk = DatumId::new();
  let second_pk = DatumId::new();
  let values = vec![FieldValue::String("a@example.com".to_string())];

  let first = seisin_types::client::write_typed_datum_client(
    &addr,
    "read_user",
    "write_user",
    &def,
    first_pk,
    &values,
  )
  .unwrap();
  assert_eq!(first, None, "the first writer must not see a violation");

  let second = seisin_types::client::write_typed_datum_client(
    &addr,
    "read_user",
    "write_user",
    &def,
    second_pk,
    &values,
  )
  .unwrap();
  let (conflict_op, _sk_key) = second.expect("the second writer must see a violation");
  assert_eq!(conflict_op, "resolve_duplicate_email");
}
