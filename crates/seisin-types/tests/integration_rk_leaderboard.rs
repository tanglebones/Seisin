use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::InMemoryStore;
use seisin_node::index_handler::IndexKindRegistry;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ops::context::OpContext;
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{Request, Response, RkQueryKind};
use seisin_ring::ring::Ring;
use seisin_types::field::{FieldType, FieldValue};
use seisin_types::rk_index::rk_key;
use seisin_types::rk_kind::register_rk_index_kind;
use seisin_types::schema::{DatumTypeDef, IndexDef};
use seisin_types::typed_context::TypedOpContext;
use seisin_types::{decode_datum, encode_datum};

fn player_type() -> DatumTypeDef {
  DatumTypeDef::new("player")
    .field("score", FieldType::I64)
    .index(IndexDef::Rk {
      field: "score".to_string(),
    })
}

fn start_node(data_dir: std::path::PathBuf) -> String {
  let def = player_type();
  let mut ops = OpRegistry::new();
  let write_def = def.clone();
  ops.register(
    "write_player",
    Box::new(move |ctx: &mut OpContext, ids, payload| {
      let values = decode_datum(&write_def, payload).unwrap();
      let mut tctx = TypedOpContext::new(ctx);
      tctx.get(ids[0], &write_def).unwrap();
      tctx.set(ids[0], &write_def, values).unwrap();
      vec![]
    }),
  );

  let mut index_kinds = IndexKindRegistry::new();
  register_rk_index_kind(&mut index_kinds, data_dir);

  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  let node_id = NodeId(1);
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_id, 2)])));
  let peer_link_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(ops),
    Arc::clone(&ring),
    node_id,
    peer_link_listener,
    Arc::new(std::collections::HashMap::new()),
    Arc::new(index_kinds),
  ));
  let address_book = Arc::new(std::collections::HashMap::new());
  thread::spawn(move || serve(listener, node_id, ring, address_book, pool));
  thread::sleep(std::time::Duration::from_millis(100));
  addr
}

fn write_score(addr: &str, def: &DatumTypeDef, pk: DatumId, score: i64) {
  let response = seisin_client::call(
    addr,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "write_player".to_string(),
      datum_ids: vec![pk],
      payload: encode_datum(def, &[FieldValue::I64(score)]).unwrap(),
    },
  )
  .unwrap();
  assert_eq!(response, Response::OpResult { payload: vec![] });
}

fn rk_query(addr: &str, query: RkQueryKind) -> Vec<(Vec<u8>, DatumId)> {
  let response = seisin_client::call(
    addr,
    Request::RkQuery {
      index_datum_id: rk_key("player", "score"),
      query,
    },
  )
  .unwrap();
  match response {
    Response::RkQueryResult { entries } => entries,
    other => panic!("expected RkQueryResult, got {other:?}"),
  }
}

#[test]
fn writes_maintain_the_leaderboard_and_queries_answer_over_the_wire() {
  let data_dir = tempfile::tempdir().unwrap();
  let addr = start_node(data_dir.path().to_path_buf());
  let def = player_type();

  let players: Vec<DatumId> = (0..5).map(|_| DatumId::new()).collect();
  for (i, pk) in players.iter().enumerate() {
    write_score(&addr, &def, *pk, (i as i64 + 1) * 100); // 100..500
  }

  let top = rk_query(&addr, RkQueryKind::TopN(2));
  assert_eq!(
    top.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
    vec![players[4], players[3]] // 500, then 400
  );

  let bottom = rk_query(&addr, RkQueryKind::BottomN(2));
  assert_eq!(
    bottom.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
    vec![players[0], players[1]] // 100, then 200
  );

  // A score update moves the entry: player 0 jumps to the top.
  write_score(&addr, &def, players[0], 900);
  let top = rk_query(&addr, RkQueryKind::TopN(1));
  assert_eq!(top[0].1, players[0]);

  // Still exactly 5 entries (moved, not duplicated).
  let all = rk_query(&addr, RkQueryKind::TopN(100));
  assert_eq!(all.len(), 5);

  let sample = rk_query(&addr, RkQueryKind::PercentileSample(3));
  assert_eq!(sample.len(), 3);
}
