use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::InMemoryStore;
use seisin_node::index_handler::IndexKindRegistry;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{LbExecuteOp, LbQueryReq, LbResult, Request, Response};
use seisin_ring::ring::Ring;
use seisin_types::field::FieldValue;
use seisin_types::lb::{encode_score, lb_board_key, LbClassDef, LbRule, LbScoreType};
use seisin_types::lb_kind::register_lb_class;

fn racing_class() -> LbClassDef {
  LbClassDef {
    name: "racing".to_string(),
    score_type: LbScoreType::I64,
    display_len: 32,
    rule: LbRule::Max,
  }
}

fn start_node(data_dir: std::path::PathBuf) -> String {
  let mut index_kinds = IndexKindRegistry::new();
  register_lb_class(&mut index_kinds, racing_class(), data_dir);

  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  let node_id = NodeId(1);
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_id, 2)])));
  let peer_link_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(OpRegistry::new()),
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

fn submit(
  addr: &str,
  board_id: DatumId,
  player: DatumId,
  display: &str,
  score: i64,
  friends: Vec<DatumId>,
) -> LbResult {
  let rank_key = encode_score(&racing_class(), &FieldValue::I64(score)).unwrap();
  let response = seisin_client::call(
    addr,
    Request::LbExecute {
      board_id,
      class: "racing".to_string(),
      op: LbExecuteOp::Update {
        player_id: player,
        display: display.as_bytes().to_vec(),
        rank_key,
        friend_ids: friends,
        top: 5,
        window: 3,
      },
    },
  )
  .unwrap();
  match response {
    Response::LbResult(result) => result,
    other => panic!("expected LbResult, got {other:?}"),
  }
}

fn query(addr: &str, board_id: DatumId, bottom: u32) -> LbResult {
  let response = seisin_client::call(
    addr,
    Request::LbQuery {
      board_id,
      class: "racing".to_string(),
      query: LbQueryReq {
        top: 5,
        bottom,
        around_player: None,
        window: 0,
        friend_ids: vec![],
      },
    },
  )
  .unwrap();
  match response {
    Response::LbResult(result) => result,
    other => panic!("expected LbResult, got {other:?}"),
  }
}

#[test]
fn boards_update_query_and_stay_independent_over_the_wire() {
  let data_dir = tempfile::tempdir().unwrap();
  let addr = start_node(data_dir.path().to_path_buf());
  let desert = lb_board_key("racing", "season1", "desert");
  let ice = lb_board_key("racing", "season1", "ice");

  let (alice, bob, carol) = (DatumId::new(), DatumId::new(), DatumId::new());

  submit(&addr, desert, alice, "Alice", 100, vec![]);
  submit(&addr, desert, bob, "Bob", 300, vec![]);
  let result = submit(&addr, desert, carol, "Carol", 200, vec![alice, bob]);

  assert_eq!(result.total, 3);
  assert_eq!(result.player_rank, Some(1));
  let top: Vec<&[u8]> = result.top.iter().map(|e| e.display.as_slice()).collect();
  assert_eq!(
    top,
    vec![b"Bob".as_slice(), b"Carol".as_slice(), b"Alice".as_slice()]
  );
  assert_eq!(result.friends.len(), 2);

  // Max rule over the wire: a worse score changes nothing.
  let result = submit(&addr, desert, bob, "Bob", 50, vec![]);
  assert_eq!(result.player_rank, Some(0));
  assert_eq!(result.total, 3);

  // Same players, different area config: an independent board.
  let result = submit(&addr, ice, alice, "Alice", 999, vec![bob]);
  assert_eq!(result.total, 1);
  assert_eq!(result.player_rank, Some(0));
  assert!(result.friends.is_empty()); // bob has no ice score

  // Read-only query with a bottom list.
  let result = query(&addr, desert, 2);
  assert_eq!(result.total, 3);
  assert_eq!(result.bottom.len(), 2);
  assert_eq!(result.bottom[0].display, b"Alice".to_vec()); // worst first

  // Removal over the wire.
  let response = seisin_client::call(
    &addr,
    Request::LbExecute {
      board_id: desert,
      class: "racing".to_string(),
      op: LbExecuteOp::Remove { player_id: alice },
    },
  )
  .unwrap();
  match response {
    Response::LbResult(result) => assert_eq!(result.total, 2),
    other => panic!("expected LbResult, got {other:?}"),
  }
}
