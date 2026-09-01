use std::collections::{HashMap, HashSet};
use std::net::TcpListener;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::InMemoryStore;
use seisin_node::collection_store::RemoteCollectionStore;
use seisin_node::gossip_state::ClusterState;
use seisin_node::index_handler::IndexKindRegistry;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_node::store_server::{serve_store, StoreNode};
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{LbExecuteOp, LbQueryReq, LbResult, Request, Response};
use seisin_ring::ring::Ring;
use seisin_types::field::FieldValue;
use seisin_types::lb::{encode_score, lb_board_key, LbClassDef, LbRule, LbScoreType};
use seisin_types::lb_cache::LbCacheConfig;
use seisin_types::lb_kind::register_lb_class;

// Matches lb_cache.rs's private LB_REPLICATION constant.
const LB_REPLICATION: u16 = 2;

fn racing_class() -> LbClassDef {
  LbClassDef {
    name: "racing".to_string(),
    score_type: LbScoreType::I64,
    display_len: 32,
    rule: LbRule::Max,
  }
}

fn start_storage(node_id: NodeId) -> (String, tempfile::TempDir) {
  let dir = tempfile::tempdir().unwrap();
  let log = Arc::new(Mutex::new(
    seisin_storage::datum_log::DatumLog::open(&dir.path().join("datum_log.dlog")).unwrap(),
  ));
  let node = Arc::new(StoreNode {
    log,
    node_id,
    heartbeat: Arc::new(seisin_node::heartbeat::Heartbeat::new()),
    self_halt_threshold: std::time::Duration::from_secs(3600),
    transfers: Arc::new(seisin_node::transfer::TransferManager::default()),
    data_dir: dir.path().to_path_buf(),
    collections: Mutex::new(HashMap::new()),
  });
  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  thread::spawn(move || serve_store(listener, node));
  (addr, dir)
}

/// Boots a real compute node backed by real storage nodes, with a
/// caller-supplied cache-config resolver — the whole point of this
/// test file is to exercise cache sizes small enough to force eviction.
fn start_node(
  cache_config: impl Fn(DatumId) -> LbCacheConfig + Send + Sync + 'static,
) -> (String, Vec<tempfile::TempDir>) {
  let mut index_kinds = IndexKindRegistry::new();
  register_lb_class(&mut index_kinds, racing_class(), cache_config);

  let mut store_addresses = HashMap::new();
  let mut storage_alive = HashSet::new();
  let mut storage_dirs = Vec::new();
  let mut storage_members = Vec::new();
  for i in 0..LB_REPLICATION {
    let node_id = NodeId(100 + i as u64);
    let (addr, dir) = start_storage(node_id);
    store_addresses.insert(node_id, addr);
    storage_alive.insert(node_id);
    storage_dirs.push(dir);
    storage_members.push((node_id, 1u32));
  }

  let compute_node_id = NodeId(1);
  let compute_ring = Arc::new(RwLock::new(Ring::from_members(&[(compute_node_id, 2)])));
  let storage_ring = Arc::new(RwLock::new(Ring::from_members(&storage_members)));
  let cluster = Arc::new(ClusterState {
    compute_ring: Arc::clone(&compute_ring),
    storage_ring,
    store_addresses: Arc::new(RwLock::new(store_addresses)),
    identity_book: Arc::new(RwLock::new(HashMap::new())),
    storage_alive: Arc::new(RwLock::new(storage_alive)),
    storage_stale: Arc::new(RwLock::new(HashSet::new())),
    halt: Arc::new(seisin_node::halt::HaltState::new()),
  });

  index_kinds.attach_collection_store(Arc::new(RemoteCollectionStore::new(Arc::clone(&cluster))));

  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  let peer_link_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(OpRegistry::new()),
    Arc::clone(&compute_ring),
    compute_node_id,
    peer_link_listener,
    Arc::new(HashMap::new()),
    Arc::new(index_kinds),
  ));
  let address_book = Arc::new(HashMap::new());
  thread::spawn(move || serve(listener, compute_node_id, cluster, address_book, pool));
  thread::sleep(std::time::Duration::from_millis(100));
  (addr, storage_dirs)
}

fn submit(addr: &str, board_id: DatumId, player: DatumId, display: &str, score: i64) -> LbResult {
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
        friend_ids: vec![],
        top: 0,
        window: 0,
      },
    },
  )
  .unwrap();
  match response {
    Response::LbResult(result) => result,
    other => panic!("expected LbResult, got {other:?}"),
  }
}

fn query_top(addr: &str, board_id: DatumId, top: u32) -> LbResult {
  let response = seisin_client::call(
    addr,
    Request::LbQuery {
      board_id,
      class: "racing".to_string(),
      query: LbQueryReq {
        top,
        bottom: 0,
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

fn query_around(addr: &str, board_id: DatumId, player: DatumId, window: u32) -> LbResult {
  let response = seisin_client::call(
    addr,
    Request::LbQuery {
      board_id,
      class: "racing".to_string(),
      query: LbQueryReq {
        top: 0,
        bottom: 0,
        around_player: Some(player),
        window,
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
fn pinned_windows_survive_a_board_larger_than_the_cache() {
  let (addr, _dirs) = start_node(|_board_id| LbCacheConfig {
    pinned_top: 3,
    pinned_bottom: 3,
    max_cached_entries: 10, // leaves room for a 4-entry LRU
  });
  let board = lb_board_key("racing", "big", "default");

  let players: Vec<DatumId> = (0..20).map(|_| DatumId::new()).collect();
  for (i, player) in players.iter().enumerate() {
    submit(&addr, board, *player, &format!("p{i}"), i as i64);
  }

  let result = query_top(&addr, board, 3);
  assert_eq!(result.total, 20);
  let top_names: Vec<&[u8]> = result.top.iter().map(|e| e.display.as_slice()).collect();
  // Highest scores are players 19, 18, 17 (score == index).
  assert_eq!(
    top_names,
    vec![b"p19".as_slice(), b"p18".as_slice(), b"p17".as_slice()]
  );
}

#[test]
fn a_middle_player_not_in_either_pinned_window_still_resolves() {
  let (addr, _dirs) = start_node(|_board_id| LbCacheConfig {
    pinned_top: 2,
    pinned_bottom: 2,
    max_cached_entries: 6,
  });
  let board = lb_board_key("racing", "big", "default");

  let players: Vec<DatumId> = (0..10).map(|_| DatumId::new()).collect();
  for (i, player) in players.iter().enumerate() {
    submit(&addr, board, *player, &format!("p{i}"), i as i64);
  }

  // Player 5 (score 5) is in neither pinned-top (scores 9,8) nor
  // pinned-bottom (scores 0,1) — this must still resolve correctly,
  // proving the point/around-player storage round trip, not just the
  // pinned windows, is exercised.
  let around = query_around(&addr, board, players[5], 3);
  assert_eq!(around.player_rank, Some(4)); // best-first: rank 0 = score 9, so score 5 is rank 4
  assert!(around.around.iter().any(|e| e.display == b"p5".to_vec()));
}
