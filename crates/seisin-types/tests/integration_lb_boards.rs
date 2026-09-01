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

// Matches lb_cache.rs's private LB_REPLICATION constant — kept in sync
// by hand since that constant isn't part of the crate's public surface.
const LB_REPLICATION: u16 = 2;

fn racing_class() -> LbClassDef {
  LbClassDef {
    name: "racing".to_string(),
    score_type: LbScoreType::I64,
    display_len: 32,
    rule: LbRule::Max,
  }
}

fn generous_config(_board_id: DatumId) -> LbCacheConfig {
  LbCacheConfig {
    pinned_top: 50,
    pinned_bottom: 50,
    max_cached_entries: 500,
  }
}

/// Boots one storage node on a tempdir and returns its address (and the
/// tempdir, kept alive for the caller).
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

/// Boots a real compute node backed by `LB_REPLICATION` real storage
/// nodes — the lb board data now lives in the storage tier, not on this
/// compute node's local disk.
fn start_node() -> (String, Vec<tempfile::TempDir>) {
  let mut index_kinds = IndexKindRegistry::new();
  register_lb_class(&mut index_kinds, racing_class(), generous_config);

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
  let (addr, _storage_dirs) = start_node();
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
