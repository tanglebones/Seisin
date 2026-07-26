use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_node::index_handler::IndexKindRegistry;
use seisin_node::pool::WorkerPool;
use seisin_node::remote_store::RemoteStore;
use seisin_node::store_server::serve_store;
use seisin_ops::registry::OpRegistry;
use seisin_ring::ring::Ring;
use seisin_storage::datum_log::DatumLog;

/// Boots a storage "node" (store listener over a delta log in `dir`)
/// and returns its address.
fn start_storage(dir: &std::path::Path) -> String {
  let log = Arc::new(Mutex::new(
    DatumLog::open(&dir.join("datum_log.dlog")).unwrap(),
  ));
  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  thread::spawn(move || serve_store(listener, log));
  addr
}

fn remote_store(storage_addr: &str) -> Arc<RemoteStore> {
  let storage_ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(100), 1)])));
  let mut addresses = HashMap::new();
  addresses.insert(NodeId(100), storage_addr.to_string());
  Arc::new(RemoteStore::new(
    storage_ring,
    Arc::new(RwLock::new(addresses)),
  ))
}

/// A compute pool backed by the given store, with byte read/write ops.
fn compute_pool(store: Arc<RemoteStore>) -> WorkerPool {
  let mut ops = OpRegistry::new();
  ops.register(
    "put_first",
    Box::new(|ctx, ids, payload| {
      ctx.put(ids[0], payload.to_vec());
      vec![]
    }),
  );
  ops.register(
    "get_first",
    Box::new(|ctx, ids, _payload| ctx.get(ids[0]).unwrap_or_default()),
  );
  // Read-modify-write, the typed layer's actual shape (ensure_tracked
  // always reads before a set) — the read repopulates the owning
  // thread's cache, which is what makes the delta path available at
  // commit time (post-op release invalidates cache entries).
  ops.register(
    "read_then_put",
    Box::new(|ctx, ids, payload| {
      let _ = ctx.get(ids[0]);
      ctx.put(ids[0], payload.to_vec());
      vec![]
    }),
  );
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 2)])));
  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  WorkerPool::spawn(
    store,
    2,
    Arc::new(ops),
    ring,
    NodeId(1),
    listener,
    Arc::new(HashMap::new()),
    Arc::new(IndexKindRegistry::new()),
  )
}

#[test]
fn write_through_survives_cache_eviction_and_storage_restart() {
  let dir = tempfile::tempdir().unwrap();
  let storage_addr = start_storage(dir.path());
  let store = remote_store(&storage_addr);
  let pool = compute_pool(Arc::clone(&store));

  let id = DatumId::new();
  pool
    .run_op(
      DatumId::new(),
      "put_first".to_string(),
      vec![id],
      b"durable".to_vec(),
    )
    .unwrap();

  // Evict every compute cache entry: the next read MUST come from
  // storage.
  pool.evict_non_native(Arc::new(|_| false));
  let read = pool
    .run_op(DatumId::new(), "get_first".to_string(), vec![id], vec![])
    .unwrap();
  assert_eq!(read, b"durable".to_vec());

  // Storage restart: reopen the SAME log directory fresh (recovery
  // scan) behind a new listener — the acked write must survive.
  let restarted_addr = start_storage(dir.path());
  let store2 = remote_store(&restarted_addr);
  let pool2 = compute_pool(store2);
  let read = pool2
    .run_op(DatumId::new(), "get_first".to_string(), vec![id], vec![])
    .unwrap();
  assert_eq!(read, b"durable".to_vec());
}

#[test]
fn a_small_change_to_a_large_datum_ships_a_small_log_delta() {
  let dir = tempfile::tempdir().unwrap();
  let storage_addr = start_storage(dir.path());
  let store = remote_store(&storage_addr);
  let pool = compute_pool(Arc::clone(&store));
  let log_path = dir.path().join("datum_log.dlog");

  let id = DatumId::new();
  let big = vec![42u8; 1_000_000];
  pool
    .run_op(
      DatumId::new(),
      "put_first".to_string(),
      vec![id],
      big.clone(),
    )
    .unwrap();
  let after_full = std::fs::metadata(&log_path).unwrap().len();

  // One-byte change in the middle: the cache still holds the old
  // value on the owning thread, so the write ships as a delta.
  let mut changed = big.clone();
  changed[500_000] = 7;
  pool
    .run_op(
      DatumId::new(),
      "read_then_put".to_string(),
      vec![id],
      changed.clone(),
    )
    .unwrap();
  let after_delta = std::fs::metadata(&log_path).unwrap().len();
  let growth = after_delta - after_full;
  assert!(
    growth < 10_000,
    "expected a small delta append, log grew by {growth} bytes"
  );

  // And it reads back exactly, from storage, after eviction.
  pool.evict_non_native(Arc::new(|_| false));
  let read = pool
    .run_op(DatumId::new(), "get_first".to_string(), vec![id], vec![])
    .unwrap();
  assert_eq!(read, changed);
}

#[test]
fn capacity_weights_spread_ids_across_storage_nodes() {
  // Pure ring-function check: with weights (1, 3), both nodes receive
  // placements and the heavier node receives more.
  let ring = Ring::from_members(&[(NodeId(1), 1), (NodeId(2), 3)]);
  let mut counts: HashMap<NodeId, usize> = HashMap::new();
  for _ in 0..400 {
    let (node, _) = ring.native(DatumId::new());
    *counts.entry(node).or_insert(0) += 1;
  }
  let light = *counts.get(&NodeId(1)).unwrap_or(&0);
  let heavy = *counts.get(&NodeId(2)).unwrap_or(&0);
  assert!(light > 0 && heavy > 0);
  assert!(
    heavy > light,
    "weights ignored: light={light} heavy={heavy}"
  );
}
