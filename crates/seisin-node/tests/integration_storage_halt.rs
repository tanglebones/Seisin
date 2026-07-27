use std::collections::{HashMap, HashSet};
use std::net::TcpListener;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_gossip::membership::{Incarnation, MemberRole, MemberStatus, MemberUpdate};
use seisin_node::gossip_client::run_gossip_loop;
use seisin_node::gossip_server::{serve_gossip, serve_gossip_storage};
use seisin_node::gossip_state::{ClusterState, GossipState};
use seisin_node::halt::HaltState;
use seisin_node::heartbeat::Heartbeat;
use seisin_node::pool::WorkerPool;
use seisin_node::remote_store::RemoteStore;
use seisin_node::server::serve;
use seisin_node::store_server::{serve_store, StoreNode};
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{Request, Response};
use seisin_ring::ring::Ring;
use seisin_storage::datum_log::DatumLog;

const PROBE_INTERVAL_MILLIS: u64 = 20;
const PROBE_TIMEOUT_MILLIS: u64 = 20;
const SUSPICION_TIMEOUT_MILLIS: u64 = 40;

#[test]
fn a_dead_storage_node_halts_client_traffic_with_the_reason() {
  let compute_id = NodeId(1);
  let storage_id = NodeId(9);

  // Storage node: delta log + store listener + ack-only gossip
  // responder.
  let store_dir = tempfile::tempdir().unwrap();
  let log = Arc::new(Mutex::new(
    DatumLog::open(&store_dir.path().join("datum_log.dlog")).unwrap(),
  ));
  let store_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let store_addr = store_listener.local_addr().unwrap().to_string();
  // A shared heartbeat between the store server and the gossip responder:
  // being probed keeps the store server serving. A large threshold — this
  // test exercises the compute-side halt, not storage self-halt.
  let storage_heartbeat = Arc::new(Heartbeat::new());
  let store_node = Arc::new(StoreNode {
    log,
    node_id: storage_id,
    heartbeat: Arc::clone(&storage_heartbeat),
    self_halt_threshold: Duration::from_secs(3600),
    transfers: Arc::new(seisin_node::transfer::TransferManager::default()),
  });
  thread::spawn(move || serve_store(store_listener, store_node));

  let storage_gossip_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let storage_gossip_addr = storage_gossip_listener.local_addr().unwrap().to_string();

  let compute_client_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let compute_addr = compute_client_listener.local_addr().unwrap().to_string();
  let compute_gossip_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let compute_gossip_addr = compute_gossip_listener.local_addr().unwrap().to_string();
  let peer_link_listener = TcpListener::bind("127.0.0.1:0").unwrap();

  // Shared membership seed: one compute member, one storage member.
  let seed = |table: &mut seisin_gossip::membership::MemberTable| {
    table.merge_update(MemberUpdate {
      node_id: compute_id,
      incarnation: Incarnation(0),
      status: MemberStatus::Alive,
      client_address: compute_addr.clone(),
      gossip_address: compute_gossip_addr.clone(),
      thread_count: 2,
      role: MemberRole::Compute,
      capacity_weight: 0,
      store_address: String::new(),
      log_id: [0u8; 16],
    });
    table.merge_update(MemberUpdate {
      node_id: storage_id,
      incarnation: Incarnation(0),
      status: MemberStatus::Alive,
      client_address: String::new(),
      gossip_address: storage_gossip_addr.clone(),
      thread_count: 1,
      role: MemberRole::Storage,
      capacity_weight: 1,
      store_address: store_addr.clone(),
      log_id: [0u8; 16],
    });
  };

  let storage_gossip = Arc::new(GossipState::new());
  seed(&mut storage_gossip.member_table.lock().unwrap());
  thread::spawn(move || {
    serve_gossip_storage(storage_gossip_listener, storage_gossip, storage_heartbeat)
  });

  // Compute node: pool over RemoteStore; gossip loop probing everyone
  // (including the storage member).
  let compute_ring = Arc::new(RwLock::new(Ring::from_members(&[(compute_id, 2)])));
  let storage_ring = Arc::new(RwLock::new(Ring::from_members(&[(storage_id, 1)])));
  let store_addresses = Arc::new(RwLock::new(HashMap::from([(
    storage_id,
    store_addr.clone(),
  )])));
  let halt = Arc::new(HaltState::new());
  let cluster = Arc::new(ClusterState {
    compute_ring: Arc::clone(&compute_ring),
    storage_ring: Arc::clone(&storage_ring),
    store_addresses: Arc::clone(&store_addresses),
    identity_book: Arc::new(RwLock::new(HashMap::new())),
    storage_alive: Arc::new(RwLock::new(HashSet::from([storage_id]))),
    storage_stale: Arc::new(RwLock::new(HashSet::new())),
    halt: Arc::clone(&halt),
  });

  let mut ops = OpRegistry::new();
  ops.register(
    "put",
    Box::new(|ctx, ids, payload| {
      ctx.put(ids[0], payload.to_vec());
      vec![]
    }),
  );
  let remote = Arc::new(RemoteStore::new(
    Arc::clone(&storage_ring),
    Arc::clone(&store_addresses),
  ));
  let pool = Arc::new(WorkerPool::spawn(
    remote,
    2,
    Arc::new(ops),
    Arc::clone(&compute_ring),
    compute_id,
    peer_link_listener,
    Arc::new(HashMap::new()),
    Arc::new(seisin_node::index_handler::IndexKindRegistry::new()),
  ));

  let compute_gossip = Arc::new(GossipState::new());
  seed(&mut compute_gossip.member_table.lock().unwrap());
  {
    let cluster = Arc::clone(&cluster);
    let address_book = Arc::new(HashMap::from([(compute_id, compute_addr.clone())]));
    let pool = Arc::clone(&pool);
    thread::spawn(move || {
      serve(
        compute_client_listener,
        compute_id,
        cluster,
        address_book,
        pool,
      )
    });
  }
  {
    let gossip = Arc::clone(&compute_gossip);
    let cluster = Arc::clone(&cluster);
    let pool = Arc::clone(&pool);
    thread::spawn(move || serve_gossip(compute_gossip_listener, compute_id, gossip, cluster, pool));
  }
  {
    let gossip = Arc::clone(&compute_gossip);
    let cluster = Arc::clone(&cluster);
    let pool = Arc::clone(&pool);
    thread::spawn(move || {
      run_gossip_loop(
        compute_id,
        gossip,
        cluster,
        pool,
        PROBE_INTERVAL_MILLIS,
        PROBE_TIMEOUT_MILLIS,
        SUSPICION_TIMEOUT_MILLIS,
      )
    });
  }
  thread::sleep(Duration::from_millis(100));

  // Healthy: an op writes through the storage tier and succeeds.
  let put = |payload: &[u8]| {
    seisin_client::call(
      &compute_addr,
      Request::Op {
        op_id: DatumId::new(),
        op_name: "put".to_string(),
        datum_ids: vec![DatumId::new()],
        payload: payload.to_vec(),
      },
    )
    .unwrap()
  };
  assert_eq!(put(b"before"), Response::OpResult { payload: vec![] });
  assert!(!halt.is_halted());

  // --- Halt half. An in-process gossip responder can't be killed from
  // outside its thread, so the death scenario uses a second compute
  // node whose config names a storage member on a reserved-but-silent
  // gossip address — the same "dead from the start" simulation the
  // existing failure-detection test uses. Its detector must confirm
  // the storage member dead and engage the halt.
  let silent_gossip = TcpListener::bind("127.0.0.1:0")
    .unwrap()
    .local_addr()
    .unwrap()
    .to_string();
  let compute2_id = NodeId(2);
  let compute2_client_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let compute2_addr = compute2_client_listener.local_addr().unwrap().to_string();
  let compute2_gossip_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let compute2_gossip_addr = compute2_gossip_listener.local_addr().unwrap().to_string();
  let peer_link2 = TcpListener::bind("127.0.0.1:0").unwrap();

  let compute2_ring = Arc::new(RwLock::new(Ring::from_members(&[(compute2_id, 2)])));
  let halt2 = Arc::new(HaltState::new());
  let cluster2 = Arc::new(ClusterState {
    compute_ring: Arc::clone(&compute2_ring),
    storage_ring: Arc::new(RwLock::new(Ring::from_members(&[(storage_id, 1)]))),
    store_addresses: Arc::new(RwLock::new(HashMap::new())),
    identity_book: Arc::new(RwLock::new(HashMap::new())),
    storage_alive: Arc::new(RwLock::new(HashSet::from([storage_id]))),
    storage_stale: Arc::new(RwLock::new(HashSet::new())),
    halt: Arc::clone(&halt2),
  });
  let mut ops2 = OpRegistry::new();
  ops2.register(
    "put",
    Box::new(|ctx, ids, payload| {
      ctx.put(ids[0], payload.to_vec());
      vec![]
    }),
  );
  let pool2 = Arc::new(WorkerPool::spawn(
    Arc::new(seisin_core::store::InMemoryStore::new()),
    2,
    Arc::new(ops2),
    Arc::clone(&compute2_ring),
    compute2_id,
    peer_link2,
    Arc::new(HashMap::new()),
    Arc::new(seisin_node::index_handler::IndexKindRegistry::new()),
  ));
  let gossip2 = Arc::new(GossipState::new());
  {
    let mut table = gossip2.member_table.lock().unwrap();
    table.merge_update(MemberUpdate {
      node_id: compute2_id,
      incarnation: Incarnation(0),
      status: MemberStatus::Alive,
      client_address: compute2_addr.clone(),
      gossip_address: compute2_gossip_addr.clone(),
      thread_count: 2,
      role: MemberRole::Compute,
      capacity_weight: 0,
      store_address: String::new(),
      log_id: [0u8; 16],
    });
    // The storage member exists in config but nothing listens on its
    // gossip address — a storage node that died before this compute
    // node came up.
    table.merge_update(MemberUpdate {
      node_id: storage_id,
      incarnation: Incarnation(0),
      status: MemberStatus::Alive,
      client_address: String::new(),
      gossip_address: silent_gossip.clone(),
      thread_count: 1,
      role: MemberRole::Storage,
      capacity_weight: 1,
      store_address: "127.0.0.1:1".to_string(),
      log_id: [0u8; 16],
    });
  }
  {
    let cluster = Arc::clone(&cluster2);
    let address_book = Arc::new(HashMap::from([(compute2_id, compute2_addr.clone())]));
    let pool = Arc::clone(&pool2);
    thread::spawn(move || {
      serve(
        compute2_client_listener,
        compute2_id,
        cluster,
        address_book,
        pool,
      )
    });
  }
  {
    let gossip = Arc::clone(&gossip2);
    let cluster = Arc::clone(&cluster2);
    let pool = Arc::clone(&pool2);
    thread::spawn(move || {
      serve_gossip(compute2_gossip_listener, compute2_id, gossip, cluster, pool)
    });
  }
  {
    let gossip = Arc::clone(&gossip2);
    let cluster = Arc::clone(&cluster2);
    let pool = Arc::clone(&pool2);
    thread::spawn(move || {
      run_gossip_loop(
        compute2_id,
        gossip,
        cluster,
        pool,
        PROBE_INTERVAL_MILLIS,
        PROBE_TIMEOUT_MILLIS,
        SUSPICION_TIMEOUT_MILLIS,
      )
    });
  }

  // Wait for the detector to converge the storage member to Dead.
  thread::sleep(Duration::from_millis(
    PROBE_INTERVAL_MILLIS + PROBE_TIMEOUT_MILLIS * 2 + SUSPICION_TIMEOUT_MILLIS + 500,
  ));
  assert!(halt2.is_halted(), "storage death did not engage the halt");
  let reason = halt2.reason().unwrap();
  assert!(reason.contains("storage node"), "{reason}");
  assert!(reason.contains("9"), "{reason}");

  // Client traffic is rejected with the halt reason...
  let response = seisin_client::call(
    &compute2_addr,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "put".to_string(),
      datum_ids: vec![DatumId::new()],
      payload: b"x".to_vec(),
    },
  )
  .unwrap();
  match response {
    Response::OpError { message } => {
      assert!(message.contains("cluster halted"), "{message}")
    }
    other => panic!("expected the halt error, got {other:?}"),
  }
  // ...and the compute ring itself was never touched by the storage
  // death (its own node still owns everything).
  assert_eq!(
    compute2_ring.read().unwrap().native(DatumId::new()).0,
    compute2_id
  );
}
