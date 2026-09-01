//! The node composition root, extracted so both the bare `seisin-node`
//! binary (op-less — a real solution supplies its ops in its own binary)
//! and test/solution binaries (which register ops before calling `run`)
//! share exactly one wiring of servers, pool, gossip, and storage. A
//! storage-role node runs only the store listener + ack-only gossip
//! responder; a compute node runs the client/gossip/peer-link servers,
//! the worker pool, and the probing gossip loop.

use std::collections::{HashMap, HashSet};
use std::net::TcpListener;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

use anyhow::{Context, Result};

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::{InMemoryStore, Store};
use seisin_gossip::membership::{Incarnation, MemberRole, MemberStatus, MemberUpdate};
use seisin_ops::registry::OpRegistry;
use seisin_ring::ring::Ring;

use crate::config::{NodeConfig, NodeRole};
use crate::gossip_client::run_gossip_loop;
use crate::gossip_server::{serve_gossip, serve_gossip_storage};
use crate::gossip_state::{ClusterState, GossipState};
use crate::halt::HaltState;
use crate::heartbeat::Heartbeat;
use crate::index_handler::IndexKindRegistry;
use crate::pool::WorkerPool;
use crate::remote_store::RemoteStore;
use crate::server::serve;
use crate::store_server::{serve_store, StoreNode};
use crate::transfer::TransferManager;

/// Boots this node per `config`, using `ops`/`index_kinds` for the
/// compute worker pool (ignored by a storage-role node). Blocks forever
/// (running the store server or the gossip loop) — the caller's process
/// is the node.
pub fn run(config: NodeConfig, ops: OpRegistry, index_kinds: IndexKindRegistry) -> Result<()> {
  let self_node_id = NodeId(config.self_node_id);

  let self_member = config
    .members
    .iter()
    .find(|m| m.node_id == config.self_node_id)
    .with_context(|| format!("self_node_id {} not in members", config.self_node_id))?;

  // Storage-role: store listener over the delta log + ack-only gossip
  // responder; no compute listeners, no worker pool.
  if self_member.role == NodeRole::Storage {
    let store_address = self_member
      .store_address
      .clone()
      .context("storage members must set store_address")?;
    std::fs::create_dir_all(&config.data_dir)
      .with_context(|| format!("failed to create data_dir {}", config.data_dir))?;
    let log_path = std::path::Path::new(&config.data_dir).join("datum_log.dlog");
    let log = Arc::new(std::sync::Mutex::new(
      seisin_storage::datum_log::DatumLog::open(&log_path)?,
    ));
    let self_log_id = log.lock().unwrap().log_id();
    let listener = TcpListener::bind(&store_address)
      .with_context(|| format!("failed to bind {store_address}"))?;
    println!("seisin-node {self_node_id:?} STORAGE role, store listener on {store_address}");
    let gossip = Arc::new(GossipState::new());
    seed_member_table(&gossip, &config, |m| {
      if m.node_id == config.self_node_id {
        self_log_id
      } else {
        [0u8; 16]
      }
    });
    let gossip_listener = TcpListener::bind(&self_member.gossip_address)
      .with_context(|| format!("failed to bind {}", self_member.gossip_address))?;
    let heartbeat = Arc::new(Heartbeat::new());
    {
      let gossip = Arc::clone(&gossip);
      let heartbeat = Arc::clone(&heartbeat);
      thread::spawn(move || serve_gossip_storage(gossip_listener, gossip, heartbeat));
    }
    let store_node = Arc::new(StoreNode {
      log,
      node_id: self_node_id,
      heartbeat,
      self_halt_threshold: std::time::Duration::from_millis(config.self_halt_threshold_millis()),
      transfers: Arc::new(TransferManager::default()),
      data_dir: std::path::PathBuf::from(&config.data_dir),
      collections: Mutex::new(HashMap::new()),
    });
    serve_store(listener, store_node);
    return Ok(());
  }

  // Compute-role.
  let self_address = config.self_address().to_string();
  let self_gossip_address = self_member.gossip_address.clone();
  let self_thread_count = self_member.thread_count;
  let self_peer_link_address = self_member.peer_link_address.clone();

  // The compute ring holds only compute-role members — a storage node
  // must never be a compute owner (it runs no client/peer-link servers).
  let ring_members: Vec<(NodeId, u32)> = config
    .members
    .iter()
    .filter(|m| m.role == NodeRole::Compute)
    .map(|m| (NodeId(m.node_id), m.thread_count))
    .collect();
  let ring = Arc::new(RwLock::new(Ring::from_members(&ring_members)));

  let address_book = Arc::new(
    config
      .members
      .iter()
      .map(|m| (NodeId(m.node_id), m.address.clone()))
      .collect::<HashMap<_, _>>(),
  );
  let peer_link_address_book = Arc::new(
    config
      .members
      .iter()
      .map(|m| (NodeId(m.node_id), m.peer_link_address.clone()))
      .collect::<HashMap<_, _>>(),
  );

  let gossip = Arc::new(GossipState::new());
  // Compute nodes learn a storage member's log id via gossip, not config.
  seed_member_table(&gossip, &config, |_| [0u8; 16]);

  let peer_link_listener = TcpListener::bind(&self_peer_link_address)
    .with_context(|| format!("failed to bind {self_peer_link_address}"))?;
  println!("seisin-node {self_node_id:?} peer-link listener on {self_peer_link_address}");

  let storage_members: Vec<(NodeId, u32)> = config
    .storage_ring_members()
    .into_iter()
    .map(|(id, w)| (NodeId(id), w))
    .collect();
  let storage_ring = Arc::new(RwLock::new(Ring::from_members(&storage_members)));
  let store_addresses: Arc<RwLock<HashMap<NodeId, String>>> = Arc::new(RwLock::new(
    config
      .store_address_book()
      .into_iter()
      .map(|(id, addr)| (NodeId(id), addr))
      .collect(),
  ));
  let halt = Arc::new(HaltState::new());
  let storage_alive: Arc<RwLock<HashSet<NodeId>>> = Arc::new(RwLock::new(
    storage_members.iter().map(|(id, _)| *id).collect(),
  ));
  let cluster = Arc::new(ClusterState {
    compute_ring: Arc::clone(&ring),
    storage_ring: Arc::clone(&storage_ring),
    store_addresses,
    identity_book: Arc::new(RwLock::new(HashMap::<NodeId, DatumId>::new())),
    storage_alive,
    storage_stale: Arc::new(RwLock::new(HashSet::new())),
    halt,
  });
  let store: Arc<dyn Store> = if storage_members.is_empty() {
    Arc::new(InMemoryStore::new())
  } else {
    Arc::new(RemoteStore::new(Arc::clone(&cluster)))
  };
  index_kinds.attach_collection_store(Arc::new(
    crate::collection_store::RemoteCollectionStore::new(Arc::clone(&cluster)),
  ));
  let pool = Arc::new(WorkerPool::spawn(
    store,
    self_thread_count,
    Arc::new(ops),
    Arc::clone(&ring),
    self_node_id,
    peer_link_listener,
    peer_link_address_book,
    Arc::new(index_kinds),
  ));

  let client_listener =
    TcpListener::bind(&self_address).with_context(|| format!("failed to bind {self_address}"))?;
  println!("seisin-node {self_node_id:?} client listener on {self_address}");
  {
    let cluster = Arc::clone(&cluster);
    let address_book = Arc::clone(&address_book);
    let pool = Arc::clone(&pool);
    thread::spawn(move || serve(client_listener, self_node_id, cluster, address_book, pool));
  }

  let gossip_listener = TcpListener::bind(&self_gossip_address)
    .with_context(|| format!("failed to bind {self_gossip_address}"))?;
  println!("seisin-node {self_node_id:?} gossip listener on {self_gossip_address}");
  {
    let gossip = Arc::clone(&gossip);
    let cluster = Arc::clone(&cluster);
    let pool = Arc::clone(&pool);
    thread::spawn(move || serve_gossip(gossip_listener, self_node_id, gossip, cluster, pool));
  }

  run_gossip_loop(
    self_node_id,
    gossip,
    cluster,
    pool,
    config.probe_interval_millis(),
    config.probe_timeout_millis(),
    config.suspicion_timeout_millis(),
  );
  Ok(())
}

/// Seeds every configured member into the gossip table; `log_id_of`
/// supplies each member's log id (self's real id on a storage node,
/// zero otherwise).
fn seed_member_table(
  gossip: &GossipState,
  config: &NodeConfig,
  log_id_of: impl Fn(&crate::config::MemberConfig) -> [u8; 16],
) {
  let mut table = gossip.member_table.lock().unwrap();
  for member in &config.members {
    table.merge_update(MemberUpdate {
      node_id: NodeId(member.node_id),
      incarnation: Incarnation(0),
      status: MemberStatus::Alive,
      client_address: member.address.clone(),
      gossip_address: member.gossip_address.clone(),
      thread_count: member.thread_count,
      role: match member.role {
        NodeRole::Compute => MemberRole::Compute,
        NodeRole::Storage => MemberRole::Storage,
      },
      capacity_weight: member.capacity_weight.unwrap_or(1),
      store_address: member.store_address.clone().unwrap_or_default(),
      log_id: log_id_of(member),
    });
  }
}
