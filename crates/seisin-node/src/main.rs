use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;

use anyhow::{Context, Result};

use seisin_core::authority::NodeId;
use seisin_core::store::InMemoryStore;
use seisin_gossip::membership::{Incarnation, MemberRole, MemberStatus, MemberUpdate};
use seisin_node::config::NodeConfig;
use seisin_node::gossip_client::run_gossip_loop;
use seisin_node::gossip_server::serve_gossip;
use seisin_node::gossip_state::GossipState;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ring::ring::Ring;

fn main() -> Result<()> {
  let config_path = std::env::var("SEISIN_NODE_CONFIG")
    .context("SEISIN_NODE_CONFIG must name a RON config file")?;
  let config = NodeConfig::load(&config_path)?;

  let self_node_id = NodeId(config.self_node_id);

  // Storage-role nodes run only the store listener over the delta log
  // — no compute listeners, no gossip in Part A (static storage ring).
  let self_member = config
    .members
    .iter()
    .find(|m| m.node_id == config.self_node_id)
    .with_context(|| format!("self_node_id {} not in members", config.self_node_id))?;
  if self_member.role == seisin_node::config::NodeRole::Storage {
    let store_address = self_member
      .store_address
      .clone()
      .context("storage members must set store_address")?;
    std::fs::create_dir_all(&config.data_dir)
      .with_context(|| format!("failed to create data_dir {}", config.data_dir))?;
    let log_path = std::path::Path::new(&config.data_dir).join("datum_log.dlog");
    let log = std::sync::Arc::new(std::sync::Mutex::new(
      seisin_storage::datum_log::DatumLog::open(&log_path)?,
    ));
    let self_log_id = log.lock().unwrap().log_id();
    let listener = TcpListener::bind(&store_address)
      .with_context(|| format!("failed to bind {store_address}"))?;
    println!("seisin-node {self_node_id:?} STORAGE role, store listener on {store_address}");
    // Join the gossip fabric as an ack-only responder so compute
    // nodes' failure detectors can track this member's liveness.
    let gossip = Arc::new(GossipState::new());
    {
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
            seisin_node::config::NodeRole::Compute => MemberRole::Compute,
            seisin_node::config::NodeRole::Storage => MemberRole::Storage,
          },
          capacity_weight: member.capacity_weight.unwrap_or(1),
          store_address: member.store_address.clone().unwrap_or_default(),
          log_id: if member.node_id == config.self_node_id {
            self_log_id
          } else {
            [0u8; 16]
          },
        });
      }
    }
    let gossip_listener = TcpListener::bind(&self_member.gossip_address)
      .with_context(|| format!("failed to bind {}", self_member.gossip_address))?;
    let heartbeat = Arc::new(seisin_node::heartbeat::Heartbeat::new());
    {
      let gossip = Arc::clone(&gossip);
      let heartbeat = Arc::clone(&heartbeat);
      thread::spawn(move || {
        seisin_node::gossip_server::serve_gossip_storage(gossip_listener, gossip, heartbeat)
      });
    }
    let store_node = Arc::new(seisin_node::store_server::StoreNode {
      log,
      node_id: self_node_id,
      heartbeat,
      self_halt_threshold: std::time::Duration::from_millis(
        seisin_gossip::failure_detector::SUSPICION_TIMEOUT_MILLIS,
      ),
      transfers: Arc::new(seisin_node::transfer::TransferManager::default()),
    });
    seisin_node::store_server::serve_store(listener, store_node);
    return Ok(());
  }
  let self_address = config.self_address().to_string();
  let self_gossip_address = config
    .members
    .iter()
    .find(|m| m.node_id == config.self_node_id)
    .map(|m| m.gossip_address.clone())
    .with_context(|| {
      format!(
        "self_node_id {} not present in members",
        config.self_node_id
      )
    })?;
  let self_thread_count = config
    .members
    .iter()
    .find(|m| m.node_id == config.self_node_id)
    .map(|m| m.thread_count)
    .with_context(|| {
      format!(
        "self_node_id {} not present in members",
        config.self_node_id
      )
    })?;
  let self_peer_link_address = config
    .members
    .iter()
    .find(|m| m.node_id == config.self_node_id)
    .map(|m| m.peer_link_address.clone())
    .with_context(|| {
      format!(
        "self_node_id {} not present in members",
        config.self_node_id
      )
    })?;

  let members: Vec<(NodeId, u32)> = config
    .members
    .iter()
    .map(|m| (NodeId(m.node_id), m.thread_count))
    .collect();
  let ring = Arc::new(RwLock::new(Ring::from_members(&members)));

  let address_book: HashMap<NodeId, String> = config
    .members
    .iter()
    .map(|m| (NodeId(m.node_id), m.address.clone()))
    .collect();
  let address_book = Arc::new(address_book);

  let peer_link_address_book: HashMap<NodeId, String> = config
    .members
    .iter()
    .map(|m| (NodeId(m.node_id), m.peer_link_address.clone()))
    .collect();
  let peer_link_address_book = Arc::new(peer_link_address_book);

  let gossip = Arc::new(GossipState::new());
  {
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
          seisin_node::config::NodeRole::Compute => MemberRole::Compute,
          seisin_node::config::NodeRole::Storage => MemberRole::Storage,
        },
        capacity_weight: member.capacity_weight.unwrap_or(1),
        store_address: member.store_address.clone().unwrap_or_default(),
        // Compute nodes learn a storage member's log id via gossip
        // (its self-update), not from config — zero here.
        log_id: [0u8; 16],
      });
    }
  }

  let peer_link_listener = TcpListener::bind(&self_peer_link_address)
    .with_context(|| format!("failed to bind {self_peer_link_address}"))?;
  println!("seisin-node {self_node_id:?} peer-link listener on {self_peer_link_address}");

  // With storage members configured, compute writes through to the
  // storage tier (write-before-ack); without any, the in-memory store
  // keeps single-process deployments working.
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
  let halt = Arc::new(seisin_node::halt::HaltState::new());
  let identity_book: Arc<RwLock<HashMap<NodeId, seisin_core::datum::DatumId>>> =
    Arc::new(RwLock::new(HashMap::new()));
  let cluster = Arc::new(seisin_node::gossip_state::ClusterState {
    compute_ring: Arc::clone(&ring),
    storage_ring: Arc::clone(&storage_ring),
    store_addresses: Arc::clone(&store_addresses),
    identity_book: Arc::clone(&identity_book),
    halt: Arc::clone(&halt),
  });
  let store: Arc<dyn seisin_core::store::Store> = if storage_members.is_empty() {
    Arc::new(InMemoryStore::new())
  } else {
    Arc::new(seisin_node::remote_store::RemoteStore::new(
      storage_ring,
      store_addresses,
    ))
  };
  // No solution has been wired up yet — empty op and index-kind
  // registries until a real solution built on this framework registers
  // its ops and index kinds (e.g. seisin_types::rk_kind::
  // register_rk_index_kind with config.data_dir) in its own binary;
  // this bare framework binary can't do it itself without a
  // seisin-node <-> seisin-types dependency cycle.
  let pool = Arc::new(WorkerPool::spawn(
    store,
    self_thread_count,
    Arc::new(seisin_ops::registry::OpRegistry::new()),
    Arc::clone(&ring),
    self_node_id,
    peer_link_listener,
    peer_link_address_book,
    Arc::new(seisin_node::index_handler::IndexKindRegistry::new()),
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
    seisin_gossip::failure_detector::PROBE_TIMEOUT_MILLIS,
    seisin_gossip::failure_detector::PROBE_TIMEOUT_MILLIS,
    seisin_gossip::failure_detector::SUSPICION_TIMEOUT_MILLIS,
  );
  Ok(())
}
