use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;

use anyhow::{Context, Result};

use seisin_core::authority::NodeId;
use seisin_core::store::InMemoryStore;
use seisin_gossip::membership::{Incarnation, MemberStatus, MemberUpdate};
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
  let self_address = config.self_address().to_string();
  let self_gossip_address = config
    .members
    .iter()
    .find(|m| m.node_id == config.self_node_id)
    .map(|m| m.gossip_address.clone())
    .with_context(|| format!("self_node_id {} not present in members", config.self_node_id))?;
  let self_thread_count = config
    .members
    .iter()
    .find(|m| m.node_id == config.self_node_id)
    .map(|m| m.thread_count)
    .with_context(|| format!("self_node_id {} not present in members", config.self_node_id))?;

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
      });
    }
  }

  let store = Arc::new(InMemoryStore::new());
  let pool = Arc::new(WorkerPool::spawn(store, self_thread_count));

  let client_listener =
    TcpListener::bind(&self_address).with_context(|| format!("failed to bind {self_address}"))?;
  println!("seisin-node {self_node_id:?} client listener on {self_address}");
  {
    let ring = Arc::clone(&ring);
    let address_book = Arc::clone(&address_book);
    let pool = Arc::clone(&pool);
    thread::spawn(move || serve(client_listener, self_node_id, ring, address_book, pool));
  }

  let gossip_listener = TcpListener::bind(&self_gossip_address)
    .with_context(|| format!("failed to bind {self_gossip_address}"))?;
  println!("seisin-node {self_node_id:?} gossip listener on {self_gossip_address}");
  {
    let gossip = Arc::clone(&gossip);
    let ring = Arc::clone(&ring);
    let pool = Arc::clone(&pool);
    thread::spawn(move || serve_gossip(gossip_listener, self_node_id, gossip, ring, pool));
  }

  run_gossip_loop(
    self_node_id,
    gossip,
    ring,
    pool,
    seisin_gossip::failure_detector::PROBE_TIMEOUT_MILLIS,
    seisin_gossip::failure_detector::PROBE_TIMEOUT_MILLIS,
    seisin_gossip::failure_detector::SUSPICION_TIMEOUT_MILLIS,
  );
  Ok(())
}
