use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::Arc;

use anyhow::{Context, Result};

use seisin_core::authority::NodeId;
use seisin_core::store::InMemoryStore;
use seisin_node::config::NodeConfig;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ring::ring::Ring;

fn main() -> Result<()> {
    let config_path =
        std::env::var("SEISIN_NODE_CONFIG").context("SEISIN_NODE_CONFIG must name a RON config file")?;
    let config = NodeConfig::load(&config_path)?;

    let self_node_id = NodeId(config.self_node_id);
    let self_address = config.self_address().to_string();

    let members: Vec<(NodeId, u32)> =
        config.members.iter().map(|m| (NodeId(m.node_id), m.thread_count)).collect();
    let ring = Arc::new(Ring::from_members(&members));

    let address_book: HashMap<NodeId, String> =
        config.members.iter().map(|m| (NodeId(m.node_id), m.address.clone())).collect();
    let address_book = Arc::new(address_book);

    let self_thread_count = config
        .members
        .iter()
        .find(|m| m.node_id == config.self_node_id)
        .map(|m| m.thread_count)
        .with_context(|| format!("self_node_id {} not present in members", config.self_node_id))?;

    let listener =
        TcpListener::bind(&self_address).with_context(|| format!("failed to bind {self_address}"))?;
    println!("seisin-node {self_node_id:?} listening on {self_address}");

    let pool = Arc::new(WorkerPool::spawn(Arc::new(InMemoryStore::new()), self_thread_count));
    serve(listener, self_node_id, ring, address_book, pool);
    Ok(())
}
