//! The bare framework node binary: reads `SEISIN_NODE_CONFIG` and boots
//! the node with *empty* op/index-kind registries. A real solution built
//! on this framework supplies its ops and index kinds in its own binary
//! (registering them before calling `seisin_node::node::run`) — this bare
//! binary can't, without a seisin-node <-> seisin-types dependency cycle.

use anyhow::{Context, Result};

use seisin_node::config::NodeConfig;
use seisin_node::index_handler::IndexKindRegistry;
use seisin_ops::registry::OpRegistry;

fn main() -> Result<()> {
  let config_path = std::env::var("SEISIN_NODE_CONFIG")
    .context("SEISIN_NODE_CONFIG must name a RON config file")?;
  let config = NodeConfig::load(&config_path)?;
  seisin_node::node::run(config, OpRegistry::new(), IndexKindRegistry::new())
}
