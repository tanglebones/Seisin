//! CLI for the storage migration driver. Two-phase by default: a bare
//! run prints the plan; `--apply` executes it.
//!
//! Usage:
//!   seisin-migrate <config.ron>            # dry run — print the plan
//!   seisin-migrate <config.ron> --apply    # execute the migration
//!   seisin-migrate resume <config.ron>     # verify identity, clear a halt
//!
//! Config (RON):
//!   (
//!     compute_addresses: ["127.0.0.1:7878", "127.0.0.1:7879"],
//!     proposed: [
//!       (node_id: 3, weight: 4, store_address: "127.0.0.1:6880"),
//!       (node_id: 4, weight: 4, store_address: "127.0.0.1:6881"),
//!     ],
//!   )
//! The proposed set is the *whole* desired storage ring: add = current +
//! new node, remove = current − node, reweight = new weights. Log ids
//! are resolved by the driver via Identify, so they are not in the file.

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use seisin_core::authority::NodeId;
use seisin_protocol::StorageMember;

#[derive(Debug, Deserialize)]
struct MigrateConfig {
  compute_addresses: Vec<String>,
  #[serde(default)]
  proposed: Vec<ProposedMember>,
}

#[derive(Debug, Deserialize)]
struct ProposedMember {
  node_id: u64,
  weight: u32,
  store_address: String,
}

fn main() -> Result<()> {
  let args: Vec<String> = std::env::args().collect();
  match args.get(1).map(String::as_str) {
    Some("resume") => {
      let path = args
        .get(2)
        .context("usage: seisin-migrate resume <config.ron>")?;
      let config = load(path)?;
      seisin_migrate::resume(&config.compute_addresses)
    }
    Some("recover") => {
      let path = args
        .get(2)
        .context("usage: seisin-migrate recover <config.ron> [--apply]")?;
      let apply = args.iter().any(|a| a == "--apply");
      let config = load(path)?;
      seisin_migrate::recover(&config.compute_addresses, apply)?;
      Ok(())
    }
    Some(path) => {
      let apply = args.iter().any(|a| a == "--apply");
      let config = load(path)?;
      let proposed: Vec<StorageMember> = config
        .proposed
        .iter()
        .map(|m| StorageMember {
          node_id: NodeId(m.node_id),
          weight: m.weight,
          store_address: m.store_address.clone(),
          // Resolved by the driver via Identify.
          log_id: seisin_core::datum::DatumId::from_bytes([0u8; 16]),
        })
        .collect();
      seisin_migrate::migrate(&config.compute_addresses, &proposed, apply)?;
      Ok(())
    }
    None => {
      bail!(
        "usage:\n  seisin-migrate <config.ron> [--apply]\n  \
         seisin-migrate recover <config.ron> [--apply]\n  \
         seisin-migrate resume <config.ron>"
      )
    }
  }
}

fn load(path: &str) -> Result<MigrateConfig> {
  let source =
    std::fs::read_to_string(path).with_context(|| format!("failed to read config {path}"))?;
  ron::from_str(&source).with_context(|| format!("failed to parse migrate config {path}"))
}
