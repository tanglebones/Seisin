//! This node's identity plus the static compute-ring membership for
//! Sub-project 2a. Sub-project 2b replaces the `members` list (learned
//! here from a config file) with SWIM-gossiped join/leave events feeding
//! the same `Ring` type — this struct's job is only ever "what does this
//! process currently believe the membership is," regardless of source.

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct MemberConfig {
    pub node_id: u64,
    pub address: String,
    pub thread_count: u32,
}

#[derive(Debug, Deserialize)]
pub struct NodeConfig {
    pub self_node_id: u64,
    pub members: Vec<MemberConfig>,
}

impl NodeConfig {
    pub fn parse(source: &str) -> Result<Self> {
        ron::from_str(source).context("failed to parse node config RON")
    }

    pub fn load(path: &str) -> Result<Self> {
        let source = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {path}"))?;
        Self::parse(&source)
    }

    /// This node's own address, looked up from `members` by `self_node_id`.
    ///
    /// # Panics
    /// Panics if `self_node_id` isn't present in `members` — a config
    /// file that doesn't list itself is a startup-time configuration bug,
    /// not a runtime condition to recover from.
    pub fn self_address(&self) -> &str {
        self.members
            .iter()
            .find(|m| m.node_id == self.self_node_id)
            .map(|m| m.address.as_str())
            .expect("self_node_id must be present in members")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
(
    self_node_id: 1,
    members: [
        (node_id: 1, address: "127.0.0.1:7878", thread_count: 2),
        (node_id: 2, address: "127.0.0.1:7879", thread_count: 4),
    ],
)
"#;

    #[test]
    fn parses_a_well_formed_config() {
        let config = NodeConfig::parse(SAMPLE).unwrap();
        assert_eq!(config.self_node_id, 1);
        assert_eq!(config.members.len(), 2);
        assert_eq!(config.members[1].thread_count, 4);
    }

    #[test]
    fn self_address_finds_the_matching_member() {
        let config = NodeConfig::parse(SAMPLE).unwrap();
        assert_eq!(config.self_address(), "127.0.0.1:7878");
    }

    #[test]
    fn rejects_malformed_ron() {
        assert!(NodeConfig::parse("not valid ron {{{").is_err());
    }
}
