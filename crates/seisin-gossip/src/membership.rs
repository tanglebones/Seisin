//! The SWIM membership table: merges incoming `MemberUpdate` facts using
//! the standard SWIM precedence rule (higher incarnation always wins; at
//! equal incarnation, a more severe status wins), and exposes the
//! status-transition operations a failure detector drives (added in
//! Sub-project 2b-iii).

use std::collections::HashMap;

use seisin_core::authority::NodeId;

/// A node's self-reported generation number. A node bumps this to
/// refute a false suspicion (see `MemberTable::confirm_alive_self`); any
/// update at a higher incarnation always supersedes one at a lower
/// incarnation, regardless of status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Incarnation(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberStatus {
  Alive,
  Suspect,
  Dead,
}

impl MemberStatus {
  /// Ordering used to break ties at equal incarnation: `Dead` is most
  /// severe, `Alive` least.
  fn severity(self) -> u8 {
    match self {
      MemberStatus::Alive => 0,
      MemberStatus::Suspect => 1,
      MemberStatus::Dead => 2,
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberUpdate {
  pub node_id: NodeId,
  pub incarnation: Incarnation,
  pub status: MemberStatus,
  pub client_address: String,
  pub gossip_address: String,
  pub thread_count: u32,
}

struct MemberRecord {
  incarnation: Incarnation,
  status: MemberStatus,
  client_address: String,
  gossip_address: String,
  thread_count: u32,
}

impl MemberRecord {
  fn to_update(&self, node_id: NodeId) -> MemberUpdate {
    MemberUpdate {
      node_id,
      incarnation: self.incarnation,
      status: self.status,
      client_address: self.client_address.clone(),
      gossip_address: self.gossip_address.clone(),
      thread_count: self.thread_count,
    }
  }
}

#[derive(Default)]
pub struct MemberTable {
  members: HashMap<NodeId, MemberRecord>,
}

impl MemberTable {
  pub fn new() -> Self {
    Self::default()
  }

  /// Applies an incoming fact using the SWIM precedence rule. Returns
  /// whether it actually changed this table's state (a stale update, or
  /// one at the same incarnation but no more severe than what's already
  /// recorded, is ignored and returns `false`).
  pub fn merge_update(&mut self, update: MemberUpdate) -> bool {
    use std::collections::hash_map::Entry;

    match self.members.entry(update.node_id) {
      Entry::Vacant(slot) => {
        slot.insert(MemberRecord {
          incarnation: update.incarnation,
          status: update.status,
          client_address: update.client_address,
          gossip_address: update.gossip_address,
          thread_count: update.thread_count,
        });
        true
      }
      Entry::Occupied(mut slot) => {
        let current = slot.get();
        let accept = update.incarnation > current.incarnation
          || (update.incarnation == current.incarnation
            && update.status.severity() > current.status.severity());
        if accept {
          slot.insert(MemberRecord {
            incarnation: update.incarnation,
            status: update.status,
            client_address: update.client_address,
            gossip_address: update.gossip_address,
            thread_count: update.thread_count,
          });
        }
        accept
      }
    }
  }

  pub fn get(&self, node_id: NodeId) -> Option<MemberUpdate> {
    self.members.get(&node_id).map(|record| record.to_update(node_id))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn update(node_id: u64, incarnation: u64, status: MemberStatus) -> MemberUpdate {
    MemberUpdate {
      node_id: NodeId(node_id),
      incarnation: Incarnation(incarnation),
      status,
      client_address: "127.0.0.1:7878".to_string(),
      gossip_address: "127.0.0.1:8878".to_string(),
      thread_count: 2,
    }
  }

  #[test]
  fn first_update_for_a_node_is_always_accepted() {
    let mut table = MemberTable::new();
    let accepted = table.merge_update(update(1, 0, MemberStatus::Alive));
    assert!(accepted);
    assert_eq!(table.get(NodeId(1)), Some(update(1, 0, MemberStatus::Alive)));
  }

  #[test]
  fn higher_incarnation_always_wins() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 0, MemberStatus::Dead));
    let accepted = table.merge_update(update(1, 1, MemberStatus::Alive));
    assert!(accepted);
    assert_eq!(table.get(NodeId(1)).unwrap().status, MemberStatus::Alive);
  }

  #[test]
  fn lower_incarnation_is_ignored() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 5, MemberStatus::Alive));
    let accepted = table.merge_update(update(1, 4, MemberStatus::Dead));
    assert!(!accepted);
    assert_eq!(table.get(NodeId(1)).unwrap().status, MemberStatus::Alive);
  }

  #[test]
  fn at_equal_incarnation_more_severe_status_wins() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 0, MemberStatus::Alive));
    let accepted = table.merge_update(update(1, 0, MemberStatus::Suspect));
    assert!(accepted);
    assert_eq!(table.get(NodeId(1)).unwrap().status, MemberStatus::Suspect);
  }

  #[test]
  fn at_equal_incarnation_less_severe_status_is_ignored() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 0, MemberStatus::Suspect));
    let accepted = table.merge_update(update(1, 0, MemberStatus::Alive));
    assert!(!accepted);
    assert_eq!(table.get(NodeId(1)).unwrap().status, MemberStatus::Suspect);
  }

  #[test]
  fn get_on_unknown_node_returns_none() {
    let table = MemberTable::new();
    assert_eq!(table.get(NodeId(42)), None);
  }
}
