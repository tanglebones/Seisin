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
    self
      .members
      .get(&node_id)
      .map(|record| record.to_update(node_id))
  }

  /// Transitions a currently-`Alive` member to `Suspect`. No-op (returns
  /// `None`) if the node is unknown or already `Suspect`/`Dead` — a
  /// failure detector (added in Sub-project 2b-iii) calls this on a
  /// probe timeout.
  pub fn mark_suspect(&mut self, node_id: NodeId) -> Option<MemberUpdate> {
    let record = self.members.get_mut(&node_id)?;
    if record.status != MemberStatus::Alive {
      return None;
    }
    record.status = MemberStatus::Suspect;
    Some(record.to_update(node_id))
  }

  /// Transitions a currently-`Alive` or `Suspect` member to `Dead`.
  /// No-op if unknown or already `Dead` — a failure detector calls this
  /// after the suspicion timeout elapses with no refutation.
  pub fn mark_dead(&mut self, node_id: NodeId) -> Option<MemberUpdate> {
    let record = self.members.get_mut(&node_id)?;
    if record.status == MemberStatus::Dead {
      return None;
    }
    record.status = MemberStatus::Dead;
    Some(record.to_update(node_id))
  }

  /// Bumps this node's own incarnation and marks it `Alive` — used to
  /// refute a false suspicion (gossip reporting this node as `Suspect`
  /// when it's actually fine).
  ///
  /// # Panics
  /// Panics if `self_id` isn't already registered in this table — a node
  /// must register itself before it can confirm its own liveness.
  pub fn confirm_alive_self(&mut self, self_id: NodeId) -> MemberUpdate {
    let record = self
      .members
      .get_mut(&self_id)
      .expect("self must already be registered");
    record.incarnation = Incarnation(record.incarnation.0 + 1);
    record.status = MemberStatus::Alive;
    record.to_update(self_id)
  }

  /// A full snapshot of every known member's current update, in no
  /// particular order.
  pub fn all(&self) -> Vec<MemberUpdate> {
    self
      .members
      .iter()
      .map(|(node_id, record)| record.to_update(*node_id))
      .collect()
  }

  /// All members currently believed `Alive`, in ascending `NodeId` order.
  pub fn alive_members(&self) -> Vec<NodeId> {
    let mut ids: Vec<NodeId> = self
      .members
      .iter()
      .filter(|(_, record)| record.status == MemberStatus::Alive)
      .map(|(node_id, _)| *node_id)
      .collect();
    ids.sort_by_key(|node_id| node_id.0);
    ids
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
    assert_eq!(
      table.get(NodeId(1)),
      Some(update(1, 0, MemberStatus::Alive))
    );
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

  #[test]
  fn mark_suspect_transitions_an_alive_member() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 0, MemberStatus::Alive));
    let result = table.mark_suspect(NodeId(1));
    assert_eq!(result.unwrap().status, MemberStatus::Suspect);
    assert_eq!(table.get(NodeId(1)).unwrap().status, MemberStatus::Suspect);
  }

  #[test]
  fn mark_suspect_on_unknown_node_is_a_no_op() {
    let mut table = MemberTable::new();
    assert_eq!(table.mark_suspect(NodeId(1)), None);
  }

  #[test]
  fn mark_suspect_on_already_suspect_member_is_a_no_op() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 0, MemberStatus::Suspect));
    assert_eq!(table.mark_suspect(NodeId(1)), None);
  }

  #[test]
  fn mark_dead_transitions_a_suspect_member() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 0, MemberStatus::Suspect));
    let result = table.mark_dead(NodeId(1));
    assert_eq!(result.unwrap().status, MemberStatus::Dead);
  }

  #[test]
  fn mark_dead_on_already_dead_member_is_a_no_op() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 0, MemberStatus::Dead));
    assert_eq!(table.mark_dead(NodeId(1)), None);
  }

  #[test]
  fn confirm_alive_self_bumps_incarnation_and_clears_suspicion() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 3, MemberStatus::Suspect));
    let result = table.confirm_alive_self(NodeId(1));
    assert_eq!(result.incarnation, Incarnation(4));
    assert_eq!(result.status, MemberStatus::Alive);
    assert_eq!(table.get(NodeId(1)).unwrap().incarnation, Incarnation(4));
  }

  #[test]
  fn all_returns_every_known_member() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 0, MemberStatus::Alive));
    table.merge_update(update(2, 0, MemberStatus::Suspect));
    let mut all = table.all();
    all.sort_by_key(|u| u.node_id.0);
    assert_eq!(
      all,
      vec![
        update(1, 0, MemberStatus::Alive),
        update(2, 0, MemberStatus::Suspect)
      ]
    );
  }

  #[test]
  fn alive_members_excludes_suspect_and_dead_and_is_sorted() {
    let mut table = MemberTable::new();
    table.merge_update(update(3, 0, MemberStatus::Alive));
    table.merge_update(update(1, 0, MemberStatus::Alive));
    table.merge_update(update(2, 0, MemberStatus::Suspect));
    table.merge_update(update(4, 0, MemberStatus::Dead));
    assert_eq!(table.alive_members(), vec![NodeId(1), NodeId(3)]);
  }
}
