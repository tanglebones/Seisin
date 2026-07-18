//! The epoch sequencer: whichever live member currently has the lowest
//! `NodeId` is responsible for assigning epoch numbers to ring
//! mutations (see the design doc's "Compute Ring Mechanics" section).
//! No election protocol is needed — every node computes this
//! independently from its own membership view, and it updates
//! automatically as nodes join or leave.

use seisin_core::authority::NodeId;

pub fn is_sequencer(self_id: NodeId, alive_members: &[NodeId]) -> bool {
  alive_members.iter().min().is_some_and(|lowest| *lowest == self_id)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn lowest_node_id_is_the_sequencer() {
    assert!(is_sequencer(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]));
  }

  #[test]
  fn a_higher_node_id_is_not_the_sequencer() {
    assert!(!is_sequencer(NodeId(2), &[NodeId(1), NodeId(2), NodeId(3)]));
  }

  #[test]
  fn a_node_absent_from_the_alive_list_is_not_the_sequencer() {
    assert!(!is_sequencer(NodeId(1), &[NodeId(2), NodeId(3)]));
  }

  #[test]
  fn a_lone_alive_member_is_its_own_sequencer() {
    assert!(is_sequencer(NodeId(5), &[NodeId(5)]));
  }

  #[test]
  fn an_empty_alive_list_has_no_sequencer() {
    assert!(!is_sequencer(NodeId(1), &[]));
  }
}
