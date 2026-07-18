//! The epoch sequencer: whichever live member currently has the lowest
//! `NodeId` is responsible for assigning epoch numbers to ring
//! mutations (see the design doc's "Compute Ring Mechanics" section).
//! No election protocol is needed — every node computes this
//! independently from its own membership view, and it updates
//! automatically as nodes join or leave.

use std::collections::BTreeMap;

use seisin_core::authority::NodeId;

pub fn is_sequencer(self_id: NodeId, alive_members: &[NodeId]) -> bool {
  alive_members
    .iter()
    .min()
    .is_some_and(|lowest| *lowest == self_id)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RingMutation {
  Join { node_id: NodeId, thread_count: u32 },
  Leave { node_id: NodeId },
}

/// Buffers epoch-numbered ring mutations and releases them for
/// application only in strict, gapless epoch order — required because
/// gossip delivers `(epoch, mutation)` records with no ordering
/// guarantee, but the ring's swap-with-last mutation must be applied
/// identically (and therefore in the same order) on every node.
#[derive(Default)]
pub struct MutationLog {
  applied_epoch: u64,
  pending: BTreeMap<u64, RingMutation>,
}

impl MutationLog {
  pub fn new() -> Self {
    Self::default()
  }

  /// Buffers a sequenced mutation. An epoch at or before what's already
  /// been applied is ignored (a stale re-delivery).
  pub fn record(&mut self, epoch: u64, mutation: RingMutation) {
    if epoch > self.applied_epoch {
      self.pending.insert(epoch, mutation);
    }
  }

  /// Returns every mutation now ready to apply, in epoch order, and
  /// advances the applied-epoch marker past them. A missing epoch (a
  /// gap) leaves everything after it buffered until the gap is filled.
  pub fn drain_applicable(&mut self) -> Vec<RingMutation> {
    let mut ready = Vec::new();
    loop {
      let next_epoch = self.applied_epoch + 1;
      match self.pending.remove(&next_epoch) {
        Some(mutation) => {
          ready.push(mutation);
          self.applied_epoch = next_epoch;
        }
        None => break,
      }
    }
    ready
  }
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

#[cfg(test)]
mod mutation_log_tests {
  use super::*;

  #[test]
  fn drains_in_order_epochs_immediately() {
    let mut log = MutationLog::new();
    log.record(
      1,
      RingMutation::Join {
        node_id: NodeId(1),
        thread_count: 2,
      },
    );
    log.record(2, RingMutation::Leave { node_id: NodeId(1) });
    assert_eq!(
      log.drain_applicable(),
      vec![
        RingMutation::Join {
          node_id: NodeId(1),
          thread_count: 2
        },
        RingMutation::Leave { node_id: NodeId(1) },
      ]
    );
  }

  #[test]
  fn withholds_mutations_until_the_gap_before_them_is_filled() {
    let mut log = MutationLog::new();
    log.record(2, RingMutation::Leave { node_id: NodeId(1) });
    assert_eq!(log.drain_applicable(), vec![]);

    log.record(
      1,
      RingMutation::Join {
        node_id: NodeId(1),
        thread_count: 2,
      },
    );
    assert_eq!(
      log.drain_applicable(),
      vec![
        RingMutation::Join {
          node_id: NodeId(1),
          thread_count: 2
        },
        RingMutation::Leave { node_id: NodeId(1) },
      ]
    );
  }

  #[test]
  fn a_stale_epoch_is_ignored() {
    let mut log = MutationLog::new();
    log.record(
      1,
      RingMutation::Join {
        node_id: NodeId(1),
        thread_count: 2,
      },
    );
    log.drain_applicable();

    // Re-delivery of an already-applied epoch must not re-appear or panic.
    log.record(
      1,
      RingMutation::Join {
        node_id: NodeId(1),
        thread_count: 2,
      },
    );
    assert_eq!(log.drain_applicable(), vec![]);
  }

  #[test]
  fn drain_with_nothing_pending_returns_empty() {
    let mut log = MutationLog::new();
    assert_eq!(log.drain_applicable(), vec![]);
  }
}
