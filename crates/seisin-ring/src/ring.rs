//! The compute ring: maps a datum to its currently native (node, thread).
//!
//! Built from a static member list for now (Sub-project 2a); Sub-project
//! 2b replaces the static list with SWIM-gossiped join/leave mutations
//! applied via the swap-with-last algorithm, epoch-ordered by an elected
//! sequencer — see the design doc's "Compute Ring Mechanics" section.
//! This type doesn't care where its slots came from, so that later
//! change doesn't require rewriting it.

use seisin_core::authority::{NodeId, ThreadId};
use seisin_core::datum::DatumId;

use crate::jump_hash::JumpBackHasher;

pub struct Ring {
  slots: Vec<(NodeId, ThreadId)>,
}

impl Ring {
  pub fn empty() -> Self {
    Self { slots: Vec::new() }
  }

  /// Appends `thread_count` new slots for `node_id` to the end of the
  /// ring. Per jump-consistent-hash's own guarantee, growing `n` only
  /// remaps keys that land in the newly-added range — every existing
  /// key's owner is unaffected.
  pub fn apply_join(&mut self, node_id: NodeId, thread_count: u32) {
    for t in 0..thread_count {
      self.slots.push((node_id, ThreadId(t)));
    }
  }

  /// Builds a ring from a static member list: `(node_id, thread_count)`
  /// pairs. Each member contributes `thread_count` slots, in order.
  pub fn from_members(members: &[(NodeId, u32)]) -> Self {
    let mut ring = Self::empty();
    for (node_id, thread_count) in members {
      ring.apply_join(*node_id, *thread_count);
    }
    ring
  }

  /// Removes all of `node_id`'s slots via swap-with-last: swap the
  /// removed slot with whatever's at the last index, then shrink by
  /// one. This is the standard technique for removing an arbitrary (not
  /// just the highest-index) slot while preserving jump-consistent-
  /// hash's minimal-remap guarantee for every untouched slot. The result
  /// is a deterministic function of the starting array and `node_id`, so
  /// every node applying the same mutation to the same starting ring
  /// converges on an identical result — required for the epoch-ordered
  /// replay in Sub-project 2b-ii.
  pub fn apply_leave(&mut self, node_id: NodeId) {
    let mut i = 0;
    while i < self.slots.len() {
      if self.slots[i].0 == node_id {
        let last = self.slots.len() - 1;
        self.slots.swap(i, last);
        self.slots.pop();
        // Don't advance i: the slot just swapped into position i might
        // also belong to node_id if it had multiple thread slots.
      } else {
        i += 1;
      }
    }
  }

  /// Returns the datum's current native (node, thread).
  ///
  /// # Panics
  /// Panics if the ring has no slots (an empty member list).
  pub fn native(&self, datum_id: DatumId) -> (NodeId, ThreadId) {
    let mut hasher = JumpBackHasher::new();
    let index = hasher.hash(hash_key(datum_id), self.slots.len() as u32);
    self.slots[index as usize]
  }
}

/// Derives the u64 hash key for a datum_id from its trailing 8 bytes
/// (UUIDv7's `rand_b` field, which is fully random) rather than its
/// leading bytes (mostly a monotonic timestamp, which would concentrate
/// ids created in the same millisecond into adjacent hash inputs).
fn hash_key(datum_id: DatumId) -> u64 {
  let bytes = datum_id.as_bytes();
  u64::from_le_bytes(bytes[8..16].try_into().unwrap())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn apply_join_adds_the_new_members_slots() {
    let mut ring = Ring::empty();
    ring.apply_join(NodeId(1), 2);
    for _ in 0..50 {
      let (node_id, thread_id) = ring.native(DatumId::new());
      assert_eq!(node_id, NodeId(1));
      assert!(thread_id.0 < 2);
    }
  }

  #[test]
  fn from_members_matches_building_via_apply_join() {
    let via_constructor = Ring::from_members(&[(NodeId(1), 2), (NodeId(2), 3)]);
    let mut via_mutation = Ring::empty();
    via_mutation.apply_join(NodeId(1), 2);
    via_mutation.apply_join(NodeId(2), 3);

    let id = DatumId::new();
    assert_eq!(via_constructor.native(id), via_mutation.native(id));
  }

  #[test]
  fn native_is_deterministic_for_the_same_ring() {
    let ring = Ring::from_members(&[(NodeId(1), 2), (NodeId(2), 3)]);
    let id = DatumId::new();
    assert_eq!(ring.native(id), ring.native(id));
  }

  #[test]
  fn native_always_resolves_to_a_configured_member_slot() {
    let ring = Ring::from_members(&[(NodeId(1), 2), (NodeId(2), 3)]);
    for _ in 0..100 {
      let (node_id, thread_id) = ring.native(DatumId::new());
      let valid =
        (node_id == NodeId(1) && thread_id.0 < 2) || (node_id == NodeId(2) && thread_id.0 < 3);
      assert!(valid, "unexpected owner: {node_id:?} {thread_id:?}");
    }
  }

  #[test]
  fn single_member_ring_always_resolves_to_that_member() {
    let ring = Ring::from_members(&[(NodeId(9), 1)]);
    assert_eq!(ring.native(DatumId::new()), (NodeId(9), ThreadId(0)));
  }

  #[test]
  fn apply_leave_removes_a_single_slot_member() {
    let mut ring = Ring::from_members(&[(NodeId(1), 1), (NodeId(2), 1)]);
    ring.apply_leave(NodeId(1));
    for _ in 0..50 {
      let (node_id, _) = ring.native(DatumId::new());
      assert_eq!(node_id, NodeId(2));
    }
  }

  #[test]
  fn apply_leave_removes_all_of_a_multi_slot_members_slots() {
    let mut ring = Ring::from_members(&[(NodeId(1), 2), (NodeId(2), 1)]);
    ring.apply_leave(NodeId(1));
    for _ in 0..50 {
      let (node_id, thread_id) = ring.native(DatumId::new());
      assert_eq!(node_id, NodeId(2));
      assert_eq!(thread_id, ThreadId(0));
    }
  }

  #[test]
  fn apply_leave_only_removes_the_named_member() {
    let mut ring = Ring::from_members(&[(NodeId(1), 1), (NodeId(2), 1), (NodeId(3), 1)]);
    ring.apply_leave(NodeId(2));
    for _ in 0..50 {
      let (node_id, _) = ring.native(DatumId::new());
      assert!(node_id == NodeId(1) || node_id == NodeId(3), "unexpected owner: {node_id:?}");
    }
  }

  #[test]
  fn apply_leave_on_an_unknown_member_is_a_no_op() {
    let mut ring = Ring::from_members(&[(NodeId(1), 1)]);
    let id = DatumId::new();
    let before = ring.native(id);
    ring.apply_leave(NodeId(999));
    assert_eq!(ring.native(id), before);
  }

  #[test]
  #[should_panic]
  fn native_panics_once_the_last_member_has_left() {
    let mut ring = Ring::from_members(&[(NodeId(1), 1)]);
    ring.apply_leave(NodeId(1));
    ring.native(DatumId::new());
  }
}
