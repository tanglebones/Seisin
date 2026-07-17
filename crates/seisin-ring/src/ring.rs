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
    /// Builds a ring from a static member list: `(node_id, thread_count)`
    /// pairs. Each member contributes `thread_count` slots, in order.
    pub fn from_members(members: &[(NodeId, u32)]) -> Self {
        let mut slots = Vec::new();
        for (node_id, thread_count) in members {
            for t in 0..*thread_count {
                slots.push((*node_id, ThreadId(t)));
            }
        }
        Self { slots }
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
            let valid = (node_id == NodeId(1) && thread_id.0 < 2)
                || (node_id == NodeId(2) && thread_id.0 < 3);
            assert!(valid, "unexpected owner: {node_id:?} {thread_id:?}");
        }
    }

    #[test]
    fn single_member_ring_always_resolves_to_that_member() {
        let ring = Ring::from_members(&[(NodeId(9), 1)]);
        assert_eq!(ring.native(DatumId::new()), (NodeId(9), ThreadId(0)));
    }
}
