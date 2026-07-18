//! Shared, cross-thread gossip state: the membership table, the
//! epoch-ordered mutation log, and a small buffer of recently-seen
//! mutations re-gossiped on every outbound message as cheap insurance
//! against one lost message (this project doesn't implement full
//! SWIM-style epidemic retransmission tracking — see the design doc and
//! this plan's "deliberately out of scope" note).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};

use seisin_core::authority::NodeId;
use seisin_gossip::membership::{MemberTable, MemberUpdate};
use seisin_gossip::sequencer::{MutationLog, RingMutation};
use seisin_ring::ring::Ring;

use crate::pool::WorkerPool;

const RECENT_MUTATIONS_CAP: usize = 16;

pub struct GossipState {
  pub member_table: Mutex<MemberTable>,
  pub mutation_log: Mutex<MutationLog>,
  recent_mutations: Mutex<VecDeque<(u64, RingMutation)>>,
}

impl Default for GossipState {
  fn default() -> Self {
    Self::new()
  }
}

impl GossipState {
  pub fn new() -> Self {
    Self {
      member_table: Mutex::new(MemberTable::new()),
      mutation_log: Mutex::new(MutationLog::new()),
      recent_mutations: Mutex::new(VecDeque::new()),
    }
  }

  /// Records a mutation into the epoch-ordered log (for correct-order
  /// application) and into the small recent-mutations buffer (for
  /// re-gossiping), whether it originated locally (this node is the
  /// sequencer) or arrived from a peer.
  pub fn record_mutation(&self, epoch: u64, mutation: RingMutation) {
    self.mutation_log.lock().unwrap().record(epoch, mutation);
    let mut recent = self.recent_mutations.lock().unwrap();
    recent.push_back((epoch, mutation));
    while recent.len() > RECENT_MUTATIONS_CAP {
      recent.pop_front();
    }
  }

  /// The full membership snapshot plus recently-seen mutations to
  /// attach to an outbound gossip message.
  pub fn piggyback(&self) -> (Vec<MemberUpdate>, Vec<(u64, RingMutation)>) {
    let updates = self.member_table.lock().unwrap().all();
    let mutations = self.recent_mutations.lock().unwrap().iter().copied().collect();
    (updates, mutations)
  }

  /// Merges an incoming message's piggybacked updates and mutations.
  pub fn merge_incoming(&self, updates: Vec<MemberUpdate>, mutations: Vec<(u64, RingMutation)>) {
    {
      let mut table = self.member_table.lock().unwrap();
      for update in updates {
        table.merge_update(update);
      }
    }
    for (epoch, mutation) in mutations {
      self.record_mutation(epoch, mutation);
    }
  }
}

/// Applies every ring mutation that's now ready (in epoch order) to
/// `ring`, then evicts from `pool`'s cache any entry this node no longer
/// natively owns as a result — see the design doc's "Cache Invalidation
/// on Ring Membership Change" section.
pub fn apply_ready_mutations(gossip: &GossipState, ring: &Arc<RwLock<Ring>>, self_node_id: NodeId, pool: &WorkerPool) {
  let ready = gossip.mutation_log.lock().unwrap().drain_applicable();
  if ready.is_empty() {
    return;
  }
  {
    let mut ring_guard = ring.write().unwrap();
    for mutation in &ready {
      match *mutation {
        RingMutation::Join { node_id, thread_count } => ring_guard.apply_join(node_id, thread_count),
        RingMutation::Leave { node_id } => ring_guard.apply_leave(node_id),
      }
    }
  }
  let ring = Arc::clone(ring);
  pool.evict_non_native(Arc::new(move |id| ring.read().unwrap().native(id).0 == self_node_id));
}

#[cfg(test)]
mod tests {
  use super::*;
  use seisin_gossip::membership::{Incarnation, MemberStatus};

  fn sample_update(node_id: u64) -> MemberUpdate {
    MemberUpdate {
      node_id: NodeId(node_id),
      incarnation: Incarnation(0),
      status: MemberStatus::Alive,
      client_address: "127.0.0.1:7878".to_string(),
      gossip_address: "127.0.0.1:8878".to_string(),
      thread_count: 1,
    }
  }

  #[test]
  fn merge_incoming_applies_updates_and_mutations() {
    let gossip = GossipState::new();
    gossip.merge_incoming(
      vec![sample_update(1)],
      vec![(1, RingMutation::Join { node_id: NodeId(1), thread_count: 1 })],
    );
    assert_eq!(gossip.member_table.lock().unwrap().get(NodeId(1)), Some(sample_update(1)));
    assert_eq!(
      gossip.mutation_log.lock().unwrap().drain_applicable(),
      vec![RingMutation::Join { node_id: NodeId(1), thread_count: 1 }]
    );
  }

  #[test]
  fn piggyback_includes_merged_updates_and_recorded_mutations() {
    let gossip = GossipState::new();
    gossip.merge_incoming(vec![sample_update(1)], vec![]);
    gossip.record_mutation(1, RingMutation::Join { node_id: NodeId(1), thread_count: 1 });
    let (updates, mutations) = gossip.piggyback();
    assert_eq!(updates, vec![sample_update(1)]);
    assert_eq!(mutations, vec![(1, RingMutation::Join { node_id: NodeId(1), thread_count: 1 })]);
  }

  #[test]
  fn recent_mutations_buffer_is_bounded() {
    let gossip = GossipState::new();
    for epoch in 1..=(RECENT_MUTATIONS_CAP as u64 + 5) {
      gossip.record_mutation(epoch, RingMutation::Leave { node_id: NodeId(epoch) });
    }
    assert_eq!(gossip.piggyback().1.len(), RECENT_MUTATIONS_CAP);
  }
}
