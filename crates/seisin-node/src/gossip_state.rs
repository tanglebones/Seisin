//! Shared, cross-thread gossip state: the membership table, the
//! epoch-ordered mutation log, and a small buffer of recently-seen
//! mutations re-gossiped on every outbound message as cheap insurance
//! against one lost message (this project doesn't implement full
//! SWIM-style epidemic retransmission tracking — see the design doc and
//! this plan's "deliberately out of scope" note).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_gossip::membership::{MemberRole, MemberTable, MemberUpdate};
use std::collections::HashMap;

use crate::halt::HaltState;
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
    let mutations = self
      .recent_mutations
      .lock()
      .unwrap()
      .iter()
      .copied()
      .collect();
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

/// Everything a ready ring mutation can touch, bundled so the three
/// apply call sites (gossip server, gossip loop, tests) stay in sync:
/// mutations route by the affected node's ROLE (from the member
/// table) — compute mutations to the compute ring as before; a storage
/// join extends the storage ring + store-address book; a storage
/// leave (confirmed dead) engages the fail-stop halt.
pub struct ClusterState {
  pub compute_ring: Arc<RwLock<Ring>>,
  pub storage_ring: Arc<RwLock<Ring>>,
  pub store_addresses: Arc<RwLock<HashMap<NodeId, String>>>,
  /// Log id per storage member — the identity resume-after-halt
  /// verifies against. Populated by `InstallStorageRing` (driver-
  /// provided) and reconciled from gossiped `MemberUpdate.log_id`.
  pub identity_book: Arc<RwLock<HashMap<NodeId, DatumId>>>,
  pub halt: Arc<HaltState>,
}

impl ClusterState {
  /// A compute-only cluster state: the storage ring and its address /
  /// identity books start empty, with a fresh halt. For single-tier
  /// compute deployments and tests that exercise no storage tier.
  pub fn compute_only(compute_ring: Arc<RwLock<Ring>>) -> Self {
    Self {
      compute_ring,
      storage_ring: Arc::new(RwLock::new(Ring::from_members(&[]))),
      store_addresses: Arc::new(RwLock::new(HashMap::new())),
      identity_book: Arc::new(RwLock::new(HashMap::new())),
      halt: Arc::new(HaltState::new()),
    }
  }
}

/// Copies every known storage member's non-zero log id from the member
/// table into the identity book. Idempotent; called after each gossip
/// apply so config-seeded storage members (which never emit a Join
/// mutation) still land in the book once their self-update propagates.
pub fn reconcile_identity_book(gossip: &GossipState, cluster: &ClusterState) {
  let members = gossip.member_table.lock().unwrap().all();
  let mut book = cluster.identity_book.write().unwrap();
  for update in members {
    if update.role == MemberRole::Storage && update.log_id != [0u8; 16] {
      book.insert(update.node_id, DatumId::from_bytes(update.log_id));
    }
  }
}

/// Applies every ring mutation that's now ready (in epoch order),
/// role-routed per `ClusterState`; compute-ring changes then evict
/// non-native cache entries and release a departed node's locks — see
/// the design doc's "Cache Invalidation on Ring Membership Change" and
/// "Crash Detection & Lock Release" sections.
pub fn apply_ready_mutations(
  gossip: &GossipState,
  cluster: &ClusterState,
  self_node_id: NodeId,
  pool: &WorkerPool,
) {
  let ready = gossip.mutation_log.lock().unwrap().drain_applicable();
  if ready.is_empty() {
    return;
  }
  let mut compute_changed = false;
  for mutation in &ready {
    let node_id = match *mutation {
      RingMutation::Join { node_id, .. } | RingMutation::Leave { node_id } => node_id,
    };
    // Unknown members default to Compute — pre-Part-B behavior.
    let member = gossip.member_table.lock().unwrap().get(node_id);
    let role = member
      .as_ref()
      .map(|m| m.role)
      .unwrap_or(MemberRole::Compute);
    match (role, *mutation) {
      (
        MemberRole::Compute,
        RingMutation::Join {
          node_id,
          thread_count,
        },
      ) => {
        cluster
          .compute_ring
          .write()
          .unwrap()
          .apply_join(node_id, thread_count);
        compute_changed = true;
      }
      (MemberRole::Compute, RingMutation::Leave { node_id }) => {
        cluster.compute_ring.write().unwrap().apply_leave(node_id);
        compute_changed = true;
      }
      (MemberRole::Storage, RingMutation::Join { node_id, .. }) => {
        // Availability only — a joined storage node records its store
        // address (and log id, if gossiped) so a driver can migrate it
        // into the ring later. It does NOT enter the storage ring here:
        // auto-extending a jump-hash ring re-homes existing keys whose
        // bytes never moved, a read-miss once real data exists. The ring
        // changes exclusively via a driver `InstallStorageRing`.
        if let Some(update) = &member {
          if !update.store_address.is_empty() {
            cluster
              .store_addresses
              .write()
              .unwrap()
              .insert(node_id, update.store_address.clone());
          }
          if update.log_id != [0u8; 16] {
            cluster
              .identity_book
              .write()
              .unwrap()
              .insert(node_id, DatumId::from_bytes(update.log_id));
          }
        }
      }
      (MemberRole::Storage, RingMutation::Leave { node_id }) => {
        // No replication in v1: a lost storage member that is still
        // serving the ring is fail-stop for the cluster. A drained node
        // left the ring at flip time, so its later Leave finds it absent
        // and is ignored — planned removal avoids the halt for free.
        if cluster.storage_ring.read().unwrap().contains(node_id) {
          cluster.halt.halt(format!(
            "cluster halted: storage node {node_id:?} confirmed dead — fail-stop (no replication in v1)"
          ));
        }
      }
    }
  }
  if compute_changed {
    let ring_for_cache = Arc::clone(&cluster.compute_ring);
    pool.evict_non_native(Arc::new(move |id| {
      ring_for_cache.read().unwrap().native(id).0 == self_node_id
    }));
    for mutation in &ready {
      if let RingMutation::Leave { node_id } = *mutation {
        pool.release_locks_held_by(node_id);
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use seisin_core::datum::DatumId;
  use seisin_gossip::membership::{Incarnation, MemberRole, MemberStatus};

  fn sample_update(node_id: u64) -> MemberUpdate {
    MemberUpdate {
      node_id: NodeId(node_id),
      incarnation: Incarnation(0),
      status: MemberStatus::Alive,
      client_address: "127.0.0.1:7878".to_string(),
      gossip_address: "127.0.0.1:8878".to_string(),
      thread_count: 1,
      role: MemberRole::Compute,
      capacity_weight: 0,
      store_address: String::new(),
      log_id: [0u8; 16],
    }
  }

  #[test]
  fn merge_incoming_applies_updates_and_mutations() {
    let gossip = GossipState::new();
    gossip.merge_incoming(
      vec![sample_update(1)],
      vec![(
        1,
        RingMutation::Join {
          node_id: NodeId(1),
          thread_count: 1,
        },
      )],
    );
    assert_eq!(
      gossip.member_table.lock().unwrap().get(NodeId(1)),
      Some(sample_update(1))
    );
    assert_eq!(
      gossip.mutation_log.lock().unwrap().drain_applicable(),
      vec![RingMutation::Join {
        node_id: NodeId(1),
        thread_count: 1
      }]
    );
  }

  #[test]
  fn piggyback_includes_merged_updates_and_recorded_mutations() {
    let gossip = GossipState::new();
    gossip.merge_incoming(vec![sample_update(1)], vec![]);
    gossip.record_mutation(
      1,
      RingMutation::Join {
        node_id: NodeId(1),
        thread_count: 1,
      },
    );
    let (updates, mutations) = gossip.piggyback();
    assert_eq!(updates, vec![sample_update(1)]);
    assert_eq!(
      mutations,
      vec![(
        1,
        RingMutation::Join {
          node_id: NodeId(1),
          thread_count: 1
        }
      )]
    );
  }

  #[test]
  fn recent_mutations_buffer_is_bounded() {
    let gossip = GossipState::new();
    for epoch in 1..=(RECENT_MUTATIONS_CAP as u64 + 5) {
      gossip.record_mutation(
        epoch,
        RingMutation::Leave {
          node_id: NodeId(epoch),
        },
      );
    }
    assert_eq!(gossip.piggyback().1.len(), RECENT_MUTATIONS_CAP);
  }

  #[test]
  fn apply_ready_mutations_releases_locks_held_by_a_departing_node() {
    use crate::pool::WorkerPool;
    use seisin_core::datum::DatumId;
    use seisin_core::store::InMemoryStore;
    use seisin_ops::registry::OpRegistry;
    use std::net::TcpListener;

    let node_a = NodeId(1);
    let node_b = NodeId(2);
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_a, 1), (node_b, 1)])));

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let pool = WorkerPool::spawn(
      Arc::new(InMemoryStore::new()),
      1,
      Arc::new(OpRegistry::new()),
      Arc::clone(&ring),
      node_a,
      listener,
      Arc::new(std::collections::HashMap::new()),
      Arc::new(crate::index_handler::IndexKindRegistry::new()),
    );

    let gossip = GossipState::new();
    gossip.record_mutation(1, RingMutation::Leave { node_id: node_b });

    // This shouldn't panic, and the ring should reflect the departure
    // afterward — the release-broadcast itself is exercised in
    // isolation by pool.rs's own test (Task 2) and proven end-to-end
    // by Task 7's full crash integration test.
    let cluster = test_cluster(Arc::clone(&ring));
    apply_ready_mutations(&gossip, &cluster, node_a, &pool);
    assert_eq!(ring.read().unwrap().native(DatumId::new()).0, node_a);
  }

  fn test_cluster(compute_ring: Arc<RwLock<Ring>>) -> ClusterState {
    ClusterState {
      compute_ring,
      storage_ring: Arc::new(RwLock::new(Ring::from_members(&[]))),
      store_addresses: Arc::new(RwLock::new(HashMap::new())),
      identity_book: Arc::new(RwLock::new(HashMap::new())),
      halt: Arc::new(HaltState::new()),
    }
  }

  #[test]
  fn reconcile_identity_book_copies_storage_log_ids() {
    let cluster = test_cluster(Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)]))));
    let gossip = GossipState::new();
    {
      let mut table = gossip.member_table.lock().unwrap();
      // A storage member with a real log id -> copied.
      let mut with_id = storage_member(9, 4, "127.0.0.1:6999");
      with_id.log_id = [3u8; 16];
      table.merge_update(with_id);
      // A storage member with a zero log id -> skipped.
      table.merge_update(storage_member(8, 1, "127.0.0.1:6998"));
      // A compute member -> skipped even if it somehow carried a log id.
      table.merge_update(sample_update(1));
    }
    reconcile_identity_book(&gossip, &cluster);
    let book = cluster.identity_book.read().unwrap();
    assert_eq!(book.get(&NodeId(9)), Some(&DatumId::from_bytes([3u8; 16])));
    assert!(!book.contains_key(&NodeId(8)));
    assert!(!book.contains_key(&NodeId(1)));
  }

  fn test_pool(ring: &Arc<RwLock<Ring>>) -> WorkerPool {
    use seisin_core::store::InMemoryStore;
    use seisin_ops::registry::OpRegistry;
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    WorkerPool::spawn(
      Arc::new(InMemoryStore::new()),
      1,
      Arc::new(OpRegistry::new()),
      Arc::clone(ring),
      NodeId(1),
      listener,
      Arc::new(std::collections::HashMap::new()),
      Arc::new(crate::index_handler::IndexKindRegistry::new()),
    )
  }

  fn storage_member(node_id: u64, weight: u32, addr: &str) -> MemberUpdate {
    MemberUpdate {
      node_id: NodeId(node_id),
      incarnation: Incarnation(0),
      status: MemberStatus::Alive,
      client_address: String::new(),
      gossip_address: String::new(),
      thread_count: 1,
      role: MemberRole::Storage,
      capacity_weight: weight,
      store_address: addr.to_string(),
      log_id: [0u8; 16],
    }
  }

  #[test]
  fn a_storage_join_records_availability_without_touching_the_ring() {
    let compute_ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let pool = test_pool(&compute_ring);
    let cluster = test_cluster(Arc::clone(&compute_ring)); // empty storage ring
    let gossip = GossipState::new();
    let mut member = storage_member(9, 4, "127.0.0.1:6999");
    member.log_id = [5u8; 16];
    gossip.member_table.lock().unwrap().merge_update(member);
    gossip.record_mutation(
      1,
      RingMutation::Join {
        node_id: NodeId(9),
        thread_count: 1,
      },
    );
    apply_ready_mutations(&gossip, &cluster, NodeId(1), &pool);
    // The node did NOT enter the storage ring — availability only.
    assert!(!cluster.storage_ring.read().unwrap().contains(NodeId(9)));
    assert!(cluster.storage_ring.read().unwrap().node_ids().is_empty());
    // ...but the address and log identity books learned it.
    assert_eq!(
      cluster.store_addresses.read().unwrap().get(&NodeId(9)),
      Some(&"127.0.0.1:6999".to_string())
    );
    assert_eq!(
      cluster.identity_book.read().unwrap().get(&NodeId(9)),
      Some(&DatumId::from_bytes([5u8; 16]))
    );
    // ...and neither the compute ring nor the halt were touched.
    assert_eq!(
      compute_ring.read().unwrap().native(DatumId::new()).0,
      NodeId(1)
    );
    assert!(!cluster.halt.is_halted());
  }

  #[test]
  fn a_storage_leave_engages_the_halt_when_the_node_is_in_the_ring() {
    let compute_ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let pool = test_pool(&compute_ring);
    // The departing node is actually serving the storage ring.
    let cluster = ClusterState {
      compute_ring: Arc::clone(&compute_ring),
      storage_ring: Arc::new(RwLock::new(Ring::from_members(&[(NodeId(9), 1)]))),
      store_addresses: Arc::new(RwLock::new(HashMap::new())),
      identity_book: Arc::new(RwLock::new(HashMap::new())),
      halt: Arc::new(HaltState::new()),
    };
    let gossip = GossipState::new();
    gossip
      .member_table
      .lock()
      .unwrap()
      .merge_update(storage_member(9, 4, "127.0.0.1:6999"));
    gossip.record_mutation(1, RingMutation::Leave { node_id: NodeId(9) });
    apply_ready_mutations(&gossip, &cluster, NodeId(1), &pool);
    assert!(cluster.halt.is_halted());
    let reason = cluster.halt.reason().unwrap();
    assert!(reason.contains("storage node"), "{reason}");
    assert!(reason.contains("9"), "{reason}");
    assert_eq!(
      compute_ring.read().unwrap().native(DatumId::new()).0,
      NodeId(1) // compute ring untouched
    );
  }

  #[test]
  fn a_storage_leave_is_ignored_when_the_node_is_not_in_the_ring() {
    let compute_ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let pool = test_pool(&compute_ring);
    let cluster = test_cluster(Arc::clone(&compute_ring)); // empty storage ring
    let gossip = GossipState::new();
    gossip
      .member_table
      .lock()
      .unwrap()
      .merge_update(storage_member(9, 4, "127.0.0.1:6999"));
    gossip.record_mutation(1, RingMutation::Leave { node_id: NodeId(9) });
    apply_ready_mutations(&gossip, &cluster, NodeId(1), &pool);
    // A drained node (no longer in the ring) leaving does NOT halt.
    assert!(!cluster.halt.is_halted());
  }
}
