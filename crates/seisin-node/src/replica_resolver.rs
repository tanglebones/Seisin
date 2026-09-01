//! The replica-selection/failure-bookkeeping logic shared by every
//! networked store built on the storage ring (`RemoteStore` for blob
//! datums, `RemoteCollectionStore` for ordered collections) — pulled
//! out once both needed the identical "resolve serving replicas, mark
//! one stale, halt on total loss" behavior rather than duplicating it.

use std::sync::Arc;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;

use crate::gossip_state::ClusterState;

pub struct ReplicaResolver {
  cluster: Arc<ClusterState>,
}

impl ReplicaResolver {
  pub fn new(cluster: Arc<ClusterState>) -> Self {
    Self { cluster }
  }

  /// The wrapped cluster state — for callers (like `RemoteCollectionStore`)
  /// that need direct access (e.g. `store_addresses`) beyond what this
  /// type exposes.
  pub fn cluster(&self) -> &Arc<ClusterState> {
    &self.cluster
  }

  /// The id's replica set restricted to nodes that can actually serve
  /// it right now — in the ring, alive, and not stale — in rank order
  /// (rank 0, the primary, first).
  pub fn serving_replicas(&self, id: DatumId, n: u16) -> Vec<NodeId> {
    let replicas = self
      .cluster
      .storage_ring
      .read()
      .unwrap()
      .replicas(id, n as usize);
    let alive = self.cluster.storage_alive.read().unwrap();
    let stale = self.cluster.storage_stale.read().unwrap();
    replicas
      .into_iter()
      .filter(|node| alive.contains(node) && !stale.contains(node))
      .collect()
  }

  /// Excludes `node` from future serving until a driver re-replication
  /// re-admits it — used when a call to it fails mid-operation.
  pub fn mark_stale(&self, node: NodeId) {
    self.cluster.storage_stale.write().unwrap().insert(node);
  }

  /// Engages the coordinated whole-cluster halt for an id whose every
  /// replica is gone, then fail-stops this worker.
  pub fn halt_total_loss(&self, id: DatumId) -> ! {
    let reason =
      format!("cluster halted: every replica of {id:?} is unreachable — total shard loss");
    self.cluster.halt.halt(reason.clone());
    panic!("{reason}");
  }
}
