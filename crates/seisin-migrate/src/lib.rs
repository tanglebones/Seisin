//! The storage migration driver — a client-side admin program, same
//! philosophy as the scan drivers: the cluster only executes explicit
//! commands, never self-initiates rebalancing. Add, planned remove, and
//! capacity reweight are all one mechanism — "the storage ring's
//! member/weight set changes" — driven as: plan → bulk copy → pause →
//! tail → flip → resume → retire. The driver does all ring math and
//! hands storage explicit id lists; storage nodes stay ring-ignorant.
//!
//! Crash safety: a crashed driver leaves the cluster on the old ring
//! with inert extra copies at destinations (unreachable until a flip
//! names them owner). Re-running is idempotent — transfers are
//! Put-based (last write per id wins) under a fresh transfer id, and the
//! flip is held under the pause. `Retire` (the only destructive step) is
//! gated behind every compute node confirming the flip.

use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use anyhow::{bail, Context, Result};

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_protocol::store_wire::{store_call, StoreRequest, StoreResponse};
use seisin_protocol::{Request, Response, StorageMember};
use seisin_ring::ring::Ring;

/// A single datum's move: its id and the owning nodes it moves between.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Move {
  pub id: DatumId,
  pub source: NodeId,
  pub dest: NodeId,
}

/// The moved set over replica sets: for each `(id, n)`, one copy per
/// node that is a replica of `id` under `new` but not under `old`,
/// sourced from `id`'s current primary (rank-0 old replica, which holds
/// it and receives every write during the copy so the dirty tail is
/// complete). Pure ring math — add, remove, reweight, and re-replication
/// all reduce to this. At n=1 it is exactly the old single-owner move.
pub fn plan_moves(old: &Ring, new: &Ring, ids: &[(DatumId, u16)]) -> Vec<Move> {
  let mut moves = Vec::new();
  for &(id, n) in ids {
    let old_set = old.replicas(id, n as usize);
    // Provisional source: the current primary (rank 0), which holds the
    // datum. `migrate` reroutes this to a reachable old replica when the
    // primary is itself the node being recovered-from (a crashed node).
    let Some(&source) = old_set.first() else {
      continue; // no current replica — nothing to copy
    };
    for dest in new.replicas(id, n as usize) {
      if !old_set.contains(&dest) {
        moves.push(Move { id, source, dest });
      }
    }
  }
  moves
}

/// The superseded copies to reclaim after the flip: for each `(id, n)`,
/// the nodes that were a replica under `old` but are not under `new`.
fn plan_drops(old: &Ring, new: &Ring, ids: &[(DatumId, u16)]) -> Vec<(NodeId, DatumId)> {
  let mut drops = Vec::new();
  for &(id, n) in ids {
    let new_set = new.replicas(id, n as usize);
    for node in old.replicas(id, n as usize) {
      if !new_set.contains(&node) {
        drops.push((node, id));
      }
    }
  }
  drops
}

/// The outcome of a `migrate` run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
  pub applied: bool,
  pub total_moves: usize,
  /// `(source, dest, count)` per moving pair.
  pub per_pair: Vec<(NodeId, NodeId, usize)>,
}

/// Poll interval while waiting for an async bulk copy to finish — a
/// courtesy pause so the driver doesn't hot-spin the source node.
const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Runs the full migration to `proposed` (an add, remove, or reweight —
/// whichever the proposed member/weight set expresses). Prints the plan;
/// only mutates the cluster when `apply` is true (two-phase plan→execute
/// per the house guidelines).
pub fn migrate(
  compute_addrs: &[String],
  proposed: &[StorageMember],
  apply: bool,
) -> Result<Report> {
  if compute_addrs.is_empty() {
    bail!("need at least one compute node address");
  }

  // --- Plan ---
  let current = get_cluster_config(&compute_addrs[0])?;
  let current_addr = addr_map(&current);

  // Resolve each proposed member's real log id via Identify — this also
  // proves the node is reachable and is who the plan says it is.
  let mut resolved = Vec::with_capacity(proposed.len());
  for m in proposed {
    let (node_id, log_id) = identify(&m.store_address)
      .with_context(|| format!("identifying proposed node {:?}", m.node_id))?;
    if node_id != m.node_id {
      bail!(
        "proposed node at {} reports id {node_id:?}, expected {:?}",
        m.store_address,
        m.node_id
      );
    }
    resolved.push(StorageMember {
      log_id,
      ..m.clone()
    });
  }
  let proposed_addr = addr_map(&resolved);

  let old = ring_of(&current);
  let new = ring_of(&resolved);

  // Enumerate every (id, n) across current sources, deduped — a
  // replicated datum is listed by each of its replicas. An unreachable
  // source is skipped (a `recover` run has dead nodes in the ring; every
  // recoverable datum is still listed by a surviving replica).
  let mut id_factors: HashMap<DatumId, u16> = HashMap::new();
  let mut reachable: std::collections::HashSet<NodeId> = std::collections::HashSet::new();
  for m in &current {
    match list_all_ids(&m.store_address) {
      Ok(page) => {
        reachable.insert(m.node_id);
        for (id, n) in page {
          id_factors.insert(id, n);
        }
      }
      Err(e) => println!("  (skipping unreachable source {:?}: {e})", m.node_id),
    }
  }
  let all_ids: Vec<(DatumId, u16)> = id_factors.iter().map(|(id, n)| (*id, *n)).collect();
  let drops = plan_drops(&old, &new, &all_ids);
  // Reroute each copy's source off any unreachable node (a `recover`
  // run's crashed replica) onto a reachable old replica of the same
  // datum; a datum with no reachable old replica is unrecoverable and
  // dropped from the plan.
  let moves: Vec<Move> = plan_moves(&old, &new, &all_ids)
    .into_iter()
    .filter_map(|mut mv| {
      if !reachable.contains(&mv.source) {
        let n = *id_factors.get(&mv.id).unwrap_or(&1);
        mv.source = old
          .replicas(mv.id, n as usize)
          .into_iter()
          .find(|node| reachable.contains(node))?;
      }
      Some(mv)
    })
    .collect();

  let mut by_pair: HashMap<(NodeId, NodeId), Vec<DatumId>> = HashMap::new();
  for mv in &moves {
    by_pair.entry((mv.source, mv.dest)).or_default().push(mv.id);
  }
  let per_pair: Vec<(NodeId, NodeId, usize)> = by_pair
    .iter()
    .map(|((s, d), ids)| (*s, *d, ids.len()))
    .collect();

  println!(
    "migration plan: {} replica copy(ies) across {} (source -> dest) pair(s); {} superseded copy(ies) to reclaim",
    moves.len(),
    per_pair.len(),
    drops.len()
  );
  for (s, d, n) in &per_pair {
    println!("  {s:?} -> {d:?}: {n}");
  }

  if !apply {
    println!("dry run — pass --apply to execute");
    return Ok(Report {
      applied: false,
      total_moves: moves.len(),
      per_pair,
    });
  }

  // --- Bulk copy (client writes keep flowing) ---
  // (source_addr, transfer_id) per moving pair, for the tail/retire.
  let mut transfers: Vec<(String, DatumId)> = Vec::new();
  for ((source, dest), ids) in &by_pair {
    let source_addr = current_addr
      .get(source)
      .with_context(|| format!("no address for source {source:?}"))?;
    let dest_addr = proposed_addr
      .get(dest)
      .with_context(|| format!("no address for dest {dest:?}"))?;
    let transfer_id = DatumId::new();
    expect_store_ack(
      source_addr,
      &StoreRequest::Transfer {
        transfer_id,
        ids: ids.clone(),
        dest_address: dest_addr.clone(),
      },
    )?;
    loop {
      match store_call(source_addr, &StoreRequest::TransferStatus { transfer_id })? {
        StoreResponse::TransferProgress { done: true, .. } => break,
        StoreResponse::TransferProgress { .. } => thread::sleep(POLL_INTERVAL),
        other => bail!("TransferStatus at {source_addr} got {other:?}"),
      }
    }
    transfers.push((source_addr.clone(), transfer_id));
  }

  // --- Pause every compute node ---
  for addr in compute_addrs {
    expect_ack(
      addr,
      Request::Pause {
        reason: "migrating".to_string(),
      },
    )?;
  }

  // --- Tail: re-send the dirty set on each source ---
  for (addr, transfer_id) in &transfers {
    expect_store_ack(
      addr,
      &StoreRequest::FinishTransfer {
        transfer_id: *transfer_id,
      },
    )?;
  }

  // --- Flip: install the new ring everywhere, held under the pause ---
  for addr in compute_addrs {
    expect_ack(
      addr,
      Request::InstallStorageRing {
        members: resolved.clone(),
      },
    )?;
  }

  // --- Resume, then reclaim superseded copies (the only destructive
  // step): delete each dropped replica's now-inert copy. This is the
  // only place data is removed, and it runs after the flip named the new
  // replica set, so a datum always has a live replica throughout. ---
  for addr in compute_addrs {
    expect_ack(addr, Request::Resume)?;
  }
  for (node, id) in &drops {
    let Some(addr) = current_addr.get(node) else {
      continue;
    };
    // Best-effort: a dropped node may itself be the dead one (a recover
    // run). Its copy is already inert (the ring no longer names it), so a
    // failed delete only leaks space until compaction reclaims it.
    let _ = store_call(addr, &StoreRequest::Delete { id: *id });
  }

  println!("migration applied");
  Ok(Report {
    applied: true,
    total_moves: moves.len(),
    per_pair,
  })
}

/// Resume-after-halt: verify every storage ring member's identity, then
/// clear the halt on every compute node. Reads the identity book via
/// `GetClusterConfig` (served while halted), `Identify`s each node, and
/// refuses (halt stands) on any node-id/log-id mismatch — an impostor
/// (same node id, blank or wrong disk) is provably not holding the acked
/// data.
pub fn resume(compute_addrs: &[String]) -> Result<()> {
  if compute_addrs.is_empty() {
    bail!("need at least one compute node address");
  }
  let members = get_cluster_config(&compute_addrs[0])?;
  for m in &members {
    let (node_id, log_id) = identify(&m.store_address)
      .with_context(|| format!("identifying {:?} at {}", m.node_id, m.store_address))?;
    if node_id != m.node_id || log_id != m.log_id {
      bail!(
        "impostor detected: storage node at {} reports {node_id:?}/{log_id:?}, \
         expected {:?}/{:?} — halt stands",
        m.store_address,
        m.node_id,
        m.log_id
      );
    }
  }
  for addr in compute_addrs {
    expect_ack(addr, Request::ClearHalt)?;
  }
  println!(
    "resume: identity verified for {} storage node(s); halt cleared",
    members.len()
  );
  Ok(())
}

/// Recover-after-loss: drop every unreachable storage node from the ring
/// and restore the replication factor of its shards onto the survivors.
/// Reads the current ring, `Identify`s each member to find the dead
/// ones, and runs a migration to the surviving ring (same weights). A
/// datum with no surviving replica (an N=1 total loss) cannot be
/// recovered this way — restart that node on its log directory and
/// `resume` instead.
pub fn recover(compute_addrs: &[String], apply: bool) -> Result<Report> {
  if compute_addrs.is_empty() {
    bail!("need at least one compute node address");
  }
  let current = get_cluster_config(&compute_addrs[0])?;
  let survivors: Vec<StorageMember> = current
    .iter()
    .filter(|m| identify(&m.store_address).is_ok())
    .cloned()
    .collect();
  let dropped = current.len() - survivors.len();
  if dropped == 0 {
    println!("recover: every storage node is reachable; nothing to do");
    return Ok(Report {
      applied: false,
      total_moves: 0,
      per_pair: Vec::new(),
    });
  }
  println!("recover: {dropped} unreachable node(s) dropped; restoring replication onto survivors");
  migrate(compute_addrs, &survivors, apply)
}

// --- helpers ---

fn addr_map(members: &[StorageMember]) -> HashMap<NodeId, String> {
  members
    .iter()
    .map(|m| (m.node_id, m.store_address.clone()))
    .collect()
}

fn ring_of(members: &[StorageMember]) -> Ring {
  Ring::from_members(
    &members
      .iter()
      .map(|m| (m.node_id, m.weight))
      .collect::<Vec<_>>(),
  )
}

fn get_cluster_config(compute_addr: &str) -> Result<Vec<StorageMember>> {
  match seisin_client::call(compute_addr, Request::GetClusterConfig)? {
    Response::ClusterConfig { members } => Ok(members),
    other => bail!("GetClusterConfig at {compute_addr} got {other:?}"),
  }
}

fn identify(store_addr: &str) -> Result<(NodeId, DatumId)> {
  match store_call(store_addr, &StoreRequest::Identify)? {
    StoreResponse::Identity { node_id, log_id } => Ok((node_id, log_id)),
    other => bail!("Identify at {store_addr} got {other:?}"),
  }
}

fn list_all_ids(store_addr: &str) -> Result<Vec<(DatumId, u16)>> {
  const PAGE: u32 = 1024;
  let mut ids = Vec::new();
  let mut after = None;
  loop {
    match store_call(store_addr, &StoreRequest::ListIds { after, limit: PAGE })? {
      StoreResponse::IdList { ids: page, done } => {
        after = page.last().map(|(id, _n)| *id);
        let stop = done || page.is_empty();
        ids.extend(page);
        if stop {
          break;
        }
      }
      other => bail!("ListIds at {store_addr} got {other:?}"),
    }
  }
  Ok(ids)
}

fn expect_ack(compute_addr: &str, request: Request) -> Result<()> {
  match seisin_client::call(compute_addr, request)? {
    Response::Ack => Ok(()),
    other => bail!("expected Ack from {compute_addr}, got {other:?}"),
  }
}

fn expect_store_ack(store_addr: &str, request: &StoreRequest) -> Result<()> {
  match store_call(store_addr, request)? {
    StoreResponse::Ack => Ok(()),
    other => bail!("expected Ack from storage {store_addr}, got {other:?}"),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// A corpus of `count` ids, each at replication factor `n`.
  fn corpus(count: usize, n: u16) -> Vec<(DatumId, u16)> {
    (0..count).map(|_| (DatumId::new(), n)).collect()
  }

  #[test]
  fn add_moves_a_subset_to_the_new_node_and_leaves_the_rest() {
    let old = Ring::from_members(&[(NodeId(1), 1)]);
    let new = Ring::from_members(&[(NodeId(1), 1), (NodeId(2), 1)]);
    let ids = corpus(200, 1);
    let moves = plan_moves(&old, &new, &ids);
    assert!(!moves.is_empty() && moves.len() < ids.len());
    for mv in &moves {
      assert_eq!(mv.source, NodeId(1));
      assert_eq!(mv.dest, NodeId(2));
    }
  }

  #[test]
  fn remove_moves_every_id_off_the_departing_node_to_the_survivor() {
    let old = Ring::from_members(&[(NodeId(1), 1), (NodeId(2), 1)]);
    let new = Ring::from_members(&[(NodeId(1), 1)]);
    let ids = corpus(200, 1);
    let moves = plan_moves(&old, &new, &ids);
    for mv in &moves {
      assert_eq!(old.native(mv.id).0, NodeId(2));
      assert_eq!(mv.dest, NodeId(1));
      // Source is the drained node itself — alive during a planned
      // remove, and the only holder of its single copy.
      assert_eq!(mv.source, NodeId(2));
    }
    let on_two = ids
      .iter()
      .filter(|(id, _)| old.native(*id).0 == NodeId(2))
      .count();
    assert_eq!(moves.len(), on_two);
  }

  #[test]
  fn reweight_moves_a_nonempty_subset_toward_the_heavier_node() {
    let old = Ring::from_members(&[(NodeId(1), 1), (NodeId(2), 1)]);
    let new = Ring::from_members(&[(NodeId(1), 1), (NodeId(2), 3)]);
    let moves = plan_moves(&old, &new, &corpus(300, 1));
    assert!(!moves.is_empty());
    let to_two = moves.iter().filter(|m| m.dest == NodeId(2)).count();
    let to_one = moves.iter().filter(|m| m.dest == NodeId(1)).count();
    assert!(
      to_two > to_one,
      "expected net flow to node 2: {to_two} vs {to_one}"
    );
  }

  #[test]
  fn an_identical_ring_moves_nothing() {
    let ring_a = Ring::from_members(&[(NodeId(1), 2), (NodeId(2), 1)]);
    let ring_b = Ring::from_members(&[(NodeId(1), 2), (NodeId(2), 1)]);
    assert!(plan_moves(&ring_a, &ring_b, &corpus(200, 1)).is_empty());
  }

  #[test]
  fn adding_a_node_replicates_each_datum_to_a_new_replica() {
    // With N=2 and a 3rd node added, some datums gain node 3 as a replica.
    let old = Ring::from_members(&[(NodeId(1), 1), (NodeId(2), 1)]);
    let new = Ring::from_members(&[(NodeId(1), 1), (NodeId(2), 1), (NodeId(3), 1)]);
    let ids = corpus(200, 2);
    let moves = plan_moves(&old, &new, &ids);
    assert!(moves.iter().any(|m| m.dest == NodeId(3)));
    // Every move's source is an old replica of that id, and its dest is a
    // new replica that was not already an old replica.
    for mv in &moves {
      let old_set = old.replicas(mv.id, 2);
      let new_set = new.replicas(mv.id, 2);
      assert!(old_set.contains(&mv.source));
      assert!(new_set.contains(&mv.dest) && !old_set.contains(&mv.dest));
    }
  }

  #[test]
  fn recover_re_replication_restores_two_replicas_among_survivors() {
    // A 3-node, N=2 cluster loses node 3: re-replication restores N onto
    // the survivors. (Sourcing off the dead node is `migrate`'s reroute
    // job, exercised end-to-end in the integration suite.)
    let old = Ring::from_members(&[(NodeId(1), 1), (NodeId(2), 1), (NodeId(3), 1)]);
    let new = Ring::from_members(&[(NodeId(1), 1), (NodeId(2), 1)]);
    let ids = corpus(300, 2);
    let moves = plan_moves(&old, &new, &ids);
    assert!(
      !moves.is_empty(),
      "losing a replica should trigger re-replication"
    );
    // Every datum ends with 2 replicas among the two survivors.
    for (id, _) in &ids {
      let new_set = new.replicas(*id, 2);
      assert_eq!(new_set.len(), 2);
      for node in &new_set {
        assert!(*node == NodeId(1) || *node == NodeId(2));
      }
    }
  }

  #[test]
  fn drops_reclaim_only_nodes_that_left_the_replica_set() {
    let old = Ring::from_members(&[(NodeId(1), 1), (NodeId(2), 1)]);
    let new = Ring::from_members(&[(NodeId(1), 1)]);
    let ids = corpus(200, 1);
    let drops = plan_drops(&old, &new, &ids);
    for (node, id) in &drops {
      assert_eq!(*node, NodeId(2)); // only the removed node's copies are reclaimed
      assert!(!new.replicas(*id, 1).contains(node));
    }
  }
}
