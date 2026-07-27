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

/// The moved set: every id whose owning node differs between `old` and
/// `new`. Pure ring math — add, remove, and reweight all reduce to this.
pub fn plan_moves(old: &Ring, new: &Ring, ids: &[DatumId]) -> Vec<Move> {
  ids
    .iter()
    .filter_map(|&id| {
      let source = old.native(id).0;
      let dest = new.native(id).0;
      (source != dest).then_some(Move { id, source, dest })
    })
    .collect()
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

  let mut all_ids = Vec::new();
  for m in &current {
    all_ids.extend(
      list_all_ids(&m.store_address).with_context(|| format!("listing ids on {:?}", m.node_id))?,
    );
  }
  let moves = plan_moves(&old, &new, &all_ids);

  let mut by_pair: HashMap<(NodeId, NodeId), Vec<DatumId>> = HashMap::new();
  for mv in &moves {
    by_pair.entry((mv.source, mv.dest)).or_default().push(mv.id);
  }
  let per_pair: Vec<(NodeId, NodeId, usize)> = by_pair
    .iter()
    .map(|((s, d), ids)| (*s, *d, ids.len()))
    .collect();

  println!(
    "migration plan: {} datum(s) move across {} (source -> dest) pair(s)",
    moves.len(),
    per_pair.len()
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

  // --- Resume, then retire (the only destructive step) ---
  for addr in compute_addrs {
    expect_ack(addr, Request::Resume)?;
  }
  for (addr, transfer_id) in &transfers {
    expect_store_ack(
      addr,
      &StoreRequest::Retire {
        transfer_id: *transfer_id,
      },
    )?;
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

fn list_all_ids(store_addr: &str) -> Result<Vec<DatumId>> {
  const PAGE: u32 = 1024;
  let mut ids = Vec::new();
  let mut after = None;
  loop {
    match store_call(store_addr, &StoreRequest::ListIds { after, limit: PAGE })? {
      StoreResponse::IdList { ids: page, done } => {
        after = page.last().copied();
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

  fn ids(n: usize) -> Vec<DatumId> {
    (0..n).map(|_| DatumId::new()).collect()
  }

  #[test]
  fn add_moves_a_subset_to_the_new_node_and_leaves_the_rest() {
    let old = Ring::from_members(&[(NodeId(1), 1)]);
    let new = Ring::from_members(&[(NodeId(1), 1), (NodeId(2), 1)]);
    let corpus = ids(200);
    let moves = plan_moves(&old, &new, &corpus);
    // Some ids move; every move goes from node 1 to node 2 (nothing was
    // anywhere else), and no id both stays and moves.
    assert!(!moves.is_empty() && moves.len() < corpus.len());
    for mv in &moves {
      assert_eq!(mv.source, NodeId(1));
      assert_eq!(mv.dest, NodeId(2));
    }
  }

  #[test]
  fn remove_moves_every_id_off_the_departing_node_to_the_survivor() {
    let old = Ring::from_members(&[(NodeId(1), 1), (NodeId(2), 1)]);
    let new = Ring::from_members(&[(NodeId(1), 1)]);
    let corpus = ids(200);
    let moves = plan_moves(&old, &new, &corpus);
    for mv in &moves {
      // Only ids that were on the removed node move, and they all land
      // on the survivor.
      assert_eq!(old.native(mv.id).0, NodeId(2));
      assert_eq!(mv.dest, NodeId(1));
    }
    // Every id that was on node 2 is accounted for as a move.
    let on_two = corpus
      .iter()
      .filter(|id| old.native(**id).0 == NodeId(2))
      .count();
    assert_eq!(moves.len(), on_two);
  }

  #[test]
  fn reweight_moves_a_nonempty_subset_toward_the_heavier_node() {
    let old = Ring::from_members(&[(NodeId(1), 1), (NodeId(2), 1)]);
    let new = Ring::from_members(&[(NodeId(1), 1), (NodeId(2), 3)]);
    let corpus = ids(300);
    let moves = plan_moves(&old, &new, &corpus);
    assert!(!moves.is_empty());
    // Net flow is toward node 2 (the heavier one gains placements).
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
    assert!(plan_moves(&ring_a, &ring_b, &ids(200)).is_empty());
  }
}
