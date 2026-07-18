//! The background probing loop: once per `probe_interval_millis`, this
//! node self-refutes any false suspicion against it, directly probes the
//! next peer (round-robin, not random — see this plan's "deliberately
//! out of scope" note on why indirect probing and true randomness aren't
//! needed for this project's scale), checks for timed-out probes via the
//! `FailureDetector`, and — if this node is the elected epoch sequencer
//! — mints a `Leave` mutation for any peer just confirmed dead.

use std::net::TcpStream;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use seisin_core::authority::NodeId;
use seisin_gossip::clock::SystemClock;
use seisin_gossip::failure_detector::{FailureDetector, TimeoutAction};
use seisin_gossip::membership::MemberStatus;
use seisin_gossip::sequencer::{is_sequencer, RingMutation};
use seisin_gossip::wire::{decode_gossip_message, encode_gossip_message, GossipMessage};
use seisin_protocol::{read_frame, write_frame};
use seisin_ring::ring::Ring;

use crate::gossip_state::{apply_ready_mutations, GossipState};
use crate::pool::WorkerPool;

/// Runs forever. Callers spawn this on a dedicated background thread.
pub fn run_gossip_loop(
  self_node_id: NodeId,
  gossip: Arc<GossipState>,
  ring: Arc<RwLock<Ring>>,
  pool: Arc<WorkerPool>,
  probe_interval_millis: u64,
  probe_timeout_millis: u64,
  suspicion_timeout_millis: u64,
) {
  let clock = SystemClock::new();
  let mut detector = FailureDetector::new(&clock, probe_timeout_millis, suspicion_timeout_millis);
  let mut round_robin_index: usize = 0;
  // The target of a probe that hasn't yet been acked or timed out. Only
  // one probe is outstanding at a time — critical for a small cluster:
  // if a new probe were started for the same sole peer on every tick,
  // its timer would perpetually reset and it would never time out.
  let mut outstanding_probe: Option<NodeId> = None;

  loop {
    thread::sleep(Duration::from_millis(probe_interval_millis));

    self_refute_if_falsely_suspected(self_node_id, &gossip);

    if outstanding_probe.is_none() {
      let candidates: Vec<NodeId> = {
        let table = gossip.member_table.lock().unwrap();
        table
          .alive_members()
          .into_iter()
          .filter(|id| *id != self_node_id)
          .collect()
      };
      if !candidates.is_empty() {
        let target = candidates[round_robin_index % candidates.len()];
        round_robin_index = round_robin_index.wrapping_add(1);
        detector.begin_direct_probe(target);
        outstanding_probe = Some(target);
      }
    }
    if let Some(target) = outstanding_probe {
      if send_ping(&gossip, &target) {
        detector.on_ack(target);
        outstanding_probe = None;
      }
    }

    for action in detector.check_timeouts() {
      match action {
        TimeoutAction::EscalateToIndirect(_) => {
          // Deliberately not acted on in this plan — see the plan's
          // "deliberately out of scope" note. The probe simply proceeds
          // to the next timeout (MarkSuspect) untouched.
        }
        TimeoutAction::MarkSuspect(target) => {
          gossip.member_table.lock().unwrap().mark_suspect(target);
        }
        TimeoutAction::MarkDead(target) => {
          gossip.member_table.lock().unwrap().mark_dead(target);
          mint_leave_if_sequencer(self_node_id, &gossip, target);
          if outstanding_probe == Some(target) {
            outstanding_probe = None;
          }
        }
      }
    }

    apply_ready_mutations(&gossip, &ring, self_node_id, &pool);
  }
}

fn self_refute_if_falsely_suspected(self_node_id: NodeId, gossip: &GossipState) {
  let is_suspected = gossip
    .member_table
    .lock()
    .unwrap()
    .get(self_node_id)
    .is_some_and(|update| update.status == MemberStatus::Suspect);
  if is_suspected {
    let refutation = gossip
      .member_table
      .lock()
      .unwrap()
      .confirm_alive_self(self_node_id);
    gossip.member_table.lock().unwrap().merge_update(refutation);
  }
}

fn mint_leave_if_sequencer(self_node_id: NodeId, gossip: &GossipState, dead_node: NodeId) {
  let alive = gossip.member_table.lock().unwrap().alive_members();
  if !is_sequencer(self_node_id, &alive) {
    return;
  }
  let next_epoch = highest_seen_epoch(gossip) + 1;
  gossip.record_mutation(next_epoch, RingMutation::Leave { node_id: dead_node });
}

fn highest_seen_epoch(gossip: &GossipState) -> u64 {
  gossip
    .piggyback()
    .1
    .iter()
    .map(|(epoch, _)| *epoch)
    .max()
    .unwrap_or(0)
}

/// Sends a `Ping` to `target` and merges its `Ack` reply, if any.
/// Returns whether an `Ack` was actually received — the caller uses this
/// to clear the failure detector's tracking for `target`.
fn send_ping(gossip: &GossipState, target: &NodeId) -> bool {
  let address = match gossip.member_table.lock().unwrap().get(*target) {
    Some(update) => update.gossip_address,
    None => return false,
  };
  let Ok(mut stream) = TcpStream::connect(&address) else {
    return false;
  };
  let (updates, mutations) = gossip.piggyback();
  let ping = GossipMessage::Ping { updates, mutations };
  if write_frame(&mut stream, &encode_gossip_message(&ping)).is_err() {
    return false;
  }
  let Ok(payload) = read_frame(&mut stream) else {
    return false;
  };
  let Ok(GossipMessage::Ack { updates, mutations }) = decode_gossip_message(&payload) else {
    return false;
  };
  gossip.merge_incoming(updates, mutations);
  true
}
