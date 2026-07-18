//! SWIM's direct-probe → indirect-probe → suspect → dead state machine,
//! driven entirely by explicit calls so it's deterministic and testable
//! without real sleeping. This type doesn't pick which peers to probe or
//! send anything over a socket — see Sub-project 2b-iii-c for the real
//! networked loop that drives this and acts on its output.

use std::collections::HashMap;

use seisin_core::authority::NodeId;

use crate::clock::{ClockSource, Tick};

/// How long to wait for a direct ack before escalating to indirect
/// probing.
pub const PROBE_TIMEOUT_MILLIS: u64 = 1_000;
/// How long a member stays `Suspect` before being declared `Dead` with
/// no refutation.
pub const SUSPICION_TIMEOUT_MILLIS: u64 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeState {
  AwaitingDirectAck { started_at: Tick },
  AwaitingIndirectAck { started_at: Tick },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutAction {
  /// The direct probe to this node timed out; the caller should pick a
  /// handful of random peers and ask them to probe it on this node's
  /// behalf (a `PingReq`).
  EscalateToIndirect(NodeId),
  /// Indirect probing also timed out with no ack; the caller should mark
  /// this node `Suspect` in the `MemberTable`.
  MarkSuspect(NodeId),
  /// This node has been `Suspect` longer than the suspicion timeout with
  /// no refutation; the caller should mark it `Dead`.
  MarkDead(NodeId),
}

pub struct FailureDetector<'c, C: ClockSource> {
  clock: &'c C,
  probes: HashMap<NodeId, ProbeState>,
  suspected_since: HashMap<NodeId, Tick>,
}

impl<'c, C: ClockSource> FailureDetector<'c, C> {
  pub fn new(clock: &'c C) -> Self {
    Self {
      clock,
      probes: HashMap::new(),
      suspected_since: HashMap::new(),
    }
  }

  /// Records that a direct probe to `target` was just sent.
  pub fn begin_direct_probe(&mut self, target: NodeId) {
    self.probes.insert(target, ProbeState::AwaitingDirectAck { started_at: self.clock.now() });
  }

  /// An ack (direct or relayed via an indirect probe) arrived from
  /// `target` — clears any outstanding probe or suspicion tracking for
  /// it.
  pub fn on_ack(&mut self, target: NodeId) {
    self.probes.remove(&target);
    self.suspected_since.remove(&target);
  }

  /// Call periodically (e.g. once per probe interval) to check for
  /// timed-out probes and expired suspicions. Returns every action the
  /// caller needs to take as a result, in no particular order.
  pub fn check_timeouts(&mut self) -> Vec<TimeoutAction> {
    let now = self.clock.now();
    let mut actions = Vec::new();

    let mut escalate = Vec::new();
    let mut suspect = Vec::new();
    for (&target, state) in self.probes.iter() {
      match state {
        ProbeState::AwaitingDirectAck { started_at } => {
          if now.0.saturating_sub(started_at.0) >= PROBE_TIMEOUT_MILLIS {
            escalate.push(target);
          }
        }
        ProbeState::AwaitingIndirectAck { started_at } => {
          if now.0.saturating_sub(started_at.0) >= PROBE_TIMEOUT_MILLIS {
            suspect.push(target);
          }
        }
      }
    }
    for target in escalate {
      self.probes.insert(target, ProbeState::AwaitingIndirectAck { started_at: now });
      actions.push(TimeoutAction::EscalateToIndirect(target));
    }
    for target in suspect {
      self.probes.remove(&target);
      self.suspected_since.insert(target, now);
      actions.push(TimeoutAction::MarkSuspect(target));
    }

    let mut dead = Vec::new();
    for (&target, &since) in self.suspected_since.iter() {
      if now.0.saturating_sub(since.0) >= SUSPICION_TIMEOUT_MILLIS {
        dead.push(target);
      }
    }
    for target in dead {
      self.suspected_since.remove(&target);
      actions.push(TimeoutAction::MarkDead(target));
    }

    actions
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::clock::FakeClock;

  #[test]
  fn no_action_before_the_probe_timeout_elapses() {
    let clock = FakeClock::new();
    let mut fd = FailureDetector::new(&clock);
    fd.begin_direct_probe(NodeId(1));
    clock.advance(PROBE_TIMEOUT_MILLIS - 1);
    assert_eq!(fd.check_timeouts(), vec![]);
  }

  #[test]
  fn escalates_to_indirect_after_the_probe_timeout() {
    let clock = FakeClock::new();
    let mut fd = FailureDetector::new(&clock);
    fd.begin_direct_probe(NodeId(1));
    clock.advance(PROBE_TIMEOUT_MILLIS);
    assert_eq!(fd.check_timeouts(), vec![TimeoutAction::EscalateToIndirect(NodeId(1))]);
  }

  #[test]
  fn marks_suspect_after_the_indirect_probe_also_times_out() {
    let clock = FakeClock::new();
    let mut fd = FailureDetector::new(&clock);
    fd.begin_direct_probe(NodeId(1));
    clock.advance(PROBE_TIMEOUT_MILLIS);
    fd.check_timeouts(); // escalates to indirect
    clock.advance(PROBE_TIMEOUT_MILLIS);
    assert_eq!(fd.check_timeouts(), vec![TimeoutAction::MarkSuspect(NodeId(1))]);
  }

  #[test]
  fn marks_dead_after_the_suspicion_timeout_elapses() {
    let clock = FakeClock::new();
    let mut fd = FailureDetector::new(&clock);
    fd.begin_direct_probe(NodeId(1));
    clock.advance(PROBE_TIMEOUT_MILLIS);
    fd.check_timeouts(); // escalates to indirect
    clock.advance(PROBE_TIMEOUT_MILLIS);
    fd.check_timeouts(); // marks suspect
    clock.advance(SUSPICION_TIMEOUT_MILLIS);
    assert_eq!(fd.check_timeouts(), vec![TimeoutAction::MarkDead(NodeId(1))]);
  }

  #[test]
  fn an_ack_clears_a_pending_direct_probe() {
    let clock = FakeClock::new();
    let mut fd = FailureDetector::new(&clock);
    fd.begin_direct_probe(NodeId(1));
    fd.on_ack(NodeId(1));
    clock.advance(PROBE_TIMEOUT_MILLIS * 10);
    assert_eq!(fd.check_timeouts(), vec![]);
  }

  #[test]
  fn an_ack_clears_an_active_suspicion() {
    let clock = FakeClock::new();
    let mut fd = FailureDetector::new(&clock);
    fd.begin_direct_probe(NodeId(1));
    clock.advance(PROBE_TIMEOUT_MILLIS);
    fd.check_timeouts();
    clock.advance(PROBE_TIMEOUT_MILLIS);
    fd.check_timeouts(); // now Suspect

    fd.on_ack(NodeId(1));
    clock.advance(SUSPICION_TIMEOUT_MILLIS * 10);
    assert_eq!(fd.check_timeouts(), vec![]);
  }

  #[test]
  fn check_timeouts_with_nothing_tracked_returns_empty() {
    let clock = FakeClock::new();
    let mut fd = FailureDetector::new(&clock);
    assert_eq!(fd.check_timeouts(), vec![]);
  }
}
