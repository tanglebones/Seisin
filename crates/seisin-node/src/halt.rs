//! The cluster serving gate, with two flavors:
//!
//! - **Halt** — permanent, first-reason-wins: set when a storage member
//!   is confirmed dead (no replication in v1 — the cluster stops rather
//!   than serve from a partially-lost dataset). Cleared only by the
//!   migration driver's `resume` after it has verified log identity
//!   (never by a compute node on its own).
//! - **Pause** — resumable, driver-owned: held during a live migration
//!   so the ring can flip atomically, then released. Carries its own
//!   reason.
//!
//! `gate()` is the single answer the request path consults: halt takes
//! precedence over pause, and a distinct message prefix ("cluster
//! halted" vs "cluster paused") lets clients tell "cluster is down" from
//! "retry shortly".

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

#[derive(Default)]
pub struct HaltState {
  halted: AtomicBool,
  reason: Mutex<Option<String>>,
  paused: AtomicBool,
  pause_reason: Mutex<Option<String>>,
}

impl HaltState {
  pub fn new() -> Self {
    Self::default()
  }

  /// Engages the permanent halt. The first reason wins; later calls are
  /// no-ops (the first loss is the one that matters operationally).
  pub fn halt(&self, reason: String) {
    let mut slot = self.reason.lock().unwrap();
    if slot.is_none() {
      *slot = Some(reason);
    }
    self.halted.store(true, Ordering::SeqCst);
  }

  pub fn is_halted(&self) -> bool {
    self.halted.load(Ordering::SeqCst)
  }

  pub fn reason(&self) -> Option<String> {
    self.reason.lock().unwrap().clone()
  }

  /// Clears the permanent halt — driver-only, after log-identity
  /// verification. Resets the reason too so a subsequent genuine loss
  /// can re-arm the halt with its own reason.
  pub fn clear_halt(&self) {
    self.halted.store(false, Ordering::SeqCst);
    *self.reason.lock().unwrap() = None;
  }

  /// Engages the resumable pause (driver-owned; last writer wins).
  pub fn pause(&self, reason: String) {
    *self.pause_reason.lock().unwrap() = Some(reason);
    self.paused.store(true, Ordering::SeqCst);
  }

  /// Clears the pause. Never touches the halt — a `resume` after a real
  /// loss leaves the halt standing.
  pub fn resume(&self) {
    self.paused.store(false, Ordering::SeqCst);
    *self.pause_reason.lock().unwrap() = None;
  }

  pub fn is_paused(&self) -> bool {
    self.paused.load(Ordering::SeqCst)
  }

  pub fn pause_reason(&self) -> Option<String> {
    self.pause_reason.lock().unwrap().clone()
  }

  /// The single gate answer for the op path. `Some(message)` rejects the
  /// op; `None` serves. Halt beats pause; the message prefix
  /// distinguishes the two flavors for clients.
  pub fn gate(&self) -> Option<String> {
    if self.is_halted() {
      return Some(
        self
          .reason()
          .unwrap_or_else(|| "cluster halted".to_string()),
      );
    }
    if self.is_paused() {
      return Some(format!(
        "cluster paused: {}",
        self.pause_reason().unwrap_or_default()
      ));
    }
    None
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn halt_engages_once_and_keeps_the_first_reason() {
    let halt = HaltState::new();
    assert!(!halt.is_halted());
    halt.halt("first loss".to_string());
    halt.halt("second loss".to_string());
    assert!(halt.is_halted());
    assert_eq!(halt.reason(), Some("first loss".to_string()));
  }

  #[test]
  fn pause_then_resume_round_trips() {
    let halt = HaltState::new();
    assert_eq!(halt.gate(), None);
    halt.pause("migrating".to_string());
    assert!(halt.is_paused());
    let gate = halt.gate().unwrap();
    assert!(gate.contains("cluster paused"), "{gate}");
    assert!(gate.contains("migrating"), "{gate}");
    halt.resume();
    assert!(!halt.is_paused());
    assert_eq!(halt.gate(), None);
  }

  #[test]
  fn halt_beats_pause_in_the_gate_and_resume_does_not_clear_a_halt() {
    let halt = HaltState::new();
    halt.pause("migrating".to_string());
    halt.halt("storage node 9 confirmed dead".to_string());
    // Both set: clients see the halt reason, not the pause.
    let gate = halt.gate().unwrap();
    assert!(gate.contains("storage node 9"), "{gate}");
    // Resume clears the pause but the halt stands.
    halt.resume();
    assert!(halt.is_halted());
    assert!(halt.gate().unwrap().contains("storage node 9"));
  }

  #[test]
  fn clear_halt_lets_a_later_halt_rearm() {
    let halt = HaltState::new();
    halt.halt("first loss".to_string());
    halt.clear_halt();
    assert!(!halt.is_halted());
    assert_eq!(halt.gate(), None);
    halt.halt("second loss".to_string());
    assert_eq!(halt.reason(), Some("second loss".to_string()));
  }
}
