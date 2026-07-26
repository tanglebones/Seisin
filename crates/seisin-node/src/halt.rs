//! The coordinated fail-stop flag: set when any storage member is
//! confirmed dead (no replication in v1 — the cluster halts rather
//! than serve from a partially-lost dataset). Each compute node's own
//! failure detector converges on the same confirmed-dead update, so
//! every node halts itself without a separate broadcast. Resume is
//! operator restart; auto-resume needs log-identity verification
//! (deferred — see the Part B design doc).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

#[derive(Default)]
pub struct HaltState {
  halted: AtomicBool,
  reason: Mutex<Option<String>>,
}

impl HaltState {
  pub fn new() -> Self {
    Self::default()
  }

  /// Engages the halt. The first reason wins; later calls are no-ops
  /// (the first loss is the one that matters operationally).
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
}
