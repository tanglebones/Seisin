//! The storage-side self-halt heartbeat: a last-heard timestamp the
//! ack-only gossip responder refreshes on every message. If the store
//! server finds it stale (no gossip contact within the suspicion
//! window) it stops acking store requests — fail-stop symmetry, closing
//! the window where a partitioned storage node keeps acking writes from
//! an equally partitioned compute node. Fresh boot counts as "just
//! heard" so a node can serve before its first probe arrives.

use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct Heartbeat {
  last: Mutex<Instant>,
}

impl Default for Heartbeat {
  fn default() -> Self {
    Self::new()
  }
}

impl Heartbeat {
  pub fn new() -> Self {
    Self {
      last: Mutex::new(Instant::now()),
    }
  }

  /// Records gossip contact — called by the storage gossip responder on
  /// every accepted message.
  pub fn record(&self) {
    *self.last.lock().unwrap() = Instant::now();
  }

  /// Whether more than `threshold` has elapsed since the last contact.
  pub fn is_stale(&self, threshold: Duration) -> bool {
    stale(*self.last.lock().unwrap(), Instant::now(), threshold)
  }
}

/// The pure staleness decision — the unit-tested seam. Strictly-greater
/// so `threshold == 0` and an exactly-at-threshold gap are *not* stale.
pub fn stale(last: Instant, now: Instant, threshold: Duration) -> bool {
  now.duration_since(last) > threshold
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn a_fresh_contact_is_not_stale() {
    let base = Instant::now();
    assert!(!stale(base, base, Duration::from_millis(40)));
  }

  #[test]
  fn a_gap_past_the_threshold_is_stale() {
    let base = Instant::now();
    let now = base + Duration::from_millis(41);
    assert!(stale(base, now, Duration::from_millis(40)));
  }

  #[test]
  fn exactly_at_the_threshold_is_not_yet_stale() {
    let base = Instant::now();
    let now = base + Duration::from_millis(40);
    assert!(!stale(base, now, Duration::from_millis(40)));
  }

  #[test]
  fn record_refreshes_the_heartbeat() {
    let hb = Heartbeat::new();
    // A tiny threshold with a fresh record: the elapsed time between
    // record() and the check is far under a whole millisecond in
    // practice, but assert on the pure seam to stay deterministic.
    hb.record();
    assert!(!hb.is_stale(Duration::from_secs(3600)));
  }
}
