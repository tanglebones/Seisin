//! An injectable clock abstraction for the failure detector. Uses a
//! plain millisecond tick rather than `std::time::Instant` specifically
//! because `Instant` has no safe way to fake its internals — a custom
//! tick type lets tests advance time explicitly and deterministically
//! instead of sleeping.

use std::cell::Cell;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Tick(pub u64);

pub trait ClockSource {
  fn now(&self) -> Tick;
}

/// The real wall clock, measured as milliseconds elapsed since this
/// `SystemClock` was constructed.
pub struct SystemClock {
  start: Instant,
}

impl SystemClock {
  pub fn new() -> Self {
    Self {
      start: Instant::now(),
    }
  }
}

impl Default for SystemClock {
  fn default() -> Self {
    Self::new()
  }
}

impl ClockSource for SystemClock {
  fn now(&self) -> Tick {
    Tick(self.start.elapsed().as_millis() as u64)
  }
}

/// A manually-advanced clock for tests — starts at `Tick(0)` and only
/// moves forward when `advance` is called.
pub struct FakeClock {
  now: Cell<u64>,
}

impl Default for FakeClock {
  fn default() -> Self {
    Self::new()
  }
}

impl FakeClock {
  pub fn new() -> Self {
    Self { now: Cell::new(0) }
  }

  pub fn advance(&self, millis: u64) {
    self.now.set(self.now.get() + millis);
  }
}

impl ClockSource for FakeClock {
  fn now(&self) -> Tick {
    Tick(self.now.get())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn fake_clock_starts_at_zero() {
    let clock = FakeClock::new();
    assert_eq!(clock.now(), Tick(0));
  }

  #[test]
  fn fake_clock_advances_by_the_given_amount() {
    let clock = FakeClock::new();
    clock.advance(100);
    clock.advance(50);
    assert_eq!(clock.now(), Tick(150));
  }

  #[test]
  fn system_clock_is_non_decreasing() {
    let clock = SystemClock::new();
    let first = clock.now();
    let second = clock.now();
    assert!(second >= first);
  }
}
