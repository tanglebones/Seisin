# Failure Detector Implementation Plan (Sub-project 2b-iii-b)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement SWIM's direct-probe → indirect-probe → suspect →
dead state machine as pure, deterministic logic driven by an injectable
clock, so it's fully unit-testable without any real sleeping or timing
flakiness. This is the last piece before Sub-project 2b-iii-c wires
everything into a real gossiping node with actual sockets and a real
timer loop.

**Architecture:** A `ClockSource` trait abstracts "what time is it" as an
opaque, monotonically increasing `Tick` (milliseconds since some
arbitrary start) rather than `std::time::Instant`, specifically so tests
can supply a `FakeClock` they advance explicitly instead of sleeping —
`std::time::Instant` has no safe way to fake its internals, which is why
this uses a custom tick type instead. `FailureDetector` tracks, per
node currently being probed, which phase it's in (awaiting a direct ack,
awaiting an indirect ack) and, once suspected, when that started. The
caller drives it entirely through explicit calls — `begin_direct_probe`,
`on_ack`, and a periodic `check_timeouts` that returns exactly what
happened (escalate to indirect, mark suspect, mark dead) for the caller
to act on (send messages, update the `MemberTable`). It intentionally
does **not** pick which peers to probe or send anything over a socket —
that's 2b-iii-c's job, once real randomness and real I/O are in play.

**Tech Stack:** Same as prior plans (Rust 2021, no new dependencies).

## Global Constraints

(Same as prior plans' — repeated since every task's requirements
implicitly include them.)

- `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` must
  pass; 2-space indent via the repo's `rustfmt.toml`.
- Time abstracted behind a trait so it can be faked in tests instead of
  sleeping or relying on real entropy (`GUIDELINES.md`, Rust) — the whole
  point of this plan's `ClockSource` design.
- Public items get `///`/`//!` doc comments describing invariants and
  guarantees.

**From the design doc's "Dynamic Gossip Membership Mechanics" section:**
probe timeout and suspicion timeout are 1 second and 5 seconds
respectively (this plan encodes them as plain constants, not yet
configurable). Indirect-probe fanout (3 random peers) and the choice of
*which* peer to probe next are 2b-iii-c's concern, since both need real
randomness this plan deliberately keeps out.

---

### Task 1: `ClockSource`, `Tick`, `SystemClock`, `FakeClock`

**Files:**
- Create: `crates/seisin-gossip/src/clock.rs`
- Modify: `crates/seisin-gossip/src/lib.rs`

**Interfaces:**
- Produces: `seisin_gossip::clock::{Tick, ClockSource, SystemClock,
  FakeClock}`. `Tick` wraps a `u64` millisecond count and derives
  `PartialOrd`/`Ord` (needed to compare elapsed time). `ClockSource::now(&self)
  -> Tick`. `SystemClock` implements it against the real wall clock.
  `FakeClock::new() -> Self` starts at `Tick(0)`; `advance(&self, millis:
  u64)` moves it forward — used by every later test in this plan instead
  of sleeping.

- [ ] **Step 1: Write the failing test**

`crates/seisin-gossip/src/clock.rs`:

```rust
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
    let _ = Instant::now();
    unimplemented!()
  }
}

impl ClockSource for SystemClock {
  fn now(&self) -> Tick {
    unimplemented!()
  }
}

/// A manually-advanced clock for tests — starts at `Tick(0)` and only
/// moves forward when `advance` is called.
pub struct FakeClock {
  now: Cell<u64>,
}

impl FakeClock {
  pub fn new() -> Self {
    unimplemented!()
  }

  pub fn advance(&self, millis: u64) {
    let _ = millis;
    unimplemented!()
  }
}

impl ClockSource for FakeClock {
  fn now(&self) -> Tick {
    unimplemented!()
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
```

Add `pub mod clock;` to `crates/seisin-gossip/src/lib.rs`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-gossip`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement**

```rust
impl SystemClock {
  pub fn new() -> Self {
    Self { start: Instant::now() }
  }
}

impl ClockSource for SystemClock {
  fn now(&self) -> Tick {
    Tick(self.start.elapsed().as_millis() as u64)
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
```

(Remove the stray `let _ = Instant::now(); unimplemented!()` placeholder
body from `SystemClock::new` — replace the whole method.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-gossip`
Expected: PASS (3 new tests; 36 total in the crate)

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-gossip/src/clock.rs crates/seisin-gossip/src/lib.rs
git commit -m "feat: add ClockSource/Tick abstraction (SystemClock, FakeClock)"
git push
```

---

### Task 2: `FailureDetector`

**Files:**
- Create: `crates/seisin-gossip/src/failure_detector.rs`
- Modify: `crates/seisin-gossip/src/lib.rs`

**Interfaces:**
- Consumes: `seisin_core::authority::NodeId`,
  `seisin_gossip::clock::{ClockSource, Tick}`.
- Produces: `seisin_gossip::failure_detector::{FailureDetector,
  TimeoutAction, PROBE_TIMEOUT_MILLIS, SUSPICION_TIMEOUT_MILLIS}`.
  `FailureDetector::new(&C) -> Self`, `begin_direct_probe(&mut self,
  NodeId)`, `on_ack(&mut self, NodeId)`, `check_timeouts(&mut self) ->
  Vec<TimeoutAction>`. `TimeoutAction::{EscalateToIndirect(NodeId),
  MarkSuspect(NodeId), MarkDead(NodeId)}`.

- [ ] **Step 1: Write the failing test**

`crates/seisin-gossip/src/failure_detector.rs`:

```rust
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
    let _ = target;
    unimplemented!()
  }

  /// An ack (direct or relayed via an indirect probe) arrived from
  /// `target` — clears any outstanding probe or suspicion tracking for
  /// it.
  pub fn on_ack(&mut self, target: NodeId) {
    let _ = target;
    unimplemented!()
  }

  /// Call periodically (e.g. once per probe interval) to check for
  /// timed-out probes and expired suspicions. Returns every action the
  /// caller needs to take as a result, in no particular order.
  pub fn check_timeouts(&mut self) -> Vec<TimeoutAction> {
    unimplemented!()
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
```

Add `pub mod failure_detector;` to `crates/seisin-gossip/src/lib.rs`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-gossip`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement**

```rust
  pub fn begin_direct_probe(&mut self, target: NodeId) {
    self.probes.insert(target, ProbeState::AwaitingDirectAck { started_at: self.clock.now() });
  }

  pub fn on_ack(&mut self, target: NodeId) {
    self.probes.remove(&target);
    self.suspected_since.remove(&target);
  }

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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-gossip`
Expected: PASS (7 new tests; 43 total in the crate)

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-gossip/src/failure_detector.rs crates/seisin-gossip/src/lib.rs
git commit -m "feat: add FailureDetector (direct/indirect probe, suspect, dead)"
git push
```

---

### Task 3: Quality gate

**Files:** none (verification only).

- [ ] **Step 1: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS (all tests across all crates)

- [ ] **Step 2: Run the formatting and lint gate**

Run: `cargo fmt --check`
Expected: no output

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings/errors

Fix anything either command reports before continuing.

- [ ] **Step 3: Commit and push if the gate needed any fixes**

```bash
git add -A
git commit -m "chore: fmt/clippy fixes for failure detector"
git push
```

(Skip this step entirely if Steps 1–2 needed no changes.)

---

## Self-Review Notes

- **Spec coverage:** injectable clock ✓ (Task 1), direct→indirect→
  suspect→dead state machine driven by explicit calls ✓ (Task 2). Peer
  selection (who to probe, who to relay through), the real socket loop,
  and node wiring are Sub-project 2b-iii-c, not here.
- **Placeholder scan:** no TBD/TODO; every `unimplemented!()` stub is
  replaced with real code within the same task.
- **Type consistency:** `Tick`, `ClockSource`, `FailureDetector`, and
  `TimeoutAction` match exactly between stub and implementation steps.
