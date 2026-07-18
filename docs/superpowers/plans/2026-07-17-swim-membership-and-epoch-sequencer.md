# SWIM Membership & Epoch Sequencer Implementation Plan (Sub-project 2b-ii)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the SWIM membership state machine and the epoch
sequencer as pure, deterministic logic — no sockets, no timers, no
background threads. This is the part of dynamic gossip membership that
can be fully unit-tested without any timing-dependent behavior; wiring it
into a real gossiping node over the network is Sub-project 2b-iii, built
on top.

**Architecture:** A new `seisin-gossip` crate holds two independent
pieces. `membership.rs` is a `MemberTable`: given `MemberUpdate` facts
(node id, incarnation, status, addresses, thread count), it applies the
standard SWIM merge rule (higher incarnation always wins; at equal
incarnation, a more severe status — Alive < Suspect < Dead — wins) and
exposes status-transition helpers (`mark_suspect`, `mark_dead`,
`confirm_alive_self`) that a future failure detector will call.
`sequencer.rs` holds `is_sequencer` (a pure function: the live member
with the lowest `NodeId` is the sequencer, computed independently by
every node) and `MutationLog`, which buffers epoch-numbered ring
mutations and releases them for application only in strict, gapless
epoch order.

**Tech Stack:** Same as prior plans (Rust 2021, no new external
dependencies beyond what `seisin-core` already pulls in).

## Global Constraints

(Same as prior plans' — repeated since every task's requirements
implicitly include them.)

- `anyhow::Result<T>` + `bail!()`/`.context()` is the only accepted error
  style (not needed in this plan's pure-logic code, but keep in mind if
  any fallible path shows up).
- `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` must
  pass; 2-space indent via the repo's `rustfmt.toml`.
- Prefer many small, single-purpose crates over one monolith.
- Public items get `///`/`//!` doc comments describing invariants and
  guarantees.

**From the design doc's "Dynamic Gossip Membership Mechanics" section:**

- Ring mutations are derived from membership transitions, not a separate
  message type: a first-seen `Alive` is a Join, a confirmed `Dead` is a
  Leave. That translation (a `MemberUpdate` becoming a `RingMutation`)
  happens in Sub-project 2b-iii, once there's an actual node driving the
  failure detector and deciding when to mint one. This plan only builds
  the ordering/buffering machinery (`MutationLog`) and the primitive
  status-transition operations (`MemberTable`) those decisions will call.
- The wire format for gossip messages (how `MemberUpdate`/mutation
  records get serialized between nodes) is also 2b-iii's concern, once
  it's clear exactly what needs to go over the wire alongside the real
  socket loop — designing it in isolation here risked getting it wrong.

---

### Task 1: `seisin-gossip` scaffold and `MemberTable::merge_update`

**Files:**
- Create: `crates/seisin-gossip/Cargo.toml`
- Create: `crates/seisin-gossip/src/lib.rs`
- Create: `crates/seisin-gossip/src/membership.rs`
- Modify: `Cargo.toml` (workspace root)

**Interfaces:**
- Consumes: `seisin_core::authority::NodeId`.
- Produces: `seisin_gossip::membership::{Incarnation, MemberStatus,
  MemberUpdate, MemberTable}`. `MemberTable::new() -> Self`,
  `merge_update(&mut self, MemberUpdate) -> bool` (returns whether the
  update actually changed local state — a stale or less-severe-at-equal-
  incarnation update is ignored and returns `false`), `get(&self, NodeId)
  -> Option<MemberUpdate>`.

- [ ] **Step 1: Scaffold the crate and write the failing test**

`crates/seisin-gossip/Cargo.toml`:

```toml
[package]
name = "seisin-gossip"
version = "0.1.0"
edition = "2021"

[dependencies]
seisin-core = { path = "../seisin-core" }
```

Add `"crates/seisin-gossip"` to the workspace `members` list in the root
`Cargo.toml`.

`crates/seisin-gossip/src/lib.rs`:

```rust
pub mod membership;
pub mod sequencer;
```

(The `sequencer` module is created in Task 3 — if executing strictly
task-by-task, use just `pub mod membership;` for this task and add the
second line in Task 3.)

`crates/seisin-gossip/src/membership.rs`:

```rust
//! The SWIM membership table: merges incoming `MemberUpdate` facts using
//! the standard SWIM precedence rule (higher incarnation always wins; at
//! equal incarnation, a more severe status wins), and exposes the
//! status-transition operations a failure detector drives (added in
//! Sub-project 2b-iii).

use std::collections::HashMap;

use seisin_core::authority::NodeId;

/// A node's self-reported generation number. A node bumps this to
/// refute a false suspicion (see `MemberTable::confirm_alive_self`); any
/// update at a higher incarnation always supersedes one at a lower
/// incarnation, regardless of status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Incarnation(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberStatus {
  Alive,
  Suspect,
  Dead,
}

impl MemberStatus {
  /// Ordering used to break ties at equal incarnation: `Dead` is most
  /// severe, `Alive` least.
  fn severity(self) -> u8 {
    match self {
      MemberStatus::Alive => 0,
      MemberStatus::Suspect => 1,
      MemberStatus::Dead => 2,
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberUpdate {
  pub node_id: NodeId,
  pub incarnation: Incarnation,
  pub status: MemberStatus,
  pub client_address: String,
  pub gossip_address: String,
  pub thread_count: u32,
}

struct MemberRecord {
  incarnation: Incarnation,
  status: MemberStatus,
  client_address: String,
  gossip_address: String,
  thread_count: u32,
}

impl MemberRecord {
  fn to_update(&self, node_id: NodeId) -> MemberUpdate {
    MemberUpdate {
      node_id,
      incarnation: self.incarnation,
      status: self.status,
      client_address: self.client_address.clone(),
      gossip_address: self.gossip_address.clone(),
      thread_count: self.thread_count,
    }
  }
}

#[derive(Default)]
pub struct MemberTable {
  members: HashMap<NodeId, MemberRecord>,
}

impl MemberTable {
  pub fn new() -> Self {
    Self::default()
  }

  /// Applies an incoming fact using the SWIM precedence rule. Returns
  /// whether it actually changed this table's state (a stale update, or
  /// one at the same incarnation but no more severe than what's already
  /// recorded, is ignored and returns `false`).
  pub fn merge_update(&mut self, update: MemberUpdate) -> bool {
    let _ = update;
    unimplemented!()
  }

  pub fn get(&self, node_id: NodeId) -> Option<MemberUpdate> {
    let _ = node_id;
    unimplemented!()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn update(node_id: u64, incarnation: u64, status: MemberStatus) -> MemberUpdate {
    MemberUpdate {
      node_id: NodeId(node_id),
      incarnation: Incarnation(incarnation),
      status,
      client_address: "127.0.0.1:7878".to_string(),
      gossip_address: "127.0.0.1:8878".to_string(),
      thread_count: 2,
    }
  }

  #[test]
  fn first_update_for_a_node_is_always_accepted() {
    let mut table = MemberTable::new();
    let accepted = table.merge_update(update(1, 0, MemberStatus::Alive));
    assert!(accepted);
    assert_eq!(table.get(NodeId(1)), Some(update(1, 0, MemberStatus::Alive)));
  }

  #[test]
  fn higher_incarnation_always_wins() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 0, MemberStatus::Dead));
    let accepted = table.merge_update(update(1, 1, MemberStatus::Alive));
    assert!(accepted);
    assert_eq!(table.get(NodeId(1)).unwrap().status, MemberStatus::Alive);
  }

  #[test]
  fn lower_incarnation_is_ignored() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 5, MemberStatus::Alive));
    let accepted = table.merge_update(update(1, 4, MemberStatus::Dead));
    assert!(!accepted);
    assert_eq!(table.get(NodeId(1)).unwrap().status, MemberStatus::Alive);
  }

  #[test]
  fn at_equal_incarnation_more_severe_status_wins() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 0, MemberStatus::Alive));
    let accepted = table.merge_update(update(1, 0, MemberStatus::Suspect));
    assert!(accepted);
    assert_eq!(table.get(NodeId(1)).unwrap().status, MemberStatus::Suspect);
  }

  #[test]
  fn at_equal_incarnation_less_severe_status_is_ignored() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 0, MemberStatus::Suspect));
    let accepted = table.merge_update(update(1, 0, MemberStatus::Alive));
    assert!(!accepted);
    assert_eq!(table.get(NodeId(1)).unwrap().status, MemberStatus::Suspect);
  }

  #[test]
  fn get_on_unknown_node_returns_none() {
    let table = MemberTable::new();
    assert_eq!(table.get(NodeId(42)), None);
  }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-gossip`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement `merge_update` and `get`**

```rust
impl MemberTable {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn merge_update(&mut self, update: MemberUpdate) -> bool {
    use std::collections::hash_map::Entry;

    match self.members.entry(update.node_id) {
      Entry::Vacant(slot) => {
        slot.insert(MemberRecord {
          incarnation: update.incarnation,
          status: update.status,
          client_address: update.client_address,
          gossip_address: update.gossip_address,
          thread_count: update.thread_count,
        });
        true
      }
      Entry::Occupied(mut slot) => {
        let current = slot.get();
        let accept = update.incarnation > current.incarnation
          || (update.incarnation == current.incarnation
            && update.status.severity() > current.status.severity());
        if accept {
          slot.insert(MemberRecord {
            incarnation: update.incarnation,
            status: update.status,
            client_address: update.client_address,
            gossip_address: update.gossip_address,
            thread_count: update.thread_count,
          });
        }
        accept
      }
    }
  }

  pub fn get(&self, node_id: NodeId) -> Option<MemberUpdate> {
    self.members.get(&node_id).map(|record| record.to_update(node_id))
  }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-gossip`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit and push**

```bash
git add Cargo.toml crates/seisin-gossip
git commit -m "feat: add seisin-gossip MemberTable with SWIM merge rule"
git push
```

---

### Task 2: Status transitions and `alive_members`

**Files:**
- Modify: `crates/seisin-gossip/src/membership.rs`

**Interfaces:**
- Produces: `MemberTable::mark_suspect(&mut self, NodeId) ->
  Option<MemberUpdate>`, `mark_dead(&mut self, NodeId) ->
  Option<MemberUpdate>`, `confirm_alive_self(&mut self, NodeId) ->
  MemberUpdate`, `alive_members(&self) -> Vec<NodeId>`.

- [ ] **Step 1: Write the failing test**

Add to `crates/seisin-gossip/src/membership.rs`, inside `impl
MemberTable`:

```rust
  /// Transitions a currently-`Alive` member to `Suspect`. No-op (returns
  /// `None`) if the node is unknown or already `Suspect`/`Dead` — a
  /// failure detector (added in Sub-project 2b-iii) calls this on a
  /// probe timeout.
  pub fn mark_suspect(&mut self, node_id: NodeId) -> Option<MemberUpdate> {
    let _ = node_id;
    unimplemented!()
  }

  /// Transitions a currently-`Alive` or `Suspect` member to `Dead`.
  /// No-op if unknown or already `Dead` — a failure detector calls this
  /// after the suspicion timeout elapses with no refutation.
  pub fn mark_dead(&mut self, node_id: NodeId) -> Option<MemberUpdate> {
    let _ = node_id;
    unimplemented!()
  }

  /// Bumps this node's own incarnation and marks it `Alive` — used to
  /// refute a false suspicion (gossip reporting this node as `Suspect`
  /// when it's actually fine).
  ///
  /// # Panics
  /// Panics if `self_id` isn't already registered in this table — a node
  /// must register itself before it can confirm its own liveness.
  pub fn confirm_alive_self(&mut self, self_id: NodeId) -> MemberUpdate {
    let _ = self_id;
    unimplemented!()
  }

  /// All members currently believed `Alive`, in ascending `NodeId` order.
  pub fn alive_members(&self) -> Vec<NodeId> {
    unimplemented!()
  }
```

Add these tests:

```rust
  #[test]
  fn mark_suspect_transitions_an_alive_member() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 0, MemberStatus::Alive));
    let result = table.mark_suspect(NodeId(1));
    assert_eq!(result.unwrap().status, MemberStatus::Suspect);
    assert_eq!(table.get(NodeId(1)).unwrap().status, MemberStatus::Suspect);
  }

  #[test]
  fn mark_suspect_on_unknown_node_is_a_no_op() {
    let mut table = MemberTable::new();
    assert_eq!(table.mark_suspect(NodeId(1)), None);
  }

  #[test]
  fn mark_suspect_on_already_suspect_member_is_a_no_op() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 0, MemberStatus::Suspect));
    assert_eq!(table.mark_suspect(NodeId(1)), None);
  }

  #[test]
  fn mark_dead_transitions_a_suspect_member() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 0, MemberStatus::Suspect));
    let result = table.mark_dead(NodeId(1));
    assert_eq!(result.unwrap().status, MemberStatus::Dead);
  }

  #[test]
  fn mark_dead_on_already_dead_member_is_a_no_op() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 0, MemberStatus::Dead));
    assert_eq!(table.mark_dead(NodeId(1)), None);
  }

  #[test]
  fn confirm_alive_self_bumps_incarnation_and_clears_suspicion() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 3, MemberStatus::Suspect));
    let result = table.confirm_alive_self(NodeId(1));
    assert_eq!(result.incarnation, Incarnation(4));
    assert_eq!(result.status, MemberStatus::Alive);
    assert_eq!(table.get(NodeId(1)).unwrap().incarnation, Incarnation(4));
  }

  #[test]
  fn alive_members_excludes_suspect_and_dead_and_is_sorted() {
    let mut table = MemberTable::new();
    table.merge_update(update(3, 0, MemberStatus::Alive));
    table.merge_update(update(1, 0, MemberStatus::Alive));
    table.merge_update(update(2, 0, MemberStatus::Suspect));
    table.merge_update(update(4, 0, MemberStatus::Dead));
    assert_eq!(table.alive_members(), vec![NodeId(1), NodeId(3)]);
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-gossip`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement**

```rust
  pub fn mark_suspect(&mut self, node_id: NodeId) -> Option<MemberUpdate> {
    let record = self.members.get_mut(&node_id)?;
    if record.status != MemberStatus::Alive {
      return None;
    }
    record.status = MemberStatus::Suspect;
    Some(record.to_update(node_id))
  }

  pub fn mark_dead(&mut self, node_id: NodeId) -> Option<MemberUpdate> {
    let record = self.members.get_mut(&node_id)?;
    if record.status == MemberStatus::Dead {
      return None;
    }
    record.status = MemberStatus::Dead;
    Some(record.to_update(node_id))
  }

  pub fn confirm_alive_self(&mut self, self_id: NodeId) -> MemberUpdate {
    let record = self
      .members
      .get_mut(&self_id)
      .expect("self must already be registered");
    record.incarnation = Incarnation(record.incarnation.0 + 1);
    record.status = MemberStatus::Alive;
    record.to_update(self_id)
  }

  pub fn alive_members(&self) -> Vec<NodeId> {
    let mut ids: Vec<NodeId> = self
      .members
      .iter()
      .filter(|(_, record)| record.status == MemberStatus::Alive)
      .map(|(node_id, _)| *node_id)
      .collect();
    ids.sort_by_key(|node_id| node_id.0);
    ids
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-gossip`
Expected: PASS (13 tests)

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-gossip/src/membership.rs
git commit -m "feat: add MemberTable status transitions and alive_members"
git push
```

---

### Task 3: Epoch sequencer (`is_sequencer`)

**Files:**
- Create: `crates/seisin-gossip/src/sequencer.rs`
- Modify: `crates/seisin-gossip/src/lib.rs`
- Modify: `crates/seisin-core/src/authority.rs`

**Interfaces:**
- Consumes: `seisin_core::authority::NodeId` (needs `Ord` added).
- Produces: `seisin_gossip::sequencer::is_sequencer(NodeId, &[NodeId]) ->
  bool`.

- [ ] **Step 1: Add `Ord`/`PartialOrd` to `NodeId` and write the failing test**

In `crates/seisin-core/src/authority.rs`, change:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u64);
```

to:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u64);
```

`crates/seisin-gossip/src/sequencer.rs`:

```rust
//! The epoch sequencer: whichever live member currently has the lowest
//! `NodeId` is responsible for assigning epoch numbers to ring
//! mutations (see the design doc's "Compute Ring Mechanics" section).
//! No election protocol is needed — every node computes this
//! independently from its own membership view, and it updates
//! automatically as nodes join or leave.

use seisin_core::authority::NodeId;

pub fn is_sequencer(self_id: NodeId, alive_members: &[NodeId]) -> bool {
  let _ = (self_id, alive_members);
  unimplemented!()
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn lowest_node_id_is_the_sequencer() {
    assert!(is_sequencer(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]));
  }

  #[test]
  fn a_higher_node_id_is_not_the_sequencer() {
    assert!(!is_sequencer(NodeId(2), &[NodeId(1), NodeId(2), NodeId(3)]));
  }

  #[test]
  fn a_node_absent_from_the_alive_list_is_not_the_sequencer() {
    assert!(!is_sequencer(NodeId(1), &[NodeId(2), NodeId(3)]));
  }

  #[test]
  fn a_lone_alive_member_is_its_own_sequencer() {
    assert!(is_sequencer(NodeId(5), &[NodeId(5)]));
  }

  #[test]
  fn an_empty_alive_list_has_no_sequencer() {
    assert!(!is_sequencer(NodeId(1), &[]));
  }
}
```

Add `pub mod sequencer;` to `crates/seisin-gossip/src/lib.rs`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-gossip`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement**

```rust
pub fn is_sequencer(self_id: NodeId, alive_members: &[NodeId]) -> bool {
  alive_members.iter().min().is_some_and(|lowest| *lowest == self_id)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-gossip`
Expected: PASS (5 new tests; 18 total in the crate)

Also run: `cargo test -p seisin-core` (the `NodeId` derive change touches
a shared type)
Expected: PASS (18 tests, unaffected)

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-core/src/authority.rs crates/seisin-gossip/src/sequencer.rs crates/seisin-gossip/src/lib.rs
git commit -m "feat: add epoch sequencer election (is_sequencer)"
git push
```

---

### Task 4: `MutationLog`

**Files:**
- Modify: `crates/seisin-gossip/src/sequencer.rs`

**Interfaces:**
- Produces: `seisin_gossip::sequencer::{RingMutation, MutationLog}`.
  `RingMutation::{Join { node_id: NodeId, thread_count: u32 }, Leave {
  node_id: NodeId }}`. `MutationLog::new() -> Self`, `record(&mut self,
  epoch: u64, RingMutation)`, `drain_applicable(&mut self) ->
  Vec<RingMutation>`.

- [ ] **Step 1: Write the failing test**

Add to `crates/seisin-gossip/src/sequencer.rs`:

```rust
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RingMutation {
  Join { node_id: NodeId, thread_count: u32 },
  Leave { node_id: NodeId },
}

/// Buffers epoch-numbered ring mutations and releases them for
/// application only in strict, gapless epoch order — required because
/// gossip delivers `(epoch, mutation)` records with no ordering
/// guarantee, but the ring's swap-with-last mutation must be applied
/// identically (and therefore in the same order) on every node.
pub struct MutationLog {
  applied_epoch: u64,
  pending: BTreeMap<u64, RingMutation>,
}

impl Default for MutationLog {
  fn default() -> Self {
    Self {
      applied_epoch: 0,
      pending: BTreeMap::new(),
    }
  }
}

impl MutationLog {
  pub fn new() -> Self {
    Self::default()
  }

  /// Buffers a sequenced mutation. An epoch at or before what's already
  /// been applied is ignored (a stale re-delivery).
  pub fn record(&mut self, epoch: u64, mutation: RingMutation) {
    let _ = (epoch, mutation);
    unimplemented!()
  }

  /// Returns every mutation now ready to apply, in epoch order, and
  /// advances the applied-epoch marker past them. A missing epoch (a
  /// gap) leaves everything after it buffered until the gap is filled.
  pub fn drain_applicable(&mut self) -> Vec<RingMutation> {
    unimplemented!()
  }
}

#[cfg(test)]
mod mutation_log_tests {
  use super::*;

  #[test]
  fn drains_in_order_epochs_immediately() {
    let mut log = MutationLog::new();
    log.record(1, RingMutation::Join { node_id: NodeId(1), thread_count: 2 });
    log.record(2, RingMutation::Leave { node_id: NodeId(1) });
    assert_eq!(
      log.drain_applicable(),
      vec![
        RingMutation::Join { node_id: NodeId(1), thread_count: 2 },
        RingMutation::Leave { node_id: NodeId(1) },
      ]
    );
  }

  #[test]
  fn withholds_mutations_until_the_gap_before_them_is_filled() {
    let mut log = MutationLog::new();
    log.record(2, RingMutation::Leave { node_id: NodeId(1) });
    assert_eq!(log.drain_applicable(), vec![]);

    log.record(1, RingMutation::Join { node_id: NodeId(1), thread_count: 2 });
    assert_eq!(
      log.drain_applicable(),
      vec![
        RingMutation::Join { node_id: NodeId(1), thread_count: 2 },
        RingMutation::Leave { node_id: NodeId(1) },
      ]
    );
  }

  #[test]
  fn a_stale_epoch_is_ignored() {
    let mut log = MutationLog::new();
    log.record(1, RingMutation::Join { node_id: NodeId(1), thread_count: 2 });
    log.drain_applicable();

    // Re-delivery of an already-applied epoch must not re-appear or panic.
    log.record(1, RingMutation::Join { node_id: NodeId(1), thread_count: 2 });
    assert_eq!(log.drain_applicable(), vec![]);
  }

  #[test]
  fn drain_with_nothing_pending_returns_empty() {
    let mut log = MutationLog::new();
    assert_eq!(log.drain_applicable(), vec![]);
  }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-gossip`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement**

```rust
  pub fn record(&mut self, epoch: u64, mutation: RingMutation) {
    if epoch > self.applied_epoch {
      self.pending.insert(epoch, mutation);
    }
  }

  pub fn drain_applicable(&mut self) -> Vec<RingMutation> {
    let mut ready = Vec::new();
    loop {
      let next_epoch = self.applied_epoch + 1;
      match self.pending.remove(&next_epoch) {
        Some(mutation) => {
          ready.push(mutation);
          self.applied_epoch = next_epoch;
        }
        None => break,
      }
    }
    ready
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-gossip`
Expected: PASS (4 new tests; 22 total in the crate)

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-gossip/src/sequencer.rs
git commit -m "feat: add MutationLog for epoch-ordered ring mutation replay"
git push
```

---

### Task 5: Quality gate

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
git commit -m "chore: fmt/clippy fixes for SWIM membership and epoch sequencer"
git push
```

(Skip this step entirely if Steps 1–2 needed no changes.)

---

## Self-Review Notes

- **Spec coverage:** SWIM merge rule ✓ (Task 1), status transitions
  (`mark_suspect`/`mark_dead`/`confirm_alive_self`) ✓ (Task 2), epoch
  sequencer election ✓ (Task 3), epoch-ordered mutation buffering ✓
  (Task 4). The actual failure detector (probe/timeout loop), the gossip
  wire format, and wiring any of this into a running node are explicitly
  Sub-project 2b-iii, not here.
- **Placeholder scan:** no TBD/TODO; every `unimplemented!()` stub is
  replaced with real code within the same task.
- **Type consistency:** `MemberUpdate`, `MemberStatus`, `Incarnation`,
  `RingMutation`, and `MutationLog` are each defined once and referenced
  identically wherever consumed later in the plan.
