# Ring Mutations & Cache Eviction Implementation Plan (Sub-project 2b-i)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the two building blocks Sub-project 2b's SWIM gossip layer
needs but that have nothing to do with networking: `Ring` mutation
methods (join/leave, replacing the "built once from a static list"
behavior from 2a) and a `Cache` eviction method that keeps ownership
handoffs correct. Both are pure, deterministic logic — no threads,
timers, or sockets — so they're split out from the actual gossip protocol
(Sub-project 2b-ii) to keep each piece independently testable.

**Architecture:** `Ring` gains `apply_join`/`apply_leave`, implementing
the swap-with-last technique from the design doc's "Compute Ring
Mechanics" section; `from_members` is refactored to use them so there's
one code path. `Cache` gains `evict_non_native`, implementing the revised
rule from "Cache Invalidation on Ring Membership Change": on each applied
mutation, a node scans its own cache and drops entries it no longer
natively owns, so that regaining ownership later is guaranteed to be a
cache miss.

**Tech Stack:** Same as prior plans (Rust 2021, `anyhow`).

## Global Constraints

(Same as prior plans' — repeated since every task's requirements
implicitly include them.)

- `anyhow::Result<T>` + `bail!()`/`.context()` is the only accepted error
  style.
- `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` must
  pass; 2-space indent via the repo's `rustfmt.toml`.
- Public items get `///`/`//!` doc comments describing invariants and
  guarantees.

---

### Task 1: `Ring::empty` and `Ring::apply_join`

**Files:**
- Modify: `crates/seisin-ring/src/ring.rs`

**Interfaces:**
- Produces: `Ring::empty() -> Self`, `Ring::apply_join(&mut self, NodeId,
  u32)`. `from_members` is refactored to build on top of these (same
  public signature, same behavior, no test changes needed for it).

- [ ] **Step 1: Write the failing test**

Add to `crates/seisin-ring/src/ring.rs`, inside the `tests` module:

```rust
  #[test]
  fn apply_join_adds_the_new_members_slots() {
    let mut ring = Ring::empty();
    ring.apply_join(NodeId(1), 2);
    for _ in 0..50 {
      let (node_id, thread_id) = ring.native(DatumId::new());
      assert_eq!(node_id, NodeId(1));
      assert!(thread_id.0 < 2);
    }
  }

  #[test]
  fn from_members_matches_building_via_apply_join() {
    let via_constructor = Ring::from_members(&[(NodeId(1), 2), (NodeId(2), 3)]);
    let mut via_mutation = Ring::empty();
    via_mutation.apply_join(NodeId(1), 2);
    via_mutation.apply_join(NodeId(2), 3);

    let id = DatumId::new();
    assert_eq!(via_constructor.native(id), via_mutation.native(id));
  }
```

Add the stub methods to `impl Ring`:

```rust
  pub fn empty() -> Self {
    unimplemented!()
  }

  pub fn apply_join(&mut self, node_id: NodeId, thread_count: u32) {
    let _ = (node_id, thread_count);
    unimplemented!()
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-ring`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement, and refactor `from_members` to use them**

```rust
impl Ring {
  pub fn empty() -> Self {
    Self { slots: Vec::new() }
  }

  /// Builds a ring from a static member list: `(node_id, thread_count)`
  /// pairs. Each member contributes `thread_count` slots, in order.
  pub fn from_members(members: &[(NodeId, u32)]) -> Self {
    let mut ring = Self::empty();
    for (node_id, thread_count) in members {
      ring.apply_join(*node_id, *thread_count);
    }
    ring
  }

  /// Appends `thread_count` new slots for `node_id` to the end of the
  /// ring. Per jump-consistent-hash's own guarantee, growing `n` only
  /// remaps keys that land in the newly-added range — every existing
  /// key's owner is unaffected.
  pub fn apply_join(&mut self, node_id: NodeId, thread_count: u32) {
    for t in 0..thread_count {
      self.slots.push((node_id, ThreadId(t)));
    }
  }

  // native() unchanged — remove the old from_members body above it.
```

(Remove the old inline `from_members` implementation that built `slots`
directly with a `Vec::new()` + nested loop; replace it with the version
above that delegates to `empty()`/`apply_join`.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-ring`
Expected: PASS (7 tests: 5 existing + 2 new)

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-ring/src/ring.rs
git commit -m "feat: add Ring::empty and Ring::apply_join"
git push
```

---

### Task 2: `Ring::apply_leave`

**Files:**
- Modify: `crates/seisin-ring/src/ring.rs`

**Interfaces:**
- Produces: `Ring::apply_leave(&mut self, NodeId)`.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module:

```rust
  #[test]
  fn apply_leave_removes_a_single_slot_member() {
    let mut ring = Ring::from_members(&[(NodeId(1), 1), (NodeId(2), 1)]);
    ring.apply_leave(NodeId(1));
    for _ in 0..50 {
      let (node_id, _) = ring.native(DatumId::new());
      assert_eq!(node_id, NodeId(2));
    }
  }

  #[test]
  fn apply_leave_removes_all_of_a_multi_slot_members_slots() {
    let mut ring = Ring::from_members(&[(NodeId(1), 2), (NodeId(2), 1)]);
    ring.apply_leave(NodeId(1));
    for _ in 0..50 {
      let (node_id, thread_id) = ring.native(DatumId::new());
      assert_eq!(node_id, NodeId(2));
      assert_eq!(thread_id, ThreadId(0));
    }
  }

  #[test]
  fn apply_leave_only_removes_the_named_member() {
    let mut ring = Ring::from_members(&[(NodeId(1), 1), (NodeId(2), 1), (NodeId(3), 1)]);
    ring.apply_leave(NodeId(2));
    for _ in 0..50 {
      let (node_id, _) = ring.native(DatumId::new());
      assert!(node_id == NodeId(1) || node_id == NodeId(3), "unexpected owner: {node_id:?}");
    }
  }

  #[test]
  fn apply_leave_on_an_unknown_member_is_a_no_op() {
    let mut ring = Ring::from_members(&[(NodeId(1), 1)]);
    let id = DatumId::new();
    let before = ring.native(id);
    ring.apply_leave(NodeId(999));
    assert_eq!(ring.native(id), before);
  }

  #[test]
  #[should_panic]
  fn native_panics_once_the_last_member_has_left() {
    let mut ring = Ring::from_members(&[(NodeId(1), 1)]);
    ring.apply_leave(NodeId(1));
    ring.native(DatumId::new());
  }
```

Add the stub method to `impl Ring`:

```rust
  pub fn apply_leave(&mut self, node_id: NodeId) {
    let _ = node_id;
    unimplemented!()
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-ring`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement `apply_leave`**

```rust
  /// Removes all of `node_id`'s slots via swap-with-last: swap the
  /// removed slot with whatever's at the last index, then shrink by
  /// one. This is the standard technique for removing an arbitrary (not
  /// just the highest-index) slot while preserving jump-consistent-
  /// hash's minimal-remap guarantee for every untouched slot. The result
  /// is a deterministic function of the starting array and `node_id`, so
  /// every node applying the same mutation to the same starting ring
  /// converges on an identical result — required for the epoch-ordered
  /// replay in Sub-project 2b-ii.
  pub fn apply_leave(&mut self, node_id: NodeId) {
    let mut i = 0;
    while i < self.slots.len() {
      if self.slots[i].0 == node_id {
        let last = self.slots.len() - 1;
        self.slots.swap(i, last);
        self.slots.pop();
        // Don't advance i: the slot just swapped into position i might
        // also belong to node_id if it had multiple thread slots.
      } else {
        i += 1;
      }
    }
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-ring`
Expected: PASS (12 tests)

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-ring/src/ring.rs
git commit -m "feat: add Ring::apply_leave (swap-with-last removal)"
git push
```

---

### Task 3: `Cache::evict_non_native`

**Files:**
- Modify: `crates/seisin-core/src/cache.rs`

**Interfaces:**
- Produces: `Cache::evict_non_native(&mut self, impl FnMut(DatumId) ->
  bool)`. Takes a predicate rather than a `Ring` reference directly, so
  `seisin-core` doesn't need to depend on `seisin-ring` — the caller (in
  Sub-project 2b-ii) supplies a closure like `|id| ring.native(id) ==
  (self_node_id, self_thread_id)`.

- [ ] **Step 1: Write the failing test**

Add to `crates/seisin-core/src/cache.rs`, inside the `tests` module:

```rust
  #[test]
  fn evict_non_native_removes_entries_the_predicate_rejects() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let mut cache = Cache::new(Arc::clone(&store));
    let kept = DatumId::new();
    let evicted = DatumId::new();
    cache.put(kept, b"kept".to_vec());
    cache.put(evicted, b"evicted".to_vec());

    cache.evict_non_native(|id| id == kept);

    // The evicted entry must reload from the store rather than serve a
    // stale cached value: mutate storage directly, then confirm get
    // picks up the new value.
    store.put(evicted, b"updated".to_vec());
    assert_eq!(cache.get(evicted), Some(b"updated".to_vec()));

    // The kept entry is unaffected.
    assert_eq!(cache.get(kept), Some(b"kept".to_vec()));
  }
```

Add the stub method to `impl Cache`:

```rust
  pub fn evict_non_native(&mut self, is_native: impl FnMut(DatumId) -> bool) {
    let _ = is_native;
    unimplemented!()
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-core`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement**

```rust
  /// Evicts every cached entry for which `is_native(datum_id)` returns
  /// false — called after a ring mutation to drop entries this node no
  /// longer natively owns. This guarantees that if this node later
  /// regains ownership of one of them, `get` is a hard miss and reloads
  /// from the store, rather than serving a value that might predate
  /// another node's writes in the interim (see the design doc's "Cache
  /// Invalidation on Ring Membership Change" section).
  pub fn evict_non_native(&mut self, mut is_native: impl FnMut(DatumId) -> bool) {
    self.entries.retain(|&id, _| is_native(id));
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-core`
Expected: PASS (18 tests: 17 existing + 1 new)

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-core/src/cache.rs
git commit -m "feat: add Cache::evict_non_native"
git push
```

---

### Task 4: Quality gate

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
git commit -m "chore: fmt/clippy fixes for ring mutations and cache eviction"
git push
```

(Skip this step entirely if Steps 1–2 needed no changes.)

---

## Self-Review Notes

- **Spec coverage:** `apply_join`/`apply_leave` implementing swap-with-
  last ✓ (Tasks 1–2), `evict_non_native` implementing the revised
  cache-invalidation rule ✓ (Task 3). SWIM gossip, the epoch sequencer,
  and wiring mutations into a running node are explicitly Sub-project
  2b-ii, not here.
- **Placeholder scan:** no TBD/TODO; every `unimplemented!()` stub is
  replaced with real code within the same task.
- **Type consistency:** `Ring::apply_join`/`apply_leave` and
  `Cache::evict_non_native` signatures match exactly between their
  stub and implementation steps.
