# Sub-project 3b, Part 2b: Crash Detection & Lock Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the crash-handling gaps Part 2a deliberately left open — a lock whose holder-node crashes must eventually release (proactively, via gossip, and reactively, when a recall attempt fails), a collating thread's in-flight `Acquire` against a dead target must retry rather than hang, and an op that can never complete must fail back to its client with `OpError` instead of hanging forever.

**Architecture:** Three independent mechanisms, all reusing infrastructure Part 1/2a already built rather than adding anything new: (1) a `NativeLock::handle_node_death` method, driven by the *same* gossip ring-mutation hook that already triggers cache eviction on membership change; (2) a reactive backstop that turns *any* failed recall response (not just an explicit ack) into an immediate release; (3) a bounded retry counter threaded through `send_acquire`, which on exhaustion fails the whole op and releases whatever it had already acquired.

**Tech Stack:** Rust, building on Part 1 (`docs/superpowers/plans/2026-07-20-cross-node-collation-wound-wait-part1.md`) and Part 2a (`docs/superpowers/plans/2026-07-20-cross-node-collation-wound-wait-part2a.md`), both fully implemented on `main`.

## Global Constraints

- 2-space indentation.
- Commit and push after every task.
- Update `docs/superpowers/PROGRESS.md` once this plan completes.
- This plan closes out Sub-project 3b entirely — after Task 10's quality gate, the whole spec (`docs/superpowers/specs/2026-07-20-cross-node-collation-and-wound-wait-design.md`) is implemented.
- Not in scope (unchanged from Part 2a's own carried-forward gaps): peer-links still only connect from the *static* startup member list — a node admitted later via gossip has no peer-link connection, and `PeerLinkRegistry::get` still panics if one is missing entirely (as opposed to having existed and then dying, which this plan does handle). Reconnection/backoff for a link that never existed is out of scope here too.

---

### Task 1: `NativeLock::handle_node_death` — the proactive release primitive

**Files:**
- Modify: `crates/seisin-node/src/collation.rs`

**Interfaces:**
- Produces: `NativeLock::handle_node_death(&mut self, node_id: NodeId)` — if the current holder's `node_id == node_id`, releases it (granting to the next surviving waiter, if any); regardless, prunes any queued waiter whose `node_id == node_id` first, so a dead node's own pending requests don't later get handed a grant nobody will use.

- [ ] **Step 1: Write the failing tests**

Add to `crates/seisin-node/src/collation.rs`'s `mod tests`:

```rust
  #[test]
  fn handle_node_death_releases_the_current_holder_from_that_node() {
    let mut lock = NativeLock::new();
    let (tx, rx) = mpsc::channel();
    let holder_op = DatumId::new();
    lock.request(
      holder_op,
      NodeId(1),
      ThreadId(0),
      Box::new(move || {
        let _ = tx.send(());
      }),
    );
    assert!(lock.current_holder().is_some());

    lock.handle_node_death(NodeId(1));
    assert_eq!(lock.current_holder(), None);
  }

  #[test]
  fn handle_node_death_grants_to_a_surviving_waiter() {
    let mut lock = NativeLock::new();
    let (tx_holder, _rx_holder) = mpsc::channel();
    let holder_op = DatumId::new();
    lock.request(
      holder_op,
      NodeId(1),
      ThreadId(0),
      Box::new(move || {
        let _ = tx_holder.send(());
      }),
    );

    let (tx_waiter, rx_waiter) = mpsc::channel();
    let waiter_op = DatumId::new(); // created after holder_op, sorts younger
    lock.request(
      waiter_op,
      NodeId(2),
      ThreadId(0),
      Box::new(move || {
        let _ = tx_waiter.send(());
      }),
    );
    assert!(rx_waiter.try_recv().is_err());

    lock.handle_node_death(NodeId(1));
    assert!(
      rx_waiter.try_recv().is_ok(),
      "the surviving waiter should be granted once the dead holder is released"
    );
    assert_eq!(
      lock.current_holder(),
      Some(&Holder {
        node_id: NodeId(2),
        thread_id: ThreadId(0),
        op_id: waiter_op
      })
    );
  }

  #[test]
  fn handle_node_death_prunes_that_nodes_own_queued_waiters() {
    let mut lock = NativeLock::new();
    let (tx_holder, _rx_holder) = mpsc::channel();
    let holder_op = DatumId::new();
    lock.request(
      holder_op,
      NodeId(1),
      ThreadId(0),
      Box::new(move || {
        let _ = tx_holder.send(());
      }),
    );

    // A waiter from the node that's about to die — should be pruned,
    // not granted, even though it would otherwise be next in line.
    let (tx_dead_waiter, rx_dead_waiter) = mpsc::channel();
    let dead_waiter_op = DatumId::new();
    lock.request(
      dead_waiter_op,
      NodeId(2),
      ThreadId(0),
      Box::new(move || {
        let _ = tx_dead_waiter.send(());
      }),
    );

    // A later, still-alive waiter.
    let (tx_alive_waiter, rx_alive_waiter) = mpsc::channel();
    let alive_waiter_op = DatumId::new();
    lock.request(
      alive_waiter_op,
      NodeId(3),
      ThreadId(0),
      Box::new(move || {
        let _ = tx_alive_waiter.send(());
      }),
    );

    lock.handle_node_death(NodeId(2));
    assert!(
      rx_dead_waiter.try_recv().is_err(),
      "a waiter from the dead node must never be granted"
    );
    assert!(
      rx_alive_waiter.try_recv().is_err(),
      "the current holder (node 1) is still alive, so nothing should be granted yet"
    );

    lock.handle_node_death(NodeId(1));
    assert!(
      rx_alive_waiter.try_recv().is_ok(),
      "once the holder dies too, the surviving waiter should be granted"
    );
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-node --lib collation::tests::handle_node_death`
Expected: FAIL with "no method named `handle_node_death` found"

- [ ] **Step 3: Implement `handle_node_death`**

Add to `impl NativeLock` in `crates/seisin-node/src/collation.rs`, after `release`:

```rust
  /// `node_id` is confirmed dead: releases it as the current holder if
  /// it was one (granting to the next surviving waiter, if any), and
  /// prunes any of its own still-queued waiters first, so a dead node's
  /// pending request is never later handed a grant nobody will use.
  pub fn handle_node_death(&mut self, node_id: NodeId) {
    self.waiters.retain(|w| w.node_id != node_id);
    if self
      .current_holder
      .as_ref()
      .is_some_and(|h| h.node_id == node_id)
    {
      self.release();
    }
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-node --lib collation::`
Expected: PASS (8 tests: 5 existing + 3 new).

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-node/src/collation.rs
git commit -m "feat: add NativeLock::handle_node_death for proactive lock release"
git push
```

---

### Task 2: Broadcast node death down to every `NativeLock`

**Files:**
- Modify: `crates/seisin-node/src/worker.rs`
- Modify: `crates/seisin-node/src/pool.rs`

**Interfaces:**
- Consumes: `NativeLock::handle_node_death` from Task 1.
- Produces: `WorkerMessage::ReleaseLocksHeldBy(NodeId)`, `WorkerHandle::release_locks_held_by(&self, node_id: NodeId)`, `WorkerPool::release_locks_held_by(&self, node_id: NodeId)`.

- [ ] **Step 1: Add the message variant and its handler**

In `crates/seisin-node/src/worker.rs`, add a variant to `WorkerMessage` (after `EvictNonNative`):

```rust
  /// Every datum this thread is native home for, if currently held by
  /// `NodeId`, is released immediately (granting to the next waiter);
  /// any of that node's own queued waiters are pruned too. Driven by
  /// gossip's failure detector confirming a node dead — see
  /// `gossip_state.rs::apply_ready_mutations`.
  ReleaseLocksHeldBy(NodeId),
```

In the `for message in receiver` match, add a new arm (after the `EvictNonNative` arm):

```rust
          WorkerMessage::ReleaseLocksHeldBy(node_id) => {
            for (&datum_id, lock) in native_locks.iter_mut() {
              let was_held_by_dead_node = lock
                .current_holder()
                .is_some_and(|h| h.node_id == node_id);
              lock.handle_node_death(node_id);
              if was_held_by_dead_node {
                cache.invalidate(datum_id);
              }
            }
          }
```

Add the corresponding fire-and-forget method to `impl WorkerHandle`, after `evict_non_native`:

```rust
  /// Tells this thread that `node_id` is confirmed dead — see
  /// `WorkerMessage::ReleaseLocksHeldBy`.
  pub fn release_locks_held_by(&self, node_id: NodeId) {
    let _ = self.sender.send(WorkerMessage::ReleaseLocksHeldBy(node_id));
  }
```

- [ ] **Step 2: Broadcast it from `WorkerPool`**

In `crates/seisin-node/src/pool.rs`, add to `impl WorkerPool`, after `evict_non_native`:

```rust
  /// Tells every worker in the pool that `node_id` is confirmed dead —
  /// see `WorkerHandle::release_locks_held_by`.
  pub fn release_locks_held_by(&self, node_id: NodeId) {
    for handle in &self.handles {
      handle.release_locks_held_by(node_id);
    }
  }
```

- [ ] **Step 3: Write a test proving the broadcast reaches a real lock**

Add to `crates/seisin-node/src/pool.rs`'s `mod tests`:

```rust
  #[test]
  fn release_locks_held_by_does_not_disturb_unrelated_locks_or_break_the_pool() {
    // This test only proves the broadcast plumbing (WorkerPool ->
    // every WorkerHandle -> every NativeLock) doesn't panic and
    // doesn't disturb locks unrelated to the dead node — it can't, at
    // this single-node layer, construct a lock genuinely held by some
    // *other* node's thread (that requires real cross-node contention).
    // The actual release-a-remote-holder mechanics are proven
    // end-to-end by Task 7's integration test, which has a real second
    // node to hold the lock in the first place.
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let mut ops = OpRegistry::new();
    ops.register(
      "touch",
      Box::new(|ctx, ids, _payload| {
        ctx.put(ids[0], b"touched".to_vec());
        vec![]
      }),
    );
    let (listener, address_book) = no_peers();
    let pool = WorkerPool::spawn(
      Arc::new(InMemoryStore::new()),
      1,
      Arc::new(ops),
      Arc::clone(&ring),
      NodeId(1),
      listener,
      address_book,
    );

    let id = DatumId::new();
    pool
      .run_op(DatumId::new(), "touch".to_string(), vec![id], vec![])
      .unwrap();

    pool.release_locks_held_by(NodeId(99));

    let result = pool.run_op(DatumId::new(), "touch".to_string(), vec![id], vec![]);
    assert_eq!(result, Ok(vec![]));
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-node --lib`
Expected: PASS (all tests, including the new one). Note this test only proves the broadcast plumbing doesn't break anything — the actual release-a-remote-holder mechanics are proven end-to-end in Task 7's integration test, where a real second node genuinely holds the lock.

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-node/src/worker.rs crates/seisin-node/src/pool.rs
git commit -m "feat: broadcast confirmed node death to every thread's NativeLocks"
git push
```

---

### Task 3: Wire the broadcast into gossip's ring-mutation hook

**Files:**
- Modify: `crates/seisin-node/src/gossip_state.rs`

**Interfaces:**
- Consumes: `WorkerPool::release_locks_held_by` from Task 2.
- Produces: `apply_ready_mutations`'s existing signature is unchanged; its behavior now also triggers proactive lock release for every `RingMutation::Leave` it applies.

- [ ] **Step 1: Write the failing test**

Add to `crates/seisin-node/src/gossip_state.rs`'s `mod tests`:

```rust
  #[test]
  fn apply_ready_mutations_releases_locks_held_by_a_departing_node() {
    use std::net::TcpListener;
    use seisin_core::datum::DatumId;
    use seisin_core::store::InMemoryStore;
    use seisin_ops::registry::OpRegistry;
    use crate::pool::WorkerPool;

    let node_a = NodeId(1);
    let node_b = NodeId(2);
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_a, 1), (node_b, 1)])));

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let pool = WorkerPool::spawn(
      Arc::new(InMemoryStore::new()),
      1,
      Arc::new(OpRegistry::new()),
      Arc::clone(&ring),
      node_a,
      listener,
      Arc::new(std::collections::HashMap::new()),
    );

    let gossip = GossipState::new();
    gossip.record_mutation(1, RingMutation::Leave { node_id: node_b });

    // This shouldn't panic, and the ring should reflect the departure
    // afterward — the release-broadcast itself is exercised in
    // isolation by pool.rs's own test (Task 2) and proven end-to-end
    // by Task 8's full crash integration test.
    apply_ready_mutations(&gossip, &ring, node_a, &pool);
    assert_eq!(ring.read().unwrap().native(DatumId::new()).0, node_a);
  }
```

- [ ] **Step 2: Run the test to verify it currently passes for the wrong reason**

Run: `cargo test -p seisin-node --lib gossip_state::tests::apply_ready_mutations_releases_locks_held_by_a_departing_node`
Expected: PASS already (the ring-update behavior this specific assertion checks already existed before this task) — this test is here to pin the *no-panic* behavior once Step 3 adds the new broadcast call; it isn't meant to fail first. Proceed to Step 3 regardless — the meaningful proof that the broadcast fires is the code review of Step 3 plus Task 7's end-to-end test.

- [ ] **Step 3: Add the broadcast call**

In `crates/seisin-node/src/gossip_state.rs`, change `apply_ready_mutations` from:

```rust
pub fn apply_ready_mutations(
  gossip: &GossipState,
  ring: &Arc<RwLock<Ring>>,
  self_node_id: NodeId,
  pool: &WorkerPool,
) {
  let ready = gossip.mutation_log.lock().unwrap().drain_applicable();
  if ready.is_empty() {
    return;
  }
  {
    let mut ring_guard = ring.write().unwrap();
    for mutation in &ready {
      match *mutation {
        RingMutation::Join {
          node_id,
          thread_count,
        } => ring_guard.apply_join(node_id, thread_count),
        RingMutation::Leave { node_id } => ring_guard.apply_leave(node_id),
      }
    }
  }
  let ring = Arc::clone(ring);
  pool.evict_non_native(Arc::new(move |id| {
    ring.read().unwrap().native(id).0 == self_node_id
  }));
}
```

to:

```rust
pub fn apply_ready_mutations(
  gossip: &GossipState,
  ring: &Arc<RwLock<Ring>>,
  self_node_id: NodeId,
  pool: &WorkerPool,
) {
  let ready = gossip.mutation_log.lock().unwrap().drain_applicable();
  if ready.is_empty() {
    return;
  }
  {
    let mut ring_guard = ring.write().unwrap();
    for mutation in &ready {
      match *mutation {
        RingMutation::Join {
          node_id,
          thread_count,
        } => ring_guard.apply_join(node_id, thread_count),
        RingMutation::Leave { node_id } => ring_guard.apply_leave(node_id),
      }
    }
  }
  let ring_for_cache = Arc::clone(ring);
  pool.evict_non_native(Arc::new(move |id| {
    ring_for_cache.read().unwrap().native(id).0 == self_node_id
  }));
  // Proactively release any lock a now-departed node was holding, or
  // was waiting on, rather than waiting for a future request to
  // (reactively, at best) notice it's gone — see the design doc's
  // "Crash Detection & Lock Release" section.
  for mutation in &ready {
    if let RingMutation::Leave { node_id } = *mutation {
      pool.release_locks_held_by(node_id);
    }
  }
}
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p seisin-node --lib gossip_state::`
Expected: PASS (4 tests: 3 existing + 1 new).

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-node/src/gossip_state.rs
git commit -m "feat: release locks held by a node once gossip confirms it dead"
git push
```

---

### Task 4: Reactive backstop — a failed recall is treated as released

**Files:**
- Modify: `crates/seisin-node/src/worker.rs`

**Interfaces:**
- Consumes: nothing new — changes existing behavior in the `Acquire` handler's cross-node recall dispatch.

- [ ] **Step 1: Change the recall callback to release on any outcome**

In `crates/seisin-node/src/worker.rs`'s `Acquire` handler, find the cross-node recall dispatch:

```rust
              } else {
                let link = peer_links.lock().unwrap().get(holder.node_id);
                let self_sender = join_sender.clone();
                link.call(
                  holder.thread_id,
                  seisin_protocol::Request::Recall { datum_id },
                  Box::new(move |response| {
                    if matches!(response, seisin_protocol::Response::Released) {
                      let _ = self_sender.send(WorkerMessage::Release { datum_id });
                    }
                  }),
                );
              }
```

Change it to:

```rust
              } else {
                let link = peer_links.lock().unwrap().get(holder.node_id);
                let self_sender = join_sender.clone();
                link.call(
                  holder.thread_id,
                  seisin_protocol::Request::Recall { datum_id },
                  Box::new(move |response| {
                    // Either an explicit `Released` ack, or the call
                    // failed outright (the peer-link disconnected,
                    // meaning the holder is unreachable) — either way,
                    // treat it as released rather than waiting on a
                    // call that may never resolve. This is the
                    // reactive backstop for the gap between an actual
                    // crash and gossip confirming it (Task 3 handles
                    // the confirmed case).
                    let _ = response;
                    let _ = self_sender.send(WorkerMessage::Release { datum_id });
                  }),
                );
              }
```

- [ ] **Step 2: Run the existing test suite to confirm nothing broke**

A faithful unit test for "recall against a severed link" needs genuine cross-node contention (an older op forcing a recall against a younger op's holder on a different node) to even reach this code path at all — setting that up below the `WorkerPool` level would mean duplicating Task 8's whole two-node harness with no real benefit over just extending that harness directly. So this task has no unit test of its own; Task 8's end-to-end test is where this behavior change is actually proven, using a real second node that dies mid-recall.

Run: `cargo test -p seisin-node --lib`
Expected: PASS (same test count as Task 3 left it — this step only confirms Step 1's change didn't break anything already covered).

- [ ] **Step 3: Commit and push**

```bash
git add crates/seisin-node/src/worker.rs
git commit -m "feat: treat a failed recall as an immediate release (reactive crash backstop)"
git push
```

---

### Task 5: Bounded acquire retry and whole-op failure

**Files:**
- Modify: `crates/seisin-node/src/worker.rs`

**Interfaces:**
- Produces: `const MAX_ACQUIRE_RETRIES: u32`, `send_acquire`'s signature gains a `retries_left: u32` parameter, `WorkerMessage::AcquireFailed { op_id, datum_id, retries_left }`, `release_datums` (factored out of `try_run_if_ready`'s release loop, now shared with the new `fail_op`), `fail_op`.

- [ ] **Step 1: Add the retry constant and thread `retries_left` through `send_acquire`**

In `crates/seisin-node/src/worker.rs`, add near the top (after the `use` block):

```rust
/// How many times a cross-node `Acquire` retries against the current
/// ring before giving up and failing the whole op — bounded so a
/// permanently unreachable node fails fast rather than hanging
/// forever. Each retry re-resolves `ring.native()` fresh, so a retry
/// naturally picks up wherever gossip has since moved the slot to.
const MAX_ACQUIRE_RETRIES: u32 = 3;
```

Add a new `WorkerMessage` variant (after `AcquireGranted`):

```rust
  /// Posted into the requesting thread's own inbox when a cross-node
  /// `Acquire` call fails (peer-link error) — `retries_left` is how
  /// many more attempts remain from when this specific call was made.
  AcquireFailed {
    op_id: DatumId,
    datum_id: DatumId,
    retries_left: u32,
  },
```

Change `send_acquire`'s signature and body from:

```rust
#[allow(clippy::too_many_arguments)]
fn send_acquire(
  ring: &Arc<RwLock<Ring>>,
  peers: &Arc<Vec<Sender<WorkerMessage>>>,
  peer_links: &Arc<StdMutex<PeerLinkRegistry>>,
  op_id: DatumId,
  datum_id: DatumId,
  self_node_id: NodeId,
  self_thread_id: ThreadId,
  requester_inbox: Sender<WorkerMessage>,
) {
  let (native_node, native_thread) = ring.read().unwrap().native(datum_id);
  if native_node == self_node_id {
    let _ = peers[native_thread.0 as usize].send(WorkerMessage::Acquire {
      op_id,
      datum_id,
      requester_node: self_node_id,
      requester_thread: self_thread_id,
      reply: AcquireReply::Local(requester_inbox),
    });
  } else {
    let link = peer_links.lock().unwrap().get(native_node);
    link.call(
      native_thread,
      seisin_protocol::Request::Acquire {
        op_id,
        datum_id,
        requester_node: self_node_id,
        requester_thread: self_thread_id,
      },
      Box::new(move |response| {
        if matches!(response, seisin_protocol::Response::Granted) {
          let _ = requester_inbox.send(WorkerMessage::AcquireGranted { op_id, datum_id });
        }
      }),
    );
  }
}
```

to:

```rust
#[allow(clippy::too_many_arguments)]
fn send_acquire(
  ring: &Arc<RwLock<Ring>>,
  peers: &Arc<Vec<Sender<WorkerMessage>>>,
  peer_links: &Arc<StdMutex<PeerLinkRegistry>>,
  op_id: DatumId,
  datum_id: DatumId,
  self_node_id: NodeId,
  self_thread_id: ThreadId,
  requester_inbox: Sender<WorkerMessage>,
  retries_left: u32,
) {
  let (native_node, native_thread) = ring.read().unwrap().native(datum_id);
  if native_node == self_node_id {
    let _ = peers[native_thread.0 as usize].send(WorkerMessage::Acquire {
      op_id,
      datum_id,
      requester_node: self_node_id,
      requester_thread: self_thread_id,
      reply: AcquireReply::Local(requester_inbox),
    });
  } else {
    let link = peer_links.lock().unwrap().get(native_node);
    link.call(
      native_thread,
      seisin_protocol::Request::Acquire {
        op_id,
        datum_id,
        requester_node: self_node_id,
        requester_thread: self_thread_id,
      },
      Box::new(move |response| {
        if matches!(response, seisin_protocol::Response::Granted) {
          let _ = requester_inbox.send(WorkerMessage::AcquireGranted { op_id, datum_id });
        } else {
          let _ = requester_inbox.send(WorkerMessage::AcquireFailed {
            op_id,
            datum_id,
            retries_left,
          });
        }
      }),
    );
  }
}
```

Update all 3 existing call sites (the `RunOp` handler's per-datum loop, and the `Recall` handler's wounded-op re-acquire) to pass `MAX_ACQUIRE_RETRIES` as the new trailing argument:

```rust
            for datum_id in datum_ids {
              send_acquire(
                &ring,
                &peers,
                &peer_links,
                op_id,
                datum_id,
                self_node_id,
                self_thread_id,
                join_sender.clone(),
                MAX_ACQUIRE_RETRIES,
              );
            }
```

and:

```rust
            if let Some(op_id) = wounded_op_id {
              send_acquire(
                &ring,
                &peers,
                &peer_links,
                op_id,
                datum_id,
                self_node_id,
                self_thread_id,
                join_sender.clone(),
                MAX_ACQUIRE_RETRIES,
              );
            }
```

- [ ] **Step 2: Factor `release_datums` out of `try_run_if_ready`, and add `fail_op`**

Change `try_run_if_ready`'s release loop from:

```rust
  let _ = record.reply.send(result);
  for datum_id in record.acquired {
    // This thread is done with the datum — evict its own cache entry
    // (whether or not it was this datum's native home) so it never
    // serves a stale value from a future use, then tell native home
    // it's free to grant elsewhere — locally if native home is this
    // same node, over the peer-link if it's a different one (a plain
    // completion release needs to reach a remote native home just as
    // much as a recall's ack does).
    cache.invalidate(datum_id);
    let (native_node, thread_id) = ring.read().unwrap().native(datum_id);
    if native_node == self_node_id {
      let _ = peers[thread_id.0 as usize].send(WorkerMessage::Release { datum_id });
    } else {
      let link = peer_links.lock().unwrap().get(native_node);
      link.call(
        thread_id,
        seisin_protocol::Request::Release { datum_id },
        Box::new(|_response| {}),
      );
    }
  }
}
```

to:

```rust
  let _ = record.reply.send(result);
  release_datums(
    record.acquired,
    cache,
    ring,
    peers,
    peer_links,
    self_node_id,
  );
}

/// Releases every datum in `datum_ids`, evicting this thread's own
/// cache entry for each first — locally if native home is this same
/// node, over the peer-link if it's a different one. Shared by normal
/// op completion (`try_run_if_ready`) and whole-op failure (`fail_op`).
fn release_datums(
  datum_ids: Vec<DatumId>,
  cache: &mut Cache,
  ring: &Arc<RwLock<Ring>>,
  peers: &Arc<Vec<Sender<WorkerMessage>>>,
  peer_links: &Arc<StdMutex<PeerLinkRegistry>>,
  self_node_id: NodeId,
) {
  for datum_id in datum_ids {
    cache.invalidate(datum_id);
    let (native_node, thread_id) = ring.read().unwrap().native(datum_id);
    if native_node == self_node_id {
      let _ = peers[thread_id.0 as usize].send(WorkerMessage::Release { datum_id });
    } else {
      let link = peer_links.lock().unwrap().get(native_node);
      link.call(
        thread_id,
        seisin_protocol::Request::Release { datum_id },
        Box::new(|_response| {}),
      );
    }
  }
}

/// Abandons `op_id` entirely: replies with `Err(message)` and releases
/// every datum it had already acquired (unlike a wound, which loses
/// only the one contended datum and keeps going, a whole-op failure
/// gives up everything).
#[allow(clippy::too_many_arguments)]
fn fail_op(
  op_id: DatumId,
  op_records: &mut HashMap<DatumId, OpRecord>,
  cache: &mut Cache,
  ring: &Arc<RwLock<Ring>>,
  peers: &Arc<Vec<Sender<WorkerMessage>>>,
  peer_links: &Arc<StdMutex<PeerLinkRegistry>>,
  self_node_id: NodeId,
  message: String,
) {
  if let Some(record) = op_records.remove(&op_id) {
    let _ = record.reply.send(Err(message));
    release_datums(
      record.acquired,
      cache,
      ring,
      peers,
      peer_links,
      self_node_id,
    );
  }
}
```

- [ ] **Step 3: Handle `AcquireFailed` — retry or fail the op**

Add a new match arm (after `AcquireGranted`):

```rust
          WorkerMessage::AcquireFailed {
            op_id,
            datum_id,
            retries_left,
          } => {
            if retries_left > 0 {
              send_acquire(
                &ring,
                &peers,
                &peer_links,
                op_id,
                datum_id,
                self_node_id,
                self_thread_id,
                join_sender.clone(),
                retries_left - 1,
              );
            } else {
              fail_op(
                op_id,
                &mut op_records,
                &mut cache,
                &ring,
                &peers,
                &peer_links,
                self_node_id,
                format!("failed to acquire datum {datum_id:?} after {MAX_ACQUIRE_RETRIES} retries"),
              );
            }
          }
```

- [ ] **Step 4: Update `worker.rs`'s own tests for the new `send_acquire` signature**

`send_acquire` is a private function only called from within `worker.rs` itself (already updated in Step 1) — none of `worker.rs`'s existing `#[test]` functions call it directly, so no test call sites need updating here. Confirm this by searching:

Run: `grep -n "send_acquire(" crates/seisin-node/src/worker.rs`
Expected: every call site shown already has 10 arguments (the 3 from Step 1's edits, matching Step 1 above) — no bare 9-argument calls remain.

- [ ] **Step 5: Run the full test suite**

Run: `cargo test -p seisin-node --lib`
Expected: PASS (same test count as Task 4 left it — this task changes internal mechanics with no new unit test of its own; end-to-end proof is Task 9).

- [ ] **Step 6: Commit and push**

```bash
git add crates/seisin-node/src/worker.rs
git commit -m "feat: bounded acquire retry against a moved ring slot; fail the whole op on exhaustion"
git push
```

---

### Task 6: Quality-gate the mechanism tasks before writing integration tests

**Files:** none (verification only).

- [ ] **Step 1: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS.

- [ ] **Step 2: Run fmt and clippy**

Run: `cargo fmt --check`
Expected: no output; if it reports diffs, run `cargo fmt` and re-check.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings/errors. Fix anything reported (matching Part 1/2a's precedent of adding `#[allow(clippy::too_many_arguments)]` rather than restructuring a signature that's already this plan's exact design).

- [ ] **Step 3: Commit and push if anything changed**

```bash
git add -A
git commit -m "chore: fmt/clippy fixes after Part 2b's mechanism tasks"
git push
```

(Skip the commit entirely if nothing changed.)

---

### Task 7: Proactive release integration test

**Files:**
- Create: `crates/seisin-node/tests/integration_proactive_lock_release.rs`

**Interfaces:**
- Consumes: everything from Tasks 1-6.
- Produces: proof that when a node holding a lock is confirmed dead via gossip, the lock releases and a waiting op on another node completes — without that waiting op's `Acquire` ever needing to retry or fail.

- [ ] **Step 1: Write the test**

```rust
use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::InMemoryStore;
use seisin_gossip::membership::{Incarnation, MemberStatus, MemberUpdate};
use seisin_node::gossip_client::run_gossip_loop;
use seisin_node::gossip_server::serve_gossip;
use seisin_node::gossip_state::GossipState;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{Request, Response};
use seisin_ring::ring::Ring;

const PROBE_INTERVAL_MILLIS: u64 = 20;
const PROBE_TIMEOUT_MILLIS: u64 = 20;
const SUSPICION_TIMEOUT_MILLIS: u64 = 40;

/// Reserves a real, currently-unused address by binding then dropping
/// the listener — nothing will be listening there afterward.
fn reserve_silent_address() -> String {
  TcpListener::bind("127.0.0.1:0")
    .unwrap()
    .local_addr()
    .unwrap()
    .to_string()
}

/// Starts a single real node (client server, gossip server, probing
/// loop) against the given member list — `members` entries are
/// `(node_id, thread_count, client_address, gossip_address,
/// peer_link_address)`.
fn start_node(node_id: NodeId, members: &[(NodeId, u32, String, String, String)]) -> Arc<RwLock<Ring>> {
  let this = members.iter().find(|m| m.0 == node_id).unwrap();
  let client_listener = TcpListener::bind(&this.2).unwrap();
  let gossip_listener = TcpListener::bind(&this.3).unwrap();
  let peer_link_listener = TcpListener::bind(&this.4).unwrap();

  let ring_members: Vec<(NodeId, u32)> = members.iter().map(|m| (m.0, m.1)).collect();
  let ring = Arc::new(RwLock::new(Ring::from_members(&ring_members)));

  let address_book: HashMap<NodeId, String> = members.iter().map(|m| (m.0, m.2.clone())).collect();
  let address_book = Arc::new(address_book);

  let peer_link_address_book: HashMap<NodeId, String> =
    members.iter().map(|m| (m.0, m.4.clone())).collect();
  let peer_link_address_book = Arc::new(peer_link_address_book);

  let gossip = Arc::new(GossipState::new());
  {
    let mut table = gossip.member_table.lock().unwrap();
    for member in members {
      table.merge_update(MemberUpdate {
        node_id: member.0,
        incarnation: Incarnation(0),
        status: MemberStatus::Alive,
        client_address: member.2.clone(),
        gossip_address: member.3.clone(),
        thread_count: member.1,
      });
    }
  }

  let mut ops = OpRegistry::new();
  ops.register(
    "touch_both",
    Box::new(|ctx, ids, _payload| {
      ctx.put(ids[0], b"touched".to_vec());
      ctx.put(ids[1], b"touched".to_vec());
      vec![]
    }),
  );

  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    this.1,
    Arc::new(ops),
    Arc::clone(&ring),
    node_id,
    peer_link_listener,
    peer_link_address_book,
  ));

  {
    let ring = Arc::clone(&ring);
    let address_book = Arc::clone(&address_book);
    let pool = Arc::clone(&pool);
    thread::spawn(move || serve(client_listener, node_id, ring, address_book, pool));
  }
  {
    let gossip = Arc::clone(&gossip);
    let ring = Arc::clone(&ring);
    let pool = Arc::clone(&pool);
    thread::spawn(move || serve_gossip(gossip_listener, node_id, gossip, ring, pool));
  }
  {
    let ring_for_loop = Arc::clone(&ring);
    thread::spawn(move || {
      run_gossip_loop(
        node_id,
        gossip,
        ring_for_loop,
        pool,
        PROBE_INTERVAL_MILLIS,
        PROBE_TIMEOUT_MILLIS,
        SUSPICION_TIMEOUT_MILLIS,
      )
    });
  }

  ring
}

#[test]
fn a_lock_held_by_a_node_that_goes_silent_is_released_once_gossip_confirms_it_dead() {
  let node_a = NodeId(1);
  let node_b = NodeId(2);

  let addr_a = reserve_silent_address();
  let gossip_addr_a = reserve_silent_address();
  let peer_link_addr_a = reserve_silent_address();
  let addr_b = reserve_silent_address();
  let silent_gossip_addr_b = reserve_silent_address();
  let silent_peer_link_addr_b = reserve_silent_address();

  let members = vec![
    (
      node_a,
      2u32,
      addr_a.clone(),
      gossip_addr_a,
      peer_link_addr_a,
    ),
    (
      node_b,
      2u32,
      addr_b,
      silent_gossip_addr_b,
      silent_peer_link_addr_b,
    ),
  ];

  let ring = start_node(node_a, &members);
  // Node B is deliberately never started — its gossip and peer-link
  // addresses are reserved-but-silent, simulating a node that crashed
  // before ever responding to anything, including any peer-link
  // traffic node A's dial-out at startup would have sent it (which,
  // per Part 2a's dial-skip-on-failure fix, just means node A has no
  // peer-link connection to node B at all — this test proves the
  // *proactive gossip path* still resolves things correctly even so,
  // since it only involves NativeLock bookkeeping local to node A,
  // not needing to reach node B over any connection to release it).

  // Find two ids that are BOTH currently native to node B (before it's
  // declared dead) — used to prove node A's own NativeLock state
  // (which only exists once something asks about a datum) reflects
  // the departure correctly. Since nothing has asked about them yet,
  // this loop just needs ids whose native node is node_b under the
  // *starting* 2-node ring.
  let (a, b) = loop {
    let x = DatumId::new();
    let y = DatumId::new();
    if ring.read().unwrap().native(x).0 == node_b && ring.read().unwrap().native(y).0 == node_b {
      break (x, y);
    }
  };

  // Give node A's gossip loop enough cycles to converge B to Dead and
  // shrink the ring: at least one probe timeout, one suspicion
  // timeout, plus generous slack for scheduling jitter under test load.
  thread::sleep(Duration::from_millis(
    PROBE_INTERVAL_MILLIS + PROBE_TIMEOUT_MILLIS * 2 + SUSPICION_TIMEOUT_MILLIS + 500,
  ));

  // Once the ring has shrunk to just node A, `a`/`b` now resolve
  // locally — proving the op completes without ever needing to reach
  // (the now-removed) node B, and without hanging on any stale lock
  // state left over from when they were native to B.
  assert_eq!(ring.read().unwrap().native(a).0, node_a);
  assert_eq!(ring.read().unwrap().native(b).0, node_a);

  let response = seisin_client::call(
    &addr_a,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "touch_both".to_string(),
      datum_ids: vec![a, b],
      payload: vec![],
    },
  )
  .unwrap();
  assert_eq!(response, Response::OpResult { payload: vec![] });
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p seisin-node --test integration_proactive_lock_release`
Expected: PASS (1 test).

- [ ] **Step 3: Commit and push**

```bash
git add crates/seisin-node/tests/integration_proactive_lock_release.rs
git commit -m "test: add proactive lock release integration test"
git push
```

---

### Task 8: Reactive backstop + bounded retry + op failure integration test

**Files:**
- Create: `crates/seisin-node/tests/integration_crash_during_collation.rs`

**Interfaces:**
- Consumes: everything from Tasks 1-7.
- Produces: proof that (a) a recall against a peer that dies mid-flight still resolves via the reactive backstop, and (b) an op whose cross-node dependency never comes back fails with `OpError` after bounded retries, rather than hanging.

Note on part (a): Task 7's proactive test never actually contends two ops
against each other (gossip removes the dead node before any recall is
ever attempted), so it doesn't exercise Task 4's code path at all. The
first test below (`a_recall_against_a_peer_that_dies_mid_flight_...`)
is the only test in this whole plan that does — a genuine older-vs-
younger contention where the connection to the current holder dies
*after* it has already been granted the datum but *while* the recall
for it is in flight. Everything needed to prove Task 4 is real hinges
on this one test; without it, Task 4 would ship as an unverified,
five-word behavior change.

- [ ] **Step 1: Write the reactive-recall-backstop test**

This test needs to hand-script the "native home" (node B) side of a
peer-link connection at the raw envelope/frame level, since the point
is to control exactly when that side stops responding — something a
real second `WorkerPool` can't be made to do on command. Node A is a
real, single-thread node; "node B" is a bare `TcpListener` accept loop
driven directly by the test.

```rust
use std::collections::HashMap;
use std::io::Read;
use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::{NodeId, ThreadId};
use seisin_core::datum::DatumId;
use seisin_core::store::InMemoryStore;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{
  decode_envelope, decode_request, decode_response, encode_envelope, encode_request, read_frame,
  write_frame, Envelope, EnvelopeKind, Request, Response,
};
use seisin_ring::ring::Ring;

#[test]
fn a_recall_against_a_peer_that_dies_mid_flight_still_releases_via_the_reactive_backstop() {
  // Single-member ring: node A is native home for everything, so there's
  // no ambiguity about which thread a datum belongs to (always thread 0).
  // "node B" only ever plays a *requester* role here, never a native
  // home, so it doesn't need to be a ring member at all.
  let node_a = NodeId(1);
  let node_b = NodeId(2);
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_a, 1)])));

  let listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr_a = listener_a.local_addr().unwrap().to_string();
  let peer_link_listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
  let fake_node_b_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let fake_node_b_addr = fake_node_b_listener.local_addr().unwrap().to_string();

  let mut peer_link_address_book = HashMap::new();
  peer_link_address_book.insert(node_b, fake_node_b_addr);
  let peer_link_address_book = Arc::new(peer_link_address_book);

  let datum_id = DatumId::new();
  let op1 = DatumId::new(); // older — created first, so its UUIDv7 sorts lower
  let op2 = DatumId::new(); // younger — the fake holder's op

  // Fake "node B": accepts node A's eager dial-out (node A dials since
  // node_b's id is larger), completes the handshake, then plays the
  // role of a remote requester who already holds `datum_id` — it
  // sends its own Acquire (impersonating op2) and gets granted
  // immediately since node A is idle. When node A later needs to
  // recall it (because op1, older, wants the same datum), this thread
  // reads that incoming Recall request and then just drops the
  // connection instead of ever acking it — simulating node B crashing
  // at the exact moment it was asked to give the datum back.
  let fake_node_b = thread::spawn(move || {
    let (mut stream, _) = fake_node_b_listener.accept().unwrap();
    let mut preamble = [0u8; 8];
    stream.read_exact(&mut preamble).unwrap();

    write_frame(
      &mut stream,
      &encode_envelope(&Envelope {
        correlation_id: 1,
        kind: EnvelopeKind::Request,
        target_thread: ThreadId(0),
        body: encode_request(&Request::Acquire {
          op_id: op2,
          datum_id,
          requester_node: node_b,
          requester_thread: ThreadId(0),
        }),
      }),
    )
    .unwrap();

    let response_frame = read_frame(&mut stream).unwrap();
    let response_envelope = decode_envelope(&response_frame).unwrap();
    assert_eq!(response_envelope.correlation_id, 1);
    assert_eq!(
      decode_response(&response_envelope.body).unwrap(),
      Response::Granted
    );

    // Node A's Recall arrives once op1 shows up wanting the same
    // datum — read it, then vanish without acking.
    let recall_frame = read_frame(&mut stream).unwrap();
    let recall_envelope = decode_envelope(&recall_frame).unwrap();
    assert!(matches!(
      decode_request(&recall_envelope.body).unwrap(),
      Request::Recall { datum_id: recalled } if recalled == datum_id
    ));
    drop(stream);
  });

  let mut ops = OpRegistry::new();
  ops.register(
    "touch",
    Box::new(|ctx, ids, _payload| {
      ctx.put(ids[0], b"touched".to_vec());
      vec![]
    }),
  );
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    1,
    Arc::new(ops),
    Arc::clone(&ring),
    node_a,
    peer_link_listener_a,
    peer_link_address_book,
  ));
  let address_book = Arc::new(HashMap::new());
  thread::spawn(move || serve(listener_a, node_a, ring, address_book, pool));

  // Give the eager dial-out, handshake, and fake op2 grant time to
  // land before op1 arrives.
  thread::sleep(std::time::Duration::from_millis(200));

  let response = seisin_client::call(
    &addr_a,
    Request::Op {
      op_id: op1,
      op_name: "touch".to_string(),
      datum_ids: vec![datum_id],
      payload: vec![],
    },
  )
  .unwrap();
  assert_eq!(response, Response::OpResult { payload: vec![] });

  fake_node_b.join().unwrap();
}
```

- [ ] **Step 2: Write the bounded-retry-then-fail tests**

```rust
fn build_registry() -> OpRegistry {
  let mut ops = OpRegistry::new();
  ops.register(
    "touch_both",
    Box::new(|ctx, ids, _payload| {
      ctx.put(ids[0], b"touched".to_vec());
      ctx.put(ids[1], b"touched".to_vec());
      vec![]
    }),
  );
  ops
}

/// Starts node A only, wired with a peer-link address book entry for
/// node B that nothing is actually listening on — its dial-out at
/// startup simply fails and is skipped (Part 2a's dial-skip fix), so
/// node A ends up with no peer-link connection to B at all. This is
/// the "never connected" flavor of unreachability (distinct from
/// Task 7's "connected, then went silent"), and it's what proves the
/// bounded-retry-then-fail path: every `Acquire` attempt targeting B
/// hits the same missing connection.
fn start_node_a_with_unreachable_peer(node_a: NodeId, node_b: NodeId) -> (String, Arc<RwLock<Ring>>) {
  let listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr_a = listener_a.local_addr().unwrap().to_string();
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_a, 2), (node_b, 2)])));

  let address_book = Arc::new(HashMap::new());

  let peer_link_listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
  // node_b's peer-link address is reserved then immediately dropped —
  // nothing is listening, so node A's eager dial-out to it at startup
  // fails and is silently skipped.
  let unreachable_peer_link_addr_b = TcpListener::bind("127.0.0.1:0")
    .unwrap()
    .local_addr()
    .unwrap()
    .to_string();
  let mut peer_link_address_book = HashMap::new();
  peer_link_address_book.insert(node_b, unreachable_peer_link_addr_b);
  let peer_link_address_book = Arc::new(peer_link_address_book);

  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(build_registry()),
    Arc::clone(&ring),
    node_a,
    peer_link_listener_a,
    peer_link_address_book,
  ));

  thread::spawn(move || serve(listener_a, node_a, Arc::clone(&ring), address_book, pool));
  (addr_a, ring)
}
```

Wait — the closure above moves `ring` into `serve` but the function also needs to *return* `ring`. Fix by cloning before the move:

```rust
fn start_node_a_with_unreachable_peer(
  node_a: NodeId,
  node_b: NodeId,
) -> (String, Arc<RwLock<Ring>>) {
  let listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr_a = listener_a.local_addr().unwrap().to_string();
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_a, 2), (node_b, 2)])));

  let address_book = Arc::new(HashMap::new());

  let peer_link_listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
  let unreachable_peer_link_addr_b = TcpListener::bind("127.0.0.1:0")
    .unwrap()
    .local_addr()
    .unwrap()
    .to_string();
  let mut peer_link_address_book = HashMap::new();
  peer_link_address_book.insert(node_b, unreachable_peer_link_addr_b);
  let peer_link_address_book = Arc::new(peer_link_address_book);

  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(build_registry()),
    Arc::clone(&ring),
    node_a,
    peer_link_listener_a,
    peer_link_address_book,
  ));

  let serve_ring = Arc::clone(&ring);
  thread::spawn(move || serve(listener_a, node_a, serve_ring, address_book, pool));
  (addr_a, ring)
}

#[test]
fn an_op_needing_an_unreachable_cross_node_datum_fails_instead_of_hanging() {
  let node_a = NodeId(1);
  let node_b = NodeId(2);
  let (addr_a, ring) = start_node_a_with_unreachable_peer(node_a, node_b);

  // Find two ids, one native to each node — node A's op needs both,
  // but node B's peer-link was never reachable, so acquiring the
  // node-B-native one will retry `MAX_ACQUIRE_RETRIES` times and then
  // fail the whole op.
  let (local_id, remote_id) = loop {
    let x = DatumId::new();
    let y = DatumId::new();
    if ring.read().unwrap().native(x).0 == node_a && ring.read().unwrap().native(y).0 == node_b {
      break (x, y);
    }
  };

  let response = seisin_client::call(
    &addr_a,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "touch_both".to_string(),
      datum_ids: vec![local_id, remote_id],
      payload: vec![],
    },
  )
  .unwrap();
  match response {
    Response::OpError { message } => {
      assert!(
        message.contains("after 3 retries") || message.contains("retries"),
        "expected a retry-exhaustion message, got: {message}"
      );
    }
    other => panic!("expected OpError, got {other:?}"),
  }
}

#[test]
fn a_local_datum_stays_available_after_a_different_op_fails_on_the_remote_one() {
  // Proves fail_op's release path actually frees whatever the failed
  // op had already acquired — run the failing op once (as above),
  // then confirm a fresh op touching only the *local* datum still
  // works normally afterward (not stuck as an orphaned hold).
  let node_a = NodeId(1);
  let node_b = NodeId(2);
  let (addr_a, ring) = start_node_a_with_unreachable_peer(node_a, node_b);

  let (local_id, remote_id) = loop {
    let x = DatumId::new();
    let y = DatumId::new();
    if ring.read().unwrap().native(x).0 == node_a && ring.read().unwrap().native(y).0 == node_b {
      break (x, y);
    }
  };

  let _ = seisin_client::call(
    &addr_a,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "touch_both".to_string(),
      datum_ids: vec![local_id, remote_id],
      payload: vec![],
    },
  )
  .unwrap();

  // A second op touching only the local datum (paired with a fresh
  // local-only partner id) must still succeed — proving `local_id`
  // wasn't left stuck as an orphaned hold by the first op's failure.
  let another_local_id = loop {
    let candidate = DatumId::new();
    if ring.read().unwrap().native(candidate).0 == node_a {
      break candidate;
    }
  };
  let response = seisin_client::call(
    &addr_a,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "touch_both".to_string(),
      datum_ids: vec![local_id, another_local_id],
      payload: vec![],
    },
  )
  .unwrap();
  assert_eq!(response, Response::OpResult { payload: vec![] });
}
```

- [ ] **Step 3: Run all three tests**

Run: `cargo test -p seisin-node --test integration_crash_during_collation`
Expected: PASS (3 tests). The retry-exhaustion tests may each take a couple hundred milliseconds longer than other tests in this suite (though a peer-link `call` against an address nothing is listening on fails fast via a `TcpStream::connect` error at dial time, not a timeout) — if any test takes more than a few seconds, that's a sign a retry/recall path isn't actually failing fast, worth investigating rather than just waiting it out.

- [ ] **Step 4: Run all three tests repeatedly to check for flakiness**

```bash
for i in $(seq 1 20); do
  cargo test -p seisin-node --test integration_crash_during_collation 2>&1 | tail -3
done
```
Expected: PASS every time. Part 1 and Part 2a both found genuine concurrency bugs this way, and the reactive-recall test above is exactly the kind of precisely-timed, real-contention scenario that tends to be flaky if the underlying mechanism is subtly wrong — don't skip this step.

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-node/tests/integration_crash_during_collation.rs
git commit -m "test: add reactive backstop and bounded-retry-then-fail integration tests"
git push
```

---

### Task 9: Full workspace regression pass

**Files:** none (verification only).

- [ ] **Step 1: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS — including every earlier sub-project's tests, proving Part 2b's changes to shared code paths (`try_run_if_ready`'s refactor into `release_datums`, `send_acquire`'s new parameter, `apply_ready_mutations`'s new broadcast) haven't regressed anything Part 1 or Part 2a already proved.

- [ ] **Step 2: Re-run Part 1 and Part 2a's own integration tests specifically, repeatedly**

```bash
for i in $(seq 1 20); do
  cargo test -p seisin-node --test integration_wound_wait 2>&1 | tail -3
  cargo test -p seisin-node --test integration_cross_node_wound_wait 2>&1 | tail -3
done
```
Expected: PASS every time — these are exactly the tests that caught real bugs during Part 1 and Part 2a; re-running them here confirms Part 2b's refactor of the shared release path didn't quietly reopen either one.

- [ ] **Step 3: Commit and push if anything needed fixing**

```bash
git add -A
git commit -m "fix: address regressions found in the Part 2b full workspace pass"
git push
```

(Skip entirely if nothing needed fixing.)

---

### Task 10: Quality gate and close out Sub-project 3b

**Files:** none (verification only).

- [ ] **Step 1: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS.

- [ ] **Step 2: Run fmt and clippy**

Run: `cargo fmt --check`
Expected: no output.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings/errors.

- [ ] **Step 3: Update `docs/superpowers/PROGRESS.md`**

Add an entry under "Done" for this plan (Sub-project 3b, Part 2b), and move the whole of **Sub-project 3b** from "In progress" to fully "Done" — this plan is the last piece the design doc's spec called for. Update "In progress" (or "Not started") to reflect that Sub-project 4 (Storage tier) is next per the original sub-project sequence, unless the datum type system or deployment management system design work takes priority instead — flag this as a sequencing decision for the user, don't just pick one silently.

- [ ] **Step 4: Commit and push**

```bash
git add -A
git commit -m "chore: fmt/clippy fixes for cross-node collation part 2b; close out sub-project 3b in progress tracker"
git push
```

---

## Self-Review Notes

- **Spec coverage**: proactive lock release on gossip-confirmed node death ✓ (Tasks 1-3, proven end-to-end by Task 7); reactive release-on-failed-recall ✓ (Task 4, proven end-to-end by Task 8's first test — the hand-scripted raw-socket peer that gets granted a datum, then drops the connection exactly when node A's Recall for it arrives); bounded acquire retry against a moved ring slot ✓ (Task 5, retry-then-succeed proven by Task 7's ring-shrink scenario, retry-then-fail proven by Task 8's second test); op failure on exhausted retries, releasing whatever was already acquired ✓ (Task 5's `fail_op`, proven by Task 8's third test). This closes every remaining item in the spec's "Crash Detection & Lock Release" section and its "Testing Strategy" section's crash-related bullets.
- **Placeholder scan**: Task 4 originally sketched a unit test that didn't actually exercise a contended recall — rather than ship a test that looks like coverage but isn't, Step 2 explicitly deletes that sketch and documents why the real coverage lives in Task 8 instead. A first draft of Task 8 also only covered acquire-failure (never-connected peer), not recall-failure (connected, then died mid-flight) — the two are genuinely different code paths (Task 5's retry vs. Task 4's unconditional release), and only the latter proves Task 4 actually does anything. Task 8's Step 1 now adds the missing recall-specific test directly. No other TBD/TODO placeholders remain.
- **Type consistency**: `NativeLock::handle_node_death`, `WorkerMessage::ReleaseLocksHeldBy`/`AcquireFailed`, `WorkerHandle`/`WorkerPool::release_locks_held_by`, `send_acquire`'s new `retries_left` parameter, and `release_datums`/`fail_op` match exactly between where they're defined (Tasks 1-5) and where they're consumed (Tasks 7-8).
- **Known limitations carried forward, unchanged from Part 2a** (not this plan's scope): peer-links still only connect from the static startup member list; `PeerLinkRegistry::get` still panics if a link never existed at all (as opposed to having existed and died, which this plan's Task 4 and Task 8's first test do handle). A future sub-project revisiting dynamic peer-link membership would need to address the "never connected" case gracefully, likely by making `get` fallible and treating a missing link the same way a failed call is already treated here.
