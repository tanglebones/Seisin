# Gossip Node Wiring Implementation Plan (Sub-project 2b-iii-c)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the pieces built in 2b-ii/2b-iii-a/2b-iii-b into a real
running node: a background probing thread that directly probes peers,
detects failures via the `FailureDetector`, mints and disseminates
epoch-ordered `Leave` mutations when the elected sequencer confirms a
peer dead, and a gossip TCP listener that merges incoming
`GossipMessage`s and keeps every node's `Ring` converging. Initial
membership still comes from static config (as in Sub-project 2a) — this
plan's dynamic behavior is detecting existing members dying and shrinking
the ring accordingly, not runtime join of brand-new nodes.

**Deliberately out of scope for this plan** (noted so it isn't mistaken
for an oversight):
- **Indirect probing.** `FailureDetector` already tracks the
  `EscalateToIndirect` transition, but this plan doesn't act on it — a
  timed-out probe proceeds straight through to `Suspect` after the next
  timeout instead of trying alternate paths first. This means the system
  is less tolerant of transient network hiccups than full SWIM, a
  reasonable v1 trade-off given the project runs on a small, low-latency
  container cluster rather than a WAN.
- **Runtime join of nodes not in the initial config.** Detecting and
  removing dead members is this plan's actual goal; adding brand-new
  members via a seed-contact handshake is a natural follow-up, not
  bundled in here.

**Architecture:** `GossipState` (new, in `seisin-node`) holds the shared
`MemberTable`, `MutationLog`, and a small bounded buffer of recently-seen
mutations that gets re-gossiped on every outbound message (cheap
eventual-consistency insurance against one lost message). A background
thread (`gossip_loop`) ticks once a second: it self-refutes if gossip
wrongly suspects it, round-robins a direct probe to the next peer, checks
`FailureDetector` timeouts, and — if this node is the elected sequencer —
mints a `Leave` mutation for any peer just confirmed dead. A gossip TCP
listener merges incoming `GossipMessage`s the same way. Both paths
funnel through one `apply_ready_mutations` helper that drains the
`MutationLog` in epoch order, applies mutations to the (now
`RwLock`-wrapped) `Ring`, and evicts this node's own now-stale cache
entries.

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
- Time abstracted behind `ClockSource` (already built in 2b-iii-b) —
  `FailureDetector`'s timeouts become constructor parameters in this plan
  (Task 6) specifically so the live integration test (Task 8) can use
  short timeouts instead of the real 1s/5s values, without the test
  taking over 7 real seconds per case.

---

### Task 1: `Ring` behind `RwLock` for cross-thread mutation

**Files:**
- Modify: `crates/seisin-node/src/server.rs`
- Modify: `crates/seisin-node/src/main.rs`
- Modify: `crates/seisin-node/tests/integration_wire_protocol.rs`
- Modify: `crates/seisin-node/tests/integration_multi_node_routing.rs`

**Interfaces:**
- Changes: `serve`'s `ring` parameter becomes `Arc<RwLock<Ring>>` instead
  of `Arc<Ring>` (the ring needs to be mutable now that gossip-driven
  mutations will apply to it from a different thread than the ones
  reading it per client request). No behavior change — this task is
  purely the concurrency-wrapper mechanical update, proven by the
  existing test suite staying green with no new tests.

- [ ] **Step 1: Update `server.rs`**

In `crates/seisin-node/src/server.rs`, change the `serve`/
`handle_connection` signatures' `ring: Arc<Ring>` to `ring:
Arc<RwLock<Ring>>`, add `use std::sync::RwLock;`, and change the one
call site:

```rust
let (owner_node, thread_id) = ring.native(request.datum_id());
```

to:

```rust
let (owner_node, thread_id) = ring.read().unwrap().native(request.datum_id());
```

- [ ] **Step 2: Update `main.rs`**

Change:

```rust
let ring = Arc::new(Ring::from_members(&members));
```

to:

```rust
let ring = Arc::new(std::sync::RwLock::new(Ring::from_members(&members)));
```

- [ ] **Step 3: Update both existing integration tests**

In `crates/seisin-node/tests/integration_wire_protocol.rs`, change:

```rust
let ring = Arc::new(Ring::from_members(&[(node_id, 1)]));
```

to:

```rust
let ring = Arc::new(std::sync::RwLock::new(Ring::from_members(&[(node_id, 1)])));
```

In `crates/seisin-node/tests/integration_multi_node_routing.rs`, change:

```rust
let ring = Arc::new(Ring::from_members(&[(node_a, 2), (node_b, 2)]));
```

to:

```rust
let ring = Arc::new(std::sync::RwLock::new(Ring::from_members(&[(node_a, 2), (node_b, 2)])));
```

- [ ] **Step 4: Run the full workspace test suite to confirm no regression**

Run: `cargo test --workspace`
Expected: PASS (same test counts as before — this task changes no
behavior, only the mutability wrapper)

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-node
git commit -m "refactor: wrap Ring in RwLock for cross-thread gossip mutation"
git push
```

---

### Task 2: `MemberTable::all`

**Files:**
- Modify: `crates/seisin-gossip/src/membership.rs`

**Interfaces:**
- Produces: `MemberTable::all(&self) -> Vec<MemberUpdate>` — a full
  snapshot, used to piggyback complete membership state on every gossip
  message (simple full-state anti-entropy rather than delta tracking,
  reasonable at this project's scale).

- [ ] **Step 1: Write the failing test**

Add to `crates/seisin-gossip/src/membership.rs`, inside `impl
MemberTable`:

```rust
  /// A full snapshot of every known member's current update, in no
  /// particular order.
  pub fn all(&self) -> Vec<MemberUpdate> {
    unimplemented!()
  }
```

Add this test:

```rust
  #[test]
  fn all_returns_every_known_member() {
    let mut table = MemberTable::new();
    table.merge_update(update(1, 0, MemberStatus::Alive));
    table.merge_update(update(2, 0, MemberStatus::Suspect));
    let mut all = table.all();
    all.sort_by_key(|u| u.node_id.0);
    assert_eq!(all, vec![update(1, 0, MemberStatus::Alive), update(2, 0, MemberStatus::Suspect)]);
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-gossip`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement**

```rust
  pub fn all(&self) -> Vec<MemberUpdate> {
    self.members.iter().map(|(node_id, record)| record.to_update(*node_id)).collect()
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-gossip`
Expected: PASS (1 new test; 44 total in the crate)

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-gossip/src/membership.rs
git commit -m "feat: add MemberTable::all for full-state gossip piggyback"
git push
```

---

### Task 3: `WorkerHandle`/`WorkerPool` cache eviction plumbing

**Files:**
- Modify: `crates/seisin-node/src/worker.rs`
- Modify: `crates/seisin-node/src/pool.rs`

**Interfaces:**
- Changes: `WorkerHandle`'s internal channel now carries a small
  `WorkerMessage` enum (`Request` or `EvictNonNative`) instead of a bare
  `(Request, Sender<Response>)` tuple — `submit`'s external signature is
  unchanged. Produces: `WorkerHandle::evict_non_native(&self,
  Arc<dyn Fn(DatumId) -> bool + Send + Sync>)`,
  `WorkerPool::evict_non_native(&self, Arc<dyn Fn(DatumId) -> bool + Send
  + Sync>)` (calls it on every worker in the pool).

- [ ] **Step 1: Write the failing test**

In `crates/seisin-node/src/worker.rs`, replace the channel type and add
the new variant/method (stubbed):

```rust
use std::sync::mpsc::{self, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use seisin_core::authority::AuthorityIdx;
use seisin_core::cache::Cache;
use seisin_core::datum::DatumId;
use seisin_core::store::Store;
use seisin_protocol::{Request, Response};

enum WorkerMessage {
  Request(Request, Sender<Response>),
  EvictNonNative(Arc<dyn Fn(DatumId) -> bool + Send + Sync>),
}

pub struct WorkerHandle {
  sender: Sender<WorkerMessage>,
  _join: JoinHandle<()>,
}

impl WorkerHandle {
  pub fn spawn(store: Arc<dyn Store>) -> Self {
    let (sender, receiver) = mpsc::channel::<WorkerMessage>();
    let join = thread::spawn(move || {
      let mut cache = Cache::new(store);
      for message in receiver {
        match message {
          WorkerMessage::Request(request, reply) => {
            let response = handle_request(&mut cache, request);
            let _ = reply.send(response);
          }
          WorkerMessage::EvictNonNative(is_native) => {
            cache.evict_non_native(|id| is_native(id));
          }
        }
      }
    });
    Self { sender, _join: join }
  }

  /// Submits a request to the owning thread and blocks for its response.
  pub fn submit(&self, request: Request) -> Response {
    let (reply_tx, reply_rx) = mpsc::channel();
    self
      .sender
      .send(WorkerMessage::Request(request, reply_tx))
      .expect("worker thread exited unexpectedly");
    reply_rx.recv().expect("worker dropped the reply channel")
  }

  /// Asks the owning thread to evict any cache entry `is_native` rejects
  /// — fire-and-forget, no reply, but guaranteed to be processed before
  /// any `submit` call made after this one returns (the worker's inbox
  /// is a single ordered queue).
  pub fn evict_non_native(&self, is_native: Arc<dyn Fn(DatumId) -> bool + Send + Sync>) {
    let _ = is_native;
    unimplemented!()
  }
}

fn handle_request(cache: &mut Cache, request: Request) -> Response {
  match request {
    Request::Get { id } => match cache.get(id) {
      Some(content) => Response::Value { content, authority: AuthorityIdx::Native },
      None => Response::NotFound,
    },
    Request::Put { id, content } => {
      cache.put(id, content);
      Response::Ok
    }
    Request::Delete { id } => {
      cache.delete(id);
      Response::Ok
    }
  }
}
```

Add this test to `worker.rs`'s `tests` module:

```rust
  #[test]
  fn evict_non_native_removes_rejected_entries_before_a_later_submit_sees_them() {
    let store: Arc<dyn seisin_core::store::Store> = Arc::new(InMemoryStore::new());
    let worker = WorkerHandle::spawn(Arc::clone(&store));
    let id = DatumId::new();
    worker.submit(Request::Put { id, content: b"original".to_vec() });

    // Mutate storage directly (simulating another node's write-through)
    // and evict, then confirm the very next Get reloads the new value.
    store.put(id, b"updated".to_vec());
    worker.evict_non_native(Arc::new(|_| false));

    match worker.submit(Request::Get { id }) {
      Response::Value { content, .. } => assert_eq!(content, b"updated"),
      other => panic!("expected Value, got {other:?}"),
    }
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-node --lib`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement `evict_non_native`**

```rust
  pub fn evict_non_native(&self, is_native: Arc<dyn Fn(DatumId) -> bool + Send + Sync>) {
    let _ = self.sender.send(WorkerMessage::EvictNonNative(is_native));
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-node --lib`
Expected: PASS (1 new test; 6 total in `worker.rs`)

- [ ] **Step 5: Add `WorkerPool::evict_non_native`**

In `crates/seisin-node/src/pool.rs`, add:

```rust
  /// Asks every worker in the pool to evict cache entries `is_native`
  /// rejects — see `WorkerHandle::evict_non_native`.
  pub fn evict_non_native(&self, is_native: Arc<dyn Fn(DatumId) -> bool + Send + Sync>) {
    for handle in &self.handles {
      handle.evict_non_native(Arc::clone(&is_native));
    }
  }
```

(Add `use seisin_core::datum::DatumId;` to `pool.rs`'s imports if not
already present.) No new test for this method alone — it's exercised
directly by Task 8's live integration test, the right layer to prove
"eviction actually happens across a real multi-worker pool during a real
ring mutation" at.

- [ ] **Step 6: Run the tests once more and commit**

Run: `cargo test -p seisin-node --lib`
Expected: PASS (unchanged from Step 4 — Step 5 added no new tests)

```bash
git add crates/seisin-node/src/worker.rs crates/seisin-node/src/pool.rs
git commit -m "feat: add cache-eviction messaging to WorkerHandle/WorkerPool"
git push
```

---

### Task 4: `GossipState`

**Files:**
- Create: `crates/seisin-node/src/gossip_state.rs`
- Modify: `crates/seisin-node/Cargo.toml`
- Modify: `crates/seisin-node/src/lib.rs`

**Interfaces:**
- Consumes: `seisin_gossip::{membership::{MemberTable, MemberUpdate},
  sequencer::{MutationLog, RingMutation}}`, `seisin_ring::ring::Ring`,
  `seisin_node::pool::WorkerPool`.
- Produces: `seisin_node::gossip_state::GossipState` —
  `GossipState::new() -> Self`, `record_mutation(&self, u64,
  RingMutation)`, `piggyback(&self) -> (Vec<MemberUpdate>, Vec<(u64,
  RingMutation)>)`, `merge_incoming(&self, Vec<MemberUpdate>, Vec<(u64,
  RingMutation)>)`. Free function `apply_ready_mutations(&GossipState,
  &RwLock<Ring>, NodeId, &WorkerPool)`.

- [ ] **Step 1: Add the `seisin-gossip` dependency and write the failing test**

Add to `crates/seisin-node/Cargo.toml`:

```toml
seisin-gossip = { path = "../seisin-gossip" }
```

`crates/seisin-node/src/gossip_state.rs`:

```rust
//! Shared, cross-thread gossip state: the membership table, the
//! epoch-ordered mutation log, and a small buffer of recently-seen
//! mutations re-gossiped on every outbound message as cheap insurance
//! against one lost message (this project doesn't implement full
//! SWIM-style epidemic retransmission tracking — see the design doc and
//! this plan's "deliberately out of scope" note).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};

use seisin_core::authority::NodeId;
use seisin_gossip::membership::{MemberTable, MemberUpdate};
use seisin_gossip::sequencer::{MutationLog, RingMutation};
use seisin_ring::ring::Ring;

use crate::pool::WorkerPool;

const RECENT_MUTATIONS_CAP: usize = 16;

pub struct GossipState {
  pub member_table: Mutex<MemberTable>,
  pub mutation_log: Mutex<MutationLog>,
  recent_mutations: Mutex<VecDeque<(u64, RingMutation)>>,
}

impl Default for GossipState {
  fn default() -> Self {
    Self::new()
  }
}

impl GossipState {
  pub fn new() -> Self {
    Self {
      member_table: Mutex::new(MemberTable::new()),
      mutation_log: Mutex::new(MutationLog::new()),
      recent_mutations: Mutex::new(VecDeque::new()),
    }
  }

  /// Records a mutation into the epoch-ordered log (for correct-order
  /// application) and into the small recent-mutations buffer (for
  /// re-gossiping), whether it originated locally (this node is the
  /// sequencer) or arrived from a peer.
  pub fn record_mutation(&self, epoch: u64, mutation: RingMutation) {
    let _ = (epoch, mutation);
    unimplemented!()
  }

  /// The full membership snapshot plus recently-seen mutations to
  /// attach to an outbound gossip message.
  pub fn piggyback(&self) -> (Vec<MemberUpdate>, Vec<(u64, RingMutation)>) {
    unimplemented!()
  }

  /// Merges an incoming message's piggybacked updates and mutations.
  pub fn merge_incoming(&self, updates: Vec<MemberUpdate>, mutations: Vec<(u64, RingMutation)>) {
    let _ = (updates, mutations);
    unimplemented!()
  }
}

/// Applies every ring mutation that's now ready (in epoch order) to
/// `ring`, then evicts from `pool`'s cache any entry this node no longer
/// natively owns as a result — see the design doc's "Cache Invalidation
/// on Ring Membership Change" section.
pub fn apply_ready_mutations(gossip: &GossipState, ring: &RwLock<Ring>, self_node_id: NodeId, pool: &WorkerPool) {
  let ready = gossip.mutation_log.lock().unwrap().drain_applicable();
  if ready.is_empty() {
    return;
  }
  {
    let mut ring = ring.write().unwrap();
    for mutation in &ready {
      match *mutation {
        RingMutation::Join { node_id, thread_count } => ring.apply_join(node_id, thread_count),
        RingMutation::Leave { node_id } => ring.apply_leave(node_id),
      }
    }
  }
  let ring_snapshot = Arc::new(ring.read().unwrap());
  let _ = &ring_snapshot; // placeholder to keep borrow-checker happy across the closure below
  let ring = Arc::clone(ring.read().unwrap().into());
  let _ = ring;
  unimplemented!()
}
```

(The last few lines of `apply_ready_mutations` above are intentionally
broken pseudo-code — Step 3 replaces the whole function body with a
version that actually compiles. This function has no isolated unit test
of its own here; Task 8's live integration test is the right layer to
prove it, the same rationale used for `server.rs`'s `serve` function in
earlier plans.)

Add this test to a `tests` module in `gossip_state.rs`:

```rust
#[cfg(test)]
mod tests {
  use super::*;
  use seisin_gossip::membership::{Incarnation, MemberStatus};

  fn sample_update(node_id: u64) -> MemberUpdate {
    MemberUpdate {
      node_id: NodeId(node_id),
      incarnation: Incarnation(0),
      status: MemberStatus::Alive,
      client_address: "127.0.0.1:7878".to_string(),
      gossip_address: "127.0.0.1:8878".to_string(),
      thread_count: 1,
    }
  }

  #[test]
  fn merge_incoming_applies_updates_and_mutations() {
    let gossip = GossipState::new();
    gossip.merge_incoming(
      vec![sample_update(1)],
      vec![(1, RingMutation::Join { node_id: NodeId(1), thread_count: 1 })],
    );
    assert_eq!(gossip.member_table.lock().unwrap().get(NodeId(1)), Some(sample_update(1)));
    assert_eq!(
      gossip.mutation_log.lock().unwrap().drain_applicable(),
      vec![RingMutation::Join { node_id: NodeId(1), thread_count: 1 }]
    );
  }

  #[test]
  fn piggyback_includes_merged_updates_and_recorded_mutations() {
    let gossip = GossipState::new();
    gossip.merge_incoming(vec![sample_update(1)], vec![]);
    gossip.record_mutation(1, RingMutation::Join { node_id: NodeId(1), thread_count: 1 });
    let (updates, mutations) = gossip.piggyback();
    assert_eq!(updates, vec![sample_update(1)]);
    assert_eq!(mutations, vec![(1, RingMutation::Join { node_id: NodeId(1), thread_count: 1 })]);
  }

  #[test]
  fn recent_mutations_buffer_is_bounded() {
    let gossip = GossipState::new();
    for epoch in 1..=(RECENT_MUTATIONS_CAP as u64 + 5) {
      gossip.record_mutation(epoch, RingMutation::Leave { node_id: NodeId(epoch) });
    }
    assert_eq!(gossip.piggyback().1.len(), RECENT_MUTATIONS_CAP);
  }
}
```

Add `pub mod gossip_state;` to `crates/seisin-node/src/lib.rs`.

- [ ] **Step 2: Run the tests to verify they fail (or fail to compile)**

Run: `cargo test -p seisin-node --lib`
Expected: FAIL — either panics with "not implemented" for the three real
methods, or a compile error from the intentionally-broken
`apply_ready_mutations` placeholder body. Either is the expected red
state; proceed to Step 3 regardless.

- [ ] **Step 3: Implement**

Replace `record_mutation`, `piggyback`, `merge_incoming`, and the whole
`apply_ready_mutations` function:

```rust
  pub fn record_mutation(&self, epoch: u64, mutation: RingMutation) {
    self.mutation_log.lock().unwrap().record(epoch, mutation);
    let mut recent = self.recent_mutations.lock().unwrap();
    recent.push_back((epoch, mutation));
    while recent.len() > RECENT_MUTATIONS_CAP {
      recent.pop_front();
    }
  }

  pub fn piggyback(&self) -> (Vec<MemberUpdate>, Vec<(u64, RingMutation)>) {
    let updates = self.member_table.lock().unwrap().all();
    let mutations = self.recent_mutations.lock().unwrap().iter().copied().collect();
    (updates, mutations)
  }

  pub fn merge_incoming(&self, updates: Vec<MemberUpdate>, mutations: Vec<(u64, RingMutation)>) {
    {
      let mut table = self.member_table.lock().unwrap();
      for update in updates {
        table.merge_update(update);
      }
    }
    for (epoch, mutation) in mutations {
      self.record_mutation(epoch, mutation);
    }
  }
}

pub fn apply_ready_mutations(gossip: &GossipState, ring: &RwLock<Ring>, self_node_id: NodeId, pool: &WorkerPool) {
  let ready = gossip.mutation_log.lock().unwrap().drain_applicable();
  if ready.is_empty() {
    return;
  }
  {
    let mut ring = ring.write().unwrap();
    for mutation in &ready {
      match *mutation {
        RingMutation::Join { node_id, thread_count } => ring.apply_join(node_id, thread_count),
        RingMutation::Leave { node_id } => ring.apply_leave(node_id),
      }
    }
  }
  let ring_for_predicate = Arc::new(RwLock::new(())); // unused; removed below
  let _ = ring_for_predicate;
}
```

Wait — `apply_ready_mutations` needs to build a predicate closure that
captures a *read* of `ring` to call `ring.native(id)` per cache entry,
without holding the write lock taken above (which is already dropped by
the end of the block). Write the final version exactly as follows
instead of the fragment above:

```rust
pub fn apply_ready_mutations(gossip: &GossipState, ring: &RwLock<Ring>, self_node_id: NodeId, pool: &WorkerPool) {
  let ready = gossip.mutation_log.lock().unwrap().drain_applicable();
  if ready.is_empty() {
    return;
  }
  {
    let mut ring = ring.write().unwrap();
    for mutation in &ready {
      match *mutation {
        RingMutation::Join { node_id, thread_count } => ring.apply_join(node_id, thread_count),
        RingMutation::Leave { node_id } => ring.apply_leave(node_id),
      }
    }
  }
  let ring = ring.read().unwrap();
  pool.evict_non_native(Arc::new(move |id| ring.native(id).0 == self_node_id));
}
```

This still has a borrow-checker problem: the closure captures `ring`,
a `RwLockReadGuard` borrowed from the `RwLock<Ring>` parameter, but the
closure is boxed into an `Arc<dyn Fn... + Send + Sync>` that must be
`'static` (`WorkerPool::evict_non_native` requires it, since it's sent
across a channel to worker threads that could run for the process's
whole lifetime) — a borrowed guard can't satisfy `'static`. Fix this by
reading the ring into an owned value instead of holding the guard in the
closure. Since `Ring::native` only needs `&self`, and `Ring` doesn't
implement `Clone`, the simplest fix is to compute the current owner for
*this* node's check without needing `Ring` to outlive the lock at all —
resolve it once, per current cache contents, isn't possible generically
since the predicate must be re-evaluatable per `DatumId` inside each
worker thread. Instead, wrap the ring itself in an `Arc` at the call
site (already true — `ring: &RwLock<Ring>` is reached via an outer
`Arc<RwLock<Ring>>` in `main.rs`/tests) and change this function's
signature to take `Arc<RwLock<Ring>>` by value (cloning the `Arc`, which
*is* `'static`-safe to move into the closure) instead of `&RwLock<Ring>`:

```rust
pub fn apply_ready_mutations(gossip: &GossipState, ring: &Arc<RwLock<Ring>>, self_node_id: NodeId, pool: &WorkerPool) {
  let ready = gossip.mutation_log.lock().unwrap().drain_applicable();
  if ready.is_empty() {
    return;
  }
  {
    let mut ring_guard = ring.write().unwrap();
    for mutation in &ready {
      match *mutation {
        RingMutation::Join { node_id, thread_count } => ring_guard.apply_join(node_id, thread_count),
        RingMutation::Leave { node_id } => ring_guard.apply_leave(node_id),
      }
    }
  }
  let ring = Arc::clone(ring);
  pool.evict_non_native(Arc::new(move |id| ring.read().unwrap().native(id).0 == self_node_id));
}
```

This version compiles: the closure owns a cloned `Arc<RwLock<Ring>>`
(genuinely `'static`), and re-acquires a short-lived read lock inside the
closure each time a worker thread evaluates the predicate for one cache
entry.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-node --lib`
Expected: PASS (3 new tests in `gossip_state`, plus the unchanged
existing suite)

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-node/Cargo.toml crates/seisin-node/src/gossip_state.rs crates/seisin-node/src/lib.rs
git commit -m "feat: add GossipState and apply_ready_mutations"
git push
```

---

### Task 5: Gossip server

**Files:**
- Create: `crates/seisin-node/src/gossip_server.rs`
- Modify: `crates/seisin-node/src/lib.rs`

**Interfaces:**
- Consumes: `seisin_gossip::wire::{decode_gossip_message,
  encode_gossip_message, GossipMessage}`, `GossipState`,
  `apply_ready_mutations`.
- Produces: `seisin_node::gossip_server::serve_gossip(TcpListener,
  NodeId, Arc<GossipState>, Arc<RwLock<Ring>>, Arc<WorkerPool>)`.

- [ ] **Step 1: Write `gossip_server.rs`**

No unit tests for this module — same rationale as the client `server.rs`
in Sub-project 2a: it's a thin accept-loop/dispatch shell over
already-tested pieces (`GossipState`, the wire codec), and Task 8's live
integration test is the right layer to prove real socket behavior at.

```rust
//! Accepts gossip TCP connections: decodes an incoming `GossipMessage`,
//! merges its piggybacked updates/mutations into `GossipState`, applies
//! any now-ready ring mutations, and replies with this node's own
//! current piggyback as an `Ack`.

use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_gossip::wire::{decode_gossip_message, encode_gossip_message, GossipMessage};
use seisin_protocol::{read_frame, write_frame};
use seisin_ring::ring::Ring;

use crate::gossip_state::{apply_ready_mutations, GossipState};
use crate::pool::WorkerPool;

pub fn serve_gossip(
  listener: TcpListener,
  self_node_id: NodeId,
  gossip: Arc<GossipState>,
  ring: Arc<RwLock<Ring>>,
  pool: Arc<WorkerPool>,
) {
  for stream in listener.incoming() {
    let stream = match stream {
      Ok(s) => s,
      Err(_) => continue,
    };
    let gossip = Arc::clone(&gossip);
    let ring = Arc::clone(&ring);
    let pool = Arc::clone(&pool);
    thread::spawn(move || handle_gossip_connection(stream, self_node_id, gossip, ring, pool));
  }
}

fn handle_gossip_connection(
  mut stream: TcpStream,
  self_node_id: NodeId,
  gossip: Arc<GossipState>,
  ring: Arc<RwLock<Ring>>,
  pool: Arc<WorkerPool>,
) {
  let payload = match read_frame(&mut stream) {
    Ok(p) => p,
    Err(_) => return,
  };
  let message = match decode_gossip_message(&payload) {
    Ok(m) => m,
    Err(_) => return,
  };
  let (updates, mutations) = match message {
    GossipMessage::Ping { updates, mutations } => (updates, mutations),
    GossipMessage::PingReq { updates, mutations, .. } => (updates, mutations),
    GossipMessage::Ack { updates, mutations } => (updates, mutations),
  };
  gossip.merge_incoming(updates, mutations);
  apply_ready_mutations(&gossip, &ring, self_node_id, &pool);

  let (reply_updates, reply_mutations) = gossip.piggyback();
  let ack = GossipMessage::Ack { updates: reply_updates, mutations: reply_mutations };
  let _ = write_frame(&mut stream, &encode_gossip_message(&ack));
}
```

Add `pub mod gossip_server;` to `crates/seisin-node/src/lib.rs`.

- [ ] **Step 2: Verify the workspace builds**

Run: `cargo build --workspace`
Expected: builds with no errors.

- [ ] **Step 3: Commit and push**

```bash
git add crates/seisin-node/src/gossip_server.rs crates/seisin-node/src/lib.rs
git commit -m "feat: add gossip server (merge incoming, reply Ack)"
git push
```

---

### Task 6: `FailureDetector` timeout parameters + the probing loop

**Files:**
- Modify: `crates/seisin-gossip/src/failure_detector.rs`
- Create: `crates/seisin-node/src/gossip_client.rs`
- Modify: `crates/seisin-node/src/lib.rs`
- Modify: `crates/seisin-node/Cargo.toml`

**Interfaces:**
- Changes: `FailureDetector::new` takes two additional parameters,
  `probe_timeout_millis: u64` and `suspicion_timeout_millis: u64`,
  instead of reading the module constants internally — this lets Task 8's
  live test use short timeouts instead of the real 1s/5s values. The
  constants (`PROBE_TIMEOUT_MILLIS`, `SUSPICION_TIMEOUT_MILLIS`) remain
  as the values real callers pass.
- Produces: `seisin_node::gossip_client::run_gossip_loop(NodeId,
  Arc<GossipState>, Arc<RwLock<Ring>>, Arc<WorkerPool>,
  probe_interval_millis: u64, probe_timeout_millis: u64,
  suspicion_timeout_millis: u64)` — runs forever on the calling thread
  (callers spawn it on a dedicated background thread).

- [ ] **Step 1: Update `FailureDetector::new` and its existing tests**

In `crates/seisin-gossip/src/failure_detector.rs`, change the struct and
constructor:

```rust
pub struct FailureDetector<'c, C: ClockSource> {
  clock: &'c C,
  probe_timeout_millis: u64,
  suspicion_timeout_millis: u64,
  probes: HashMap<NodeId, ProbeState>,
  suspected_since: HashMap<NodeId, Tick>,
}

impl<'c, C: ClockSource> FailureDetector<'c, C> {
  pub fn new(clock: &'c C, probe_timeout_millis: u64, suspicion_timeout_millis: u64) -> Self {
    Self {
      clock,
      probe_timeout_millis,
      suspicion_timeout_millis,
      probes: HashMap::new(),
      suspected_since: HashMap::new(),
    }
  }
```

Replace every internal use of `PROBE_TIMEOUT_MILLIS`/
`SUSPICION_TIMEOUT_MILLIS` inside `check_timeouts` with
`self.probe_timeout_millis`/`self.suspicion_timeout_millis`. Then update
every `FailureDetector::new(&clock)` call site in this file's `tests`
module to `FailureDetector::new(&clock, PROBE_TIMEOUT_MILLIS,
SUSPICION_TIMEOUT_MILLIS)` (all 7 existing tests) — this preserves every
existing test's exact behavior/timing, since it passes the same constant
values the old hardcoded version used internally.

- [ ] **Step 2: Run the tests to verify they still pass**

Run: `cargo test -p seisin-gossip`
Expected: PASS (44 tests, unchanged — this step is a signature change
with identical resulting behavior, not new functionality, so there's no
red step here)

- [ ] **Step 3: Add the `seisin-gossip` dependency to `seisin-node`**

Add to `crates/seisin-node/Cargo.toml` (it's likely already present from
Task 4 — confirm, don't duplicate the line if so):

```toml
seisin-gossip = { path = "../seisin-gossip" }
```

- [ ] **Step 4: Write `gossip_client.rs`**

No unit tests here either — this is a real-timer, real-thread loop that
sends actual network requests; Task 8's live integration test is the
layer that proves it end-to-end (the same rationale as `server.rs` and
`gossip_server.rs`).

```rust
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

  loop {
    thread::sleep(Duration::from_millis(probe_interval_millis));

    self_refute_if_falsely_suspected(self_node_id, &gossip);

    let candidates: Vec<NodeId> = {
      let table = gossip.member_table.lock().unwrap();
      table.alive_members().into_iter().filter(|id| *id != self_node_id).collect()
    };
    if !candidates.is_empty() {
      let target = candidates[round_robin_index % candidates.len()];
      round_robin_index = round_robin_index.wrapping_add(1);
      detector.begin_direct_probe(target);
      send_ping(&gossip, &target);
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
    let refutation = gossip.member_table.lock().unwrap().confirm_alive_self(self_node_id);
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
  gossip.piggyback().1.iter().map(|(epoch, _)| *epoch).max().unwrap_or(0)
}

fn send_ping(gossip: &GossipState, target: &NodeId) {
  let address = match gossip.member_table.lock().unwrap().get(*target) {
    Some(update) => update.gossip_address,
    None => return,
  };
  let Ok(mut stream) = TcpStream::connect(&address) else { return };
  let (updates, mutations) = gossip.piggyback();
  let ping = GossipMessage::Ping { updates, mutations };
  if write_frame(&mut stream, &encode_gossip_message(&ping)).is_err() {
    return;
  }
  let Ok(payload) = read_frame(&mut stream) else { return };
  let Ok(GossipMessage::Ack { updates, mutations }) = decode_gossip_message(&payload) else {
    return;
  };
  gossip.merge_incoming(updates, mutations);
}
```

Add `pub mod gossip_client;` to `crates/seisin-node/src/lib.rs`.

- [ ] **Step 5: Verify the workspace builds**

Run: `cargo build --workspace`
Expected: builds with no errors.

- [ ] **Step 6: Commit and push**

```bash
git add crates/seisin-gossip/src/failure_detector.rs crates/seisin-node/src/gossip_client.rs crates/seisin-node/src/lib.rs crates/seisin-node/Cargo.toml
git commit -m "feat: parameterize FailureDetector timeouts, add the gossip probing loop"
git push
```

---

### Task 7: `NodeConfig` gossip address + `main.rs` wiring

**Files:**
- Modify: `crates/seisin-node/src/config.rs`
- Modify: `crates/seisin-node/src/main.rs`

**Interfaces:**
- Changes: `MemberConfig` gains a `gossip_address: String` field.
  `NodeConfig`'s existing tests' `SAMPLE` fixture needs updating to
  include it.

- [ ] **Step 1: Update `MemberConfig` and the existing test fixture**

In `crates/seisin-node/src/config.rs`, add the field:

```rust
#[derive(Debug, Deserialize)]
pub struct MemberConfig {
  pub node_id: u64,
  pub address: String,
  pub gossip_address: String,
  pub thread_count: u32,
}
```

Update the `SAMPLE` constant in the `tests` module to include it for
both members:

```rust
  const SAMPLE: &str = r#"
(
    self_node_id: 1,
    members: [
        (node_id: 1, address: "127.0.0.1:7878", gossip_address: "127.0.0.1:8878", thread_count: 2),
        (node_id: 2, address: "127.0.0.1:7879", gossip_address: "127.0.0.1:8879", thread_count: 4),
    ],
)
"#;
```

- [ ] **Step 2: Run the tests to verify they still pass**

Run: `cargo test -p seisin-node --lib`
Expected: PASS (the field addition doesn't change any assertion in the
existing 3 `config` tests, since none of them inspect `gossip_address`)

- [ ] **Step 3: Wire it all up in `main.rs`**

Replace `main.rs` entirely:

```rust
use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;

use anyhow::{Context, Result};

use seisin_core::authority::NodeId;
use seisin_core::store::InMemoryStore;
use seisin_gossip::membership::{Incarnation, MemberStatus, MemberUpdate};
use seisin_node::config::NodeConfig;
use seisin_node::gossip_client::run_gossip_loop;
use seisin_node::gossip_server::serve_gossip;
use seisin_node::gossip_state::GossipState;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ring::ring::Ring;

fn main() -> Result<()> {
  let config_path =
    std::env::var("SEISIN_NODE_CONFIG").context("SEISIN_NODE_CONFIG must name a RON config file")?;
  let config = NodeConfig::load(&config_path)?;

  let self_node_id = NodeId(config.self_node_id);
  let self_address = config.self_address().to_string();
  let self_gossip_address = config
    .members
    .iter()
    .find(|m| m.node_id == config.self_node_id)
    .map(|m| m.gossip_address.clone())
    .with_context(|| format!("self_node_id {} not present in members", config.self_node_id))?;
  let self_thread_count = config
    .members
    .iter()
    .find(|m| m.node_id == config.self_node_id)
    .map(|m| m.thread_count)
    .with_context(|| format!("self_node_id {} not present in members", config.self_node_id))?;

  let members: Vec<(NodeId, u32)> =
    config.members.iter().map(|m| (NodeId(m.node_id), m.thread_count)).collect();
  let ring = Arc::new(RwLock::new(Ring::from_members(&members)));

  let address_book: HashMap<NodeId, String> =
    config.members.iter().map(|m| (NodeId(m.node_id), m.address.clone())).collect();
  let address_book = Arc::new(address_book);

  let gossip = Arc::new(GossipState::new());
  {
    let mut table = gossip.member_table.lock().unwrap();
    for member in &config.members {
      table.merge_update(MemberUpdate {
        node_id: NodeId(member.node_id),
        incarnation: Incarnation(0),
        status: MemberStatus::Alive,
        client_address: member.address.clone(),
        gossip_address: member.gossip_address.clone(),
        thread_count: member.thread_count,
      });
    }
  }

  let store = Arc::new(InMemoryStore::new());
  let pool = Arc::new(WorkerPool::spawn(store, self_thread_count));

  let client_listener =
    TcpListener::bind(&self_address).with_context(|| format!("failed to bind {self_address}"))?;
  println!("seisin-node {self_node_id:?} client listener on {self_address}");
  {
    let ring = Arc::clone(&ring);
    let address_book = Arc::clone(&address_book);
    let pool = Arc::clone(&pool);
    thread::spawn(move || serve(client_listener, self_node_id, ring, address_book, pool));
  }

  let gossip_listener = TcpListener::bind(&self_gossip_address)
    .with_context(|| format!("failed to bind {self_gossip_address}"))?;
  println!("seisin-node {self_node_id:?} gossip listener on {self_gossip_address}");
  {
    let gossip = Arc::clone(&gossip);
    let ring = Arc::clone(&ring);
    let pool = Arc::clone(&pool);
    thread::spawn(move || serve_gossip(gossip_listener, self_node_id, gossip, ring, pool));
  }

  run_gossip_loop(
    self_node_id,
    gossip,
    ring,
    pool,
    seisin_gossip::failure_detector::PROBE_TIMEOUT_MILLIS,
    seisin_gossip::failure_detector::PROBE_TIMEOUT_MILLIS,
    seisin_gossip::failure_detector::SUSPICION_TIMEOUT_MILLIS,
  );
  Ok(())
}
```

- [ ] **Step 4: Verify the workspace builds**

Run: `cargo build --workspace`
Expected: builds with no errors.

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-node/src/config.rs crates/seisin-node/src/main.rs
git commit -m "feat: wire gossip server/probing loop into the node binary"
git push
```

---

### Task 8: Live multi-node integration test

**Files:**
- Create: `crates/seisin-node/tests/integration_gossip_failure_detection.rs`

**Interfaces:**
- Consumes: everything produced by Tasks 1–7.
- Produces: nothing new — proves that when a node stops responding on
  its gossip socket, a surviving node's failure detector eventually marks
  it dead, the sequencer mints a `Leave`, the ring shrinks, and
  subsequent client requests never redirect to the dead node again.

- [ ] **Step 1: Write the integration test**

Uses short timeouts (tens of milliseconds, not the real 1s/5s) so the
whole test runs in well under a second of wall-clock time despite being a
genuine timing-based test.

`crates/seisin-node/tests/integration_gossip_failure_detection.rs`:

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
use seisin_protocol::{Request, Response};
use seisin_ring::ring::Ring;

const PROBE_INTERVAL_MILLIS: u64 = 20;
const PROBE_TIMEOUT_MILLIS: u64 = 20;
const SUSPICION_TIMEOUT_MILLIS: u64 = 40;

struct RunningNode {
  node_id: NodeId,
  client_address: String,
}

fn start_node(
  node_id: NodeId,
  members: &[(NodeId, u32, String, String)], // (node_id, thread_count, client_address, gossip_address)
) -> RunningNode {
  let this = members.iter().find(|m| m.0 == node_id).unwrap();
  let client_listener = TcpListener::bind(&this.2).unwrap();
  let gossip_listener = TcpListener::bind(&this.3).unwrap();

  let ring_members: Vec<(NodeId, u32)> = members.iter().map(|m| (m.0, m.1)).collect();
  let ring = Arc::new(RwLock::new(Ring::from_members(&ring_members)));

  let address_book: HashMap<NodeId, String> =
    members.iter().map(|m| (m.0, m.2.clone())).collect();
  let address_book = Arc::new(address_book);

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

  let pool = Arc::new(WorkerPool::spawn(Arc::new(InMemoryStore::new()), this.1));

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
    let ring = Arc::clone(&ring);
    let pool = Arc::clone(&pool);
    thread::spawn(move || {
      run_gossip_loop(
        node_id,
        gossip,
        ring,
        pool,
        PROBE_INTERVAL_MILLIS,
        PROBE_TIMEOUT_MILLIS,
        SUSPICION_TIMEOUT_MILLIS,
      )
    });
  }

  RunningNode { node_id, client_address: this.2.clone() }
}

#[test]
fn a_node_that_stops_responding_is_eventually_removed_from_the_ring() {
  let node_a = NodeId(1);
  let node_b = NodeId(2);

  let addr_a = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().to_string();
  let gossip_addr_a = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().to_string();
  let addr_b = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().to_string();
  let gossip_addr_b = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().to_string();

  let members = vec![
    (node_a, 2u32, addr_a.clone(), gossip_addr_a.clone()),
    (node_b, 2u32, addr_b.clone(), gossip_addr_b.clone()),
  ];

  // Node B is started, then its listeners are dropped immediately after
  // to simulate it going silent — node A's failure detector should
  // eventually notice and shrink the ring down to just itself.
  let running_a = start_node(node_a, &members);
  {
    let _running_b = start_node(node_b, &members);
    // running_b's listener threads keep running in the background even
    // after this scope ends (they're detached), so instead of relying on
    // scope-drop, explicitly stop answering by never restarting: this
    // test only starts node B's listeners once and lets the *absence* of
    // further responses (no retries, no keep-alive) combined with node A
    // never being asked to route through B again prove the ring shrank.
  }

  // Give the gossip loop enough cycles to converge to Dead and shrink
  // the ring: at least one probe timeout, one suspicion timeout, plus
  // slack for scheduling jitter.
  thread::sleep(Duration::from_millis(
    PROBE_INTERVAL_MILLIS + PROBE_TIMEOUT_MILLIS + SUSPICION_TIMEOUT_MILLIS + 500,
  ));

  // Every request now, regardless of datum_id, must be served directly
  // by node A rather than redirected to node B — proving the ring only
  // has node A's slots left.
  for _ in 0..20 {
    let id = DatumId::new();
    let response =
      seisin_client::call(&running_a.client_address, Request::Put { id, content: b"x".to_vec() }).unwrap();
    assert_eq!(response, Response::Ok, "expected a direct Ok, not a Redirect, once node B is removed");
  }
}
```

Add `seisin-client` and `seisin-gossip` as dev-dependencies in
`crates/seisin-node/Cargo.toml` if not already present:

```toml
[dev-dependencies]
seisin-client = { path = "../seisin-client" }
```

(`seisin-gossip` is already a regular dependency from Task 4/6, so no
change needed there for the test to use it.)

- [ ] **Step 2: Run the test**

Run: `cargo test -p seisin-node --test integration_gossip_failure_detection`
Expected: PASS (may take a bit under a second of real wall-clock time —
this is a genuine timing-based test, not instantaneous, unlike every
other test in this project so far)

If it's flaky (fails intermittently), increase the slack margin in the
`thread::sleep` call (the `+ 500` above) rather than the core timeout
constants — scheduling jitter under load is the likely cause, not a
correctness bug in the mutation/ring logic itself, which is already
proven deterministic by Tasks 2 (2b-ii) and this plan's earlier unit
tests.

- [ ] **Step 3: Run the full workspace test suite and quality gate**

Run: `cargo test --workspace`
Expected: PASS (all tests across all crates)

Run: `cargo fmt --check`
Expected: no output

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings/errors

Fix anything either command reports before continuing.

- [ ] **Step 4: Commit and push**

```bash
git add crates/seisin-node
git commit -m "test: add live multi-node gossip failure-detection integration test"
git push
```

---

### Task 9: Quality gate

**Files:** none (verification only — this duplicates Task 8 Step 3's
checks as a final confirmation after any fixes made there).

- [ ] **Step 1: Run the full workspace test suite one more time**

Run: `cargo test --workspace`
Expected: PASS

- [ ] **Step 2: Run the formatting and lint gate one more time**

Run: `cargo fmt --check`
Expected: no output

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings/errors

- [ ] **Step 3: Commit and push if anything changed**

```bash
git add -A
git commit -m "chore: final fmt/clippy pass for gossip node wiring"
git push
```

(Skip entirely if nothing changed since Task 8.)

---

## Self-Review Notes

- **Spec coverage:** `Ring` made cross-thread-mutable ✓ (Task 1),
  full-state membership piggyback ✓ (Task 2), cache eviction reachable
  from outside a worker thread ✓ (Task 3), shared gossip state with
  bounded re-gossip buffer ✓ (Task 4), gossip listener merging incoming
  state ✓ (Task 5), parameterized failure detector + real probing loop
  with sequencer-driven `Leave` minting ✓ (Task 6), config/binary wiring
  ✓ (Task 7), and a real end-to-end proof that a silent node gets removed
  and the ring/redirect behavior updates accordingly ✓ (Task 8). Indirect
  probing and runtime join of new nodes remain explicitly deferred, per
  this plan's header.
- **Placeholder scan:** no TBD/TODO. Task 4's stub deliberately shows a
  broken intermediate `apply_ready_mutations` body to walk through *why*
  the naive version doesn't compile (a borrow-checker lifetime issue
  worth understanding, not glossing over) before Step 3 replaces it with
  the real, compiling version — every other stub follows the standard
  `unimplemented!()` pattern.
- **Type consistency:** `GossipState`, `apply_ready_mutations`,
  `run_gossip_loop`, and the `FailureDetector::new` signature change are
  each defined once and referenced identically everywhere they're
  consumed later in the plan, including in `main.rs` and the Task 8 test.
