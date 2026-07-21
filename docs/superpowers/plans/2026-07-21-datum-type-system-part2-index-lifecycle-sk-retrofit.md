# Datum Type System, Part 2 (revised): Automatic Index Maintenance & Op Lifecycle + sk Retrofit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the previously-shipped two-round-trip sk design with the mechanism the design doc's "Automatic Index Maintenance & Op Lifecycle" section actually calls for: index structures stay resident on their own native-home thread forever (never rebuilt from bytes, never collated onto a foreign thread), an op's writes are staged until every triggered index update succeeds, and field-level change detection happens automatically — a solution's op author never writes index-maintenance code by hand.

**Architecture:** A new, framework-level `IndexHandlerRegistry` (in `seisin-node`, parallel to `OpRegistry`) lets `seisin-types` register "how to apply an update to index kind X" without `seisin-node` ever knowing what "sk" means — keeping the framework type-agnostic. A new wire message pair (`Request::IndexUpdate`/`Response::IndexUpdateResult`) and `WorkerMessage::IndexUpdate` extend `worker.rs`'s existing non-blocking message-passing (same pattern as `Acquire`/`Recall`/`Release`). `OpContext` gains staged (not immediate) writes and a way to schedule index updates; `worker.rs`'s op completion path dispatches them and defers commit-or-fail until every reply is in. `seisin-types` gains a `TypedOpContext` whose `Drop` impl automatically diffs before/after field values and schedules whatever sk index updates the change requires — no manual `write_typed_datum` call.

**Tech Stack:** Rust, hand-rolled binary encoding (matching every prior part), no new external dependencies. `seisin-types` gains a new *regular* (not dev) dependency on `seisin-node`, to reference `IndexHandlerRegistry` — this is intentionally one-directional (`seisin-node` never depends on `seisin-types`), so the framework itself stays type-agnostic per Part 1's original commitment.

## Global Constraints

- 2-space indentation.
- Commit and push after every task.
- No serde or other external encoding dependency.
- This plan supersedes Datum Type System Part 2's old two-round-trip sk design entirely — `write_typed_datum`/`delete_typed_datum`/`WriteTypedResult`/`encode_write_result`/`decode_write_result`/`client.rs`'s `write_typed_datum_client`, and the old `insert_sk_entry`/`remove_sk_entry` (which operated on `OpContext` directly) are all removed and replaced, not kept alongside the new mechanism.
- Per the design doc's explicit "still best-effort, not a hard cross-op guarantee" note: an index update that succeeds while a *different* index update in the same op reports a violation is **not rolled back** in this plan — only the pk write and the reply to the client are gated on every index update succeeding. Reversing an already-applied, non-violating sibling update is a known, documented limitation (see Task 6), not something this plan solves.
- Parts 3 (rk) and 4 (tk) build on this same mechanism; Part 5 (relational/FK constraints, including the `fk_pending` tracking structure) is separate and later.

---

### Task 1: `IndexHandlerRegistry`

**Files:**
- Create: `crates/seisin-node/src/index_handler.rs`
- Modify: `crates/seisin-node/src/lib.rs`

**Interfaces:**
- Produces: `IndexHandler = Box<dyn Fn(Option<&[u8]>, &[u8]) -> (Vec<u8>, Option<String>) + Send + Sync>`, `IndexHandlerRegistry::new()`, `IndexHandlerRegistry::register(&mut self, kind, handler)`, `IndexHandlerRegistry::apply(&self, kind: &str, current: Option<&[u8]>, payload: &[u8]) -> Result<(Vec<u8>, Option<String>), String>`.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/seisin-node/src/index_handler.rs
#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn apply_dispatches_to_the_registered_handler() {
    let mut registry = IndexHandlerRegistry::new();
    registry.register(
      "uppercase",
      Box::new(|_current, payload| (payload.iter().map(u8::to_ascii_uppercase).collect(), None)),
    );
    let (result, violation) = registry.apply("uppercase", None, b"hello").unwrap();
    assert_eq!(result, b"HELLO");
    assert!(violation.is_none());
  }

  #[test]
  fn apply_on_an_unregistered_kind_is_an_error() {
    let registry = IndexHandlerRegistry::new();
    assert!(registry.apply("nope", None, b"x").is_err());
  }

  #[test]
  fn apply_can_report_a_violation() {
    let mut registry = IndexHandlerRegistry::new();
    registry.register(
      "reject_everything",
      Box::new(|_current, payload| (payload.to_vec(), Some("nope".to_string()))),
    );
    let (_, violation) = registry.apply("reject_everything", None, b"x").unwrap();
    assert_eq!(violation, Some("nope".to_string()));
  }

  #[test]
  fn apply_passes_current_through_to_the_handler() {
    let mut registry = IndexHandlerRegistry::new();
    registry.register(
      "echo_current",
      Box::new(|current, _payload| (current.unwrap_or(b"cold").to_vec(), None)),
    );
    let (cold, _) = registry.apply("echo_current", None, b"").unwrap();
    assert_eq!(cold, b"cold");
    let (warm, _) = registry.apply("echo_current", Some(b"warm"), b"").unwrap();
    assert_eq!(warm, b"warm");
  }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-node --lib index_handler::`
Expected: FAIL to compile — nothing in this file exists yet.

- [ ] **Step 3: Implement `IndexHandlerRegistry`**

```rust
// crates/seisin-node/src/index_handler.rs, above the tests module
//! The table of registered "how to apply an update to index kind X"
//! callbacks — parallel to `seisin_ops::registry::OpRegistry`, but for
//! index-kind logic rather than solution ops. Registration happens once
//! at startup; lookup happens once per incoming `IndexUpdate`. This
//! stays entirely inside `seisin-node` so the framework itself remains
//! agnostic of what "sk"/"rk"/"tk" even mean — a solution (or
//! `seisin-types`) registers the handler; this registry just dispatches
//! to it by name. See the design doc's "Automatic Index Maintenance &
//! Op Lifecycle" section.

use std::collections::HashMap;

/// Applies one update to an index's resident state. `current` is the
/// index's current resident bytes (`None` on a cold miss — nothing
/// resident yet), `payload` is the opaque update to apply. Returns the
/// new resident bytes and, if the update was rejected (e.g. a
/// uniqueness violation), a message describing why — a rejected update
/// leaves the caller's resident/stored state untouched (see
/// `worker.rs`'s `IndexUpdate` handling).
pub type IndexHandler =
  Box<dyn Fn(Option<&[u8]>, &[u8]) -> (Vec<u8>, Option<String>) + Send + Sync>;

#[derive(Default)]
pub struct IndexHandlerRegistry {
  handlers: HashMap<String, IndexHandler>,
}

impl IndexHandlerRegistry {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn register(&mut self, kind: impl Into<String>, handler: IndexHandler) {
    self.handlers.insert(kind.into(), handler);
  }

  /// Looks up `kind` and applies `payload` against `current`. Returns
  /// `Err` if `kind` was never registered.
  pub fn apply(
    &self,
    kind: &str,
    current: Option<&[u8]>,
    payload: &[u8],
  ) -> Result<(Vec<u8>, Option<String>), String> {
    let handler = self
      .handlers
      .get(kind)
      .ok_or_else(|| format!("no index handler registered for kind {kind:?}"))?;
    Ok(handler(current, payload))
  }
}
```

Add `pub mod index_handler;` to `crates/seisin-node/src/lib.rs`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-node --lib index_handler::`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-node/src/index_handler.rs crates/seisin-node/src/lib.rs
git commit -m "feat: add IndexHandlerRegistry for pluggable index-kind logic"
git push
```

---

### Task 2: Wire protocol — `Request::IndexUpdate` / `Response::IndexUpdateResult`

**Files:**
- Modify: `crates/seisin-protocol/src/lib.rs`

**Interfaces:**
- Produces: `Request::IndexUpdate { target: DatumId, op_id: DatumId, index_kind: String, payload: Vec<u8> }`, `Response::IndexUpdateResult { violation: Option<String> }`.

- [ ] **Step 1: Write the failing tests**

Add to `crates/seisin-protocol/src/lib.rs`'s `mod tests`:

```rust
  #[test]
  fn round_trips_index_update_request() {
    let req = Request::IndexUpdate {
      target: DatumId::new(),
      op_id: DatumId::new(),
      index_kind: "sk".to_string(),
      payload: vec![1, 2, 3],
    };
    let encoded = encode_request(&req);
    assert_eq!(decode_request(&encoded).unwrap(), req);
  }

  #[test]
  fn round_trips_index_update_request_with_empty_payload() {
    let req = Request::IndexUpdate {
      target: DatumId::new(),
      op_id: DatumId::new(),
      index_kind: "sk".to_string(),
      payload: vec![],
    };
    let encoded = encode_request(&req);
    assert_eq!(decode_request(&encoded).unwrap(), req);
  }

  #[test]
  fn round_trips_index_update_result_without_violation() {
    let resp = Response::IndexUpdateResult { violation: None };
    let encoded = encode_response(&resp);
    assert_eq!(decode_response(&encoded).unwrap(), resp);
  }

  #[test]
  fn round_trips_index_update_result_with_violation() {
    let resp = Response::IndexUpdateResult {
      violation: Some("duplicate value".to_string()),
    };
    let encoded = encode_response(&resp);
    assert_eq!(decode_response(&encoded).unwrap(), resp);
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-protocol --lib tests::round_trips_index_update`
Expected: FAIL to compile — `Request::IndexUpdate`/`Response::IndexUpdateResult` don't exist yet.

- [ ] **Step 3: Add the variants and their codecs**

In `crates/seisin-protocol/src/lib.rs`, add to `Request`:

```rust
  /// Node-to-node only: applies one update to the index datum this
  /// frame's peer-link envelope targets, on behalf of `op_id`. `payload`
  /// is opaque to the framework — interpreted by whichever
  /// `IndexHandler` is registered for `index_kind`. Never sent by a
  /// client.
  IndexUpdate {
    target: DatumId,
    op_id: DatumId,
    index_kind: String,
    payload: Vec<u8>,
  },
```

Add to `Response`:

```rust
  /// Reply to an `IndexUpdate` — `violation` is `Some(message)` if the
  /// update was rejected (e.g. a uniqueness constraint), in which case
  /// the index's resident/stored state was left untouched.
  IndexUpdateResult {
    violation: Option<String>,
  },
```

Add new opcode constants near the existing ones:

```rust
const OP_INDEX_UPDATE: u8 = 5;
```
```rust
const RESP_INDEX_UPDATE_RESULT: u8 = 6;
```

In `encode_request`, add a new match arm:

```rust
    Request::IndexUpdate {
      target,
      op_id,
      index_kind,
      payload,
    } => {
      buf.push(OP_INDEX_UPDATE);
      buf.extend_from_slice(&target.as_bytes());
      buf.extend_from_slice(&op_id.as_bytes());
      let kind_bytes = index_kind.as_bytes();
      buf.extend_from_slice(&(kind_bytes.len() as u32).to_le_bytes());
      buf.extend_from_slice(kind_bytes);
      buf.extend_from_slice(payload);
    }
```

In `decode_request`'s match, add:

```rust
    OP_INDEX_UPDATE => decode_index_update_request(buf),
```

Add the decode function, near `decode_op_request`:

```rust
fn decode_index_update_request(buf: &[u8]) -> Result<Request> {
  if buf.len() < 1 + ID_LEN + ID_LEN + 4 {
    bail!("index update request too short for its fixed fields");
  }
  let target = DatumId::from_bytes(buf[1..1 + ID_LEN].try_into().unwrap());
  let op_id_offset = 1 + ID_LEN;
  let op_id = DatumId::from_bytes(
    buf[op_id_offset..op_id_offset + ID_LEN]
      .try_into()
      .unwrap(),
  );
  let kind_len_offset = op_id_offset + ID_LEN;
  let kind_len =
    u32::from_le_bytes(buf[kind_len_offset..kind_len_offset + 4].try_into().unwrap()) as usize;
  let kind_offset = kind_len_offset + 4;
  if buf.len() < kind_offset + kind_len {
    bail!("index update request too short for its index_kind");
  }
  let index_kind = String::from_utf8(buf[kind_offset..kind_offset + kind_len].to_vec())
    .context("index_kind was not valid utf8")?;
  let payload = buf[kind_offset + kind_len..].to_vec();
  Ok(Request::IndexUpdate {
    target,
    op_id,
    index_kind,
    payload,
  })
}
```

In `encode_response`, add:

```rust
    Response::IndexUpdateResult { violation } => {
      buf.push(RESP_INDEX_UPDATE_RESULT);
      match violation {
        None => buf.push(0),
        Some(message) => {
          buf.push(1);
          buf.extend_from_slice(message.as_bytes());
        }
      }
    }
```

In `decode_response`'s match, add:

```rust
    RESP_INDEX_UPDATE_RESULT => {
      if buf.len() < 2 {
        bail!("index update result too short for its flag byte");
      }
      let violation = match buf[1] {
        0 => None,
        1 => Some(
          String::from_utf8(buf[2..].to_vec())
            .context("index update violation message was not valid utf8")?,
        ),
        flag => bail!("unknown index update result flag: {flag}"),
      };
      Ok(Response::IndexUpdateResult { violation })
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-protocol --lib`
Expected: PASS (all existing tests + 4 new).

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-protocol/src/lib.rs
git commit -m "feat: add Request::IndexUpdate / Response::IndexUpdateResult wire messages"
git push
```

---

### Task 3: `OpContext` staged writes and `schedule_index_update`

**Files:**
- Modify: `crates/seisin-ops/src/context.rs`

**Interfaces:**
- Produces: `PendingIndexUpdate { pub target: DatumId, pub index_kind: String, pub payload: Vec<u8> }`, `OpContext::schedule_index_update(&mut self, target, index_kind, payload)`, `OpContext::take_staged_writes(&mut self) -> Vec<(DatumId, Option<Vec<u8>>)>`, `OpContext::take_pending_index_updates(&mut self) -> Vec<PendingIndexUpdate>`.
- `OpContext::get`/`put`/`delete` keep their existing signatures — their *behavior* changes from "immediate" to "staged, but read-your-own-writes within the same op."

- [ ] **Step 1: Write the failing tests**

Add to `crates/seisin-ops/src/context.rs`'s `mod tests` (existing tests in this file must keep passing unchanged — they only exercise `get`/`put`/`delete`, whose observable behavior within a single `OpContext` instance doesn't change):

```rust
  #[test]
  fn a_staged_write_is_not_visible_on_the_underlying_cache_until_taken_and_committed() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let id = DatumId::new();
    {
      let mut ctx = OpContext::new(&mut cache);
      ctx.put(id, b"staged".to_vec());
      assert_eq!(ctx.get(id), Some(b"staged".to_vec())); // read-your-own-write
    }
    assert_eq!(cache.get(id), None); // never committed
  }

  #[test]
  fn take_staged_writes_returns_every_put_and_delete() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let put_id = DatumId::new();
    let delete_id = DatumId::new();
    ctx.put(put_id, b"hello".to_vec());
    ctx.delete(delete_id);
    let mut writes = ctx.take_staged_writes();
    writes.sort_by_key(|(id, _)| *id);
    let mut expected = vec![(put_id, Some(b"hello".to_vec())), (delete_id, None)];
    expected.sort_by_key(|(id, _)| *id);
    assert_eq!(writes, expected);
  }

  #[test]
  fn take_staged_writes_drains_so_a_second_call_is_empty() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    ctx.put(DatumId::new(), b"x".to_vec());
    assert_eq!(ctx.take_staged_writes().len(), 1);
    assert_eq!(ctx.take_staged_writes().len(), 0);
  }

  #[test]
  fn schedule_index_update_is_collected_by_take_pending_index_updates() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let target = DatumId::new();
    ctx.schedule_index_update(target, "sk", vec![9, 9]);
    let updates = ctx.take_pending_index_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].target, target);
    assert_eq!(updates[0].index_kind, "sk");
    assert_eq!(updates[0].payload, vec![9, 9]);
    assert_eq!(ctx.take_pending_index_updates().len(), 0);
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-ops --lib context::tests::a_staged_write`
Expected: FAIL to compile — `take_staged_writes`/`schedule_index_update`/`take_pending_index_updates` don't exist yet.

- [ ] **Step 3: Implement staged writes and index-update scheduling**

Replace the whole of `crates/seisin-ops/src/context.rs`'s non-test content:

```rust
//! `OpContext` is the interface a solution-defined operation function
//! uses to read/write the datums the framework has already collated
//! onto this thread. It operates at the byte level — the typed datum
//! layer from the design doc's "Datum Type System" notes is a separate
//! layer a solution's own generated code would sit on top of; the
//! framework itself stays type-agnostic.
//!
//! Writes are staged, not immediate: an op's `put`/`delete` calls are
//! held in memory (read-your-own-writes within the same op, via `get`)
//! until the framework (`worker.rs`) decides it's safe to commit them —
//! immediately, for an op that scheduled no index updates, or once
//! every scheduled index update has succeeded. See the design doc's
//! "Automatic Index Maintenance & Op Lifecycle" section.

use std::collections::HashMap;

use seisin_core::cache::Cache;
use seisin_core::datum::DatumId;

/// One index update an op wants applied, once its own effects are known
/// to be safe to commit. Framework-internal: solution op authors never
/// construct this directly — a typed accessor layer (`seisin-types`)
/// calls `OpContext::schedule_index_update` on their behalf.
pub struct PendingIndexUpdate {
  pub target: DatumId,
  pub index_kind: String,
  pub payload: Vec<u8>,
}

pub struct OpContext<'a> {
  cache: &'a mut Cache,
  staged: HashMap<DatumId, Option<Vec<u8>>>,
  pending_index_updates: Vec<PendingIndexUpdate>,
}

impl<'a> OpContext<'a> {
  pub fn new(cache: &'a mut Cache) -> Self {
    Self {
      cache,
      staged: HashMap::new(),
      pending_index_updates: Vec::new(),
    }
  }

  /// Reads `id`'s current value. A value staged by an earlier `put`/
  /// `delete` in this same op is visible immediately (read-your-own-
  /// writes), even though nothing is actually committed to the
  /// underlying cache/storage until the framework says so.
  pub fn get(&mut self, id: DatumId) -> Option<Vec<u8>> {
    if let Some(staged) = self.staged.get(&id) {
      return staged.clone();
    }
    self.cache.get(id)
  }

  pub fn put(&mut self, id: DatumId, content: Vec<u8>) {
    self.staged.insert(id, Some(content));
  }

  pub fn delete(&mut self, id: DatumId) {
    self.staged.insert(id, None);
  }

  /// Schedules an index update to be dispatched, once this op's
  /// business logic finishes, to whichever thread natively owns
  /// `target`. Framework-internal.
  pub fn schedule_index_update(
    &mut self,
    target: DatumId,
    index_kind: impl Into<String>,
    payload: Vec<u8>,
  ) {
    self.pending_index_updates.push(PendingIndexUpdate {
      target,
      index_kind: index_kind.into(),
      payload,
    });
  }

  /// Drains every staged write from this op. Framework-internal — the
  /// caller (`worker.rs`) is responsible for actually committing these
  /// to the underlying cache once it's known safe to do so.
  pub fn take_staged_writes(&mut self) -> Vec<(DatumId, Option<Vec<u8>>)> {
    std::mem::take(&mut self.staged).into_iter().collect()
  }

  /// Drains every index update this op scheduled. Framework-internal.
  pub fn take_pending_index_updates(&mut self) -> Vec<PendingIndexUpdate> {
    std::mem::take(&mut self.pending_index_updates)
  }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-ops --lib`
Expected: PASS (all existing tests + 4 new — existing tests are unchanged and still pass, since `get`/`put`/`delete`'s observable behavior *within one `OpContext` instance* is unchanged).

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-ops/src/context.rs
git commit -m "feat: stage OpContext writes and add index-update scheduling"
git push
```

---

### Task 4: `WorkerHandle` — resident index cache and `IndexUpdate` handling

**Files:**
- Modify: `crates/seisin-node/src/worker.rs`

**Interfaces:**
- Consumes: `IndexHandlerRegistry` (Task 1), `Request::IndexUpdate`/`Response::IndexUpdateResult` (Task 2).
- Produces: `WorkerMessage::IndexUpdate { target, op_id, index_kind, payload, reply: IndexUpdateReply }`, `WorkerMessage::IndexUpdateReplied { op_id, target, violation: Option<String> }`, `IndexUpdateReply` (mirrors `AcquireReply`/`RecallReply`), `WorkerHandle::spawn` gains an `index_handlers: Arc<IndexHandlerRegistry>` parameter.

This task only adds the *receiving* side (a thread that owns an index datum, applying updates to it) and the message/reply plumbing — dispatching updates from the *originating* op's side is Task 6.

- [ ] **Step 1: Add the `IndexUpdateReply` type and the two new `WorkerMessage` variants**

In `crates/seisin-node/src/worker.rs`, add near `AcquireReply`/`RecallReply`:

```rust
/// Where an `IndexUpdate`'s eventual reply should be delivered — same
/// shape as `AcquireReply`/`RecallReply`, for the same reason.
pub(crate) enum IndexUpdateReply {
  Local(Sender<WorkerMessage>),
  Remote(Arc<PeerLink>, u64),
}

impl IndexUpdateReply {
  fn respond(self, op_id: DatumId, target: DatumId, violation: Option<String>) {
    match self {
      IndexUpdateReply::Local(inbox) => {
        let _ = inbox.send(WorkerMessage::IndexUpdateReplied {
          op_id,
          target,
          violation,
        });
      }
      IndexUpdateReply::Remote(link, correlation_id) => {
        link.respond(
          correlation_id,
          seisin_protocol::Response::IndexUpdateResult { violation },
        );
      }
    }
  }
}
```

Add to `WorkerMessage`, after `EvictNonNative`/`ReleaseLocksHeldBy` (wherever the enum currently ends):

```rust
  /// Sent to whichever thread natively owns `target`, applying one
  /// update to that index's resident state. See the design doc's
  /// "Automatic Index Maintenance & Op Lifecycle" section.
  IndexUpdate {
    target: DatumId,
    op_id: DatumId,
    index_kind: String,
    payload: Vec<u8>,
    reply: IndexUpdateReply,
  },
  /// Posted into the originating op's own thread inbox once a
  /// dispatched `IndexUpdate` gets a reply.
  IndexUpdateReplied {
    op_id: DatumId,
    target: DatumId,
    violation: Option<String>,
  },
```

- [ ] **Step 2: Give `WorkerHandle::spawn` a resident index cache and the registry**

Change `WorkerHandle::spawn`'s signature from:

```rust
  pub(crate) fn spawn(
    self_thread_id: ThreadId,
    receiver: Receiver<WorkerMessage>,
    sender: Sender<WorkerMessage>,
    peers: Arc<Vec<Sender<WorkerMessage>>>,
    store: Arc<dyn Store>,
    ops: Arc<OpRegistry>,
    ring: Arc<RwLock<Ring>>,
    self_node_id: NodeId,
    peer_links: Arc<StdMutex<PeerLinkRegistry>>,
  ) -> Self {
```

to:

```rust
  #[allow(clippy::too_many_arguments)]
  pub(crate) fn spawn(
    self_thread_id: ThreadId,
    receiver: Receiver<WorkerMessage>,
    sender: Sender<WorkerMessage>,
    peers: Arc<Vec<Sender<WorkerMessage>>>,
    store: Arc<dyn Store>,
    ops: Arc<OpRegistry>,
    ring: Arc<RwLock<Ring>>,
    self_node_id: NodeId,
    peer_links: Arc<StdMutex<PeerLinkRegistry>>,
    index_handlers: Arc<crate::index_handler::IndexHandlerRegistry>,
  ) -> Self {
```

(There's already an `#[allow(clippy::too_many_arguments)]` above the *old* signature — keep it; the snippet above just shows the new parameter added.)

Inside the thread body, add a new local next to `native_locks`/`op_records`:

```rust
      let mut index_cache: HashMap<DatumId, Vec<u8>> = HashMap::new();
```

- [ ] **Step 3: Handle incoming `WorkerMessage::IndexUpdate`**

Add a new match arm in the `for message in receiver` loop, alongside the others:

```rust
          WorkerMessage::IndexUpdate {
            target,
            op_id,
            index_kind,
            payload,
            reply,
          } => {
            let current = match index_cache.get(&target) {
              Some(bytes) => Some(bytes.clone()),
              None => cache.get(target),
            };
            match index_handlers.apply(&index_kind, current.as_deref(), &payload) {
              Ok((new_bytes, violation)) => {
                if violation.is_none() {
                  index_cache.insert(target, new_bytes.clone());
                  cache.put(target, new_bytes);
                }
                reply.respond(op_id, target, violation);
              }
              Err(message) => {
                reply.respond(op_id, target, Some(message));
              }
            }
          }
```

`WorkerMessage::IndexUpdateReplied` is handled in Task 6 (it belongs to the *originating* op's side, which Task 6 builds) — for now, add a no-op arm so the match stays exhaustive:

```rust
          WorkerMessage::IndexUpdateReplied { .. } => {
            // Wired up properly in Task 6 (the op-lifecycle rewrite);
            // for now there's no op-record state to update yet.
          }
```

- [ ] **Step 4: Update `WorkerHandle::spawn`'s own call site inside `worker.rs`'s tests**

`worker.rs`'s `#[cfg(test)] mod tests` constructs `WorkerHandle`/pools directly in a few places. Run:

Run: `cargo build -p seisin-node --tests 2>&1 | grep "this function takes"`
Expected: a handful of "this function takes 10 arguments but 9 were supplied" errors pointing at every call site inside `worker.rs`'s own test module — add `Arc::new(crate::index_handler::IndexHandlerRegistry::new())` as the new last argument to each one reported.

- [ ] **Step 5: Run the full `seisin-node` lib test suite**

Run: `cargo test -p seisin-node --lib`
Expected: PASS — same test count as before this task (this task only adds plumbing; nothing yet exercises `IndexUpdate` end-to-end, since nothing calls `schedule_index_update` yet).

- [ ] **Step 6: Commit and push**

```bash
git add crates/seisin-node/src/worker.rs
git commit -m "feat: add resident index cache and IndexUpdate handling to WorkerHandle"
git push
```

---

### Task 5: Cross-node wiring and updating every `WorkerPool::spawn` call site

**Files:**
- Modify: `crates/seisin-node/src/pool.rs`
- Modify: `crates/seisin-node/src/main.rs`
- Modify: `crates/seisin-node/src/gossip_state.rs`
- Modify: `crates/seisin-node/tests/integration_cross_node_wound_wait.rs`
- Modify: `crates/seisin-node/tests/integration_multi_node_routing.rs`
- Modify: `crates/seisin-node/tests/integration_gossip_failure_detection.rs`
- Modify: `crates/seisin-node/tests/integration_crash_during_collation.rs`
- Modify: `crates/seisin-node/tests/integration_proactive_lock_release.rs`
- Modify: `crates/seisin-node/tests/integration_op_collation.rs`
- Modify: `crates/seisin-node/tests/integration_wire_protocol.rs`
- Modify: `crates/seisin-node/tests/integration_wound_wait.rs`
- Modify: `crates/seisin-node/tests/integration_cross_node_collation.rs`

**Interfaces:**
- Consumes: `IndexHandlerRegistry` (Task 1), `Request::IndexUpdate` (Task 2), `WorkerHandle::spawn`'s new parameter (Task 4).
- Produces: `WorkerPool::spawn` gains an `index_handlers: Arc<IndexHandlerRegistry>` parameter (last position); `pool.rs`'s cross-node `on_request` dispatch handles `Request::IndexUpdate`.

- [ ] **Step 1: Thread `index_handlers` through `WorkerPool::spawn` and handle `Request::IndexUpdate` cross-node**

Change `WorkerPool::spawn`'s signature in `crates/seisin-node/src/pool.rs` from:

```rust
  pub fn spawn(
    store: Arc<dyn Store>,
    thread_count: u32,
    ops: Arc<OpRegistry>,
    ring: Arc<RwLock<Ring>>,
    self_node_id: NodeId,
    peer_link_listener: TcpListener,
    peer_link_address_book: Arc<HashMap<NodeId, String>>,
  ) -> Self {
```

to:

```rust
  #[allow(clippy::too_many_arguments)]
  pub fn spawn(
    store: Arc<dyn Store>,
    thread_count: u32,
    ops: Arc<OpRegistry>,
    ring: Arc<RwLock<Ring>>,
    self_node_id: NodeId,
    peer_link_listener: TcpListener,
    peer_link_address_book: Arc<HashMap<NodeId, String>>,
    index_handlers: Arc<crate::index_handler::IndexHandlerRegistry>,
  ) -> Self {
```

(The existing signature already carries `#[allow(clippy::too_many_arguments)]` — keep it.)

In the `on_request` closure, add a new match arm for `Request::IndexUpdate` (alongside `Acquire`/`Recall`/`Release`):

```rust
        seisin_protocol::Request::IndexUpdate {
          target,
          op_id,
          index_kind,
          payload,
        } => WorkerMessage::IndexUpdate {
          target,
          op_id,
          index_kind,
          payload,
          reply: crate::worker::IndexUpdateReply::Remote(Arc::clone(&link), correlation_id),
        },
```

Change the `WorkerHandle::spawn(...)` call inside `.map(|(idx, receiver)| ...)` from:

```rust
    let handles = receivers
      .into_iter()
      .enumerate()
      .map(|(idx, receiver)| {
        WorkerHandle::spawn(
          ThreadId(idx as u32),
          receiver,
          peers[idx].clone(),
          Arc::clone(&peers),
          Arc::clone(&store),
          Arc::clone(&ops),
          Arc::clone(&ring),
          self_node_id,
          Arc::clone(&peer_links),
        )
      })
      .collect();
```

to:

```rust
    let handles = receivers
      .into_iter()
      .enumerate()
      .map(|(idx, receiver)| {
        WorkerHandle::spawn(
          ThreadId(idx as u32),
          receiver,
          peers[idx].clone(),
          Arc::clone(&peers),
          Arc::clone(&store),
          Arc::clone(&ops),
          Arc::clone(&ring),
          self_node_id,
          Arc::clone(&peer_links),
          Arc::clone(&index_handlers),
        )
      })
      .collect();
```

(`Arc::clone(&index_handlers)` works directly off the `index_handlers` parameter `WorkerPool::spawn` now takes — no separate binding needed, same as `store`/`ops`/`ring` above.)

- [ ] **Step 2: Update `main.rs`'s real `WorkerPool::spawn` call**

In `crates/seisin-node/src/main.rs`, change:

```rust
  let pool = Arc::new(WorkerPool::spawn(
    store,
    self_thread_count,
    Arc::new(seisin_ops::registry::OpRegistry::new()),
    Arc::clone(&ring),
    self_node_id,
    peer_link_listener,
    peer_link_address_book,
  ));
```

to:

```rust
  let pool = Arc::new(WorkerPool::spawn(
    store,
    self_thread_count,
    Arc::new(seisin_ops::registry::OpRegistry::new()),
    Arc::clone(&ring),
    self_node_id,
    peer_link_listener,
    peer_link_address_book,
    Arc::new(seisin_node::index_handler::IndexHandlerRegistry::new()),
  ));
```

- [ ] **Step 3: Update every remaining call site**

Run: `cargo build --workspace --tests 2>&1 | grep -B2 "this function takes"`

Expected: one error per remaining `WorkerPool::spawn` call site missing the new argument — `crates/seisin-node/src/gossip_state.rs`'s own test module, and each of the nine `crates/seisin-node/tests/integration_*.rs` files listed above. For each one reported, add `Arc::new(seisin_node::index_handler::IndexHandlerRegistry::new()),` (or, from inside `seisin-node`'s own `gossip_state.rs` test module, `Arc::new(crate::index_handler::IndexHandlerRegistry::new()),`) as the new last argument to that file's `WorkerPool::spawn(...)` call, matching the pattern in Step 2 above exactly. Repeat until the build is clean.

- [ ] **Step 4: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS — every existing test across every crate, since an empty `IndexHandlerRegistry` behaves identically to no registry at all for any op that never calls `schedule_index_update` (which is every op in these tests — none of them use the new typed layer yet).

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-node/src/pool.rs crates/seisin-node/src/main.rs crates/seisin-node/src/gossip_state.rs crates/seisin-node/tests/integration_cross_node_wound_wait.rs crates/seisin-node/tests/integration_multi_node_routing.rs crates/seisin-node/tests/integration_gossip_failure_detection.rs crates/seisin-node/tests/integration_crash_during_collation.rs crates/seisin-node/tests/integration_proactive_lock_release.rs crates/seisin-node/tests/integration_op_collation.rs crates/seisin-node/tests/integration_wire_protocol.rs crates/seisin-node/tests/integration_wound_wait.rs crates/seisin-node/tests/integration_cross_node_collation.rs
git commit -m "feat: thread IndexHandlerRegistry through WorkerPool and handle IndexUpdate cross-node"
git push
```

---

### Task 6: Op lifecycle — dispatch index updates, commit or fail

**Files:**
- Modify: `crates/seisin-node/src/worker.rs`

**Interfaces:**
- Consumes: `OpContext::take_staged_writes`/`take_pending_index_updates` (Task 3), `IndexUpdateReply`/`WorkerMessage::IndexUpdate`/`IndexUpdateReplied` (Task 4), `Request::IndexUpdate` (Task 2).
- Produces: `OpRecord` gains an `index_update_state: Option<IndexUpdateState>` field; `try_run_if_ready` dispatches pending index updates instead of always committing immediately; `WorkerMessage::IndexUpdateReplied` actually drives commit-or-fail.

- [ ] **Step 1: Write the failing tests**

Add to `crates/seisin-node/src/worker.rs`'s `mod tests` (these exercise the lifecycle directly through a real `WorkerPool`/`WorkerHandle`, registering a real `IndexHandlerRegistry` entry so the dispatch has somewhere real to go):

```rust
  #[test]
  fn an_op_with_no_index_updates_commits_immediately_as_before() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let mut ops = OpRegistry::new();
    ops.register(
      "touch",
      Box::new(|ctx: &mut OpContext, ids, _payload| {
        ctx.put(ids[0], b"touched".to_vec());
        vec![]
      }),
    );
    let (listener, address_book) = no_peers();
    let pool = WorkerPool::spawn(
      Arc::new(InMemoryStore::new()),
      1,
      Arc::new(ops),
      ring,
      NodeId(1),
      listener,
      address_book,
      Arc::new(crate::index_handler::IndexHandlerRegistry::new()),
    );
    let id = DatumId::new();
    let result = pool.run_op(DatumId::new(), "touch".to_string(), vec![id], vec![]);
    assert_eq!(result, Ok(vec![]));
  }

  #[test]
  fn an_op_that_schedules_an_index_update_waits_for_it_before_committing() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let index_target = DatumId::new();
    let mut ops = OpRegistry::new();
    ops.register(
      "touch_with_index",
      Box::new(move |ctx: &mut OpContext, ids, _payload| {
        ctx.put(ids[0], b"touched".to_vec());
        ctx.schedule_index_update(index_target, "always_ok", vec![]);
        vec![]
      }),
    );
    let mut index_handlers = crate::index_handler::IndexHandlerRegistry::new();
    index_handlers.register(
      "always_ok",
      Box::new(|_current, payload| (payload.to_vec(), None)),
    );
    let (listener, address_book) = no_peers();
    let pool = WorkerPool::spawn(
      Arc::new(InMemoryStore::new()),
      1,
      Arc::new(ops),
      ring,
      NodeId(1),
      listener,
      address_book,
      Arc::new(index_handlers),
    );
    let id = DatumId::new();
    let result = pool.run_op(DatumId::new(), "touch_with_index".to_string(), vec![id], vec![]);
    assert_eq!(result, Ok(vec![]));
  }

  #[test]
  fn a_violation_from_a_scheduled_index_update_fails_the_whole_op_with_no_write() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let index_target = DatumId::new();
    let mut ops = OpRegistry::new();
    ops.register(
      "touch_with_rejected_index",
      Box::new(move |ctx: &mut OpContext, ids, _payload| {
        ctx.put(ids[0], b"should not be written".to_vec());
        ctx.schedule_index_update(index_target, "always_reject", vec![]);
        vec![]
      }),
    );
    let mut index_handlers = crate::index_handler::IndexHandlerRegistry::new();
    index_handlers.register(
      "always_reject",
      Box::new(|_current, payload| (payload.to_vec(), Some("rejected".to_string()))),
    );
    let (listener, address_book) = no_peers();
    let pool = WorkerPool::spawn(
      Arc::new(InMemoryStore::new()),
      1,
      Arc::new(ops),
      ring,
      NodeId(1),
      listener,
      address_book,
      Arc::new(index_handlers),
    );
    let id = DatumId::new();
    let result = pool.run_op(
      DatumId::new(),
      "touch_with_rejected_index".to_string(),
      vec![id],
      vec![],
    );
    assert_eq!(result, Err("rejected".to_string()));

    // Confirm the pk write really never landed.
    let mut ops2 = OpRegistry::new();
    ops2.register(
      "read",
      Box::new(|ctx: &mut OpContext, ids, _payload| ctx.get(ids[0]).unwrap_or_default()),
    );
    // (A fresh pool sharing the same store would be needed to observe
    // this from the outside in a real integration test — Task 8 covers
    // that end-to-end. This unit test's assertion above, that the op's
    // own result is `Err`, is what this task is responsible for.)
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-node --lib worker::tests::an_op_that_schedules_an_index_update`
Expected: FAIL — the op currently ignores `take_pending_index_updates` entirely (ignores anything scheduled) and always commits/replies immediately, so this test currently hangs or the pending update is silently dropped without ever affecting the reply. If it hangs rather than fails cleanly, that's expected too — `try_run_if_ready` doesn't dispatch anything yet, so nothing will ever call back; interrupt and proceed to Step 3.

- [ ] **Step 3: Rewrite `try_run_if_ready` and add index-update dispatch/reply handling**

Change `OpRecord`'s definition from:

```rust
struct OpRecord {
  op_name: String,
  payload: Vec<u8>,
  /// The caller's original datum_ids, in the order it specified them —
  /// preserved separately from `acquired` (below) because grants can
  /// arrive in a different order (e.g. a same-thread self-grant beats a
  /// cross-thread one), and an op function indexes its `ids` parameter
  /// positionally, so invocation must use the caller's order, not
  /// arrival order.
  datum_ids: Vec<DatumId>,
  still_needed: Vec<DatumId>,
  acquired: Vec<DatumId>,
  reply: Sender<Result<Vec<u8>, String>>,
}
```

to:

```rust
struct OpRecord {
  op_name: String,
  payload: Vec<u8>,
  /// The caller's original datum_ids, in the order it specified them —
  /// preserved separately from `acquired` (below) because grants can
  /// arrive in a different order (e.g. a same-thread self-grant beats a
  /// cross-thread one), and an op function indexes its `ids` parameter
  /// positionally, so invocation must use the caller's order, not
  /// arrival order.
  datum_ids: Vec<DatumId>,
  still_needed: Vec<DatumId>,
  acquired: Vec<DatumId>,
  reply: Sender<Result<Vec<u8>, String>>,
  /// `None` until the op's business logic has run and scheduled at
  /// least one index update; `Some` while waiting on those updates'
  /// replies. See the design doc's "Automatic Index Maintenance & Op
  /// Lifecycle" section.
  index_update_state: Option<IndexUpdateState>,
}

struct IndexUpdateState {
  staged_writes: Vec<(DatumId, Option<Vec<u8>>)>,
  op_result: Vec<u8>,
  pending: usize,
  /// The first violation seen, if any — an op can have scheduled
  /// several index updates; the whole op fails if *any* of them
  /// reports one, but only one message is kept for the reply.
  violation: Option<String>,
}
```

Update the `WorkerMessage::RunOp` handler's `OpRecord` construction (in `WorkerHandle::spawn`'s thread body) to add the new field:

```rust
            op_records.insert(
              op_id,
              OpRecord {
                op_name,
                payload,
                datum_ids: datum_ids.clone(),
                still_needed: datum_ids.clone(),
                acquired: Vec::new(),
                reply,
                index_update_state: None,
              },
            );
```

Replace `try_run_if_ready`'s entire body:

```rust
/// If `op_id`'s record has nothing left to acquire and hasn't already
/// entered its index-update phase, runs it. An op that scheduled no
/// index updates commits and replies immediately, exactly as before;
/// one that scheduled at least one dispatches them and waits — see
/// `WorkerMessage::IndexUpdateReplied` for the commit-or-fail step.
#[allow(clippy::too_many_arguments)]
fn try_run_if_ready(
  op_id: DatumId,
  op_records: &mut HashMap<DatumId, OpRecord>,
  cache: &mut Cache,
  ops: &OpRegistry,
  ring: &Arc<RwLock<Ring>>,
  peers: &Arc<Vec<Sender<WorkerMessage>>>,
  peer_links: &Arc<StdMutex<PeerLinkRegistry>>,
  self_node_id: NodeId,
  join_sender: &Sender<WorkerMessage>,
) {
  let ready = op_records.get(&op_id).is_some_and(|record| {
    record.still_needed.is_empty() && record.index_update_state.is_none()
  });
  if !ready {
    return;
  }
  let record = op_records.get_mut(&op_id).unwrap();
  let mut ctx = OpContext::new(cache);
  let result = ops.invoke(
    &record.op_name,
    &mut ctx,
    &record.datum_ids,
    &record.payload,
  );
  let staged_writes = ctx.take_staged_writes();
  let pending_index_updates = ctx.take_pending_index_updates();

  let op_result = match result {
    Err(message) => {
      let record = op_records.remove(&op_id).unwrap();
      let _ = record.reply.send(Err(message));
      release_datums(
        record.acquired,
        cache,
        ring,
        peers,
        peer_links,
        self_node_id,
      );
      return;
    }
    Ok(op_result) => op_result,
  };

  if pending_index_updates.is_empty() {
    for (id, content) in staged_writes {
      match content {
        Some(bytes) => cache.put(id, bytes),
        None => cache.delete(id),
      }
    }
    let record = op_records.remove(&op_id).unwrap();
    let _ = record.reply.send(Ok(op_result));
    release_datums(
      record.acquired,
      cache,
      ring,
      peers,
      peer_links,
      self_node_id,
    );
    return;
  }

  let pending = pending_index_updates.len();
  record.index_update_state = Some(IndexUpdateState {
    staged_writes,
    op_result,
    pending,
    violation: None,
  });
  for update in pending_index_updates {
    dispatch_index_update(
      ring,
      peers,
      peer_links,
      self_node_id,
      op_id,
      update.target,
      update.index_kind,
      update.payload,
      join_sender.clone(),
    );
  }
}

/// Sends an `IndexUpdate` for `target` on behalf of `op_id` to
/// whichever thread `ring.native()` currently names — locally or
/// cross-node, mirroring `send_acquire`'s same local/remote split. A
/// missing peer-link, or a call that fails after connecting, is
/// reported back as a violation (failing the whole op) rather than
/// assumed successful — there's no retry here (unlike bounded acquire
/// retry), a deliberate v1 simplification.
#[allow(clippy::too_many_arguments)]
fn dispatch_index_update(
  ring: &Arc<RwLock<Ring>>,
  peers: &Arc<Vec<Sender<WorkerMessage>>>,
  peer_links: &Arc<StdMutex<PeerLinkRegistry>>,
  self_node_id: NodeId,
  op_id: DatumId,
  target: DatumId,
  index_kind: String,
  payload: Vec<u8>,
  requester_inbox: Sender<WorkerMessage>,
) {
  let (native_node, native_thread) = ring.read().unwrap().native(target);
  if native_node == self_node_id {
    let _ = peers[native_thread.0 as usize].send(WorkerMessage::IndexUpdate {
      target,
      op_id,
      index_kind,
      payload,
      reply: IndexUpdateReply::Local(requester_inbox),
    });
  } else {
    match peer_links.lock().unwrap().get(native_node) {
      Some(link) => {
        link.call(
          native_thread,
          seisin_protocol::Request::IndexUpdate {
            target,
            op_id,
            index_kind,
            payload,
          },
          Box::new(move |response| {
            let violation = match response {
              seisin_protocol::Response::IndexUpdateResult { violation } => violation,
              other => Some(format!(
                "unexpected response applying index update to {target:?}: {other:?}"
              )),
            };
            let _ = requester_inbox.send(WorkerMessage::IndexUpdateReplied {
              op_id,
              target,
              violation,
            });
          }),
        );
      }
      None => {
        let _ = requester_inbox.send(WorkerMessage::IndexUpdateReplied {
          op_id,
          target,
          violation: Some(format!("no peer-link connection to node {native_node:?}")),
        });
      }
    }
  }
}
```

Replace the earlier placeholder `WorkerMessage::IndexUpdateReplied { .. } => { ... }` arm (added in Task 4 Step 3) with the real handling:

```rust
          WorkerMessage::IndexUpdateReplied {
            op_id,
            target,
            violation,
          } => {
            let _ = target;
            if let Some(record) = op_records.get_mut(&op_id) {
              if let Some(state) = &mut record.index_update_state {
                state.pending -= 1;
                if violation.is_some() && state.violation.is_none() {
                  state.violation = violation;
                }
                if state.pending == 0 {
                  let record = op_records.remove(&op_id).unwrap();
                  let state = record.index_update_state.unwrap();
                  if let Some(message) = state.violation {
                    let _ = record.reply.send(Err(message));
                  } else {
                    for (id, content) in state.staged_writes {
                      match content {
                        Some(bytes) => cache.put(id, bytes),
                        None => cache.delete(id),
                      }
                    }
                    let _ = record.reply.send(Ok(state.op_result));
                  }
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
            }
          }
```

`try_run_if_ready` has exactly two call sites in `crates/seisin-node/src/worker.rs` (inside the `RunOp` handler and the `AcquireGranted` handler), and both currently end identically:

```rust
            try_run_if_ready(
              op_id,
              &mut op_records,
              &mut cache,
              &ops,
              &ring,
              &peers,
              &peer_links,
              self_node_id,
            );
```

Change both occurrences to add `&join_sender` as the new trailing argument:

```rust
            try_run_if_ready(
              op_id,
              &mut op_records,
              &mut cache,
              &ops,
              &ring,
              &peers,
              &peer_links,
              self_node_id,
              &join_sender,
            );
```

(`join_sender` is already in scope at both call sites — it's the same `Sender<WorkerMessage>` clone the thread's own message-sending helpers already use.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-node --lib worker::`
Expected: PASS — including the three new tests and every pre-existing `worker::tests::*` test unchanged.

- [ ] **Step 5: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS.

- [ ] **Step 6: Commit and push**

```bash
git add crates/seisin-node/src/worker.rs
git commit -m "feat: dispatch scheduled index updates and gate op commit on their replies"
git push
```

---

### Task 7: `seisin-types` — sk as an `IndexHandler`, retiring the old sk write path

**Files:**
- Modify: `crates/seisin-types/Cargo.toml`
- Modify: `crates/seisin-types/src/sk_index.rs`
- Delete: `crates/seisin-types/src/typed_write.rs`
- Delete: `crates/seisin-types/src/client.rs`
- Delete: `crates/seisin-types/tests/integration_typed_write_client.rs`
- Modify: `crates/seisin-types/src/lib.rs`

**Interfaces:**
- Consumes: `IndexHandlerRegistry` (Task 1, `seisin-node`).
- Produces: `SkIndexOp { Insert { pk_id, unique_conflict_op: Option<String> }, Remove { pk_id } }`, `encode_sk_index_op`/`decode_sk_index_op`, `apply_sk_index_update(current: Option<&[u8]>, payload: &[u8]) -> (Vec<u8>, Option<String>)`, `register_sk_index_handler(registry: &mut IndexHandlerRegistry)`.
- Removes: `write_typed_datum`/`delete_typed_datum`/`WriteTypedResult`/`encode_write_result`/`decode_write_result` (superseded — see this plan's Global Constraints), `write_typed_datum_client`, the old `insert_sk_entry`/`remove_sk_entry` (which operated on `OpContext` directly — replaced by the byte-level `apply_sk_index_update`), and `UniquenessViolation` (superseded by the plain `String` violation message `IndexHandler` already uses).

- [ ] **Step 1: Add the `seisin-node` dependency and drop dependencies the deleted files needed**

`crates/seisin-types/Cargo.toml` becomes:

```toml
[package]
name = "seisin-types"
version = "0.1.0"
edition = "2021"

[dependencies]
seisin-core = { path = "../seisin-core" }
seisin-node = { path = "../seisin-node" }
anyhow = "1"

[dev-dependencies]
seisin-ring = { path = "../seisin-ring" }
```

(`seisin-ops` is now only needed by `typed_context.rs`, added in Task 8 — leave it out of `[dependencies]` here and add it back then, to avoid an unused-dependency state in between; if `cargo build` in this task complains about needing `seisin-ops` for something Task 7 itself still uses, add it back now instead — check before assuming. `seisin-client`/`seisin-protocol` were only needed by the now-deleted `client.rs`; drop them. `seisin-node` is now a *regular* dependency, not dev-only — its dev-only entry from Part 2's Task 6 is removed since it's promoted here.)

- [ ] **Step 2: Delete the superseded files**

```bash
git rm crates/seisin-types/src/typed_write.rs crates/seisin-types/src/client.rs crates/seisin-types/tests/integration_typed_write_client.rs
```

Remove `pub mod typed_write;` and `pub mod client;` from `crates/seisin-types/src/lib.rs`.

- [ ] **Step 3: Write the failing tests for the new sk `IndexHandler`**

Replace `crates/seisin-types/src/sk_index.rs`'s existing `insert_sk_entry`/`remove_sk_entry`/`UniquenessViolation`-related tests (the four tests added in Part 2's Task 4: `insert_then_remove_round_trips_through_a_real_cache`, `a_second_insert_of_a_different_pk_id_is_flagged_as_a_violation`, `inserting_the_same_pk_id_twice_is_not_a_violation`, `remove_on_a_missing_key_is_a_no_op`) with:

```rust
  #[test]
  fn encode_decode_round_trips_an_insert_op() {
    let op = SkIndexOp::Insert {
      pk_id: DatumId::new(),
      unique_conflict_op: Some("resolve".to_string()),
    };
    assert_eq!(decode_sk_index_op(&encode_sk_index_op(&op)).unwrap(), op);
  }

  #[test]
  fn encode_decode_round_trips_an_insert_op_without_a_conflict_op() {
    let op = SkIndexOp::Insert {
      pk_id: DatumId::new(),
      unique_conflict_op: None,
    };
    assert_eq!(decode_sk_index_op(&encode_sk_index_op(&op)).unwrap(), op);
  }

  #[test]
  fn encode_decode_round_trips_a_remove_op() {
    let op = SkIndexOp::Remove { pk_id: DatumId::new() };
    assert_eq!(decode_sk_index_op(&encode_sk_index_op(&op)).unwrap(), op);
  }

  #[test]
  fn apply_insert_on_a_cold_target_creates_a_single_entry_list() {
    let pk_id = DatumId::new();
    let payload = encode_sk_index_op(&SkIndexOp::Insert {
      pk_id,
      unique_conflict_op: None,
    });
    let (new_bytes, violation) = apply_sk_index_update(None, &payload);
    assert!(violation.is_none());
    let entries = seisin_core::sk::decode_sk_entries(&new_bytes).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, pk_id);
  }

  #[test]
  fn apply_remove_on_an_existing_entry_removes_it() {
    let pk_id = DatumId::new();
    let insert_payload = encode_sk_index_op(&SkIndexOp::Insert {
      pk_id,
      unique_conflict_op: None,
    });
    let (after_insert, _) = apply_sk_index_update(None, &insert_payload);
    let remove_payload = encode_sk_index_op(&SkIndexOp::Remove { pk_id });
    let (after_remove, violation) = apply_sk_index_update(Some(&after_insert), &remove_payload);
    assert!(violation.is_none());
    assert_eq!(seisin_core::sk::decode_sk_entries(&after_remove).unwrap(), vec![]);
  }

  #[test]
  fn apply_insert_of_the_same_pk_id_twice_is_not_a_violation() {
    let pk_id = DatumId::new();
    let payload = encode_sk_index_op(&SkIndexOp::Insert {
      pk_id,
      unique_conflict_op: Some("resolve".to_string()),
    });
    let (first, _) = apply_sk_index_update(None, &payload);
    let (second, violation) = apply_sk_index_update(Some(&first), &payload);
    assert!(violation.is_none());
    assert_eq!(
      seisin_core::sk::decode_sk_entries(&second).unwrap().len(),
      1
    );
  }

  #[test]
  fn apply_insert_of_a_different_pk_id_when_unique_reports_a_violation() {
    let first_pk = DatumId::new();
    let second_pk = DatumId::new();
    let first_payload = encode_sk_index_op(&SkIndexOp::Insert {
      pk_id: first_pk,
      unique_conflict_op: Some("resolve".to_string()),
    });
    let (after_first, _) = apply_sk_index_update(None, &first_payload);
    let second_payload = encode_sk_index_op(&SkIndexOp::Insert {
      pk_id: second_pk,
      unique_conflict_op: Some("resolve".to_string()),
    });
    let (_, violation) = apply_sk_index_update(Some(&after_first), &second_payload);
    assert_eq!(violation, Some("resolve".to_string()));
  }

  #[test]
  fn apply_insert_of_a_different_pk_id_without_unique_is_not_a_violation() {
    let first_pk = DatumId::new();
    let second_pk = DatumId::new();
    let first_payload = encode_sk_index_op(&SkIndexOp::Insert {
      pk_id: first_pk,
      unique_conflict_op: None,
    });
    let (after_first, _) = apply_sk_index_update(None, &first_payload);
    let second_payload = encode_sk_index_op(&SkIndexOp::Insert {
      pk_id: second_pk,
      unique_conflict_op: None,
    });
    let (after_second, violation) = apply_sk_index_update(Some(&after_first), &second_payload);
    assert!(violation.is_none());
    assert_eq!(
      seisin_core::sk::decode_sk_entries(&after_second).unwrap().len(),
      2
    );
  }

  #[test]
  fn register_sk_index_handler_wires_the_kind_name_correctly() {
    let mut registry = seisin_node::index_handler::IndexHandlerRegistry::new();
    register_sk_index_handler(&mut registry);
    let pk_id = DatumId::new();
    let payload = encode_sk_index_op(&SkIndexOp::Insert {
      pk_id,
      unique_conflict_op: None,
    });
    let (new_bytes, violation) = registry.apply("sk", None, &payload).unwrap();
    assert!(violation.is_none());
    assert_eq!(
      seisin_core::sk::decode_sk_entries(&new_bytes).unwrap()[0].0,
      pk_id
    );
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-types --lib sk_index::`
Expected: FAIL to compile — `SkIndexOp`/`encode_sk_index_op`/`decode_sk_index_op`/`apply_sk_index_update`/`register_sk_index_handler` don't exist yet, and the old `insert_sk_entry`/`remove_sk_entry`/`UniquenessViolation`-based tests you just replaced are gone from the file (nothing references the old symbols anymore, so this is just the new tests failing to compile against not-yet-written code).

- [ ] **Step 3: Replace the old `insert_sk_entry`/`remove_sk_entry`/`UniquenessViolation` code with the new `IndexHandler`**

In `crates/seisin-types/src/sk_index.rs`, remove the `use seisin_core::authority::AuthorityIdx;`, `use seisin_ops::context::OpContext;` imports and the entire `UniquenessViolation`/`insert_sk_entry`/`remove_sk_entry` block (everything between `sk_key`'s closing brace and the `#[cfg(test)]` module), replacing it with:

```rust
use seisin_core::authority::AuthorityIdx;
use seisin_core::sk::{decode_sk_entries, encode_sk_entries};
use seisin_node::index_handler::IndexHandlerRegistry;

/// One update to apply to an sk index's entry list — the payload
/// `IndexHandlerRegistry` dispatches for the `"sk"` kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkIndexOp {
  Insert {
    pk_id: DatumId,
    unique_conflict_op: Option<String>,
  },
  Remove {
    pk_id: DatumId,
  },
}

const SK_OP_INSERT: u8 = 0;
const SK_OP_REMOVE: u8 = 1;

pub fn encode_sk_index_op(op: &SkIndexOp) -> Vec<u8> {
  let mut buf = Vec::new();
  match op {
    SkIndexOp::Insert {
      pk_id,
      unique_conflict_op,
    } => {
      buf.push(SK_OP_INSERT);
      buf.extend_from_slice(&pk_id.as_bytes());
      match unique_conflict_op {
        None => buf.push(0),
        Some(name) => {
          buf.push(1);
          let name_bytes = name.as_bytes();
          buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
          buf.extend_from_slice(name_bytes);
        }
      }
    }
    SkIndexOp::Remove { pk_id } => {
      buf.push(SK_OP_REMOVE);
      buf.extend_from_slice(&pk_id.as_bytes());
    }
  }
  buf
}

pub fn decode_sk_index_op(buf: &[u8]) -> Result<SkIndexOp> {
  if buf.is_empty() {
    bail!("sk index op payload is empty");
  }
  if buf.len() < 1 + 16 {
    bail!("sk index op payload too short for a pk_id");
  }
  let pk_id = DatumId::from_bytes(buf[1..17].try_into().unwrap());
  match buf[0] {
    SK_OP_REMOVE => Ok(SkIndexOp::Remove { pk_id }),
    SK_OP_INSERT => {
      if buf.len() < 18 {
        bail!("sk insert op payload too short for its conflict_op flag");
      }
      let unique_conflict_op = match buf[17] {
        0 => None,
        1 => {
          if buf.len() < 22 {
            bail!("sk insert op payload too short for its conflict_op length");
          }
          let name_len = u32::from_le_bytes(buf[18..22].try_into().unwrap()) as usize;
          if buf.len() != 22 + name_len {
            bail!("sk insert op payload length mismatch for its conflict_op name");
          }
          Some(
            String::from_utf8(buf[22..22 + name_len].to_vec())
              .context("sk insert op's conflict_op name was not valid utf8")?,
          )
        }
        flag => bail!("unknown sk insert op conflict_op flag: {flag}"),
      };
      Ok(SkIndexOp::Insert {
        pk_id,
        unique_conflict_op,
      })
    }
    tag => bail!("unknown sk index op tag: {tag}"),
  }
}

/// The `IndexHandler` for the `"sk"` kind — applies one `SkIndexOp`
/// against `current`'s decoded entry list (empty if `None`, a cold sk
/// key with nothing resident/stored yet).
pub fn apply_sk_index_update(current: Option<&[u8]>, payload: &[u8]) -> (Vec<u8>, Option<String>) {
  let mut entries = match current {
    Some(bytes) => decode_sk_entries(bytes).unwrap_or_default(),
    None => Vec::new(),
  };
  let op = match decode_sk_index_op(payload) {
    Ok(op) => op,
    Err(e) => {
      return (
        encode_sk_entries(&entries),
        Some(format!("malformed sk index payload: {e}")),
      )
    }
  };
  match op {
    SkIndexOp::Remove { pk_id } => {
      entries.retain(|(id, _)| *id != pk_id);
      (encode_sk_entries(&entries), None)
    }
    SkIndexOp::Insert {
      pk_id,
      unique_conflict_op,
    } => {
      if let Some(conflict_op) = &unique_conflict_op {
        if entries.iter().any(|(id, _)| *id != pk_id) {
          return (encode_sk_entries(&entries), Some(conflict_op.clone()));
        }
      }
      if !entries.iter().any(|(id, _)| *id == pk_id) {
        entries.push((pk_id, AuthorityIdx::Native));
      }
      (encode_sk_entries(&entries), None)
    }
  }
}

/// Registers the `"sk"` kind's `IndexHandler` — call once at startup,
/// alongside registering a solution's ops.
pub fn register_sk_index_handler(registry: &mut IndexHandlerRegistry) {
  registry.register(
    "sk",
    Box::new(|current, payload| apply_sk_index_update(current, payload)),
  );
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-types --lib sk_index::`
Expected: PASS (16 tests: the 6 `sk_key` tests from Part 2's Task 2, unchanged, plus the 10 new ones above).

- [ ] **Step 5: Run the full `seisin-types` lib test suite**

Run: `cargo test -p seisin-types --lib`
Expected: PASS — `field::`/`encoding::`/`schema::` tests unchanged; `sk_index::` as above. (`typed_write::`/`client::` no longer exist, so there's nothing left to run for them.)

- [ ] **Step 6: Commit and push**

```bash
git add crates/seisin-types/Cargo.toml crates/seisin-types/src/sk_index.rs crates/seisin-types/src/lib.rs
git rm crates/seisin-types/src/typed_write.rs crates/seisin-types/src/client.rs crates/seisin-types/tests/integration_typed_write_client.rs
git commit -m "feat: replace the old OpContext-based sk write path with a byte-level sk IndexHandler"
git push
```

---

### Task 8: `TypedOpContext` (automatic diffing) and an end-to-end integration test

**Files:**
- Create: `crates/seisin-types/src/typed_context.rs`
- Create: `crates/seisin-types/tests/integration_automatic_index_maintenance.rs`
- Modify: `crates/seisin-types/Cargo.toml`
- Modify: `crates/seisin-types/src/lib.rs`

**Interfaces:**
- Consumes: `DatumTypeDef`/`IndexDef`/`FieldValue` (Part 1), `sk_key`/`SkIndexOp`/`encode_sk_index_op` (Task 7), `OpContext::get`/`put`/`delete`/`schedule_index_update` (Task 3).
- Produces: `TypedOpContext::new(ctx)`, `TypedOpContext::get(pk_id, def) -> Option<Vec<FieldValue>>`, `TypedOpContext::set(pk_id, def, values)`, `TypedOpContext::delete(pk_id, def)`.

- [ ] **Step 1: Add `seisin-ops` back as a regular dependency**

After Task 7, `crates/seisin-types/Cargo.toml` reads:

```toml
[package]
name = "seisin-types"
version = "0.1.0"
edition = "2021"

[dependencies]
seisin-core = { path = "../seisin-core" }
seisin-node = { path = "../seisin-node" }
anyhow = "1"

[dev-dependencies]
seisin-ring = { path = "../seisin-ring" }
```

Change it to:

```toml
[package]
name = "seisin-types"
version = "0.1.0"
edition = "2021"

[dependencies]
seisin-core = { path = "../seisin-core" }
seisin-node = { path = "../seisin-node" }
seisin-ops = { path = "../seisin-ops" }
anyhow = "1"

[dev-dependencies]
seisin-ring = { path = "../seisin-ring" }
```

(`seisin-node` is already a *regular* dependency from Task 7, so it's automatically available to this task's integration test too — no separate dev-dependency entry needed for it.)

- [ ] **Step 2: Write the failing unit tests**

```rust
// crates/seisin-types/src/typed_context.rs
#[cfg(test)]
mod tests {
  use super::*;
  use crate::field::FieldType;
  use crate::schema::{ConflictOp, DatumTypeDef, IndexDef};
  use crate::sk_index::{decode_sk_index_op, sk_key, SkIndexOp};
  use seisin_core::cache::Cache;
  use seisin_core::store::InMemoryStore;
  use seisin_ops::context::OpContext;
  use std::sync::Arc;

  fn user_type() -> DatumTypeDef {
    DatumTypeDef::new("user")
      .field("name", FieldType::String)
      .field("age", FieldType::I64)
      .index(IndexDef::Sk {
        field: "name".to_string(),
        unique: None,
      })
  }

  #[test]
  fn a_fresh_create_schedules_one_insert_and_no_remove() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let def = user_type();
    let pk_id = DatumId::new();
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.set(
        pk_id,
        &def,
        vec![FieldValue::String("cliff".to_string()), FieldValue::I64(41)],
      );
    } // tctx dropped here — diffing happens now
    let updates = ctx.take_pending_index_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].index_kind, "sk");
    let expected_key = sk_key("user", "name", &FieldValue::String("cliff".to_string())).unwrap();
    assert_eq!(updates[0].target, expected_key);
    match decode_sk_index_op(&updates[0].payload).unwrap() {
      SkIndexOp::Insert { pk_id: id, .. } => assert_eq!(id, pk_id),
      other => panic!("expected an Insert op, got {other:?}"),
    }
  }

  #[test]
  fn updating_the_indexed_field_schedules_a_remove_and_an_insert() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let def = user_type();
    let pk_id = DatumId::new();

    // First write: establishes the pk datum's initial content directly
    // via the underlying OpContext (simulating an earlier op).
    {
      let mut ctx = OpContext::new(&mut cache);
      ctx.put(
        pk_id,
        crate::encode_datum(
          &def,
          &[FieldValue::String("cliff".to_string()), FieldValue::I64(41)],
        )
        .unwrap(),
      );
      for (id, content) in ctx.take_staged_writes() {
        if let Some(bytes) = content {
          cache.put(id, bytes);
        }
      }
    }

    let mut ctx = OpContext::new(&mut cache);
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.get(pk_id, &def);
      tctx.set(
        pk_id,
        &def,
        vec![
          FieldValue::String("clifford".to_string()),
          FieldValue::I64(41),
        ],
      );
    }
    let updates = ctx.take_pending_index_updates();
    assert_eq!(updates.len(), 2);
    let old_key = sk_key("user", "name", &FieldValue::String("cliff".to_string())).unwrap();
    let new_key = sk_key("user", "name", &FieldValue::String("clifford".to_string())).unwrap();
    assert!(updates.iter().any(|u| u.target == old_key
      && matches!(decode_sk_index_op(&u.payload).unwrap(), SkIndexOp::Remove { .. })));
    assert!(updates.iter().any(|u| u.target == new_key
      && matches!(decode_sk_index_op(&u.payload).unwrap(), SkIndexOp::Insert { .. })));
  }

  #[test]
  fn writing_the_same_indexed_value_again_schedules_nothing() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let def = user_type();
    let pk_id = DatumId::new();
    let values = vec![FieldValue::String("cliff".to_string()), FieldValue::I64(41)];
    {
      let mut ctx = OpContext::new(&mut cache);
      ctx.put(pk_id, crate::encode_datum(&def, &values).unwrap());
      for (id, content) in ctx.take_staged_writes() {
        if let Some(bytes) = content {
          cache.put(id, bytes);
        }
      }
    }

    let mut ctx = OpContext::new(&mut cache);
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.get(pk_id, &def);
      tctx.set(pk_id, &def, values);
    }
    assert_eq!(ctx.take_pending_index_updates().len(), 0);
  }

  #[test]
  fn a_plain_get_with_no_set_schedules_nothing() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let def = user_type();
    let pk_id = DatumId::new();
    let mut ctx = OpContext::new(&mut cache);
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.get(pk_id, &def);
    }
    assert_eq!(ctx.take_pending_index_updates().len(), 0);
  }

  #[test]
  fn delete_schedules_a_remove_from_every_declared_sk_index() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let def = user_type();
    let pk_id = DatumId::new();
    let values = vec![FieldValue::String("cliff".to_string()), FieldValue::I64(41)];
    {
      let mut ctx = OpContext::new(&mut cache);
      ctx.put(pk_id, crate::encode_datum(&def, &values).unwrap());
      for (id, content) in ctx.take_staged_writes() {
        if let Some(bytes) = content {
          cache.put(id, bytes);
        }
      }
    }

    let mut ctx = OpContext::new(&mut cache);
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.get(pk_id, &def);
      tctx.delete(pk_id, &def);
    }
    let updates = ctx.take_pending_index_updates();
    assert_eq!(updates.len(), 1);
    let key = sk_key("user", "name", &FieldValue::String("cliff".to_string())).unwrap();
    assert_eq!(updates[0].target, key);
    assert!(matches!(
      decode_sk_index_op(&updates[0].payload).unwrap(),
      SkIndexOp::Remove { .. }
    ));
  }

  #[test]
  fn a_unique_index_carries_its_conflict_op_into_the_scheduled_insert() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let def = DatumTypeDef::new("user")
      .field("email", FieldType::String)
      .index(IndexDef::Sk {
        field: "email".to_string(),
        unique: Some(ConflictOp("resolve".to_string())),
      });
    let pk_id = DatumId::new();
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.set(
        pk_id,
        &def,
        vec![FieldValue::String("a@example.com".to_string())],
      );
    }
    let updates = ctx.take_pending_index_updates();
    match decode_sk_index_op(&updates[0].payload).unwrap() {
      SkIndexOp::Insert {
        unique_conflict_op, ..
      } => assert_eq!(unique_conflict_op, Some("resolve".to_string())),
      other => panic!("expected an Insert op, got {other:?}"),
    }
  }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p seisin-types --lib typed_context::`
Expected: FAIL to compile — `TypedOpContext` doesn't exist yet.

- [ ] **Step 4: Implement `TypedOpContext`**

```rust
// crates/seisin-types/src/typed_context.rs, above the tests module
//! A typed accessor wrapping `OpContext`, used by a solution's op
//! handler instead of raw `ctx.get`/`ctx.put`. Field-level changes are
//! detected automatically on drop and turned into scheduled index
//! updates — the op author never writes index-maintenance code by
//! hand. See the design doc's "Automatic Index Maintenance & Op
//! Lifecycle" section.

use std::collections::HashMap;

use seisin_core::datum::DatumId;
use seisin_ops::context::OpContext;

use crate::field::FieldValue;
use crate::schema::{decode_datum, encode_datum, DatumTypeDef, IndexDef};
use crate::sk_index::{encode_sk_index_op, sk_key, SkIndexOp};

struct TrackedDatum {
  def: DatumTypeDef,
  before: Option<Vec<FieldValue>>,
  after: Option<Vec<FieldValue>>,
  touched: bool,
}

pub struct TypedOpContext<'a, 'b> {
  ctx: &'b mut OpContext<'a>,
  tracked: HashMap<DatumId, TrackedDatum>,
}

impl<'a, 'b> TypedOpContext<'a, 'b> {
  pub fn new(ctx: &'b mut OpContext<'a>) -> Self {
    Self {
      ctx,
      tracked: HashMap::new(),
    }
  }

  /// Reads `pk_id`'s current typed value, decoding via `def`. Remembers
  /// it as the "before" snapshot for diffing on drop, if `pk_id` hasn't
  /// been tracked yet this op.
  pub fn get(&mut self, pk_id: DatumId, def: &DatumTypeDef) -> Option<Vec<FieldValue>> {
    let values = self
      .ctx
      .get(pk_id)
      .and_then(|bytes| decode_datum(def, &bytes).ok());
    self.tracked.entry(pk_id).or_insert_with(|| TrackedDatum {
      def: def.clone(),
      before: values.clone(),
      after: values.clone(),
      touched: false,
    });
    values
  }

  /// Writes `pk_id`'s new typed value. The byte write is staged
  /// immediately via the underlying `OpContext`; index maintenance is
  /// computed automatically on drop.
  pub fn set(&mut self, pk_id: DatumId, def: &DatumTypeDef, values: Vec<FieldValue>) {
    self.ensure_tracked(pk_id, def);
    if let Ok(bytes) = encode_datum(def, &values) {
      self.ctx.put(pk_id, bytes);
    }
    let entry = self.tracked.get_mut(&pk_id).unwrap();
    entry.after = Some(values);
    entry.touched = true;
  }

  /// Deletes `pk_id`. Same tracking/diffing as `set`, but with an
  /// `after` of `None` — every declared sk index gets a remove
  /// scheduled for whatever the "before" value was.
  pub fn delete(&mut self, pk_id: DatumId, def: &DatumTypeDef) {
    self.ensure_tracked(pk_id, def);
    self.ctx.delete(pk_id);
    let entry = self.tracked.get_mut(&pk_id).unwrap();
    entry.after = None;
    entry.touched = true;
  }

  fn ensure_tracked(&mut self, pk_id: DatumId, def: &DatumTypeDef) {
    if self.tracked.contains_key(&pk_id) {
      return;
    }
    let before = self
      .ctx
      .get(pk_id)
      .and_then(|bytes| decode_datum(def, &bytes).ok());
    self.tracked.insert(
      pk_id,
      TrackedDatum {
        def: def.clone(),
        before,
        after: None,
        touched: false,
      },
    );
  }
}

impl<'a, 'b> Drop for TypedOpContext<'a, 'b> {
  fn drop(&mut self) {
    for (pk_id, tracked) in self.tracked.drain() {
      if !tracked.touched {
        continue;
      }
      for index in &tracked.def.indexes {
        let IndexDef::Sk { field, unique } = index;
        let Some(field_idx) = tracked.def.fields.iter().position(|(name, _)| name == field)
        else {
          continue;
        };
        let old_value = tracked.before.as_ref().map(|v| v[field_idx].clone());
        let new_value = tracked.after.as_ref().map(|v| v[field_idx].clone());
        if old_value == new_value {
          continue;
        }
        if let Some(old_value) = &old_value {
          if let Ok(old_key) = sk_key(&tracked.def.name, field, old_value) {
            let payload = encode_sk_index_op(&SkIndexOp::Remove { pk_id });
            self.ctx.schedule_index_update(old_key, "sk", payload);
          }
        }
        if let Some(new_value) = &new_value {
          if let Ok(new_key) = sk_key(&tracked.def.name, field, new_value) {
            let conflict_op = unique.as_ref().map(|op| op.0.clone());
            let payload = encode_sk_index_op(&SkIndexOp::Insert {
              pk_id,
              unique_conflict_op: conflict_op,
            });
            self.ctx.schedule_index_update(new_key, "sk", payload);
          }
        }
      }
    }
  }
}
```

Add `pub mod typed_context;` to `crates/seisin-types/src/lib.rs`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p seisin-types --lib typed_context::`
Expected: PASS (6 tests).

- [ ] **Step 6: Write the end-to-end integration test**

```rust
// crates/seisin-types/tests/integration_automatic_index_maintenance.rs
use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::InMemoryStore;
use seisin_node::index_handler::IndexHandlerRegistry;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ops::context::OpContext;
use seisin_ops::registry::OpRegistry;
use seisin_ring::ring::Ring;
use seisin_types::field::{FieldType, FieldValue};
use seisin_types::schema::{ConflictOp, DatumTypeDef, IndexDef};
use seisin_types::sk_index::register_sk_index_handler;
use seisin_types::typed_context::TypedOpContext;
use seisin_types::{decode_datum, encode_datum};
use seisin_protocol::{Request, Response};

fn user_type() -> DatumTypeDef {
  DatumTypeDef::new("user")
    .field("email", FieldType::String)
    .index(IndexDef::Sk {
      field: "email".to_string(),
      unique: Some(ConflictOp("resolve_duplicate_email".to_string())),
    })
}

fn start_node() -> String {
  let def = user_type();
  let mut ops = OpRegistry::new();
  let write_def = def.clone();
  ops.register(
    "write_user",
    Box::new(move |ctx: &mut OpContext, ids, payload| {
      let values = decode_datum(&write_def, payload).unwrap();
      let mut tctx = TypedOpContext::new(ctx);
      tctx.get(ids[0], &write_def);
      tctx.set(ids[0], &write_def, values);
      vec![]
    }),
  );

  let mut index_handlers = IndexHandlerRegistry::new();
  register_sk_index_handler(&mut index_handlers);

  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  let node_id = NodeId(1);
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_id, 1)])));
  let peer_link_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    1,
    Arc::new(ops),
    Arc::clone(&ring),
    node_id,
    peer_link_listener,
    Arc::new(std::collections::HashMap::new()),
    Arc::new(index_handlers),
  ));
  let address_book = Arc::new(std::collections::HashMap::new());
  thread::spawn(move || serve(listener, node_id, ring, address_book, pool));
  thread::sleep(std::time::Duration::from_millis(100));
  addr
}

#[test]
fn a_second_write_of_the_same_unique_value_fails_the_whole_op_automatically() {
  let addr = start_node();
  let def = user_type();

  let first_pk = DatumId::new();
  let values = vec![FieldValue::String("a@example.com".to_string())];
  let first_response = seisin_client::call(
    &addr,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "write_user".to_string(),
      datum_ids: vec![first_pk],
      payload: encode_datum(&def, &values).unwrap(),
    },
  )
  .unwrap();
  assert_eq!(first_response, Response::OpResult { payload: vec![] });

  let second_pk = DatumId::new();
  let second_response = seisin_client::call(
    &addr,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "write_user".to_string(),
      datum_ids: vec![second_pk],
      payload: encode_datum(&def, &values).unwrap(),
    },
  )
  .unwrap();
  match second_response {
    Response::OpError { message } => assert_eq!(message, "resolve_duplicate_email"),
    other => panic!("expected OpError, got {other:?}"),
  }
}
```

This integration test needs `seisin-client`/`seisin-protocol` again (Task 7 dropped them from `[dependencies]` since the old `client.rs` was their only consumer) — but only as dev-dependencies now, since only tests use them. Change `crates/seisin-types/Cargo.toml`'s `[dev-dependencies]` from:

```toml
[dev-dependencies]
seisin-ring = { path = "../seisin-ring" }
```

to:

```toml
[dev-dependencies]
seisin-ring = { path = "../seisin-ring" }
seisin-client = { path = "../seisin-client" }
seisin-protocol = { path = "../seisin-protocol" }
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `cargo test -p seisin-types --test integration_automatic_index_maintenance`
Expected: PASS (1 test).

- [ ] **Step 8: Run it repeatedly to check for flakiness**

```bash
for i in $(seq 1 10); do
  cargo test -p seisin-types --test integration_automatic_index_maintenance 2>&1 | tail -3
done
```
Expected: PASS every time — this test exercises the full cross-thread op-lifecycle dispatch (even though both the pk datum and the sk index happen to be native to the same single-thread node here, the dispatch still goes through the real `WorkerMessage::IndexUpdate`/`IndexUpdateReplied` round trip, not a shortcut).

- [ ] **Step 9: Commit and push**

```bash
git add crates/seisin-types/Cargo.toml crates/seisin-types/src/typed_context.rs crates/seisin-types/src/lib.rs crates/seisin-types/tests/integration_automatic_index_maintenance.rs
git commit -m "feat: add TypedOpContext with automatic index-update diffing on drop"
git push
```

---

### Task 9: Quality gate and progress tracker update

**Files:** `docs/superpowers/PROGRESS.md` (modify).

- [ ] **Step 1: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS — including every earlier sub-project's tests, confirming the op-lifecycle rewrite and staged-write change haven't regressed anything.

- [ ] **Step 2: Run fmt and clippy**

Run: `cargo fmt --check`
Expected: no output; if it reports diffs, run `cargo fmt` and re-check.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings/errors.

- [ ] **Step 3: Re-run the concurrency-sensitive integration tests repeatedly**

This plan touches `worker.rs`'s core op-completion path — the same file Part 1 and Part 2a/2b each found real concurrency bugs in via repeated runs, not single passes.

```bash
for i in $(seq 1 20); do
  cargo test -p seisin-node --test integration_wound_wait 2>&1 | tail -3
  cargo test -p seisin-node --test integration_cross_node_wound_wait 2>&1 | tail -3
  cargo test -p seisin-node --test integration_op_collation 2>&1 | tail -3
  cargo test -p seisin-types --test integration_automatic_index_maintenance 2>&1 | tail -3
done
```
Expected: PASS every time.

- [ ] **Step 4: Update `docs/superpowers/PROGRESS.md`**

Add an entry under "Done" for this plan (Datum Type System, Part 2 revised), summarizing: `IndexHandlerRegistry`, the `Request::IndexUpdate`/`Response::IndexUpdateResult` wire pair, `OpContext`'s staged writes and `schedule_index_update`, the three-phase op lifecycle in `worker.rs` (dispatch → wait → commit-or-fail), the resident per-thread index cache, `TypedOpContext`'s Drop-based automatic diffing, and the sk `IndexHandler` replacing Part 2's old two-round-trip design. Note that Part 3 (rk — the splay-tree leaderboard) builds on this same mechanism and is next.

- [ ] **Step 5: Commit and push**

```bash
git add -A
git commit -m "chore: fmt/clippy fixes for datum type system part 2 (index lifecycle + sk retrofit); update progress tracker"
git push
```

---

## Self-Review Notes

- **Spec coverage**: "Automatic Index Maintenance & Op Lifecycle" ✓ (Tasks 1-6: `IndexHandlerRegistry`, wire messages, staged writes, resident cache, three-phase lifecycle); sk's revised "Update flow"/"Uniqueness" ✓ (Tasks 7-8: `SkIndexOp`/`apply_sk_index_update` as the `"sk"` `IndexHandler`, `TypedOpContext`'s automatic diffing, end-to-end uniqueness rejection proven in Task 8's integration test). rk/tk (Parts 3/4) and relational constraints (Part 5) are explicitly out of scope for this plan.
- **Placeholder scan**: no TBD/TODO; every step has complete code. Task 6's Step 1 test `a_violation_from_a_scheduled_index_update_fails_the_whole_op_with_no_write` includes an inline note that observing "the write never landed from the outside" needs a second pool sharing the same store, which is genuinely Task 8's integration test's job, not a placeholder — the unit test's own assertion (the op's `Result` is `Err`) is real and complete.
- **Type consistency**: `IndexHandlerRegistry`/`IndexHandler` (Task 1) match exactly what `worker.rs` (Tasks 4/6) and `sk_index.rs` (Task 7) consume. `Request::IndexUpdate`/`Response::IndexUpdateResult` (Task 2) match exactly what `dispatch_index_update`/`pool.rs`'s `on_request` (Tasks 5-6) construct and destructure. `OpContext::schedule_index_update`/`take_staged_writes`/`take_pending_index_updates`/`PendingIndexUpdate` (Task 3) match exactly what `worker.rs` (Task 6) and `TypedOpContext` (Task 8) consume. `SkIndexOp`/`encode_sk_index_op`/`decode_sk_index_op` (Task 7) match exactly what `TypedOpContext`'s `Drop` impl (Task 8) constructs.
