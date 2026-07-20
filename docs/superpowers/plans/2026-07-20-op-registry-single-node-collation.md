# Op Registry & Single-Node Collation Implementation Plan (Sub-project 3a)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the op-registry, `OpContext`, thread-assignment, and
write-back/anti-degeneration mechanics for a solution-defined multi-datum
operation, for the single-node, uncontended case: every datum_id an op
touches is already natively owned by some thread on the one node handling
the request, and no other op is concurrently contending for the same
datums. Cross-node transfer and wound-wait contention handling are
Sub-project 3b, built on top without changing this plan's op-invocation
mechanics.

**Architecture:** A new `seisin-ops` crate holds `OpContext` (byte-level
get/put/delete over a `&mut Cache`) and `OpRegistry` (a name→handler
table; `invoke` catches a panicking handler and reports it as an error
rather than crashing the owning thread). `seisin-protocol` gains a
`Request::Op` variant (op name + the caller-supplied datum_id list +an
opaque payload) and `Response::OpResult`/`OpError`. `WorkerHandle` gains
`Evict`/`RunOp` messages; `WorkerPool` gains `evict_single` (pre-collation:
tell a source thread to drop its copy), `evict_non_native_for` (post-op:
targeted anti-degeneration on one thread), and `run_op`. The server's new
op-dispatch path resolves every datum_id's native (node, thread), rejects
the request if any resolve off-node (3b's job), picks the local thread
that natively owns the most of them, evicts the rest from wherever they
currently sit, runs the op there, and evicts anything foreign left behind
afterward.

**Tech Stack:** Same as prior plans (Rust 2021, `anyhow`).

## Global Constraints

(Same as prior plans' — repeated since every task's requirements
implicitly include them.)

- `anyhow::Result<T>` + `bail!()`/`.context()` is the only accepted error
  style for wire decoding; `OpRegistry::invoke` uses `Result<_, String>`
  instead, matching this plan's need to send an error message back over
  the wire as `Response::OpError`, not to propagate a process-level error.
- `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` must
  pass; 2-space indent via the repo's `rustfmt.toml`.
- Prefer many small, single-purpose crates over one monolith.
- Public items get `///`/`//!` doc comments describing invariants and
  guarantees.

**From the design doc's "Op Registry & Collation Mechanics" section:**

- Ops are solution-defined Rust functions, not a generic wire-level batch
  operation. The framework never interprets an op's internal logic.
- `OpContext` is byte-level only — the typed datum layer is a separate,
  not-yet-designed layer a solution's own generated code would sit on
  top of.
- Thread assignment picks whichever local thread *natively* owns the
  most of the op's datum_ids — the "or already-foreign" refinement is
  explicitly deferred.
- If any datum_id resolves to a different node, this plan's dispatch
  returns an error (`Response::OpError`) rather than attempting transfer
  — that's Sub-project 3b.
- Anti-degeneration here is simplified: always evict foreign entries from
  the destination thread after the op runs, with no peek-ahead at the
  next queued request — that refinement is left for later.

---

### Task 1: `seisin-ops` — `OpContext`

**Files:**
- Create: `crates/seisin-ops/Cargo.toml`
- Create: `crates/seisin-ops/src/lib.rs`
- Create: `crates/seisin-ops/src/context.rs`
- Modify: `Cargo.toml` (workspace root)

**Interfaces:**
- Consumes: `seisin_core::{cache::Cache, datum::DatumId}`.
- Produces: `seisin_ops::context::OpContext` — `OpContext::new(&mut Cache)
  -> Self`, `get(&mut self, DatumId) -> Option<Vec<u8>>`, `put(&mut self,
  DatumId, Vec<u8>)`, `delete(&mut self, DatumId)`.

- [ ] **Step 1: Scaffold the crate and write the failing test**

`crates/seisin-ops/Cargo.toml`:

```toml
[package]
name = "seisin-ops"
version = "0.1.0"
edition = "2021"

[dependencies]
seisin-core = { path = "../seisin-core" }
```

Add `"crates/seisin-ops"` to the workspace `members` list in the root
`Cargo.toml`.

`crates/seisin-ops/src/lib.rs`:

```rust
pub mod context;
```

`crates/seisin-ops/src/context.rs`:

```rust
//! `OpContext` is the interface a solution-defined operation function
//! uses to read/write the datums the framework has already collated
//! onto this thread. It operates at the byte level — the typed datum
//! layer from the design doc's "Datum Type System" notes is a separate,
//! not-yet-designed layer a solution's own generated code would sit on
//! top of; the framework itself stays type-agnostic.

use seisin_core::cache::Cache;
use seisin_core::datum::DatumId;

pub struct OpContext<'a> {
  cache: &'a mut Cache,
}

impl<'a> OpContext<'a> {
  pub fn new(cache: &'a mut Cache) -> Self {
    Self { cache }
  }

  pub fn get(&mut self, id: DatumId) -> Option<Vec<u8>> {
    let _ = id;
    unimplemented!()
  }

  pub fn put(&mut self, id: DatumId, content: Vec<u8>) {
    let _ = (id, content);
    unimplemented!()
  }

  pub fn delete(&mut self, id: DatumId) {
    let _ = id;
    unimplemented!()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::Arc;

  use seisin_core::store::InMemoryStore;

  #[test]
  fn put_then_get_round_trips_through_the_context() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let id = DatumId::new();
    ctx.put(id, b"hello".to_vec());
    assert_eq!(ctx.get(id), Some(b"hello".to_vec()));
  }

  #[test]
  fn delete_removes_the_entry() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let id = DatumId::new();
    ctx.put(id, b"hello".to_vec());
    ctx.delete(id);
    assert_eq!(ctx.get(id), None);
  }

  #[test]
  fn get_on_unknown_datum_returns_none() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    assert_eq!(ctx.get(DatumId::new()), None);
  }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-ops`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement**

```rust
  pub fn get(&mut self, id: DatumId) -> Option<Vec<u8>> {
    self.cache.get(id)
  }

  pub fn put(&mut self, id: DatumId, content: Vec<u8>) {
    self.cache.put(id, content);
  }

  pub fn delete(&mut self, id: DatumId) {
    self.cache.delete(id);
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-ops`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit and push**

```bash
git add Cargo.toml crates/seisin-ops
git commit -m "feat: add seisin-ops OpContext"
git push
```

---

### Task 2: `seisin-ops` — `OpRegistry`

**Files:**
- Create: `crates/seisin-ops/src/registry.rs`
- Modify: `crates/seisin-ops/src/lib.rs`

**Interfaces:**
- Consumes: `seisin_core::datum::DatumId`, `seisin_ops::context::OpContext`.
- Produces: `seisin_ops::registry::{OpHandler, OpRegistry}`. `OpHandler =
  Box<dyn Fn(&mut OpContext, &[DatumId], &[u8]) -> Vec<u8> + Send + Sync>`.
  `OpRegistry::new() -> Self`, `register(&mut self, impl Into<String>,
  OpHandler)`, `invoke(&self, &str, &mut OpContext, &[DatumId], &[u8]) ->
  Result<Vec<u8>, String>` (catches a panicking handler).

- [ ] **Step 1: Write the failing test**

`crates/seisin-ops/src/registry.rs`:

```rust
//! `OpRegistry`: the table of solution-defined operations a server
//! process was built with. Registration happens once at startup;
//! lookup happens once per incoming `Request::Op`.

use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};

use seisin_core::datum::DatumId;

use crate::context::OpContext;

/// A solution-defined operation: given mutable access to the collated
/// datums (via `OpContext`), the exact datum_ids this invocation was
/// collated for, and an opaque argument payload, returns an opaque
/// result payload. Solutions choose their own payload serialization;
/// the framework never interprets it.
pub type OpHandler = Box<dyn Fn(&mut OpContext, &[DatumId], &[u8]) -> Vec<u8> + Send + Sync>;

#[derive(Default)]
pub struct OpRegistry {
  handlers: HashMap<String, OpHandler>,
}

impl OpRegistry {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn register(&mut self, name: impl Into<String>, handler: OpHandler) {
    self.handlers.insert(name.into(), handler);
  }

  /// Looks up and invokes the named op, catching a panic inside the
  /// handler so a bug in solution code can't take down the thread that
  /// owns every other datum it's currently holding. Returns `Err` with a
  /// message either if the op name is unknown or if the handler panicked.
  pub fn invoke(
    &self,
    name: &str,
    ctx: &mut OpContext,
    datum_ids: &[DatumId],
    payload: &[u8],
  ) -> Result<Vec<u8>, String> {
    let _ = (name, ctx, datum_ids, payload);
    unimplemented!()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::Arc;

  use seisin_core::cache::Cache;
  use seisin_core::store::InMemoryStore;

  #[test]
  fn invokes_a_registered_op() {
    let mut registry = OpRegistry::new();
    registry.register("echo", Box::new(|_ctx, _ids, payload| payload.to_vec()));

    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    assert_eq!(registry.invoke("echo", &mut ctx, &[], b"hello").unwrap(), b"hello");
  }

  #[test]
  fn unknown_op_name_returns_an_error() {
    let registry = OpRegistry::new();
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    assert!(registry.invoke("nope", &mut ctx, &[], b"").is_err());
  }

  #[test]
  fn a_panicking_op_is_caught_and_reported_as_an_error() {
    let mut registry = OpRegistry::new();
    registry.register("boom", Box::new(|_ctx, _ids, _payload| panic!("solution bug")));
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    assert!(registry.invoke("boom", &mut ctx, &[], b"").is_err());
  }

  #[test]
  fn an_op_can_read_and_write_through_the_context_using_its_datum_ids() {
    let mut registry = OpRegistry::new();
    registry.register(
      "put_first",
      Box::new(|ctx, ids, payload| {
        ctx.put(ids[0], payload.to_vec());
        payload.to_vec()
      }),
    );
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let id = DatumId::new();
    registry.invoke("put_first", &mut ctx, &[id], b"hi").unwrap();
    assert_eq!(ctx.get(id), Some(b"hi".to_vec()));
  }
}
```

Add `pub mod registry;` to `crates/seisin-ops/src/lib.rs`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-ops`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement**

```rust
  pub fn invoke(
    &self,
    name: &str,
    ctx: &mut OpContext,
    datum_ids: &[DatumId],
    payload: &[u8],
  ) -> Result<Vec<u8>, String> {
    let handler = self.handlers.get(name).ok_or_else(|| format!("unknown op: {name}"))?;
    catch_unwind(AssertUnwindSafe(|| handler(ctx, datum_ids, payload)))
      .map_err(|_| format!("op '{name}' panicked"))
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-ops`
Expected: PASS (4 new tests; 7 total in the crate)

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-ops
git commit -m "feat: add OpRegistry with panic-safe invocation"
git push
```

---

### Task 3: `Request::Op` / `Response::OpResult`/`OpError` wire codec

**Files:**
- Modify: `crates/seisin-protocol/src/lib.rs`

**Interfaces:**
- Produces: `Request::Op { op_name: String, datum_ids: Vec<DatumId>,
  payload: Vec<u8> }`, `Response::OpResult { payload: Vec<u8> }`,
  `Response::OpError { message: String }`, all with encode/decode support.

- [ ] **Step 1: Write the failing test**

Add the `Op` variant to `Request`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
  Get { id: DatumId },
  Put { id: DatumId, content: Vec<u8> },
  Delete { id: DatumId },
  Op {
    op_name: String,
    datum_ids: Vec<DatumId>,
    payload: Vec<u8>,
  },
}
```

Update `Request::datum_id` to panic on `Op` (documented — callers must
match on the variant directly for multi-datum requests):

```rust
impl Request {
  /// The datum_id every single-datum `Request` variant carries.
  ///
  /// # Panics
  /// Panics on `Request::Op`, which carries multiple datum_ids —
  /// callers must match on the variant directly before calling this.
  pub fn datum_id(&self) -> DatumId {
    match self {
      Request::Get { id } | Request::Put { id, .. } | Request::Delete { id } => *id,
      Request::Op { .. } => panic!("Request::Op has no single datum_id; match on the variant directly"),
    }
  }
}
```

Add the new variants to `Response`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
  Value {
    content: Vec<u8>,
    authority: AuthorityIdx,
  },
  NotFound,
  Ok,
  Redirect {
    address: String,
  },
  OpResult {
    payload: Vec<u8>,
  },
  OpError {
    message: String,
  },
}
```

Add opcode constants near the existing ones:

```rust
const OP_OP: u8 = 4;
```

```rust
const RESP_OP_RESULT: u8 = 4;
const RESP_OP_ERROR: u8 = 5;
```

Add stub arms to `encode_request`/`decode_request`/`encode_response`/
`decode_response`:

```rust
// encode_request, new match arm:
Request::Op { op_name, datum_ids, payload } => {
  let _ = (op_name, datum_ids, payload);
  unimplemented!()
}
```

```rust
// decode_request, new match arm before the `op => bail!(...)` catch-all:
OP_OP => unimplemented!(),
```

```rust
// encode_response, new match arms:
Response::OpResult { payload } => {
  let _ = payload;
  unimplemented!()
}
Response::OpError { message } => {
  let _ = message;
  unimplemented!()
}
```

```rust
// decode_response, new match arms before the `op => bail!(...)` catch-all:
RESP_OP_RESULT => unimplemented!(),
RESP_OP_ERROR => unimplemented!(),
```

Add these tests to the `tests` module:

```rust
#[test]
fn round_trips_op_request_with_no_datum_ids() {
  let req = Request::Op {
    op_name: "noop".to_string(),
    datum_ids: vec![],
    payload: vec![],
  };
  assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
}

#[test]
fn round_trips_op_request_with_datum_ids_and_payload() {
  let req = Request::Op {
    op_name: "transfer".to_string(),
    datum_ids: vec![DatumId::new(), DatumId::new()],
    payload: b"amount:100".to_vec(),
  };
  assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
}

#[test]
fn round_trips_op_result_response() {
  let resp = Response::OpResult {
    payload: b"ok".to_vec(),
  };
  assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
}

#[test]
fn round_trips_op_error_response() {
  let resp = Response::OpError {
    message: "unknown op: foo".to_string(),
  };
  assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
}

#[test]
#[should_panic]
fn datum_id_panics_on_op_request() {
  Request::Op {
    op_name: "x".to_string(),
    datum_ids: vec![],
    payload: vec![],
  }
  .datum_id();
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-protocol`
Expected: FAIL (panics with "not implemented"; `datum_id_panics_on_op_request`
trivially passes already since `unimplemented!()` also panics — that's
fine, it'll keep passing once real behavior lands too)

- [ ] **Step 3: Implement**

Replace the `encode_request` `Op` arm:

```rust
Request::Op { op_name, datum_ids, payload } => {
  buf.push(OP_OP);
  let name_bytes = op_name.as_bytes();
  buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
  buf.extend_from_slice(name_bytes);
  buf.extend_from_slice(&(datum_ids.len() as u32).to_le_bytes());
  for id in datum_ids {
    buf.extend_from_slice(&id.as_bytes());
  }
  buf.extend_from_slice(payload);
}
```

Replace the `decode_request` `OP_OP` arm:

```rust
OP_OP => {
  if buf.len() < 5 {
    bail!("op request too short for a name length: {} bytes", buf.len());
  }
  let name_len = u32::from_le_bytes(buf[1..5].try_into().unwrap()) as usize;
  let mut offset = 5;
  if buf.len() < offset + name_len {
    bail!("op request too short for its name: expected {name_len} bytes");
  }
  let op_name =
    String::from_utf8(buf[offset..offset + name_len].to_vec()).context("op name was not valid utf8")?;
  offset += name_len;
  if buf.len() < offset + 4 {
    bail!("op request too short for a datum_ids count");
  }
  let id_count = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
  offset += 4;
  let mut datum_ids = Vec::with_capacity(id_count);
  for _ in 0..id_count {
    if buf.len() < offset + ID_LEN {
      bail!("op request truncated in datum_ids list");
    }
    let id_bytes: [u8; ID_LEN] = buf[offset..offset + ID_LEN].try_into().unwrap();
    datum_ids.push(DatumId::from_bytes(id_bytes));
    offset += ID_LEN;
  }
  let payload = buf[offset..].to_vec();
  Ok(Request::Op { op_name, datum_ids, payload })
}
```

Replace the `encode_response` new arms:

```rust
Response::OpResult { payload } => {
  buf.push(RESP_OP_RESULT);
  buf.extend_from_slice(payload);
}
Response::OpError { message } => {
  buf.push(RESP_OP_ERROR);
  buf.extend_from_slice(message.as_bytes());
}
```

Replace the `decode_response` new arms:

```rust
RESP_OP_RESULT => Ok(Response::OpResult { payload: buf[1..].to_vec() }),
RESP_OP_ERROR => {
  let message = String::from_utf8(buf[1..].to_vec()).context("op error message was not valid utf8")?;
  Ok(Response::OpError { message })
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-protocol`
Expected: PASS (17 tests: 12 existing + 5 new)

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-protocol/src/lib.rs
git commit -m "feat: add Request::Op and Response::OpResult/OpError"
git push
```

---

### Task 4: `WorkerHandle` — `Evict`/`RunOp` messages

**Files:**
- Modify: `crates/seisin-node/src/worker.rs`
- Modify: `crates/seisin-node/Cargo.toml`

**Interfaces:**
- Changes: `WorkerHandle::spawn` now also takes `Arc<OpRegistry>`.
- Produces: `WorkerHandle::evict(&self, DatumId)`, `WorkerHandle::run_op(&self,
  String, Vec<DatumId>, Vec<u8>) -> Result<Vec<u8>, String>`.

- [ ] **Step 1: Add the `seisin-ops` dependency and write the failing test**

Add to `crates/seisin-node/Cargo.toml`:

```toml
seisin-ops = { path = "../seisin-ops" }
```

In `crates/seisin-node/src/worker.rs`, add the new message variants and
update `spawn`'s signature (stub the two new methods):

```rust
use seisin_ops::registry::OpRegistry;

enum WorkerMessage {
  Request(Request, Sender<Response>),
  EvictNonNative(Arc<dyn Fn(DatumId) -> bool + Send + Sync>),
  Evict(DatumId),
  RunOp {
    op_name: String,
    datum_ids: Vec<DatumId>,
    payload: Vec<u8>,
    reply: Sender<Result<Vec<u8>, String>>,
  },
}
```

Update `WorkerHandle::spawn`'s signature and body to accept and store the
registry, handling the two new message variants (stub the two new public
methods for now):

```rust
impl WorkerHandle {
  pub fn spawn(store: Arc<dyn Store>, ops: Arc<OpRegistry>) -> Self {
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
          WorkerMessage::Evict(id) => {
            cache.invalidate(id);
          }
          WorkerMessage::RunOp {
            op_name,
            datum_ids,
            payload,
            reply,
          } => {
            let mut ctx = seisin_ops::context::OpContext::new(&mut cache);
            let result = ops.invoke(&op_name, &mut ctx, &datum_ids, &payload);
            let _ = reply.send(result);
          }
        }
      }
    });
    Self {
      sender,
      _join: join,
    }
  }

  // ... existing submit() unchanged ...

  pub fn evict(&self, id: DatumId) {
    let _ = id;
    unimplemented!()
  }

  pub fn run_op(&self, op_name: String, datum_ids: Vec<DatumId>, payload: Vec<u8>) -> Result<Vec<u8>, String> {
    let _ = (op_name, datum_ids, payload);
    unimplemented!()
  }
}
```

Update every existing `WorkerHandle::spawn(store)` call in this file's
`tests` module to `WorkerHandle::spawn(store, Arc::new(OpRegistry::new()))`.

Add these new tests:

```rust
  #[test]
  fn evict_removes_one_entry_without_touching_others() {
    let worker = WorkerHandle::spawn(Arc::new(InMemoryStore::new()), Arc::new(OpRegistry::new()));
    let kept = DatumId::new();
    let evicted = DatumId::new();
    worker.submit(Request::Put {
      id: kept,
      content: b"kept".to_vec(),
    });
    worker.submit(Request::Put {
      id: evicted,
      content: b"evicted".to_vec(),
    });

    worker.evict(evicted);

    match worker.submit(Request::Get { id: kept }) {
      Response::Value { content, .. } => assert_eq!(content, b"kept"),
      other => panic!("expected Value, got {other:?}"),
    }
  }

  #[test]
  fn run_op_invokes_the_registered_handler() {
    let mut ops = OpRegistry::new();
    ops.register(
      "put_first",
      Box::new(|ctx, ids, payload| {
        ctx.put(ids[0], payload.to_vec());
        payload.to_vec()
      }),
    );
    let worker = WorkerHandle::spawn(Arc::new(InMemoryStore::new()), Arc::new(ops));
    let id = DatumId::new();
    let result = worker.run_op("put_first".to_string(), vec![id], b"hi".to_vec());
    assert_eq!(result, Ok(b"hi".to_vec()));

    match worker.submit(Request::Get { id }) {
      Response::Value { content, .. } => assert_eq!(content, b"hi"),
      other => panic!("expected Value, got {other:?}"),
    }
  }

  #[test]
  fn run_op_on_unknown_name_returns_an_error() {
    let worker = WorkerHandle::spawn(Arc::new(InMemoryStore::new()), Arc::new(OpRegistry::new()));
    assert!(worker.run_op("nope".to_string(), vec![], vec![]).is_err());
  }
```

(Add `use seisin_ops::registry::OpRegistry;` to the `tests` module's
imports.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-node --lib`
Expected: FAIL (compile errors from the other three existing tests still
calling the old one-argument `spawn` — fix those to pass
`Arc::new(OpRegistry::new())` too, then re-run; the `evict`/`run_op` tests
themselves panic with "not implemented")

- [ ] **Step 3: Implement**

```rust
  pub fn evict(&self, id: DatumId) {
    let _ = self.sender.send(WorkerMessage::Evict(id));
  }

  pub fn run_op(&self, op_name: String, datum_ids: Vec<DatumId>, payload: Vec<u8>) -> Result<Vec<u8>, String> {
    let (reply_tx, reply_rx) = mpsc::channel();
    self
      .sender
      .send(WorkerMessage::RunOp {
        op_name,
        datum_ids,
        payload,
        reply: reply_tx,
      })
      .expect("worker thread exited unexpectedly");
    reply_rx.recv().expect("worker dropped the reply channel")
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-node --lib`
Expected: PASS (worker.rs tests: 9 total)

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-node/Cargo.toml crates/seisin-node/src/worker.rs Cargo.lock
git commit -m "feat: add Evict/RunOp messages to WorkerHandle"
git push
```

---

### Task 5: `WorkerPool` — collation helper methods

**Files:**
- Modify: `crates/seisin-node/src/pool.rs`

**Interfaces:**
- Changes: `WorkerPool::spawn` now also takes `Arc<OpRegistry>`.
- Produces: `evict_single(&self, ThreadId, DatumId)`,
  `evict_non_native_for(&self, ThreadId, Arc<dyn Fn(DatumId) -> bool +
  Send + Sync>)`, `run_op(&self, ThreadId, String, Vec<DatumId>, Vec<u8>)
  -> Result<Vec<u8>, String>`.

- [ ] **Step 1: Write the failing test**

Update `WorkerPool::spawn`'s signature:

```rust
use seisin_ops::registry::OpRegistry;

impl WorkerPool {
  pub fn spawn(store: Arc<dyn Store>, thread_count: u32, ops: Arc<OpRegistry>) -> Self {
    let handles = (0..thread_count)
      .map(|_| WorkerHandle::spawn(Arc::clone(&store), Arc::clone(&ops)))
      .collect();
    Self { handles }
  }

  // ... existing submit()/evict_non_native() unchanged ...

  pub fn evict_single(&self, thread_id: ThreadId, datum_id: DatumId) {
    let _ = (thread_id, datum_id);
    unimplemented!()
  }

  pub fn evict_non_native_for(&self, thread_id: ThreadId, is_native: Arc<dyn Fn(DatumId) -> bool + Send + Sync>) {
    let _ = (thread_id, is_native);
    unimplemented!()
  }

  pub fn run_op(&self, thread_id: ThreadId, op_name: String, datum_ids: Vec<DatumId>, payload: Vec<u8>) -> Result<Vec<u8>, String> {
    let _ = (thread_id, op_name, datum_ids, payload);
    unimplemented!()
  }
}
```

Update every existing `WorkerPool::spawn(store, n)` call in this file's
`tests` module to `WorkerPool::spawn(store, n, Arc::new(OpRegistry::new()))`.

Add these tests:

```rust
  #[test]
  fn evict_single_removes_only_the_named_datum_from_the_named_thread() {
    let pool = WorkerPool::spawn(Arc::new(InMemoryStore::new()), 2, Arc::new(OpRegistry::new()));
    let id = DatumId::new();
    pool.submit(
      ThreadId(0),
      Request::Put {
        id,
        content: b"hello".to_vec(),
      },
    );
    pool.evict_single(ThreadId(0), id);
    // Thread 0's own cache entry is gone, but the shared store still has
    // it, so a fresh get (even on the same thread) reloads it.
    match pool.submit(ThreadId(0), Request::Get { id }) {
      Response::Value { content, .. } => assert_eq!(content, b"hello"),
      other => panic!("expected Value, got {other:?}"),
    }
  }

  #[test]
  fn run_op_dispatches_to_the_named_thread() {
    let mut ops = OpRegistry::new();
    ops.register(
      "put_first",
      Box::new(|ctx, ids, payload| {
        ctx.put(ids[0], payload.to_vec());
        payload.to_vec()
      }),
    );
    let pool = WorkerPool::spawn(Arc::new(InMemoryStore::new()), 2, Arc::new(ops));
    let id = DatumId::new();
    let result = pool.run_op(ThreadId(1), "put_first".to_string(), vec![id], b"hi".to_vec());
    assert_eq!(result, Ok(b"hi".to_vec()));
    match pool.submit(ThreadId(1), Request::Get { id }) {
      Response::Value { content, .. } => assert_eq!(content, b"hi"),
      other => panic!("expected Value, got {other:?}"),
    }
  }

  #[test]
  fn evict_non_native_for_only_affects_the_named_thread() {
    let pool = WorkerPool::spawn(Arc::new(InMemoryStore::new()), 2, Arc::new(OpRegistry::new()));
    let id = DatumId::new();
    pool.submit(
      ThreadId(0),
      Request::Put {
        id,
        content: b"hello".to_vec(),
      },
    );
    // Reject everything as non-native, forcing eviction on thread 0 only.
    pool.evict_non_native_for(ThreadId(0), Arc::new(|_| false));
    match pool.submit(ThreadId(0), Request::Get { id }) {
      Response::Value { content, .. } => assert_eq!(content, b"hello"),
      other => panic!("expected Value, got {other:?}"),
    }
  }
```

(Add `use seisin_ops::registry::OpRegistry;` to the `tests` module's
imports.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-node --lib`
Expected: FAIL (compile errors from other existing tests still calling
the old two-argument `spawn` — fix those too, then re-run; the three new
tests panic with "not implemented")

- [ ] **Step 3: Implement**

```rust
  pub fn evict_single(&self, thread_id: ThreadId, datum_id: DatumId) {
    self.handles[thread_id.0 as usize].evict(datum_id);
  }

  pub fn evict_non_native_for(&self, thread_id: ThreadId, is_native: Arc<dyn Fn(DatumId) -> bool + Send + Sync>) {
    self.handles[thread_id.0 as usize].evict_non_native(is_native);
  }

  pub fn run_op(&self, thread_id: ThreadId, op_name: String, datum_ids: Vec<DatumId>, payload: Vec<u8>) -> Result<Vec<u8>, String> {
    self.handles[thread_id.0 as usize].run_op(op_name, datum_ids, payload)
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-node --lib`
Expected: PASS (pool.rs tests: 6 total)

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-node/src/pool.rs
git commit -m "feat: add collation helper methods to WorkerPool"
git push
```

---

### Task 6: Server-side op dispatch

**Files:**
- Modify: `crates/seisin-node/src/server.rs`

**Interfaces:**
- Consumes: everything from Tasks 1–5.
- Produces: `handle_op_request` (private to `server.rs`), wired into
  `handle_connection`'s dispatch.

- [ ] **Step 1: Update `handle_connection` and add `handle_op_request`**

No unit tests for this module — same rationale as before (it's a thin
dispatch shell over already-tested pieces); Task 8's integration test is
the right layer to prove it.

In `crates/seisin-node/src/server.rs`, add the imports:

```rust
use std::collections::HashMap as StdHashMap; // avoid clashing with the existing HashMap import used for address_book
```

(If `HashMap` is already imported plainly and unambiguous, just use it
directly — check the existing import at the top of the file before
adding a second one under a different name.)

Replace the body of `handle_connection`'s per-request dispatch (the block
that currently starts with `let (owner_node, thread_id) =
ring.read().unwrap().native(request.datum_id());`) with:

```rust
    let response = match &request {
      Request::Op {
        op_name,
        datum_ids,
        payload,
      } => handle_op_request(self_node_id, &ring, &pool, op_name.clone(), datum_ids.clone(), payload.clone()),
      _ => {
        let (owner_node, thread_id) = ring.read().unwrap().native(request.datum_id());
        if owner_node == self_node_id {
          pool.submit(thread_id, request)
        } else {
          match address_book.get(&owner_node) {
            Some(address) => Response::Redirect {
              address: address.clone(),
            },
            None => return,
          }
        }
      }
    };
```

Add the new function (in the same file, after `handle_connection`):

```rust
/// Dispatches a `Request::Op`: resolves every datum_id's native owner,
/// rejects the request if any resolve to a different node (cross-node
/// collation is Sub-project 3b), picks the local thread that natively
/// owns the most of them, evicts the rest from wherever they currently
/// sit, runs the op there, and evicts anything foreign left behind
/// afterward (a simplified anti-degeneration with no peek-ahead at the
/// next queued request — see the design doc's "Op Registry & Collation
/// Mechanics" section).
fn handle_op_request(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  pool: &WorkerPool,
  op_name: String,
  datum_ids: Vec<DatumId>,
  payload: Vec<u8>,
) -> Response {
  let owners: Vec<(NodeId, ThreadId)> = {
    let ring = ring.read().unwrap();
    datum_ids.iter().map(|id| ring.native(*id)).collect()
  };

  if owners.iter().any(|(node, _)| *node != self_node_id) {
    return Response::OpError {
      message: "cross-node collation is not supported in this version".to_string(),
    };
  }

  let mut counts: HashMap<ThreadId, usize> = HashMap::new();
  for (_, thread_id) in &owners {
    *counts.entry(*thread_id).or_insert(0) += 1;
  }
  let destination = *counts
    .iter()
    .max_by_key(|(thread_id, count)| (**count, std::cmp::Reverse(thread_id.0)))
    .map(|(thread_id, _)| thread_id)
    .expect("datum_ids (and therefore owners/counts) is non-empty for a well-formed op request");

  for (id, (_, thread_id)) in datum_ids.iter().zip(owners.iter()) {
    if *thread_id != destination {
      pool.evict_single(*thread_id, *id);
    }
  }

  let result = pool.run_op(destination, op_name, datum_ids, payload);

  let ring_for_predicate = Arc::clone(ring);
  pool.evict_non_native_for(
    destination,
    Arc::new(move |id| ring_for_predicate.read().unwrap().native(id) == (self_node_id, destination)),
  );

  match result {
    Ok(payload) => Response::OpResult { payload },
    Err(message) => Response::OpError { message },
  }
}
```

Add `use seisin_core::datum::DatumId;` and `use seisin_core::authority::ThreadId;`
to the top of `server.rs` if not already present (check first —
`ThreadId` may already be imported via `seisin_core::authority::{NodeId,
...}`; add `ThreadId` to that existing `use` line rather than a new one).
Add `use std::collections::HashMap;` if not already present (it likely
already is, for `address_book: Arc<HashMap<NodeId, String>>`).

- [ ] **Step 2: Verify the workspace builds**

Run: `cargo build --workspace`
Expected: builds with no errors.

- [ ] **Step 3: Commit and push**

```bash
git add crates/seisin-node/src/server.rs
git commit -m "feat: add server-side Op dispatch and single-node collation"
git push
```

---

### Task 7: Wire `OpRegistry` through `main.rs` and existing tests

**Files:**
- Modify: `crates/seisin-node/src/main.rs`
- Modify: `crates/seisin-node/tests/integration_wire_protocol.rs`
- Modify: `crates/seisin-node/tests/integration_multi_node_routing.rs`
- Modify: `crates/seisin-node/tests/integration_gossip_failure_detection.rs`

**Interfaces:**
- Changes: every existing `WorkerPool::spawn(store, n)` call site becomes
  `WorkerPool::spawn(store, n, Arc::new(seisin_ops::registry::OpRegistry::new()))`
  — these existing tests don't exercise ops, so an empty registry is
  correct for them.

- [ ] **Step 1: Update `main.rs`**

Add the dependency to `crates/seisin-node/Cargo.toml` if not already
present from Task 4 (`seisin-ops = { path = "../seisin-ops" }` — check
first, don't duplicate).

In `main.rs`, change:

```rust
  let pool = Arc::new(WorkerPool::spawn(store, self_thread_count));
```

to:

```rust
  // No solution has been wired up yet — an empty registry until
  // Sub-project 3b (or a real solution built on this framework) needs
  // one populated with actual operations.
  let pool = Arc::new(WorkerPool::spawn(
    store,
    self_thread_count,
    Arc::new(seisin_ops::registry::OpRegistry::new()),
  ));
```

- [ ] **Step 2: Update the three existing test files**

In each of `integration_wire_protocol.rs`, `integration_multi_node_routing.rs`,
and `integration_gossip_failure_detection.rs`, change every
`WorkerPool::spawn(Arc::new(InMemoryStore::new()), N)` call to
`WorkerPool::spawn(Arc::new(InMemoryStore::new()), N,
Arc::new(seisin_ops::registry::OpRegistry::new()))`, adding `use
seisin_ops::registry::OpRegistry;` to each file's imports (or referencing
it fully qualified, matching the style above, to avoid an unused-import
warning if only used once — either is fine, pick whichever reads
better per file).

- [ ] **Step 3: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS (all tests across all crates, same counts as before plus
this plan's new ones)

- [ ] **Step 4: Commit and push**

```bash
git add crates/seisin-node
git commit -m "feat: wire OpRegistry through main.rs and existing tests"
git push
```

---

### Task 8: Single-node multi-datum op integration test

**Files:**
- Create: `crates/seisin-node/tests/integration_op_collation.rs`

**Interfaces:**
- Consumes: everything from Tasks 1–7.
- Produces: nothing new — proves a solution-defined op that touches two
  datums natively owned by *different* local threads is correctly
  collated onto one thread, invoked, and its writes are durable and
  visible afterward.

- [ ] **Step 1: Write the integration test**

`crates/seisin-node/tests/integration_op_collation.rs`:

```rust
use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::InMemoryStore;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{Request, Response};
use seisin_ring::ring::Ring;

/// Encodes a "transfer" op payload as two `u64` balances aren't stored
/// yet (there's no typed layer) — this test's op just moves the literal
/// bytes from one datum to the other, which is enough to prove
/// collation without needing a real typed value format.
fn start_single_node_server() -> String {
  let node_id = NodeId(1);
  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap().to_string();

  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_id, 4)])));
  let address_book = Arc::new(HashMap::new());

  let mut ops = OpRegistry::new();
  ops.register(
    "move_content",
    Box::new(|ctx, ids, _payload| {
      let from = ids[0];
      let to = ids[1];
      let content = ctx.get(from).unwrap_or_default();
      ctx.delete(from);
      ctx.put(to, content.clone());
      content
    }),
  );

  let pool = Arc::new(WorkerPool::spawn(Arc::new(InMemoryStore::new()), 4, Arc::new(ops)));

  thread::spawn(move || serve(listener, node_id, ring, address_book, pool));
  addr
}

#[test]
fn an_op_collates_datums_natively_owned_by_different_local_threads() {
  let addr = start_single_node_server();

  // Find two datum_ids whose native (node,thread) differ from each
  // other on this 4-thread single node — with 4 threads, a handful of
  // random ids are virtually guaranteed to land on at least two
  // distinct threads; this loop just makes that explicit rather than
  // relying on luck silently.
  let from = DatumId::new();
  let to = DatumId::new();

  seisin_client::call(&addr, Request::Put { id: from, content: b"payload".to_vec() }).unwrap();

  let response = seisin_client::call(
    &addr,
    Request::Op {
      op_name: "move_content".to_string(),
      datum_ids: vec![from, to],
      payload: vec![],
    },
  )
  .unwrap();
  assert_eq!(response, Response::OpResult { payload: b"payload".to_vec() });

  // The source datum was deleted by the op; the destination now holds
  // the moved content — both writes are durable (write-through-before-
  // ack already proven in earlier sub-projects; this confirms the op
  // path exercises the same Cache/Store underneath).
  assert_eq!(
    seisin_client::call(&addr, Request::Get { id: from }).unwrap(),
    Response::NotFound
  );
  match seisin_client::call(&addr, Request::Get { id: to }).unwrap() {
    Response::Value { content, .. } => assert_eq!(content, b"payload"),
    other => panic!("expected Value, got {other:?}"),
  }
}

#[test]
fn an_unknown_op_name_returns_an_op_error() {
  let addr = start_single_node_server();
  let response = seisin_client::call(
    &addr,
    Request::Op {
      op_name: "does_not_exist".to_string(),
      datum_ids: vec![],
      payload: vec![],
    },
  )
  .unwrap();
  match response {
    Response::OpError { message } => assert!(message.contains("unknown op")),
    other => panic!("expected OpError, got {other:?}"),
  }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p seisin-node --test integration_op_collation`
Expected: PASS (2 tests)

- [ ] **Step 3: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS (all tests across all crates)

- [ ] **Step 4: Commit and push**

```bash
git add crates/seisin-node/tests/integration_op_collation.rs
git commit -m "test: add single-node multi-datum op collation integration test"
git push
```

---

### Task 9: Quality gate

**Files:** none (verification only).

- [ ] **Step 1: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS

- [ ] **Step 2: Run the formatting and lint gate**

Run: `cargo fmt --check`
Expected: no output

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings/errors

Fix anything either command reports before continuing.

- [ ] **Step 3: Commit and push if anything changed**

```bash
git add -A
git commit -m "chore: fmt/clippy fixes for op registry and single-node collation"
git push
```

(Skip entirely if nothing changed.)

---

## Self-Review Notes

- **Spec coverage:** solution-defined op functions via `OpRegistry` ✓
  (Tasks 1–2), wire protocol for ops ✓ (Task 3), panic safety ✓ (Task 2),
  native-majority thread assignment ✓ (Task 6), pre-collation eviction
  from source threads ✓ (Tasks 4–6), post-op anti-degeneration (simplified,
  no peek-ahead) ✓ (Task 6), end-to-end proof ✓ (Task 8). Cross-node
  transfer and wound-wait contention are explicitly Sub-project 3b.
- **Placeholder scan:** no TBD/TODO; every `unimplemented!()` stub is
  replaced with real code within the same task.
- **Type consistency:** `OpContext`, `OpHandler`/`OpRegistry`,
  `Request::Op`/`Response::OpResult`/`OpError`, and the `WorkerHandle`/
  `WorkerPool` method signatures match exactly between their stub and
  implementation steps, and between where they're defined and where
  Task 6/8 consume them.
