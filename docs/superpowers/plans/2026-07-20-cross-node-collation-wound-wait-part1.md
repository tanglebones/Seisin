# Sub-project 3b, Part 1: Wire Unification & Same-Node Wound-Wait Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Retire `Get`/`Put`/`Delete` as wire variants in favor of a single `Request::Op` shape carrying a client-generated `op_id`, and implement the full acquire/wound-wait mechanics (native-home lock manager, recall, wait-queue) restricted to contention *within one node's threads* — cross-node acquisition stays explicitly rejected, exactly as 3a left it, until Part 2 adds the peer-link connection layer.

**Architecture:** Every datum's native-home worker thread becomes the sole lock manager for it (`NativeLock`, in a new `collation.rs`) — no separate coordinator, no distributed holder bookkeeping. Each worker thread also now tracks in-flight op records (`op_id -> { still_needed, acquired, reply }`) and drives its own collation by sending itself and its local peers `Acquire`/`Recall`/`Release` messages, all non-blocking (plain `mpsc::Sender::send`, no thread ever waits on another). Op-to-thread assignment happens once, per op, in `WorkerPool` (native-majority heuristic, same as 3a).

**Tech Stack:** Rust, `std::sync::mpsc`, existing `seisin-core`/`seisin-protocol`/`seisin-ops`/`seisin-ring` crates.

## Global Constraints

- 2-space indentation (existing repo convention).
- `#![deny(warnings)]` at `seisin-node`'s crate root — expect to remove it temporarily starting Task 3 (the workspace will not fully compile again until Task 5, matching the precedent already set in 3a's Task 4-7 arc) and restore it once Task 5 lands.
- Commit and push after every task (per standing project instruction — durability in case of lost machine access).
- Update `docs/superpowers/PROGRESS.md` once this plan completes.
- No cross-node acquisition in this plan — `handle_op_request` rejects (via `OpError`) any op whose datum_ids resolve to more than one distinct native node. That's Part 2's job.

---

### Task 1: Retire Get/Put/Delete from the wire protocol; add op_id to Op

**Files:**
- Modify: `crates/seisin-protocol/src/lib.rs`
- Modify: `crates/seisin-core/src/sk.rs:3-7` (doc comment only, no code change — it references "same Get/Put/Delete path," which needs a small wording update since those variants are gone)

**Interfaces:**
- Produces: `Request::Op { op_id: DatumId, op_name: String, datum_ids: Vec<DatumId>, payload: Vec<u8> }` — the *only* `Request` variant. `Response` keeps only `Redirect`, `OpResult`, `OpError`.
- Removes: `Request::Get`/`Put`/`Delete`, `Request::datum_id()`, `Response::Value`/`NotFound`/`Ok`.

This task breaks every downstream crate that references the removed variants (`seisin-node`, `seisin-client`'s tests, all `seisin-node` integration tests) — expected; they're fixed in later tasks. `seisin-protocol` itself compiles and passes its own tests standalone.

- [ ] **Step 1: Rewrite `crates/seisin-protocol/src/lib.rs`**

Replace the entire file with:

```rust
//! The custom binary wire protocol between clients and compute nodes (and,
//! in later sub-projects, between nodes themselves). No auth/encryption
//! layer by design — the system trusts the network boundary.
//!
//! Every request is an `Op` — a plain read or write is just a trivially-
//! registered op, no different in kind from an arbitrary domain op (see
//! the design doc's "Why Get/Put/Delete Disappear as Wire Variants"
//! section). Every op carries a client-generated `op_id`, the ordering
//! token wound-wait collation (a later sub-project) uses to resolve
//! contention between two ops' collation attempts.

use std::io::{self, Read, Write};

use anyhow::{bail, Context, Result};

use seisin_core::datum::DatumId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
  Op {
    op_id: DatumId,
    op_name: String,
    datum_ids: Vec<DatumId>,
    payload: Vec<u8>,
  },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
  Redirect { address: String },
  OpResult { payload: Vec<u8> },
  OpError { message: String },
}

const OP_OP: u8 = 1;

const RESP_REDIRECT: u8 = 1;
const RESP_OP_RESULT: u8 = 2;
const RESP_OP_ERROR: u8 = 3;

const ID_LEN: usize = 16;

pub fn encode_request(req: &Request) -> Vec<u8> {
  let mut buf = Vec::new();
  match req {
    Request::Op {
      op_id,
      op_name,
      datum_ids,
      payload,
    } => {
      buf.push(OP_OP);
      buf.extend_from_slice(&op_id.as_bytes());
      let name_bytes = op_name.as_bytes();
      buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
      buf.extend_from_slice(name_bytes);
      buf.extend_from_slice(&(datum_ids.len() as u32).to_le_bytes());
      for id in datum_ids {
        buf.extend_from_slice(&id.as_bytes());
      }
      buf.extend_from_slice(payload);
    }
  }
  buf
}

pub fn decode_request(buf: &[u8]) -> Result<Request> {
  if buf.is_empty() {
    bail!("empty request payload");
  }
  match buf[0] {
    OP_OP => decode_op_request(buf),
    op => bail!("unknown request opcode: {op}"),
  }
}

fn decode_op_request(buf: &[u8]) -> Result<Request> {
  if buf.len() < 1 + ID_LEN {
    bail!("op request too short for an op_id: {} bytes", buf.len());
  }
  let op_id = DatumId::from_bytes(buf[1..1 + ID_LEN].try_into().unwrap());
  let mut offset = 1 + ID_LEN;

  if buf.len() < offset + 4 {
    bail!("op request too short for a name length");
  }
  let name_len = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
  offset += 4;
  if buf.len() < offset + name_len {
    bail!("op request too short for its name: expected {name_len} bytes");
  }
  let op_name = String::from_utf8(buf[offset..offset + name_len].to_vec())
    .context("op name was not valid utf8")?;
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
  Ok(Request::Op {
    op_id,
    op_name,
    datum_ids,
    payload,
  })
}

pub fn encode_response(resp: &Response) -> Vec<u8> {
  let mut buf = Vec::new();
  match resp {
    Response::Redirect { address } => {
      buf.push(RESP_REDIRECT);
      let addr_bytes = address.as_bytes();
      buf.extend_from_slice(&(addr_bytes.len() as u32).to_le_bytes());
      buf.extend_from_slice(addr_bytes);
    }
    Response::OpResult { payload } => {
      buf.push(RESP_OP_RESULT);
      buf.extend_from_slice(payload);
    }
    Response::OpError { message } => {
      buf.push(RESP_OP_ERROR);
      buf.extend_from_slice(message.as_bytes());
    }
  }
  buf
}

pub fn decode_response(buf: &[u8]) -> Result<Response> {
  if buf.is_empty() {
    bail!("empty response payload");
  }
  match buf[0] {
    RESP_REDIRECT => {
      if buf.len() < 5 {
        bail!(
          "redirect response too short for an address length: {} bytes",
          buf.len()
        );
      }
      let addr_len = u32::from_le_bytes(buf[1..5].try_into().unwrap()) as usize;
      if buf.len() != 5 + addr_len {
        bail!(
          "redirect response length mismatch: expected {} bytes, got {}",
          5 + addr_len,
          buf.len()
        );
      }
      let address =
        String::from_utf8(buf[5..].to_vec()).context("redirect address was not valid utf8")?;
      Ok(Response::Redirect { address })
    }
    RESP_OP_RESULT => Ok(Response::OpResult {
      payload: buf[1..].to_vec(),
    }),
    RESP_OP_ERROR => {
      let message =
        String::from_utf8(buf[1..].to_vec()).context("op error message was not valid utf8")?;
      Ok(Response::OpError { message })
    }
    op => bail!("unknown response opcode: {op}"),
  }
}

/// Writes a length-prefixed frame: a 4-byte little-endian length followed
/// by `payload`.
pub fn write_frame<W: Write>(w: &mut W, payload: &[u8]) -> io::Result<()> {
  let len = u32::try_from(payload.len()).map_err(|_| {
    io::Error::new(
      io::ErrorKind::InvalidInput,
      "payload too large for a u32 frame length",
    )
  })?;
  w.write_all(&len.to_le_bytes())?;
  w.write_all(payload)?;
  w.flush()
}

/// Frames larger than this are rejected outright rather than allocated —
/// caps how much memory a single malformed or malicious length prefix can
/// make a connection handler allocate before any content is even read.
pub const MAX_FRAME_LEN: u32 = 64 * 1024 * 1024;

/// Reads a single length-prefixed frame written by `write_frame`.
pub fn read_frame<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
  let mut len_buf = [0u8; 4];
  r.read_exact(&mut len_buf)?;
  let len = u32::from_le_bytes(len_buf);
  if len > MAX_FRAME_LEN {
    return Err(io::Error::new(
      io::ErrorKind::InvalidData,
      format!("frame length {len} exceeds MAX_FRAME_LEN ({MAX_FRAME_LEN})"),
    ));
  }
  let mut payload = vec![0u8; len as usize];
  r.read_exact(&mut payload)?;
  Ok(payload)
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::io::Cursor;

  #[test]
  fn round_trips_op_request_with_no_datum_ids() {
    let req = Request::Op {
      op_id: DatumId::new(),
      op_name: "noop".to_string(),
      datum_ids: vec![],
      payload: vec![],
    };
    assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
  }

  #[test]
  fn round_trips_op_request_with_datum_ids_and_payload() {
    let req = Request::Op {
      op_id: DatumId::new(),
      op_name: "transfer".to_string(),
      datum_ids: vec![DatumId::new(), DatumId::new()],
      payload: b"amount:100".to_vec(),
    };
    assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
  }

  #[test]
  fn rejects_unknown_request_opcode() {
    let mut buf = encode_request(&Request::Op {
      op_id: DatumId::new(),
      op_name: "noop".to_string(),
      datum_ids: vec![],
      payload: vec![],
    });
    buf[0] = 99;
    assert!(decode_request(&buf).is_err());
  }

  #[test]
  fn round_trips_redirect_response() {
    let resp = Response::Redirect {
      address: "127.0.0.1:7879".to_string(),
    };
    assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
  }

  #[test]
  fn rejects_redirect_with_invalid_utf8_address() {
    let mut buf = encode_response(&Response::Redirect {
      address: "x".to_string(),
    });
    // Corrupt the one address byte into an invalid UTF-8 continuation byte.
    *buf.last_mut().unwrap() = 0x80;
    assert!(decode_response(&buf).is_err());
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
  fn frame_round_trips_over_a_buffer() {
    let mut buf = Vec::new();
    write_frame(&mut buf, b"payload bytes").unwrap();
    let mut cursor = Cursor::new(buf);
    assert_eq!(read_frame(&mut cursor).unwrap(), b"payload bytes");
  }

  #[test]
  fn rejects_a_frame_length_over_the_max() {
    let oversized_len = MAX_FRAME_LEN + 1;
    let mut cursor = Cursor::new(oversized_len.to_le_bytes().to_vec());
    let err = read_frame(&mut cursor).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
  }
}
```

- [ ] **Step 2: Update the doc comment in `crates/seisin-core/src/sk.rs`**

Change the top-of-file comment's second line from:
```rust
//! A secondary-key datum (e.g. `sk:user.name:cliff`) is a regular datum —
//! same Get/Put/Delete path as any primary-key datum — whose content
```
to:
```rust
//! A secondary-key datum (e.g. `sk:user.name:cliff`) is a regular datum —
//! same collate-then-run op path as any primary-key datum — whose content
```

- [ ] **Step 3: Run `seisin-protocol`'s own tests**

Run: `cargo test -p seisin-protocol`
Expected: PASS (all tests in this crate). Every other crate in the workspace will fail to *compile* at this point — that's expected and fixed by Task 5.

- [ ] **Step 4: Commit and push**

```bash
git add crates/seisin-protocol/src/lib.rs crates/seisin-core/src/sk.rs
git commit -m "feat: retire Get/Put/Delete wire variants, add op_id to Request::Op"
git push
```

---

### Task 2: `NativeLock` — the native-home lock manager

**Files:**
- Create: `crates/seisin-node/src/collation.rs`
- Modify: `crates/seisin-node/src/lib.rs` (add `pub mod collation;`)
- Modify: `crates/seisin-core/src/datum.rs` (derive ordering on `DatumId`)

**Interfaces:**
- Produces: `NativeLock::new()`, `NativeLock::request(op_id, node_id, thread_id, on_granted) -> AcquireOutcome`, `NativeLock::release()`, `NativeLock::current_holder() -> Option<&Holder>`. `AcquireOutcome::{GrantedImmediately, RecallNeeded(Holder), Queued}`. `Holder { node_id, thread_id, op_id }`.
- Consumes: `seisin_core::authority::{NodeId, ThreadId}`, `seisin_core::datum::DatumId` (now `Ord`).

This is a pure data structure — no threads, no channels beyond a caller-supplied closure. It compiles and its tests pass fully standalone, independent of Task 1's breakage elsewhere in the workspace (this crate, `seisin-node`, won't build as a whole yet, but this module's unit tests don't need the rest of the crate to build once run via a later task — see the note in Step 5).

- [ ] **Step 1: Derive ordering on `DatumId`**

In `crates/seisin-core/src/datum.rs`, change:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DatumId(Uuid);
```
to:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatumId(Uuid);
```

Add this test to `datum.rs`'s existing `mod tests`:
```rust
  #[test]
  fn ordering_matches_creation_order() {
    let first = DatumId::new();
    let second = DatumId::new();
    assert!(first < second, "a later-created UUIDv7 id must sort greater");
  }
```

Run: `cargo test -p seisin-core`
Expected: PASS (all tests, including the new one).

- [ ] **Step 2: Write `collation.rs`'s failing tests first**

Create `crates/seisin-node/src/collation.rs`:

```rust
//! The native-home lock manager for a single datum: tracks who currently
//! holds it and, when contended, resolves priority via the client-
//! generated op_id (older always wins) — see the design doc's "Acquire &
//! Wound-Wait Mechanics" section. A pure data structure with no
//! threading/network concerns of its own; `worker.rs` drives it from
//! inbox messages, translating a grant into whatever message it needs to
//! send via the `on_granted` callback passed into `request`.

use std::collections::VecDeque;

use seisin_core::authority::{NodeId, ThreadId};
use seisin_core::datum::DatumId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Holder {
  pub node_id: NodeId,
  pub thread_id: ThreadId,
  pub op_id: DatumId,
}

struct Waiter {
  op_id: DatumId,
  node_id: NodeId,
  thread_id: ThreadId,
  on_granted: Box<dyn FnOnce() + Send>,
}

/// What the caller must do next, as a result of a `request()` call.
#[derive(Debug, PartialEq, Eq)]
pub enum AcquireOutcome {
  /// No one held it; the requester is now the holder (its `on_granted`
  /// callback has already fired).
  GrantedImmediately,
  /// The requester is older than the current holder — the caller must
  /// send a recall to `Holder`. The requester's `on_granted` callback
  /// fires later, once `release()` is called after the recall completes.
  RecallNeeded(Holder),
  /// The requester is younger than the current holder; it's queued.
  /// Its `on_granted` callback fires once granted, with no polling
  /// needed.
  Queued,
}

#[derive(Default)]
pub struct NativeLock {
  current_holder: Option<Holder>,
  waiters: VecDeque<Waiter>,
}

impl NativeLock {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn current_holder(&self) -> Option<&Holder> {
    self.current_holder.as_ref()
  }

  /// Requests the lock on behalf of `op_id`. Always enqueues the
  /// request in ascending op_id order (a younger request can arrive
  /// after an older one is already queued — e.g. a third op joining
  /// after a recall), then decides what the caller must do next.
  pub fn request(
    &mut self,
    op_id: DatumId,
    node_id: NodeId,
    thread_id: ThreadId,
    on_granted: Box<dyn FnOnce() + Send>,
  ) -> AcquireOutcome {
    let needs_recall = self
      .current_holder
      .as_ref()
      .is_some_and(|h| op_id < h.op_id);
    let insert_at = self
      .waiters
      .iter()
      .position(|w| op_id < w.op_id)
      .unwrap_or(self.waiters.len());
    self.waiters.insert(
      insert_at,
      Waiter {
        op_id,
        node_id,
        thread_id,
        on_granted,
      },
    );
    if self.current_holder.is_none() {
      self.grant_front();
      return AcquireOutcome::GrantedImmediately;
    }
    if needs_recall {
      return AcquireOutcome::RecallNeeded(self.current_holder.clone().unwrap());
    }
    AcquireOutcome::Queued
  }

  /// Releases the current holder (its op finished, or it was recalled
  /// and acknowledged) and grants the datum to the oldest waiter, if
  /// any.
  pub fn release(&mut self) {
    self.current_holder = None;
    self.grant_front();
  }

  fn grant_front(&mut self) {
    if let Some(waiter) = self.waiters.pop_front() {
      self.current_holder = Some(Holder {
        node_id: waiter.node_id,
        thread_id: waiter.thread_id,
        op_id: waiter.op_id,
      });
      (waiter.on_granted)();
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::mpsc;

  #[test]
  fn first_request_on_an_idle_lock_is_granted_immediately() {
    let mut lock = NativeLock::new();
    let (tx, rx) = mpsc::channel();
    let op_id = DatumId::new();
    let outcome = lock.request(
      op_id,
      NodeId(1),
      ThreadId(0),
      Box::new(move || {
        let _ = tx.send(());
      }),
    );
    assert_eq!(outcome, AcquireOutcome::GrantedImmediately);
    assert!(rx.try_recv().is_ok());
    assert_eq!(
      lock.current_holder(),
      Some(&Holder {
        node_id: NodeId(1),
        thread_id: ThreadId(0),
        op_id
      })
    );
  }

  #[test]
  fn a_younger_request_against_a_held_lock_is_queued_without_firing() {
    let mut lock = NativeLock::new();
    let (tx1, _rx1) = mpsc::channel();
    let older = DatumId::new();
    lock.request(
      older,
      NodeId(1),
      ThreadId(0),
      Box::new(move || {
        let _ = tx1.send(());
      }),
    );

    let (tx2, rx2) = mpsc::channel();
    let younger = DatumId::new(); // created after `older`, so it sorts greater
    let outcome = lock.request(
      younger,
      NodeId(1),
      ThreadId(1),
      Box::new(move || {
        let _ = tx2.send(());
      }),
    );
    assert_eq!(outcome, AcquireOutcome::Queued);
    assert!(rx2.try_recv().is_err());
  }

  #[test]
  fn an_older_request_against_a_held_lock_needs_a_recall() {
    let mut lock = NativeLock::new();
    let a = DatumId::new();
    let b = DatumId::new();
    let (older, younger) = if a < b { (a, b) } else { (b, a) };

    let (tx_holder, _rx_holder) = mpsc::channel();
    lock.request(
      younger,
      NodeId(1),
      ThreadId(0),
      Box::new(move || {
        let _ = tx_holder.send(());
      }),
    );

    let (tx_requester, _rx_requester) = mpsc::channel();
    let outcome = lock.request(
      older,
      NodeId(2),
      ThreadId(3),
      Box::new(move || {
        let _ = tx_requester.send(());
      }),
    );
    match outcome {
      AcquireOutcome::RecallNeeded(holder) => assert_eq!(holder.op_id, younger),
      other => panic!("expected RecallNeeded, got {other:?}"),
    }
  }

  #[test]
  fn release_grants_to_the_oldest_queued_waiter_even_if_it_arrived_second() {
    let mut lock = NativeLock::new();
    let holder_id = DatumId::new();
    let (tx_holder, _rx_holder) = mpsc::channel();
    lock.request(
      holder_id,
      NodeId(1),
      ThreadId(0),
      Box::new(move || {
        let _ = tx_holder.send(());
      }),
    );

    let a = DatumId::new();
    let b = DatumId::new();
    let (first_waiter, second_waiter) = if a < b { (a, b) } else { (b, a) };
    let (tx1, rx1) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();
    // Insert the younger of the two waiters first, to prove granting
    // order is by op_id, not arrival order.
    lock.request(
      second_waiter,
      NodeId(1),
      ThreadId(1),
      Box::new(move || {
        let _ = tx2.send(());
      }),
    );
    lock.request(
      first_waiter,
      NodeId(1),
      ThreadId(2),
      Box::new(move || {
        let _ = tx1.send(());
      }),
    );

    lock.release();
    assert!(
      rx1.try_recv().is_ok(),
      "the oldest waiter should be granted first"
    );
    assert!(
      rx2.try_recv().is_err(),
      "the second-oldest waiter should still be waiting"
    );
    assert_eq!(
      lock.current_holder(),
      Some(&Holder {
        node_id: NodeId(1),
        thread_id: ThreadId(2),
        op_id: first_waiter
      })
    );
  }

  #[test]
  fn release_on_an_idle_lock_is_a_no_op() {
    let mut lock = NativeLock::new();
    lock.release();
    assert_eq!(lock.current_holder(), None);
  }
}
```

- [ ] **Step 3: Wire the new module into the crate**

In `crates/seisin-node/src/lib.rs`, add `collation` to the module list (keep alphabetical with the rest):
```rust
#![deny(warnings)]

pub mod collation;
pub mod config;
pub mod gossip_client;
pub mod gossip_server;
pub mod gossip_state;
pub mod pool;
pub mod server;
pub mod worker;
```

- [ ] **Step 4: Run `collation.rs`'s tests**

Run: `cargo test -p seisin-node --lib collation::`
Expected: this will *fail to compile* right now, because `pool.rs`/`server.rs`/`worker.rs` still reference the retired `Request::Get`/`Put`/`Delete` and old `WorkerHandle`/`WorkerPool` signatures from Task 1's breakage — the whole `seisin-node` lib target must compile before any of its tests can run. This is expected; proceed to Task 3, which starts resolving it. Once Task 5 restores full compilation, come back and confirm this specific test module passes (folded into Task 5's final verification step) — don't skip re-checking it then.

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-core/src/datum.rs crates/seisin-node/src/collation.rs crates/seisin-node/src/lib.rs
git commit -m "feat: add NativeLock, the native-home wound-wait lock manager"
git push
```

---

### Task 3: `worker.rs` — full rework for acquire/wound-wait + op collation

**Files:**
- Modify: `crates/seisin-node/src/worker.rs` (full rewrite)

**Interfaces:**
- Consumes: `NativeLock`/`AcquireOutcome`/`Holder` from Task 2; `Ring::native` from `seisin-ring`; `OpRegistry`/`OpContext` from `seisin-ops` (unchanged); `Request::Op`/`Response` from Task 1.
- Produces: `WorkerHandle::spawn(thread_id: ThreadId, receiver: Receiver<WorkerMessage>, sender: Sender<WorkerMessage>, peers: Arc<Vec<Sender<WorkerMessage>>>, store: Arc<dyn Store>, ops: Arc<OpRegistry>, ring: Arc<RwLock<Ring>>, self_node_id: NodeId) -> Self`, `WorkerHandle::run_op(op_id: DatumId, op_name: String, datum_ids: Vec<DatumId>, payload: Vec<u8>) -> Result<Vec<u8>, String>`, `WorkerHandle::evict_non_native(is_native: Arc<dyn Fn(DatumId) -> bool + Send + Sync>)` (unchanged from 3a). `pub(crate) enum WorkerMessage` (see below) — `pub(crate)` since only `pool.rs` in this crate needs to construct the channel pair Task 4 threads through.

This is the largest task in the plan. The workspace still won't fully compile after this task (`pool.rs`, `server.rs`, `main.rs`, and the integration tests still call the old APIs) — that's resolved across Tasks 4-5. Remove `#![deny(warnings)]` from `crates/seisin-node/src/lib.rs` for now (restore it in Task 5).

- [ ] **Step 1: Remove `#![deny(warnings)]` temporarily**

In `crates/seisin-node/src/lib.rs`, remove the `#![deny(warnings)]` line (keep the rest of the file as Task 2 left it).

- [ ] **Step 2: Rewrite `crates/seisin-node/src/worker.rs`**

Replace the entire file with:

```rust
//! One owning thread per `ThreadId`. Each thread is the sole lock
//! manager (via `NativeLock`) for every datum whose `ring.native()`
//! resolves here, and independently tracks its own in-flight op records
//! (op_id -> still-needed/acquired datum_ids) for ops assigned to it.
//! All cross-thread coordination is non-blocking message passing — no
//! thread ever blocks waiting on another; a request that can't be
//! granted immediately is queued at the native-home thread and the
//! requester is notified later via an ordinary inbox message. See the
//! design doc's "Node/Thread Architecture" section.

use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};

use seisin_core::authority::{NodeId, ThreadId};
use seisin_core::cache::Cache;
use seisin_core::datum::DatumId;
use seisin_core::store::Store;
use seisin_ops::context::OpContext;
use seisin_ops::registry::OpRegistry;
use seisin_ring::ring::Ring;

use crate::collation::{AcquireOutcome, NativeLock};

pub(crate) enum WorkerMessage {
  /// A client's `Request::Op`, assigned to this thread as its
  /// destination (see `pool.rs`'s native-majority heuristic).
  RunOp {
    op_id: DatumId,
    op_name: String,
    datum_ids: Vec<DatumId>,
    payload: Vec<u8>,
    reply: Sender<Result<Vec<u8>, String>>,
  },
  /// Sent to whichever thread is native home for `datum_id`, requesting
  /// the lock on behalf of `op_id`. `requester_inbox` is where the
  /// grant (`AcquireGranted`) is posted once available — always a
  /// local `WorkerMessage` sender in this plan (cross-node requesters
  /// are a later sub-project).
  Acquire {
    op_id: DatumId,
    datum_id: DatumId,
    requester_node: NodeId,
    requester_thread: ThreadId,
    requester_inbox: Sender<WorkerMessage>,
  },
  /// Posted into the requesting thread's own inbox once its `Acquire`
  /// is granted (immediately, or after a wait/recall).
  AcquireGranted { op_id: DatumId, datum_id: DatumId },
  /// Sent by native home to whoever currently holds `datum_id`, asking
  /// it to evict and release. `native_home_inbox` is where the
  /// resulting `Release` is sent once done.
  Recall {
    datum_id: DatumId,
    requesting_op_id: DatumId,
    native_home_inbox: Sender<WorkerMessage>,
  },
  /// Tells native home that whoever held `datum_id` is done with it
  /// (normal op completion, or a recall's evict-and-ack) — grants it to
  /// the oldest waiter, if any.
  Release { datum_id: DatumId },
  /// Evicts every cached entry `is_native` rejects — used after a ring
  /// mutation; unrelated to op collation.
  EvictNonNative(Arc<dyn Fn(DatumId) -> bool + Send + Sync>),
}

struct OpRecord {
  op_name: String,
  payload: Vec<u8>,
  still_needed: Vec<DatumId>,
  acquired: Vec<DatumId>,
  reply: Sender<Result<Vec<u8>, String>>,
}

pub struct WorkerHandle {
  sender: Sender<WorkerMessage>,
  _join: JoinHandle<()>,
}

impl WorkerHandle {
  #[allow(clippy::too_many_arguments)]
  pub fn spawn(
    self_thread_id: ThreadId,
    receiver: Receiver<WorkerMessage>,
    sender: Sender<WorkerMessage>,
    peers: Arc<Vec<Sender<WorkerMessage>>>,
    store: Arc<dyn Store>,
    ops: Arc<OpRegistry>,
    ring: Arc<RwLock<Ring>>,
    self_node_id: NodeId,
  ) -> Self {
    let join_sender = sender.clone();
    let join = thread::spawn(move || {
      let mut cache = Cache::new(store);
      let mut native_locks: HashMap<DatumId, NativeLock> = HashMap::new();
      let mut op_records: HashMap<DatumId, OpRecord> = HashMap::new();

      for message in receiver {
        match message {
          WorkerMessage::RunOp {
            op_id,
            op_name,
            datum_ids,
            payload,
            reply,
          } => {
            op_records.insert(
              op_id,
              OpRecord {
                op_name,
                payload,
                still_needed: datum_ids.clone(),
                acquired: Vec::new(),
                reply,
              },
            );
            for datum_id in datum_ids {
              send_acquire(
                &ring,
                &peers,
                op_id,
                datum_id,
                self_node_id,
                self_thread_id,
                join_sender.clone(),
              );
            }
            try_run_if_ready(op_id, &mut op_records, &mut cache, &ops, &ring, &peers);
          }
          WorkerMessage::Acquire {
            op_id,
            datum_id,
            requester_node,
            requester_thread,
            requester_inbox,
          } => {
            let lock = native_locks.entry(datum_id).or_default();
            let on_granted = Box::new(move || {
              let _ = requester_inbox.send(WorkerMessage::AcquireGranted { op_id, datum_id });
            });
            let outcome = lock.request(op_id, requester_node, requester_thread, on_granted);
            if let AcquireOutcome::RecallNeeded(holder) = outcome {
              let _ = peers[holder.thread_id.0 as usize].send(WorkerMessage::Recall {
                datum_id,
                requesting_op_id: op_id,
                native_home_inbox: join_sender.clone(),
              });
            }
          }
          WorkerMessage::AcquireGranted { op_id, datum_id } => {
            if let Some(record) = op_records.get_mut(&op_id) {
              record.still_needed.retain(|id| *id != datum_id);
              record.acquired.push(datum_id);
            }
            try_run_if_ready(op_id, &mut op_records, &mut cache, &ops, &ring, &peers);
          }
          WorkerMessage::Recall {
            datum_id,
            requesting_op_id: _,
            native_home_inbox,
          } => {
            cache.invalidate(datum_id);
            let mut wounded_op_id = None;
            for (op_id, record) in op_records.iter_mut() {
              if let Some(pos) = record.acquired.iter().position(|id| *id == datum_id) {
                record.acquired.remove(pos);
                record.still_needed.push(datum_id);
                wounded_op_id = Some(*op_id);
                break;
              }
            }
            let _ = native_home_inbox.send(WorkerMessage::Release { datum_id });
            if let Some(op_id) = wounded_op_id {
              send_acquire(
                &ring,
                &peers,
                op_id,
                datum_id,
                self_node_id,
                self_thread_id,
                join_sender.clone(),
              );
            }
          }
          WorkerMessage::Release { datum_id } => {
            if let Some(lock) = native_locks.get_mut(&datum_id) {
              lock.release();
            }
          }
          WorkerMessage::EvictNonNative(is_native) => {
            cache.evict_non_native(|id| is_native(id));
          }
        }
      }
    });
    Self {
      sender,
      _join: join,
    }
  }

  /// Returns a clone of this thread's inbox sender — used by `pool.rs`
  /// to relay requests directly to a specific thread by `ThreadId`.
  pub(crate) fn sender(&self) -> Sender<WorkerMessage> {
    self.sender.clone()
  }

  /// Assigns a new op to this thread and blocks for its result. The
  /// thread's own message loop drives collation (acquiring whatever
  /// datums it doesn't already hold) before invoking the op.
  pub fn run_op(
    &self,
    op_id: DatumId,
    op_name: String,
    datum_ids: Vec<DatumId>,
    payload: Vec<u8>,
  ) -> Result<Vec<u8>, String> {
    let (reply_tx, reply_rx) = std::sync::mpsc::channel();
    self
      .sender
      .send(WorkerMessage::RunOp {
        op_id,
        op_name,
        datum_ids,
        payload,
        reply: reply_tx,
      })
      .expect("worker thread exited unexpectedly");
    reply_rx.recv().expect("worker dropped the reply channel")
  }

  /// Asks the owning thread to evict any cache entry `is_native` rejects
  /// — fire-and-forget, no reply, but guaranteed to be processed before
  /// any `run_op` call made after this one returns (the worker's inbox
  /// is a single ordered queue).
  pub fn evict_non_native(&self, is_native: Arc<dyn Fn(DatumId) -> bool + Send + Sync>) {
    let _ = self.sender.send(WorkerMessage::EvictNonNative(is_native));
  }
}

/// Sends an `Acquire` for `datum_id` on behalf of `op_id` to whichever
/// thread `ring.native()` currently names — always via a message, even
/// when that's the calling thread itself (a cheap, non-blocking
/// self-send), so there's exactly one code path regardless of whether
/// the native home turns out to be local-to-self or a different local
/// thread.
#[allow(clippy::too_many_arguments)]
fn send_acquire(
  ring: &Arc<RwLock<Ring>>,
  peers: &Arc<Vec<Sender<WorkerMessage>>>,
  op_id: DatumId,
  datum_id: DatumId,
  self_node_id: NodeId,
  self_thread_id: ThreadId,
  requester_inbox: Sender<WorkerMessage>,
) {
  let (_, native_thread) = ring.read().unwrap().native(datum_id);
  let _ = peers[native_thread.0 as usize].send(WorkerMessage::Acquire {
    op_id,
    datum_id,
    requester_node: self_node_id,
    requester_thread: self_thread_id,
    requester_inbox,
  });
}

/// If `op_id`'s record has nothing left to acquire, runs it, replies,
/// then releases every acquired datum back toward its native home.
fn try_run_if_ready(
  op_id: DatumId,
  op_records: &mut HashMap<DatumId, OpRecord>,
  cache: &mut Cache,
  ops: &OpRegistry,
  ring: &Arc<RwLock<Ring>>,
  peers: &Arc<Vec<Sender<WorkerMessage>>>,
) {
  let ready = op_records
    .get(&op_id)
    .is_some_and(|record| record.still_needed.is_empty());
  if !ready {
    return;
  }
  let record = op_records.remove(&op_id).unwrap();
  let mut ctx = OpContext::new(cache);
  let result = ops.invoke(&record.op_name, &mut ctx, &record.acquired, &record.payload);
  let _ = record.reply.send(result);
  for datum_id in record.acquired {
    let (_, thread_id) = ring.read().unwrap().native(datum_id);
    let _ = peers[thread_id.0 as usize].send(WorkerMessage::Release { datum_id });
  }
}
```

- [ ] **Step 3: Write `worker.rs`'s own tests**

Add this `mod tests` block at the bottom of `crates/seisin-node/src/worker.rs`:

```rust
#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::mpsc;

  use seisin_core::store::InMemoryStore;

  /// Spawns `thread_count` interconnected `WorkerHandle`s sharing one
  /// store, ring, and op registry — a minimal in-process pool, built by
  /// hand here since `pool.rs` doesn't exist in this shape yet (Task 4).
  fn spawn_test_pool(thread_count: u32, ring: Arc<RwLock<Ring>>, ops: OpRegistry) -> Vec<WorkerHandle> {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let ops = Arc::new(ops);
    let mut senders = Vec::with_capacity(thread_count as usize);
    let mut receivers = Vec::with_capacity(thread_count as usize);
    for _ in 0..thread_count {
      let (tx, rx) = mpsc::channel();
      senders.push(tx);
      receivers.push(rx);
    }
    let peers = Arc::new(senders);
    receivers
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
          NodeId(1),
        )
      })
      .collect()
  }

  fn register_echo_ops(ops: &mut OpRegistry) {
    ops.register(
      "put_first",
      Box::new(|ctx, ids, payload| {
        ctx.put(ids[0], payload.to_vec());
        payload.to_vec()
      }),
    );
    ops.register(
      "get_first",
      Box::new(|ctx, ids, _payload| ctx.get(ids[0]).unwrap_or_default()),
    );
  }

  #[test]
  fn a_single_datum_op_on_its_own_native_thread_runs_immediately() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let mut ops = OpRegistry::new();
    register_echo_ops(&mut ops);
    let handles = spawn_test_pool(1, ring, ops);
    let id = DatumId::new();

    let result = handles[0].run_op(
      DatumId::new(),
      "put_first".to_string(),
      vec![id],
      b"hello".to_vec(),
    );
    assert_eq!(result, Ok(b"hello".to_vec()));

    let result = handles[0].run_op(DatumId::new(), "get_first".to_string(), vec![id], vec![]);
    assert_eq!(result, Ok(b"hello".to_vec()));
  }

  #[test]
  fn an_op_collates_a_datum_natively_owned_by_a_different_local_thread() {
    // With 4 threads, find two ids whose native homes differ so this
    // test actually exercises cross-thread acquisition rather than
    // relying on luck.
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 4)])));
    let (from, to) = loop {
      let a = DatumId::new();
      let b = DatumId::new();
      if ring.read().unwrap().native(a) != ring.read().unwrap().native(b) {
        break (a, b);
      }
    };
    let mut ops = OpRegistry::new();
    ops.register(
      "move_content",
      Box::new(|ctx, ids, _payload| {
        let content = ctx.get(ids[0]).unwrap_or_default();
        ctx.delete(ids[0]);
        ctx.put(ids[1], content.clone());
        content
      }),
    );
    register_echo_ops(&mut ops);
    let handles = spawn_test_pool(4, ring.clone(), ops);

    let (_, from_thread) = ring.read().unwrap().native(from);
    handles[from_thread.0 as usize]
      .run_op(
        DatumId::new(),
        "put_first".to_string(),
        vec![from],
        b"payload".to_vec(),
      )
      .unwrap();

    // Dispatch the collating op from thread 0 regardless of where
    // `from`/`to` natively live, to prove collation reaches across
    // threads.
    let result = handles[0].run_op(
      DatumId::new(),
      "move_content".to_string(),
      vec![from, to],
      vec![],
    );
    assert_eq!(result, Ok(b"payload".to_vec()));
  }

  #[test]
  fn an_older_op_recalls_a_datum_from_a_younger_ops_in_flight_collation() {
    // Classic two-op cycle: op1 needs (a, b), op2 needs (b, a) — opposite
    // acquisition order — with op1 the strictly older op_id. Neither
    // should deadlock; op1 (older) must win any contention immediately
    // and both ops must eventually complete. A genuine deadlock would
    // hang `thread::join` below rather than fail an assertion — that's
    // an accepted trade-off here: `cargo test`'s own harness timeout is
    // the backstop, and a hang is just as informative as a clean
    // failure for this specific bug class.
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 4)])));
    let (a, b) = loop {
      let x = DatumId::new();
      let y = DatumId::new();
      if ring.read().unwrap().native(x) != ring.read().unwrap().native(y) {
        break (x, y);
      }
    };
    let mut ops = OpRegistry::new();
    ops.register(
      "touch_both",
      Box::new(|ctx, ids, _payload| {
        ctx.put(ids[0], b"touched".to_vec());
        ctx.put(ids[1], b"touched".to_vec());
        vec![]
      }),
    );
    let handles = Arc::new(spawn_test_pool(4, ring, ops));

    let op1 = DatumId::new(); // older: created first
    let op2 = DatumId::new(); // younger

    let handles_a = Arc::clone(&handles);
    let thread1 = std::thread::spawn(move || {
      handles_a[0].run_op(op1, "touch_both".to_string(), vec![a, b], vec![])
    });
    let handles_b = Arc::clone(&handles);
    let thread2 = std::thread::spawn(move || {
      handles_b[1].run_op(op2, "touch_both".to_string(), vec![b, a], vec![])
    });

    let result1 = thread1.join().unwrap();
    let result2 = thread2.join().unwrap();
    assert_eq!(result1, Ok(vec![]));
    assert_eq!(result2, Ok(vec![]));
  }
```

- [ ] **Step 4: Attempt to run the tests**

Run: `cargo test -p seisin-node --lib worker::`
Expected: FAIL to compile — `pool.rs` and `server.rs` still reference the old `WorkerHandle`/`WorkerPool` APIs. This is expected; proceed to Task 4.

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-node/src/worker.rs crates/seisin-node/src/lib.rs
git commit -m "feat: rework worker.rs for acquire/wound-wait and op collation"
git push
```

---

### Task 4: `pool.rs` — rework for threaded peer wiring and op dispatch

**Files:**
- Modify: `crates/seisin-node/src/pool.rs` (full rewrite)

**Interfaces:**
- Consumes: `WorkerHandle::spawn` (new signature), `WorkerHandle::run_op`, `WorkerHandle::sender()`, `WorkerHandle::evict_non_native` from Task 3.
- Produces: `WorkerPool::spawn(store: Arc<dyn Store>, thread_count: u32, ops: Arc<OpRegistry>, ring: Arc<RwLock<Ring>>, self_node_id: NodeId) -> Self`, `WorkerPool::run_op(op_id: DatumId, op_name: String, datum_ids: Vec<DatumId>, payload: Vec<u8>) -> Result<Vec<u8>, String>` (destination chosen by 3a's native-majority heuristic among local threads), `WorkerPool::evict_non_native` (unchanged, broadcasts to every thread).
- Removes: `WorkerPool::submit`, `evict_single`, `evict_non_native_for` (3a's collation-specific helpers — no longer needed now that acquire/release is driven internally by each worker thread's own message handling, not orchestrated externally).

- [ ] **Step 1: Rewrite `crates/seisin-node/src/pool.rs`**

```rust
//! A pool of owning threads (one `WorkerHandle` per `ThreadId`),
//! interconnected so any thread can reach any other by `ThreadId` for
//! acquire/recall/release traffic (see `worker.rs`), all sharing this
//! node's backing store and op registry.

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, RwLock};

use seisin_core::authority::{NodeId, ThreadId};
use seisin_core::datum::DatumId;
use seisin_core::store::Store;
use seisin_ops::registry::OpRegistry;
use seisin_ring::ring::Ring;

use crate::worker::{WorkerHandle, WorkerMessage};

pub struct WorkerPool {
  handles: Vec<WorkerHandle>,
  ring: Arc<RwLock<Ring>>,
}

impl WorkerPool {
  /// Spawns `thread_count` interconnected worker threads sharing one
  /// `store` and `ops` registry.
  pub fn spawn(
    store: Arc<dyn Store>,
    thread_count: u32,
    ops: Arc<OpRegistry>,
    ring: Arc<RwLock<Ring>>,
    self_node_id: NodeId,
  ) -> Self {
    let mut senders = Vec::with_capacity(thread_count as usize);
    let mut receivers = Vec::with_capacity(thread_count as usize);
    for _ in 0..thread_count {
      let (tx, rx) = mpsc::channel::<WorkerMessage>();
      senders.push(tx);
      receivers.push(rx);
    }
    let peers = Arc::new(senders);
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
        )
      })
      .collect();
    Self { handles, ring }
  }

  /// Picks the local thread that natively owns the most of
  /// `datum_ids` (3a's thread-assignment heuristic, unchanged), assigns
  /// the op to it, and blocks for the result. Callers are responsible
  /// for having already confirmed every id in `datum_ids` resolves to
  /// this node — see `server.rs::handle_op_request`.
  pub fn run_op(
    &self,
    op_id: DatumId,
    op_name: String,
    datum_ids: Vec<DatumId>,
    payload: Vec<u8>,
  ) -> Result<Vec<u8>, String> {
    let destination = self.pick_destination_thread(&datum_ids);
    self.handles[destination.0 as usize].run_op(op_id, op_name, datum_ids, payload)
  }

  fn pick_destination_thread(&self, datum_ids: &[DatumId]) -> ThreadId {
    let ring = self.ring.read().unwrap();
    let mut counts: HashMap<ThreadId, usize> = HashMap::new();
    for id in datum_ids {
      let (_, thread_id) = ring.native(*id);
      *counts.entry(thread_id).or_insert(0) += 1;
    }
    *counts
      .iter()
      .max_by_key(|(thread_id, count)| (**count, std::cmp::Reverse(thread_id.0)))
      .map(|(thread_id, _)| thread_id)
      .unwrap_or(&ThreadId(0))
  }

  /// Asks every worker in the pool to evict cache entries `is_native`
  /// rejects — see `WorkerHandle::evict_non_native`.
  pub fn evict_non_native(&self, is_native: Arc<dyn Fn(DatumId) -> bool + Send + Sync>) {
    for handle in &self.handles {
      handle.evict_non_native(Arc::clone(&is_native));
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use seisin_core::store::InMemoryStore;

  fn test_pool(thread_count: u32) -> WorkerPool {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), thread_count)])));
    WorkerPool::spawn(
      Arc::new(InMemoryStore::new()),
      thread_count,
      Arc::new(OpRegistry::new()),
      ring,
      NodeId(1),
    )
  }

  #[test]
  fn run_op_executes_a_registered_op_and_returns_its_result() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 2)])));
    let mut ops = OpRegistry::new();
    ops.register(
      "put_first",
      Box::new(|ctx, ids, payload| {
        ctx.put(ids[0], payload.to_vec());
        payload.to_vec()
      }),
    );
    let pool = WorkerPool::spawn(
      Arc::new(InMemoryStore::new()),
      2,
      Arc::new(ops),
      ring,
      NodeId(1),
    );
    let id = DatumId::new();
    let result = pool.run_op(DatumId::new(), "put_first".to_string(), vec![id], b"hi".to_vec());
    assert_eq!(result, Ok(b"hi".to_vec()));
  }

  #[test]
  fn run_op_on_unknown_name_returns_an_error() {
    let pool = test_pool(1);
    assert!(pool
      .run_op(DatumId::new(), "nope".to_string(), vec![], vec![])
      .is_err());
  }

  #[test]
  fn writes_from_one_op_are_visible_to_a_later_op_regardless_of_thread() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 2)])));
    let mut ops = OpRegistry::new();
    ops.register(
      "put_first",
      Box::new(|ctx, ids, payload| {
        ctx.put(ids[0], payload.to_vec());
        vec![]
      }),
    );
    ops.register(
      "get_first",
      Box::new(|ctx, ids, _payload| ctx.get(ids[0]).unwrap_or_default()),
    );
    let pool = WorkerPool::spawn(
      Arc::new(InMemoryStore::new()),
      2,
      Arc::new(ops),
      ring,
      NodeId(1),
    );
    let id = DatumId::new();
    pool
      .run_op(DatumId::new(), "put_first".to_string(), vec![id], b"hello".to_vec())
      .unwrap();
    let result = pool.run_op(DatumId::new(), "get_first".to_string(), vec![id], vec![]);
    assert_eq!(result, Ok(b"hello".to_vec()));
  }
}
```

- [ ] **Step 2: Run `seisin-node`'s lib tests**

Run: `cargo test -p seisin-node --lib`
Expected: FAIL to compile — `server.rs` still calls the retired `WorkerPool::submit`/`evict_single`/`evict_non_native_for` and the old 3-argument `WorkerPool::spawn`. Proceed to Task 5.

- [ ] **Step 3: Commit and push**

```bash
git add crates/seisin-node/src/pool.rs
git commit -m "feat: rework WorkerPool for threaded peer wiring and op dispatch"
git push
```

---

### Task 5: `server.rs` — simplified dispatch; restore full compilation

**Files:**
- Modify: `crates/seisin-node/src/server.rs` (full rewrite)
- Modify: `crates/seisin-node/src/lib.rs` (restore `#![deny(warnings)]`)

**Interfaces:**
- Consumes: `WorkerPool::run_op` from Task 4; `Request::Op`/`Response` from Task 1.
- Produces: `serve` (unchanged signature), `handle_connection` (private, simplified — `Request` only has one variant now), `handle_op_request` (private, rewritten to also handle the "all datum_ids native to one other node" redirect case, unifying what used to be separate single-datum-routing and op-collation-routing logic).

This task is where full workspace compilation is restored for `seisin-node`'s own lib — `cargo test -p seisin-node --lib` must pass by the end of this task (main.rs and the integration tests are still broken until Tasks 6-8).

- [ ] **Step 1: Rewrite `crates/seisin-node/src/server.rs`**

```rust
//! Accepts client TCP connections and routes each `Request::Op`: serve
//! directly if every one of its datum_ids natively belongs to this
//! node, redirect if they all belong to exactly one other node (the
//! same idea as before 3b, just generalized from a single datum_id to a
//! list), or reject if they're genuinely spread across more than one
//! node (cross-node collation — a later sub-project).

use std::collections::{HashMap, HashSet};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_protocol::{decode_request, encode_response, read_frame, write_frame, Request, Response};
use seisin_ring::ring::Ring;

use crate::pool::WorkerPool;

/// Runs the accept loop on `listener`, spawning one handler thread per
/// connection, until the listener errors out (e.g. the socket is closed).
pub fn serve(
  listener: TcpListener,
  self_node_id: NodeId,
  ring: Arc<RwLock<Ring>>,
  address_book: Arc<HashMap<NodeId, String>>,
  pool: Arc<WorkerPool>,
) {
  for stream in listener.incoming() {
    let stream = match stream {
      Ok(s) => s,
      Err(_) => continue,
    };
    let ring = Arc::clone(&ring);
    let address_book = Arc::clone(&address_book);
    let pool = Arc::clone(&pool);
    thread::spawn(move || handle_connection(stream, self_node_id, ring, address_book, pool));
  }
}

fn handle_connection(
  mut stream: TcpStream,
  self_node_id: NodeId,
  ring: Arc<RwLock<Ring>>,
  address_book: Arc<HashMap<NodeId, String>>,
  pool: Arc<WorkerPool>,
) {
  loop {
    let payload = match read_frame(&mut stream) {
      Ok(p) => p,
      Err(_) => return, // connection closed or errored
    };
    let request = match decode_request(&payload) {
      Ok(r) => r,
      Err(_) => return, // malformed request: drop the connection
    };
    let Request::Op {
      op_id,
      op_name,
      datum_ids,
      payload,
    } = request;
    let response = handle_op_request(
      self_node_id,
      &ring,
      &address_book,
      &pool,
      op_id,
      op_name,
      datum_ids,
      payload,
    );
    if write_frame(&mut stream, &encode_response(&response)).is_err() {
      return;
    }
  }
}

/// Resolves every datum_id's native node. If they're all this node,
/// runs the op locally. If they're all exactly one *other* node,
/// redirects there (the client reconnects and retries — the same
/// mechanism a single-datum request used before 3b, just generalized).
/// If they're spread across more than one node, rejects — cross-node
/// collation is a later sub-project.
fn handle_op_request(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  pool: &WorkerPool,
  op_id: DatumId,
  op_name: String,
  datum_ids: Vec<DatumId>,
  payload: Vec<u8>,
) -> Response {
  let native_nodes: HashSet<NodeId> = {
    let ring = ring.read().unwrap();
    datum_ids.iter().map(|id| ring.native(*id).0).collect()
  };

  if native_nodes.len() > 1 {
    return Response::OpError {
      message: "cross-node collation is not supported in this version".to_string(),
    };
  }

  if let Some(&only_node) = native_nodes.iter().next() {
    if only_node != self_node_id {
      return match address_book.get(&only_node) {
        Some(address) => Response::Redirect {
          address: address.clone(),
        },
        // Ring and address book disagree — a static-config bug in this
        // plan's scope; there's no sensible response, so this arm is
        // unreachable in practice, but return an error rather than
        // panicking a connection thread over a config mismatch.
        None => Response::OpError {
          message: format!("no known address for node {only_node:?}"),
        },
      };
    }
  }

  match pool.run_op(op_id, op_name, datum_ids, payload) {
    Ok(payload) => Response::OpResult { payload },
    Err(message) => Response::OpError { message },
  }
}
```

- [ ] **Step 2: Restore `#![deny(warnings)]`**

In `crates/seisin-node/src/lib.rs`, restore the line removed in Task 3:
```rust
#![deny(warnings)]

pub mod collation;
pub mod config;
pub mod gossip_client;
pub mod gossip_server;
pub mod gossip_state;
pub mod pool;
pub mod server;
pub mod worker;
```

- [ ] **Step 3: Run the full `seisin-node` lib test suite**

Run: `cargo test -p seisin-node --lib`
Expected: PASS — this now includes Task 2's `collation::` tests, Task 3's `worker::` tests (including the wound-wait cycle test), and Task 4's `pool::` tests, all compiling and passing together for the first time.

- [ ] **Step 4: Commit and push**

```bash
git add crates/seisin-node/src/server.rs crates/seisin-node/src/lib.rs
git commit -m "feat: simplify server dispatch for unified Op requests; restore full lib compilation"
git push
```

---

### Task 6: Wire the new `WorkerPool::spawn` signature through `main.rs`

**Files:**
- Modify: `crates/seisin-node/src/main.rs:78-86`

**Interfaces:**
- Consumes: `WorkerPool::spawn`'s new 5-argument signature from Task 4.

- [ ] **Step 1: Update the `WorkerPool::spawn` call site**

In `crates/seisin-node/src/main.rs`, change:
```rust
  let store = Arc::new(InMemoryStore::new());
  // No solution has been wired up yet — an empty registry until
  // Sub-project 3b (or a real solution built on this framework) needs
  // one populated with actual operations.
  let pool = Arc::new(WorkerPool::spawn(
    store,
    self_thread_count,
    Arc::new(seisin_ops::registry::OpRegistry::new()),
  ));
```
to:
```rust
  let store = Arc::new(InMemoryStore::new());
  // No solution has been wired up yet — an empty registry until a real
  // solution built on this framework needs one populated with actual
  // operations.
  let pool = Arc::new(WorkerPool::spawn(
    store,
    self_thread_count,
    Arc::new(seisin_ops::registry::OpRegistry::new()),
    Arc::clone(&ring),
    self_node_id,
  ));
```

(`ring` and `self_node_id` are already in scope earlier in `main()` — no new imports needed.)

- [ ] **Step 2: Verify the binary builds**

Run: `cargo build -p seisin-node --bin seisin-node`
Expected: builds with no errors.

- [ ] **Step 3: Commit and push**

```bash
git add crates/seisin-node/src/main.rs
git commit -m "feat: wire the new WorkerPool::spawn signature through main.rs"
git push
```

---

### Task 7: Rewrite `integration_wire_protocol.rs` against registered ops

**Files:**
- Modify: `crates/seisin-node/tests/integration_wire_protocol.rs` (full rewrite)

**Interfaces:**
- Consumes: `WorkerPool::spawn`'s new signature, `Request::Op`/`Response::OpResult`/`OpError`.

- [ ] **Step 1: Rewrite the file**

```rust
use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::sk::{decode_sk_entries, encode_sk_entries};
use seisin_core::store::InMemoryStore;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{
  decode_response, encode_request, read_frame, write_frame, Request, Response,
};
use seisin_ring::ring::Ring;

/// A not-found sentinel this test's `get` op returns when the datum is
/// absent, since `ctx.get` returns `Option` but an op's return type is a
/// plain `Vec<u8>` — the framework itself never interprets an op's
/// payload, so this convention is entirely test-local.
const NOT_FOUND_SENTINEL: &[u8] = b"__NOT_FOUND__";

fn build_registry() -> OpRegistry {
  let mut ops = OpRegistry::new();
  ops.register(
    "put",
    Box::new(|ctx, ids, payload| {
      ctx.put(ids[0], payload.to_vec());
      vec![]
    }),
  );
  ops.register(
    "get",
    Box::new(|ctx, ids, _payload| {
      ctx
        .get(ids[0])
        .unwrap_or_else(|| NOT_FOUND_SENTINEL.to_vec())
    }),
  );
  ops.register(
    "delete",
    Box::new(|ctx, ids, _payload| {
      ctx.delete(ids[0]);
      vec![]
    }),
  );
  ops
}

fn start_test_server() -> SocketAddr {
  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap();
  let node_id = NodeId(1);
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_id, 1)])));
  let address_book = Arc::new(HashMap::new());
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    1,
    Arc::new(build_registry()),
    Arc::clone(&ring),
    node_id,
  ));
  thread::spawn(move || serve(listener, node_id, ring, address_book, pool));
  addr
}

fn round_trip(stream: &mut TcpStream, request: Request) -> Response {
  write_frame(stream, &encode_request(&request)).unwrap();
  let payload = read_frame(stream).unwrap();
  decode_response(&payload).unwrap()
}

fn op_result(response: Response) -> Vec<u8> {
  match response {
    Response::OpResult { payload } => payload,
    other => panic!("expected OpResult, got {other:?}"),
  }
}

#[test]
fn put_then_get_returns_stored_content() {
  let addr = start_test_server();
  let mut stream = TcpStream::connect(addr).unwrap();
  let id = DatumId::new();

  assert_eq!(
    op_result(round_trip(
      &mut stream,
      Request::Op {
        op_id: DatumId::new(),
        op_name: "put".to_string(),
        datum_ids: vec![id],
        payload: b"hello".to_vec(),
      }
    )),
    Vec::<u8>::new()
  );
  assert_eq!(
    op_result(round_trip(
      &mut stream,
      Request::Op {
        op_id: DatumId::new(),
        op_name: "get".to_string(),
        datum_ids: vec![id],
        payload: vec![],
      }
    )),
    b"hello".to_vec()
  );
}

#[test]
fn get_on_missing_datum_returns_the_not_found_sentinel() {
  let addr = start_test_server();
  let mut stream = TcpStream::connect(addr).unwrap();
  assert_eq!(
    op_result(round_trip(
      &mut stream,
      Request::Op {
        op_id: DatumId::new(),
        op_name: "get".to_string(),
        datum_ids: vec![DatumId::new()],
        payload: vec![],
      }
    )),
    NOT_FOUND_SENTINEL.to_vec()
  );
}

#[test]
fn delete_removes_datum() {
  let addr = start_test_server();
  let mut stream = TcpStream::connect(addr).unwrap();
  let id = DatumId::new();

  round_trip(
    &mut stream,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "put".to_string(),
      datum_ids: vec![id],
      payload: b"data".to_vec(),
    },
  );
  round_trip(
    &mut stream,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "delete".to_string(),
      datum_ids: vec![id],
      payload: vec![],
    },
  );
  assert_eq!(
    op_result(round_trip(
      &mut stream,
      Request::Op {
        op_id: DatumId::new(),
        op_name: "get".to_string(),
        datum_ids: vec![id],
        payload: vec![],
      }
    )),
    NOT_FOUND_SENTINEL.to_vec()
  );
}

#[test]
fn secondary_key_datum_round_trips_as_a_regular_datum() {
  let addr = start_test_server();
  let mut stream = TcpStream::connect(addr).unwrap();
  let sk_id = DatumId::new();
  let entries = vec![(DatumId::new(), seisin_core::authority::AuthorityIdx::Native)];
  let content = encode_sk_entries(&entries);

  round_trip(
    &mut stream,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "put".to_string(),
      datum_ids: vec![sk_id],
      payload: content,
    },
  );
  let got = op_result(round_trip(
    &mut stream,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "get".to_string(),
      datum_ids: vec![sk_id],
      payload: vec![],
    },
  ));
  assert_eq!(decode_sk_entries(&got).unwrap(), entries);
}
```

- [ ] **Step 2: Run this test file**

Run: `cargo test -p seisin-node --test integration_wire_protocol`
Expected: PASS (4 tests).

- [ ] **Step 3: Commit and push**

```bash
git add crates/seisin-node/tests/integration_wire_protocol.rs
git commit -m "test: rewrite integration_wire_protocol against registered ops"
git push
```

---

### Task 8: Update the remaining integration test files

**Files:**
- Modify: `crates/seisin-node/tests/integration_multi_node_routing.rs`
- Modify: `crates/seisin-node/tests/integration_gossip_failure_detection.rs`
- Modify: `crates/seisin-node/tests/integration_op_collation.rs`

**Interfaces:**
- Consumes: `WorkerPool::spawn`'s new signature; `Request::Op`.

- [ ] **Step 1: Rewrite `integration_multi_node_routing.rs`**

Replace the whole file:

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

fn build_registry() -> OpRegistry {
  let mut ops = OpRegistry::new();
  ops.register(
    "put",
    Box::new(|ctx, ids, payload| {
      ctx.put(ids[0], payload.to_vec());
      vec![]
    }),
  );
  ops.register(
    "get",
    Box::new(|ctx, ids, _payload| ctx.get(ids[0]).unwrap_or_default()),
  );
  ops
}

fn start_two_node_cluster() -> (String, String) {
  let listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
  let listener_b = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr_a = listener_a.local_addr().unwrap().to_string();
  let addr_b = listener_b.local_addr().unwrap().to_string();

  let node_a = NodeId(1);
  let node_b = NodeId(2);
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_a, 2), (node_b, 2)])));

  let mut address_book = HashMap::new();
  address_book.insert(node_a, addr_a.clone());
  address_book.insert(node_b, addr_b.clone());
  let address_book = Arc::new(address_book);

  let pool_a = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(build_registry()),
    Arc::clone(&ring),
    node_a,
  ));
  let pool_b = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(build_registry()),
    Arc::clone(&ring),
    node_b,
  ));

  {
    let ring = Arc::clone(&ring);
    let address_book = Arc::clone(&address_book);
    thread::spawn(move || serve(listener_a, node_a, ring, address_book, pool_a));
  }
  {
    let ring = Arc::clone(&ring);
    let address_book = Arc::clone(&address_book);
    thread::spawn(move || serve(listener_b, node_b, ring, address_book, pool_b));
  }

  (addr_a, addr_b)
}

#[test]
fn put_and_get_route_correctly_across_two_nodes_regardless_of_entry_point() {
  let (addr_a, addr_b) = start_two_node_cluster();

  for _ in 0..20 {
    let id = DatumId::new();
    let content = format!("value-for-{id:?}").into_bytes();

    // Always PUT via node A's address, regardless of who actually owns it.
    let put_resp = seisin_client::call(
      &addr_a,
      Request::Op {
        op_id: DatumId::new(),
        op_name: "put".to_string(),
        datum_ids: vec![id],
        payload: content.clone(),
      },
    )
    .unwrap();
    assert_eq!(put_resp, Response::OpResult { payload: vec![] });

    // Always GET via node B's address.
    let get_resp = seisin_client::call(
      &addr_b,
      Request::Op {
        op_id: DatumId::new(),
        op_name: "get".to_string(),
        datum_ids: vec![id],
        payload: vec![],
      },
    )
    .unwrap();
    assert_eq!(
      get_resp,
      Response::OpResult {
        payload: content
      }
    );
  }
}
```

- [ ] **Step 2: Update `integration_gossip_failure_detection.rs`**

Change the imports to add `seisin_ops::registry::OpRegistry` and change the `WorkerPool::spawn` call site (inside `start_node`) from:
```rust
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    this.1,
    Arc::new(seisin_ops::registry::OpRegistry::new()),
  ));
```
to:
```rust
  let mut ops = OpRegistry::new();
  ops.register(
    "put",
    Box::new(|ctx, ids, payload| {
      ctx.put(ids[0], payload.to_vec());
      vec![]
    }),
  );
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    this.1,
    Arc::new(ops),
    Arc::clone(&ring),
    node_id,
  ));
```

Then update the test body itself — change:
```rust
  for _ in 0..20 {
    let id = DatumId::new();
    let response = seisin_client::call(
      &addr_a,
      Request::Put {
        id,
        content: b"x".to_vec(),
      },
    )
    .unwrap();
    assert_eq!(
      response,
      Response::Ok,
      "expected a direct Ok, not a Redirect, once node B is removed from the ring"
    );
  }
```
to:
```rust
  for _ in 0..20 {
    let id = DatumId::new();
    let response = seisin_client::call(
      &addr_a,
      Request::Op {
        op_id: DatumId::new(),
        op_name: "put".to_string(),
        datum_ids: vec![id],
        payload: b"x".to_vec(),
      },
    )
    .unwrap();
    assert_eq!(
      response,
      Response::OpResult { payload: vec![] },
      "expected a direct OpResult, not a Redirect, once node B is removed from the ring"
    );
  }
```

- [ ] **Step 3: Update `integration_op_collation.rs`**

Add `op_id: DatumId::new(),` to every `Request::Op { ... }` construction in this file (both `start_single_node_server`'s `WorkerPool::spawn` call already matches the new 5-argument shape from Task 4/6 conceptually — check it actually does; if `ring`/`node_id` aren't already threaded through `WorkerPool::spawn` in this file, add them the same way as Step 1 above), and change every `Response::OpResult`/`OpError` assertion's shape only if needed (it shouldn't need to change beyond the `op_id` addition).

Since this file was already `Op`-shaped from 3a, the only required edits are:
1. Add `op_id: DatumId::new(),` as the first field in each `Request::Op { ... }` literal.
2. Update the `WorkerPool::spawn` call in `start_single_node_server` to the new 5-argument form:
```rust
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    4,
    Arc::new(ops),
    Arc::clone(&ring),
    node_id,
  ));
```

- [ ] **Step 4: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS (all tests, across all crates).

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-node/tests/integration_multi_node_routing.rs crates/seisin-node/tests/integration_gossip_failure_detection.rs crates/seisin-node/tests/integration_op_collation.rs
git commit -m "test: update remaining integration tests for the unified Op wire protocol"
git push
```

---

### Task 9: Same-node wound-wait integration test

**Files:**
- Create: `crates/seisin-node/tests/integration_wound_wait.rs`

**Interfaces:**
- Consumes: everything from Tasks 1-8.
- Produces: an end-to-end (real TCP, not in-process `WorkerHandle`s directly) proof of the classic two-op cycle resolving without deadlock on a single multi-thread node — complementary to Task 3's in-process unit test, this one goes through the real wire protocol and server dispatch.

- [ ] **Step 1: Write the test**

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

fn start_single_node_server(thread_count: u32) -> (String, Arc<RwLock<Ring>>, NodeId) {
  let node_id = NodeId(1);
  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_id, thread_count)])));
  let address_book = Arc::new(HashMap::new());

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
    thread_count,
    Arc::new(ops),
    Arc::clone(&ring),
    node_id,
  ));
  thread::spawn(move || serve(listener, node_id, ring.clone(), address_book, pool));
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_id, thread_count)])));
  (addr, ring, node_id)
}

#[test]
fn two_ops_needing_the_same_two_datums_in_opposite_order_both_complete() {
  let (addr, ring, _node_id) = start_single_node_server(4);

  // Find two ids whose native homes differ, so the two ops below
  // actually contend across threads rather than trivially both running
  // on the same one.
  let (a, b) = loop {
    let x = DatumId::new();
    let y = DatumId::new();
    if ring.read().unwrap().native(x) != ring.read().unwrap().native(y) {
      break (x, y);
    }
  };

  let addr1 = addr.clone();
  let addr2 = addr;
  let op1 = DatumId::new(); // older
  let op2 = DatumId::new(); // younger

  let thread1 = thread::spawn(move || {
    seisin_client::call(
      &addr1,
      Request::Op {
        op_id: op1,
        op_name: "touch_both".to_string(),
        datum_ids: vec![a, b],
        payload: vec![],
      },
    )
  });
  let thread2 = thread::spawn(move || {
    seisin_client::call(
      &addr2,
      Request::Op {
        op_id: op2,
        op_name: "touch_both".to_string(),
        datum_ids: vec![b, a],
        payload: vec![],
      },
    )
  });

  let result1 = thread1.join().unwrap().unwrap();
  let result2 = thread2.join().unwrap().unwrap();
  assert_eq!(result1, Response::OpResult { payload: vec![] });
  assert_eq!(result2, Response::OpResult { payload: vec![] });
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p seisin-node --test integration_wound_wait`
Expected: PASS (1 test). If it hangs, that indicates a deadlock in the acquire/recall logic from Tasks 2-3 — stop and debug rather than proceeding (per the systematic-debugging skill, not by adding timeouts to paper over it).

- [ ] **Step 3: Commit and push**

```bash
git add crates/seisin-node/tests/integration_wound_wait.rs
git commit -m "test: add end-to-end same-node wound-wait integration test"
git push
```

---

### Task 10: Quality gate

**Files:** none (verification only).

- [ ] **Step 1: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS.

- [ ] **Step 2: Run the formatting and lint gate**

Run: `cargo fmt --check`
Expected: no output. If it reports diffs, run `cargo fmt` and re-check.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings/errors. Fix anything reported before continuing.

- [ ] **Step 3: Update `docs/superpowers/PROGRESS.md`**

Add an entry under "Done" for this plan (Sub-project 3b, Part 1), and update "In progress" to note Part 2 (peer-link + real cross-node acquisition) is next.

- [ ] **Step 4: Commit and push**

```bash
git add -A
git commit -m "chore: fmt/clippy fixes for cross-node collation part 1; update progress tracker"
git push
```

(Skip the `git commit` if nothing changed beyond `PROGRESS.md`, but still commit that.)

---

## Self-Review Notes

- **Spec coverage**: `op_id` on every request ✓ (Task 1); `Request::Op` as the only wire variant ✓ (Task 1); native-home-as-sole-lock-manager mechanics (grant/recall/wait-queue, op_id-ordered) ✓ (Task 2); non-blocking worker threads, op records, message-driven collation ✓ (Task 3); thread-to-thread wiring via `WorkerPool` ✓ (Task 4); simplified dispatch unifying single-op and multi-datum-op routing ✓ (Task 5); end-to-end proof of the classic two-op cycle resolving without deadlock, both in-process (Task 3) and over real TCP (Task 9) ✓. Peer-link multiplexing and genuine cross-node acquisition are explicitly Part 2, not covered here, matching the plan split agreed with the user.
- **Placeholder scan**: no TBD/TODO; every code block is complete, runnable Rust matching the surrounding file's existing style.
- **Type consistency**: `WorkerMessage`, `OpRecord`, `NativeLock`/`AcquireOutcome`/`Holder`, and `WorkerHandle`/`WorkerPool`'s method signatures match exactly between where they're defined (Tasks 2-4) and where they're consumed (Tasks 5-9).
- **Known limitation carried forward** (not fixed in this plan, noted for awareness): if ring membership changes concurrently with an in-flight collation attempt, a worker thread's `native_locks` entries for datums it's no longer native for become orphaned bookkeeping — `evict_non_native` already drops the *cache* entries on a ring mutation, but doesn't yet reconcile `native_locks`/`op_records` state. This wasn't part of the approved spec's scope and is deferred; flag it if it becomes a problem once ring membership changes and op collation are exercised together in a later sub-project.
