# Sub-project 3b, Part 2a: Peer-Link Multiplexing & Real Cross-Node Acquisition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Part 1's "reject if a multi-datum op's datums span more than one node" placeholder with genuine cross-node acquisition — a multi-datum op whose datums live on different nodes gets collated onto one thread by pulling the remote ones over the wire, using the exact same `NativeLock`/wound-wait mechanics Part 1 already proved, just with the `Acquire`/`Recall` messages now able to cross a real network boundary.

**Architecture:** One persistent, multiplexed TCP connection per node pair (`peer_link.rs`), established once at startup — the lower `NodeId` in a pair dials the higher one — carrying `{correlation_id, kind, target_thread, body}` envelopes that wrap the existing `Request`/`Response` encoding. `worker.rs`'s `send_acquire`/recall-dispatch pick between an in-process `WorkerMessage` send (same node) and a `peer_link.call(...)` (different node) transparently; either way, the eventual outcome always arrives back as a plain `WorkerMessage` posted into the right thread's own inbox, so no worker thread ever blocks and no cross-thread state is ever touched from the wrong thread.

**Tech Stack:** Rust, `std::sync::mpsc`, `std::net::TcpStream`, existing `seisin-protocol`/`seisin-core`/`seisin-ring` crates, building on Part 1 (`docs/superpowers/plans/2026-07-20-cross-node-collation-wound-wait-part1.md`, fully implemented on `main`).

## Global Constraints

- 2-space indentation.
- `#![deny(warnings)]` at `seisin-node`'s crate root — same temporary-removal-then-restore pattern as Part 1, since this plan also touches `worker.rs`/`pool.rs`/`server.rs`/`main.rs` together.
- Commit and push after every task.
- Update `docs/superpowers/PROGRESS.md` once this plan completes.
- **Deliberate simplification vs. the spec's "established lazily on first need" wording**: this plan establishes every peer-link connection *eagerly at startup* (the lower-`NodeId` side of each pair dials the higher one, from the static config member list) rather than lazily on first use. This sidesteps a real problem lazy connection would otherwise create — a worker thread calling `TcpStream::connect` inline would block, violating the "no worker thread ever blocks" invariant Part 1 established — without adding another async-dial layer to solve it. Reacting to *dynamic* (gossip-driven) membership changes by dialing newly-joined peers is not covered here; it's a known gap, noted in this plan's final task.
- **Crash detection and lock release are explicitly out of scope for this plan** — that's Part 2b, per the spec's own Part 1/Part 2 split. A dead peer here just causes an `Acquire`/`Recall` call to fail or hang, exactly like Part 1's existing lazy-reclaim gaps; this plan is only about proving the happy-path mechanism works.

---

### Task 1: Wire protocol — `Acquire`/`Recall` requests, `Granted`/`Released` responses, and the peer-link envelope

**Files:**
- Modify: `crates/seisin-protocol/src/lib.rs`

**Interfaces:**
- Produces: `Request::Acquire { op_id: DatumId, datum_id: DatumId, requester_node: NodeId, requester_thread: ThreadId }`, `Request::Recall { datum_id: DatumId }`, `Response::Granted`, `Response::Released`. `pub enum EnvelopeKind { Request, Response }`, `pub struct Envelope { pub correlation_id: u64, pub kind: EnvelopeKind, pub target_thread: ThreadId, pub body: Vec<u8> }`, `pub fn encode_envelope(env: &Envelope) -> Vec<u8>`, `pub fn decode_envelope(buf: &[u8]) -> Result<Envelope>`.

- [ ] **Step 1: Add the new `Request`/`Response` variants and their codec**

In `crates/seisin-protocol/src/lib.rs`, add the import:
```rust
use seisin_core::authority::{NodeId, ThreadId};
```
(alongside the existing `use seisin_core::datum::DatumId;`).

Change the `Request` enum to:
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
  Op {
    op_id: DatumId,
    op_name: String,
    datum_ids: Vec<DatumId>,
    payload: Vec<u8>,
  },
  /// Node-to-node only: requests the wound-wait lock on `datum_id` at
  /// whichever thread this frame's peer-link envelope targets, on
  /// behalf of `op_id`. Never sent by a client.
  Acquire {
    op_id: DatumId,
    datum_id: DatumId,
    requester_node: NodeId,
    requester_thread: ThreadId,
  },
  /// Node-to-node only: asks the envelope-targeted thread to evict and
  /// release `datum_id` right now.
  Recall {
    datum_id: DatumId,
  },
}
```

Change the `Response` enum to:
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
  Redirect { address: String },
  OpResult { payload: Vec<u8> },
  OpError { message: String },
  /// Reply to a granted `Acquire` — no content, see the design doc's
  /// "No Content In Transfer Messages" section.
  Granted,
  /// Reply to an acknowledged `Recall`.
  Released,
}
```

Add the new opcodes alongside the existing ones:
```rust
const OP_OP: u8 = 1;
const OP_ACQUIRE: u8 = 2;
const OP_RECALL: u8 = 3;

const RESP_REDIRECT: u8 = 1;
const RESP_OP_RESULT: u8 = 2;
const RESP_OP_ERROR: u8 = 3;
const RESP_GRANTED: u8 = 4;
const RESP_RELEASED: u8 = 5;
```

In `encode_request`, add arms:
```rust
    Request::Acquire {
      op_id,
      datum_id,
      requester_node,
      requester_thread,
    } => {
      buf.push(OP_ACQUIRE);
      buf.extend_from_slice(&op_id.as_bytes());
      buf.extend_from_slice(&datum_id.as_bytes());
      buf.extend_from_slice(&requester_node.0.to_le_bytes());
      buf.extend_from_slice(&requester_thread.0.to_le_bytes());
    }
    Request::Recall { datum_id } => {
      buf.push(OP_RECALL);
      buf.extend_from_slice(&datum_id.as_bytes());
    }
```

In `decode_request`, add the new opcodes to the `match buf[0]`:
```rust
pub fn decode_request(buf: &[u8]) -> Result<Request> {
  if buf.is_empty() {
    bail!("empty request payload");
  }
  match buf[0] {
    OP_OP => decode_op_request(buf),
    OP_ACQUIRE => decode_acquire_request(buf),
    OP_RECALL => decode_recall_request(buf),
    op => bail!("unknown request opcode: {op}"),
  }
}

fn decode_acquire_request(buf: &[u8]) -> Result<Request> {
  if buf.len() != 1 + ID_LEN + ID_LEN + 8 + 4 {
    bail!(
      "acquire request has the wrong length: expected {} bytes, got {}",
      1 + ID_LEN + ID_LEN + 8 + 4,
      buf.len()
    );
  }
  let op_id = DatumId::from_bytes(buf[1..1 + ID_LEN].try_into().unwrap());
  let datum_id = DatumId::from_bytes(buf[1 + ID_LEN..1 + 2 * ID_LEN].try_into().unwrap());
  let node_offset = 1 + 2 * ID_LEN;
  let requester_node = NodeId(u64::from_le_bytes(
    buf[node_offset..node_offset + 8].try_into().unwrap(),
  ));
  let thread_offset = node_offset + 8;
  let requester_thread = ThreadId(u32::from_le_bytes(
    buf[thread_offset..thread_offset + 4].try_into().unwrap(),
  ));
  Ok(Request::Acquire {
    op_id,
    datum_id,
    requester_node,
    requester_thread,
  })
}

fn decode_recall_request(buf: &[u8]) -> Result<Request> {
  if buf.len() != 1 + ID_LEN {
    bail!(
      "recall request has the wrong length: expected {} bytes, got {}",
      1 + ID_LEN,
      buf.len()
    );
  }
  let datum_id = DatumId::from_bytes(buf[1..1 + ID_LEN].try_into().unwrap());
  Ok(Request::Recall { datum_id })
}
```

In `encode_response`, add arms:
```rust
    Response::Granted => buf.push(RESP_GRANTED),
    Response::Released => buf.push(RESP_RELEASED),
```

In `decode_response`, add:
```rust
    RESP_GRANTED => Ok(Response::Granted),
    RESP_RELEASED => Ok(Response::Released),
```

- [ ] **Step 2: Write the round-trip tests for the new variants**

Add to `crates/seisin-protocol/src/lib.rs`'s `mod tests`:
```rust
  #[test]
  fn round_trips_acquire_request() {
    let req = Request::Acquire {
      op_id: DatumId::new(),
      datum_id: DatumId::new(),
      requester_node: seisin_core::authority::NodeId(7),
      requester_thread: seisin_core::authority::ThreadId(3),
    };
    assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
  }

  #[test]
  fn round_trips_recall_request() {
    let req = Request::Recall {
      datum_id: DatumId::new(),
    };
    assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
  }

  #[test]
  fn round_trips_granted_response() {
    assert_eq!(
      decode_response(&encode_response(&Response::Granted)).unwrap(),
      Response::Granted
    );
  }

  #[test]
  fn round_trips_released_response() {
    assert_eq!(
      decode_response(&encode_response(&Response::Released)).unwrap(),
      Response::Released
    );
  }

  #[test]
  fn rejects_a_truncated_acquire_request() {
    let mut buf = encode_request(&Request::Acquire {
      op_id: DatumId::new(),
      datum_id: DatumId::new(),
      requester_node: seisin_core::authority::NodeId(1),
      requester_thread: seisin_core::authority::ThreadId(0),
    });
    buf.truncate(buf.len() - 1);
    assert!(decode_request(&buf).is_err());
  }
```

Run: `cargo test -p seisin-protocol`
Expected: PASS (all existing tests plus these 5 new ones).

- [ ] **Step 3: Add the `Envelope` type and its codec**

Append to `crates/seisin-protocol/src/lib.rs` (after the `Response` codec functions, before `write_frame`):
```rust
/// Which of the two peer-link message flows this envelope carries —
/// peer-link connections are bidirectional and multiplexed, so an
/// incoming frame could be either a response to one of *our* earlier
/// calls or a fresh incoming request from the peer; the reader needs
/// this tag to know which.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvelopeKind {
  Request,
  Response,
}

/// A peer-link frame: `body` is an encoded `Request` or `Response`
/// (per `kind`), tagged with a `correlation_id` so many concurrent
/// calls can share one connection, and (for requests only) a
/// `target_thread` naming which local worker thread on the receiving
/// node should handle it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
  pub correlation_id: u64,
  pub kind: EnvelopeKind,
  pub target_thread: ThreadId,
  pub body: Vec<u8>,
}

const ENVELOPE_KIND_REQUEST: u8 = 0;
const ENVELOPE_KIND_RESPONSE: u8 = 1;

pub fn encode_envelope(env: &Envelope) -> Vec<u8> {
  let mut buf = Vec::with_capacity(13 + env.body.len());
  buf.extend_from_slice(&env.correlation_id.to_le_bytes());
  buf.push(match env.kind {
    EnvelopeKind::Request => ENVELOPE_KIND_REQUEST,
    EnvelopeKind::Response => ENVELOPE_KIND_RESPONSE,
  });
  buf.extend_from_slice(&env.target_thread.0.to_le_bytes());
  buf.extend_from_slice(&env.body);
  buf
}

pub fn decode_envelope(buf: &[u8]) -> Result<Envelope> {
  if buf.len() < 13 {
    bail!("envelope too short for its header: {} bytes", buf.len());
  }
  let correlation_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
  let kind = match buf[8] {
    ENVELOPE_KIND_REQUEST => EnvelopeKind::Request,
    ENVELOPE_KIND_RESPONSE => EnvelopeKind::Response,
    tag => bail!("invalid envelope kind tag: {tag}"),
  };
  let target_thread = ThreadId(u32::from_le_bytes(buf[9..13].try_into().unwrap()));
  let body = buf[13..].to_vec();
  Ok(Envelope {
    correlation_id,
    kind,
    target_thread,
    body,
  })
}
```

Add tests:
```rust
  #[test]
  fn round_trips_a_request_envelope() {
    let env = Envelope {
      correlation_id: 42,
      kind: EnvelopeKind::Request,
      target_thread: seisin_core::authority::ThreadId(3),
      body: encode_request(&Request::Recall {
        datum_id: DatumId::new(),
      }),
    };
    assert_eq!(decode_envelope(&encode_envelope(&env)).unwrap(), env);
  }

  #[test]
  fn round_trips_a_response_envelope() {
    let env = Envelope {
      correlation_id: 7,
      kind: EnvelopeKind::Response,
      target_thread: seisin_core::authority::ThreadId(0),
      body: encode_response(&Response::Granted),
    };
    assert_eq!(decode_envelope(&encode_envelope(&env)).unwrap(), env);
  }

  #[test]
  fn rejects_an_envelope_too_short_for_its_header() {
    assert!(decode_envelope(&[0u8; 5]).is_err());
  }

  #[test]
  fn rejects_an_envelope_with_an_invalid_kind_tag() {
    let mut buf = encode_envelope(&Envelope {
      correlation_id: 1,
      kind: EnvelopeKind::Request,
      target_thread: seisin_core::authority::ThreadId(0),
      body: vec![],
    });
    buf[8] = 99;
    assert!(decode_envelope(&buf).is_err());
  }
```

- [ ] **Step 4: Run the full test suite for this crate**

Run: `cargo test -p seisin-protocol`
Expected: PASS (all tests, including the 4 new envelope tests).

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-protocol/src/lib.rs
git commit -m "feat: add Acquire/Recall requests, Granted/Released responses, and the peer-link envelope codec"
git push
```

---

### Task 2: `peer_link.rs` — the multiplexed connection primitive

**Files:**
- Create: `crates/seisin-node/src/peer_link.rs`
- Modify: `crates/seisin-node/src/lib.rs` (add `pub mod peer_link;`)

**Interfaces:**
- Consumes: `Envelope`/`EnvelopeKind`/`encode_envelope`/`decode_envelope`/`Request`/`Response`/`encode_request`/`decode_request`/`encode_response`/`decode_response` from Task 1; `read_frame`/`write_frame` (already existing).
- Produces: `pub struct PeerLink`, `pub fn spawn(stream: TcpStream, on_request: Arc<dyn Fn(ThreadId, Request, Arc<PeerLink>, u64) + Send + Sync>) -> Arc<PeerLink>`, `PeerLink::call(&self, target_thread: ThreadId, request: Request, on_response: Box<dyn FnOnce(Response) + Send>)`, `PeerLink::respond(&self, correlation_id: u64, response: Response)`.

This module deliberately knows nothing about `WorkerMessage` — it's a reusable, protocol-level connection primitive; `worker.rs`/`pool.rs` (Tasks 4-5) are what translate its callbacks into worker-internal messages. This keeps it fully unit-testable with a real TCP loopback pair and no `WorkerPool` involved.

- [ ] **Step 1: Write `peer_link.rs`**

```rust
//! A single persistent, multiplexed connection to one remote node,
//! carrying `Acquire`/`Recall` traffic for every local thread's
//! cross-node collation needs — see the design doc's "Server-to-Server
//! Connection Multiplexing" section. One `PeerLink` per node pair,
//! never one per thread, keeps connection count at O(servers), not
//! O(threads^2).
//!
//! This module is deliberately unaware of `WorkerMessage` — callers
//! supply an `on_request` callback that translates an incoming
//! `Request` into whatever local dispatch they need; `PeerLink` itself
//! only ever deals in `Request`/`Response`/`Envelope`.

use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use seisin_core::authority::ThreadId;
use seisin_protocol::{
  decode_envelope, decode_request, decode_response, encode_envelope, encode_request,
  encode_response, read_frame, write_frame, Envelope, EnvelopeKind, Request, Response,
};

pub struct PeerLink {
  writer_tx: Sender<Vec<u8>>,
  pending: Mutex<HashMap<u64, Box<dyn FnOnce(Response) + Send>>>,
  next_correlation_id: AtomicU64,
}

impl PeerLink {
  /// Sends `request` to `target_thread` on the peer, invoking
  /// `on_response` once a reply arrives — or, if the connection drops
  /// before one does, with a synthetic `Response::OpError`. Never
  /// blocks the caller; `on_response` runs on this link's reader
  /// thread, so it should do nothing more than post a message
  /// somewhere (matching the same pattern `NativeLock`'s `on_granted`
  /// callback already uses).
  pub fn call(&self, target_thread: ThreadId, request: Request, on_response: Box<dyn FnOnce(Response) + Send>) {
    let correlation_id = self.next_correlation_id.fetch_add(1, Ordering::Relaxed);
    self
      .pending
      .lock()
      .unwrap()
      .insert(correlation_id, on_response);
    let envelope = Envelope {
      correlation_id,
      kind: EnvelopeKind::Request,
      target_thread,
      body: encode_request(&request),
    };
    let _ = self.writer_tx.send(encode_envelope(&envelope));
  }

  /// Replies to a request this link's `on_request` callback was
  /// previously handed, tagged with the same `correlation_id` it
  /// arrived with.
  pub fn respond(&self, correlation_id: u64, response: Response) {
    let envelope = Envelope {
      correlation_id,
      kind: EnvelopeKind::Response,
      target_thread: ThreadId(0), // unused for responses
      body: encode_response(&response),
    };
    let _ = self.writer_tx.send(encode_envelope(&envelope));
  }
}

/// Spawns the writer and reader threads for an already-connected
/// `stream` and returns the shared handle both sides use.
/// `on_request` is invoked (from the reader thread) for every incoming
/// request frame, with `(target_thread, request, link, correlation_id)`
/// — implementations should dispatch locally and eventually call
/// `link.respond(correlation_id, ...)`.
pub fn spawn(
  stream: TcpStream,
  on_request: Arc<dyn Fn(ThreadId, Request, Arc<PeerLink>, u64) + Send + Sync>,
) -> Arc<PeerLink> {
  let (writer_tx, writer_rx) = mpsc::channel::<Vec<u8>>();
  let link = Arc::new(PeerLink {
    writer_tx,
    pending: Mutex::new(HashMap::new()),
    next_correlation_id: AtomicU64::new(0),
  });

  let mut write_stream = stream
    .try_clone()
    .expect("failed to clone peer-link stream for its writer half");
  thread::spawn(move || {
    for envelope_bytes in writer_rx {
      if write_frame(&mut write_stream, &envelope_bytes).is_err() {
        break;
      }
    }
  });

  let mut read_stream = stream;
  let reader_link = Arc::clone(&link);
  thread::spawn(move || {
    loop {
      let frame = match read_frame(&mut read_stream) {
        Ok(f) => f,
        Err(_) => break,
      };
      let envelope = match decode_envelope(&frame) {
        Ok(e) => e,
        Err(_) => continue,
      };
      match envelope.kind {
        EnvelopeKind::Response => {
          let callback = reader_link
            .pending
            .lock()
            .unwrap()
            .remove(&envelope.correlation_id);
          if let Some(callback) = callback {
            if let Ok(response) = decode_response(&envelope.body) {
              callback(response);
            }
          }
        }
        EnvelopeKind::Request => {
          if let Ok(request) = decode_request(&envelope.body) {
            on_request(
              envelope.target_thread,
              request,
              Arc::clone(&reader_link),
              envelope.correlation_id,
            );
          }
        }
      }
    }
    // Connection dropped: fail every still-pending call so callers
    // don't hang forever waiting for a response that will never
    // arrive now. (Proactive crash detection beyond this reactive
    // backstop is Part 2b's scope.)
    let pending = std::mem::take(&mut *reader_link.pending.lock().unwrap());
    for (_, callback) in pending {
      callback(Response::OpError {
        message: "peer link disconnected".to_string(),
      });
    }
  });

  link
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::net::TcpListener;
  use std::sync::mpsc as std_mpsc;
  use std::time::Duration;

  use seisin_core::datum::DatumId;

  /// Connects a real loopback TCP pair and spawns a `PeerLink` on each
  /// end, letting `on_request_b` decide how side B answers incoming
  /// requests.
  fn connected_pair(
    on_request_a: Arc<dyn Fn(ThreadId, Request, Arc<PeerLink>, u64) + Send + Sync>,
    on_request_b: Arc<dyn Fn(ThreadId, Request, Arc<PeerLink>, u64) + Send + Sync>,
  ) -> (Arc<PeerLink>, Arc<PeerLink>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let accept_thread = thread::spawn(move || {
      let (stream, _) = listener.accept().unwrap();
      spawn(stream, on_request_b)
    });
    let client_stream = TcpStream::connect(addr).unwrap();
    let link_a = spawn(client_stream, on_request_a);
    let link_b = accept_thread.join().unwrap();
    (link_a, link_b)
  }

  fn no_op_dispatch() -> Arc<dyn Fn(ThreadId, Request, Arc<PeerLink>, u64) + Send + Sync> {
    Arc::new(|_thread, _request, _link, _cid| {})
  }

  #[test]
  fn a_call_reaches_the_peers_on_request_callback() {
    let (tx, rx) = std_mpsc::channel();
    let on_request_b: Arc<dyn Fn(ThreadId, Request, Arc<PeerLink>, u64) + Send + Sync> =
      Arc::new(move |thread, request, link, cid| {
        let _ = tx.send((thread, request));
        link.respond(cid, Response::Granted);
      });
    let (link_a, _link_b) = connected_pair(no_op_dispatch(), on_request_b);

    let (reply_tx, reply_rx) = std_mpsc::channel();
    let datum_id = DatumId::new();
    let op_id = DatumId::new();
    link_a.call(
      ThreadId(3),
      Request::Acquire {
        op_id,
        datum_id,
        requester_node: seisin_core::authority::NodeId(1),
        requester_thread: ThreadId(0),
      },
      Box::new(move |response| {
        let _ = reply_tx.send(response);
      }),
    );

    let (received_thread, received_request) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(received_thread, ThreadId(3));
    assert_eq!(
      received_request,
      Request::Acquire {
        op_id,
        datum_id,
        requester_node: seisin_core::authority::NodeId(1),
        requester_thread: ThreadId(0),
      }
    );
    assert_eq!(reply_rx.recv_timeout(Duration::from_secs(5)).unwrap(), Response::Granted);
  }

  #[test]
  fn many_concurrent_calls_each_get_routed_back_to_the_right_caller() {
    let on_request_b: Arc<dyn Fn(ThreadId, Request, Arc<PeerLink>, u64) + Send + Sync> =
      Arc::new(|_thread, _request, link, cid| {
        link.respond(cid, Response::Granted);
      });
    let (link_a, _link_b) = connected_pair(no_op_dispatch(), on_request_b);

    let mut receivers = Vec::new();
    for _ in 0..20 {
      let (tx, rx) = std_mpsc::channel();
      link_a.call(
        ThreadId(0),
        Request::Recall {
          datum_id: DatumId::new(),
        },
        Box::new(move |response| {
          let _ = tx.send(response);
        }),
      );
      receivers.push(rx);
    }
    for rx in receivers {
      assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), Response::Granted);
    }
  }

  #[test]
  fn a_dropped_connection_fails_pending_calls_instead_of_hanging() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let accept_thread = thread::spawn(move || {
      let (stream, _) = listener.accept().unwrap();
      // Immediately drop the accepted stream, simulating the peer
      // vanishing without ever answering.
      drop(stream);
    });
    let client_stream = TcpStream::connect(addr).unwrap();
    let link = spawn(client_stream, no_op_dispatch());
    accept_thread.join().unwrap();

    let (tx, rx) = std_mpsc::channel();
    link.call(
      ThreadId(0),
      Request::Recall {
        datum_id: DatumId::new(),
      },
      Box::new(move |response| {
        let _ = tx.send(response);
      }),
    );
    match rx.recv_timeout(Duration::from_secs(5)).unwrap() {
      Response::OpError { .. } => {}
      other => panic!("expected OpError on disconnect, got {other:?}"),
    }
  }
}
```

- [ ] **Step 2: Wire the module into the crate**

In `crates/seisin-node/src/lib.rs`, add `peer_link` to the module list (keep alphabetical):
```rust
#![deny(warnings)]

pub mod collation;
pub mod config;
pub mod gossip_client;
pub mod gossip_server;
pub mod gossip_state;
pub mod peer_link;
pub mod pool;
pub mod server;
pub mod worker;
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p seisin-node --lib peer_link::`
Expected: PASS (3 tests).

- [ ] **Step 4: Commit and push**

```bash
git add crates/seisin-node/src/peer_link.rs crates/seisin-node/src/lib.rs
git commit -m "feat: add peer_link, the multiplexed node-to-node connection primitive"
git push
```

---

### Task 3: `PeerLinkRegistry` — one link per remote node, established at startup

**Files:**
- Modify: `crates/seisin-node/src/peer_link.rs`

**Interfaces:**
- Consumes: `PeerLink`/`spawn` from Task 2.
- Produces: `pub struct PeerLinkRegistry`, `pub fn start(listener: TcpListener, self_node_id: NodeId, address_book: Arc<HashMap<NodeId, String>>, on_request: Arc<dyn Fn(ThreadId, Request, Arc<PeerLink>, u64) + Send + Sync>) -> Arc<PeerLinkRegistry>`, `PeerLinkRegistry::get(&self, node_id: NodeId) -> Arc<PeerLink>`.

Every peer-link connection starts with a tiny handshake: right after connecting, the dialing side writes its own `NodeId` as an 8-byte little-endian preamble (before any framed envelope traffic), so the accepting side knows which peer just connected without needing to guess from the socket address.

- [ ] **Step 1: Write the registry, on top of `peer_link.rs`**

Append to `crates/seisin-node/src/peer_link.rs` (after the existing `spawn` function, before `#[cfg(test)]`):

```rust
use std::collections::HashMap as StdHashMap;
use std::io::{Read, Write};
use std::net::TcpListener;

use seisin_core::authority::NodeId;

/// One `PeerLink` per remote node, dialed eagerly at startup rather
/// than lazily on first need — a worker thread calling `get` must
/// never block on a fresh `TcpStream::connect`, so every connection
/// this node will ever originate is already established before any
/// traffic flows. The lower `NodeId` in a pair always dials the higher
/// one; the higher one only ever accepts, so exactly one connection
/// exists per pair regardless of which side needs it first.
pub struct PeerLinkRegistry {
  links: StdHashMap<NodeId, Arc<PeerLink>>,
}

impl PeerLinkRegistry {
  /// Dials every peer in `address_book` with a larger `NodeId` than
  /// `self_node_id`, then starts accepting inbound connections from
  /// smaller-`NodeId` peers on `listener` — the deterministic "lower
  /// dials higher" rule below is what guarantees exactly one
  /// connection per pair regardless of which side needs it first.
  /// Returns once every outbound dial has completed; the inbound
  /// accept loop keeps running in the background afterward.
  pub fn start(
    listener: TcpListener,
    self_node_id: NodeId,
    address_book: Arc<StdHashMap<NodeId, String>>,
    on_request: Arc<dyn Fn(ThreadId, Request, Arc<PeerLink>, u64) + Send + Sync>,
  ) -> Arc<Mutex<PeerLinkRegistry>> {
    let registry = Arc::new(Mutex::new(PeerLinkRegistry {
      links: StdHashMap::new(),
    }));

    // Dial every peer with a larger NodeId — the deterministic
    // "who dials whom" rule that avoids both sides racing to connect.
    // A peer that isn't reachable yet (not started, or started after
    // this node) is skipped rather than treated as fatal — this node
    // still needs to finish starting up. It simply won't have a link
    // to that peer until something re-establishes one; a later
    // Acquire/Recall targeting it panics at `get` (below), an accepted
    // v1 gap — reconnection/backoff is Part 2b's crash-handling scope,
    // not this plan's.
    for (&peer_id, address) in address_book.iter() {
      if peer_id <= self_node_id {
        continue;
      }
      let Ok(mut stream) = TcpStream::connect(address) else {
        continue;
      };
      if stream.write_all(&self_node_id.0.to_le_bytes()).is_err() {
        continue;
      }
      let link = spawn(stream, Arc::clone(&on_request));
      registry.lock().unwrap().links.insert(peer_id, link);
    }

    // Accept inbound connections from every peer with a smaller
    // NodeId, reading each one's handshake preamble to learn who it is.
    {
      let registry = Arc::clone(&registry);
      thread::spawn(move || {
        for stream in listener.incoming() {
          let mut stream = match stream {
            Ok(s) => s,
            Err(_) => continue,
          };
          let mut preamble = [0u8; 8];
          if stream.read_exact(&mut preamble).is_err() {
            continue;
          }
          let peer_id = NodeId(u64::from_le_bytes(preamble));
          let link = spawn(stream, Arc::clone(&on_request));
          registry.lock().unwrap().links.insert(peer_id, link);
        }
      });
    }

    registry
  }
}

impl PeerLinkRegistry {
  /// Returns the link to `node_id`.
  ///
  /// # Panics
  /// Panics if no link to `node_id` exists — expected to normally be
  /// dialed or accepted at startup (see `start`), but a peer that was
  /// unreachable at dial time, or joined dynamically after this node
  /// started, has no entry either. Either way, an `Acquire`/`Recall`
  /// that hits this is a case this plan doesn't yet handle gracefully
  /// — reconnection/backoff is Part 2b's crash-handling scope.
  pub fn get(&self, node_id: NodeId) -> Arc<PeerLink> {
    Arc::clone(
      self
        .links
        .get(&node_id)
        .unwrap_or_else(|| panic!("no peer-link connection to node {node_id:?}")),
    )
  }
}
```

- [ ] **Step 2: Write a test proving a 3-node handshake converges correctly**

Add to `peer_link.rs`'s `mod tests`:
```rust
  #[test]
  fn registry_connects_every_pair_regardless_of_dial_direction() {
    use seisin_core::authority::NodeId;

    let node_a = NodeId(1);
    let node_b = NodeId(2);
    let node_c = NodeId(3);

    let listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").unwrap();
    let listener_c = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr_a = listener_a.local_addr().unwrap().to_string();
    let addr_b = listener_b.local_addr().unwrap().to_string();
    let addr_c = listener_c.local_addr().unwrap().to_string();

    let mut address_book = std::collections::HashMap::new();
    address_book.insert(node_a, addr_a);
    address_book.insert(node_b, addr_b);
    address_book.insert(node_c, addr_c);
    let address_book = Arc::new(address_book);

    // Start c and b first (they only accept from a smaller NodeId, so
    // nothing needs to dial yet), then a last (which dials both).
    let registry_c = PeerLinkRegistry::start(listener_c, node_c, Arc::clone(&address_book), no_op_dispatch());
    let registry_b = PeerLinkRegistry::start(listener_b, node_b, Arc::clone(&address_book), no_op_dispatch());
    let registry_a = PeerLinkRegistry::start(listener_a, node_a, Arc::clone(&address_book), no_op_dispatch());

    // Give the accept threads a moment to register the inbound
    // handshakes that a's and b's dial-outs just triggered.
    thread::sleep(Duration::from_millis(200));

    // Every pair connects, regardless of which side happened to dial:
    // a dials both b and c (1 < 2, 1 < 3); b dials c (2 < 3); c dials
    // no one (nothing in the set is greater than 3).
    registry_a.lock().unwrap().get(node_b);
    registry_a.lock().unwrap().get(node_c);
    registry_b.lock().unwrap().get(node_a);
    registry_b.lock().unwrap().get(node_c);
    registry_c.lock().unwrap().get(node_a);
    registry_c.lock().unwrap().get(node_b);
  }
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p seisin-node --lib peer_link::`
Expected: PASS (4 tests).

- [ ] **Step 4: Commit and push**

```bash
git add crates/seisin-node/src/peer_link.rs
git commit -m "feat: add PeerLinkRegistry, eager startup-time connection establishment"
git push
```

---

### Task 4: `worker.rs` — route `Acquire`/`Recall` across node boundaries

**Files:**
- Modify: `crates/seisin-node/src/worker.rs`

**Interfaces:**
- Consumes: `PeerLink`/`PeerLinkRegistry` from Tasks 2-3.
- Produces: `WorkerMessage::Acquire`'s `requester_inbox` field replaced by `reply: AcquireReply`; `WorkerMessage::Recall`'s `native_home_inbox` field replaced by `reply: RecallReply`; new `pub(crate) enum AcquireReply` and `pub(crate) enum RecallReply`; `WorkerHandle::spawn` gains a `peer_links: Arc<Mutex<PeerLinkRegistry>>` parameter; `send_acquire` and the `Recall`-issuing code in the `Acquire` handler both gain a `peer_links` parameter and route cross-node calls through it.

This task removes the workspace's ability to compile until Task 5 (`pool.rs`) is updated too — same transient-breakage pattern Part 1 used.

- [ ] **Step 1: Remove `#![deny(warnings)]` temporarily**

In `crates/seisin-node/src/lib.rs`, remove the `#![deny(warnings)]` line.

- [ ] **Step 2: Add the reply-target enums and update `WorkerMessage`**

In `crates/seisin-node/src/worker.rs`, add imports:
```rust
use std::sync::Mutex as StdMutex;

use crate::peer_link::{PeerLink, PeerLinkRegistry};
```

Add, right after the `WorkerMessage` enum's closing brace (before `struct OpRecord`):
```rust
/// Where an `Acquire`'s eventual grant should be delivered — a local
/// `WorkerMessage` send for a same-node requester, or a peer-link
/// response for a requester on a different node.
pub(crate) enum AcquireReply {
  Local(Sender<WorkerMessage>),
  Remote(Arc<PeerLink>, u64),
}

impl AcquireReply {
  fn grant(self, op_id: DatumId, datum_id: DatumId) {
    match self {
      AcquireReply::Local(inbox) => {
        let _ = inbox.send(WorkerMessage::AcquireGranted { op_id, datum_id });
      }
      AcquireReply::Remote(link, correlation_id) => {
        link.respond(correlation_id, seisin_protocol::Response::Granted);
      }
    }
  }
}

/// Where a `Recall`'s eventual ack should be delivered — same shape as
/// `AcquireReply`, for the same reason.
pub(crate) enum RecallReply {
  Local(Sender<WorkerMessage>),
  Remote(Arc<PeerLink>, u64),
}

impl RecallReply {
  fn ack(self, datum_id: DatumId) {
    match self {
      RecallReply::Local(inbox) => {
        let _ = inbox.send(WorkerMessage::Release { datum_id });
      }
      RecallReply::Remote(link, correlation_id) => {
        link.respond(correlation_id, seisin_protocol::Response::Released);
      }
    }
  }
}
```

Change the `WorkerMessage::Acquire` and `WorkerMessage::Recall` variants:
```rust
  Acquire {
    op_id: DatumId,
    datum_id: DatumId,
    requester_node: NodeId,
    requester_thread: ThreadId,
    reply: AcquireReply,
  },
  AcquireGranted { op_id: DatumId, datum_id: DatumId },
  Recall {
    datum_id: DatumId,
    reply: RecallReply,
  },
```

- [ ] **Step 3: Update `WorkerHandle::spawn`'s signature and the `Acquire`/`Recall` handlers**

Change `WorkerHandle::spawn`'s signature to take one more parameter:
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
  ) -> Self {
```

Inside the thread closure, change the `RunOp` handler's calls to `send_acquire` to pass `&peer_links` (it already passes `&ring`, `&peers`, etc. — add `&peer_links` right after `&peers`):
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
              );
            }
```

Change the `Acquire` handler:
```rust
          WorkerMessage::Acquire {
            op_id,
            datum_id,
            requester_node,
            requester_thread,
            reply,
          } => {
            let lock = native_locks.entry(datum_id).or_default();
            let on_granted = Box::new(move || {
              reply.grant(op_id, datum_id);
            });
            let outcome = lock.request(op_id, requester_node, requester_thread, on_granted);
            if let AcquireOutcome::RecallNeeded(holder) = outcome {
              let recall_reply = RecallReply::Local(join_sender.clone());
              if holder.node_id == self_node_id {
                let _ = peers[holder.thread_id.0 as usize].send(WorkerMessage::Recall {
                  datum_id,
                  reply: recall_reply,
                });
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
            }
          }
```

Change the `Recall` handler:
```rust
          WorkerMessage::Recall { datum_id, reply } => {
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
            reply.ack(datum_id);
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
              );
            }
          }
```

- [ ] **Step 4: Update `send_acquire` to route cross-node**

Replace `send_acquire`'s definition:
```rust
/// Sends an `Acquire` for `datum_id` on behalf of `op_id` to whichever
/// thread `ring.native()` currently names. Same-node targets (even
/// this thread itself) go through a plain, non-blocking message send;
/// different-node targets go through that node's peer-link. Either
/// way, the eventual grant arrives back as an ordinary
/// `WorkerMessage::AcquireGranted` posted into this thread's own
/// inbox — the calling thread's loop never blocks on the outcome.
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

- [ ] **Step 5: Update `worker.rs`'s own tests for the new `spawn` signature**

In `worker.rs`'s `mod tests`, add the import:
```rust
  use crate::peer_link::PeerLinkRegistry;
```

Add this helper (used by `spawn_test_pool` below):
```rust
  fn empty_peer_links() -> Arc<StdMutex<PeerLinkRegistry>> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    PeerLinkRegistry::start(
      listener,
      NodeId(1),
      Arc::new(HashMap::new()),
      Arc::new(|_thread, _request, _link, _cid| {}),
    )
  }
```

Update `spawn_test_pool` to construct and thread through a shared registry:
```rust
  fn spawn_test_pool(
    thread_count: u32,
    ring: Arc<RwLock<Ring>>,
    ops: OpRegistry,
  ) -> Vec<WorkerHandle> {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let ops = Arc::new(ops);
    let peer_links = empty_peer_links();
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
          Arc::clone(&peer_links),
        )
      })
      .collect()
  }
```

- [ ] **Step 6: Attempt to run the tests**

Run: `cargo test -p seisin-node --lib worker::`
Expected: FAIL to compile — `pool.rs` still calls the old 7-argument `WorkerHandle::spawn` and `WorkerPool::spawn`, and `server.rs` is unaffected but `pool.rs` won't build. Proceed to Task 5.

- [ ] **Step 7: Commit and push**

```bash
git add crates/seisin-node/src/worker.rs crates/seisin-node/src/lib.rs
git commit -m "feat: route Acquire/Recall across node boundaries via peer_link"
git push
```

---

### Task 5: `pool.rs` — construct the registry and thread it through

**Files:**
- Modify: `crates/seisin-node/src/pool.rs`

**Interfaces:**
- Consumes: `PeerLinkRegistry::start` from Task 3; `WorkerHandle::spawn`'s new signature from Task 4.
- Produces: `WorkerPool::spawn(store, thread_count, ops, ring, self_node_id, peer_link_listener: TcpListener, peer_link_address_book: Arc<HashMap<NodeId, String>>) -> Self` — the registry is built *inside* `spawn`, once `peers` exists, resolving the circular dependency (the registry's `on_request` dispatch closure needs `peers`; each worker thread needs the registry) by sequencing: build `peers` → build the registry → spawn workers.

This is where full workspace compilation is restored for `seisin-node`'s own lib — `cargo test -p seisin-node --lib` must pass by the end of this task.

- [ ] **Step 1: Rewrite `WorkerPool::spawn`**

In `crates/seisin-node/src/pool.rs`, add imports:
```rust
use std::net::TcpListener;

use crate::peer_link::{PeerLink, PeerLinkRegistry};
use crate::worker::{AcquireReply, RecallReply};
```

Replace `WorkerPool`'s struct and `spawn`:
```rust
pub struct WorkerPool {
  handles: Vec<WorkerHandle>,
  ring: Arc<RwLock<Ring>>,
}

impl WorkerPool {
  /// Spawns `thread_count` interconnected worker threads sharing one
  /// `store` and `ops` registry, plus the node-to-node peer-link
  /// registry every thread uses for cross-node `Acquire`/`Recall`
  /// traffic — built here, after `peers` exists but before any worker
  /// thread starts, since the registry's own incoming-request dispatch
  /// needs `peers` to route by `ThreadId`.
  #[allow(clippy::too_many_arguments)]
  pub fn spawn(
    store: Arc<dyn Store>,
    thread_count: u32,
    ops: Arc<OpRegistry>,
    ring: Arc<RwLock<Ring>>,
    self_node_id: NodeId,
    peer_link_listener: TcpListener,
    peer_link_address_book: Arc<HashMap<NodeId, String>>,
  ) -> Self {
    let mut senders = Vec::with_capacity(thread_count as usize);
    let mut receivers = Vec::with_capacity(thread_count as usize);
    for _ in 0..thread_count {
      let (tx, rx) = mpsc::channel::<WorkerMessage>();
      senders.push(tx);
      receivers.push(rx);
    }
    let peers = Arc::new(senders);

    let dispatch_peers = Arc::clone(&peers);
    let on_request: Arc<dyn Fn(ThreadId, seisin_protocol::Request, Arc<PeerLink>, u64) + Send + Sync> =
      Arc::new(move |target_thread, request, link, correlation_id| {
        let message = match request {
          seisin_protocol::Request::Acquire {
            op_id,
            datum_id,
            requester_node,
            requester_thread,
          } => WorkerMessage::Acquire {
            op_id,
            datum_id,
            requester_node,
            requester_thread,
            reply: AcquireReply::Remote(Arc::clone(&link), correlation_id),
          },
          seisin_protocol::Request::Recall { datum_id } => WorkerMessage::Recall {
            datum_id,
            reply: RecallReply::Remote(Arc::clone(&link), correlation_id),
          },
          seisin_protocol::Request::Op { .. } => return, // client-only; never sent over a peer-link
        };
        let _ = dispatch_peers[target_thread.0 as usize].send(message);
      });
    let peer_links = PeerLinkRegistry::start(
      peer_link_listener,
      self_node_id,
      peer_link_address_book,
      on_request,
    );

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
    Self { handles, ring }
  }
```

`WorkerMessage`, `AcquireReply`, and `RecallReply` are `pub(crate)` (Task 4), so `pool.rs` can name them via `crate::worker::...` even though `worker.rs` doesn't `pub use` them at the crate root.

- [ ] **Step 2: Update `pool.rs`'s own tests**

Add the import:
```rust
  use std::net::TcpListener;
```

Add a helper that binds a throwaway peer-link listener with an empty address book (matching every single-node test in this file — none of them need real peer-link traffic):
```rust
  fn no_peers() -> (TcpListener, Arc<HashMap<NodeId, String>>) {
    (
      TcpListener::bind("127.0.0.1:0").unwrap(),
      Arc::new(HashMap::new()),
    )
  }
```

Update every `WorkerPool::spawn(...)` call site in this file's tests to append the two new arguments, e.g.:
```rust
  fn test_pool(thread_count: u32) -> WorkerPool {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(
      NodeId(1),
      thread_count,
    )])));
    let (listener, address_book) = no_peers();
    WorkerPool::spawn(
      Arc::new(InMemoryStore::new()),
      thread_count,
      Arc::new(OpRegistry::new()),
      ring,
      NodeId(1),
      listener,
      address_book,
    )
  }
```

Apply the same two-argument addition (`listener, address_book`, using a fresh `no_peers()` call each time) to the other two `WorkerPool::spawn(...)` call sites in this file's tests (`run_op_executes_a_registered_op_and_returns_its_result` and `writes_from_one_op_are_visible_to_a_later_op_regardless_of_thread`).

- [ ] **Step 3: Run the full `seisin-node` lib test suite**

Run: `cargo test -p seisin-node --lib`
Expected: FAIL to compile — `server.rs` doesn't reference the changed APIs directly, but `main.rs` and the integration tests still call the old `WorkerPool::spawn` signature; this crate's *lib* target (which excludes `main.rs` and `tests/`) should actually compile and pass now. If it doesn't, the remaining errors are in `pool.rs`/`worker.rs` themselves — fix those before proceeding; don't move on with a broken lib.

- [ ] **Step 4: Commit and push**

```bash
git add crates/seisin-node/src/pool.rs
git commit -m "feat: construct PeerLinkRegistry inside WorkerPool::spawn and thread it through"
git push
```

---

### Task 6: `server.rs` — allow genuine cross-node collation

**Files:**
- Modify: `crates/seisin-node/src/server.rs`

**Interfaces:**
- Consumes: nothing new — this task only changes `handle_op_request`'s behavior.

- [ ] **Step 1: Stop rejecting multi-node ops**

In `crates/seisin-node/src/server.rs`, change `handle_op_request` from:
```rust
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
```
to:
```rust
  // A single-node op whose one native node isn't this one still takes
  // the cheaper redirect path (the client reconnects directly to the
  // node that already has everything it needs) — this is unchanged
  // from Part 1. Only once an op's datums are genuinely spread across
  // more than one node does it now fall through to local dispatch,
  // relying on the destination thread's own Acquire/Recall machinery
  // to pull the remote ones in — no more outright rejection.
  if native_nodes.len() == 1 {
    let only_node = *native_nodes.iter().next().unwrap();
    if only_node != self_node_id {
      return match address_book.get(&only_node) {
        Some(address) => Response::Redirect {
          address: address.clone(),
        },
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
```

(The `HashSet` import and `native_nodes` computation above this block are unchanged.)

- [ ] **Step 2: Verify the crate still builds**

Run: `cargo build -p seisin-node --lib`
Expected: builds with no errors (this file doesn't touch any of Tasks 4-5's changed signatures).

- [ ] **Step 3: Commit and push**

```bash
git add crates/seisin-node/src/server.rs
git commit -m "feat: allow genuinely cross-node ops to dispatch locally instead of rejecting them"
git push
```

---

### Task 7: `config.rs` and `main.rs` — a peer-link address per member, and startup wiring

**Files:**
- Modify: `crates/seisin-node/src/config.rs`
- Modify: `crates/seisin-node/src/main.rs`

**Interfaces:**
- Produces: `MemberConfig` gains a `peer_link_address: String` field.

- [ ] **Step 1: Add `peer_link_address` to `MemberConfig`**

In `crates/seisin-node/src/config.rs`, change:
```rust
#[derive(Debug, Deserialize)]
pub struct MemberConfig {
  pub node_id: u64,
  pub address: String,
  pub gossip_address: String,
  pub thread_count: u32,
}
```
to:
```rust
#[derive(Debug, Deserialize)]
pub struct MemberConfig {
  pub node_id: u64,
  pub address: String,
  pub gossip_address: String,
  pub peer_link_address: String,
  pub thread_count: u32,
}
```

Update the test config sample and its assertions:
```rust
  const SAMPLE: &str = r#"
(
    self_node_id: 1,
    members: [
        (node_id: 1, address: "127.0.0.1:7878", gossip_address: "127.0.0.1:8878", peer_link_address: "127.0.0.1:9878", thread_count: 2),
        (node_id: 2, address: "127.0.0.1:7879", gossip_address: "127.0.0.1:8879", peer_link_address: "127.0.0.1:9879", thread_count: 4),
    ],
)
"#;
```
(The rest of `config.rs`'s tests are unaffected — none assert on `peer_link_address` directly.)

Run: `cargo test -p seisin-node --lib config::`
Expected: PASS (3 tests, unchanged in count).

- [ ] **Step 2: Wire the peer-link listener and address book into `main.rs`**

In `crates/seisin-node/src/main.rs`, add the peer-link address lookup alongside the existing `self_gossip_address` lookup:
```rust
  let self_peer_link_address = config
    .members
    .iter()
    .find(|m| m.node_id == config.self_node_id)
    .map(|m| m.peer_link_address.clone())
    .with_context(|| {
      format!(
        "self_node_id {} not present in members",
        config.self_node_id
      )
    })?;
```

Build the peer-link address book alongside the existing client `address_book`:
```rust
  let peer_link_address_book: HashMap<NodeId, String> = config
    .members
    .iter()
    .map(|m| (NodeId(m.node_id), m.peer_link_address.clone()))
    .collect();
  let peer_link_address_book = Arc::new(peer_link_address_book);
```

Bind the peer-link listener before constructing the pool (it must exist before `WorkerPool::spawn` can use it):
```rust
  let peer_link_listener = TcpListener::bind(&self_peer_link_address)
    .with_context(|| format!("failed to bind {self_peer_link_address}"))?;
  println!("seisin-node {self_node_id:?} peer-link listener on {self_peer_link_address}");
```

Update the `WorkerPool::spawn` call:
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
    peer_link_listener,
    peer_link_address_book,
  ));
```

- [ ] **Step 3: Verify the binary builds**

Run: `cargo build -p seisin-node --bin seisin-node`
Expected: builds with no errors.

- [ ] **Step 4: Commit and push**

```bash
git add crates/seisin-node/src/config.rs crates/seisin-node/src/main.rs
git commit -m "feat: add peer_link_address to node config and wire startup connection establishment"
git push
```

---

### Task 8: Update existing integration tests for the new `WorkerPool::spawn` signature

**Files:**
- Modify: `crates/seisin-node/tests/integration_wire_protocol.rs`
- Modify: `crates/seisin-node/tests/integration_multi_node_routing.rs`
- Modify: `crates/seisin-node/tests/integration_gossip_failure_detection.rs`
- Modify: `crates/seisin-node/tests/integration_op_collation.rs`
- Modify: `crates/seisin-node/tests/integration_wound_wait.rs`

**Interfaces:**
- Consumes: `WorkerPool::spawn`'s new 7-argument signature from Task 5.

Every one of these files calls `WorkerPool::spawn` with the old 5-argument form. Each needs a bound peer-link listener and an address book passed as the two new trailing arguments.

- [ ] **Step 1: `integration_wire_protocol.rs` (single node, one call site)**

In `start_test_server`, add before the `WorkerPool::spawn` call:
```rust
  let peer_link_listener = TcpListener::bind("127.0.0.1:0").unwrap();
```
and change the `WorkerPool::spawn(...)` call to append `peer_link_listener, Arc::new(HashMap::new())`.

- [ ] **Step 2: `integration_op_collation.rs` and `integration_wound_wait.rs` (single node, one call site each)**

Same pattern as Step 1: bind a throwaway `TcpListener::bind("127.0.0.1:0").unwrap()` right before each file's `WorkerPool::spawn` call, and append `peer_link_listener, Arc::new(HashMap::new())` to the call.

- [ ] **Step 3: `integration_multi_node_routing.rs` (two nodes, two call sites — this one now needs *real* peer-link addresses since both nodes exist in the same process)**

In `start_two_node_cluster`, bind two peer-link listeners and build a shared address book:
```rust
  let peer_link_listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
  let peer_link_listener_b = TcpListener::bind("127.0.0.1:0").unwrap();
  let peer_link_addr_a = peer_link_listener_a.local_addr().unwrap().to_string();
  let peer_link_addr_b = peer_link_listener_b.local_addr().unwrap().to_string();

  let mut peer_link_address_book = HashMap::new();
  peer_link_address_book.insert(node_a, peer_link_addr_a);
  peer_link_address_book.insert(node_b, peer_link_addr_b);
  let peer_link_address_book = Arc::new(peer_link_address_book);
```
Then append `peer_link_listener_a, Arc::clone(&peer_link_address_book)` to `pool_a`'s `WorkerPool::spawn` call, and `peer_link_listener_b, peer_link_address_book` (no need to clone again, it's the last use) to `pool_b`'s.

- [ ] **Step 4: `integration_gossip_failure_detection.rs` (two nodes, one call site inside `start_node`, called once per node)**

The member tuple shape grows a fifth element (peer-link address). Change `start_node`'s signature and body from:
```rust
fn start_node(node_id: NodeId, members: &[(NodeId, u32, String, String)]) {
  let this = members.iter().find(|m| m.0 == node_id).unwrap();
  let client_listener = TcpListener::bind(&this.2).unwrap();
  let gossip_listener = TcpListener::bind(&this.3).unwrap();

  let ring_members: Vec<(NodeId, u32)> = members.iter().map(|m| (m.0, m.1)).collect();
  let ring = Arc::new(RwLock::new(Ring::from_members(&ring_members)));

  let address_book: HashMap<NodeId, String> = members.iter().map(|m| (m.0, m.2.clone())).collect();
  let address_book = Arc::new(address_book);
```
to:
```rust
fn start_node(node_id: NodeId, members: &[(NodeId, u32, String, String, String)]) {
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
```

Change the `WorkerPool::spawn` call from:
```rust
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    this.1,
    Arc::new(ops),
    Arc::clone(&ring),
    node_id,
  ));
```
to:
```rust
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    this.1,
    Arc::new(ops),
    Arc::clone(&ring),
    node_id,
    peer_link_listener,
    peer_link_address_book,
  ));
```

Change the test's `members` construction from:
```rust
  let members = vec![
    (node_a, 2u32, addr_a.clone(), gossip_addr_a.clone()),
    (node_b, 2u32, addr_b.clone(), silent_gossip_addr_b),
  ];
```
to:
```rust
  let members = vec![
    (
      node_a,
      2u32,
      addr_a.clone(),
      gossip_addr_a.clone(),
      reserve_silent_address(),
    ),
    (
      node_b,
      2u32,
      addr_b.clone(),
      silent_gossip_addr_b,
      reserve_silent_address(),
    ),
  ];
```
(Node B's peer-link address is reserved-but-silent too, same as its gossip address — nothing about this test exercises real cross-node peer-link traffic, it only needs `WorkerPool::spawn` to have *some* valid, bindable address and an address book to construct.)

- [ ] **Step 5: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS (all tests across all crates).

- [ ] **Step 6: Commit and push**

```bash
git add crates/seisin-node/tests
git commit -m "test: update existing integration tests for the new WorkerPool::spawn signature"
git push
```

---

### Task 9: Cross-node collation integration test

**Files:**
- Create: `crates/seisin-node/tests/integration_cross_node_collation.rs`

**Interfaces:**
- Consumes: everything from Tasks 1-8.
- Produces: a real 2-node cluster proving a multi-datum op whose datums are natively split across both nodes actually collates and completes.

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

fn start_two_node_cluster() -> (String, String, Arc<RwLock<Ring>>) {
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

  let peer_link_listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
  let peer_link_listener_b = TcpListener::bind("127.0.0.1:0").unwrap();
  let peer_link_addr_a = peer_link_listener_a.local_addr().unwrap().to_string();
  let peer_link_addr_b = peer_link_listener_b.local_addr().unwrap().to_string();
  let mut peer_link_address_book = HashMap::new();
  peer_link_address_book.insert(node_a, peer_link_addr_a);
  peer_link_address_book.insert(node_b, peer_link_addr_b);
  let peer_link_address_book = Arc::new(peer_link_address_book);

  let mut ops = OpRegistry::new();
  ops.register(
    "touch_both",
    Box::new(|ctx, ids, _payload| {
      ctx.put(ids[0], b"touched".to_vec());
      ctx.put(ids[1], b"touched".to_vec());
      vec![]
    }),
  );
  let mut ops_b = OpRegistry::new();
  ops_b.register(
    "touch_both",
    Box::new(|ctx, ids, _payload| {
      ctx.put(ids[0], b"touched".to_vec());
      ctx.put(ids[1], b"touched".to_vec());
      vec![]
    }),
  );

  let pool_a = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(ops),
    Arc::clone(&ring),
    node_a,
    peer_link_listener_a,
    Arc::clone(&peer_link_address_book),
  ));
  let pool_b = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(ops_b),
    Arc::clone(&ring),
    node_b,
    peer_link_listener_b,
    peer_link_address_book,
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

  (addr_a, addr_b, ring)
}

#[test]
fn an_op_collates_datums_natively_owned_by_different_nodes() {
  let (addr_a, _addr_b, ring) = start_two_node_cluster();

  // Find two ids whose native homes are on different *nodes* (not just
  // different threads), so this test actually exercises a real
  // peer-link Acquire round trip.
  let (a, b) = loop {
    let x = DatumId::new();
    let y = DatumId::new();
    if ring.read().unwrap().native(x).0 != ring.read().unwrap().native(y).0 {
      break (x, y);
    }
  };

  // Contact node A regardless of where `a`/`b` actually live — proving
  // this doesn't matter now: whichever node ends up running the op
  // pulls in whatever it doesn't already have.
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

Run: `cargo test -p seisin-node --test integration_cross_node_collation`
Expected: PASS (1 test).

- [ ] **Step 3: Commit and push**

```bash
git add crates/seisin-node/tests/integration_cross_node_collation.rs
git commit -m "test: add cross-node op collation integration test"
git push
```

---

### Task 10: Cross-node wound-wait integration test

**Files:**
- Create: `crates/seisin-node/tests/integration_cross_node_wound_wait.rs`

**Interfaces:**
- Consumes: everything from Tasks 1-9.
- Produces: proof that the classic two-op cycle resolves without deadlock when the contended datums live on *different nodes*, not just different threads on one node — the genuinely new case this plan adds over Part 1's same-node version.

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

fn start_two_node_cluster() -> (String, String, Arc<RwLock<Ring>>) {
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

  let peer_link_listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
  let peer_link_listener_b = TcpListener::bind("127.0.0.1:0").unwrap();
  let peer_link_addr_a = peer_link_listener_a.local_addr().unwrap().to_string();
  let peer_link_addr_b = peer_link_listener_b.local_addr().unwrap().to_string();
  let mut peer_link_address_book = HashMap::new();
  peer_link_address_book.insert(node_a, peer_link_addr_a);
  peer_link_address_book.insert(node_b, peer_link_addr_b);
  let peer_link_address_book = Arc::new(peer_link_address_book);

  let pool_a = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(build_registry()),
    Arc::clone(&ring),
    node_a,
    peer_link_listener_a,
    Arc::clone(&peer_link_address_book),
  ));
  let pool_b = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(build_registry()),
    Arc::clone(&ring),
    node_b,
    peer_link_listener_b,
    peer_link_address_book,
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

  (addr_a, addr_b, ring)
}

#[test]
fn two_ops_needing_cross_node_datums_in_opposite_order_both_complete() {
  let (addr_a, addr_b, ring) = start_two_node_cluster();

  let (a, b) = loop {
    let x = DatumId::new();
    let y = DatumId::new();
    if ring.read().unwrap().native(x).0 != ring.read().unwrap().native(y).0 {
      break (x, y);
    }
  };

  let op1 = DatumId::new(); // older
  let op2 = DatumId::new(); // younger

  let thread1 = thread::spawn(move || {
    seisin_client::call(
      &addr_a,
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
      &addr_b,
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

- [ ] **Step 2: Run the test, several times to check for flakiness**

Run: `cargo test -p seisin-node --test integration_cross_node_wound_wait`
Expected: PASS.

Run it repeatedly to build confidence, given Part 1 surfaced two genuine concurrency bugs that only showed up under repetition, not a single run:
```bash
for i in $(seq 1 20); do
  cargo test -p seisin-node --test integration_cross_node_wound_wait 2>&1 | tail -3
done
```
Expected: PASS every time. If any run fails or hangs, stop and debug — per the systematic-debugging skill, not by adding a timeout to paper over it. (See Part 1's plan self-review notes for the *kind* of bug this class of test caught before: ordering assumptions and cache-staleness bugs that only a real concurrent, cross-thread/cross-node run exposes.)

- [ ] **Step 3: Commit and push**

```bash
git add crates/seisin-node/tests/integration_cross_node_wound_wait.rs
git commit -m "test: add cross-node wound-wait integration test"
git push
```

---

### Task 11: Quality gate

**Files:** none (verification only).

- [ ] **Step 1: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS.

- [ ] **Step 2: Run the formatting and lint gate**

Run: `cargo fmt --check`
Expected: no output. If it reports diffs, run `cargo fmt` and re-check.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings/errors. Fix anything reported before continuing (as in Part 1, expect at least one `too_many_arguments` lint on a function this plan added or grew — add `#[allow(clippy::too_many_arguments)]` to match the existing convention rather than restructuring the signature).

- [ ] **Step 3: Update `docs/superpowers/PROGRESS.md`**

Add an entry under "Done" for this plan (Sub-project 3b, Part 2a), and update "In progress" to note Part 2b (crash detection & lock release) is next, plus flag the known gap this plan's Global Constraints section already calls out: peer-links are only ever established from the *static* startup member list, not re-established when gossip later admits a genuinely new node at runtime.

- [ ] **Step 4: Commit and push**

```bash
git add -A
git commit -m "chore: fmt/clippy fixes for cross-node collation part 2a; update progress tracker"
git push
```

(Skip the `git commit` if nothing changed beyond `PROGRESS.md`, but still commit that.)

---

## Self-Review Notes

- **Spec coverage**: real cross-node datum transfer ✓ (Tasks 4-6, 9); server-to-server connection multiplexing, O(servers) not O(threads²) ✓ (Tasks 2-3); envelope framing wrapping the existing Request/Response codec unchanged ✓ (Task 1); wound-wait genuinely working across a real network boundary, not just same-node ✓ (Task 10). Crash detection & lock release (the spec's dedicated section) is explicitly **not** covered here — that's Part 2b, called out in Global Constraints and in every task boundary where it would otherwise be tempting to reach for it (e.g. Task 2's `peer_link` disconnect handling only fails pending calls, it doesn't do anything proactive).
- **Placeholder scan**: no TBD/TODO; every code block is complete, runnable Rust.
- **Type consistency**: `Request::Acquire`/`Recall`, `Response::Granted`/`Released`, `Envelope`/`EnvelopeKind`, `PeerLink`/`PeerLinkRegistry`, and `AcquireReply`/`RecallReply` match exactly between where they're defined (Tasks 1-4) and where they're consumed (Tasks 5-10).
- **Known limitations carried forward** (not fixed in this plan): peer-links are established once at startup from the static config member list only — a node that joins the cluster later via gossip never gets a peer-link connection, so cross-node collation involving it will panic at `PeerLinkRegistry::get` rather than degrade gracefully. This is flagged in Global Constraints and PROGRESS.md rather than silently left implicit. A dead peer's in-flight `Acquire`/`Recall` calls fail via `peer_link.rs`'s disconnect handling (Task 2) but nothing *proactively* reclaims a lock a dead node was holding, and nothing retries a failed `Acquire` against a since-moved ring slot — both are Part 2b's explicit scope, per the spec's own section boundary.
- **Bug caught during self-review, fixed inline**: the dial loop in `PeerLinkRegistry::start` (Task 3) originally panicked if a configured peer wasn't reachable yet — this would have made `integration_gossip_failure_detection.rs` (Task 8, Step 4) panic on node A's own startup, since that test deliberately never starts node B. Fixed by skipping an unreachable peer instead of treating it as fatal (a node must be able to finish starting up even if a configured peer hasn't started yet); reaching for that peer later panics at `get` instead, which is the same accepted v1 gap already called out above, not a new one.
