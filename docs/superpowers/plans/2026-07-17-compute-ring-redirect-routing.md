# Compute Ring & Redirect Routing Implementation Plan (Sub-project 2a)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the single-node server from Sub-project 1 into a real
multi-node cluster: a compute ring that resolves a datum's native
(node, thread) via jump-consistent-hash, and client-side redirect so a
request landing on the wrong node gets pointed at the right one. Cluster
membership is a *static*, config-supplied list for this plan — dynamic
SWIM-gossiped membership is Sub-project 2b, built on top of this ring
without changing it.

**Architecture:** A new `seisin-ring` crate holds the vendored
jump-consistent-hash algorithm and the `Ring` type (a flat
`Vec<(NodeId, ThreadId)>`, built once from a static member list). A new
`seisin-client` crate provides a redirect-following `call()` so tests (and
future tooling) don't reimplement the follow-loop. `seisin-node` grows a
`WorkerPool` (one `WorkerHandle` per local thread slot, sharing one
backing `Store`), a RON-based `NodeConfig`, and relay logic: on each
request, look up the ring; serve locally if this node is the owner,
otherwise reply `Redirect` with the owner's address.

**Tech Stack:** Same as Sub-project 1 (Rust 2021, `anyhow`), plus `serde`
+ `ron` for the node config file.

## Global Constraints

(Same as Sub-project 1's plan — repeated here since every task's
requirements implicitly include them.)

- No auth/encryption anywhere in this protocol.
- `anyhow::Result<T>` + `bail!()`/`.context()` is the only accepted error
  style.
- Prefer many small, single-purpose crates over one monolith.
- `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` must
  pass; 2-space indent via the repo's `rustfmt.toml`.
- `#![deny(warnings)]` at the crate root of service crates
  (`seisin-node`) — added only once a module is fully implemented, since
  the intentional `unimplemented!()` stub state during a task's red step
  trips unused-import/dead-code lints that `deny(warnings)` turns into
  hard compile errors (the same accepted pattern used in Sub-project 1).
- Public items get `///`/`//!` doc comments describing invariants and
  guarantees.
- Known-answer/round-trip tests with fixed vectors preferred for
  wire-format and algorithm code.
- Config via `serde` + RON for human-edited files, not JSON/YAML
  (`GUIDELINES.md`, Rust) — used here for `NodeConfig`.

**New for this plan, from the design doc's "Compute Ring Mechanics"
section:**

- Thread count per node is derived from available CPU cores at the
  node's own startup in the real system — but for this static-membership
  plan, `NodeConfig` simply states each member's `thread_count` directly;
  wiring in real CPU-core auto-detection is Sub-project 2b's concern
  (once there's a gossip channel to announce it over).
- Ring mutation (join = append, leave = swap-with-last) is out of scope
  for this plan — the `Ring` here is built once from a static list and
  never mutated. `Ring`'s public API is deliberately shaped around "build
  from a member list" so Sub-project 2b can add mutation methods without
  changing how `native()` or the slots array work.
- Relay is client-side redirect, not node-to-node proxying: a node that
  isn't the native owner replies `Response::Redirect { address }` naming
  the correct node; it never forwards the request itself.

---

### Task 1: Vendor jump-consistent-hash

**Files:**
- Create: `crates/seisin-ring/Cargo.toml`
- Create: `crates/seisin-ring/src/lib.rs`
- Create: `crates/seisin-ring/src/jump_hash.rs`
- Modify: `Cargo.toml` (workspace root)

**Interfaces:**
- Produces: `seisin_ring::jump_hash::JumpBackHasher` —
  `JumpBackHasher::new() -> Self`, `hash(&mut self, k: u64, n: u32) -> u32`
  (result always `< n` for `n > 0`).

- [ ] **Step 1: Scaffold the crate and write the failing tests**

`crates/seisin-ring/Cargo.toml`:

```toml
[package]
name = "seisin-ring"
version = "0.1.0"
edition = "2021"

[dependencies]
seisin-core = { path = "../seisin-core" }
```

Add `"crates/seisin-ring"` to the workspace `members` list in the
root `Cargo.toml`.

`crates/seisin-ring/src/lib.rs`:

```rust
pub mod jump_hash;
pub mod ring;
```

(The `ring` module is created in Task 2 — this `pub mod ring;` line will
fail to compile until then; add it as part of Task 2 instead if you're
executing strictly task-by-task. For Task 1 alone, `lib.rs` should just
be `pub mod jump_hash;`.)

`crates/seisin-ring/src/jump_hash.rs`:

```rust
//! Jump-consistent-hash, vendored from
//! https://github.com/tanglebones/consistent_hash (the `JumpBack`
//! variant, implementing https://arxiv.org/pdf/2403.18682) rather than
//! taken as a dependency, per that repo's own guidance for a small,
//! stable algorithm like this one. Field/type names adapted to this
//! project's style; the algorithm itself is unchanged.

/// Deterministic 64-bit RNG (SplitMix64) used by the hasher.
struct SplitMix64 {
  state: u64,
}

impl Default for SplitMix64 {
  fn default() -> Self {
    Self { state: 0 }
  }
}

impl SplitMix64 {
  fn reset_with_seed(&mut self, seed: u64) {
    self.state = seed;
  }

  fn next_long(&mut self) -> u64 {
    self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = self.state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
  }
}

/// Maps key `k` deterministically to a bucket in `[0, n)`. Growing or
/// shrinking `n` by one at the boundary remaps only ~1/n keys — see the
/// design doc's "Compute Ring Mechanics" section for how `seisin-ring`
/// applies the swap-with-last technique on top of this primitive to
/// support removing an arbitrary (not just the highest-index) bucket
/// while keeping that guarantee.
pub struct JumpBackHasher {
  rng: SplitMix64,
}

impl Default for JumpBackHasher {
  fn default() -> Self {
    Self { rng: SplitMix64::default() }
  }
}

impl JumpBackHasher {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn hash(&mut self, k: u64, n: u32) -> u32 {
    let _ = (k, n);
    unimplemented!()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn trivial_cases_match_the_upstream_crate() {
    let mut h = JumpBackHasher::new();
    assert_eq!(h.hash(0, 0), 0);
    assert_eq!(h.hash(0, 1), 0);
    assert_eq!(h.hash(1, 1), 0);
    assert_eq!(h.hash(0, 2), 0);
    assert_eq!(h.hash(1, 2), 1);
  }

  #[test]
  fn result_is_always_in_range() {
    let mut h = JumpBackHasher::new();
    for n in [2u32, 3, 4, 7, 16, 31, 32, 33, 1000] {
      for k in [0u64, 1, 2, 123456789, u64::MAX - 1, u64::MAX] {
        let r = h.hash(k, n);
        assert!(r < n, "k={k}, n={n}, r={r}");
      }
    }
  }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-ring`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Port the real algorithm from the upstream crate**

Replace the `hash` method body:

```rust
pub fn hash(&mut self, k: u64, n: u32) -> u32 {
    if n <= 1 {
      return 0;
    }

    self.rng.reset_with_seed(k);
    let v = self.rng.next_long();

    let n_minus_1 = n - 1;
    let mask: u32 = (!0u32) >> n_minus_1.leading_zeros();
    let u: u32 = ((v ^ (v >> 32)) as u32) & mask;

    let mut u_work = u;
    while u_work != 0 {
      let q: u32 = 1u32 << (31 - u_work.leading_zeros());
      let shift: u32 = ((u_work.count_ones() << 5) & 63) as u32;
      let b0: u32 = ((v >> shift) as u32) & (q - 1);
      let mut b: u32 = q.wrapping_add(b0);

      loop {
        if b < n {
          return b;
        }
        let w = self.rng.next_long();

        let mask2: u32 = if q == 0x8000_0000 { 0xFFFF_FFFF } else { (q << 1) - 1 };

        b = (w as u32) & mask2;
        if b < q {
          break;
        }
        if b < n {
          return b;
        }
        b = ((w >> 32) as u32) & mask2;
        if b < q {
          break;
        }
        if b < n {
          return b;
        }
      }

      u_work ^= q;
    }

    0
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-ring`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/seisin-ring
git commit -m "feat: vendor jump-consistent-hash into seisin-ring"
```

---

### Task 2: `Ring`

**Files:**
- Create: `crates/seisin-ring/src/ring.rs`
- Modify: `crates/seisin-ring/src/lib.rs`

**Interfaces:**
- Consumes: `seisin_core::authority::{NodeId, ThreadId}`,
  `seisin_core::datum::DatumId`, `seisin_ring::jump_hash::JumpBackHasher`.
- Produces: `seisin_ring::ring::Ring` — `Ring::from_members(&[(NodeId,
  u32)]) -> Self`, `native(&self, DatumId) -> (NodeId, ThreadId)`.

- [ ] **Step 1: Write the failing test**

`crates/seisin-ring/src/ring.rs`:

```rust
//! The compute ring: maps a datum to its currently native (node, thread).
//!
//! Built from a static member list for now (Sub-project 2a); Sub-project
//! 2b replaces the static list with SWIM-gossiped join/leave mutations
//! applied via the swap-with-last algorithm, epoch-ordered by an elected
//! sequencer — see the design doc's "Compute Ring Mechanics" section.
//! This type doesn't care where its slots came from, so that later
//! change doesn't require rewriting it.

use seisin_core::authority::{NodeId, ThreadId};
use seisin_core::datum::DatumId;

use crate::jump_hash::JumpBackHasher;

pub struct Ring {
    slots: Vec<(NodeId, ThreadId)>,
}

impl Ring {
    /// Builds a ring from a static member list: `(node_id, thread_count)`
    /// pairs. Each member contributes `thread_count` slots, in order.
    pub fn from_members(members: &[(NodeId, u32)]) -> Self {
        let _ = members;
        unimplemented!()
    }

    /// Returns the datum's current native (node, thread).
    ///
    /// # Panics
    /// Panics if the ring has no slots (an empty member list).
    pub fn native(&self, datum_id: DatumId) -> (NodeId, ThreadId) {
        let _ = datum_id;
        unimplemented!()
    }
}

/// Derives the u64 hash key for a datum_id from its trailing 8 bytes
/// (UUIDv7's `rand_b` field, which is fully random) rather than its
/// leading bytes (mostly a monotonic timestamp, which would concentrate
/// ids created in the same millisecond into adjacent hash inputs).
fn hash_key(datum_id: DatumId) -> u64 {
    let bytes = datum_id.as_bytes();
    u64::from_le_bytes(bytes[8..16].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_is_deterministic_for_the_same_ring() {
        let ring = Ring::from_members(&[(NodeId(1), 2), (NodeId(2), 3)]);
        let id = DatumId::new();
        assert_eq!(ring.native(id), ring.native(id));
    }

    #[test]
    fn native_always_resolves_to_a_configured_member_slot() {
        let ring = Ring::from_members(&[(NodeId(1), 2), (NodeId(2), 3)]);
        for _ in 0..100 {
            let (node_id, thread_id) = ring.native(DatumId::new());
            let valid = (node_id == NodeId(1) && thread_id.0 < 2)
                || (node_id == NodeId(2) && thread_id.0 < 3);
            assert!(valid, "unexpected owner: {node_id:?} {thread_id:?}");
        }
    }

    #[test]
    fn single_member_ring_always_resolves_to_that_member() {
        let ring = Ring::from_members(&[(NodeId(9), 1)]);
        assert_eq!(ring.native(DatumId::new()), (NodeId(9), ThreadId(0)));
    }
}
```

Update `crates/seisin-ring/src/lib.rs` to:

```rust
pub mod jump_hash;
pub mod ring;
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-ring`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement `Ring`**

```rust
impl Ring {
    pub fn from_members(members: &[(NodeId, u32)]) -> Self {
        let mut slots = Vec::new();
        for (node_id, thread_count) in members {
            for t in 0..*thread_count {
                slots.push((*node_id, ThreadId(t)));
            }
        }
        Self { slots }
    }

    pub fn native(&self, datum_id: DatumId) -> (NodeId, ThreadId) {
        let mut hasher = JumpBackHasher::new();
        let index = hasher.hash(hash_key(datum_id), self.slots.len() as u32);
        self.slots[index as usize]
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-ring`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-ring
git commit -m "feat: add Ring mapping datum_id to native (node, thread)"
```

---

### Task 3: `Response::Redirect` and `Request::datum_id()`

**Files:**
- Modify: `crates/seisin-protocol/src/lib.rs`

**Interfaces:**
- Produces: `seisin_protocol::Response::Redirect { address: String }`
  (with encode/decode support), `seisin_protocol::Request::datum_id(&self)
  -> DatumId`.

- [ ] **Step 1: Write the failing tests**

Add the `Redirect` variant to `Response`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
  Value { content: Vec<u8>, authority: AuthorityIdx },
  NotFound,
  Ok,
  Redirect { address: String },
}
```

Add the accessor and the new opcode constant near the other `RESP_*`
constants:

```rust
const RESP_REDIRECT: u8 = 3;
```

Add this method (with a stub body) right after the `Request` enum
definition:

```rust
impl Request {
  /// The datum_id every `Request` variant carries, regardless of which
  /// operation it is — used by the server to look up the ring's native
  /// owner before dispatching.
  pub fn datum_id(&self) -> DatumId {
    unimplemented!()
  }
}
```

Add to the `encode_response`/`decode_response` match statements (stub
arms for now):

```rust
// in encode_response's match:
Response::Redirect { address } => {
  let _ = address;
  unimplemented!()
}

// in decode_response's match, add a new arm before the `op => bail!(...)` catch-all:
RESP_REDIRECT => unimplemented!(),
```

Add these tests to the `tests` module:

```rust
#[test]
fn round_trips_redirect_response() {
  let resp = Response::Redirect { address: "127.0.0.1:7879".to_string() };
  assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
}

#[test]
fn rejects_redirect_with_invalid_utf8_address() {
  let mut buf = encode_response(&Response::Redirect { address: "x".to_string() });
  // Corrupt the one address byte into an invalid UTF-8 continuation byte.
  *buf.last_mut().unwrap() = 0x80;
  assert!(decode_response(&buf).is_err());
}

#[test]
fn datum_id_returns_the_id_for_every_variant() {
  let id = DatumId::new();
  assert_eq!(Request::Get { id }.datum_id(), id);
  assert_eq!(Request::Put { id, content: vec![] }.datum_id(), id);
  assert_eq!(Request::Delete { id }.datum_id(), id);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-protocol`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement**

Replace the `Request::datum_id` stub:

```rust
impl Request {
  pub fn datum_id(&self) -> DatumId {
    match self {
      Request::Get { id } | Request::Put { id, .. } | Request::Delete { id } => *id,
    }
  }
}
```

Replace the `encode_response` stub arm:

```rust
Response::Redirect { address } => {
  buf.push(RESP_REDIRECT);
  let addr_bytes = address.as_bytes();
  buf.extend_from_slice(&(addr_bytes.len() as u32).to_le_bytes());
  buf.extend_from_slice(addr_bytes);
}
```

Replace the `decode_response` stub arm, and add `Context` to the
top-of-file `use anyhow::{bail, Result};` import:

```rust
use anyhow::{bail, Context, Result};
```

```rust
RESP_REDIRECT => {
  if buf.len() < 5 {
    bail!("redirect response too short for an address length: {} bytes", buf.len());
  }
  let addr_len = u32::from_le_bytes(buf[1..5].try_into().unwrap()) as usize;
  if buf.len() != 5 + addr_len {
    bail!(
      "redirect response length mismatch: expected {} bytes, got {}",
      5 + addr_len,
      buf.len()
    );
  }
  let address = String::from_utf8(buf[5..].to_vec()).context("redirect address was not valid utf8")?;
  Ok(Response::Redirect { address })
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-protocol`
Expected: PASS (12 tests)

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-protocol/src/lib.rs
git commit -m "feat: add Response::Redirect and Request::datum_id"
```

---

### Task 4: `seisin-client` — redirect-following call

**Files:**
- Create: `crates/seisin-client/Cargo.toml`
- Create: `crates/seisin-client/src/lib.rs`
- Modify: `Cargo.toml` (workspace root)

**Interfaces:**
- Consumes: `seisin_protocol::{Request, Response, encode_request,
  decode_request is not needed, decode_response, read_frame, write_frame}`.
- Produces: `seisin_client::call(&str, Request) -> anyhow::Result<Response>`,
  `seisin_client::MAX_REDIRECTS: u32`.

- [ ] **Step 1: Scaffold and write the failing tests**

`crates/seisin-client/Cargo.toml`:

```toml
[package]
name = "seisin-client"
version = "0.1.0"
edition = "2021"

[dependencies]
seisin-core = { path = "../seisin-core" }
seisin-protocol = { path = "../seisin-protocol" }
anyhow = "1"
```

Add `"crates/seisin-client"` to the workspace `members` list.

`crates/seisin-client/src/lib.rs`:

```rust
//! A minimal client that follows `Response::Redirect` automatically, so
//! callers (tests, future tooling) don't reimplement the redirect-follow
//! loop themselves.

use std::net::TcpStream;

use anyhow::Result;

use seisin_protocol::{decode_response, encode_request, read_frame, write_frame, Request, Response};

/// Follows at most this many redirects before giving up — guards against
/// a misconfigured or buggy cluster causing an infinite redirect loop.
pub const MAX_REDIRECTS: u32 = 8;

/// Sends `request` to `initial_address`, following any `Redirect`
/// responses (up to `MAX_REDIRECTS` hops) until a non-redirect response
/// is received.
pub fn call(initial_address: &str, request: Request) -> Result<Response> {
    let _ = (initial_address, request);
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

    use seisin_core::datum::DatumId;

    /// A tiny fake server: replies with a `Redirect` to whatever's in
    /// `redirect_targets`, in order, then replies with `final_response`.
    fn start_fake_server(redirect_targets: Vec<String>, final_response: Response) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _payload = read_frame(&mut stream).unwrap();
            let response = match redirect_targets.into_iter().next() {
                Some(address) => Response::Redirect { address },
                None => final_response,
            };
            write_frame(&mut stream, &encode_response(&response)).unwrap();
        });
        addr
    }

    #[test]
    fn returns_the_response_directly_when_there_is_no_redirect() {
        let addr = start_fake_server(vec![], Response::Ok);
        let response = call(&addr, Request::Get { id: DatumId::new() }).unwrap();
        assert_eq!(response, Response::Ok);
    }

    #[test]
    fn follows_a_single_redirect() {
        let final_addr = start_fake_server(vec![], Response::Ok);
        let first_addr = start_fake_server(vec![final_addr], Response::Ok);
        let response = call(&first_addr, Request::Get { id: DatumId::new() }).unwrap();
        assert_eq!(response, Response::Ok);
    }

    #[test]
    fn gives_up_after_max_redirects() {
        // A server that always redirects to itself never resolves.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let addr_for_thread = addr.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let _ = read_frame(&mut stream);
                let response = Response::Redirect { address: addr_for_thread.clone() };
                let _ = write_frame(&mut stream, &encode_response(&response));
            }
        });
        let result = call(&addr, Request::Get { id: DatumId::new() });
        assert!(result.is_err());
    }
}
```

Note: `encode_response`/`read_frame`/`write_frame` are used in the test
module directly, so add them to the `use seisin_protocol::{...}` import
at the top of `lib.rs` (they're already used by `call` itself, except
`encode_response`, which the tests need too — add it to the same import
line).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-client`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement `call`**

```rust
pub fn call(initial_address: &str, request: Request) -> Result<Response> {
    let mut address = initial_address.to_string();
    for _ in 0..MAX_REDIRECTS {
        let mut stream = TcpStream::connect(&address)?;
        write_frame(&mut stream, &encode_request(&request))?;
        let payload = read_frame(&mut stream)?;
        match decode_response(&payload)? {
            Response::Redirect { address: next } => address = next,
            other => return Ok(other),
        }
    }
    anyhow::bail!("gave up after {MAX_REDIRECTS} redirects, still pointed at {address}");
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-client`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/seisin-client
git commit -m "feat: add seisin-client redirect-following call()"
```

---

### Task 5: `WorkerPool`

**Files:**
- Create: `crates/seisin-node/src/pool.rs`
- Modify: `crates/seisin-node/src/lib.rs`

**Interfaces:**
- Consumes: `seisin_core::{authority::ThreadId, store::Store}`,
  `seisin_node::worker::WorkerHandle`.
- Produces: `seisin_node::pool::WorkerPool` — `WorkerPool::spawn(Arc<dyn
  Store>, u32) -> Self`, `submit(&self, ThreadId, Request) -> Response`.

- [ ] **Step 1: Write the failing test**

`crates/seisin-node/src/pool.rs`:

```rust
//! A pool of owning threads (one `WorkerHandle` per `ThreadId`) sharing
//! one backing store — this node's share of the compute ring's slots.
//! Each thread's `Cache` is independent, but since all threads on this
//! node share the same `Store`, a write on one thread's cache is visible
//! (via a store fallback on cache miss) from another thread on the same
//! node — ownership isolation across nodes is enforced by the ring/relay
//! layer in `server.rs`, not by this type.

use std::sync::Arc;

use seisin_core::authority::ThreadId;
use seisin_core::store::Store;
use seisin_protocol::{Request, Response};

use crate::worker::WorkerHandle;

pub struct WorkerPool {
    handles: Vec<WorkerHandle>,
}

impl WorkerPool {
    /// Spawns `thread_count` worker threads, each with its own `Cache`
    /// over the same shared `store`.
    pub fn spawn(store: Arc<dyn Store>, thread_count: u32) -> Self {
        let _ = (store, thread_count);
        unimplemented!()
    }

    /// Submits a request to the given thread and blocks for its response.
    ///
    /// # Panics
    /// Panics if `thread_id` is out of range for this pool — callers are
    /// expected to only submit for thread ids the ring actually assigned
    /// to this node.
    pub fn submit(&self, thread_id: ThreadId, request: Request) -> Response {
        let _ = (thread_id, request);
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use seisin_core::datum::DatumId;
    use seisin_core::store::InMemoryStore;

    #[test]
    fn each_thread_id_indexes_a_distinct_worker() {
        let pool = WorkerPool::spawn(Arc::new(InMemoryStore::new()), 2);
        assert_eq!(pool.submit(ThreadId(0), Request::Get { id: DatumId::new() }), Response::NotFound);
        assert_eq!(pool.submit(ThreadId(1), Request::Get { id: DatumId::new() }), Response::NotFound);
    }

    #[test]
    fn writes_on_one_thread_are_visible_via_the_shared_store_from_another() {
        let pool = WorkerPool::spawn(Arc::new(InMemoryStore::new()), 2);
        let id = DatumId::new();
        pool.submit(ThreadId(0), Request::Put { id, content: b"hello".to_vec() });
        match pool.submit(ThreadId(1), Request::Get { id }) {
            Response::Value { content, .. } => assert_eq!(content, b"hello"),
            other => panic!("expected Value, got {other:?}"),
        }
    }
}
```

Add `pub mod pool;` to `crates/seisin-node/src/lib.rs`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-node --lib`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement `WorkerPool`**

```rust
impl WorkerPool {
    pub fn spawn(store: Arc<dyn Store>, thread_count: u32) -> Self {
        let handles = (0..thread_count).map(|_| WorkerHandle::spawn(Arc::clone(&store))).collect();
        Self { handles }
    }

    pub fn submit(&self, thread_id: ThreadId, request: Request) -> Response {
        self.handles[thread_id.0 as usize].submit(request)
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-node --lib`
Expected: PASS (5 tests: 2 new + the 3 existing `worker` tests)

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-node/src/pool.rs crates/seisin-node/src/lib.rs
git commit -m "feat: add WorkerPool (one worker per local thread slot)"
```

---

### Task 6: `NodeConfig` (RON)

**Files:**
- Create: `crates/seisin-node/src/config.rs`
- Modify: `crates/seisin-node/Cargo.toml`
- Modify: `crates/seisin-node/src/lib.rs`

**Interfaces:**
- Produces: `seisin_node::config::{NodeConfig, MemberConfig}` —
  `NodeConfig::parse(&str) -> anyhow::Result<Self>`, `NodeConfig::load(&str)
  -> anyhow::Result<Self>`, `self_address(&self) -> &str`. Fields:
  `self_node_id: u64`, `members: Vec<MemberConfig>` where `MemberConfig`
  has `node_id: u64`, `address: String`, `thread_count: u32`.

- [ ] **Step 1: Add dependencies and write the failing test**

Add to `crates/seisin-node/Cargo.toml`:

```toml
serde = { version = "1", features = ["derive"] }
ron = "0.8"
```

`crates/seisin-node/src/config.rs`:

```rust
//! This node's identity plus the static compute-ring membership for
//! Sub-project 2a. Sub-project 2b replaces the `members` list (learned
//! here from a config file) with SWIM-gossiped join/leave events feeding
//! the same `Ring` type — this struct's job is only ever "what does this
//! process currently believe the membership is," regardless of source.

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct MemberConfig {
    pub node_id: u64,
    pub address: String,
    pub thread_count: u32,
}

#[derive(Debug, Deserialize)]
pub struct NodeConfig {
    pub self_node_id: u64,
    pub members: Vec<MemberConfig>,
}

impl NodeConfig {
    pub fn parse(source: &str) -> Result<Self> {
        let _ = source;
        unimplemented!()
    }

    pub fn load(path: &str) -> Result<Self> {
        let _ = path;
        unimplemented!()
    }

    /// This node's own address, looked up from `members` by `self_node_id`.
    ///
    /// # Panics
    /// Panics if `self_node_id` isn't present in `members` — a config
    /// file that doesn't list itself is a startup-time configuration bug,
    /// not a runtime condition to recover from.
    pub fn self_address(&self) -> &str {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
(
    self_node_id: 1,
    members: [
        (node_id: 1, address: "127.0.0.1:7878", thread_count: 2),
        (node_id: 2, address: "127.0.0.1:7879", thread_count: 4),
    ],
)
"#;

    #[test]
    fn parses_a_well_formed_config() {
        let config = NodeConfig::parse(SAMPLE).unwrap();
        assert_eq!(config.self_node_id, 1);
        assert_eq!(config.members.len(), 2);
        assert_eq!(config.members[1].thread_count, 4);
    }

    #[test]
    fn self_address_finds_the_matching_member() {
        let config = NodeConfig::parse(SAMPLE).unwrap();
        assert_eq!(config.self_address(), "127.0.0.1:7878");
    }

    #[test]
    fn rejects_malformed_ron() {
        assert!(NodeConfig::parse("not valid ron {{{").is_err());
    }
}
```

Add `pub mod config;` to `crates/seisin-node/src/lib.rs`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-node --lib`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement**

```rust
impl NodeConfig {
    pub fn parse(source: &str) -> Result<Self> {
        ron::from_str(source).context("failed to parse node config RON")
    }

    pub fn load(path: &str) -> Result<Self> {
        let source = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {path}"))?;
        Self::parse(&source)
    }

    pub fn self_address(&self) -> &str {
        self.members
            .iter()
            .find(|m| m.node_id == self.self_node_id)
            .map(|m| m.address.as_str())
            .expect("self_node_id must be present in members")
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-node --lib`
Expected: PASS (8 tests total)

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-node/Cargo.toml crates/seisin-node/src/config.rs crates/seisin-node/src/lib.rs
git commit -m "feat: add RON-based NodeConfig"
```

---

### Task 7: Server relay logic and binary wiring

**Files:**
- Modify: `crates/seisin-node/Cargo.toml`
- Modify: `crates/seisin-node/src/server.rs`
- Modify: `crates/seisin-node/src/main.rs`

**Interfaces:**
- Consumes: `seisin_ring::ring::Ring`, `seisin_core::authority::NodeId`,
  `seisin_node::{pool::WorkerPool, config::NodeConfig}`.
- Produces: `seisin_node::server::serve(TcpListener, NodeId, Arc<Ring>,
  Arc<HashMap<NodeId, String>>, Arc<WorkerPool>)`.

- [ ] **Step 1: Add the `seisin-ring` dependency**

Add to `crates/seisin-node/Cargo.toml`:

```toml
seisin-ring = { path = "../seisin-ring" }
```

- [ ] **Step 2: Rewrite `server.rs`**

There's no separate red/green step here — `serve`/`handle_connection`
have no unit tests of their own (same rationale as Sub-project 1's
Task 8: this is exercised by the Task 8 integration test in this plan,
which is the right layer to test real multi-node socket behavior at).
Replace the whole file:

```rust
//! Accepts client TCP connections and routes each request: serve
//! directly if this node is the datum's native owner per the current
//! ring, otherwise reply `Redirect` naming the owner's address.

use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use seisin_core::authority::NodeId;
use seisin_protocol::{decode_request, encode_response, read_frame, write_frame, Response};
use seisin_ring::ring::Ring;

use crate::pool::WorkerPool;

/// Runs the accept loop on `listener`, spawning one handler thread per
/// connection, until the listener errors out (e.g. the socket is closed).
pub fn serve(
    listener: TcpListener,
    self_node_id: NodeId,
    ring: Arc<Ring>,
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
    ring: Arc<Ring>,
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
        let (owner_node, thread_id) = ring.native(request.datum_id());
        let response = if owner_node == self_node_id {
            pool.submit(thread_id, request)
        } else {
            match address_book.get(&owner_node) {
                Some(address) => Response::Redirect { address: address.clone() },
                // Ring and address book disagree — a static-config bug in
                // this plan's scope; drop the connection rather than lie
                // to the client with a bogus redirect.
                None => return,
            }
        };
        if write_frame(&mut stream, &encode_response(&response)).is_err() {
            return;
        }
    }
}
```

- [ ] **Step 3: Rewrite `main.rs`**

```rust
use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::Arc;

use anyhow::{Context, Result};

use seisin_core::authority::NodeId;
use seisin_core::store::InMemoryStore;
use seisin_node::config::NodeConfig;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ring::ring::Ring;

fn main() -> Result<()> {
    let config_path =
        std::env::var("SEISIN_NODE_CONFIG").context("SEISIN_NODE_CONFIG must name a RON config file")?;
    let config = NodeConfig::load(&config_path)?;

    let self_node_id = NodeId(config.self_node_id);
    let self_address = config.self_address().to_string();

    let members: Vec<(NodeId, u32)> =
        config.members.iter().map(|m| (NodeId(m.node_id), m.thread_count)).collect();
    let ring = Arc::new(Ring::from_members(&members));

    let address_book: HashMap<NodeId, String> =
        config.members.iter().map(|m| (NodeId(m.node_id), m.address.clone())).collect();
    let address_book = Arc::new(address_book);

    let self_thread_count = config
        .members
        .iter()
        .find(|m| m.node_id == config.self_node_id)
        .map(|m| m.thread_count)
        .with_context(|| format!("self_node_id {} not present in members", config.self_node_id))?;

    let listener =
        TcpListener::bind(&self_address).with_context(|| format!("failed to bind {self_address}"))?;
    println!("seisin-node {self_node_id:?} listening on {self_address}");

    let pool = Arc::new(WorkerPool::spawn(Arc::new(InMemoryStore::new()), self_thread_count));
    serve(listener, self_node_id, ring, address_book, pool);
    Ok(())
}
```

- [ ] **Step 4: Verify the workspace builds**

Run: `cargo build --workspace`
Expected: builds with no errors.

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-node/Cargo.toml crates/seisin-node/src/server.rs crates/seisin-node/src/main.rs
git commit -m "feat: add ring-based relay routing to the server and binary"
```

---

### Task 8: Multi-node integration test & quality gate

**Files:**
- Create: `crates/seisin-node/tests/integration_multi_node_routing.rs`

**Interfaces:**
- Consumes: everything produced by Tasks 1–7.
- Produces: nothing new — proves that PUT/GET route correctly across two
  real nodes regardless of which one the client initially contacts,
  following redirects transparently via `seisin_client::call`.

- [ ] **Step 1: Write the integration test**

`crates/seisin-node/tests/integration_multi_node_routing.rs`:

```rust
use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::InMemoryStore;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_protocol::{Request, Response};
use seisin_ring::ring::Ring;

fn start_two_node_cluster() -> (String, String) {
    let listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr_a = listener_a.local_addr().unwrap().to_string();
    let addr_b = listener_b.local_addr().unwrap().to_string();

    let node_a = NodeId(1);
    let node_b = NodeId(2);
    let ring = Arc::new(Ring::from_members(&[(node_a, 2), (node_b, 2)]));

    let mut address_book = HashMap::new();
    address_book.insert(node_a, addr_a.clone());
    address_book.insert(node_b, addr_b.clone());
    let address_book = Arc::new(address_book);

    let pool_a = Arc::new(WorkerPool::spawn(Arc::new(InMemoryStore::new()), 2));
    let pool_b = Arc::new(WorkerPool::spawn(Arc::new(InMemoryStore::new()), 2));

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
        let put_resp = seisin_client::call(&addr_a, Request::Put { id, content: content.clone() }).unwrap();
        assert_eq!(put_resp, Response::Ok);

        // Always GET via node B's address.
        let get_resp = seisin_client::call(&addr_b, Request::Get { id }).unwrap();
        match get_resp {
            Response::Value { content: got, .. } => assert_eq!(got, content),
            other => panic!("expected Value, got {other:?}"),
        }
    }
}
```

Add `seisin-ring` and `seisin-client` as dev-dependencies (or plain
dependencies, since this crate already depends on `seisin-ring`) in
`crates/seisin-node/Cargo.toml`:

```toml
[dev-dependencies]
seisin-client = { path = "../seisin-client" }
```

- [ ] **Step 2: Run the integration test**

Run: `cargo test -p seisin-node --test integration_multi_node_routing`
Expected: PASS (1 test — 20 random datum_ids each round-tripped through
both possible redirect directions)

- [ ] **Step 3: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS (all tests across all five crates)

- [ ] **Step 4: Run the formatting and lint gate**

Run: `cargo fmt --check`
Expected: no output

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings/errors

Fix anything either command reports before continuing.

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-node/Cargo.toml crates/seisin-node/tests/integration_multi_node_routing.rs
git commit -m "test: add multi-node redirect routing integration test"
```

---

## Self-Review Notes

- **Spec coverage:** jump-consistent-hash vendored ✓ (Task 1), `Ring`
  native-owner resolution ✓ (Task 2), client-side `Redirect` ✓ (Task 3 +
  7), static member config ✓ (Task 6), per-node worker pool sharing one
  store ✓ (Task 5), end-to-end multi-node routing proof ✓ (Task 8). Ring
  *mutation* (join/leave), SWIM gossip, the epoch sequencer, and
  ring-epoch cache invalidation are explicitly Sub-project 2b, not here.
- **Placeholder scan:** no TBD/TODO; every `unimplemented!()` stub is
  replaced with real code within the same task.
- **Type consistency:** `Ring`, `WorkerPool`, `NodeConfig`/`MemberConfig`,
  and the `serve`/`handle_connection` signatures are defined once and
  referenced identically wherever consumed later in the plan.
