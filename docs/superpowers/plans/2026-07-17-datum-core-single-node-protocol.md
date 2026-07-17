# Datum Core & Single-Node Protocol Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up a single compute+storage node that proves the datum
model (primary and secondary-key datums as one uniform type), the tagged
Native/Foreign `authority_idx` representation, write-before-ack storage
semantics, and the custom binary wire protocol end-to-end over real TCP —
with no ring, gossip, or collation yet (those are later sub-projects).

**Architecture:** A Cargo workspace of three crates. `seisin-core` holds
pure data types and logic (datum identity, authority tagging, the
secondary-key content codec, the storage trait and its in-memory impl, and
the write-through cache) with no networking. `seisin-protocol` defines the
wire message types and length-prefixed framing. `seisin-node` is the
runnable service: one dedicated OS thread (today's only authority slot)
processes requests one at a time from a channel-based inbox, fed by a
TCP accept loop with one handler thread per client connection.

**Tech Stack:** Rust (2021 edition), `uuid` (v7 feature) for datum IDs,
`anyhow` for all fallible APIs, `std::net`/`std::thread`/`std::sync::mpsc`
for concurrency (no async runtime — see the design doc's Execution Model
decision).

## Global Constraints

- No auth/encryption anywhere in this protocol — the system trusts the
  network boundary (design doc, Non-Goals).
- Any mutation must be written through to the backing store before the
  operation is considered complete (design doc, write-before-ack).
- A secondary-key datum is a regular datum — same Get/Put/Delete path as a
  primary-key datum, no special-casing (design doc, Data Model).
- `authority_idx` is a tagged Native/Foreign value, never persisted to
  storage (design doc, Data Model / Routing & Ownership Protocol).
- `anyhow::Result<T>` + `bail!()`/`.context()` is the only accepted error
  style — no hand-rolled error enums (`GUIDELINES.md`, Rust).
- Prefer many small, single-purpose crates with a thin shared lib at the
  bottom of the dependency graph, over one monolithic crate
  (`GUIDELINES.md`, Rust).
- `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` must
  pass; 2-space indent is already configured via the repo's `rustfmt.toml`
  (`GUIDELINES.md`, Rust).
- `#![deny(warnings)]` at the crate root of the service crate
  (`seisin-node`) (`GUIDELINES.md`, Rust).
- Public items get `///`/`//!` doc comments describing invariants and
  guarantees, not restating the code (`GUIDELINES.md`, Rust).
- Prefer known-answer/round-trip tests with fixed byte vectors over purely
  generative round-trip tests for wire-format code, since this establishes
  a format later sub-projects (and other languages, potentially) must
  match exactly (`GUIDELINES.md`, Rust).
- Datum IDs are UUIDv7 (`GUIDELINES.md`'s "UUIDv7 for DB primary keys, not
  sequential/guessable IDs", and design doc's Data Model).

**Accepted deviation:** `DatumId::new()` calls `Uuid::now_v7()` directly
rather than routing through an injected `ClockSource` trait
(`GUIDELINES.md`'s time-abstraction guidance). None of this plan's tests
need to control the embedded timestamp — only distinctness and byte
round-tripping are asserted. Revisit this if a later sub-project's
wound-wait/op-ordering logic needs deterministic time control in tests.

---

### Task 1: Workspace scaffolding & `DatumId`

**Files:**
- Create: `Cargo.toml` (workspace root)
- Create: `crates/seisin-core/Cargo.toml`
- Create: `crates/seisin-core/src/lib.rs`
- Create: `crates/seisin-core/src/datum.rs`

**Interfaces:**
- Produces: `seisin_core::datum::DatumId` — `DatumId::new() -> Self`,
  `DatumId::from_bytes([u8; 16]) -> Self`, `DatumId::as_bytes(&self) ->
  [u8; 16]`. Implements `Debug, Clone, Copy, PartialEq, Eq, Hash, Default`.

- [ ] **Step 1: Create the workspace and crate scaffolding**

`Cargo.toml` (workspace root):

```toml
[workspace]
resolver = "2"
members = ["crates/seisin-core", "crates/seisin-protocol", "crates/seisin-node"]
```

`crates/seisin-core/Cargo.toml`:

```toml
[package]
name = "seisin-core"
version = "0.1.0"
edition = "2021"

[dependencies]
uuid = { version = "1", features = ["v7"] }
anyhow = "1"
```

`crates/seisin-core/src/lib.rs`:

```rust
pub mod datum;
```

- [ ] **Step 2: Write the failing test in `datum.rs`**

```rust
//! Identity for a single datum — the unit of ownership in the system.

use uuid::Uuid;

/// A globally unique identifier for a datum (primary-key or secondary-key).
/// Backed by a UUIDv7 so ids are k-sortable by creation time, which the
/// wound-wait collation scheme (a later sub-project) relies on for op
/// ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DatumId(Uuid);

impl DatumId {
    pub fn new() -> Self {
        unimplemented!()
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        let _ = bytes;
        unimplemented!()
    }

    pub fn as_bytes(&self) -> [u8; 16] {
        unimplemented!()
    }
}

impl Default for DatumId {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_bytes() {
        let id = DatumId::new();
        let restored = DatumId::from_bytes(id.as_bytes());
        assert_eq!(id, restored);
    }

    #[test]
    fn new_ids_are_distinct() {
        assert_ne!(DatumId::new(), DatumId::new());
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p seisin-core`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 4: Implement `DatumId` for real**

Replace the three method bodies in `crates/seisin-core/src/datum.rs`:

```rust
impl DatumId {
    /// Generates a fresh UUIDv7-backed id from the current time.
    pub fn new() -> Self {
        DatumId(Uuid::now_v7())
    }

    /// Reconstructs an id from its 16-byte wire representation.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        DatumId(Uuid::from_bytes(bytes))
    }

    /// Returns the 16-byte wire representation of this id.
    pub fn as_bytes(&self) -> [u8; 16] {
        *self.0.as_bytes()
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p seisin-core`
Expected: PASS (2 tests)

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/seisin-core
git commit -m "feat: add seisin-core crate with DatumId"
```

---

### Task 2: `AuthorityIdx`

**Files:**
- Create: `crates/seisin-core/src/authority.rs`
- Modify: `crates/seisin-core/src/lib.rs`

**Interfaces:**
- Consumes: none.
- Produces: `seisin_core::authority::{NodeId, ThreadId, AuthorityIdx,
  ENCODED_LEN}`. `AuthorityIdx::encode(&self) -> [u8; ENCODED_LEN]`,
  `AuthorityIdx::decode([u8; ENCODED_LEN]) -> anyhow::Result<AuthorityIdx>`.
  `ENCODED_LEN` is `13` (1 tag byte + 8-byte `NodeId` + 4-byte `ThreadId`).

- [ ] **Step 1: Write the failing test**

`crates/seisin-core/src/authority.rs`:

```rust
//! The tagged Native/Foreign authority_idx representation.
//!
//! `Native` is resolved by hashing the datum_id through the current
//! consistent-hash ring (added in a later sub-project); `Foreign` is a
//! direct pointer stamped when a datum is collated onto a non-native
//! thread. The tag is fixed at assignment time and never reinterpreted by
//! rehashing, so it stays correct even if ring membership changes
//! concurrently — see the design doc's "Cache Invalidation on Ring
//! Membership Change" section.

use anyhow::Result;

/// Identifies a compute node within the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeId(pub u64);

/// Identifies one owning thread (authority slot) within a compute node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThreadId(pub u32);

/// Current owner of a datum: either its hash-derived native home, or a
/// thread it has been temporarily collated onto.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityIdx {
    Native,
    Foreign { node_id: NodeId, thread_id: ThreadId },
}

/// Wire size of an encoded `AuthorityIdx`: 1 tag byte + 8-byte node id + 4-byte thread id.
pub const ENCODED_LEN: usize = 13;

impl AuthorityIdx {
    pub fn encode(&self) -> [u8; ENCODED_LEN] {
        unimplemented!()
    }

    pub fn decode(buf: [u8; ENCODED_LEN]) -> Result<Self> {
        let _ = buf;
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VECTORS: &[(AuthorityIdx, [u8; ENCODED_LEN])] = &[
        (AuthorityIdx::Native, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
        (
            AuthorityIdx::Foreign { node_id: NodeId(1), thread_id: ThreadId(2) },
            [1, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0],
        ),
    ];

    #[test]
    fn encodes_known_vectors() {
        for (value, expected_bytes) in VECTORS {
            assert_eq!(&value.encode(), expected_bytes);
        }
    }

    #[test]
    fn decodes_known_vectors() {
        for (expected_value, bytes) in VECTORS {
            assert_eq!(AuthorityIdx::decode(*bytes).unwrap(), *expected_value);
        }
    }

    #[test]
    fn rejects_invalid_tag() {
        let mut bytes = [0u8; ENCODED_LEN];
        bytes[0] = 2;
        assert!(AuthorityIdx::decode(bytes).is_err());
    }
}
```

Add to `crates/seisin-core/src/lib.rs`:

```rust
pub mod authority;
pub mod datum;
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-core`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement `encode`/`decode`**

```rust
use anyhow::{bail, Result};

const TAG_NATIVE: u8 = 0;
const TAG_FOREIGN: u8 = 1;

impl AuthorityIdx {
    pub fn encode(&self) -> [u8; ENCODED_LEN] {
        let mut buf = [0u8; ENCODED_LEN];
        match self {
            AuthorityIdx::Native => buf[0] = TAG_NATIVE,
            AuthorityIdx::Foreign { node_id, thread_id } => {
                buf[0] = TAG_FOREIGN;
                buf[1..9].copy_from_slice(&node_id.0.to_le_bytes());
                buf[9..13].copy_from_slice(&thread_id.0.to_le_bytes());
            }
        }
        buf
    }

    pub fn decode(buf: [u8; ENCODED_LEN]) -> Result<Self> {
        match buf[0] {
            TAG_NATIVE => Ok(AuthorityIdx::Native),
            TAG_FOREIGN => {
                let node_id = NodeId(u64::from_le_bytes(buf[1..9].try_into().unwrap()));
                let thread_id = ThreadId(u32::from_le_bytes(buf[9..13].try_into().unwrap()));
                Ok(AuthorityIdx::Foreign { node_id, thread_id })
            }
            tag => bail!("invalid authority_idx tag byte: {tag}"),
        }
    }
}
```

(Replace the placeholder `use anyhow::Result;` line at the top of the file
with `use anyhow::{bail, Result};`.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-core`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-core/src/authority.rs crates/seisin-core/src/lib.rs
git commit -m "feat: add tagged AuthorityIdx (Native/Foreign)"
```

---

### Task 3: Secondary-key entry codec

**Files:**
- Create: `crates/seisin-core/src/sk.rs`
- Modify: `crates/seisin-core/src/lib.rs`

**Interfaces:**
- Consumes: `seisin_core::datum::DatumId`, `seisin_core::authority::{AuthorityIdx, ENCODED_LEN}`.
- Produces: `seisin_core::sk::encode_sk_entries(&[(DatumId, AuthorityIdx)])
  -> Vec<u8>`, `seisin_core::sk::decode_sk_entries(&[u8]) ->
  anyhow::Result<Vec<(DatumId, AuthorityIdx)>>`.

- [ ] **Step 1: Write the failing test**

`crates/seisin-core/src/sk.rs`:

```rust
//! Encode/decode for secondary-key datum content.
//!
//! A secondary-key datum (e.g. `sk:user.name:cliff`) is a regular datum —
//! same Get/Put/Delete path as any primary-key datum — whose content
//! happens to be an encoded list of `(DatumId, AuthorityIdx)` pairs for
//! the primary datums it currently matches. The wire/server layer never
//! needs to know this; it just sees bytes.

use anyhow::Result;

use crate::authority::AuthorityIdx;
use crate::datum::DatumId;

pub fn encode_sk_entries(entries: &[(DatumId, AuthorityIdx)]) -> Vec<u8> {
    let _ = entries;
    unimplemented!()
}

pub fn decode_sk_entries(bytes: &[u8]) -> Result<Vec<(DatumId, AuthorityIdx)>> {
    let _ = bytes;
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::{NodeId, ThreadId};

    #[test]
    fn round_trips_empty_list() {
        let encoded = encode_sk_entries(&[]);
        assert_eq!(decode_sk_entries(&encoded).unwrap(), vec![]);
    }

    #[test]
    fn round_trips_multiple_entries() {
        let entries = vec![
            (DatumId::new(), AuthorityIdx::Native),
            (DatumId::new(), AuthorityIdx::Foreign { node_id: NodeId(7), thread_id: ThreadId(3) }),
        ];
        let encoded = encode_sk_entries(&entries);
        assert_eq!(decode_sk_entries(&encoded).unwrap(), entries);
    }

    #[test]
    fn rejects_truncated_input() {
        let entries = vec![(DatumId::new(), AuthorityIdx::Native)];
        let mut encoded = encode_sk_entries(&entries);
        encoded.truncate(encoded.len() - 1);
        assert!(decode_sk_entries(&encoded).is_err());
    }
}
```

Add `pub mod sk;` to `crates/seisin-core/src/lib.rs`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-core`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement the codec**

Replace the two function bodies:

```rust
use anyhow::{bail, Result};

use crate::authority::{AuthorityIdx, ENCODED_LEN as AUTHORITY_LEN};
use crate::datum::DatumId;

const ENTRY_LEN: usize = 16 + AUTHORITY_LEN;

pub fn encode_sk_entries(entries: &[(DatumId, AuthorityIdx)]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + entries.len() * ENTRY_LEN);
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (id, authority) in entries {
        buf.extend_from_slice(&id.as_bytes());
        buf.extend_from_slice(&authority.encode());
    }
    buf
}

pub fn decode_sk_entries(bytes: &[u8]) -> Result<Vec<(DatumId, AuthorityIdx)>> {
    if bytes.len() < 4 {
        bail!("sk entry list truncated: missing count prefix");
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let expected_len = 4 + count * ENTRY_LEN;
    if bytes.len() != expected_len {
        bail!(
            "sk entry list length mismatch: expected {expected_len} bytes for {count} entries, got {}",
            bytes.len()
        );
    }
    let mut entries = Vec::with_capacity(count);
    let mut offset = 4;
    for _ in 0..count {
        let id_bytes: [u8; 16] = bytes[offset..offset + 16].try_into().unwrap();
        let authority_bytes: [u8; AUTHORITY_LEN] =
            bytes[offset + 16..offset + ENTRY_LEN].try_into().unwrap();
        entries.push((DatumId::from_bytes(id_bytes), AuthorityIdx::decode(authority_bytes)?));
        offset += ENTRY_LEN;
    }
    Ok(entries)
}
```

(Replace the placeholder `use anyhow::Result;` line with the imports above.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-core`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-core/src/sk.rs crates/seisin-core/src/lib.rs
git commit -m "feat: add secondary-key entry codec"
```

---

### Task 4: `Store` trait & `InMemoryStore`

**Files:**
- Create: `crates/seisin-core/src/store.rs`
- Modify: `crates/seisin-core/src/lib.rs`

**Interfaces:**
- Consumes: `seisin_core::datum::DatumId`.
- Produces: `seisin_core::store::Store` trait (`get(&self, DatumId) ->
  Option<Vec<u8>>`, `put(&self, DatumId, Vec<u8>)`, `delete(&self,
  DatumId)`, `Send + Sync`), `seisin_core::store::InMemoryStore` (impl
  `Store`, `InMemoryStore::new() -> Self`).

- [ ] **Step 1: Write the failing test**

`crates/seisin-core/src/store.rs`:

```rust
//! The durable-source-of-truth abstraction. `InMemoryStore` stands in for
//! the real sharded storage tier (a later sub-project) — for a
//! single-node deployment, storage and compute share a process, but the
//! `Store` trait boundary is what later lets a networked storage tier
//! slot in without touching `Cache` or the worker.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::datum::DatumId;

pub trait Store: Send + Sync {
    fn get(&self, id: DatumId) -> Option<Vec<u8>>;
    fn put(&self, id: DatumId, content: Vec<u8>);
    fn delete(&self, id: DatumId);
}

#[derive(Default)]
pub struct InMemoryStore {
    data: Mutex<HashMap<DatumId, Vec<u8>>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Store for InMemoryStore {
    fn get(&self, id: DatumId) -> Option<Vec<u8>> {
        let _ = id;
        unimplemented!()
    }

    fn put(&self, id: DatumId, content: Vec<u8>) {
        let _ = (id, content);
        unimplemented!()
    }

    fn delete(&self, id: DatumId) {
        let _ = id;
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_then_get_returns_content() {
        let store = InMemoryStore::new();
        let id = DatumId::new();
        store.put(id, b"hello".to_vec());
        assert_eq!(store.get(id), Some(b"hello".to_vec()));
    }

    #[test]
    fn get_on_missing_id_returns_none() {
        let store = InMemoryStore::new();
        assert_eq!(store.get(DatumId::new()), None);
    }

    #[test]
    fn delete_removes_content() {
        let store = InMemoryStore::new();
        let id = DatumId::new();
        store.put(id, b"hello".to_vec());
        store.delete(id);
        assert_eq!(store.get(id), None);
    }

    #[test]
    fn delete_on_missing_id_is_a_no_op() {
        let store = InMemoryStore::new();
        store.delete(DatumId::new());
    }
}
```

Add `pub mod store;` to `crates/seisin-core/src/lib.rs`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-core`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement `Store` for `InMemoryStore`**

```rust
impl Store for InMemoryStore {
    fn get(&self, id: DatumId) -> Option<Vec<u8>> {
        self.data.lock().unwrap().get(&id).cloned()
    }

    fn put(&self, id: DatumId, content: Vec<u8>) {
        self.data.lock().unwrap().insert(id, content);
    }

    fn delete(&self, id: DatumId) {
        self.data.lock().unwrap().remove(&id);
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-core`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-core/src/store.rs crates/seisin-core/src/lib.rs
git commit -m "feat: add Store trait and InMemoryStore"
```

---

### Task 5: `Cache` (write-through working copy)

**Files:**
- Create: `crates/seisin-core/src/cache.rs`
- Modify: `crates/seisin-core/src/lib.rs`

**Interfaces:**
- Consumes: `seisin_core::datum::DatumId`, `seisin_core::store::Store`.
- Produces: `seisin_core::cache::Cache` — `Cache::new(Arc<dyn Store>) ->
  Self`, `get(&mut self, DatumId) -> Option<Vec<u8>>`, `put(&mut self,
  DatumId, Vec<u8>)`, `delete(&mut self, DatumId)`, `invalidate(&mut self,
  DatumId)`.

- [ ] **Step 1: Write the failing test**

`crates/seisin-core/src/cache.rs`:

```rust
//! The owning-thread's in-memory working-copy cache. Every mutation is
//! written through to the backing `Store` *before* this returns, so an
//! acknowledged write is never only-in-memory — this is what makes lazy
//! crash recovery safe in later sub-projects (see the design doc's
//! "Collation & Op Execution" section: write-before-ack).

use std::collections::HashMap;
use std::sync::Arc;

use crate::datum::DatumId;
use crate::store::Store;

pub struct Cache {
    store: Arc<dyn Store>,
    entries: HashMap<DatumId, Vec<u8>>,
}

impl Cache {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store, entries: HashMap::new() }
    }

    /// Returns the datum's content, serving from the local cache on a hit
    /// and loading through to the store on a miss.
    pub fn get(&mut self, id: DatumId) -> Option<Vec<u8>> {
        let _ = id;
        unimplemented!()
    }

    /// Writes through to the store, then updates the local cache. Returns
    /// only after the store write completes.
    pub fn put(&mut self, id: DatumId, content: Vec<u8>) {
        let _ = (id, content);
        unimplemented!()
    }

    /// Deletes from the store, then evicts the local cache entry.
    pub fn delete(&mut self, id: DatumId) {
        let _ = id;
        unimplemented!()
    }

    /// Evicts a cache entry without touching the store — used when a
    /// datum's ownership moves away from this thread.
    pub fn invalidate(&mut self, id: DatumId) {
        let _ = id;
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::InMemoryStore;

    #[test]
    fn get_on_empty_cache_and_store_returns_none() {
        let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
        assert_eq!(cache.get(DatumId::new()), None);
    }

    #[test]
    fn put_is_readable_directly_from_the_store() {
        let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
        let mut cache = Cache::new(Arc::clone(&store));
        let id = DatumId::new();
        cache.put(id, b"hello".to_vec());
        assert_eq!(store.get(id), Some(b"hello".to_vec()));
    }

    #[test]
    fn get_after_put_returns_the_written_content() {
        let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
        let id = DatumId::new();
        cache.put(id, b"hello".to_vec());
        assert_eq!(cache.get(id), Some(b"hello".to_vec()));
    }

    #[test]
    fn invalidate_forces_a_reload_from_the_store() {
        let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
        let mut cache = Cache::new(Arc::clone(&store));
        let id = DatumId::new();
        cache.put(id, b"original".to_vec());

        // Mutate storage directly, bypassing the cache, to simulate another
        // node having written through while this thread held a now-stale
        // cache entry.
        store.put(id, b"updated".to_vec());
        cache.invalidate(id);

        assert_eq!(cache.get(id), Some(b"updated".to_vec()));
    }

    #[test]
    fn delete_removes_from_both_cache_and_store() {
        let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
        let mut cache = Cache::new(Arc::clone(&store));
        let id = DatumId::new();
        cache.put(id, b"hello".to_vec());
        cache.delete(id);
        assert_eq!(cache.get(id), None);
        assert_eq!(store.get(id), None);
    }
}
```

Add `pub mod cache;` to `crates/seisin-core/src/lib.rs`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-core`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement `Cache`**

```rust
impl Cache {
    pub fn get(&mut self, id: DatumId) -> Option<Vec<u8>> {
        if let Some(content) = self.entries.get(&id) {
            return Some(content.clone());
        }
        let loaded = self.store.get(id)?;
        self.entries.insert(id, loaded.clone());
        Some(loaded)
    }

    pub fn put(&mut self, id: DatumId, content: Vec<u8>) {
        self.store.put(id, content.clone());
        self.entries.insert(id, content);
    }

    pub fn delete(&mut self, id: DatumId) {
        self.store.delete(id);
        self.entries.remove(&id);
    }

    pub fn invalidate(&mut self, id: DatumId) {
        self.entries.remove(&id);
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-core`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-core/src/cache.rs crates/seisin-core/src/lib.rs
git commit -m "feat: add write-through Cache over Store"
```

---

### Task 6: Wire protocol (messages + framing)

**Files:**
- Create: `crates/seisin-protocol/Cargo.toml`
- Create: `crates/seisin-protocol/src/lib.rs`

**Interfaces:**
- Consumes: `seisin_core::datum::DatumId`,
  `seisin_core::authority::{AuthorityIdx, ENCODED_LEN}`.
- Produces: `seisin_protocol::{Request, Response, encode_request,
  decode_request, encode_response, decode_response, write_frame,
  read_frame}`. `Request` variants: `Get { id: DatumId }`, `Put { id:
  DatumId, content: Vec<u8> }`, `Delete { id: DatumId }`. `Response`
  variants: `Value { content: Vec<u8>, authority: AuthorityIdx }`,
  `NotFound`, `Ok`. `decode_request`/`decode_response` return
  `anyhow::Result<_>`. `write_frame`/`read_frame` return `std::io::Result<_>`
  (transport-level I/O, kept separate from content-decode errors so
  callers can distinguish a dropped connection from a malformed payload).

- [ ] **Step 1: Write the failing test**

`crates/seisin-protocol/Cargo.toml`:

```toml
[package]
name = "seisin-protocol"
version = "0.1.0"
edition = "2021"

[dependencies]
seisin-core = { path = "../seisin-core" }
anyhow = "1"
```

`crates/seisin-protocol/src/lib.rs`:

```rust
//! The custom binary wire protocol between clients and compute nodes (and,
//! in later sub-projects, between nodes themselves). No auth/encryption
//! layer by design — the system trusts the network boundary.

use std::io::{self, Read, Write};

use anyhow::Result;

use seisin_core::authority::{AuthorityIdx, ENCODED_LEN as AUTHORITY_LEN};
use seisin_core::datum::DatumId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Get { id: DatumId },
    Put { id: DatumId, content: Vec<u8> },
    Delete { id: DatumId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Value { content: Vec<u8>, authority: AuthorityIdx },
    NotFound,
    Ok,
}

const OP_GET: u8 = 1;
const OP_PUT: u8 = 2;
const OP_DELETE: u8 = 3;

const RESP_VALUE: u8 = 0;
const RESP_NOT_FOUND: u8 = 1;
const RESP_OK: u8 = 2;

const ID_LEN: usize = 16;

pub fn encode_request(req: &Request) -> Vec<u8> {
    let _ = req;
    unimplemented!()
}

pub fn decode_request(buf: &[u8]) -> Result<Request> {
    let _ = buf;
    unimplemented!()
}

pub fn encode_response(resp: &Response) -> Vec<u8> {
    let _ = resp;
    unimplemented!()
}

pub fn decode_response(buf: &[u8]) -> Result<Response> {
    let _ = buf;
    unimplemented!()
}

/// Writes a length-prefixed frame: a 4-byte little-endian length followed
/// by `payload`.
pub fn write_frame<W: Write>(w: &mut W, payload: &[u8]) -> io::Result<()> {
    let _ = (w, payload);
    unimplemented!()
}

/// Reads a single length-prefixed frame written by `write_frame`.
pub fn read_frame<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
    let _ = r;
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use seisin_core::authority::{NodeId, ThreadId};
    use std::io::Cursor;

    #[test]
    fn round_trips_get_request() {
        let req = Request::Get { id: DatumId::new() };
        assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
    }

    #[test]
    fn round_trips_put_request() {
        let req = Request::Put { id: DatumId::new(), content: b"hello".to_vec() };
        assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
    }

    #[test]
    fn round_trips_delete_request() {
        let req = Request::Delete { id: DatumId::new() };
        assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
    }

    #[test]
    fn rejects_unknown_request_opcode() {
        let mut buf = encode_request(&Request::Get { id: DatumId::new() });
        buf[0] = 99;
        assert!(decode_request(&buf).is_err());
    }

    #[test]
    fn round_trips_value_response() {
        let resp = Response::Value {
            content: b"hello".to_vec(),
            authority: AuthorityIdx::Foreign { node_id: NodeId(1), thread_id: ThreadId(2) },
        };
        assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
    }

    #[test]
    fn round_trips_not_found_response() {
        assert_eq!(decode_response(&encode_response(&Response::NotFound)).unwrap(), Response::NotFound);
    }

    #[test]
    fn round_trips_ok_response() {
        assert_eq!(decode_response(&encode_response(&Response::Ok)).unwrap(), Response::Ok);
    }

    #[test]
    fn frame_round_trips_over_a_buffer() {
        let mut buf = Vec::new();
        write_frame(&mut buf, b"payload bytes").unwrap();
        let mut cursor = Cursor::new(buf);
        assert_eq!(read_frame(&mut cursor).unwrap(), b"payload bytes");
    }
}
```

Add `"crates/seisin-protocol"` — already present in the workspace
`members` list from Task 1.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-protocol`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement the protocol functions**

```rust
use anyhow::{bail, Context, Result};

pub fn encode_request(req: &Request) -> Vec<u8> {
    let mut buf = Vec::new();
    match req {
        Request::Get { id } => {
            buf.push(OP_GET);
            buf.extend_from_slice(&id.as_bytes());
        }
        Request::Put { id, content } => {
            buf.push(OP_PUT);
            buf.extend_from_slice(&id.as_bytes());
            buf.extend_from_slice(content);
        }
        Request::Delete { id } => {
            buf.push(OP_DELETE);
            buf.extend_from_slice(&id.as_bytes());
        }
    }
    buf
}

pub fn decode_request(buf: &[u8]) -> Result<Request> {
    if buf.len() < 1 + ID_LEN {
        bail!("request payload too short for an id: {} bytes", buf.len());
    }
    let id = DatumId::from_bytes(buf[1..1 + ID_LEN].try_into().unwrap());
    match buf[0] {
        OP_GET => Ok(Request::Get { id }),
        OP_PUT => Ok(Request::Put { id, content: buf[1 + ID_LEN..].to_vec() }),
        OP_DELETE => Ok(Request::Delete { id }),
        op => bail!("unknown request opcode: {op}"),
    }
}

pub fn encode_response(resp: &Response) -> Vec<u8> {
    let mut buf = Vec::new();
    match resp {
        Response::Value { content, authority } => {
            buf.push(RESP_VALUE);
            buf.extend_from_slice(&authority.encode());
            buf.extend_from_slice(content);
        }
        Response::NotFound => buf.push(RESP_NOT_FOUND),
        Response::Ok => buf.push(RESP_OK),
    }
    buf
}

pub fn decode_response(buf: &[u8]) -> Result<Response> {
    if buf.is_empty() {
        bail!("empty response payload");
    }
    match buf[0] {
        RESP_VALUE => {
            if buf.len() < 1 + AUTHORITY_LEN {
                bail!("value response too short for an authority_idx: {} bytes", buf.len());
            }
            let authority_bytes: [u8; AUTHORITY_LEN] = buf[1..1 + AUTHORITY_LEN].try_into().unwrap();
            let authority = AuthorityIdx::decode(authority_bytes)?;
            Ok(Response::Value { content: buf[1 + AUTHORITY_LEN..].to_vec(), authority })
        }
        RESP_NOT_FOUND => Ok(Response::NotFound),
        RESP_OK => Ok(Response::Ok),
        op => bail!("unknown response opcode: {op}"),
    }
}

pub fn write_frame<W: Write>(w: &mut W, payload: &[u8]) -> io::Result<()> {
    let len = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "payload too large for a u32 frame length"))?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(payload)?;
    w.flush()
}

pub fn read_frame<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload)?;
    Ok(payload)
}
```

(Replace the top-of-file `use anyhow::Result;` with `use anyhow::{bail,
Context, Result};` — `Context` is unused for now and will be used once a
later sub-project adds fallible I/O paths that need it; if `cargo clippy`
flags it as unused, drop it back to `use anyhow::{bail, Result};`.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-protocol`
Expected: PASS (8 tests)

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-protocol
git commit -m "feat: add seisin-protocol wire messages and framing"
```

---

### Task 7: Worker thread

**Files:**
- Create: `crates/seisin-node/Cargo.toml`
- Create: `crates/seisin-node/src/lib.rs`
- Create: `crates/seisin-node/src/worker.rs`

**Interfaces:**
- Consumes: `seisin_core::{authority::AuthorityIdx, cache::Cache,
  store::Store}`, `seisin_protocol::{Request, Response}`.
- Produces: `seisin_node::worker::WorkerHandle` — `WorkerHandle::spawn(Arc<dyn
  Store>) -> Self`, `submit(&self, Request) -> Response`.

- [ ] **Step 1: Write the failing test**

`crates/seisin-node/Cargo.toml`:

```toml
[package]
name = "seisin-node"
version = "0.1.0"
edition = "2021"

[dependencies]
seisin-core = { path = "../seisin-core" }
seisin-protocol = { path = "../seisin-protocol" }
anyhow = "1"
```

`crates/seisin-node/src/lib.rs`:

```rust
#![deny(warnings)]

pub mod worker;
```

`crates/seisin-node/src/worker.rs`:

```rust
//! The single owning thread for this sub-project's one authority slot.
//! Later sub-projects add a ring of these, one per (node, thread); for
//! now there is exactly one, so every response reports `AuthorityIdx::Native`.

use std::sync::mpsc::{self, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use seisin_core::authority::AuthorityIdx;
use seisin_core::cache::Cache;
use seisin_core::store::Store;
use seisin_protocol::{Request, Response};

pub struct WorkerHandle {
    sender: Sender<(Request, Sender<Response>)>,
    _join: JoinHandle<()>,
}

impl WorkerHandle {
    pub fn spawn(store: Arc<dyn Store>) -> Self {
        let _ = store;
        unimplemented!()
    }

    /// Submits a request to the owning thread and blocks for its response.
    pub fn submit(&self, request: Request) -> Response {
        let _ = request;
        unimplemented!()
    }
}

fn handle_request(cache: &mut Cache, request: Request) -> Response {
    let _ = (cache, request);
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use seisin_core::datum::DatumId;
    use seisin_core::store::InMemoryStore;

    #[test]
    fn put_then_get_returns_the_stored_content() {
        let worker = WorkerHandle::spawn(Arc::new(InMemoryStore::new()));
        let id = DatumId::new();

        assert_eq!(worker.submit(Request::Put { id, content: b"hello".to_vec() }), Response::Ok);
        assert_eq!(
            worker.submit(Request::Get { id }),
            Response::Value { content: b"hello".to_vec(), authority: AuthorityIdx::Native }
        );
    }

    #[test]
    fn get_on_missing_datum_returns_not_found() {
        let worker = WorkerHandle::spawn(Arc::new(InMemoryStore::new()));
        assert_eq!(worker.submit(Request::Get { id: DatumId::new() }), Response::NotFound);
    }

    #[test]
    fn delete_then_get_returns_not_found() {
        let worker = WorkerHandle::spawn(Arc::new(InMemoryStore::new()));
        let id = DatumId::new();
        worker.submit(Request::Put { id, content: b"hello".to_vec() });
        assert_eq!(worker.submit(Request::Delete { id }), Response::Ok);
        assert_eq!(worker.submit(Request::Get { id }), Response::NotFound);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-node`
Expected: FAIL (panics with "not implemented")

- [ ] **Step 3: Implement `WorkerHandle` and `handle_request`**

```rust
impl WorkerHandle {
    pub fn spawn(store: Arc<dyn Store>) -> Self {
        let (sender, receiver) = mpsc::channel::<(Request, Sender<Response>)>();
        let join = thread::spawn(move || {
            let mut cache = Cache::new(store);
            for (request, reply) in receiver {
                let response = handle_request(&mut cache, request);
                let _ = reply.send(response);
            }
        });
        Self { sender, _join: join }
    }

    pub fn submit(&self, request: Request) -> Response {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender
            .send((request, reply_tx))
            .expect("worker thread exited unexpectedly");
        reply_rx.recv().expect("worker dropped the reply channel")
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

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-node`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-node
git commit -m "feat: add worker thread for the single authority slot"
```

---

### Task 8: TCP server & binary entry point

**Files:**
- Create: `crates/seisin-node/src/server.rs`
- Create: `crates/seisin-node/src/main.rs`
- Modify: `crates/seisin-node/src/lib.rs`

**Interfaces:**
- Consumes: `seisin_protocol::{decode_request, encode_response, read_frame,
  write_frame}`, `seisin_node::worker::WorkerHandle`.
- Produces: `seisin_node::server::serve(TcpListener, Arc<WorkerHandle>)`.

- [ ] **Step 1: Write `server.rs`**

`crates/seisin-node/src/server.rs`:

```rust
//! Accepts client TCP connections and bridges the wire protocol to the
//! worker thread. One thread per connection; each request blocks on the
//! worker's reply before the next request on that connection is read.

use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use seisin_protocol::{decode_request, encode_response, read_frame, write_frame};

use crate::worker::WorkerHandle;

/// Runs the accept loop on `listener`, spawning one handler thread per
/// connection, until the listener errors out (e.g. the socket is closed).
pub fn serve(listener: TcpListener, worker: Arc<WorkerHandle>) {
    for stream in listener.incoming() {
        let stream = match stream {
            Ok(s) => s,
            Err(_) => continue,
        };
        let worker = Arc::clone(&worker);
        thread::spawn(move || handle_connection(stream, worker));
    }
}

fn handle_connection(mut stream: TcpStream, worker: Arc<WorkerHandle>) {
    loop {
        let payload = match read_frame(&mut stream) {
            Ok(p) => p,
            Err(_) => return, // connection closed or errored
        };
        let request = match decode_request(&payload) {
            Ok(r) => r,
            Err(_) => return, // malformed request: drop the connection
        };
        let response = worker.submit(request);
        let response_bytes = encode_response(&response);
        if write_frame(&mut stream, &response_bytes).is_err() {
            return;
        }
    }
}
```

This task has no unit tests of its own — `serve`/`handle_connection` are
exercised by the end-to-end integration tests in Task 9, which is the
right layer to test real socket behavior at (per `GUIDELINES.md`'s test
pyramid: this is exactly the kind of thing a thin top layer of full-stack
tests should cover, not something to fake a `TcpStream` for in a unit
test).

Update `crates/seisin-node/src/lib.rs`:

```rust
#![deny(warnings)]

pub mod server;
pub mod worker;
```

- [ ] **Step 2: Write `main.rs`**

`crates/seisin-node/src/main.rs`:

```rust
use std::net::TcpListener;
use std::sync::Arc;

use anyhow::{Context, Result};

use seisin_core::store::InMemoryStore;
use seisin_node::server::serve;
use seisin_node::worker::WorkerHandle;

fn main() -> Result<()> {
    let addr = std::env::var("SEISIN_LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:7878".to_string());
    let listener = TcpListener::bind(&addr).with_context(|| format!("failed to bind {addr}"))?;
    println!("seisin-node listening on {addr}");

    let store = Arc::new(InMemoryStore::new());
    let worker = Arc::new(WorkerHandle::spawn(store));
    serve(listener, worker);
    Ok(())
}
```

- [ ] **Step 3: Verify it builds and runs**

Run: `cargo build -p seisin-node`
Expected: builds with no errors.

Run: `SEISIN_LISTEN_ADDR=127.0.0.1:0 timeout 1 cargo run -p seisin-node || true`

Expected: prints `seisin-node listening on 127.0.0.1:0` before the
`timeout` kills it (there's no client to serve yet — this just confirms
the binary starts and binds).

- [ ] **Step 4: Commit**

```bash
git add crates/seisin-node/src/server.rs crates/seisin-node/src/main.rs crates/seisin-node/src/lib.rs
git commit -m "feat: add TCP server and seisin-node binary entry point"
```

---

### Task 9: End-to-end integration tests + quality gate

**Files:**
- Create: `crates/seisin-node/tests/integration_wire_protocol.rs`

**Interfaces:**
- Consumes: everything produced by Tasks 1–8.
- Produces: nothing new — this is the final proof that the whole vertical
  slice (client → wire protocol → worker → cache → store) works over a
  real TCP socket, including the "SK datum is a regular datum" claim from
  the design doc.

- [ ] **Step 1: Write the integration tests**

`crates/seisin-node/tests/integration_wire_protocol.rs`:

```rust
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use seisin_core::authority::AuthorityIdx;
use seisin_core::datum::DatumId;
use seisin_core::sk::{decode_sk_entries, encode_sk_entries};
use seisin_core::store::InMemoryStore;
use seisin_node::server::serve;
use seisin_node::worker::WorkerHandle;
use seisin_protocol::{decode_response, encode_request, read_frame, write_frame, Request, Response};

fn start_test_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let worker = Arc::new(WorkerHandle::spawn(Arc::new(InMemoryStore::new())));
    thread::spawn(move || serve(listener, worker));
    addr
}

fn round_trip(stream: &mut TcpStream, request: Request) -> Response {
    write_frame(stream, &encode_request(&request)).unwrap();
    let payload = read_frame(stream).unwrap();
    decode_response(&payload).unwrap()
}

#[test]
fn put_then_get_returns_stored_content() {
    let addr = start_test_server();
    let mut stream = TcpStream::connect(addr).unwrap();
    let id = DatumId::new();

    assert_eq!(round_trip(&mut stream, Request::Put { id, content: b"hello".to_vec() }), Response::Ok);
    assert_eq!(
        round_trip(&mut stream, Request::Get { id }),
        Response::Value { content: b"hello".to_vec(), authority: AuthorityIdx::Native }
    );
}

#[test]
fn get_on_missing_datum_returns_not_found() {
    let addr = start_test_server();
    let mut stream = TcpStream::connect(addr).unwrap();
    assert_eq!(round_trip(&mut stream, Request::Get { id: DatumId::new() }), Response::NotFound);
}

#[test]
fn delete_removes_datum() {
    let addr = start_test_server();
    let mut stream = TcpStream::connect(addr).unwrap();
    let id = DatumId::new();

    round_trip(&mut stream, Request::Put { id, content: b"data".to_vec() });
    assert_eq!(round_trip(&mut stream, Request::Delete { id }), Response::Ok);
    assert_eq!(round_trip(&mut stream, Request::Get { id }), Response::NotFound);
}

#[test]
fn secondary_key_datum_round_trips_as_a_regular_datum() {
    let addr = start_test_server();
    let mut stream = TcpStream::connect(addr).unwrap();
    let sk_id = DatumId::new();
    let entries = vec![(DatumId::new(), AuthorityIdx::Native)];
    let content = encode_sk_entries(&entries);

    round_trip(&mut stream, Request::Put { id: sk_id, content: content.clone() });
    match round_trip(&mut stream, Request::Get { id: sk_id }) {
        Response::Value { content: got, .. } => assert_eq!(decode_sk_entries(&got).unwrap(), entries),
        other => panic!("expected Value, got {other:?}"),
    }
}
```

- [ ] **Step 2: Run the integration tests**

Run: `cargo test -p seisin-node --test integration_wire_protocol`
Expected: PASS (4 tests)

- [ ] **Step 3: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS (all tests across all three crates)

- [ ] **Step 4: Run the formatting and lint gate**

Run: `cargo fmt --check`
Expected: no output (already formatted)

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings/errors

If either command reports an issue, fix it (`cargo fmt` to auto-format,
or address the specific clippy lint) before continuing.

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-node/tests
git commit -m "test: add end-to-end wire protocol integration tests"
```

---

## Self-Review Notes

- **Spec coverage:** primary-key datum CRUD ✓ (Task 6–9), SK-as-regular-
  datum ✓ (Task 9's dedicated test), tagged Native/Foreign `authority_idx`
  ✓ (Task 2), write-before-ack ✓ (Task 5's `Cache::put`/`delete` write to
  `Store` before returning), custom binary framing ✓ (Task 6), no
  auth/encryption ✓ (nothing added). Ring, gossip, collation, and the real
  storage tier are explicitly out of scope for this plan — they're
  Sub-projects 2–4.
- **Placeholder scan:** no TBD/TODO; every `unimplemented!()` stub is
  replaced with real code within the same task, before that task's commit
  step.
- **Type consistency:** `DatumId`, `AuthorityIdx` (with `ENCODED_LEN`),
  `Store`, `Cache`, `Request`/`Response`, and `WorkerHandle` are each
  defined once and referenced with identical names/signatures in every
  later task that consumes them.
