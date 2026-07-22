# Index Storage Engine (rk) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `seisin-storage`, a standalone crate implementing a generic, byte-keyed, disk-backed counted B+Tree, with zero dependency on `DatumId`/ring/gossip/node concepts.

**Architecture:** Fixed-size keys/values chosen at tree-creation time, fixed-size pages (a configurable power-of-2 size, minimum 4096 bytes) in one file per tree, insert-only (upsert, no delete — so no free-list needed). A superblock (page 0) carries format/config metadata and is validated on open, returning `Result` rather than panicking on mismatch/corruption. Leaf pages are sibling-linked for bounded forward/backward scans; internal pages carry a subtree-entry-count alongside each child pointer for O(log n) rank-based lookup. No WAL, no per-write fsync, no crash-safety machinery — this tree is a rebuildable performance structure, not a source of truth; a `rebuild_from` operation wipes and bulk-loads from a caller-supplied iterator (the caller's responsibility to re-derive entries from a full datum scan).

**Tech Stack:** Rust, `anyhow` for errors, `tempfile` (dev-dependency) for test fixtures. No other new dependencies.

## Global Constraints

- Fixed-size keys and values, chosen at tree-creation time (`key_size`/`value_size` in bytes), fixed for the tree's lifetime.
- Insert is upsert only — **no delete** in this plan's scope.
- `page_size` must be a power of 2, `>= 4096`, configurable per tree at creation (never hardcoded).
- No WAL/fsync/crash-safety machinery — `open()` validates the superblock and returns `Result` (never panics) on mismatch; recovery is the caller's responsibility via `rebuild_from`.
- 2-space indentation (matches this repo's `rustfmt.toml`: `tab_spaces = 2`).
- Error handling: `anyhow::Result<T>` + `bail!()`/`.context()` throughout — this repo's only accepted style for new Rust code.
- Explicitly out of scope for this plan (see the spec's Open Questions): rk's own `IndexKind` logic built on this engine, node-function/placement wiring, page-size auto-detection, and an operator-facing benchmark tool.

---

### Task 1: Crate scaffold and raw page I/O

**Files:**
- Create: `crates/seisin-storage/Cargo.toml`
- Create: `crates/seisin-storage/src/lib.rs`
- Create: `crates/seisin-storage/src/page_store.rs`
- Modify: `/Users/cliff/play/seisin/Cargo.toml` (add `crates/seisin-storage` to workspace `members`)

**Interfaces:**
- Produces: `PageId = u64` and `NULL_PAGE: PageId = 0` (in `lib.rs`); `PageStore::create(path: &Path, page_size: u32) -> Result<Self>`, `PageStore::open(path: &Path, page_size: u32) -> Result<Self>`, `PageStore::page_size(&self) -> u32`, `PageStore::read_page(&mut self, id: PageId) -> Result<Vec<u8>>`, `PageStore::write_page(&mut self, id: PageId, bytes: &[u8]) -> Result<()>`, `PageStore::truncate(&mut self) -> Result<()>`.

- [ ] **Step 1: Create the crate and register it in the workspace**

```toml
# crates/seisin-storage/Cargo.toml
[package]
name = "seisin-storage"
version = "0.1.0"
edition = "2021"

[dependencies]
anyhow = "1"

[dev-dependencies]
tempfile = "3"
```

Modify `/Users/cliff/play/seisin/Cargo.toml`'s `members` list to add `"crates/seisin-storage"`:

```toml
[workspace]
resolver = "2"
members = ["crates/seisin-core", "crates/seisin-protocol", "crates/seisin-node", "crates/seisin-ring", "crates/seisin-client", "crates/seisin-gossip", "crates/seisin-ops", "crates/seisin-types", "crates/seisin-storage"]
```

- [ ] **Step 2: Write `lib.rs` and the failing tests for `page_store.rs`**

```rust
// crates/seisin-storage/src/lib.rs
pub mod page_store;

pub type PageId = u64;
pub const NULL_PAGE: PageId = 0;
```

```rust
// crates/seisin-storage/src/page_store.rs
//! Raw fixed-size page I/O over a single file. Agnostic of superblock/
//! B+Tree semantics — just reads and writes whole `page_size`-byte pages
//! by page id. Page id 0 is where `seisin-storage`'s superblock always
//! lives, but `PageStore` itself doesn't know or care about that.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{bail, Result};

use crate::PageId;

pub struct PageStore {
  file: File,
  page_size: u32,
}

impl PageStore {
  /// Creates a new, empty page file at `path`, truncating it if it
  /// already exists.
  pub fn create(path: &Path, page_size: u32) -> Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .truncate(true)
      .open(path)?;
    Ok(Self { file, page_size })
  }

  /// Opens an existing page file at `path`. `page_size` must already be
  /// known (read from the superblock's first page by the caller before
  /// calling this) since page offsets depend on it.
  pub fn open(path: &Path, page_size: u32) -> Result<Self> {
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    Ok(Self { file, page_size })
  }

  pub fn page_size(&self) -> u32 {
    self.page_size
  }

  /// Reads page `id`'s full `page_size` bytes. A page beyond the current
  /// end of file (e.g. one never written) reads back as all zeros.
  pub fn read_page(&mut self, id: PageId) -> Result<Vec<u8>> {
    let offset = id * self.page_size as u64;
    let mut buf = vec![0u8; self.page_size as usize];
    let file_len = self.file.metadata()?.len();
    if offset >= file_len {
      return Ok(buf);
    }
    self.file.seek(SeekFrom::Start(offset))?;
    let to_read = self.page_size as u64;
    if offset + to_read > file_len {
      let available = (file_len - offset) as usize;
      self.file.read_exact(&mut buf[..available])?;
    } else {
      self.file.read_exact(&mut buf)?;
    }
    Ok(buf)
  }

  /// Writes `bytes` (must be exactly `page_size` long) as page `id`,
  /// extending the file if `id` is beyond its current end.
  pub fn write_page(&mut self, id: PageId, bytes: &[u8]) -> Result<()> {
    if bytes.len() != self.page_size as usize {
      bail!(
        "write_page expected exactly {} bytes, got {}",
        self.page_size,
        bytes.len()
      );
    }
    let offset = id * self.page_size as u64;
    self.file.seek(SeekFrom::Start(offset))?;
    self.file.write_all(bytes)?;
    Ok(())
  }

  /// Truncates the file to zero length — used by `rebuild_from` to wipe
  /// and start over.
  pub fn truncate(&mut self) -> Result<()> {
    self.file.set_len(0)?;
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use tempfile::NamedTempFile;

  #[test]
  fn writes_and_reads_back_a_page() {
    let tmp = NamedTempFile::new().unwrap();
    let mut store = PageStore::create(tmp.path(), 4096).unwrap();
    let mut page = vec![0u8; 4096];
    page[0] = 42;
    store.write_page(3, &page).unwrap();
    let read_back = store.read_page(3).unwrap();
    assert_eq!(read_back, page);
  }

  #[test]
  fn reading_an_unwritten_page_returns_zeros() {
    let tmp = NamedTempFile::new().unwrap();
    let mut store = PageStore::create(tmp.path(), 4096).unwrap();
    let page = store.read_page(5).unwrap();
    assert_eq!(page, vec![0u8; 4096]);
  }

  #[test]
  fn write_page_rejects_the_wrong_length() {
    let tmp = NamedTempFile::new().unwrap();
    let mut store = PageStore::create(tmp.path(), 4096).unwrap();
    assert!(store.write_page(0, &[0u8; 100]).is_err());
  }

  #[test]
  fn open_reads_back_pages_written_before_reopening() {
    let tmp = NamedTempFile::new().unwrap();
    {
      let mut store = PageStore::create(tmp.path(), 4096).unwrap();
      let mut page = vec![0u8; 4096];
      page[10] = 7;
      store.write_page(2, &page).unwrap();
    }
    let mut store = PageStore::open(tmp.path(), 4096).unwrap();
    let page = store.read_page(2).unwrap();
    assert_eq!(page[10], 7);
  }

  #[test]
  fn truncate_empties_the_file() {
    let tmp = NamedTempFile::new().unwrap();
    let mut store = PageStore::create(tmp.path(), 4096).unwrap();
    store.write_page(0, &vec![9u8; 4096]).unwrap();
    store.truncate().unwrap();
    let page = store.read_page(0).unwrap();
    assert_eq!(page, vec![0u8; 4096]);
  }
}
```

- [ ] **Step 3: Run the tests to verify they fail to compile/run before this task's code exists**

Run: `cargo test -p seisin-storage --lib`
Expected: FAIL — crate doesn't build yet until the files above exist (this step is really "run it once the files are saved but before double-checking it's wired into the workspace" — since Step 2 already contains the real implementation, not a stub, this step confirms the crate compiles and the tests pass on the first real attempt).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-storage --lib`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock crates/seisin-storage
git commit -m "feat: scaffold seisin-storage crate with raw fixed-size page I/O"
```

---

### Task 2: Superblock format

**Files:**
- Create: `crates/seisin-storage/src/superblock.rs`
- Modify: `crates/seisin-storage/src/lib.rs`

**Interfaces:**
- Consumes: nothing from Task 1 directly (pure encode/decode, no `PageStore` dependency).
- Produces: `Superblock { page_size: u32, key_size: u32, value_size: u32, root_page_id: u64, total_count: u64, next_page_id: u64 }`, `Superblock::validate_page_size(page_size: u32) -> Result<()>`, `Superblock::encode(&self, page_size: u32) -> Vec<u8>`, `Superblock::decode(bytes: &[u8]) -> Result<Self>`.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/seisin-storage/src/superblock.rs
//! The B+Tree file's page-0 header: format identification, the fixed
//! key/value/page sizes chosen at creation time, and the tree's current
//! root/count/next-allocation state. See the design doc's "On-Disk
//! Format" section.

use anyhow::{bail, Result};

const MAGIC: &[u8; 8] = b"SEISNBT1";
const HEADER_LEN: usize = 44;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Superblock {
  pub page_size: u32,
  pub key_size: u32,
  pub value_size: u32,
  pub root_page_id: u64,
  pub total_count: u64,
  pub next_page_id: u64,
}

impl Superblock {
  pub fn validate_page_size(_page_size: u32) -> Result<()> {
    unimplemented!()
  }

  pub fn encode(&self, _page_size: u32) -> Vec<u8> {
    unimplemented!()
  }

  pub fn decode(_bytes: &[u8]) -> Result<Self> {
    unimplemented!()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn sample() -> Superblock {
    Superblock {
      page_size: 4096,
      key_size: 8,
      value_size: 16,
      root_page_id: 1,
      total_count: 0,
      next_page_id: 2,
    }
  }

  #[test]
  fn round_trips_through_bytes() {
    let sb = sample();
    let encoded = sb.encode(4096);
    assert_eq!(Superblock::decode(&encoded).unwrap(), sb);
  }

  #[test]
  fn decode_rejects_wrong_magic() {
    let sb = sample();
    let mut encoded = sb.encode(4096);
    encoded[0] = 0xFF;
    assert!(Superblock::decode(&encoded).is_err());
  }

  #[test]
  fn decode_rejects_too_short_input() {
    assert!(Superblock::decode(&[0u8; 10]).is_err());
  }

  #[test]
  fn validate_page_size_rejects_below_4096() {
    assert!(Superblock::validate_page_size(2048).is_err());
  }

  #[test]
  fn validate_page_size_rejects_non_power_of_two() {
    assert!(Superblock::validate_page_size(5000).is_err());
  }

  #[test]
  fn validate_page_size_accepts_4096_and_larger_powers_of_two() {
    assert!(Superblock::validate_page_size(4096).is_ok());
    assert!(Superblock::validate_page_size(8192).is_ok());
    assert!(Superblock::validate_page_size(65536).is_ok());
  }
}
```

Add `pub mod superblock;` to `crates/seisin-storage/src/lib.rs`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-storage --lib superblock::`
Expected: FAIL (panics with "not implemented" on every test).

- [ ] **Step 3: Implement**

Replace the three `unimplemented!()` bodies:

```rust
  pub fn validate_page_size(page_size: u32) -> Result<()> {
    if page_size < 4096 {
      bail!("page_size must be at least 4096, got {page_size}");
    }
    if !page_size.is_power_of_two() {
      bail!("page_size must be a power of 2, got {page_size}");
    }
    Ok(())
  }

  pub fn encode(&self, page_size: u32) -> Vec<u8> {
    let mut buf = vec![0u8; page_size as usize];
    buf[0..8].copy_from_slice(MAGIC);
    buf[8..12].copy_from_slice(&self.page_size.to_le_bytes());
    buf[12..16].copy_from_slice(&self.key_size.to_le_bytes());
    buf[16..20].copy_from_slice(&self.value_size.to_le_bytes());
    buf[20..28].copy_from_slice(&self.root_page_id.to_le_bytes());
    buf[28..36].copy_from_slice(&self.total_count.to_le_bytes());
    buf[36..44].copy_from_slice(&self.next_page_id.to_le_bytes());
    buf
  }

  pub fn decode(bytes: &[u8]) -> Result<Self> {
    if bytes.len() < HEADER_LEN {
      bail!("superblock page is too short to contain a header");
    }
    if &bytes[0..8] != MAGIC {
      bail!("not a seisin-storage B+Tree file (magic mismatch)");
    }
    let page_size = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let key_size = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
    let value_size = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
    let root_page_id = u64::from_le_bytes(bytes[20..28].try_into().unwrap());
    let total_count = u64::from_le_bytes(bytes[28..36].try_into().unwrap());
    let next_page_id = u64::from_le_bytes(bytes[36..44].try_into().unwrap());
    Self::validate_page_size(page_size)?;
    Ok(Self {
      page_size,
      key_size,
      value_size,
      root_page_id,
      total_count,
      next_page_id,
    })
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-storage --lib superblock::`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-storage/src/superblock.rs crates/seisin-storage/src/lib.rs
git commit -m "feat: add seisin-storage superblock format with page_size/key_size/value_size validation"
```

---

### Task 3: Leaf node encode/decode

**Files:**
- Create: `crates/seisin-storage/src/node.rs`
- Modify: `crates/seisin-storage/src/lib.rs`

**Interfaces:**
- Consumes: nothing from earlier tasks (pure encode/decode over byte slices).
- Produces: `LeafNode { prev: PageId, next: PageId, entries: Vec<(Vec<u8>, Vec<u8>)> }`, `max_leaf_entries(page_size: u32, key_size: u32, value_size: u32) -> usize`, `encode_leaf(node: &LeafNode, page_size: u32, key_size: u32, value_size: u32) -> Vec<u8>`, `decode_leaf(bytes: &[u8], key_size: u32, value_size: u32) -> Result<LeafNode>`. Also the shared constants `NODE_HEADER_LEN: usize = 32`, `LEAF_PAGE_TYPE: u8 = 0` (Task 4 adds `INTERNAL_PAGE_TYPE: u8 = 1` to this same file).

- [ ] **Step 1: Write the failing tests**

```rust
// crates/seisin-storage/src/node.rs
//! In-memory decoded representations of a page's content (leaf or
//! internal), and their fixed-size on-disk encoding. Records are
//! fixed-size (`key_size`/`value_size` chosen at tree-creation time), so
//! a page's capacity is a pure function of `page_size`/`key_size`/
//! `value_size`. See the design doc's "On-Disk Format" section.

use anyhow::{bail, Result};

use crate::PageId;

pub(crate) const NODE_HEADER_LEN: usize = 32;
pub(crate) const LEAF_PAGE_TYPE: u8 = 0;

/// How many `(key, value)` entries fit in one leaf page after its
/// header.
pub fn max_leaf_entries(page_size: u32, key_size: u32, value_size: u32) -> usize {
  let record_len = key_size as usize + value_size as usize;
  (page_size as usize - NODE_HEADER_LEN) / record_len
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafNode {
  pub prev: PageId,
  pub next: PageId,
  /// Sorted ascending by key.
  pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

pub fn encode_leaf(_node: &LeafNode, _page_size: u32, _key_size: u32, _value_size: u32) -> Vec<u8> {
  unimplemented!()
}

pub fn decode_leaf(_bytes: &[u8], _key_size: u32, _value_size: u32) -> Result<LeafNode> {
  unimplemented!()
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn round_trips_an_empty_leaf() {
    let node = LeafNode { prev: 0, next: 0, entries: vec![] };
    let encoded = encode_leaf(&node, 4096, 8, 8);
    assert_eq!(decode_leaf(&encoded, 8, 8).unwrap(), node);
  }

  #[test]
  fn round_trips_a_leaf_with_entries_and_sibling_links() {
    let node = LeafNode {
      prev: 3,
      next: 7,
      entries: vec![(vec![1; 8], vec![10; 8]), (vec![2; 8], vec![20; 8])],
    };
    let encoded = encode_leaf(&node, 4096, 8, 8);
    assert_eq!(decode_leaf(&encoded, 8, 8).unwrap(), node);
  }

  #[test]
  fn decode_rejects_a_non_leaf_page_type_byte() {
    let mut bytes = vec![0u8; 4096];
    bytes[0] = 1; // not LEAF_PAGE_TYPE
    assert!(decode_leaf(&bytes, 8, 8).is_err());
  }

  #[test]
  fn max_leaf_entries_matches_expected_capacity() {
    // (4096 - 32) / (8 + 8) = 254
    assert_eq!(max_leaf_entries(4096, 8, 8), 254);
  }

  #[test]
  fn max_leaf_entries_shrinks_with_larger_records() {
    // (4096 - 32) / (1000 + 1000) = 2
    assert_eq!(max_leaf_entries(4096, 1000, 1000), 2);
  }
}
```

Add `pub mod node;` to `crates/seisin-storage/src/lib.rs`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-storage --lib node::`
Expected: FAIL (panics with "not implemented" on the round-trip/rejection tests; the two `max_leaf_entries` tests already pass since that function is implemented).

- [ ] **Step 3: Implement `encode_leaf`/`decode_leaf`**

```rust
pub fn encode_leaf(node: &LeafNode, page_size: u32, key_size: u32, value_size: u32) -> Vec<u8> {
  let mut buf = vec![0u8; page_size as usize];
  buf[0] = LEAF_PAGE_TYPE;
  buf[8..16].copy_from_slice(&node.prev.to_le_bytes());
  buf[16..24].copy_from_slice(&node.next.to_le_bytes());
  buf[24..28].copy_from_slice(&(node.entries.len() as u32).to_le_bytes());
  let record_len = key_size as usize + value_size as usize;
  let mut offset = NODE_HEADER_LEN;
  for (key, value) in &node.entries {
    buf[offset..offset + key_size as usize].copy_from_slice(key);
    buf[offset + key_size as usize..offset + record_len].copy_from_slice(value);
    offset += record_len;
  }
  buf
}

pub fn decode_leaf(bytes: &[u8], key_size: u32, value_size: u32) -> Result<LeafNode> {
  if bytes.first() != Some(&LEAF_PAGE_TYPE) {
    bail!("page is not a leaf page (wrong page_type byte)");
  }
  let prev = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
  let next = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
  let entry_count = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
  let record_len = key_size as usize + value_size as usize;
  let mut entries = Vec::with_capacity(entry_count);
  let mut offset = NODE_HEADER_LEN;
  for _ in 0..entry_count {
    let key = bytes[offset..offset + key_size as usize].to_vec();
    let value = bytes[offset + key_size as usize..offset + record_len].to_vec();
    entries.push((key, value));
    offset += record_len;
  }
  Ok(LeafNode { prev, next, entries })
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-storage --lib node::`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-storage/src/node.rs crates/seisin-storage/src/lib.rs
git commit -m "feat: add leaf node encode/decode with fixed-size records"
```

---

### Task 4: Internal node encode/decode and page-type dispatch

**Files:**
- Modify: `crates/seisin-storage/src/node.rs`

**Interfaces:**
- Consumes: `NODE_HEADER_LEN`, `LEAF_PAGE_TYPE` (Task 3, same file).
- Produces: `InternalNode { entries: Vec<(Vec<u8>, PageId, u64)>, rightmost_child: PageId, rightmost_count: u64 }`, `max_internal_entries(page_size: u32, key_size: u32) -> usize`, `encode_internal(node: &InternalNode, page_size: u32, key_size: u32) -> Vec<u8>`, `decode_internal(bytes: &[u8], key_size: u32) -> Result<InternalNode>`, `PageType { Leaf, Internal }`, `page_type(bytes: &[u8]) -> Result<PageType>`.

Internal-node entry semantics (used by every later task): `entries[i] = (separator_key, child, count)` means `child` holds all keys strictly less than `separator_key`; `rightmost_child`/`rightmost_count` holds all keys greater than or equal to `entries[entries.len() - 1].0` (or literally all keys, if `entries` is empty).

- [ ] **Step 1: Write the failing tests**

Append to `crates/seisin-storage/src/node.rs`, above its existing `#[cfg(test)] mod tests` block:

```rust
pub(crate) const INTERNAL_PAGE_TYPE: u8 = 1;

/// How many `(separator_key, child, count)` entries fit in one internal
/// page after its header (the header itself already holds the
/// rightmost child/count, so this is purely about the entries array).
pub fn max_internal_entries(page_size: u32, key_size: u32) -> usize {
  let record_len = key_size as usize + 16; // child: u64, count: u64
  (page_size as usize - NODE_HEADER_LEN) / record_len
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalNode {
  /// Sorted ascending by separator key. `entries[i].1` holds all keys
  /// strictly less than `entries[i].0`.
  pub entries: Vec<(Vec<u8>, PageId, u64)>,
  /// Holds all keys greater than or equal to the last entry's
  /// separator (or literally all keys, if `entries` is empty).
  pub rightmost_child: PageId,
  pub rightmost_count: u64,
}

pub fn encode_internal(_node: &InternalNode, _page_size: u32, _key_size: u32) -> Vec<u8> {
  unimplemented!()
}

pub fn decode_internal(_bytes: &[u8], _key_size: u32) -> Result<InternalNode> {
  unimplemented!()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
  Leaf,
  Internal,
}

pub fn page_type(bytes: &[u8]) -> Result<PageType> {
  match bytes.first() {
    Some(&LEAF_PAGE_TYPE) => Ok(PageType::Leaf),
    Some(&INTERNAL_PAGE_TYPE) => Ok(PageType::Internal),
    _ => bail!("unrecognized page type byte"),
  }
}
```

Add these tests inside the existing `mod tests` block:

```rust
  #[test]
  fn round_trips_an_internal_node_with_no_entries() {
    let node = InternalNode { entries: vec![], rightmost_child: 9, rightmost_count: 3 };
    let encoded = encode_internal(&node, 4096, 8);
    assert_eq!(decode_internal(&encoded, 8).unwrap(), node);
  }

  #[test]
  fn round_trips_an_internal_node_with_entries() {
    let node = InternalNode {
      entries: vec![(vec![5; 8], 2, 10), (vec![9; 8], 3, 20)],
      rightmost_child: 4,
      rightmost_count: 30,
    };
    let encoded = encode_internal(&node, 4096, 8);
    assert_eq!(decode_internal(&encoded, 8).unwrap(), node);
  }

  #[test]
  fn decode_internal_rejects_a_leaf_page_type_byte() {
    let mut bytes = vec![0u8; 4096];
    bytes[0] = LEAF_PAGE_TYPE;
    assert!(decode_internal(&bytes, 8).is_err());
  }

  #[test]
  fn max_internal_entries_matches_expected_capacity() {
    // (4096 - 32) / (8 + 16) = 169
    assert_eq!(max_internal_entries(4096, 8), 169);
  }

  #[test]
  fn page_type_identifies_leaf_and_internal_pages() {
    let leaf_bytes = encode_leaf(&LeafNode { prev: 0, next: 0, entries: vec![] }, 4096, 8, 8);
    let internal_bytes =
      encode_internal(&InternalNode { entries: vec![], rightmost_child: 1, rightmost_count: 0 }, 4096, 8);
    assert_eq!(page_type(&leaf_bytes).unwrap(), PageType::Leaf);
    assert_eq!(page_type(&internal_bytes).unwrap(), PageType::Internal);
  }

  #[test]
  fn page_type_rejects_an_unrecognized_byte() {
    let mut bytes = vec![0u8; 4096];
    bytes[0] = 0xFF;
    assert!(page_type(&bytes).is_err());
  }
```

- [ ] **Step 2: Run the tests to verify the new ones fail**

Run: `cargo test -p seisin-storage --lib node::`
Expected: FAIL on the internal-node round-trip/rejection tests (panics with "not implemented"); the capacity test and leaf tests still pass.

- [ ] **Step 3: Implement `encode_internal`/`decode_internal`**

```rust
pub fn encode_internal(node: &InternalNode, page_size: u32, key_size: u32) -> Vec<u8> {
  let mut buf = vec![0u8; page_size as usize];
  buf[0] = INTERNAL_PAGE_TYPE;
  buf[8..16].copy_from_slice(&node.rightmost_child.to_le_bytes());
  buf[16..24].copy_from_slice(&node.rightmost_count.to_le_bytes());
  buf[24..28].copy_from_slice(&(node.entries.len() as u32).to_le_bytes());
  let record_len = key_size as usize + 16;
  let mut offset = NODE_HEADER_LEN;
  for (key, child, count) in &node.entries {
    buf[offset..offset + key_size as usize].copy_from_slice(key);
    let child_offset = offset + key_size as usize;
    buf[child_offset..child_offset + 8].copy_from_slice(&child.to_le_bytes());
    buf[child_offset + 8..child_offset + 16].copy_from_slice(&count.to_le_bytes());
    offset += record_len;
  }
  buf
}

pub fn decode_internal(bytes: &[u8], key_size: u32) -> Result<InternalNode> {
  if bytes.first() != Some(&INTERNAL_PAGE_TYPE) {
    bail!("page is not an internal page (wrong page_type byte)");
  }
  let rightmost_child = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
  let rightmost_count = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
  let entry_count = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
  let record_len = key_size as usize + 16;
  let mut entries = Vec::with_capacity(entry_count);
  let mut offset = NODE_HEADER_LEN;
  for _ in 0..entry_count {
    let key = bytes[offset..offset + key_size as usize].to_vec();
    let child_offset = offset + key_size as usize;
    let child = u64::from_le_bytes(bytes[child_offset..child_offset + 8].try_into().unwrap());
    let count = u64::from_le_bytes(bytes[child_offset + 8..child_offset + 16].try_into().unwrap());
    entries.push((key, child, count));
    offset += record_len;
  }
  Ok(InternalNode { entries, rightmost_child, rightmost_count })
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-storage --lib node::`
Expected: PASS (11 tests total in `node::`).

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-storage/src/node.rs
git commit -m "feat: add internal node encode/decode and page-type dispatch"
```

---

### Task 5: `BPlusTree` scaffold — create/open and single-page insert

**Files:**
- Create: `crates/seisin-storage/src/btree.rs`
- Modify: `crates/seisin-storage/src/lib.rs`

**Interfaces:**
- Consumes: `PageStore` (Task 1); `Superblock`/`Superblock::validate_page_size` (Task 2); `LeafNode`/`InternalNode`/`encode_leaf`/`decode_leaf`/`encode_internal`/`decode_internal`/`max_leaf_entries`/`max_internal_entries`/`PageType`/`page_type` (Tasks 3-4); `PageId`/`NULL_PAGE` (Task 1's `lib.rs`).
- Produces: `BPlusTree::create(path: &Path, key_size: u32, value_size: u32, page_size: u32) -> Result<Self>`, `BPlusTree::open(path: &Path) -> Result<Self>`, `BPlusTree::insert(&mut self, key: &[u8], value: &[u8]) -> Result<()>` (this task: single leaf page only, errors on overflow — Task 6 adds real splitting), `BPlusTree::len(&self) -> usize`, `BPlusTree::is_empty(&self) -> bool`. A `#[cfg(test)]`-only `all_entries_for_test(&mut self) -> Result<Vec<(Vec<u8>, Vec<u8>)>>` full in-order traversal helper, used by this and later tasks' tests to verify tree contents before `scan_forward_bounded` exists (Task 8).

- [ ] **Step 1: Write the failing tests**

```rust
// crates/seisin-storage/src/btree.rs
//! The counted B+Tree itself: create/open a tree backed by a single
//! page file, insert (upsert-only, no delete), and the bounded-scan/
//! rank-sampling/rebuild operations built in later tasks. See the
//! design doc's "Operations" section.

use std::fs::File;
use std::io::Read;
use std::path::Path;

use anyhow::{bail, Result};

use crate::node::{
  decode_internal, decode_leaf, encode_internal, encode_leaf, max_internal_entries,
  max_leaf_entries, page_type, InternalNode, LeafNode, PageType,
};
use crate::page_store::PageStore;
use crate::superblock::Superblock;
use crate::{PageId, NULL_PAGE};

pub struct BPlusTree {
  store: PageStore,
  page_size: u32,
  key_size: u32,
  value_size: u32,
  root_page_id: PageId,
  total_count: u64,
  next_page_id: PageId,
  max_leaf_entries: usize,
  max_internal_entries: usize,
}

impl BPlusTree {
  pub fn create(path: &Path, key_size: u32, value_size: u32, page_size: u32) -> Result<Self> {
    Superblock::validate_page_size(page_size)?;
    let max_leaf = max_leaf_entries(page_size, key_size, value_size);
    let max_internal = max_internal_entries(page_size, key_size);
    if max_leaf == 0 {
      bail!("key_size + value_size too large for page_size {page_size}");
    }
    if max_internal == 0 {
      bail!("key_size too large for page_size {page_size} (internal node capacity)");
    }
    let mut store = PageStore::create(path, page_size)?;
    let root_id: PageId = 1;
    let empty_leaf = LeafNode { prev: NULL_PAGE, next: NULL_PAGE, entries: vec![] };
    store.write_page(root_id, &encode_leaf(&empty_leaf, page_size, key_size, value_size))?;
    let mut tree = Self {
      store,
      page_size,
      key_size,
      value_size,
      root_page_id: root_id,
      total_count: 0,
      next_page_id: 2,
      max_leaf_entries: max_leaf,
      max_internal_entries: max_internal,
    };
    tree.write_superblock()?;
    Ok(tree)
  }

  pub fn open(path: &Path) -> Result<Self> {
    let mut header_buf = vec![0u8; 44];
    let mut file = File::open(path)?;
    file.read_exact(&mut header_buf)?;
    let sb = Superblock::decode(&header_buf)?;
    drop(file);
    let store = PageStore::open(path, sb.page_size)?;
    let max_leaf = max_leaf_entries(sb.page_size, sb.key_size, sb.value_size);
    let max_internal = max_internal_entries(sb.page_size, sb.key_size);
    Ok(Self {
      store,
      page_size: sb.page_size,
      key_size: sb.key_size,
      value_size: sb.value_size,
      root_page_id: sb.root_page_id,
      total_count: sb.total_count,
      next_page_id: sb.next_page_id,
      max_leaf_entries: max_leaf,
      max_internal_entries: max_internal,
    })
  }

  fn write_superblock(&mut self) -> Result<()> {
    let sb = Superblock {
      page_size: self.page_size,
      key_size: self.key_size,
      value_size: self.value_size,
      root_page_id: self.root_page_id,
      total_count: self.total_count,
      next_page_id: self.next_page_id,
    };
    self.store.write_page(0, &sb.encode(self.page_size))
  }

  fn allocate_page(&mut self) -> PageId {
    let id = self.next_page_id;
    self.next_page_id += 1;
    id
  }

  fn read_leaf(&mut self, id: PageId) -> Result<LeafNode> {
    let bytes = self.store.read_page(id)?;
    decode_leaf(&bytes, self.key_size, self.value_size)
  }

  fn write_leaf(&mut self, id: PageId, node: &LeafNode) -> Result<()> {
    let bytes = encode_leaf(node, self.page_size, self.key_size, self.value_size);
    self.store.write_page(id, &bytes)
  }

  fn read_internal(&mut self, id: PageId) -> Result<InternalNode> {
    let bytes = self.store.read_page(id)?;
    decode_internal(&bytes, self.key_size)
  }

  fn write_internal(&mut self, id: PageId, node: &InternalNode) -> Result<()> {
    let bytes = encode_internal(node, self.page_size, self.key_size);
    self.store.write_page(id, &bytes)
  }

  pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
    if key.len() != self.key_size as usize {
      bail!("key must be exactly {} bytes, got {}", self.key_size, key.len());
    }
    if value.len() != self.value_size as usize {
      bail!("value must be exactly {} bytes, got {}", self.value_size, value.len());
    }
    let root = self.root_page_id;
    let mut node = self.read_leaf(root)?;
    let is_new = match node.entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
      Ok(i) => {
        node.entries[i].1 = value.to_vec();
        false
      }
      Err(i) => {
        node.entries.insert(i, (key.to_vec(), value.to_vec()));
        true
      }
    };
    if node.entries.len() > self.max_leaf_entries {
      bail!("leaf overflow: splitting is not implemented yet (Task 6)");
    }
    self.write_leaf(root, &node)?;
    if is_new {
      self.total_count += 1;
    }
    self.write_superblock()?;
    Ok(())
  }

  pub fn len(&self) -> usize {
    self.total_count as usize
  }

  pub fn is_empty(&self) -> bool {
    self.total_count == 0
  }
}

#[cfg(test)]
impl BPlusTree {
  /// Full in-order traversal via recursive descent — test-only, used to
  /// verify tree structure/correctness directly. The public API's
  /// `scan_forward_bounded`/`scan_backward_bounded` (Task 8) use the
  /// leaf sibling-link walk instead (bounded, not a full descent).
  fn all_entries_for_test(&mut self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let root = self.root_page_id;
    self.collect_all_for_test(root)
  }

  fn collect_all_for_test(&mut self, page_id: PageId) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let bytes = self.store.read_page(page_id)?;
    match page_type(&bytes)? {
      PageType::Leaf => {
        let node = decode_leaf(&bytes, self.key_size, self.value_size)?;
        Ok(node.entries)
      }
      PageType::Internal => {
        let node = decode_internal(&bytes, self.key_size)?;
        let mut all = Vec::new();
        for (_, child, _) in &node.entries {
          all.extend(self.collect_all_for_test(*child)?);
        }
        all.extend(self.collect_all_for_test(node.rightmost_child)?);
        Ok(all)
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use tempfile::NamedTempFile;

  #[test]
  fn create_then_open_round_trips_an_empty_tree() {
    let tmp = NamedTempFile::new().unwrap();
    {
      let tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
      assert_eq!(tree.len(), 0);
      assert!(tree.is_empty());
    }
    let tree = BPlusTree::open(tmp.path()).unwrap();
    assert_eq!(tree.len(), 0);
  }

  #[test]
  fn create_rejects_an_invalid_page_size() {
    let tmp = NamedTempFile::new().unwrap();
    assert!(BPlusTree::create(tmp.path(), 8, 8, 2048).is_err());
  }

  #[test]
  fn insert_then_reopen_preserves_entries() {
    let tmp = NamedTempFile::new().unwrap();
    {
      let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
      tree.insert(&[1; 8], &[10; 8]).unwrap();
      tree.insert(&[2; 8], &[20; 8]).unwrap();
      assert_eq!(tree.len(), 2);
    }
    let mut tree = BPlusTree::open(tmp.path()).unwrap();
    assert_eq!(tree.len(), 2);
    let mut all = tree.all_entries_for_test().unwrap();
    all.sort();
    assert_eq!(all, vec![(vec![1; 8], vec![10; 8]), (vec![2; 8], vec![20; 8])]);
  }

  #[test]
  fn inserting_the_same_key_twice_overwrites_and_does_not_grow_len() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    tree.insert(&[1; 8], &[10; 8]).unwrap();
    tree.insert(&[1; 8], &[99; 8]).unwrap();
    assert_eq!(tree.len(), 1);
    let all = tree.all_entries_for_test().unwrap();
    assert_eq!(all, vec![(vec![1; 8], vec![99; 8])]);
  }

  #[test]
  fn insert_rejects_a_wrong_sized_key_or_value() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    assert!(tree.insert(&[1; 4], &[10; 8]).is_err());
    assert!(tree.insert(&[1; 8], &[10; 4]).is_err());
  }

  #[test]
  fn overflowing_a_single_leaf_before_splitting_is_implemented_returns_an_error() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    // (4096 - 32) / 16 = 254 entries fit; the 255th must fail until Task 6.
    for i in 0..254u64 {
      tree.insert(&i.to_le_bytes(), &i.to_le_bytes()).unwrap();
    }
    let result = tree.insert(&254u64.to_le_bytes(), &254u64.to_le_bytes());
    assert!(result.is_err());
  }
}
```

Add `pub mod btree;` to `crates/seisin-storage/src/lib.rs`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-storage --lib btree::`
Expected: FAIL to compile — none of `BPlusTree`'s methods exist yet (this step's Step 1 already wrote the real implementation, not stubs, so confirm this compiles and passes once saved, same pattern as Task 1).

- [ ] **Step 3: Run the tests to verify they pass**

Run: `cargo test -p seisin-storage --lib btree::`
Expected: PASS (6 tests).

- [ ] **Step 4: Commit**

```bash
git add crates/seisin-storage/src/btree.rs crates/seisin-storage/src/lib.rs
git commit -m "feat: add BPlusTree create/open and single-leaf insert"
```

---

### Task 6: Leaf splitting (root leaf overflow → two leaves, new internal root)

**Files:**
- Modify: `crates/seisin-storage/src/btree.rs`

**Interfaces:**
- Consumes: everything from Task 5, plus `InternalNode` (Task 4) for constructing the new root.
- Produces: `insert` now handles leaf overflow by splitting instead of erroring. Internal type `InsertOutcome { NoSplit { is_new: bool }, Split { is_new: bool, separator_key: Vec<u8>, new_page_id: PageId, new_page_count: u64 } }` (private to this module; Task 7 reuses it for internal-node splits).

Entries semantics reminder (see Task 4): a split's reported `separator_key` is always the **smallest key of `new_page_id`** — the parent (or, in this task, `insert`'s own root-split handling) treats the original page id as the smaller-key half and `new_page_id` as the larger-key half.

- [ ] **Step 1: Remove Task 5's now-superseded overflow test, write this task's failing test**

Remove `overflowing_a_single_leaf_before_splitting_is_implemented_returns_an_error` from `btree.rs`'s test module (Task 6 makes this pass instead of error, so the old assertion is now wrong, not just outdated).

Add this test to the same `mod tests` block:

```rust
  #[test]
  fn overflowing_a_single_leaf_splits_into_two_leaves_and_a_new_internal_root() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    // (4096 - 32) / 16 = 254 fit in one leaf; insert one more to force a split.
    for i in 0..255u64 {
      tree.insert(&i.to_le_bytes(), &i.to_le_bytes()).unwrap();
    }
    assert_eq!(tree.len(), 255);
    let mut all = tree.all_entries_for_test().unwrap();
    all.sort();
    let expected: Vec<(Vec<u8>, Vec<u8>)> = (0..255u64)
      .map(|i| (i.to_le_bytes().to_vec(), i.to_le_bytes().to_vec()))
      .collect();
    assert_eq!(all, expected);
  }
```

**Note on scope (discovered during execution, corrected here):** a test inserting enough entries to trigger a *second* split (e.g. 300 entries) cannot pass yet even after this task's implementation — once the root becomes an internal page (after the first split), `insert`'s call path in this task still calls `insert_into_leaf` directly on whatever `root_page_id` is, which fails immediately on an internal page's bytes. Routing an insert through an already-internal root requires Task 7's `insert_into`/`insert_into_internal` dispatch. The two multi-split tests originally planned here (`a_split_tree_survives_reopening`, `inserting_out_of_order_still_produces_a_correctly_sorted_tree`) belong in Task 7 instead, where the dispatch logic that makes them possible actually lands — see Task 7's Step 1.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p seisin-storage --lib btree::`
Expected: FAIL — `overflowing_a_single_leaf_splits_into_two_leaves_and_a_new_internal_root` fails (current `insert` errors on overflow instead of splitting).

- [ ] **Step 3: Implement leaf splitting**

Replace `insert`'s body in `crates/seisin-storage/src/btree.rs`:

```rust
enum InsertOutcome {
  NoSplit { is_new: bool },
  Split { is_new: bool, separator_key: Vec<u8>, new_page_id: PageId, new_page_count: u64 },
}

impl BPlusTree {
  pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
    if key.len() != self.key_size as usize {
      bail!("key must be exactly {} bytes, got {}", self.key_size, key.len());
    }
    if value.len() != self.value_size as usize {
      bail!("value must be exactly {} bytes, got {}", self.value_size, value.len());
    }
    let root = self.root_page_id;
    match self.insert_into_leaf(root, key, value)? {
      InsertOutcome::NoSplit { is_new } => {
        if is_new {
          self.total_count += 1;
        }
      }
      InsertOutcome::Split { is_new, separator_key, new_page_id, new_page_count } => {
        let old_root_count = self.total_count + if is_new { 1 } else { 0 } - new_page_count;
        let new_root_id = self.allocate_page();
        let new_root = InternalNode {
          entries: vec![(separator_key, root, old_root_count)],
          rightmost_child: new_page_id,
          rightmost_count: new_page_count,
        };
        self.write_internal(new_root_id, &new_root)?;
        self.root_page_id = new_root_id;
        if is_new {
          self.total_count += 1;
        }
      }
    }
    self.write_superblock()?;
    Ok(())
  }

  fn insert_into_leaf(&mut self, page_id: PageId, key: &[u8], value: &[u8]) -> Result<InsertOutcome> {
    let mut node = self.read_leaf(page_id)?;
    let is_new = match node.entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
      Ok(i) => {
        node.entries[i].1 = value.to_vec();
        false
      }
      Err(i) => {
        node.entries.insert(i, (key.to_vec(), value.to_vec()));
        true
      }
    };
    if node.entries.len() <= self.max_leaf_entries {
      self.write_leaf(page_id, &node)?;
      return Ok(InsertOutcome::NoSplit { is_new });
    }
    let mid = node.entries.len() / 2;
    let right_entries = node.entries.split_off(mid);
    let new_page_id = self.allocate_page();
    let separator_key = right_entries[0].0.clone();
    let new_page_count = right_entries.len() as u64;
    let old_next = node.next;
    node.next = new_page_id;
    let new_node = LeafNode { prev: page_id, next: old_next, entries: right_entries };
    if old_next != NULL_PAGE {
      let mut next_node = self.read_leaf(old_next)?;
      next_node.prev = new_page_id;
      self.write_leaf(old_next, &next_node)?;
    }
    self.write_leaf(page_id, &node)?;
    self.write_leaf(new_page_id, &new_node)?;
    Ok(InsertOutcome::Split { is_new, separator_key, new_page_id, new_page_count })
  }

  pub fn len(&self) -> usize {
    self.total_count as usize
  }

  pub fn is_empty(&self) -> bool {
    self.total_count == 0
  }
}
```

This replaces the old `insert` and the `len`/`is_empty` pair stays the same (repeated here so the block above is a complete, drop-in replacement for everything from `pub fn insert` through the end of the non-test `impl BPlusTree` block written in Task 5).

- [ ] **Step 3: Run the tests to verify they pass**

Run: `cargo test -p seisin-storage --lib btree::`
Expected: PASS (6 tests: the 5 remaining from Task 5 plus this task's 1 new one).

- [ ] **Step 4: Commit**

```bash
git add crates/seisin-storage/src/btree.rs
git commit -m "feat: split an overflowing leaf into two leaves under a new internal root"
```

---

### Task 7: Internal node splitting (multi-level trees)

**Files:**
- Modify: `crates/seisin-storage/src/btree.rs`

**Interfaces:**
- Consumes: `InsertOutcome` (Task 6, same module).
- Produces: `insert_into` (dispatches to leaf or internal by `page_type`), `insert_into_internal` — `insert`'s top-level call switches from calling `insert_into_leaf` directly to calling `insert_into` on the root, so a root that's already internal (from an earlier split) is handled correctly too.

To force a second-level split cheaply in a test (a realistic `page_size >= 4096` with small 8-byte keys needs tens of thousands of inserts to overflow an internal node — `max_internal_entries` is 169 for 8-byte keys at 4096 bytes, so forcing a 2-level split needs roughly `169 * 254 ≈ 43,000` inserts), this task's multi-level test uses a **much larger `key_size`/`value_size`** (1000 bytes each) so `max_leaf_entries` and `max_internal_entries` both shrink to single digits, making a 3+ level tree reachable in well under a hundred inserts.

- [ ] **Step 1: Write the failing tests**

Add to `btree.rs`'s `mod tests` block:

```rust
  #[test]
  fn a_deep_tree_with_small_capacity_pages_stays_correct_across_many_splits() {
    let tmp = NamedTempFile::new().unwrap();
    // key_size=value_size=1000 on a 4096-byte page gives max_leaf_entries=2
    // and max_internal_entries=(4096-32)/(1000+16)=4 — a handful of inserts
    // is enough to force splits several levels deep.
    let mut tree = BPlusTree::create(tmp.path(), 1000, 1000, 4096).unwrap();
    let mut keys: Vec<u64> = (0..200).collect();
    keys.reverse(); // deterministic non-ascending insertion order
    for i in &keys {
      let mut key = vec![0u8; 1000];
      key[0..8].copy_from_slice(&i.to_le_bytes());
      let mut value = vec![0u8; 1000];
      value[0..8].copy_from_slice(&i.to_le_bytes());
      tree.insert(&key, &value).unwrap();
    }
    assert_eq!(tree.len(), 200);
    let mut all = tree.all_entries_for_test().unwrap();
    all.sort();
    for (i, (key, value)) in all.iter().enumerate() {
      let mut expected_key = vec![0u8; 1000];
      expected_key[0..8].copy_from_slice(&(i as u64).to_le_bytes());
      assert_eq!(key, &expected_key);
      assert_eq!(value, &expected_key); // value mirrors key in this test
    }
  }

  #[test]
  fn upserting_an_existing_key_in_a_deep_tree_does_not_change_len() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 1000, 1000, 4096).unwrap();
    for i in 0..200u64 {
      let mut key = vec![0u8; 1000];
      key[0..8].copy_from_slice(&i.to_le_bytes());
      tree.insert(&key, &vec![1u8; 1000]).unwrap();
    }
    let mut key = vec![0u8; 1000];
    key[0..8].copy_from_slice(&50u64.to_le_bytes());
    tree.insert(&key, &vec![2u8; 1000]).unwrap();
    assert_eq!(tree.len(), 200);
    let all = tree.all_entries_for_test().unwrap();
    let updated = all.iter().find(|(k, _)| k == &key).unwrap();
    assert_eq!(updated.1, vec![2u8; 1000]);
  }

  #[test]
  fn a_split_tree_survives_reopening() {
    let tmp = NamedTempFile::new().unwrap();
    {
      let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
      for i in 0..300u64 {
        tree.insert(&i.to_le_bytes(), &i.to_le_bytes()).unwrap();
      }
    }
    let mut tree = BPlusTree::open(tmp.path()).unwrap();
    assert_eq!(tree.len(), 300);
    let mut all = tree.all_entries_for_test().unwrap();
    all.sort();
    assert_eq!(all.len(), 300);
    assert_eq!(all[0], (0u64.to_le_bytes().to_vec(), 0u64.to_le_bytes().to_vec()));
    assert_eq!(all[299], (299u64.to_le_bytes().to_vec(), 299u64.to_le_bytes().to_vec()));
  }

  #[test]
  fn inserting_out_of_order_still_produces_a_correctly_sorted_tree() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    let mut keys: Vec<u64> = (0..300).collect();
    // A fixed, deterministic shuffle (reverse order) rather than real
    // randomness, per this project's preference for seeded/deterministic
    // test inputs over nondeterministic ones.
    keys.reverse();
    for i in &keys {
      tree.insert(&i.to_le_bytes(), &i.to_le_bytes()).unwrap();
    }
    let mut all = tree.all_entries_for_test().unwrap();
    all.sort();
    let expected: Vec<(Vec<u8>, Vec<u8>)> = (0..300u64)
      .map(|i| (i.to_le_bytes().to_vec(), i.to_le_bytes().to_vec()))
      .collect();
    assert_eq!(all, expected);
  }
```

**Note:** the last two tests above were originally drafted under Task 6 but moved here — they require this task's `insert_into`/`insert_into_internal` dispatch (routing an insert through an already-internal root) to pass at all; see Task 6's Step 1 note.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-storage --lib btree::`
Expected: FAIL — all four new tests panic (root's leaf-only `insert` path calls `insert_into_leaf` on whatever `root_page_id` is, but once the root becomes internal after a first split, calling leaf-decode logic on an internal page's bytes fails page-type validation).

- [ ] **Step 3: Implement internal splitting and the leaf/internal dispatch**

Replace `insert`'s body and add `insert_into`/`insert_into_internal` in `crates/seisin-storage/src/btree.rs`:

```rust
impl BPlusTree {
  pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
    if key.len() != self.key_size as usize {
      bail!("key must be exactly {} bytes, got {}", self.key_size, key.len());
    }
    if value.len() != self.value_size as usize {
      bail!("value must be exactly {} bytes, got {}", self.value_size, value.len());
    }
    let root = self.root_page_id;
    match self.insert_into(root, key, value)? {
      InsertOutcome::NoSplit { is_new } => {
        if is_new {
          self.total_count += 1;
        }
      }
      InsertOutcome::Split { is_new, separator_key, new_page_id, new_page_count } => {
        let old_root_count = self.total_count + if is_new { 1 } else { 0 } - new_page_count;
        let new_root_id = self.allocate_page();
        let new_root = InternalNode {
          entries: vec![(separator_key, root, old_root_count)],
          rightmost_child: new_page_id,
          rightmost_count: new_page_count,
        };
        self.write_internal(new_root_id, &new_root)?;
        self.root_page_id = new_root_id;
        if is_new {
          self.total_count += 1;
        }
      }
    }
    self.write_superblock()?;
    Ok(())
  }

  fn insert_into(&mut self, page_id: PageId, key: &[u8], value: &[u8]) -> Result<InsertOutcome> {
    let bytes = self.store.read_page(page_id)?;
    match page_type(&bytes)? {
      PageType::Leaf => self.insert_into_leaf(page_id, key, value),
      PageType::Internal => self.insert_into_internal(page_id, key, value),
    }
  }

  fn insert_into_internal(&mut self, page_id: PageId, key: &[u8], value: &[u8]) -> Result<InsertOutcome> {
    let mut node = self.read_internal(page_id)?;
    let child_idx = node.entries.iter().position(|(k, _, _)| key < k.as_slice());
    let child_id = match child_idx {
      Some(i) => node.entries[i].1,
      None => node.rightmost_child,
    };
    let result = self.insert_into(child_id, key, value)?;
    match result {
      InsertOutcome::NoSplit { is_new } => {
        if is_new {
          match child_idx {
            Some(i) => node.entries[i].2 += 1,
            None => node.rightmost_count += 1,
          }
          self.write_internal(page_id, &node)?;
        }
        Ok(InsertOutcome::NoSplit { is_new })
      }
      InsertOutcome::Split { is_new, separator_key, new_page_id, new_page_count } => {
        match child_idx {
          Some(i) => {
            // child_id (the smaller-key half, unchanged page id) now
            // gets the NEW, smaller separator; new_page_id (the
            // larger-key half) takes over child_id's OLD separator.
            let old_separator = node.entries[i].0.clone();
            let adjusted_count = node.entries[i].2 + if is_new { 1 } else { 0 } - new_page_count;
            node.entries[i] = (separator_key, child_id, adjusted_count);
            node.entries.insert(i + 1, (old_separator, new_page_id, new_page_count));
          }
          None => {
            // the rightmost child split: it keeps the smaller keys and
            // becomes a regular bounded entry; new_page_id becomes the
            // new rightmost.
            let adjusted_count = node.rightmost_count + if is_new { 1 } else { 0 } - new_page_count;
            node.entries.push((separator_key, node.rightmost_child, adjusted_count));
            node.rightmost_child = new_page_id;
            node.rightmost_count = new_page_count;
          }
        }
        if node.entries.len() <= self.max_internal_entries {
          self.write_internal(page_id, &node)?;
          return Ok(InsertOutcome::NoSplit { is_new });
        }
        let mid = node.entries.len() / 2;
        let promoted_key = node.entries[mid].0.clone();
        let promoted_child = node.entries[mid].1;
        let promoted_count = node.entries[mid].2;
        let right_entries = node.entries.split_off(mid + 1);
        node.entries.truncate(mid);
        let new_page_id2 = self.allocate_page();
        let new_internal_count: u64 =
          right_entries.iter().map(|(_, _, c)| *c).sum::<u64>() + node.rightmost_count;
        let new_node = InternalNode {
          entries: right_entries,
          rightmost_child: node.rightmost_child,
          rightmost_count: node.rightmost_count,
        };
        node.rightmost_child = promoted_child;
        node.rightmost_count = promoted_count;
        self.write_internal(page_id, &node)?;
        self.write_internal(new_page_id2, &new_node)?;
        Ok(InsertOutcome::Split {
          is_new,
          separator_key: promoted_key,
          new_page_id: new_page_id2,
          new_page_count: new_internal_count,
        })
      }
    }
  }

  fn insert_into_leaf(&mut self, page_id: PageId, key: &[u8], value: &[u8]) -> Result<InsertOutcome> {
    let mut node = self.read_leaf(page_id)?;
    let is_new = match node.entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
      Ok(i) => {
        node.entries[i].1 = value.to_vec();
        false
      }
      Err(i) => {
        node.entries.insert(i, (key.to_vec(), value.to_vec()));
        true
      }
    };
    if node.entries.len() <= self.max_leaf_entries {
      self.write_leaf(page_id, &node)?;
      return Ok(InsertOutcome::NoSplit { is_new });
    }
    let mid = node.entries.len() / 2;
    let right_entries = node.entries.split_off(mid);
    let new_page_id = self.allocate_page();
    let separator_key = right_entries[0].0.clone();
    let new_page_count = right_entries.len() as u64;
    let old_next = node.next;
    node.next = new_page_id;
    let new_node = LeafNode { prev: page_id, next: old_next, entries: right_entries };
    if old_next != NULL_PAGE {
      let mut next_node = self.read_leaf(old_next)?;
      next_node.prev = new_page_id;
      self.write_leaf(old_next, &next_node)?;
    }
    self.write_leaf(page_id, &node)?;
    self.write_leaf(new_page_id, &new_node)?;
    Ok(InsertOutcome::Split { is_new, separator_key, new_page_id, new_page_count })
  }

  pub fn len(&self) -> usize {
    self.total_count as usize
  }

  pub fn is_empty(&self) -> bool {
    self.total_count == 0
  }
}
```

This replaces everything from `pub fn insert` through the end of the non-test `impl BPlusTree` block (i.e., the whole block Task 6 last wrote).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-storage --lib btree::`
Expected: PASS (10 tests total in `btree::`).

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-storage/src/btree.rs
git commit -m "feat: split an overflowing internal node, supporting multi-level trees"
```

---

### Task 8: Bounded forward/backward scans

**Files:**
- Modify: `crates/seisin-storage/src/btree.rs`

**Interfaces:**
- Consumes: `LeafNode`/`InternalNode`/`decode_internal`/`page_type`/`PageType` (Tasks 3-4), the tree's `root_page_id`/leaf sibling links (Tasks 5-7).
- Produces: `BPlusTree::scan_forward_bounded(&mut self, n: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>>`, `BPlusTree::scan_backward_bounded(&mut self, n: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>>`.

- [ ] **Step 1: Write the failing tests**

Add to `btree.rs`'s `mod tests` block:

```rust
  #[test]
  fn scan_forward_bounded_returns_the_smallest_n_entries_in_ascending_order() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    let mut keys: Vec<u64> = (0..300).collect();
    keys.reverse();
    for i in &keys {
      tree.insert(&i.to_le_bytes(), &i.to_le_bytes()).unwrap();
    }
    let result = tree.scan_forward_bounded(5).unwrap();
    let expected: Vec<(Vec<u8>, Vec<u8>)> = (0..5u64)
      .map(|i| (i.to_le_bytes().to_vec(), i.to_le_bytes().to_vec()))
      .collect();
    assert_eq!(result, expected);
  }

  #[test]
  fn scan_backward_bounded_returns_the_largest_n_entries_in_descending_order() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    for i in 0..300u64 {
      tree.insert(&i.to_le_bytes(), &i.to_le_bytes()).unwrap();
    }
    let result = tree.scan_backward_bounded(5).unwrap();
    let expected: Vec<(Vec<u8>, Vec<u8>)> = (295..300u64)
      .rev()
      .map(|i| (i.to_le_bytes().to_vec(), i.to_le_bytes().to_vec()))
      .collect();
    assert_eq!(result, expected);
  }

  #[test]
  fn scan_forward_bounded_with_n_larger_than_len_returns_everything() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    for i in 0..3u64 {
      tree.insert(&i.to_le_bytes(), &i.to_le_bytes()).unwrap();
    }
    let result = tree.scan_forward_bounded(1000).unwrap();
    assert_eq!(result.len(), 3);
  }

  #[test]
  fn scan_forward_bounded_on_an_empty_tree_returns_nothing() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    assert_eq!(tree.scan_forward_bounded(10).unwrap(), vec![]);
    assert_eq!(tree.scan_backward_bounded(10).unwrap(), vec![]);
  }

  #[test]
  fn scan_bounded_with_n_zero_returns_nothing() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    tree.insert(&1u64.to_le_bytes(), &1u64.to_le_bytes()).unwrap();
    assert_eq!(tree.scan_forward_bounded(0).unwrap(), vec![]);
    assert_eq!(tree.scan_backward_bounded(0).unwrap(), vec![]);
  }

  #[test]
  fn scan_forward_bounded_spans_multiple_leaves_via_sibling_links() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    // 300 entries with max_leaf_entries=254 forces at least one split,
    // so this exercises walking across a leaf sibling link.
    for i in 0..300u64 {
      tree.insert(&i.to_le_bytes(), &i.to_le_bytes()).unwrap();
    }
    let result = tree.scan_forward_bounded(260).unwrap();
    assert_eq!(result.len(), 260);
    assert_eq!(result[0].0, 0u64.to_le_bytes().to_vec());
    assert_eq!(result[259].0, 259u64.to_le_bytes().to_vec());
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-storage --lib btree::`
Expected: FAIL to compile — `scan_forward_bounded`/`scan_backward_bounded` don't exist yet.

- [ ] **Step 3: Implement**

Add to `crates/seisin-storage/src/btree.rs`'s non-test `impl BPlusTree` block (after `is_empty`):

```rust
  pub fn scan_forward_bounded(&mut self, n: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    if n == 0 {
      return Ok(Vec::new());
    }
    let mut page_id = self.leftmost_leaf_id()?;
    let mut results = Vec::with_capacity(n.min(self.total_count as usize));
    while page_id != NULL_PAGE && results.len() < n {
      let node = self.read_leaf(page_id)?;
      for entry in &node.entries {
        if results.len() >= n {
          break;
        }
        results.push(entry.clone());
      }
      page_id = node.next;
    }
    Ok(results)
  }

  pub fn scan_backward_bounded(&mut self, n: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    if n == 0 {
      return Ok(Vec::new());
    }
    let mut page_id = self.rightmost_leaf_id()?;
    let mut results = Vec::with_capacity(n.min(self.total_count as usize));
    while page_id != NULL_PAGE && results.len() < n {
      let node = self.read_leaf(page_id)?;
      for entry in node.entries.iter().rev() {
        if results.len() >= n {
          break;
        }
        results.push(entry.clone());
      }
      page_id = node.prev;
    }
    Ok(results)
  }

  fn leftmost_leaf_id(&mut self) -> Result<PageId> {
    let mut page_id = self.root_page_id;
    loop {
      let bytes = self.store.read_page(page_id)?;
      match page_type(&bytes)? {
        PageType::Leaf => return Ok(page_id),
        PageType::Internal => {
          let node = decode_internal(&bytes, self.key_size)?;
          page_id = match node.entries.first() {
            Some((_, child, _)) => *child,
            None => node.rightmost_child,
          };
        }
      }
    }
  }

  fn rightmost_leaf_id(&mut self) -> Result<PageId> {
    let mut page_id = self.root_page_id;
    loop {
      let bytes = self.store.read_page(page_id)?;
      match page_type(&bytes)? {
        PageType::Leaf => return Ok(page_id),
        PageType::Internal => {
          let node = decode_internal(&bytes, self.key_size)?;
          page_id = node.rightmost_child;
        }
      }
    }
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-storage --lib btree::`
Expected: PASS (16 tests total in `btree::`).

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-storage/src/btree.rs
git commit -m "feat: add bounded forward/backward leaf-sibling scans"
```

---

### Task 9: Rank-based sampling

**Files:**
- Modify: `crates/seisin-storage/src/btree.rs`

**Interfaces:**
- Consumes: internal node subtree counts (Tasks 4/7).
- Produces: `BPlusTree::sample_by_rank(&mut self, k: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>>` — target ranks are 0-indexed, computed as `i * len() / k` for `i` in `0..k`, each resolved via O(log n) counted descent.

- [ ] **Step 1: Write the failing tests**

Add to `btree.rs`'s `mod tests` block:

```rust
  #[test]
  fn sample_by_rank_returns_entries_at_the_expected_evenly_spaced_ranks() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    for i in 0..300u64 {
      tree.insert(&i.to_le_bytes(), &i.to_le_bytes()).unwrap();
    }
    // ranks = i * 300 / 5 for i in 0..5 => 0, 60, 120, 180, 240
    let result = tree.sample_by_rank(5).unwrap();
    let expected_ranks = [0u64, 60, 120, 180, 240];
    assert_eq!(result.len(), 5);
    for (entry, rank) in result.iter().zip(expected_ranks.iter()) {
      assert_eq!(entry.0, rank.to_le_bytes().to_vec());
    }
  }

  #[test]
  fn sample_by_rank_on_an_empty_tree_returns_nothing() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    assert_eq!(tree.sample_by_rank(5).unwrap(), vec![]);
  }

  #[test]
  fn sample_by_rank_with_k_zero_returns_nothing() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    tree.insert(&1u64.to_le_bytes(), &1u64.to_le_bytes()).unwrap();
    assert_eq!(tree.sample_by_rank(0).unwrap(), vec![]);
  }

  #[test]
  fn sample_by_rank_works_across_a_multi_level_tree() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 1000, 1000, 4096).unwrap();
    for i in 0..200u64 {
      let mut key = vec![0u8; 1000];
      key[0..8].copy_from_slice(&i.to_le_bytes());
      tree.insert(&key, &key).unwrap();
    }
    let result = tree.sample_by_rank(4).unwrap();
    let expected_ranks = [0u64, 50, 100, 150];
    assert_eq!(result.len(), 4);
    for (entry, rank) in result.iter().zip(expected_ranks.iter()) {
      let mut expected_key = vec![0u8; 1000];
      expected_key[0..8].copy_from_slice(&rank.to_le_bytes());
      assert_eq!(entry.0, expected_key);
    }
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-storage --lib btree::`
Expected: FAIL to compile — `sample_by_rank` doesn't exist yet.

- [ ] **Step 3: Implement**

Add to `crates/seisin-storage/src/btree.rs`'s non-test `impl BPlusTree` block:

```rust
  pub fn sample_by_rank(&mut self, k: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let total = self.total_count as usize;
    if total == 0 || k == 0 {
      return Ok(Vec::new());
    }
    let mut results = Vec::with_capacity(k);
    for i in 0..k {
      let rank = (i * total) / k;
      results.push(self.entry_at_rank(rank)?);
    }
    Ok(results)
  }

  fn entry_at_rank(&mut self, mut rank: usize) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut page_id = self.root_page_id;
    loop {
      let bytes = self.store.read_page(page_id)?;
      match page_type(&bytes)? {
        PageType::Leaf => {
          let node = decode_leaf(&bytes, self.key_size, self.value_size)?;
          if rank >= node.entries.len() {
            bail!("rank {rank} out of bounds for a leaf with {} entries", node.entries.len());
          }
          return Ok(node.entries[rank].clone());
        }
        PageType::Internal => {
          let node = decode_internal(&bytes, self.key_size)?;
          let mut descended = false;
          for (_, child, count) in &node.entries {
            let count = *count as usize;
            if rank < count {
              page_id = *child;
              descended = true;
              break;
            }
            rank -= count;
          }
          if !descended {
            page_id = node.rightmost_child;
          }
        }
      }
    }
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-storage --lib btree::`
Expected: PASS (20 tests total in `btree::`).

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-storage/src/btree.rs
git commit -m "feat: add sample_by_rank via counted internal-node descent"
```

---

### Task 10: `rebuild_from`

**Files:**
- Modify: `crates/seisin-storage/src/btree.rs`

**Interfaces:**
- Consumes: `PageStore::truncate` (Task 1), leaf/internal encode (Tasks 3-4).
- Produces: `BPlusTree::rebuild_from(&mut self, entries: impl Iterator<Item = (Vec<u8>, Vec<u8>)>) -> Result<()>` — wipes the file and bulk-loads a balanced tree.

- [ ] **Step 1: Write the failing tests**

Add to `btree.rs`'s `mod tests` block:

```rust
  #[test]
  fn rebuild_from_produces_a_tree_equivalent_to_sequential_inserts() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..300u64)
      .map(|i| (i.to_le_bytes().to_vec(), i.to_le_bytes().to_vec()))
      .collect();
    // Feed rebuild_from in a shuffled (reversed) order — it must sort
    // internally rather than assume sorted input.
    let mut shuffled = entries.clone();
    shuffled.reverse();
    tree.rebuild_from(shuffled.into_iter()).unwrap();
    assert_eq!(tree.len(), 300);
    let mut all = tree.all_entries_for_test().unwrap();
    all.sort();
    assert_eq!(all, entries);
  }

  #[test]
  fn rebuild_from_an_empty_iterator_produces_an_empty_tree() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    tree.insert(&1u64.to_le_bytes(), &1u64.to_le_bytes()).unwrap();
    tree.rebuild_from(std::iter::empty()).unwrap();
    assert_eq!(tree.len(), 0);
    assert_eq!(tree.all_entries_for_test().unwrap(), vec![]);
  }

  #[test]
  fn rebuild_from_leaves_a_usable_tree_that_still_supports_insert_and_scans() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..300u64)
      .map(|i| (i.to_le_bytes().to_vec(), i.to_le_bytes().to_vec()))
      .collect();
    tree.rebuild_from(entries.into_iter()).unwrap();
    tree.insert(&999u64.to_le_bytes(), &999u64.to_le_bytes()).unwrap();
    assert_eq!(tree.len(), 301);
    let top = tree.scan_backward_bounded(1).unwrap();
    assert_eq!(top[0].0, 999u64.to_le_bytes().to_vec());
    let sample = tree.sample_by_rank(3).unwrap();
    assert_eq!(sample.len(), 3);
  }

  #[test]
  fn rebuild_from_survives_reopening() {
    let tmp = NamedTempFile::new().unwrap();
    {
      let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
      let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..300u64)
        .map(|i| (i.to_le_bytes().to_vec(), i.to_le_bytes().to_vec()))
        .collect();
      tree.rebuild_from(entries.into_iter()).unwrap();
    }
    let mut tree = BPlusTree::open(tmp.path()).unwrap();
    assert_eq!(tree.len(), 300);
    let mut all = tree.all_entries_for_test().unwrap();
    all.sort();
    assert_eq!(all.len(), 300);
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-storage --lib btree::`
Expected: FAIL to compile — `rebuild_from` doesn't exist yet.

- [ ] **Step 3: Implement**

Add to `crates/seisin-storage/src/btree.rs`'s non-test `impl BPlusTree` block:

```rust
  pub fn rebuild_from(&mut self, entries: impl Iterator<Item = (Vec<u8>, Vec<u8>)>) -> Result<()> {
    let mut sorted: Vec<(Vec<u8>, Vec<u8>)> = entries.collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    self.store.truncate()?;
    self.next_page_id = 1;
    self.total_count = 0;
    if sorted.is_empty() {
      let root_id = self.allocate_page();
      let empty_leaf = LeafNode { prev: NULL_PAGE, next: NULL_PAGE, entries: vec![] };
      self.write_leaf(root_id, &empty_leaf)?;
      self.root_page_id = root_id;
      self.write_superblock()?;
      return Ok(());
    }
    let leaf_chunk_size = self.max_leaf_entries.max(1);
    let leaf_chunks: Vec<&[(Vec<u8>, Vec<u8>)]> = sorted.chunks(leaf_chunk_size).collect();
    let mut level: Vec<(Vec<u8>, PageId, u64)> = Vec::with_capacity(leaf_chunks.len());
    let mut leaf_ids = Vec::with_capacity(leaf_chunks.len());
    for chunk in &leaf_chunks {
      let id = self.allocate_page();
      leaf_ids.push(id);
      level.push((chunk[0].0.clone(), id, chunk.len() as u64));
    }
    for (i, chunk) in leaf_chunks.iter().enumerate() {
      let prev = if i == 0 { NULL_PAGE } else { leaf_ids[i - 1] };
      let next = if i + 1 < leaf_ids.len() { leaf_ids[i + 1] } else { NULL_PAGE };
      let node = LeafNode { prev, next, entries: chunk.to_vec() };
      self.write_leaf(leaf_ids[i], &node)?;
    }
    self.total_count = sorted.len() as u64;
    self.root_page_id = if level.len() == 1 {
      level[0].1
    } else {
      self.build_internal_levels(level)?
    };
    self.write_superblock()?;
    Ok(())
  }

  /// Builds internal levels bottom-up from `level` (a sequence of
  /// `(smallest_key_in_subtree, child_page_id, count)` triples covering
  /// the whole key range in ascending order) until exactly one page
  /// remains, returning its id as the new root.
  fn build_internal_levels(&mut self, mut level: Vec<(Vec<u8>, PageId, u64)>) -> Result<PageId> {
    while level.len() > 1 {
      let group_size = self.max_internal_entries + 1; // each internal page holds this many children
      let mut next_level = Vec::new();
      let mut i = 0;
      while i < level.len() {
        let end = (i + group_size).min(level.len());
        let group = &level[i..end];
        // The last child in the group becomes this page's rightmost
        // child (no upper bound needed within the page); every other
        // child gets an entry whose separator is the NEXT child's
        // smallest key (the exclusive upper bound for that child).
        let mut entries: Vec<(Vec<u8>, PageId, u64)> = Vec::with_capacity(group.len() - 1);
        for j in 0..group.len() - 1 {
          entries.push((group[j + 1].0.clone(), group[j].1, group[j].2));
        }
        let rightmost = group.last().unwrap();
        let total_count: u64 = entries.iter().map(|(_, _, c)| *c).sum::<u64>() + rightmost.2;
        let page_id = self.allocate_page();
        let node = InternalNode {
          entries,
          rightmost_child: rightmost.1,
          rightmost_count: rightmost.2,
        };
        self.write_internal(page_id, &node)?;
        next_level.push((group[0].0.clone(), page_id, total_count));
        i = end;
      }
      level = next_level;
    }
    Ok(level[0].1)
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-storage --lib btree::`
Expected: PASS (24 tests total in `btree::`).

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-storage/src/btree.rs
git commit -m "feat: add rebuild_from — wipe and balanced bulk-load from an entry iterator"
```

---

### Task 11: Page-size-agnostic property tests, quality gate, and progress tracker

**Files:**
- Create: `crates/seisin-storage/tests/integration_page_size_agnostic.rs`
- Modify: `docs/superpowers/PROGRESS.md`

**Interfaces:**
- Consumes: the full public `BPlusTree` API from Tasks 5-10.

- [ ] **Step 1: Write the failing property-style tests**

```rust
// crates/seisin-storage/tests/integration_page_size_agnostic.rs
//! Confirms the on-disk format and every operation are agnostic of the
//! configured page size — the same test logic runs against two distinct
//! valid page sizes (4096 and 16384), per the design doc's Testing
//! Strategy.

use seisin_storage::btree::BPlusTree;
use tempfile::NamedTempFile;

fn exercise_a_tree_at(page_size: u32) {
  let tmp = NamedTempFile::new().unwrap();
  let mut tree = BPlusTree::create(tmp.path(), 8, 8, page_size).unwrap();
  let mut keys: Vec<u64> = (0..500).collect();
  keys.reverse();
  for i in &keys {
    tree.insert(&i.to_le_bytes(), &i.to_le_bytes()).unwrap();
  }
  assert_eq!(tree.len(), 500);

  let forward = tree.scan_forward_bounded(10).unwrap();
  let expected_forward: Vec<(Vec<u8>, Vec<u8>)> =
    (0..10u64).map(|i| (i.to_le_bytes().to_vec(), i.to_le_bytes().to_vec())).collect();
  assert_eq!(forward, expected_forward);

  let backward = tree.scan_backward_bounded(10).unwrap();
  let expected_backward: Vec<(Vec<u8>, Vec<u8>)> = (490..500u64)
    .rev()
    .map(|i| (i.to_le_bytes().to_vec(), i.to_le_bytes().to_vec()))
    .collect();
  assert_eq!(backward, expected_backward);

  let sampled = tree.sample_by_rank(5).unwrap();
  assert_eq!(sampled.len(), 5);
  assert_eq!(sampled[0].0, 0u64.to_le_bytes().to_vec());

  // Overwrite an existing key: len() must not grow.
  tree.insert(&250u64.to_le_bytes(), &999u64.to_le_bytes()).unwrap();
  assert_eq!(tree.len(), 500);

  // rebuild_from round-trips at this page size too.
  let all_entries: Vec<(Vec<u8>, Vec<u8>)> = (0..500u64)
    .map(|i| (i.to_le_bytes().to_vec(), i.to_le_bytes().to_vec()))
    .collect();
  tree.rebuild_from(all_entries.clone().into_iter()).unwrap();
  assert_eq!(tree.len(), 500);
  let forward_after_rebuild = tree.scan_forward_bounded(500).unwrap();
  assert_eq!(forward_after_rebuild, all_entries);
}

#[test]
fn the_engine_behaves_identically_at_the_minimum_page_size() {
  exercise_a_tree_at(4096);
}

#[test]
fn the_engine_behaves_identically_at_a_larger_page_size() {
  exercise_a_tree_at(16384);
}

#[test]
fn create_rejects_a_non_power_of_two_page_size() {
  let tmp = NamedTempFile::new().unwrap();
  assert!(BPlusTree::create(tmp.path(), 8, 8, 5000).is_err());
}

#[test]
fn create_rejects_a_page_size_below_the_minimum() {
  let tmp = NamedTempFile::new().unwrap();
  assert!(BPlusTree::create(tmp.path(), 8, 8, 2048).is_err());
}

#[test]
fn open_rejects_a_file_with_a_corrupted_superblock() {
  let tmp = NamedTempFile::new().unwrap();
  {
    let _tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
  }
  std::fs::write(tmp.path(), [0xFFu8; 4096]).unwrap();
  assert!(BPlusTree::open(tmp.path()).is_err());
}
```

`crates/seisin-storage/src/lib.rs` currently doesn't expose `btree`/`node`/etc. as usable from an external integration test crate unless their modules are `pub` (they already are, per Tasks 1-10's `pub mod ...` declarations) — no `lib.rs` change needed for this task.

- [ ] **Step 2: Run the tests to verify they fail (or pass) honestly**

Run: `cargo test -p seisin-storage --test integration_page_size_agnostic`
Expected: PASS immediately — every operation this test exercises was already implemented and unit-tested in Tasks 5-10; this task adds no new production code, only a cross-page-size confirmation. If any of these fail, that indicates a page-size-dependent bug in an earlier task's implementation (e.g. a hardcoded `4096` that should have used `self.page_size`) — fix the earlier task's code, not this test.

- [ ] **Step 3: Run the full crate's test suite**

Run: `cargo test -p seisin-storage`
Expected: PASS — all unit tests (Tasks 1-10) plus this task's integration tests.

- [ ] **Step 4: Run fmt and clippy**

Run: `cargo fmt --check -p seisin-storage`
Expected: no output; if it reports diffs, run `cargo fmt -p seisin-storage` and re-check.

Run: `cargo clippy -p seisin-storage --all-targets -- -D warnings`
Expected: no warnings/errors.

- [ ] **Step 5: Run the full workspace test suite to confirm no regressions**

Run: `cargo test --workspace`
Expected: PASS — `seisin-storage` is additive (no existing crate depends on it yet), so every previously-passing test should still pass unchanged.

- [ ] **Step 6: Update `docs/superpowers/PROGRESS.md`**

Add an entry under "Done" summarizing: the new `seisin-storage` crate, its scope (a standalone counted B+Tree, no dependency on `DatumId`/ring/gossip/node concepts), the key design points (fixed-size keys/values, configurable power-of-2 page size `>= 4096`, insert-only/no-delete, rebuildable-from-scan durability with no WAL/fsync), its public operations (`create`/`open`/`insert`/`len`/`scan_forward_bounded`/`scan_backward_bounded`/`sample_by_rank`/`rebuild_from`), and that it was built and tested standalone per research at `docs/superpowers/research/2026-07-22-index-storage-engine-choice.md`. Note explicitly that rk's own `IndexKind` logic (built on top of this engine), node-function/placement wiring, page-size auto-detection, and an operator-facing benchmark tool remain separate, not-yet-started pieces.

- [ ] **Step 7: Commit and push**

```bash
git add -A
git commit -m "test: add page-size-agnostic property tests for seisin-storage; update progress tracker"
git push
```

---

## Self-Review Notes

- **Spec coverage**: On-Disk Format (fixed key/value sizes, configurable power-of-2 page size, insert-only/no-delete, superblock, sibling-linked leaves, counted internal nodes) ✓ Tasks 1-4; Operations (`create`/`open`/`insert`/`len`/`scan_forward_bounded`/`scan_backward_bounded`/`sample_by_rank`/`rebuild_from`) ✓ Tasks 5-10; Durability Model (no WAL/fsync, superblock validation detects corruption) ✓ Tasks 2, 5, 11; Testing Strategy's page-size-agnostic requirement ✓ Task 11. rk's `IndexKind`, node-function/placement wiring, page-size auto-detection, and the benchmark tool are explicitly out of scope per the spec and are not addressed by any task here.
- **Placeholder scan**: no TBD/TODO; every step has complete code. The one intentionally-temporary behavior (Task 5's "leaf overflow returns an error") is a real, tested, and explicitly-labeled interim state that Task 6 replaces — not a vague placeholder.
- **Type consistency**: `InsertOutcome` (introduced Task 6) is reused unchanged by Task 7's `insert_into_internal`. `PageId`/`NULL_PAGE` (Task 1) are used consistently through every later task. `LeafNode`/`InternalNode`/`encode_leaf`/`decode_leaf`/`encode_internal`/`decode_internal`/`max_leaf_entries`/`max_internal_entries`/`PageType`/`page_type` (Tasks 3-4) match exactly what `btree.rs` imports and calls in Tasks 5, 7, 8, 9, 10. The internal-node entry convention ("`entries[i].1` holds keys strictly less than `entries[i].0`; `rightmost_child` holds the rest") is stated once in Task 4 and applied consistently in Tasks 6 (root split), 7 (child split promotion/demotion), 8 (leftmost/rightmost leaf descent), 9 (rank descent), and 10 (`build_internal_levels`).
