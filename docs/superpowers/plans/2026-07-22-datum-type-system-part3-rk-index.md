# Datum Type System Part 3 — rk Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline execution — this project never uses subagents) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A working rk (ranked/leaderboard) index: writes to a declared rk field maintain a disk-backed counted B+Tree via the existing IndexUpdate lifecycle, and clients query TopN/BottomN/PercentileSample over the wire.

**Architecture:** rk rides the existing `ResidentIndex`/`IndexKind` trait rail (`seisin-node/src/index_handler.rs`) — `RkIndexKind::open` opens/creates a `seisin_storage::BPlusTree` file, `apply` does remove-then-insert, `query` answers from the live tree. Reads bypass op collation entirely via a new type-specific `Request::RkQuery` wire pair routed like `Request::Op` (redirect if not native). Spec: `docs/superpowers/specs/2026-07-23-rk-index-design.md`.

**Tech Stack:** Rust workspace, hand-rolled binary codecs (no serde on the wire), `anyhow::Result` everywhere, `tempfile` for test dirs.

## Global Constraints

- 2-space indent; `cargo fmt --check` and `cargo clippy --workspace --all-targets -- -D warnings` must stay clean after every task.
- `anyhow::Result` + `bail!()`/`.context()` for all error handling — no hand-rolled error enums. Trait methods that already use `Result<_, String>` (`IndexKind::open`, `ResidentIndex` — established in the trait refactor) keep `String`.
- No `unsafe`. No new dependencies beyond what workspace crates already use (`tempfile` is already a dev-dep of `seisin-storage`).
- Doc comments explain *why* (invariants, constraints), never restate code.
- Commit after every task (not every step) with a conventional-commits message; never push a task with failing tests.
- Every encoded `Request`/`Response` starts with `PROTOCOL_VERSION` (handled centrally in `encode_request`/`encode_response` — new variants get it for free; do not add version bytes inside variant bodies).
- Known-answer test vectors over fuzzing; tests that hit worker.rs's message loop get stress-run (see Task 9).

---

### Task 1: `ResidentIndex::query` read path on the trait

**Files:**
- Modify: `crates/seisin-node/src/index_handler.rs`

**Interfaces:**
- Consumes: existing `ResidentIndex`/`IndexKind` traits.
- Produces: `ResidentIndex::query(&self, query: &[u8]) -> Result<Vec<u8>, String>` with a default "unsupported" error impl. Task 6 overrides it for rk; Task 7's worker plumbing calls it.

- [ ] **Step 1: Write the failing test** — append to the `tests` module in `index_handler.rs`:

```rust
  #[test]
  fn query_default_impl_reports_the_kind_supports_no_queries() {
    let mut registry = IndexKindRegistry::new();
    registry.register("append", Box::new(AppendKind));
    let resident = registry
      .get("append")
      .unwrap()
      .open(DatumId::new(), None)
      .unwrap();
    let err = resident.query(b"anything").unwrap_err();
    assert!(err.contains("no queries"), "{err}");
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p seisin-node query_default_impl -- --nocapture`
Expected: FAIL — `no method named query found`.

- [ ] **Step 3: Add the default method** to the `ResidentIndex` trait:

```rust
pub trait ResidentIndex: Send {
  fn apply(&mut self, payload: &[u8]) -> IndexApplyOutcome;

  /// Answers a read-only query against the resident structure. `query`
  /// and the returned bytes are opaque to the framework — the kind's
  /// own codec (living outside `seisin-node`) defines both. Kinds that
  /// have no query surface (sk) keep this default.
  fn query(&self, query: &[u8]) -> Result<Vec<u8>, String> {
    let _ = query;
    Err("this index kind supports no queries".to_string())
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p seisin-node index_handler`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-node/src/index_handler.rs
git commit -m "feat: add default query method to ResidentIndex for index read paths"
```

---

### Task 2: `BPlusTree::remove`

**Files:**
- Modify: `crates/seisin-storage/src/btree.rs`

**Interfaces:**
- Consumes: existing `BPlusTree` internals (`read_leaf`/`read_internal`/`write_*`, `page_type`, `total_count`).
- Produces: `pub fn remove(&mut self, key: &[u8]) -> Result<bool>` — Task 6's rk apply calls it.

Implementation shape (two descents, both O(log n)): first a read-only
`contains` descent; only if the key exists, a second descent that
decrements each ancestor's subtree count on the way down and removes the
entry at the leaf. Never merges/rebalances an underfull page (accepted
limitation, documented in the spec) — an emptied leaf stays in place,
still sibling-linked, which every scan/rank path already tolerates
(iterating zero entries just moves to the next sibling; rank descent
never enters a zero-count subtree).

- [ ] **Step 1: Write the failing tests** — append to the `tests` module in `btree.rs`:

```rust
  #[test]
  fn remove_deletes_an_existing_key_and_returns_true() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    for i in 0..10u64 {
      tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }
    assert!(tree.remove(&5u64.to_be_bytes()).unwrap());
    assert_eq!(tree.len(), 9);
    let all = tree.all_entries_for_test().unwrap();
    assert!(!all.iter().any(|(k, _)| k == &5u64.to_be_bytes().to_vec()));
  }

  #[test]
  fn remove_on_a_missing_key_is_a_noop_returning_false() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    tree.insert(&1u64.to_be_bytes(), &1u64.to_be_bytes()).unwrap();
    assert!(!tree.remove(&2u64.to_be_bytes()).unwrap());
    assert_eq!(tree.len(), 1);
  }

  #[test]
  fn remove_rejects_a_wrong_sized_key() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    assert!(tree.remove(&[1u8; 4]).is_err());
  }

  #[test]
  fn remove_keeps_rank_descent_correct_across_a_multi_level_tree() {
    let tmp = NamedTempFile::new().unwrap();
    // key/value_size 1000 on 4096-byte pages: max_leaf_entries=2 — a
    // multi-level tree from a handful of inserts (same trick as the
    // deep-tree insert test above).
    let mut tree = BPlusTree::create(tmp.path(), 1000, 1000, 4096).unwrap();
    let make = |i: u64| {
      let mut k = vec![0u8; 1000];
      k[0..8].copy_from_slice(&i.to_be_bytes());
      k
    };
    for i in 0..50u64 {
      tree.insert(&make(i), &make(i)).unwrap();
    }
    // Remove every even key; odd keys remain at ranks 0..25.
    for i in (0..50u64).step_by(2) {
      assert!(tree.remove(&make(i)).unwrap());
    }
    assert_eq!(tree.len(), 25);
    let sample = tree.sample_by_rank(5).unwrap();
    // ranks 0,5,10,15,20 over remaining keys 1,3,5,... => key = 2*rank+1
    let expected: Vec<u64> = [0u64, 5, 10, 15, 20].iter().map(|r| 2 * r + 1).collect();
    for (entry, want) in sample.iter().zip(expected.iter()) {
      assert_eq!(entry.0, make(*want));
    }
    let bottom = tree.scan_forward_bounded(3).unwrap();
    assert_eq!(bottom[0].0, make(1));
    assert_eq!(bottom[2].0, make(5));
    let top = tree.scan_backward_bounded(1).unwrap();
    assert_eq!(top[0].0, make(49));
  }

  #[test]
  fn interleaved_inserts_and_removes_stay_consistent() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    // Deterministic pseudo-random walk (hand-rolled LCG, no rand dep):
    // mirror every operation into a BTreeMap and compare at the end.
    let mut model = std::collections::BTreeMap::new();
    let mut state = 0x2545F4914F6CDD1Du64;
    for _ in 0..2000 {
      state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
      let key = ((state >> 33) % 500).to_be_bytes();
      if state & 1 == 0 {
        tree.insert(&key, &key).unwrap();
        model.insert(key.to_vec(), key.to_vec());
      } else {
        let expected = model.remove(&key.to_vec()).is_some();
        assert_eq!(tree.remove(&key).unwrap(), expected);
      }
    }
    assert_eq!(tree.len(), model.len());
    let mut all = tree.all_entries_for_test().unwrap();
    all.sort();
    let expected: Vec<(Vec<u8>, Vec<u8>)> = model.into_iter().collect();
    assert_eq!(all, expected);
  }

  #[test]
  fn a_tree_with_removes_survives_reopening() {
    let tmp = NamedTempFile::new().unwrap();
    {
      let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
      for i in 0..300u64 {
        tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
      }
      for i in 100..200u64 {
        assert!(tree.remove(&i.to_be_bytes()).unwrap());
      }
    }
    let mut tree = BPlusTree::open(tmp.path()).unwrap();
    assert_eq!(tree.len(), 200);
    assert_eq!(
      tree.scan_backward_bounded(1).unwrap()[0].0,
      299u64.to_be_bytes().to_vec()
    );
  }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p seisin-storage remove`
Expected: FAIL — `no method named remove`.

- [ ] **Step 3: Implement `remove` and its private `contains` helper** — add a new `impl BPlusTree` block after the insert block:

```rust
impl BPlusTree {
  /// Removes `key` if present, returning whether it was found. Fixes
  /// every ancestor internal node's subtree count on the way down (so
  /// rank descent and sampling stay correct) via a presence check
  /// first, then a decrementing descent — counts are only touched when
  /// the key is known to exist. Does NOT merge or rebalance an
  /// underfull page afterward: pages can go sparse (even empty) over a
  /// delete-heavy history — never incorrect, just less space-efficient;
  /// an accepted, documented limitation revisited only if a real
  /// workload shows it matters.
  pub fn remove(&mut self, key: &[u8]) -> Result<bool> {
    if key.len() != self.key_size as usize {
      bail!(
        "key must be exactly {} bytes, got {}",
        self.key_size,
        key.len()
      );
    }
    if !self.contains(key)? {
      return Ok(false);
    }
    let mut page_id = self.root_page_id;
    loop {
      let bytes = self.store.read_page(page_id)?;
      match page_type(&bytes)? {
        PageType::Leaf => {
          let mut node = decode_leaf(&bytes, self.key_size, self.value_size)?;
          if let Ok(i) = node.entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
            node.entries.remove(i);
            self.write_leaf(page_id, &node)?;
          }
          break;
        }
        PageType::Internal => {
          let mut node = decode_internal(&bytes, self.key_size)?;
          let child_idx = node.entries.iter().position(|(k, _, _)| key < k.as_slice());
          let next = match child_idx {
            Some(i) => {
              node.entries[i].2 -= 1;
              node.entries[i].1
            }
            None => {
              node.rightmost_count -= 1;
              node.rightmost_child
            }
          };
          self.write_internal(page_id, &node)?;
          page_id = next;
        }
      }
    }
    self.total_count -= 1;
    self.write_superblock()?;
    Ok(true)
  }

  fn contains(&mut self, key: &[u8]) -> Result<bool> {
    let mut page_id = self.root_page_id;
    loop {
      let bytes = self.store.read_page(page_id)?;
      match page_type(&bytes)? {
        PageType::Leaf => {
          let node = decode_leaf(&bytes, self.key_size, self.value_size)?;
          return Ok(
            node
              .entries
              .binary_search_by(|(k, _)| k.as_slice().cmp(key))
              .is_ok(),
          );
        }
        PageType::Internal => {
          let node = decode_internal(&bytes, self.key_size)?;
          page_id = match node.entries.iter().find(|(k, _, _)| key < k.as_slice()) {
            Some((_, child, _)) => *child,
            None => node.rightmost_child,
          };
        }
      }
    }
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p seisin-storage`
Expected: all PASS (including all pre-existing tests and the page-size-agnostic integration tests).

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-storage/src/btree.rs
git commit -m "feat: add BPlusTree::remove with count-correct descent, no rebalancing"
```

---

### Task 3: `RkQuery`/`RkQueryResult` wire pair + codecs

**Files:**
- Modify: `crates/seisin-protocol/src/lib.rs`

**Interfaces:**
- Produces (used by Tasks 6, 7, 8):
  - `Request::RkQuery { index_datum_id: DatumId, query: RkQueryKind }`
  - `Response::RkQueryResult { entries: Vec<(Vec<u8>, DatumId)> }` (rank_key bytes always exactly 8, ordered as the query implies)
  - `pub enum RkQueryKind { TopN(u32), BottomN(u32), PercentileSample(u32) }`
  - `pub fn encode_rk_query_kind(q: &RkQueryKind) -> Vec<u8>` / `pub fn decode_rk_query_kind(buf: &[u8]) -> Result<RkQueryKind>`
  - `pub fn encode_rk_entries(entries: &[(Vec<u8>, DatumId)]) -> Vec<u8>` / `pub fn decode_rk_entries(buf: &[u8]) -> Result<Vec<(Vec<u8>, DatumId)>>`

The kind/entries codecs are public standalone functions (not just baked
into `encode_request`) because the worker treats query/result bytes as
opaque (`ResidentIndex::query`): `seisin-types`' rk impl decodes the
query bytes and encodes the result bytes using these same functions, so
the byte layout is defined exactly once, in the protocol crate.

- [ ] **Step 1: Write the failing tests** — append to the `tests` module:

```rust
  #[test]
  fn round_trips_an_rk_query_request() {
    for query in [
      RkQueryKind::TopN(10),
      RkQueryKind::BottomN(3),
      RkQueryKind::PercentileSample(7),
    ] {
      let req = Request::RkQuery {
        index_datum_id: DatumId::new(),
        query: query.clone(),
      };
      assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
    }
  }

  #[test]
  fn round_trips_an_rk_query_result_response() {
    let resp = Response::RkQueryResult {
      entries: vec![
        (vec![1u8; 8], DatumId::new()),
        (vec![2u8; 8], DatumId::new()),
      ],
    };
    assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
  }

  #[test]
  fn round_trips_an_empty_rk_query_result() {
    let resp = Response::RkQueryResult { entries: vec![] };
    assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
  }

  #[test]
  fn rk_entry_codec_rejects_a_rank_key_that_is_not_8_bytes() {
    assert!(decode_rk_entries(&encode_rk_entries(&[])).unwrap().is_empty());
    let bad = [(vec![1u8; 7], DatumId::new())];
    // encode asserts the invariant by construction: a 7-byte rank key
    // must not silently truncate/pad — panic in encode is unacceptable,
    // so encode_rk_entries takes only 8-byte keys by contract and
    // decode validates buffer arithmetic strictly.
    let mut buf = encode_rk_entries(&[(vec![1u8; 8], bad[0].1)]);
    buf.truncate(buf.len() - 1); // corrupt: short by one byte
    assert!(decode_rk_entries(&buf).is_err());
  }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p seisin-protocol rk`
Expected: FAIL — `RkQuery` variant / functions not found.

- [ ] **Step 3: Implement.** Add opcode constants after the existing ones:

```rust
const OP_RK_QUERY: u8 = 6;
const RESP_RK_QUERY_RESULT: u8 = 7;

const RK_QUERY_TOP_N: u8 = 0;
const RK_QUERY_BOTTOM_N: u8 = 1;
const RK_QUERY_PERCENTILE_SAMPLE: u8 = 2;
const RK_RANK_KEY_LEN: usize = 8;
```

Add the enum + variants (Request gets `RkQuery`, Response gets `RkQueryResult`, both with doc comments noting `RkQuery` is client-facing and routed like `Op`):

```rust
/// A read-only query against one rk index datum — client-facing, routed
/// exactly like `Request::Op` (redirect if the receiving node isn't
/// native for `index_datum_id`), but bypassing op collation entirely:
/// a query touches exactly one datum and is answered synchronously by
/// its owning thread. See the rk design doc's "Read Path" section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RkQueryKind {
  TopN(u32),
  BottomN(u32),
  PercentileSample(u32),
}
```

Codec functions (place near the other encode/decode helpers):

```rust
pub fn encode_rk_query_kind(q: &RkQueryKind) -> Vec<u8> {
  let (tag, n) = match q {
    RkQueryKind::TopN(n) => (RK_QUERY_TOP_N, *n),
    RkQueryKind::BottomN(n) => (RK_QUERY_BOTTOM_N, *n),
    RkQueryKind::PercentileSample(n) => (RK_QUERY_PERCENTILE_SAMPLE, *n),
  };
  let mut buf = vec![tag];
  buf.extend_from_slice(&n.to_le_bytes());
  buf
}

pub fn decode_rk_query_kind(buf: &[u8]) -> Result<RkQueryKind> {
  if buf.len() != 5 {
    bail!("rk query kind must be exactly 5 bytes, got {}", buf.len());
  }
  let n = u32::from_le_bytes(buf[1..5].try_into().unwrap());
  match buf[0] {
    RK_QUERY_TOP_N => Ok(RkQueryKind::TopN(n)),
    RK_QUERY_BOTTOM_N => Ok(RkQueryKind::BottomN(n)),
    RK_QUERY_PERCENTILE_SAMPLE => Ok(RkQueryKind::PercentileSample(n)),
    tag => bail!("unknown rk query kind tag: {tag}"),
  }
}

/// `entries` rank keys must be exactly 8 bytes each (the fixed
/// `encode_rank_key` width) — enforced by the producers (rk's resident
/// index), validated strictly by `decode_rk_entries`.
pub fn encode_rk_entries(entries: &[(Vec<u8>, DatumId)]) -> Vec<u8> {
  let mut buf = (entries.len() as u32).to_le_bytes().to_vec();
  for (rank_key, pk_id) in entries {
    buf.extend_from_slice(rank_key);
    buf.extend_from_slice(&pk_id.as_bytes());
  }
  buf
}

pub fn decode_rk_entries(buf: &[u8]) -> Result<Vec<(Vec<u8>, DatumId)>> {
  if buf.len() < 4 {
    bail!("rk entries buffer too short for a count");
  }
  let count = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
  let entry_len = RK_RANK_KEY_LEN + ID_LEN;
  if buf.len() != 4 + count * entry_len {
    bail!(
      "rk entries length mismatch: {} entries need {} bytes, got {}",
      count,
      4 + count * entry_len,
      buf.len()
    );
  }
  let mut entries = Vec::with_capacity(count);
  for i in 0..count {
    let start = 4 + i * entry_len;
    let rank_key = buf[start..start + RK_RANK_KEY_LEN].to_vec();
    let pk_id = DatumId::from_bytes(
      buf[start + RK_RANK_KEY_LEN..start + entry_len].try_into().unwrap(),
    );
    entries.push((rank_key, pk_id));
  }
  Ok(entries)
}
```

Wire the variants into `encode_request` (new match arm: push `OP_RK_QUERY`, then the 16-byte id, then the 5 query-kind bytes), `decode_request` (dispatch `OP_RK_QUERY` to a new `decode_rk_query_request` checking `buf.len() == 1 + ID_LEN + 5`), `encode_response` (push `RESP_RK_QUERY_RESULT` then `encode_rk_entries`), and `decode_response` (`RESP_RK_QUERY_RESULT` → `decode_rk_entries(&buf[1..])`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p seisin-protocol`
Expected: all PASS. Also `cargo build --workspace` — `pool.rs`'s `on_request` match in `seisin-node` is non-exhaustive now; add the arm there in this task:

```rust
        // Client-only, never carried over a peer-link — same as Op.
        seisin_protocol::Request::RkQuery { .. } => return,
```

and `server.rs`'s `let Request::Op ... else` still compiles (it's an `else` catch-all; the RkQuery server path is Task 8).

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-protocol/src/lib.rs crates/seisin-node/src/pool.rs
git commit -m "feat: add RkQuery/RkQueryResult wire pair with standalone rk codecs"
```

---

### Task 4: rank-key encoding + `RkIndexOp` payload codec (`seisin-types::rk_index`)

**Files:**
- Create: `crates/seisin-types/src/rk_index.rs`
- Modify: `crates/seisin-types/src/lib.rs` (add `pub mod rk_index;`)
- Modify: `crates/seisin-types/src/sk_index.rs` (make `derived_id_namespace` `pub(crate)`)
- Modify: `crates/seisin-types/Cargo.toml` (add `seisin-storage`, `seisin-protocol` as dependencies; `tempfile = "3"` as dev-dependency — copy the exact `path = "../seisin-storage"` style used by the existing entries)

**Interfaces:**
- Consumes: `FieldValue` (`crate::field`), `DatumId::from_name`, `sk_index::derived_id_namespace`.
- Produces (used by Tasks 5, 6):
  - `pub fn rk_key(type_name: &str, field_name: &str) -> DatumId`
  - `pub fn encode_rank_key(value: &FieldValue) -> Result<[u8; 8]>`
  - `pub struct RkIndexOp { pub pk_id: DatumId, pub old_rank_key: Option<[u8; 8]>, pub new_rank_key: Option<[u8; 8]> }`
  - `pub fn encode_rk_index_op(op: &RkIndexOp) -> Vec<u8>` / `pub fn decode_rk_index_op(buf: &[u8]) -> Result<RkIndexOp>`

- [ ] **Step 1: Create the module with failing tests.** Full initial file content:

```rust
//! rk (ranked/leaderboard) index maintenance: deriving the one stable
//! datum_id per declared rk index, order-preserving rank-key encoding,
//! and the update-payload codec. The resident-structure side
//! (`RkIndexKind`) is `rk_kind.rs`. See the rk design doc.

use anyhow::{bail, Result};
use seisin_core::datum::DatumId;

use crate::field::FieldValue;
use crate::sk_index::derived_id_namespace;

/// Derives the stable `DatumId` for `rk:{type_name}.{field_name}` — one
/// single id per declared rk index (no value-based partitioning,
/// unlike sk's per-distinct-value keys).
pub fn rk_key(type_name: &str, field_name: &str) -> DatumId {
  let name = format!("rk:{type_name}.{field_name}");
  DatumId::from_name(&derived_id_namespace(), name.as_bytes())
}

/// Encodes a numeric field value as 8 bytes whose raw byte-lexicographic
/// order (what the B+Tree compares) equals numeric total order.
pub fn encode_rank_key(value: &FieldValue) -> Result<[u8; 8]> {
  match value {
    FieldValue::I64(v) => {
      // Two's-complement order doesn't match unsigned byte order —
      // flipping the sign bit does: negatives (high bit set) become
      // the smaller unsigned range, positives the larger.
      Ok(((*v as u64) ^ 0x8000_0000_0000_0000).to_be_bytes())
    }
    FieldValue::F64(v) => {
      // Matches core::f64::total_cmp's own bit transform exactly, so
      // byte order equals total_cmp order (NaN included — orderable
      // rather than rejected).
      let bits = v.to_bits();
      let mask = ((bits as i64) >> 63) as u64 | 0x8000_0000_0000_0000;
      Ok((bits ^ mask).to_be_bytes())
    }
    other => bail!("rk rank_key must be I64 or F64, got {other:?}"),
  }
}

/// One update to an rk index: move `pk_id` from `old_rank_key` (absent
/// on first insert) to `new_rank_key` (absent on entity delete).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RkIndexOp {
  pub pk_id: DatumId,
  pub old_rank_key: Option<[u8; 8]>,
  pub new_rank_key: Option<[u8; 8]>,
}

fn push_optional_key(buf: &mut Vec<u8>, key: &Option<[u8; 8]>) {
  match key {
    None => buf.push(0),
    Some(k) => {
      buf.push(1);
      buf.extend_from_slice(k);
    }
  }
}

fn read_optional_key(buf: &[u8], offset: &mut usize) -> Result<Option<[u8; 8]>> {
  if buf.len() < *offset + 1 {
    bail!("rk index op truncated at an option flag");
  }
  let flag = buf[*offset];
  *offset += 1;
  match flag {
    0 => Ok(None),
    1 => {
      if buf.len() < *offset + 8 {
        bail!("rk index op truncated inside a rank key");
      }
      let key: [u8; 8] = buf[*offset..*offset + 8].try_into().unwrap();
      *offset += 8;
      Ok(Some(key))
    }
    f => bail!("unknown rk index op option flag: {f}"),
  }
}

pub fn encode_rk_index_op(op: &RkIndexOp) -> Vec<u8> {
  let mut buf = op.pk_id.as_bytes().to_vec();
  push_optional_key(&mut buf, &op.old_rank_key);
  push_optional_key(&mut buf, &op.new_rank_key);
  buf
}

pub fn decode_rk_index_op(buf: &[u8]) -> Result<RkIndexOp> {
  if buf.len() < 16 {
    bail!("rk index op payload too short for a pk_id");
  }
  let pk_id = DatumId::from_bytes(buf[0..16].try_into().unwrap());
  let mut offset = 16;
  let old_rank_key = read_optional_key(buf, &mut offset)?;
  let new_rank_key = read_optional_key(buf, &mut offset)?;
  if offset != buf.len() {
    bail!("rk index op has {} trailing bytes", buf.len() - offset);
  }
  Ok(RkIndexOp {
    pk_id,
    old_rank_key,
    new_rank_key,
  })
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn rk_key_is_stable_and_distinguishes_type_and_field() {
    assert_eq!(rk_key("user", "score"), rk_key("user", "score"));
    assert_ne!(rk_key("user", "score"), rk_key("user", "age"));
    assert_ne!(rk_key("user", "score"), rk_key("game", "score"));
  }

  #[test]
  fn i64_rank_keys_sort_byte_lexicographically_in_numeric_order() {
    let values = [i64::MIN, -300, -1, 0, 1, 256, 300, i64::MAX];
    let keys: Vec<[u8; 8]> = values
      .iter()
      .map(|v| encode_rank_key(&FieldValue::I64(*v)).unwrap())
      .collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
  }

  #[test]
  fn f64_rank_keys_sort_exactly_like_total_cmp() {
    let values = [
      f64::NEG_INFINITY,
      -1.5,
      -0.0,
      0.0,
      1.5,
      f64::INFINITY,
      f64::NAN,
      -f64::NAN,
    ];
    let mut by_bytes: Vec<f64> = values.to_vec();
    by_bytes.sort_by_key(|v| encode_rank_key(&FieldValue::F64(*v)).unwrap());
    let mut by_total_cmp: Vec<f64> = values.to_vec();
    by_total_cmp.sort_by(|a, b| a.total_cmp(b));
    let bits = |v: &f64| v.to_bits();
    assert_eq!(
      by_bytes.iter().map(bits).collect::<Vec<_>>(),
      by_total_cmp.iter().map(bits).collect::<Vec<_>>()
    );
  }

  #[test]
  fn non_numeric_values_are_rejected() {
    assert!(encode_rank_key(&FieldValue::String("x".to_string())).is_err());
    assert!(encode_rank_key(&FieldValue::Bool(true)).is_err());
  }

  #[test]
  fn rk_index_op_round_trips_all_option_combinations() {
    for (old, new) in [
      (None, Some([1u8; 8])),
      (Some([2u8; 8]), Some([3u8; 8])),
      (Some([4u8; 8]), None),
      (None, None),
    ] {
      let op = RkIndexOp {
        pk_id: DatumId::new(),
        old_rank_key: old,
        new_rank_key: new,
      };
      assert_eq!(decode_rk_index_op(&encode_rk_index_op(&op)).unwrap(), op);
    }
  }

  #[test]
  fn decode_rejects_truncated_or_padded_payloads() {
    let op = RkIndexOp {
      pk_id: DatumId::new(),
      old_rank_key: Some([1u8; 8]),
      new_rank_key: None,
    };
    let mut buf = encode_rk_index_op(&op);
    buf.push(0xFF);
    assert!(decode_rk_index_op(&buf).is_err());
    let buf = encode_rk_index_op(&op);
    assert!(decode_rk_index_op(&buf[..buf.len() - 1]).is_err());
  }
}
```

- [ ] **Step 2: Run tests to verify they fail to compile** (`derived_id_namespace` is private)

Run: `cargo test -p seisin-types rk_index`
Expected: compile error on `derived_id_namespace` visibility (plus missing `pub mod`).

- [ ] **Step 3: Wire it up** — in `sk_index.rs` change `fn derived_id_namespace()` to `pub(crate) fn derived_id_namespace()`; in `lib.rs` add `pub mod rk_index;`; add the Cargo.toml deps.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p seisin-types`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-types
git commit -m "feat: add rk rank-key encoding, derived rk key, and RkIndexOp codec"
```

---

### Task 5: `IndexDef::Rk` schema variant + automatic maintenance in `TypedOpContext`

**Files:**
- Modify: `crates/seisin-types/src/schema.rs`
- Modify: `crates/seisin-types/src/typed_context.rs`

**Interfaces:**
- Consumes: Task 4's `rk_key`, `encode_rank_key`, `RkIndexOp`, `encode_rk_index_op`.
- Produces: `IndexDef::Rk { field: String }`; `DatumTypeDef::index` panics at declaration time if an `Rk` field is undeclared or non-numeric (startup-time configuration bug, same class as `NodeConfig::self_address`'s documented panic — a solution declaring a bad schema must fail at process start, not corrupt an index later).

- [ ] **Step 1: Write the failing tests.** In `schema.rs` tests:

```rust
  #[test]
  fn an_rk_index_on_a_numeric_field_is_accepted() {
    let def = DatumTypeDef::new("player")
      .field("score", FieldType::I64)
      .index(IndexDef::Rk {
        field: "score".to_string(),
      });
    assert_eq!(def.indexes.len(), 1);
  }

  #[test]
  #[should_panic(expected = "rk index field")]
  fn an_rk_index_on_a_string_field_panics_at_declaration() {
    DatumTypeDef::new("player")
      .field("name", FieldType::String)
      .index(IndexDef::Rk {
        field: "name".to_string(),
      });
  }

  #[test]
  #[should_panic(expected = "rk index field")]
  fn an_rk_index_on_an_undeclared_field_panics_at_declaration() {
    DatumTypeDef::new("player").index(IndexDef::Rk {
      field: "score".to_string(),
    });
  }
```

In `typed_context.rs` tests:

```rust
  fn player_type() -> DatumTypeDef {
    DatumTypeDef::new("player")
      .field("score", FieldType::I64)
      .index(IndexDef::Rk {
        field: "score".to_string(),
      })
  }

  #[test]
  fn a_fresh_rk_write_schedules_one_insert_with_no_old_key() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let def = player_type();
    let pk_id = DatumId::new();
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.set(pk_id, &def, vec![FieldValue::I64(100)]).unwrap();
    }
    let updates = ctx.take_pending_index_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].index_kind, "rk");
    assert_eq!(updates[0].target, rk_key("player", "score"));
    let op = decode_rk_index_op(&updates[0].payload).unwrap();
    assert_eq!(op.pk_id, pk_id);
    assert_eq!(op.old_rank_key, None);
    assert_eq!(
      op.new_rank_key,
      Some(encode_rank_key(&FieldValue::I64(100)).unwrap())
    );
  }

  #[test]
  fn an_rk_score_change_schedules_one_update_carrying_old_and_new_keys() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let def = player_type();
    let pk_id = DatumId::new();
    {
      let mut ctx = OpContext::new(&mut cache);
      ctx.put(pk_id, crate::encode_datum(&def, &[FieldValue::I64(100)]).unwrap());
      for (id, content) in ctx.take_staged_writes() {
        if let Some(bytes) = content {
          cache.put(id, bytes);
        }
      }
    }
    let mut ctx = OpContext::new(&mut cache);
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.get(pk_id, &def).unwrap();
      tctx.set(pk_id, &def, vec![FieldValue::I64(250)]).unwrap();
    }
    let updates = ctx.take_pending_index_updates();
    assert_eq!(updates.len(), 1); // one target datum, unlike sk's two
    let op = decode_rk_index_op(&updates[0].payload).unwrap();
    assert_eq!(op.old_rank_key, Some(encode_rank_key(&FieldValue::I64(100)).unwrap()));
    assert_eq!(op.new_rank_key, Some(encode_rank_key(&FieldValue::I64(250)).unwrap()));
  }

  #[test]
  fn deleting_an_rk_indexed_datum_schedules_a_remove_only_update() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let def = player_type();
    let pk_id = DatumId::new();
    {
      let mut ctx = OpContext::new(&mut cache);
      ctx.put(pk_id, crate::encode_datum(&def, &[FieldValue::I64(100)]).unwrap());
      for (id, content) in ctx.take_staged_writes() {
        if let Some(bytes) = content {
          cache.put(id, bytes);
        }
      }
    }
    let mut ctx = OpContext::new(&mut cache);
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.delete(pk_id, &def).unwrap();
    }
    let updates = ctx.take_pending_index_updates();
    assert_eq!(updates.len(), 1);
    let op = decode_rk_index_op(&updates[0].payload).unwrap();
    assert!(op.old_rank_key.is_some());
    assert_eq!(op.new_rank_key, None);
  }

  #[test]
  fn an_unchanged_rk_score_schedules_nothing() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let def = player_type();
    let pk_id = DatumId::new();
    {
      let mut ctx = OpContext::new(&mut cache);
      ctx.put(pk_id, crate::encode_datum(&def, &[FieldValue::I64(100)]).unwrap());
      for (id, content) in ctx.take_staged_writes() {
        if let Some(bytes) = content {
          cache.put(id, bytes);
        }
      }
    }
    let mut ctx = OpContext::new(&mut cache);
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.get(pk_id, &def).unwrap();
      tctx.set(pk_id, &def, vec![FieldValue::I64(100)]).unwrap();
    }
    assert_eq!(ctx.take_pending_index_updates().len(), 0);
  }
```

Add to that file's test imports: `use crate::rk_index::{decode_rk_index_op, encode_rank_key, rk_key};`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p seisin-types`
Expected: FAIL — no `IndexDef::Rk` variant.

- [ ] **Step 3: Implement.** In `schema.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexDef {
  Sk {
    field: String,
    unique: Option<ConflictOp>,
  },
  /// One global ranked structure per `type.field` (leaderboards). The
  /// field must be declared, and numeric — enforced at declaration.
  Rk {
    field: String,
  },
}
```

and in `DatumTypeDef::index`, validate before pushing (a bad schema is a
startup-time configuration bug — panic, same documented policy as
`NodeConfig::self_address`):

```rust
  /// Declares an index on this type — see `IndexDef`.
  ///
  /// # Panics
  /// Panics if an `Rk` index names an undeclared or non-numeric field —
  /// a solution's schema declaration bug, caught at process start.
  pub fn index(mut self, index: IndexDef) -> Self {
    if let IndexDef::Rk { field } = &index {
      let declared = self.fields.iter().find(|(name, _)| name == field);
      match declared {
        Some((_, FieldType::I64)) | Some((_, FieldType::F64)) => {}
        other => panic!(
          "rk index field {:?} on type {:?} must be a declared I64 or F64 field, found {:?}",
          field, self.name, other
        ),
      }
    }
    self.indexes.push(index);
    self
  }
```

In `typed_context.rs`, the `Drop` impl's irrefutable `let IndexDef::Sk { field, unique } = index;` becomes a `match index`: the existing sk body moves into the `IndexDef::Sk { field, unique }` arm verbatim, and a new arm is added:

```rust
          IndexDef::Rk { field } => {
            let Some(field_idx) = tracked.def.fields.iter().position(|(name, _)| name == field)
            else {
              continue;
            };
            let old_value = tracked.before.as_ref().map(|v| v[field_idx].clone());
            let new_value = tracked.after.as_ref().map(|v| v[field_idx].clone());
            if old_value == new_value {
              continue;
            }
            // Declaration-time validation (schema.rs) guarantees the
            // field is numeric, so encode_rank_key cannot fail here.
            let old_rank_key = old_value.as_ref().and_then(|v| encode_rank_key(v).ok());
            let new_rank_key = new_value.as_ref().and_then(|v| encode_rank_key(v).ok());
            if old_rank_key.is_none() && new_rank_key.is_none() {
              continue;
            }
            let payload = encode_rk_index_op(&RkIndexOp {
              pk_id,
              old_rank_key,
              new_rank_key,
            });
            let target = rk_key(&tracked.def.name, field);
            self.ctx.schedule_index_update(target, "rk", payload);
          }
```

with `use crate::rk_index::{encode_rank_key, encode_rk_index_op, rk_key, RkIndexOp};` added to the imports.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p seisin-types && cargo build --workspace`
Expected: all PASS, workspace builds (nothing else matches on `IndexDef`).

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-types/src/schema.rs crates/seisin-types/src/typed_context.rs
git commit -m "feat: add IndexDef::Rk with declaration-time validation and automatic rk maintenance"
```

---

### Task 6: `RkIndexKind`/`RkResidentIndex`

**Files:**
- Create: `crates/seisin-types/src/rk_kind.rs`
- Modify: `crates/seisin-types/src/lib.rs` (add `pub mod rk_kind;`)

**Interfaces:**
- Consumes: Task 1's `ResidentIndex::query`, Task 2's `BPlusTree::remove`, Task 3's `decode_rk_query_kind`/`encode_rk_entries`, Task 4's `decode_rk_index_op`.
- Produces: `pub struct RkIndexKind { ... }` (constructed via `RkIndexKind::new(data_dir: PathBuf)`), `pub fn register_rk_index_kind(registry: &mut IndexKindRegistry, data_dir: PathBuf)` — Task 8's composition roots call the latter.

Key facts locked in here: B+Tree parameters are `key_size=24`
(8-byte rank_key ++ 16-byte pk_id), `value_size=16` (pk_id), `page_size=4096`.
The index file is named by the index datum's id
(`rk_<32-hex-chars>.btree`) — `open` only receives the `DatumId`, not the
type/field names, and the id is already the stable, collision-free
derivation of `rk:{type}.{field}`. (This supersedes the spec's
`rk_{type_name}.{field_name}.btree` naming — Task 9 updates the spec.)

- [ ] **Step 1: Create the module with failing tests.** Full initial file content:

```rust
//! The `"rk"` `IndexKind`: a disk-backed counted B+Tree per declared rk
//! index, resident as an open `seisin_storage::BPlusTree` handle on the
//! owning thread. Self-persisted — every apply outcome carries
//! `write_through: None`, because the tree's own page file is the
//! storage; there is no blob to hand back to the datum cache.

use std::path::PathBuf;

use seisin_core::datum::DatumId;
use seisin_node::index_handler::{IndexApplyOutcome, IndexKind, IndexKindRegistry, ResidentIndex};
use seisin_protocol::{decode_rk_query_kind, encode_rk_entries, RkQueryKind};
use seisin_storage::btree::BPlusTree;
use std::cell::RefCell;

use crate::rk_index::decode_rk_index_op;

/// key = rank_key (8) ++ pk_id (16): pk_id is a deterministic tiebreaker
/// so ties at the same score never collide/overwrite under upsert (real
/// data loss for a leaderboard, where ties are expected).
const RK_KEY_SIZE: u32 = 24;
const RK_VALUE_SIZE: u32 = 16;
const RK_PAGE_SIZE: u32 = 4096;

pub struct RkIndexKind {
  data_dir: PathBuf,
}

impl RkIndexKind {
  pub fn new(data_dir: PathBuf) -> Self {
    Self { data_dir }
  }
}

fn file_name_for(target: DatumId) -> String {
  let hex: String = target
    .as_bytes()
    .iter()
    .map(|b| format!("{b:02x}"))
    .collect();
  format!("rk_{hex}.btree")
}

fn composite_key(rank_key: &[u8; 8], pk_id: DatumId) -> [u8; 24] {
  let mut key = [0u8; 24];
  key[0..8].copy_from_slice(rank_key);
  key[8..24].copy_from_slice(&pk_id.as_bytes());
  key
}

pub struct RkResidentIndex {
  // RefCell because `ResidentIndex::query` takes `&self` (a read in
  // spirit) while `BPlusTree`'s page reads need `&mut self` (they seek
  // the underlying file). Single-threaded by construction: a resident
  // index lives on exactly one owning thread's map.
  tree: RefCell<BPlusTree>,
}

impl ResidentIndex for RkResidentIndex {
  fn apply(&mut self, payload: &[u8]) -> IndexApplyOutcome {
    let op = match decode_rk_index_op(payload) {
      Ok(op) => op,
      Err(e) => {
        return IndexApplyOutcome {
          violation: Some(format!("malformed rk index payload: {e}")),
          write_through: None,
        }
      }
    };
    let tree = self.tree.get_mut();
    if let Some(old) = &op.old_rank_key {
      if let Err(e) = tree.remove(&composite_key(old, op.pk_id)) {
        return IndexApplyOutcome {
          violation: Some(format!("rk remove failed: {e}")),
          write_through: None,
        };
      }
    }
    if let Some(new) = &op.new_rank_key {
      if let Err(e) = tree.insert(&composite_key(new, op.pk_id), &op.pk_id.as_bytes()) {
        return IndexApplyOutcome {
          violation: Some(format!("rk insert failed: {e}")),
          write_through: None,
        };
      }
    }
    IndexApplyOutcome {
      violation: None,
      write_through: None,
    }
  }

  fn query(&self, query: &[u8]) -> Result<Vec<u8>, String> {
    let kind = decode_rk_query_kind(query).map_err(|e| e.to_string())?;
    let mut tree = self.tree.borrow_mut();
    let raw = match kind {
      RkQueryKind::TopN(n) => tree.scan_backward_bounded(n as usize),
      RkQueryKind::BottomN(n) => tree.scan_forward_bounded(n as usize),
      RkQueryKind::PercentileSample(k) => tree.sample_by_rank(k as usize),
    }
    .map_err(|e| e.to_string())?;
    let entries: Vec<(Vec<u8>, DatumId)> = raw
      .into_iter()
      .map(|(key, value)| {
        let pk_id = DatumId::from_bytes(value.try_into().expect("rk values are 16 bytes"));
        (key[0..8].to_vec(), pk_id)
      })
      .collect();
    Ok(encode_rk_entries(&entries))
  }
}

impl IndexKind for RkIndexKind {
  /// `stored` is ignored: rk persists in its own page file, not as
  /// datum-cache bytes.
  fn open(
    &self,
    target: DatumId,
    _stored: Option<Vec<u8>>,
  ) -> Result<Box<dyn ResidentIndex>, String> {
    let path = self.data_dir.join(file_name_for(target));
    let tree = if path.exists() {
      BPlusTree::open(&path)
    } else {
      std::fs::create_dir_all(&self.data_dir)
        .map_err(|e| format!("failed to create rk data dir {:?}: {e}", self.data_dir))?;
      BPlusTree::create(&path, RK_KEY_SIZE, RK_VALUE_SIZE, RK_PAGE_SIZE)
    }
    .map_err(|e| format!("failed to open rk index file {path:?}: {e}"))?;
    Ok(Box::new(RkResidentIndex {
      tree: RefCell::new(tree),
    }))
  }
}

/// Registers the `"rk"` index kind — call once at startup with the
/// directory this node's rk index files live in.
pub fn register_rk_index_kind(registry: &mut IndexKindRegistry, data_dir: PathBuf) {
  registry.register("rk", Box::new(RkIndexKind::new(data_dir)));
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::field::FieldValue;
  use crate::rk_index::{encode_rank_key, encode_rk_index_op, RkIndexOp};
  use seisin_protocol::{decode_rk_entries, encode_rk_query_kind};

  fn open_rk(dir: &std::path::Path) -> (DatumId, Box<dyn ResidentIndex>) {
    let target = DatumId::new();
    let kind = RkIndexKind::new(dir.to_path_buf());
    (target, kind.open(target, None).unwrap())
  }

  fn insert_score(resident: &mut Box<dyn ResidentIndex>, pk_id: DatumId, score: i64) {
    let payload = encode_rk_index_op(&RkIndexOp {
      pk_id,
      old_rank_key: None,
      new_rank_key: Some(encode_rank_key(&FieldValue::I64(score)).unwrap()),
    });
    let outcome = resident.apply(&payload);
    assert!(outcome.violation.is_none());
    assert!(outcome.write_through.is_none()); // self-persisted
  }

  fn top_n(resident: &Box<dyn ResidentIndex>, n: u32) -> Vec<DatumId> {
    let bytes = resident
      .query(&encode_rk_query_kind(&RkQueryKind::TopN(n)))
      .unwrap();
    decode_rk_entries(&bytes).unwrap().into_iter().map(|(_, id)| id).collect()
  }

  #[test]
  fn inserts_then_top_n_returns_highest_scores_first() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut resident) = open_rk(dir.path());
    let (a, b, c) = (DatumId::new(), DatumId::new(), DatumId::new());
    insert_score(&mut resident, a, 100);
    insert_score(&mut resident, b, 300);
    insert_score(&mut resident, c, 200);
    assert_eq!(top_n(&resident, 2), vec![b, c]);
  }

  #[test]
  fn a_score_change_moves_the_entry_not_duplicates_it() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut resident) = open_rk(dir.path());
    let (a, b) = (DatumId::new(), DatumId::new());
    insert_score(&mut resident, a, 100);
    insert_score(&mut resident, b, 200);
    let payload = encode_rk_index_op(&RkIndexOp {
      pk_id: a,
      old_rank_key: Some(encode_rank_key(&FieldValue::I64(100)).unwrap()),
      new_rank_key: Some(encode_rank_key(&FieldValue::I64(300)).unwrap()),
    });
    assert!(resident.apply(&payload).violation.is_none());
    assert_eq!(top_n(&resident, 10), vec![a, b]); // exactly two entries
  }

  #[test]
  fn a_delete_removes_the_entry() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut resident) = open_rk(dir.path());
    let a = DatumId::new();
    insert_score(&mut resident, a, 100);
    let payload = encode_rk_index_op(&RkIndexOp {
      pk_id: a,
      old_rank_key: Some(encode_rank_key(&FieldValue::I64(100)).unwrap()),
      new_rank_key: None,
    });
    assert!(resident.apply(&payload).violation.is_none());
    assert!(top_n(&resident, 10).is_empty());
  }

  #[test]
  fn tied_scores_keep_both_entries() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut resident) = open_rk(dir.path());
    let (a, b) = (DatumId::new(), DatumId::new());
    insert_score(&mut resident, a, 100);
    insert_score(&mut resident, b, 100);
    assert_eq!(top_n(&resident, 10).len(), 2);
  }

  #[test]
  fn bottom_n_and_percentile_sample_answer_from_the_same_tree() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut resident) = open_rk(dir.path());
    for score in [10i64, 20, 30, 40, 50] {
      insert_score(&mut resident, DatumId::new(), score);
    }
    let bottom = resident
      .query(&encode_rk_query_kind(&RkQueryKind::BottomN(2)))
      .unwrap();
    assert_eq!(decode_rk_entries(&bottom).unwrap().len(), 2);
    let sample = resident
      .query(&encode_rk_query_kind(&RkQueryKind::PercentileSample(3)))
      .unwrap();
    assert_eq!(decode_rk_entries(&sample).unwrap().len(), 3);
  }

  #[test]
  fn reopening_the_same_target_reuses_the_existing_file() {
    let dir = tempfile::tempdir().unwrap();
    let target = DatumId::new();
    let kind = RkIndexKind::new(dir.path().to_path_buf());
    let a = DatumId::new();
    {
      let mut resident = kind.open(target, None).unwrap();
      insert_score(&mut resident, a, 100);
    }
    let resident = kind.open(target, None).unwrap();
    assert_eq!(top_n(&resident, 10), vec![a]);
  }

  #[test]
  fn a_malformed_payload_is_a_violation_not_a_panic() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut resident) = open_rk(dir.path());
    let outcome = resident.apply(&[0xFF, 0xFF]);
    assert!(outcome.violation.is_some());
  }
}
```

- [ ] **Step 2: Run tests to verify they fail** (module not declared yet)

Run: `cargo test -p seisin-types rk_kind`
Expected: FAIL to compile until `pub mod rk_kind;` is added.

- [ ] **Step 3: Declare the module** in `lib.rs`, fix any signature drift the compiler reports (e.g. `seisin_storage`'s module path — the crate root re-exports; check `crates/seisin-storage/src/lib.rs` and import `BPlusTree` from wherever it's actually exported).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p seisin-types && cargo clippy -p seisin-types --all-targets -- -D warnings`
Expected: all PASS. (Clippy will flag `&Box<dyn ResidentIndex>` in the test helper — take `&dyn ResidentIndex` instead if it does.)

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-types
git commit -m "feat: add RkIndexKind/RkResidentIndex over the counted B+Tree"
```

---

### Task 7: worker/pool index-query plumbing

**Files:**
- Modify: `crates/seisin-node/src/worker.rs`
- Modify: `crates/seisin-node/src/pool.rs`

**Interfaces:**
- Consumes: Task 1's `ResidentIndex::query`.
- Produces (used by Task 8):
  - `WorkerHandle::run_index_query(&self, target: DatumId, index_kind: String, query: Vec<u8>) -> Result<Vec<u8>, String>`
  - `WorkerPool::run_index_query(&self, target: DatumId, index_kind: String, query: Vec<u8>) -> Result<Vec<u8>, String>` (picks the thread via `ring.native(target)`)

- [ ] **Step 1: Write the failing test** — in `worker.rs`'s tests module (uses the existing `FixedOutcomeKind` scaffolding; extend `FixedOutcomeResident` with a `query` override first so there's something to observe):

Add to `FixedOutcomeResident`'s `impl crate::index_handler::ResidentIndex`:

```rust
    fn query(&self, query: &[u8]) -> Result<Vec<u8>, String> {
      // Echo, uppercased — enough to prove the bytes round-tripped
      // through the worker rather than being fabricated.
      Ok(query.iter().map(u8::to_ascii_uppercase).collect())
    }
```

New tests:

```rust
  #[test]
  fn run_index_query_answers_from_the_resident_index() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let mut index_kinds = crate::index_handler::IndexKindRegistry::new();
    index_kinds.register("echo", Box::new(FixedOutcomeKind { violation: None }));
    let handles = spawn_test_pool_with_index_kinds(1, ring, OpRegistry::new(), index_kinds);
    let result = handles[0].run_index_query(DatumId::new(), "echo".to_string(), b"abc".to_vec());
    assert_eq!(result, Ok(b"ABC".to_vec()));
  }

  #[test]
  fn run_index_query_on_an_unregistered_kind_is_an_error() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let handles = spawn_test_pool(1, ring, OpRegistry::new());
    assert!(handles[0]
      .run_index_query(DatumId::new(), "nope".to_string(), vec![])
      .is_err());
  }

  #[test]
  fn an_index_updated_then_queried_on_the_same_thread_uses_one_resident_instance() {
    // FixedOutcomeResident::apply writes payload through; if query
    // reached a *different* (fresh) instance the write-through wouldn't
    // matter — this just proves both message paths resolve to the same
    // resident map entry without error.
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let index_target = DatumId::new();
    let mut ops = OpRegistry::new();
    ops.register(
      "touch_with_index",
      Box::new(move |ctx: &mut OpContext, ids, _payload| {
        ctx.put(ids[0], b"touched".to_vec());
        ctx.schedule_index_update(index_target, "echo", vec![1]);
        vec![]
      }),
    );
    let mut index_kinds = crate::index_handler::IndexKindRegistry::new();
    index_kinds.register("echo", Box::new(FixedOutcomeKind { violation: None }));
    let handles = spawn_test_pool_with_index_kinds(1, ring, ops, index_kinds);
    handles[0]
      .run_op(DatumId::new(), "touch_with_index".to_string(), vec![DatumId::new()], vec![])
      .unwrap();
    let result = handles[0].run_index_query(index_target, "echo".to_string(), b"q".to_vec());
    assert_eq!(result, Ok(b"Q".to_vec()));
  }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p seisin-node run_index_query`
Expected: FAIL — no `run_index_query` method.

- [ ] **Step 3: Implement.** In `worker.rs`:

1. New `WorkerMessage` variant (after `IndexUpdateReplied`):

```rust
  /// A read-only query against the index datum `target`, answered
  /// synchronously by its owning thread from the resident structure
  /// (cold-opening it if needed) — no collation, no op record. Query
  /// and result bytes are opaque to the framework.
  IndexQuery {
    target: DatumId,
    index_kind: String,
    query: Vec<u8>,
    reply: Sender<Result<Vec<u8>, String>>,
  },
```

2. Extract the cold-open logic currently inline in the `IndexUpdate` arm into a local closure or reuse-by-shape, then add the handler arm. To keep the borrow checker simple, duplicate the small entry lookup (it's 12 lines; a shared helper needs five borrows threaded through — not worth it):

```rust
          WorkerMessage::IndexQuery {
            target,
            index_kind,
            query,
            reply,
          } => {
            let resident = match resident_indexes.entry(target) {
              std::collections::hash_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
              std::collections::hash_map::Entry::Vacant(vacancy) => index_kinds
                .get(&index_kind)
                .and_then(|kind| kind.open(target, cache.get(target)))
                .map(|resident| vacancy.insert(resident)),
            };
            let result = match resident {
              Ok(resident) => resident.query(&query),
              Err(message) => Err(message),
            };
            let _ = reply.send(result);
          }
```

3. New `WorkerHandle` method (next to `run_op`):

```rust
  /// Sends an index query to this thread and blocks for the answer —
  /// same synchronous shape as `run_op`, minus collation/op records.
  pub fn run_index_query(
    &self,
    target: DatumId,
    index_kind: String,
    query: Vec<u8>,
  ) -> Result<Vec<u8>, String> {
    let (reply_tx, reply_rx) = std::sync::mpsc::channel();
    self
      .sender
      .send(WorkerMessage::IndexQuery {
        target,
        index_kind,
        query,
        reply: reply_tx,
      })
      .expect("worker thread exited unexpectedly");
    reply_rx.recv().expect("worker dropped the reply channel")
  }
```

In `pool.rs`, next to `run_op`:

```rust
  /// Routes an index query to whichever local thread natively owns
  /// `target`. Callers must have already confirmed `target` resolves to
  /// this node — see `server.rs`'s redirect check.
  pub fn run_index_query(
    &self,
    target: DatumId,
    index_kind: String,
    query: Vec<u8>,
  ) -> Result<Vec<u8>, String> {
    let (_, thread_id) = self.ring.read().unwrap().native(target);
    self.handles[thread_id.0 as usize].run_index_query(target, index_kind, query)
  }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p seisin-node`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-node/src/worker.rs crates/seisin-node/src/pool.rs
git commit -m "feat: add synchronous index-query path through worker and pool"
```

---

### Task 8: server routing, `data_dir` config, composition-root registration

**Files:**
- Modify: `crates/seisin-node/src/server.rs`
- Modify: `crates/seisin-node/src/config.rs`
- Modify: `crates/seisin-node/src/main.rs`

**Interfaces:**
- Consumes: Task 3's wire pair + codecs, Task 7's `WorkerPool::run_index_query`, Task 6's `register_rk_index_kind` (main.rs only — note `seisin-node` cannot call it; registration happens in `main.rs`? No: `main.rs` is `seisin-node`'s own binary and cannot depend on `seisin-types` either (`seisin-types` depends on `seisin-node`; the reverse would be a cycle). The binary keeps an empty registry with a `// rk/sk kinds are registered by a solution binary — see the framework/codegen shape note in PROGRESS.md` comment; real registration is exercised by Task 9's integration test, which lives in `seisin-types` where both crates are visible. `data_dir` still lands in `NodeConfig` now so a solution binary has somewhere to read it from.)
- Produces: `Request::RkQuery` served end-to-end on a node whose registry has `"rk"` registered; `NodeConfig.data_dir: String`.

- [ ] **Step 1: Write the failing config test** — in `config.rs` tests, add `data_dir: "/tmp/seisin-data"` to `SAMPLE` (RON tuple-struct field, same syntax as the others, at the top level next to `self_node_id`) and:

```rust
  #[test]
  fn parses_the_data_dir() {
    let config = NodeConfig::parse(SAMPLE).unwrap();
    assert_eq!(config.data_dir, "/tmp/seisin-data");
  }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p seisin-node config`
Expected: FAIL — no `data_dir` field (and SAMPLE parse now fails with an unknown field until the struct gains it).

- [ ] **Step 3: Implement.**

`config.rs`:

```rust
#[derive(Debug, Deserialize)]
pub struct NodeConfig {
  pub self_node_id: u64,
  pub members: Vec<MemberConfig>,
  /// Where this node's own index/data files live (rk B+Tree files
  /// today). Node-local only — not Storage Tier placement.
  pub data_dir: String,
}
```

`server.rs` — restructure `handle_connection`'s single `let ... else` into a match, keeping `Op` behavior identical:

```rust
    let response = match request {
      Request::Op {
        op_id,
        op_name,
        datum_ids,
        payload,
      } => handle_op_request(
        self_node_id,
        &ring,
        &address_book,
        &pool,
        op_id,
        op_name,
        datum_ids,
        payload,
      ),
      Request::RkQuery {
        index_datum_id,
        query,
      } => handle_rk_query(self_node_id, &ring, &address_book, &pool, index_datum_id, query),
      // Acquire/Recall/Release/IndexUpdate are node-to-node only,
      // carried over a peer-link connection (see peer_link.rs) — a
      // client should never send one on this client-facing connection.
      _ => return,
    };
```

and the new handler:

```rust
/// Routes a client rk query: redirect if `index_datum_id` isn't native
/// here (same check as `handle_op_request`), else answer synchronously
/// from the owning thread's resident tree. The query kind is re-encoded
/// to the protocol's standalone codec bytes because the worker treats
/// query/result bytes as opaque (`ResidentIndex::query`) — the byte
/// layout is defined once, in seisin-protocol, shared with the rk
/// impl's decoder in seisin-types.
fn handle_rk_query(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  pool: &WorkerPool,
  index_datum_id: DatumId,
  query: seisin_protocol::RkQueryKind,
) -> Response {
  let native_node = ring.read().unwrap().native(index_datum_id).0;
  if native_node != self_node_id {
    return match address_book.get(&native_node) {
      Some(address) => Response::Redirect {
        address: address.clone(),
      },
      None => Response::OpError {
        message: format!("no known address for node {native_node:?}"),
      },
    };
  }
  let query_bytes = seisin_protocol::encode_rk_query_kind(&query);
  match pool.run_index_query(index_datum_id, "rk".to_string(), query_bytes) {
    Ok(result_bytes) => match seisin_protocol::decode_rk_entries(&result_bytes) {
      Ok(entries) => Response::RkQueryResult { entries },
      Err(e) => Response::OpError {
        message: format!("malformed rk query result: {e}"),
      },
    },
    Err(message) => Response::OpError { message },
  }
}
```

`main.rs` — the registry stays empty (a framework binary has no solution types); update the existing comment to also cover index kinds:

```rust
  // No solution has been wired up yet — empty op and index-kind
  // registries until a real solution built on this framework registers
  // its ops and index kinds (e.g. seisin_types::rk_kind::
  // register_rk_index_kind with config.data_dir) in its own binary.
```

(`config.data_dir` is intentionally unused by this bare binary — read it into a variable prefixed `_data_dir` only if clippy complains; otherwise leave the field for solution binaries.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: all PASS, clippy clean.

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-node/src/server.rs crates/seisin-node/src/config.rs crates/seisin-node/src/main.rs
git commit -m "feat: route client RkQuery requests and add data_dir to node config"
```

---

### Task 9: end-to-end integration test, stress runs, docs

**Files:**
- Create: `crates/seisin-types/tests/integration_rk_leaderboard.rs`
- Modify: `docs/superpowers/specs/2026-07-23-rk-index-design.md` (file-naming line)
- Modify: `docs/superpowers/PROGRESS.md`

**Interfaces:**
- Consumes: everything above, plus the existing `integration_automatic_index_maintenance.rs` node-bootstrap pattern (copy its `start_node` shape).

- [ ] **Step 1: Write the integration test** (this is the real deliverable gate — through the wire protocol, no shortcuts):

```rust
use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::InMemoryStore;
use seisin_node::index_handler::IndexKindRegistry;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ops::context::OpContext;
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{Request, Response, RkQueryKind};
use seisin_ring::ring::Ring;
use seisin_types::field::{FieldType, FieldValue};
use seisin_types::rk_index::rk_key;
use seisin_types::rk_kind::register_rk_index_kind;
use seisin_types::schema::{DatumTypeDef, IndexDef};
use seisin_types::typed_context::TypedOpContext;
use seisin_types::{decode_datum, encode_datum};

fn player_type() -> DatumTypeDef {
  DatumTypeDef::new("player")
    .field("score", FieldType::I64)
    .index(IndexDef::Rk {
      field: "score".to_string(),
    })
}

fn start_node(data_dir: std::path::PathBuf) -> String {
  let def = player_type();
  let mut ops = OpRegistry::new();
  let write_def = def.clone();
  ops.register(
    "write_player",
    Box::new(move |ctx: &mut OpContext, ids, payload| {
      let values = decode_datum(&write_def, payload).unwrap();
      let mut tctx = TypedOpContext::new(ctx);
      tctx.get(ids[0], &write_def).unwrap();
      tctx.set(ids[0], &write_def, values).unwrap();
      vec![]
    }),
  );

  let mut index_kinds = IndexKindRegistry::new();
  register_rk_index_kind(&mut index_kinds, data_dir);

  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  let node_id = NodeId(1);
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_id, 2)])));
  let peer_link_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(ops),
    Arc::clone(&ring),
    node_id,
    peer_link_listener,
    Arc::new(std::collections::HashMap::new()),
    Arc::new(index_kinds),
  ));
  let address_book = Arc::new(std::collections::HashMap::new());
  thread::spawn(move || serve(listener, node_id, ring, address_book, pool));
  thread::sleep(std::time::Duration::from_millis(100));
  addr
}

fn write_score(addr: &str, def: &DatumTypeDef, pk: DatumId, score: i64) {
  let response = seisin_client::call(
    addr,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "write_player".to_string(),
      datum_ids: vec![pk],
      payload: encode_datum(def, &[FieldValue::I64(score)]).unwrap(),
    },
  )
  .unwrap();
  assert_eq!(response, Response::OpResult { payload: vec![] });
}

fn rk_query(addr: &str, query: RkQueryKind) -> Vec<(Vec<u8>, DatumId)> {
  let response = seisin_client::call(
    addr,
    Request::RkQuery {
      index_datum_id: rk_key("player", "score"),
      query,
    },
  )
  .unwrap();
  match response {
    Response::RkQueryResult { entries } => entries,
    other => panic!("expected RkQueryResult, got {other:?}"),
  }
}

#[test]
fn writes_maintain_the_leaderboard_and_queries_answer_over_the_wire() {
  let data_dir = tempfile::tempdir().unwrap();
  let addr = start_node(data_dir.path().to_path_buf());
  let def = player_type();

  let players: Vec<DatumId> = (0..5).map(|_| DatumId::new()).collect();
  for (i, pk) in players.iter().enumerate() {
    write_score(&addr, &def, *pk, (i as i64 + 1) * 100); // 100..500
  }

  let top = rk_query(&addr, RkQueryKind::TopN(2));
  assert_eq!(
    top.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
    vec![players[4], players[3]] // 500, then 400
  );

  let bottom = rk_query(&addr, RkQueryKind::BottomN(2));
  assert_eq!(
    bottom.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
    vec![players[0], players[1]] // 100, then 200
  );

  // A score update moves the entry: player 0 jumps to the top.
  write_score(&addr, &def, players[0], 900);
  let top = rk_query(&addr, RkQueryKind::TopN(1));
  assert_eq!(top[0].1, players[0]);

  // Still exactly 5 entries (moved, not duplicated).
  let all = rk_query(&addr, RkQueryKind::TopN(100));
  assert_eq!(all.len(), 5);

  let sample = rk_query(&addr, RkQueryKind::PercentileSample(3));
  assert_eq!(sample.len(), 3);
}
```

Also add `tempfile` to `crates/seisin-types/Cargo.toml` `[dev-dependencies]` if Task 4 didn't already.

- [ ] **Step 2: Run the integration test**

Run: `cargo test -p seisin-types --test integration_rk_leaderboard`
Expected: PASS. If it hangs or fails, debug via superpowers:systematic-debugging before touching anything.

- [ ] **Step 3: Stress runs** (this touches worker.rs's core loop):

```bash
for i in $(seq 1 10); do cargo test -q -p seisin-types --test integration_rk_leaderboard || break; done
for i in $(seq 1 20); do cargo test -q -p seisin-node --test integration_wound_wait --test integration_cross_node_wound_wait --test integration_op_collation || break; done
```

Expected: zero failures across all iterations.

- [ ] **Step 4: Full gates**

Run: `cargo test --workspace && cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings`
Expected: all clean.

- [ ] **Step 5: Update docs.**
  - In `docs/superpowers/specs/2026-07-23-rk-index-design.md`, replace the file-naming sentence (`rk index files live at {data_dir}/rk_{type_name}.{field_name}.btree...`) with: files are named `rk_<index-datum-id-hex>.btree` — `IndexKind::open` receives only the `DatumId`, which is already the stable derivation of `rk:{type}.{field}`, so the id is the natural, collision-free file name.
  - In `docs/superpowers/PROGRESS.md`, add a "Datum Type System, Part 3 — rk index" entry under Done following the established format (what shipped, any bugs found during execution, the new test count), and update the "Parts ... are next" trailer text.

- [ ] **Step 6: Commit and push**

```bash
git add -A
git commit -m "feat: rk leaderboard index end-to-end (Datum Type System Part 3)"
git push
```

---

## Deliberately Out of Scope (from the spec — do not build)

- Conditional/ratchet updates ("best score wins"), rich update results (rank-after-update in the write response).
- rk index sharding; node-function/placement wiring; page-size auto-detection/benchmark tooling.
- Any change to collation destination-thread preference.
