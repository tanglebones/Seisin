# tk (Bitemporal Valid-Time) Datum Class Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline execution — this project never uses subagents) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The tk decomposed-field-storage datum class: per-(class, entity) valid-time histories — optionally per sub-part via a declared sub-key prefix — with correction-upsert writes and AsOf/Current/History/Range/SnapshotAt queries over the wire.

**Architecture:** tk rides the lb-built `execute`/`query` rail (spec: `docs/superpowers/specs/2026-07-24-tk-datum-class-design.md`). Each (class, entity) is one counted B+Tree file keyed `sub_key ++ ts_key(lower)`; values are fixed records `upper_flag ++ upper ++ len ++ value`. Residency is just the file handle — page-lazy loading is the thunked-range model. One engine addition (`rank_of_floor`); wire pair + codecs follow the lb precedent exactly.

**Tech Stack:** Rust workspace, hand-rolled codecs, `anyhow::Result`, `tempfile`.

## Global Constraints

- 2-space indent; `cargo fmt --check` and `cargo clippy --workspace --all-targets -- -D warnings` clean after every task; commit per task with the `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` trailer.
- tk is primary data: `value_width` overflow and wrong-typed values are **rejected with errors, never truncated**; `apply` is rejected (not on the framework-diff rail).
- Timestamps are i64 epoch millis, encoded order-preserving via the I64 sign-flip big-endian transform. `as_of: Option<i64>` — `None` is stamped server-side via the injected `WallClock`.
- Sub-key isolation is an invariant: floors, gap-fills, and scans never cross a sub-key prefix boundary.
- New wire variants inherit `PROTOCOL_VERSION` from `encode_request`/`encode_response` automatically.

---

### Task 1: `BPlusTree::rank_of_floor`

**Files:**
- Modify: `crates/seisin-storage/src/btree.rs`

**Interfaces:**
- Produces: `pub fn rank_of_floor(&mut self, key: &[u8]) -> Result<Option<u64>>` — 0-based ascending rank of the greatest key `<=` the probe; `None` if every key is greater (or the tree is empty).

- [ ] **Step 1: Failing tests** — append to `btree.rs` tests:

```rust
  #[test]
  fn rank_of_floor_finds_exact_keys_predecessors_and_none_before_first() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    for i in (0..300u64).step_by(2) {
      tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }
    // exact hit: key 100 is the 51st even number (rank 50)
    assert_eq!(tree.rank_of_floor(&100u64.to_be_bytes()).unwrap(), Some(50));
    // between keys: floor(101) = 100
    assert_eq!(tree.rank_of_floor(&101u64.to_be_bytes()).unwrap(), Some(50));
    // after last: floor = last entry (298, rank 149)
    assert_eq!(tree.rank_of_floor(&999u64.to_be_bytes()).unwrap(), Some(149));
    // before first: none
    assert_eq!(tree.rank_of_floor(&(0u64).to_be_bytes()).unwrap(), Some(0));
    let empty_probe = [0u8; 8];
    let mut probe = empty_probe;
    probe[7] = 0; // key 0 exists; probe below it is impossible for u64 —
                  // use a tree without key 0 instead:
    let tmp2 = NamedTempFile::new().unwrap();
    let mut tree2 = BPlusTree::create(tmp2.path(), 8, 8, 4096).unwrap();
    tree2.insert(&10u64.to_be_bytes(), &[0; 8]).unwrap();
    assert_eq!(tree2.rank_of_floor(&5u64.to_be_bytes()).unwrap(), None);
    let _ = probe;
  }

  #[test]
  fn rank_of_floor_stays_correct_across_levels_and_after_removes() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 1000, 1000, 4096).unwrap();
    let make = |i: u64| {
      let mut k = vec![0u8; 1000];
      k[0..8].copy_from_slice(&i.to_be_bytes());
      k
    };
    for i in (0..120u64).step_by(2) {
      tree.insert(&make(i), &make(i)).unwrap();
    }
    for i in (0..120u64).step_by(4) {
      assert!(tree.remove(&make(i)).unwrap()); // keep i % 4 == 2
    }
    // Remaining: 2, 6, 10, ... floor(11) = 10 at rank 2.
    assert_eq!(tree.rank_of_floor(&make(11)).unwrap(), Some(2));
    assert_eq!(tree.rank_of_floor(&make(1)).unwrap(), None);
  }

  #[test]
  fn rank_of_floor_on_an_empty_tree_is_none_and_wrong_size_errors() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    assert_eq!(tree.rank_of_floor(&1u64.to_be_bytes()).unwrap(), None);
    assert!(tree.rank_of_floor(&[1u8; 4]).is_err());
  }
```

- [ ] **Step 2: Run to verify fail** — `cargo test -p seisin-storage rank_of_floor` → method not found.

- [ ] **Step 3: Implement** — in the same `impl` block as `rank_of_key`:

```rust
  /// 0-based ascending rank of the greatest key `<=` the probe (`None`
  /// if every key is greater, or the tree is empty). Same counted
  /// descent as `rank_of_key`; at the leaf, a missed binary search
  /// steps back to the predecessor.
  pub fn rank_of_floor(&mut self, key: &[u8]) -> Result<Option<u64>> {
    if key.len() != self.key_size as usize {
      bail!(
        "key must be exactly {} bytes, got {}",
        self.key_size,
        key.len()
      );
    }
    let mut page_id = self.root_page_id;
    let mut passed: u64 = 0;
    loop {
      let bytes = self.store.read_page(page_id)?;
      match page_type(&bytes)? {
        PageType::Leaf => {
          let node = decode_leaf(&bytes, self.key_size, self.value_size)?;
          return Ok(
            match node.entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
              Ok(i) => Some(passed + i as u64),
              Err(0) => {
                if passed == 0 {
                  None
                } else {
                  Some(passed - 1)
                }
              }
              Err(i) => Some(passed + i as u64 - 1),
            },
          );
        }
        PageType::Internal => {
          let node = decode_internal(&bytes, self.key_size)?;
          let mut next = node.rightmost_child;
          for (separator, child, count) in &node.entries {
            if key < separator.as_slice() {
              next = *child;
              break;
            }
            passed += count;
          }
          page_id = next;
        }
      }
    }
  }
```

Note the `Err(0)` case with `passed > 0`: the probe falls before this
leaf's first entry but subtrees were passed on the left — the floor is
the last entry of the preceding subtree (`passed - 1`). This can occur
when the probe is smaller than every key in its leaf but a left-sibling
subtree exists (separator keys are bounds, not necessarily present
keys, especially after removes).

- [ ] **Step 4: Run to verify pass** — `cargo test -p seisin-storage && cargo clippy -p seisin-storage --all-targets -- -D warnings` → all PASS. (If clippy flags the unused `probe` scaffolding in the first test, simplify that test by deleting the dead lines.)

- [ ] **Step 5: Commit** — `git add crates/seisin-storage/src/btree.rs && git commit -m "feat: add BPlusTree rank_of_floor"`

---

### Task 2: tk wire types and codecs

**Files:**
- Modify: `crates/seisin-protocol/src/lib.rs`
- Modify: `crates/seisin-node/src/pool.rs` (peer-link arms)

**Interfaces:**
- Produces (all derive `Debug, Clone, PartialEq, Eq`):

```rust
pub enum TkOp {
  Set { sub_key: Vec<u8>, as_of: Option<i64>, value: Vec<u8> },
  Clear { sub_key: Vec<u8>, as_of: Option<i64> },
}
pub enum TkQueryReq {
  AsOf { sub_key: Vec<u8>, t: i64 },
  Current { sub_key: Vec<u8> },
  History { sub_key: Vec<u8> },
  Range { sub_key: Vec<u8>, from: i64, to: i64 },
  SnapshotAt { t: i64 },
}
pub struct TkSpan {
  pub sub_key: Vec<u8>,
  pub lower: i64,
  pub upper: Option<i64>,
  pub value: Vec<u8>,
}
pub struct TkResult { pub spans: Vec<TkSpan> }
```

  plus `Request::TkExecute { entity_datum_id: DatumId, class: String, op: TkOp }`, `Request::TkQuery { entity_datum_id: DatumId, class: String, query: TkQueryReq }`, `Response::TkResult(TkResult)`, and pub codecs `encode_tk_op`/`decode_tk_op`, `encode_tk_query_req`/`decode_tk_query_req`, `encode_tk_result`/`decode_tk_result`.

Constants: `OP_TK_EXECUTE: u8 = 9`, `OP_TK_QUERY: u8 = 10`, `RESP_TK_RESULT: u8 = 9`; op tags `TK_OP_SET = 0`, `TK_OP_CLEAR = 1`; query tags `TK_Q_AS_OF = 0`, `TK_Q_CURRENT = 1`, `TK_Q_HISTORY = 2`, `TK_Q_RANGE = 3`, `TK_Q_SNAPSHOT_AT = 4`.

Encoding reuses the existing lb cursor helpers (`put_bytes`/`take_bytes`, `take_u64` for i64 via cast, `take_u32`), plus two small new ones:

```rust
fn put_opt_i64(buf: &mut Vec<u8>, v: Option<i64>) {
  match v {
    None => buf.push(0),
    Some(v) => {
      buf.push(1);
      buf.extend_from_slice(&v.to_le_bytes());
    }
  }
}

fn take_opt_i64(buf: &[u8], offset: &mut usize) -> Result<Option<i64>> {
  if buf.len() < *offset + 1 {
    bail!("truncated option flag at offset {offset}");
  }
  let flag = buf[*offset];
  *offset += 1;
  match flag {
    0 => Ok(None),
    1 => Ok(Some(take_u64(buf, offset)? as i64)),
    f => bail!("unknown option flag: {f}"),
  }
}
```

Layouts (strict trailing-byte checks, lb style):
- `TkOp`: tag, then Set = `put_bytes(sub_key) + put_opt_i64(as_of) + put_bytes(value)`; Clear = `put_bytes(sub_key) + put_opt_i64(as_of)`.
- `TkQueryReq`: tag, then per-variant `put_bytes(sub_key)` and i64 LE fields (`t`, `from`, `to`) as applicable; `SnapshotAt` is tag + `t` only.
- `TkSpan`: `put_bytes(sub_key) + lower i64 LE + put_opt_i64(upper) + put_bytes(value)`; `TkResult` = u32 count + spans.
- Request variants: opcode + 16-byte id + `put_bytes(class)` + op/query bytes to end of frame; `decode_tk_execute_request`/`decode_tk_query_request` mirror the lb decoders. Response arm delegates to `encode_tk_result`/`decode_tk_result(&buf[1..])`.

`pool.rs` `on_request` gains, next to the lb arms:

```rust
        seisin_protocol::Request::TkExecute { .. } => return,
        seisin_protocol::Request::TkQuery { .. } => return,
```

- [ ] **Step 1: Failing tests** — append to protocol tests:

```rust
  fn sample_tk_result() -> TkResult {
    TkResult {
      spans: vec![
        TkSpan {
          sub_key: vec![7u8; 16],
          lower: -5000,
          upper: Some(1000),
          value: b"v1".to_vec(),
        },
        TkSpan {
          sub_key: vec![],
          lower: 1000,
          upper: None,
          value: b"v2".to_vec(),
        },
      ],
    }
  }

  #[test]
  fn round_trips_tk_execute_requests() {
    for op in [
      TkOp::Set {
        sub_key: vec![7u8; 16],
        as_of: Some(-123),
        value: b"amount".to_vec(),
      },
      TkOp::Set {
        sub_key: vec![],
        as_of: None,
        value: b"x".to_vec(),
      },
      TkOp::Clear {
        sub_key: vec![7u8; 16],
        as_of: None,
      },
    ] {
      let req = Request::TkExecute {
        entity_datum_id: DatumId::new(),
        class: "holdings".to_string(),
        op: op.clone(),
      };
      assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
    }
  }

  #[test]
  fn round_trips_every_tk_query_variant() {
    for query in [
      TkQueryReq::AsOf {
        sub_key: vec![1u8; 16],
        t: 42,
      },
      TkQueryReq::Current { sub_key: vec![] },
      TkQueryReq::History {
        sub_key: vec![1u8; 16],
      },
      TkQueryReq::Range {
        sub_key: vec![1u8; 16],
        from: -10,
        to: 10,
      },
      TkQueryReq::SnapshotAt { t: 99 },
    ] {
      let req = Request::TkQuery {
        entity_datum_id: DatumId::new(),
        class: "holdings".to_string(),
        query: query.clone(),
      };
      assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
    }
  }

  #[test]
  fn round_trips_tk_results_including_empty() {
    for result in [sample_tk_result(), TkResult { spans: vec![] }] {
      let resp = Response::TkResult(result.clone());
      assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
    }
  }

  #[test]
  fn tk_result_codec_rejects_a_truncated_buffer() {
    let mut buf = encode_tk_result(&sample_tk_result());
    buf.truncate(buf.len() - 1);
    assert!(decode_tk_result(&buf).is_err());
  }
```

- [ ] **Step 2: Run to verify fail** — `cargo test -p seisin-protocol tk` → types not found.
- [ ] **Step 3: Implement** per the layouts above (full enum/struct definitions from Interfaces, encode/decode with the strict cursor style; every decode ends with the trailing-bytes check except the Request decoders whose op/query bytes run to frame end).
- [ ] **Step 4: Verify** — `cargo test -p seisin-protocol && cargo build --workspace` → PASS.
- [ ] **Step 5: Commit** — `git add crates/seisin-protocol/src/lib.rs crates/seisin-node/src/pool.rs && git commit -m "feat: add tk wire types and standalone codecs"`

---

### Task 3: tk class declaration, identity, timestamps, wall clock (`seisin-types::tk`)

**Files:**
- Create: `crates/seisin-types/src/tk.rs`
- Modify: `crates/seisin-types/src/lib.rs` (add `pub mod tk;`)

**Interfaces:**
- Produces:

```rust
pub struct TkClassDef {   // derives Debug, Clone, PartialEq
  pub name: String,
  pub value_type: FieldType,
  pub value_width: u16,
  pub sub_key_width: u16,
}
pub fn tk_kind_name(class: &str) -> String;                       // "tk:{class}"
pub fn tk_entity_key(class: &str, entity: DatumId) -> DatumId;    // from "tk:{class}:{entity-hex}"
pub fn encode_ts(t: i64) -> [u8; 8];                              // sign-flip big-endian
pub fn decode_ts(key: [u8; 8]) -> i64;
pub trait WallClock: Send + Sync { fn now_millis(&self) -> i64; }
pub struct SystemWallClock;
```

Full file:

```rust
//! tk (bitemporal valid-time) class declaration and identity: the
//! per-class definition (value type/width, sub-key width), entity
//! datum-id derivation, the order-preserving timestamp transform, and
//! the wall clock the server uses to stamp `as_of: None` writes. The
//! resident-history side (`TkIndexKind`) is `tk_kind.rs`. See the tk
//! design doc.

use std::time::{SystemTime, UNIX_EPOCH};

use seisin_core::datum::DatumId;

use crate::field::FieldType;
use crate::sk_index::derived_id_namespace;

/// One tk class. Registered as registry kind `tk:{name}` — one kind
/// per class, because `IndexKind::open` only receives a `DatumId`.
#[derive(Debug, Clone, PartialEq)]
pub struct TkClassDef {
  pub name: String,
  pub value_type: FieldType,
  /// Hard cap on the encoded value, bytes. tk is primary data — an
  /// oversized value is REJECTED, never truncated (TOAST overflow at
  /// the Storage Tier lifts this later).
  pub value_width: u16,
  /// Width of the sub-key prefix, bytes; 0 = the class tracks the
  /// entity itself. Non-zero = independent histories per sub-part of
  /// the entity (e.g. entity = account, sub_key = investment id).
  pub sub_key_width: u16,
}

pub fn tk_kind_name(class: &str) -> String {
  format!("tk:{class}")
}

/// One tk datum per (class, entity): identity normalized into the
/// datum id, entities distributed by ordinary ring placement.
pub fn tk_entity_key(class: &str, entity: DatumId) -> DatumId {
  let hex: String = entity
    .as_bytes()
    .iter()
    .map(|b| format!("{b:02x}"))
    .collect();
  let name = format!("tk:{class}:{hex}");
  DatumId::from_name(&derived_id_namespace(), name.as_bytes())
}

/// i64 epoch-millis -> order-preserving 8 bytes (the I64 sign-flip
/// big-endian transform; pre-1970 backdates order correctly).
pub fn encode_ts(t: i64) -> [u8; 8] {
  ((t as u64) ^ 0x8000_0000_0000_0000).to_be_bytes()
}

pub fn decode_ts(key: [u8; 8]) -> i64 {
  (u64::from_be_bytes(key) ^ 0x8000_0000_0000_0000) as i64
}

/// Wall-clock seam for stamping `as_of: None` writes — gossip's
/// `ClockSource` is monotonic-`Instant`-based, the wrong tool for
/// epoch millis. Tests inject a fixed fake.
pub trait WallClock: Send + Sync {
  fn now_millis(&self) -> i64;
}

pub struct SystemWallClock;

impl WallClock for SystemWallClock {
  fn now_millis(&self) -> i64 {
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .expect("system clock before the unix epoch")
      .as_millis() as i64
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn entity_key_is_stable_and_distinguishes_class_and_entity() {
    let e1 = DatumId::new();
    let e2 = DatumId::new();
    assert_eq!(tk_entity_key("holdings", e1), tk_entity_key("holdings", e1));
    assert_ne!(tk_entity_key("holdings", e1), tk_entity_key("holdings", e2));
    assert_ne!(tk_entity_key("holdings", e1), tk_entity_key("prices", e1));
  }

  #[test]
  fn timestamps_round_trip_and_sort_byte_lexicographically() {
    let values = [i64::MIN, -1_000_000, -1, 0, 1, 1_000_000, i64::MAX];
    for v in values {
      assert_eq!(decode_ts(encode_ts(v)), v);
    }
    let keys: Vec<[u8; 8]> = values.iter().map(|v| encode_ts(*v)).collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
  }

  #[test]
  fn system_wall_clock_returns_a_plausible_now() {
    // 2020-01-01 in millis — anything after this is "plausible".
    assert!(SystemWallClock.now_millis() > 1_577_836_800_000);
  }

  #[test]
  fn kind_name_prefixes_the_class() {
    assert_eq!(tk_kind_name("holdings"), "tk:holdings");
  }
}
```

- [ ] **Step 1: Create the file** (above), **Step 2:** add `pub mod tk;` to `lib.rs` (alphabetical, after `sk_index`), **Step 3:** `cargo test -p seisin-types tk::` → 4 passed, **Step 4:** commit `feat: add tk class declaration, identity, timestamp transform, wall clock`.

---

### Task 4: `TkIndexKind`/`TkResidentHistory` (`seisin-types::tk_kind`)

**Files:**
- Create: `crates/seisin-types/src/tk_kind.rs`
- Modify: `crates/seisin-types/src/lib.rs` (add `pub mod tk_kind;`)

**Interfaces:**
- Consumes: Tasks 1–3 plus `encoding::decode_field_value`, lb-era rail methods.
- Produces: `pub struct TkIndexKind` (`TkIndexKind::new(def, data_dir, clock: Arc<dyn WallClock>)`), `pub fn register_tk_class(registry, def, data_dir, clock)`.

Key implementation decisions (locked):
- Tree: `key_size = sub_key_width + 8`, `value_size = 1 + 8 + 2 + value_width`, page 4096; file `tk_<datum-hex>.btree`.
- Record: `flag(1: 0=open,1=bounded) ++ encode_ts(upper) or zeroes ++ len(u16 LE) ++ value ++ pad`.
- All logic goes through a raw-floor helper that returns the global floor rank+entry even when the floor lands in another sub-key (the successor computation needs the rank either way); a prefix check decides in-sub-key-ness.
- `Set` with `lower == as_of` overwrites the value in place (bounds unchanged). `Clear` with `lower == as_of` **removes** the entry (a `[t, t)` range holds no information) and returns nothing.
- Validation order for `Set`: sub-key width, then value decodes as `value_type` consuming all bytes, then `len <= value_width` — each failure a distinct, loud error.
- `History`/`Range`/`SnapshotAt` walk rank cursors entry-at-a-time (`scan_from_rank(r, 1)`); histories are per-entity-bounded, and SnapshotAt jumps sub-keys via `rank_of_floor(sub ++ encode_ts(i64::MAX)) + 1`.

Core structure (complete `execute`/`query` logic to implement; module doc, imports, `file_name_for`, and registration mirror `lb_kind.rs` with `lb`→`tk` naming):

```rust
pub struct TkResidentHistory {
  def: TkClassDef,
  clock: Arc<dyn WallClock>,
  tree: RefCell<BPlusTree>,
}

impl TkResidentHistory {
  fn sub_w(&self) -> usize { self.def.sub_key_width as usize }

  fn composite(&self, sub_key: &[u8], t: i64) -> Vec<u8> {
    let mut key = sub_key.to_vec();
    key.extend_from_slice(&encode_ts(t));
    key
  }

  fn record(&self, upper: Option<i64>, value: &[u8]) -> Vec<u8> {
    let mut rec = vec![0u8; 1 + 8 + 2 + self.def.value_width as usize];
    if let Some(upper) = upper {
      rec[0] = 1;
      rec[1..9].copy_from_slice(&encode_ts(upper));
    }
    rec[9..11].copy_from_slice(&(value.len() as u16).to_le_bytes());
    rec[11..11 + value.len()].copy_from_slice(value);
    rec
  }

  fn span_from(&self, key: &[u8], rec: &[u8]) -> TkSpan {
    let sub_w = self.sub_w();
    let lower = decode_ts(key[sub_w..sub_w + 8].try_into().unwrap());
    let upper = if rec[0] == 1 {
      Some(decode_ts(rec[1..9].try_into().unwrap()))
    } else {
      None
    };
    let len = u16::from_le_bytes(rec[9..11].try_into().unwrap()) as usize;
    TkSpan {
      sub_key: key[..sub_w].to_vec(),
      lower,
      upper,
      value: rec[11..11 + len].to_vec(),
    }
  }

  fn entry_at(&self, tree: &mut BPlusTree, rank: u64) -> Result<Option<(Vec<u8>, Vec<u8>)>, String> {
    Ok(tree.scan_from_rank(rank, 1).map_err(|e| e.to_string())?.into_iter().next())
  }

  /// Global floor at (sub_key, t): rank + entry, regardless of which
  /// sub-key the floor lands in — callers prefix-check for
  /// in-sub-key-ness, and the successor computation needs the rank
  /// either way.
  fn raw_floor(
    &self,
    tree: &mut BPlusTree,
    sub_key: &[u8],
    t: i64,
  ) -> Result<Option<(u64, Vec<u8>, Vec<u8>)>, String> {
    let probe = self.composite(sub_key, t);
    match tree.rank_of_floor(&probe).map_err(|e| e.to_string())? {
      None => Ok(None),
      Some(rank) => {
        let (k, v) = self
          .entry_at(tree, rank)?
          .ok_or_else(|| "floor rank out of range".to_string())?;
        Ok(Some((rank, k, v)))
      }
    }
  }

  /// The span covering `t` in `sub_key`, with its rank, if any.
  fn covering(
    &self,
    tree: &mut BPlusTree,
    sub_key: &[u8],
    t: i64,
  ) -> Result<Option<(u64, TkSpan)>, String> {
    match self.raw_floor(tree, sub_key, t)? {
      Some((rank, k, v)) if &k[..self.sub_w()] == sub_key => {
        let span = self.span_from(&k, &v);
        if span.upper.is_none() || span.upper.unwrap() > t {
          Ok(Some((rank, span)))
        } else {
          Ok(None)
        }
      }
      _ => Ok(None),
    }
  }

  fn validate_set(&self, sub_key: &[u8], value: &[u8]) -> Result<(), String> {
    if sub_key.len() != self.sub_w() {
      return Err(format!(
        "sub_key must be exactly {} bytes for class {:?}, got {}",
        self.def.sub_key_width, self.def.name, sub_key.len()
      ));
    }
    let mut offset = 0;
    crate::encoding::decode_field_value(&self.def.value_type, value, &mut offset)
      .map_err(|e| format!("value does not decode as {:?}: {e}", self.def.value_type))?;
    if offset != value.len() {
      return Err(format!("value has {} trailing bytes", value.len() - offset));
    }
    if value.len() > self.def.value_width as usize {
      return Err(format!(
        "encoded value is {} bytes but class {:?} caps values at {} — rejected, never truncated \
         (tk is primary data)",
        value.len(), self.def.name, self.def.value_width
      ));
    }
    Ok(())
  }

  fn do_set(&self, sub_key: &[u8], as_of: i64, value: &[u8]) -> Result<Vec<TkSpan>, String> {
    let mut tree = self.tree.borrow_mut();
    let err = |e: anyhow::Error| e.to_string();
    match self.covering(&mut tree, sub_key, as_of)? {
      Some((_, span)) if span.lower == as_of => {
        // Same-instant value correction: bounds unchanged.
        tree
          .insert(&self.composite(sub_key, as_of), &self.record(span.upper, value))
          .map_err(err)?;
        Ok(vec![TkSpan { value: value.to_vec(), ..span }])
      }
      Some((_, span)) => {
        // Close the covering range at as_of; the new range inherits
        // its old upper — non-overlap by construction.
        tree
          .insert(
            &self.composite(sub_key, span.lower),
            &self.record(Some(as_of), &span.value),
          )
          .map_err(err)?;
        tree
          .insert(&self.composite(sub_key, as_of), &self.record(span.upper, value))
          .map_err(err)?;
        Ok(vec![TkSpan {
          sub_key: sub_key.to_vec(),
          lower: as_of,
          upper: span.upper,
          value: value.to_vec(),
        }])
      }
      None => {
        // Gap (or before the first entry of this sub-key): bound by
        // the sub-key's successor, if any — never leaks across
        // sub-parts.
        let successor_rank = match self.raw_floor(&mut tree, sub_key, as_of)? {
          Some((rank, _, _)) => rank + 1,
          None => 0,
        };
        let upper = match self.entry_at(&mut tree, successor_rank)? {
          Some((k, _)) if &k[..self.sub_w()] == sub_key => {
            Some(decode_ts(k[self.sub_w()..self.sub_w() + 8].try_into().unwrap()))
          }
          _ => None,
        };
        tree
          .insert(&self.composite(sub_key, as_of), &self.record(upper, value))
          .map_err(err)?;
        Ok(vec![TkSpan {
          sub_key: sub_key.to_vec(),
          lower: as_of,
          upper,
          value: value.to_vec(),
        }])
      }
    }
  }

  fn do_clear(&self, sub_key: &[u8], as_of: i64) -> Result<Vec<TkSpan>, String> {
    if sub_key.len() != self.sub_w() {
      return Err(format!(
        "sub_key must be exactly {} bytes, got {}",
        self.def.sub_key_width, sub_key.len()
      ));
    }
    let mut tree = self.tree.borrow_mut();
    let err = |e: anyhow::Error| e.to_string();
    match self.covering(&mut tree, sub_key, as_of)? {
      None => Ok(vec![]), // clearing inside a gap is a no-op
      Some((_, span)) if span.lower == as_of => {
        // A [t, t) range holds no information — remove outright.
        tree
          .remove(&self.composite(sub_key, span.lower))
          .map_err(err)?;
        Ok(vec![])
      }
      Some((_, span)) => {
        tree
          .insert(
            &self.composite(sub_key, span.lower),
            &self.record(Some(as_of), &span.value),
          )
          .map_err(err)?;
        Ok(vec![TkSpan { upper: Some(as_of), ..span }])
      }
    }
  }

  /// First rank belonging to `sub_key`, and a cursor-walk collector.
  fn first_rank_of(&self, tree: &mut BPlusTree, sub_key: &[u8]) -> Result<u64, String> {
    match self.raw_floor(tree, sub_key, i64::MIN)? {
      Some((rank, k, _)) if k == self.composite(sub_key, i64::MIN) => Ok(rank),
      Some((rank, _, _)) => Ok(rank + 1),
      None => Ok(0),
    }
  }

  fn collect_while(
    &self,
    tree: &mut BPlusTree,
    mut rank: u64,
    keep: impl Fn(&TkSpan) -> bool,
  ) -> Result<Vec<TkSpan>, String> {
    let mut spans = Vec::new();
    while let Some((k, v)) = self.entry_at(tree, rank)? {
      let span = self.span_from(&k, &v);
      if !keep(&span) {
        break;
      }
      spans.push(span);
      rank += 1;
    }
    Ok(spans)
  }
}
```

`execute` dispatches `TkOp` (resolving `as_of.unwrap_or_else(|| self.clock.now_millis())`, running `validate_set` first for Set), returns `encode_tk_result(&TkResult { spans })`. `query` dispatches `TkQueryReq`:

- `AsOf { sub_key, t }` → `covering` → 0..1 spans.
- `Current { sub_key }` → `raw_floor(sub, i64::MAX)`, prefix check, keep if `upper.is_none()`.
- `History { sub_key }` → `first_rank_of` + `collect_while(|s| s.sub_key == sub_key)`.
- `Range { sub_key, from, to }` → empty if `to <= from`; start = `covering(sub, from)`'s rank if some, else `first_rank_of`-style successor of the floor at `from`; `collect_while(|s| s.sub_key == sub_key && s.lower < to)`.
- `SnapshotAt { t }` → loop from rank 0: read entry, take its sub-key, `covering(sub, t)` → maybe push; jump to `rank_of_floor(composite(sub, i64::MAX)).unwrap() + 1`.

`apply` rejected with `"tk histories are maintained via execute ops, not framework index updates"`. `open` mirrors lb's (create dir, `BPlusTree::open`/`create` with the class-derived sizes; `stored` ignored — self-persisted).

- [ ] **Step 1: Write the module with its failing tests.** Tests (using a `FixedClock(i64)` fake implementing `WallClock`; class `holdings { value_type: F64, value_width: 16, sub_key_width: 16 }` and a width-0 variant; values encoded via `crate::encoding::encode_field_value`):

```rust
  // Test list (each a #[test], full bodies written at implementation
  // time following lb_kind's test style — helpers: open_history(),
  // set(board, sub, as_of, f64) -> Vec<TkSpan>, spans_of(query)):
  // - forward_set_on_empty_creates_an_open_ended_span
  // - second_set_closes_the_open_range_and_chains_uppers
  // - backdated_correction_splits_a_past_closed_range
  // - same_instant_set_overwrites_value_keeping_bounds
  // - clear_creates_a_gap_and_as_of_inside_it_returns_nothing
  // - set_into_a_gap_inherits_the_successors_lower
  // - set_before_the_first_entry_bounds_at_first_lower
  // - clear_at_exact_lower_removes_the_span_entirely
  // - two_sub_keys_never_interact (gap-fill in one unbounded by other)
  // - snapshot_at_returns_one_covering_span_per_sub_key_skipping_gaps
  // - wrong_width_sub_key_and_oversized_and_mistyped_values_rejected
  // - as_of_none_is_stamped_by_the_injected_clock
  // - current_distinguishes_open_ended_from_closed_final_range
  // - range_query_spans_gaps_and_clips_at_bounds
  // - cold_reopen_answers_from_the_file
  // - apply_is_rejected_and_malformed_execute_errors
```

  (The test-list comment is a checklist for this task's Step 1, not
  shipped code — every listed test must exist with a real body before
  Step 4.)

- [ ] **Step 2:** `cargo test -p seisin-types tk_kind` → compile failures until the module lands.
- [ ] **Step 3:** implement + `pub mod tk_kind;`.
- [ ] **Step 4:** `cargo test -p seisin-types && cargo clippy -p seisin-types --all-targets -- -D warnings` → all PASS.
- [ ] **Step 5:** commit `feat: add TkIndexKind/TkResidentHistory with sub-key valid-time histories`.

---

### Task 5: server routing

**Files:**
- Modify: `crates/seisin-node/src/server.rs`

Match arms for `Request::TkExecute`/`TkQuery` + `handle_tk_execute`/`handle_tk_query`/`tk_result_response`, mirroring the lb handlers exactly (shared `redirect_if_foreign`; kind string `format!("tk:{class}")`; execute → `run_index_execute`, query → `run_index_query`; result bytes decoded via `decode_tk_result` → `Response::TkResult`).

- [ ] **Step 1:** implement; **Step 2:** `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings` → PASS; **Step 3:** commit `feat: route client TkExecute/TkQuery requests`.

---

### Task 6: integration test, stress, docs

**Files:**
- Create: `crates/seisin-types/tests/integration_tk_history.rs`
- Modify: `docs/superpowers/PROGRESS.md`

- [ ] **Step 1: Integration test** — lb's bootstrap pattern (`start_node` with `register_tk_class(..., Arc::new(SystemWallClock))`), class `holdings { F64, value_width: 16, sub_key_width: 16 }`, two entities (accounts), two sub-keys (investments) each:
  - Set amounts at explicit times over the wire; backdate a correction; Clear one investment; then `AsOf`, `Current`, `History`, `Range`, and `SnapshotAt` verifying values and bounds — including that account A's history never shows account B's data and investment 1's gap doesn't affect investment 2.
  - An oversized value gets `Response::OpError` containing "rejected".
  - A `Set` with `as_of: None` lands with a plausible stamped time (`Current` returns it; lower > 2020-01-01 millis).
- [ ] **Step 2:** run it; debug via superpowers:systematic-debugging if needed.
- [ ] **Step 3: Stress** — 10x the tk integration test; 20x `integration_wound_wait`/`integration_cross_node_wound_wait`/`integration_op_collation`.
- [ ] **Step 4: Gates** — `cargo test --workspace && cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings`.
- [ ] **Step 5: PROGRESS.md** — Done entry for tk (established format), noting Parts 1–4 complete and Part 5 (FK constraints) next.
- [ ] **Step 6:** `git add -A && git commit -m "feat: tk bitemporal valid-time datum class end-to-end" && git push`.

---

## Deliberately Out of Scope (from the spec — do not build)

- TypedOpContext sugar; transaction-time audit; no-gaps opt-in invariant; TOAST for wide values; file consolidation/placement (Storage Tier).
