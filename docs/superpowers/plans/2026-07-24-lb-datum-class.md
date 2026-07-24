# lb (Leaderboard) Datum Class Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline execution — this project never uses subagents) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The lb structured datum class: per-(class, leaderboard, area-config) board datums holding covering `(score, player, display)` entries under a declared update rule, with single-round-trip update/query ops returning top lists, exact rank, neighbors, and friend ranks.

**Architecture:** lb is the third class on the `ResidentIndex` rail (spec: `docs/superpowers/specs/2026-07-23-lb-datum-class-design.md`). A new `execute` method on `ResidentIndex` (mutate-with-result, dispatched like the existing query path) carries solution-called ops; each board is a counted B+Tree file (key = rank_key ++ player_id, value = length-prefixed fixed-width display) plus a cold-rebuilt player→key map. Wire ops route like `RkQuery` (redirect if not native), with codecs defined once in `seisin-protocol`.

**Tech Stack:** Rust workspace, hand-rolled binary codecs, `anyhow::Result`, `tempfile` for tests.

## Global Constraints

- 2-space indent; `cargo fmt --check` and `cargo clippy --workspace --all-targets -- -D warnings` clean after every task.
- `anyhow::Result` + `bail!()` in library code; trait methods established as `Result<_, String>` (`ResidentIndex`, `IndexKind::open`) stay `String`.
- No new dependencies. No `unsafe`. Doc comments explain why, not what.
- Commit per task, conventional-commits style, `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` trailer; never commit failing tests.
- New `Request`/`Response` variants inherit the `PROTOCOL_VERSION` prefix automatically from `encode_request`/`encode_response` — never add version bytes inside variant bodies.
- Ranks on the wire and in code are **0-based from best** (rank 0 = highest score). The B+Tree is ascending, so best-rank = `total - 1 - ascending_rank`; conversions happen only inside `lb_kind.rs`.
- lb is primary data (spec's Durability Note): no rebuild-from-scan story; `apply` (the framework-diff rail) is rejected for lb boards.

---

### Task 1: `ResidentIndex::execute` default method

**Files:**
- Modify: `crates/seisin-node/src/index_handler.rs`

**Interfaces:**
- Produces: `ResidentIndex::execute(&mut self, payload: &[u8]) -> Result<Vec<u8>, String>` with a default "unsupported" error. Task 3's worker plumbing and Task 6's lb impl use it.

- [ ] **Step 1: Write the failing test** — append to the `tests` module:

```rust
  #[test]
  fn execute_default_impl_reports_the_kind_supports_no_execute_ops() {
    let mut registry = IndexKindRegistry::new();
    registry.register("append", Box::new(AppendKind));
    let mut resident = registry
      .get("append")
      .unwrap()
      .open(DatumId::new(), None)
      .unwrap();
    let err = resident.execute(b"anything").unwrap_err();
    assert!(err.contains("no execute ops"), "{err}");
  }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p seisin-node --lib execute_default`
Expected: FAIL — no method `execute`.

- [ ] **Step 3: Add the default method** to the trait, after `query`:

```rust
  /// A solution-called, mutating op against the resident structure,
  /// returning result data — payload and result bytes are opaque to
  /// the framework, exactly like `query`. Unlike `apply` (the
  /// framework-diff rail with pass/violation semantics), `execute`
  /// carries a data result; the owning thread's serial message
  /// processing makes each call atomic. Kinds with no solution-called
  /// ops (sk, rk) keep this default.
  fn execute(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
    let _ = payload;
    Err("this index kind supports no execute ops".to_string())
  }
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p seisin-node --lib index_handler`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-node/src/index_handler.rs
git commit -m "feat: add default execute method to ResidentIndex for mutate-with-result ops"
```

---

### Task 2: `BPlusTree::rank_of_key` and `scan_from_rank`

**Files:**
- Modify: `crates/seisin-storage/src/btree.rs`

**Interfaces:**
- Consumes: existing internals (`read_leaf`/`decode_internal`/`page_type`, `contains`-style descent shape).
- Produces:
  - `pub fn rank_of_key(&mut self, key: &[u8]) -> Result<Option<u64>>` — 0-based **ascending** rank of `key`, `None` if absent.
  - `pub fn scan_from_rank(&mut self, rank: u64, n: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>>` — up to `n` entries starting at ascending rank `rank` (empty if `rank >= len`), walking sibling links after one descent.

- [ ] **Step 1: Write the failing tests** — append to the `tests` module in `btree.rs`:

```rust
  #[test]
  fn rank_of_key_matches_insertion_order_and_misses_return_none() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    for i in 0..300u64 {
      tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }
    assert_eq!(tree.rank_of_key(&0u64.to_be_bytes()).unwrap(), Some(0));
    assert_eq!(tree.rank_of_key(&157u64.to_be_bytes()).unwrap(), Some(157));
    assert_eq!(tree.rank_of_key(&299u64.to_be_bytes()).unwrap(), Some(299));
    assert_eq!(tree.rank_of_key(&999u64.to_be_bytes()).unwrap(), None);
  }

  #[test]
  fn rank_of_key_stays_correct_after_removes_and_across_levels() {
    let tmp = NamedTempFile::new().unwrap();
    // key/value_size 1000 on 4096-byte pages: multi-level tree cheaply.
    let mut tree = BPlusTree::create(tmp.path(), 1000, 1000, 4096).unwrap();
    let make = |i: u64| {
      let mut k = vec![0u8; 1000];
      k[0..8].copy_from_slice(&i.to_be_bytes());
      k
    };
    for i in 0..60u64 {
      tree.insert(&make(i), &make(i)).unwrap();
    }
    for i in (0..60u64).step_by(3) {
      assert!(tree.remove(&make(i)).unwrap());
    }
    // Remaining keys are those with i % 3 != 0, in order; check a few
    // ranks against a straightforward model.
    let remaining: Vec<u64> = (0..60u64).filter(|i| i % 3 != 0).collect();
    for (rank, i) in remaining.iter().enumerate() {
      assert_eq!(tree.rank_of_key(&make(*i)).unwrap(), Some(rank as u64));
    }
    assert_eq!(tree.rank_of_key(&make(0)).unwrap(), None); // removed
  }

  #[test]
  fn scan_from_rank_returns_entries_from_that_rank_crossing_leaves() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    // 300 entries, max_leaf_entries=254 — rank 250..260 crosses a leaf link.
    for i in 0..300u64 {
      tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }
    let window = tree.scan_from_rank(250, 10).unwrap();
    assert_eq!(window.len(), 10);
    for (offset, (key, _)) in window.iter().enumerate() {
      assert_eq!(key, &(250 + offset as u64).to_be_bytes().to_vec());
    }
  }

  #[test]
  fn scan_from_rank_clamps_at_the_end_and_rejects_nothing_past_it() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    for i in 0..10u64 {
      tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }
    assert_eq!(tree.scan_from_rank(7, 100).unwrap().len(), 3);
    assert_eq!(tree.scan_from_rank(10, 5).unwrap(), vec![]);
    assert_eq!(tree.scan_from_rank(0, 0).unwrap(), vec![]);
  }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p seisin-storage rank_of_key scan_from_rank`
Expected: FAIL — methods not found.

- [ ] **Step 3: Implement** — add to the same `impl BPlusTree` block as `remove`/`contains`:

```rust
  /// 0-based ascending rank of `key` (None if absent) — the mirror of
  /// `entry_at_rank`: descend toward the key, accumulating the counts
  /// of every subtree passed on the left, then add the position inside
  /// the leaf.
  pub fn rank_of_key(&mut self, key: &[u8]) -> Result<Option<u64>> {
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
            node
              .entries
              .binary_search_by(|(k, _)| k.as_slice().cmp(key))
              .ok()
              .map(|i| passed + i as u64),
          );
        }
        PageType::Internal => {
          let node = decode_internal(&bytes, self.key_size)?;
          let mut next = node.rightmost_child;
          let mut descended = false;
          for (separator, child, count) in &node.entries {
            if key < separator.as_slice() {
              next = *child;
              descended = true;
              break;
            }
            passed += count;
          }
          let _ = descended;
          page_id = next;
        }
      }
    }
  }

  /// Up to `n` entries starting at ascending rank `rank`: one counted
  /// descent to the holding leaf, then a sibling-link walk — no
  /// per-entry descents, so a neighbors window costs one descent total.
  pub fn scan_from_rank(&mut self, rank: u64, n: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    if n == 0 || rank >= self.total_count {
      return Ok(Vec::new());
    }
    let mut remaining_rank = rank as usize;
    let mut page_id = self.root_page_id;
    loop {
      let bytes = self.store.read_page(page_id)?;
      match page_type(&bytes)? {
        PageType::Leaf => break,
        PageType::Internal => {
          let node = decode_internal(&bytes, self.key_size)?;
          let mut next = node.rightmost_child;
          for (_, child, count) in &node.entries {
            let count = *count as usize;
            if remaining_rank < count {
              next = *child;
              break;
            }
            remaining_rank -= count;
          }
          page_id = next;
        }
      }
    }
    let mut results = Vec::with_capacity(n);
    while page_id != NULL_PAGE && results.len() < n {
      let node = self.read_leaf(page_id)?;
      for entry in node.entries.iter().skip(remaining_rank) {
        if results.len() >= n {
          break;
        }
        results.push(entry.clone());
      }
      remaining_rank = 0;
      page_id = node.next;
    }
    Ok(results)
  }
```

Note the `let _ = descended;` line exists only if the compiler warns
about it — if `descended` ends up genuinely unused, delete the variable
entirely rather than silencing it (rank_of_key doesn't need it; it's
shown here to flag the intent — remove it in the final code).

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p seisin-storage && cargo clippy -p seisin-storage --all-targets -- -D warnings`
Expected: all PASS, clippy clean (delete the `descended` scaffolding per the note above).

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-storage/src/btree.rs
git commit -m "feat: add BPlusTree rank_of_key and scan_from_rank"
```

---

### Task 3: worker/pool `IndexExecute` plumbing

**Files:**
- Modify: `crates/seisin-node/src/worker.rs`
- Modify: `crates/seisin-node/src/pool.rs`

**Interfaces:**
- Consumes: Task 1's `ResidentIndex::execute`.
- Produces:
  - `WorkerHandle::run_index_execute(&self, target: DatumId, index_kind: String, payload: Vec<u8>) -> Result<Vec<u8>, String>`
  - `WorkerPool::run_index_execute(&self, target: DatumId, index_kind: String, payload: Vec<u8>) -> Result<Vec<u8>, String>` (thread picked via `ring.native(target)`)

- [ ] **Step 1: Write the failing tests.** First give the test fixture an execute override — in `worker.rs` tests, add to `impl crate::index_handler::ResidentIndex for FixedOutcomeResident`:

```rust
    fn execute(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
      // Reverse — distinct from query's uppercase, so a test can tell
      // which method actually ran.
      Ok(payload.iter().rev().copied().collect())
    }
```

Then the tests:

```rust
  #[test]
  fn run_index_execute_reaches_the_resident_index_and_returns_its_result() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let mut index_kinds = crate::index_handler::IndexKindRegistry::new();
    index_kinds.register("echo", Box::new(FixedOutcomeKind { violation: None }));
    let handles = spawn_test_pool_with_index_kinds(1, ring, OpRegistry::new(), index_kinds);
    let result = handles[0].run_index_execute(DatumId::new(), "echo".to_string(), b"abc".to_vec());
    assert_eq!(result, Ok(b"cba".to_vec()));
  }

  #[test]
  fn run_index_execute_on_an_unregistered_kind_is_an_error() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let handles = spawn_test_pool(1, ring, OpRegistry::new());
    assert!(handles[0]
      .run_index_execute(DatumId::new(), "nope".to_string(), vec![])
      .is_err());
  }

  #[test]
  fn execute_then_query_hit_the_same_resident_instance() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let mut index_kinds = crate::index_handler::IndexKindRegistry::new();
    index_kinds.register("echo", Box::new(FixedOutcomeKind { violation: None }));
    let handles = spawn_test_pool_with_index_kinds(1, ring, OpRegistry::new(), index_kinds);
    let target = DatumId::new();
    assert!(handles[0]
      .run_index_execute(target, "echo".to_string(), b"x".to_vec())
      .is_ok());
    let result = handles[0].run_index_query(target, "echo".to_string(), b"q".to_vec());
    assert_eq!(result, Ok(b"Q".to_vec()));
  }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p seisin-node run_index_execute`
Expected: FAIL — no method.

- [ ] **Step 3: Implement.** In `worker.rs`, a new variant after `IndexQuery`:

```rust
  /// A solution-called, mutating op against the index/structured datum
  /// `target`, answered synchronously by its owning thread — the
  /// mutate-with-result sibling of `IndexQuery`. No collation, no op
  /// record: single-datum atomicity comes from serial message
  /// processing on the owning thread.
  IndexExecute {
    target: DatumId,
    index_kind: String,
    payload: Vec<u8>,
    reply: Sender<Result<Vec<u8>, String>>,
  },
```

Handler arm, directly after the `IndexQuery` arm (identical cold-open
shape, differing only in the `&mut` call):

```rust
          WorkerMessage::IndexExecute {
            target,
            index_kind,
            payload,
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
              Ok(resident) => resident.execute(&payload),
              Err(message) => Err(message),
            };
            let _ = reply.send(result);
          }
```

`WorkerHandle` method next to `run_index_query`:

```rust
  /// Sends a mutate-with-result op to this thread and blocks for the
  /// answer — same synchronous shape as `run_index_query`.
  pub fn run_index_execute(
    &self,
    target: DatumId,
    index_kind: String,
    payload: Vec<u8>,
  ) -> Result<Vec<u8>, String> {
    let (reply_tx, reply_rx) = std::sync::mpsc::channel();
    self
      .sender
      .send(WorkerMessage::IndexExecute {
        target,
        index_kind,
        payload,
        reply: reply_tx,
      })
      .expect("worker thread exited unexpectedly");
    reply_rx.recv().expect("worker dropped the reply channel")
  }
```

`pool.rs`, next to `run_index_query`:

```rust
  /// Routes a mutate-with-result op to whichever local thread natively
  /// owns `target`. Callers must have already confirmed `target`
  /// resolves to this node — see `server.rs`'s redirect check.
  pub fn run_index_execute(
    &self,
    target: DatumId,
    index_kind: String,
    payload: Vec<u8>,
  ) -> Result<Vec<u8>, String> {
    let (_, thread_id) = self.ring.read().unwrap().native(target);
    self.handles[thread_id.0 as usize].run_index_execute(target, index_kind, payload)
  }
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p seisin-node`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-node/src/worker.rs crates/seisin-node/src/pool.rs
git commit -m "feat: add synchronous index-execute path through worker and pool"
```

---

### Task 4: lb wire types and codecs (`seisin-protocol`)

**Files:**
- Modify: `crates/seisin-protocol/src/lib.rs`
- Modify: `crates/seisin-node/src/pool.rs` (peer-link `on_request` arms)

**Interfaces:**
- Produces (used by Tasks 6, 7, 8):

```rust
pub enum LbExecuteOp {
  Update {
    player_id: DatumId,
    display: Vec<u8>,
    rank_key: [u8; 8],
    friend_ids: Vec<DatumId>,
    top: u32,
    window: u32,
  },
  Remove { player_id: DatumId },
}

pub struct LbQueryReq {
  pub top: u32,
  pub bottom: u32,
  pub around_player: Option<DatumId>,
  pub window: u32,
  pub friend_ids: Vec<DatumId>,
}

pub struct LbEntry {
  pub rank_key: [u8; 8],
  pub player_id: DatumId,
  pub display: Vec<u8>,
}

pub struct LbFriendRank {
  pub player_id: DatumId,
  pub rank: u64, // 0-based from best
  pub rank_key: [u8; 8],
  pub display: Vec<u8>,
}

pub struct LbResult {
  pub total: u64,
  pub player_rank: Option<u64>, // 0-based from best; None for Remove / query without around_player
  pub top: Vec<LbEntry>,
  pub bottom: Vec<LbEntry>,     // empty on execute results
  pub around: Vec<LbEntry>,
  pub friends: Vec<LbFriendRank>,
}
```

  plus `Request::LbExecute { board_id: DatumId, class: String, op: LbExecuteOp }`, `Request::LbQuery { board_id: DatumId, class: String, query: LbQueryReq }`, `Response::LbResult(LbResult)`, and standalone codecs `encode_lb_execute_op`/`decode_lb_execute_op`, `encode_lb_query_req`/`decode_lb_query_req`, `encode_lb_result`/`decode_lb_result` (all pub — the worker treats these bytes as opaque; the lb impl in `seisin-types` uses the same codecs).

All structs/enums derive `Debug, Clone, PartialEq, Eq`.

- [ ] **Step 1: Write the failing tests** — append to the protocol `tests` module:

```rust
  fn sample_lb_result() -> LbResult {
    LbResult {
      total: 42,
      player_rank: Some(7),
      top: vec![LbEntry {
        rank_key: [9u8; 8],
        player_id: DatumId::new(),
        display: b"Alice".to_vec(),
      }],
      bottom: vec![],
      around: vec![LbEntry {
        rank_key: [3u8; 8],
        player_id: DatumId::new(),
        display: b"Bob".to_vec(),
      }],
      friends: vec![LbFriendRank {
        player_id: DatumId::new(),
        rank: 11,
        rank_key: [2u8; 8],
        display: b"Carol".to_vec(),
      }],
    }
  }

  #[test]
  fn round_trips_lb_execute_requests() {
    for op in [
      LbExecuteOp::Update {
        player_id: DatumId::new(),
        display: b"Alice".to_vec(),
        rank_key: [5u8; 8],
        friend_ids: vec![DatumId::new(), DatumId::new()],
        top: 10,
        window: 5,
      },
      LbExecuteOp::Remove {
        player_id: DatumId::new(),
      },
    ] {
      let req = Request::LbExecute {
        board_id: DatumId::new(),
        class: "racing".to_string(),
        op: op.clone(),
      };
      assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
    }
  }

  #[test]
  fn round_trips_lb_query_requests_with_and_without_around_player() {
    for around in [None, Some(DatumId::new())] {
      let req = Request::LbQuery {
        board_id: DatumId::new(),
        class: "racing".to_string(),
        query: LbQueryReq {
          top: 12,
          bottom: 12,
          around_player: around,
          window: 4,
          friend_ids: vec![DatumId::new()],
        },
      };
      assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
    }
  }

  #[test]
  fn round_trips_an_lb_result_response() {
    let resp = Response::LbResult(sample_lb_result());
    assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
  }

  #[test]
  fn round_trips_an_empty_lb_result() {
    let resp = Response::LbResult(LbResult {
      total: 0,
      player_rank: None,
      top: vec![],
      bottom: vec![],
      around: vec![],
      friends: vec![],
    });
    assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
  }

  #[test]
  fn lb_result_codec_rejects_a_truncated_buffer() {
    let mut buf = encode_lb_result(&sample_lb_result());
    buf.truncate(buf.len() - 1);
    assert!(decode_lb_result(&buf).is_err());
  }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p seisin-protocol lb`
Expected: FAIL — types not found.

- [ ] **Step 3: Implement.** Constants:

```rust
const OP_LB_EXECUTE: u8 = 7;
const OP_LB_QUERY: u8 = 8;
const RESP_LB_RESULT: u8 = 8;

const LB_OP_UPDATE: u8 = 0;
const LB_OP_REMOVE: u8 = 1;
```

Codec building blocks (private helpers, placed near the rk codecs; use a shared `offset`-cursor style so decode is strict):

```rust
fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
  buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
  buf.extend_from_slice(bytes);
}

fn take_bytes(buf: &[u8], offset: &mut usize) -> Result<Vec<u8>> {
  if buf.len() < *offset + 4 {
    bail!("truncated length prefix at offset {offset}");
  }
  let len = u32::from_le_bytes(buf[*offset..*offset + 4].try_into().unwrap()) as usize;
  *offset += 4;
  if buf.len() < *offset + len {
    bail!("truncated byte field at offset {offset}: expected {len} bytes");
  }
  let bytes = buf[*offset..*offset + len].to_vec();
  *offset += len;
  Ok(bytes)
}

fn put_id(buf: &mut Vec<u8>, id: DatumId) {
  buf.extend_from_slice(&id.as_bytes());
}

fn take_id(buf: &[u8], offset: &mut usize) -> Result<DatumId> {
  if buf.len() < *offset + ID_LEN {
    bail!("truncated datum id at offset {offset}");
  }
  let id = DatumId::from_bytes(buf[*offset..*offset + ID_LEN].try_into().unwrap());
  *offset += ID_LEN;
  Ok(id)
}

fn take_u32(buf: &[u8], offset: &mut usize) -> Result<u32> {
  if buf.len() < *offset + 4 {
    bail!("truncated u32 at offset {offset}");
  }
  let v = u32::from_le_bytes(buf[*offset..*offset + 4].try_into().unwrap());
  *offset += 4;
  Ok(v)
}

fn take_u64(buf: &[u8], offset: &mut usize) -> Result<u64> {
  if buf.len() < *offset + 8 {
    bail!("truncated u64 at offset {offset}");
  }
  let v = u64::from_le_bytes(buf[*offset..*offset + 8].try_into().unwrap());
  *offset += 8;
  Ok(v)
}

fn take_rank_key(buf: &[u8], offset: &mut usize) -> Result<[u8; 8]> {
  if buf.len() < *offset + 8 {
    bail!("truncated rank key at offset {offset}");
  }
  let key: [u8; 8] = buf[*offset..*offset + 8].try_into().unwrap();
  *offset += 8;
  Ok(key)
}

fn put_id_list(buf: &mut Vec<u8>, ids: &[DatumId]) {
  buf.extend_from_slice(&(ids.len() as u32).to_le_bytes());
  for id in ids {
    put_id(buf, *id);
  }
}

fn take_id_list(buf: &[u8], offset: &mut usize) -> Result<Vec<DatumId>> {
  let count = take_u32(buf, offset)? as usize;
  let mut ids = Vec::with_capacity(count);
  for _ in 0..count {
    ids.push(take_id(buf, offset)?);
  }
  Ok(ids)
}
```

Public codecs:

```rust
pub fn encode_lb_execute_op(op: &LbExecuteOp) -> Vec<u8> {
  let mut buf = Vec::new();
  match op {
    LbExecuteOp::Update {
      player_id,
      display,
      rank_key,
      friend_ids,
      top,
      window,
    } => {
      buf.push(LB_OP_UPDATE);
      put_id(&mut buf, *player_id);
      buf.extend_from_slice(rank_key);
      put_bytes(&mut buf, display);
      put_id_list(&mut buf, friend_ids);
      buf.extend_from_slice(&top.to_le_bytes());
      buf.extend_from_slice(&window.to_le_bytes());
    }
    LbExecuteOp::Remove { player_id } => {
      buf.push(LB_OP_REMOVE);
      put_id(&mut buf, *player_id);
    }
  }
  buf
}

pub fn decode_lb_execute_op(buf: &[u8]) -> Result<LbExecuteOp> {
  if buf.is_empty() {
    bail!("empty lb execute op");
  }
  let mut offset = 1;
  let op = match buf[0] {
    LB_OP_UPDATE => {
      let player_id = take_id(buf, &mut offset)?;
      let rank_key = take_rank_key(buf, &mut offset)?;
      let display = take_bytes(buf, &mut offset)?;
      let friend_ids = take_id_list(buf, &mut offset)?;
      let top = take_u32(buf, &mut offset)?;
      let window = take_u32(buf, &mut offset)?;
      LbExecuteOp::Update {
        player_id,
        display,
        rank_key,
        friend_ids,
        top,
        window,
      }
    }
    LB_OP_REMOVE => LbExecuteOp::Remove {
      player_id: take_id(buf, &mut offset)?,
    },
    tag => bail!("unknown lb execute op tag: {tag}"),
  };
  if offset != buf.len() {
    bail!("lb execute op has {} trailing bytes", buf.len() - offset);
  }
  Ok(op)
}

pub fn encode_lb_query_req(q: &LbQueryReq) -> Vec<u8> {
  let mut buf = Vec::new();
  buf.extend_from_slice(&q.top.to_le_bytes());
  buf.extend_from_slice(&q.bottom.to_le_bytes());
  match q.around_player {
    None => buf.push(0),
    Some(id) => {
      buf.push(1);
      put_id(&mut buf, id);
    }
  }
  buf.extend_from_slice(&q.window.to_le_bytes());
  put_id_list(&mut buf, &q.friend_ids);
  buf
}

pub fn decode_lb_query_req(buf: &[u8]) -> Result<LbQueryReq> {
  let mut offset = 0;
  let top = take_u32(buf, &mut offset)?;
  let bottom = take_u32(buf, &mut offset)?;
  if buf.len() < offset + 1 {
    bail!("lb query truncated at around_player flag");
  }
  let flag = buf[offset];
  offset += 1;
  let around_player = match flag {
    0 => None,
    1 => Some(take_id(buf, &mut offset)?),
    f => bail!("unknown around_player flag: {f}"),
  };
  let window = take_u32(buf, &mut offset)?;
  let friend_ids = take_id_list(buf, &mut offset)?;
  if offset != buf.len() {
    bail!("lb query has {} trailing bytes", buf.len() - offset);
  }
  Ok(LbQueryReq {
    top,
    bottom,
    around_player,
    window,
    friend_ids,
  })
}

fn put_lb_entries(buf: &mut Vec<u8>, entries: &[LbEntry]) {
  buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
  for entry in entries {
    buf.extend_from_slice(&entry.rank_key);
    put_id(buf, entry.player_id);
    put_bytes(buf, &entry.display);
  }
}

fn take_lb_entries(buf: &[u8], offset: &mut usize) -> Result<Vec<LbEntry>> {
  let count = take_u32(buf, offset)? as usize;
  let mut entries = Vec::with_capacity(count);
  for _ in 0..count {
    entries.push(LbEntry {
      rank_key: take_rank_key(buf, offset)?,
      player_id: take_id(buf, offset)?,
      display: take_bytes(buf, offset)?,
    });
  }
  Ok(entries)
}

pub fn encode_lb_result(result: &LbResult) -> Vec<u8> {
  let mut buf = result.total.to_le_bytes().to_vec();
  match result.player_rank {
    None => buf.push(0),
    Some(rank) => {
      buf.push(1);
      buf.extend_from_slice(&rank.to_le_bytes());
    }
  }
  put_lb_entries(&mut buf, &result.top);
  put_lb_entries(&mut buf, &result.bottom);
  put_lb_entries(&mut buf, &result.around);
  buf.extend_from_slice(&(result.friends.len() as u32).to_le_bytes());
  for friend in &result.friends {
    put_id(&mut buf, friend.player_id);
    buf.extend_from_slice(&friend.rank.to_le_bytes());
    buf.extend_from_slice(&friend.rank_key);
    put_bytes(&mut buf, &friend.display);
  }
  buf
}

pub fn decode_lb_result(buf: &[u8]) -> Result<LbResult> {
  let mut offset = 0;
  let total = take_u64(buf, &mut offset)?;
  if buf.len() < offset + 1 {
    bail!("lb result truncated at player_rank flag");
  }
  let flag = buf[offset];
  offset += 1;
  let player_rank = match flag {
    0 => None,
    1 => Some(take_u64(buf, &mut offset)?),
    f => bail!("unknown player_rank flag: {f}"),
  };
  let top = take_lb_entries(buf, &mut offset)?;
  let bottom = take_lb_entries(buf, &mut offset)?;
  let around = take_lb_entries(buf, &mut offset)?;
  let friend_count = take_u32(buf, &mut offset)? as usize;
  let mut friends = Vec::with_capacity(friend_count);
  for _ in 0..friend_count {
    friends.push(LbFriendRank {
      player_id: take_id(buf, &mut offset)?,
      rank: take_u64(buf, &mut offset)?,
      rank_key: take_rank_key(buf, &mut offset)?,
      display: take_bytes(buf, &mut offset)?,
    });
  }
  if offset != buf.len() {
    bail!("lb result has {} trailing bytes", buf.len() - offset);
  }
  Ok(LbResult {
    total,
    player_rank,
    top,
    bottom,
    around,
    friends,
  })
}
```

Request/Response wiring: `encode_request` arms push the opcode, the
16-byte `board_id`, a `put_bytes`-prefixed class string, then the
`encode_lb_execute_op`/`encode_lb_query_req` bytes (no length prefix —
they run to the end of the frame). Decode mirrors: id, class
(`take_bytes` + `String::from_utf8` with `.context("lb class was not
valid utf8")`), then hand `&buf[offset..]` to the op/query decoder.
`encode_response`/`decode_response` gain `RESP_LB_RESULT` arms
delegating to `encode_lb_result`/`decode_lb_result(&buf[1..])`.

`crates/seisin-node/src/pool.rs` `on_request` gains (next to the
`RkQuery` arm):

```rust
        // Client-only, never carried over a peer-link — same as Op.
        seisin_protocol::Request::LbExecute { .. } => return,
        seisin_protocol::Request::LbQuery { .. } => return,
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p seisin-protocol && cargo build --workspace`
Expected: all PASS, workspace builds (server.rs's `_ => return` catch-all absorbs the new variants until Task 7).

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-protocol/src/lib.rs crates/seisin-node/src/pool.rs
git commit -m "feat: add lb wire types and standalone codecs"
```

---

### Task 5: lb class declaration, board identity, rank-key decode (`seisin-types::lb`)

**Files:**
- Create: `crates/seisin-types/src/lb.rs`
- Modify: `crates/seisin-types/src/lib.rs` (add `pub mod lb;`)

**Interfaces:**
- Consumes: `rk_index::encode_rank_key` (same transforms; lb reuses them), `sk_index::derived_id_namespace` (already `pub(crate)`).
- Produces (used by Tasks 6, 8):

```rust
pub enum LbScoreType { I64, F64 }
pub enum LbRule { Max, Min, Replace }
pub struct LbClassDef {
  pub name: String,
  pub score_type: LbScoreType,
  pub display_len: u16,
  pub rule: LbRule,
}
pub fn lb_kind_name(class: &str) -> String;              // "lb:{class}"
pub fn lb_board_key(class: &str, leaderboard_id: &str, area_config_id: &str) -> DatumId;
pub fn encode_score(def: &LbClassDef, value: &FieldValue) -> Result<[u8; 8]>;
pub fn decode_rank_key(score_type: &LbScoreType, key: [u8; 8]) -> FieldValue;
```

`LbClassDef`/`LbScoreType`/`LbRule` derive `Debug, Clone, PartialEq`.

- [ ] **Step 1: Create the module with failing tests.** Full file:

```rust
//! lb (leaderboard) class declaration and identity: the per-class
//! definition (score type, display width, update rule), board datum-id
//! derivation, and the rank-key decode inverse. The resident-board
//! side (`LbIndexKind`) is `lb_kind.rs`. See the lb design doc.

use anyhow::{bail, Result};
use seisin_core::datum::DatumId;

use crate::field::FieldValue;
use crate::rk_index::encode_rank_key;
use crate::sk_index::derived_id_namespace;

#[derive(Debug, Clone, PartialEq)]
pub enum LbScoreType {
  I64,
  F64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LbRule {
  /// Keep the better (higher) score.
  Max,
  /// Keep the better (lower) score.
  Min,
  /// Last write wins.
  Replace,
}

/// One leaderboard class. Registered as registry kind `lb:{name}` —
/// one kind per class, because `IndexKind::open` only receives a
/// `DatumId` and must learn the rule/display width from the registered
/// kind itself.
#[derive(Debug, Clone, PartialEq)]
pub struct LbClassDef {
  pub name: String,
  pub score_type: LbScoreType,
  pub display_len: u16,
  pub rule: LbRule,
}

pub fn lb_kind_name(class: &str) -> String {
  format!("lb:{class}")
}

/// Board identity: class + leaderboard + area configuration, all
/// normalized into the datum id — never repeated per entry. A season
/// or reset is a new `leaderboard_id`.
pub fn lb_board_key(class: &str, leaderboard_id: &str, area_config_id: &str) -> DatumId {
  let name = format!("lb:{class}:{leaderboard_id}:{area_config_id}");
  DatumId::from_name(&derived_id_namespace(), name.as_bytes())
}

/// Encodes a score for `def`, rejecting a value that doesn't match the
/// class's declared score type (the rk transforms are shared — order-
/// preserving byte encodings for I64/F64).
pub fn encode_score(def: &LbClassDef, value: &FieldValue) -> Result<[u8; 8]> {
  match (&def.score_type, value) {
    (LbScoreType::I64, FieldValue::I64(_)) | (LbScoreType::F64, FieldValue::F64(_)) => {
      encode_rank_key(value)
    }
    (expected, got) => bail!(
      "lb class {:?} declares score type {:?} but got {:?}",
      def.name,
      expected,
      got
    ),
  }
}

/// The inverse of `encode_rank_key` — bijective, so clients can render
/// scores from wire-level rank keys.
pub fn decode_rank_key(score_type: &LbScoreType, key: [u8; 8]) -> FieldValue {
  let enc = u64::from_be_bytes(key);
  match score_type {
    LbScoreType::I64 => FieldValue::I64((enc ^ 0x8000_0000_0000_0000) as i64),
    LbScoreType::F64 => {
      let bits = if enc & 0x8000_0000_0000_0000 != 0 {
        enc ^ 0x8000_0000_0000_0000
      } else {
        !enc
      };
      FieldValue::F64(f64::from_bits(bits))
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn racing() -> LbClassDef {
    LbClassDef {
      name: "racing".to_string(),
      score_type: LbScoreType::I64,
      display_len: 32,
      rule: LbRule::Max,
    }
  }

  #[test]
  fn board_key_is_stable_and_distinguishes_every_component() {
    let a = lb_board_key("racing", "season1", "desert");
    assert_eq!(a, lb_board_key("racing", "season1", "desert"));
    assert_ne!(a, lb_board_key("racing", "season2", "desert"));
    assert_ne!(a, lb_board_key("racing", "season1", "ice"));
    assert_ne!(a, lb_board_key("arena", "season1", "desert"));
  }

  #[test]
  fn encode_score_rejects_a_type_mismatch() {
    assert!(encode_score(&racing(), &FieldValue::F64(1.5)).is_err());
    assert!(encode_score(&racing(), &FieldValue::I64(100)).is_ok());
  }

  #[test]
  fn i64_rank_keys_round_trip_through_decode() {
    let def = racing();
    for v in [i64::MIN, -300, -1, 0, 1, 300, i64::MAX] {
      let key = encode_score(&def, &FieldValue::I64(v)).unwrap();
      assert_eq!(decode_rank_key(&def.score_type, key), FieldValue::I64(v));
    }
  }

  #[test]
  fn f64_rank_keys_round_trip_through_decode_bitwise() {
    let ty = LbScoreType::F64;
    for v in [f64::NEG_INFINITY, -1.5, -0.0, 0.0, 1.5, f64::INFINITY, f64::NAN] {
      let key = encode_rank_key(&FieldValue::F64(v)).unwrap();
      match decode_rank_key(&ty, key) {
        FieldValue::F64(out) => assert_eq!(out.to_bits(), v.to_bits()),
        other => panic!("expected F64, got {other:?}"),
      }
    }
  }

  #[test]
  fn kind_name_prefixes_the_class() {
    assert_eq!(lb_kind_name("racing"), "lb:racing");
  }
}
```

- [ ] **Step 2: Run to verify compile failure** (module undeclared)

Run: `cargo test -p seisin-types lb::`
Expected: nothing runs until `pub mod lb;` exists.

- [ ] **Step 3: Add `pub mod lb;`** to `crates/seisin-types/src/lib.rs` (alphabetical, before `rk_index`).

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p seisin-types lb::`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-types
git commit -m "feat: add lb class declaration, board identity, and rank-key decode"
```

---

### Task 6: `LbIndexKind`/`LbResidentBoard` (`seisin-types::lb_kind`)

**Files:**
- Create: `crates/seisin-types/src/lb_kind.rs`
- Modify: `crates/seisin-types/src/lib.rs` (add `pub mod lb_kind;`)

**Interfaces:**
- Consumes: Tasks 1–5 (`execute` trait method, `rank_of_key`/`scan_from_rank`, lb codecs, `LbClassDef`).
- Produces: `pub struct LbIndexKind` (`LbIndexKind::new(def: LbClassDef, data_dir: PathBuf)`), `pub fn register_lb_class(registry: &mut IndexKindRegistry, def: LbClassDef, data_dir: PathBuf)`.

Key decisions locked here:
- B+Tree: `key_size = 24` (rank_key 8 ++ player_id 16), `value_size = 2 + display_len` (u16 LE actual length + display bytes zero-padded — a length prefix, not trailing-zero trimming, so displays with embedded/trailing zeros stay exact), `page_size = 4096`. Files: `lb_<board-id-hex>.btree`.
- Resident state: the tree handle (`RefCell`, same single-thread justification as rk) + `by_player: HashMap<DatumId, [u8; 8]>` rebuilt by a full `scan_from_rank(0, len)` on open.
- `apply` is rejected (lb is not on the framework-diff rail).
- Rule comparison uses raw rank-key byte order (valid because the encoding is order-preserving): Max replaces when `new_key > old_key`, Min when `new_key < old_key`, Replace always (a byte-equal key is a no-op for all rules — same score, nothing moves; display refresh under an equal score is deliberately not v1 behavior).

- [ ] **Step 1: Create the module with failing tests.** Full file:

```rust
//! The `"lb:{class}"` `IndexKind`: one counted-B+Tree board file per
//! (class, leaderboard, area-config) datum, resident with a
//! player->rank-key map. lb boards are primary data (scores under
//! max/min rules exist nowhere else) — `apply` (the framework-diff
//! rail) is rejected; all writes arrive as `execute` ops.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;

use seisin_core::datum::DatumId;
use seisin_node::index_handler::{IndexApplyOutcome, IndexKind, IndexKindRegistry, ResidentIndex};
use seisin_protocol::{
  decode_lb_execute_op, decode_lb_query_req, encode_lb_result, LbEntry, LbExecuteOp,
  LbFriendRank, LbQueryReq, LbResult,
};
use seisin_storage::btree::BPlusTree;

use crate::lb::{lb_kind_name, LbClassDef, LbRule};

const LB_PAGE_SIZE: u32 = 4096;

pub struct LbIndexKind {
  def: LbClassDef,
  data_dir: PathBuf,
}

impl LbIndexKind {
  pub fn new(def: LbClassDef, data_dir: PathBuf) -> Self {
    Self { def, data_dir }
  }
}

fn file_name_for(target: DatumId) -> String {
  let hex: String = target
    .as_bytes()
    .iter()
    .map(|b| format!("{b:02x}"))
    .collect();
  format!("lb_{hex}.btree")
}

fn composite_key(rank_key: &[u8; 8], player_id: DatumId) -> [u8; 24] {
  let mut key = [0u8; 24];
  key[0..8].copy_from_slice(rank_key);
  key[8..24].copy_from_slice(&player_id.as_bytes());
  key
}

/// Value layout: u16 LE actual display length ++ display bytes ++ zero
/// padding to the fixed width — a length prefix rather than trailing-
/// zero trimming, so displays round-trip exactly.
fn encode_display(display: &[u8], display_len: u16) -> Vec<u8> {
  let capped = &display[..display.len().min(display_len as usize)];
  let mut value = vec![0u8; 2 + display_len as usize];
  value[0..2].copy_from_slice(&(capped.len() as u16).to_le_bytes());
  value[2..2 + capped.len()].copy_from_slice(capped);
  value
}

fn decode_display(value: &[u8]) -> Vec<u8> {
  let len = u16::from_le_bytes(value[0..2].try_into().unwrap()) as usize;
  value[2..2 + len.min(value.len() - 2)].to_vec()
}

fn entry_from(key: &[u8], value: &[u8]) -> LbEntry {
  LbEntry {
    rank_key: key[0..8].try_into().unwrap(),
    player_id: DatumId::from_bytes(key[8..24].try_into().unwrap()),
    display: decode_display(value),
  }
}

pub struct LbResidentBoard {
  def: LbClassDef,
  tree: RefCell<BPlusTree>,
  by_player: HashMap<DatumId, [u8; 8]>,
}

impl LbResidentBoard {
  /// Best-first entries: the tree is ascending, so "top t" is a
  /// backward scan and rank conversions are `total - 1 - ascending`.
  fn assemble(
    &self,
    player_rank_of: Option<DatumId>,
    top: u32,
    bottom: u32,
    window: u32,
    friend_ids: &[DatumId],
  ) -> Result<LbResult, String> {
    let mut tree = self.tree.borrow_mut();
    let total = tree.len() as u64;
    let err = |e: anyhow::Error| e.to_string();

    let top_entries: Vec<LbEntry> = tree
      .scan_backward_bounded(top as usize)
      .map_err(err)?
      .iter()
      .map(|(k, v)| entry_from(k, v))
      .collect();
    let bottom_entries: Vec<LbEntry> = tree
      .scan_forward_bounded(bottom as usize)
      .map_err(err)?
      .iter()
      .map(|(k, v)| entry_from(k, v))
      .collect();

    let mut player_rank = None;
    let mut around = Vec::new();
    if let Some(player_id) = player_rank_of {
      if let Some(rank_key) = self.by_player.get(&player_id) {
        let asc = tree
          .rank_of_key(&composite_key(rank_key, player_id))
          .map_err(err)?
          .ok_or_else(|| "board map/tree divergence: mapped key missing".to_string())?;
        let best_rank = total - 1 - asc;
        player_rank = Some(best_rank);
        if window > 0 {
          // A best-order window centered on the player: compute the
          // ascending range, scan once, reverse to best-first.
          let half = (window / 2) as u64;
          let best_start = best_rank.saturating_sub(half);
          let best_end = (best_start + window as u64).min(total); // exclusive
          let best_start = best_end.saturating_sub(window as u64);
          let asc_start = total - best_end;
          let mut entries: Vec<LbEntry> = tree
            .scan_from_rank(asc_start, (best_end - best_start) as usize)
            .map_err(err)?
            .iter()
            .map(|(k, v)| entry_from(k, v))
            .collect();
          entries.reverse();
          around = entries;
        }
      }
    }

    let mut friends = Vec::new();
    for friend_id in friend_ids {
      let Some(rank_key) = self.by_player.get(friend_id) else {
        continue; // not on this board — omitted per the design doc
      };
      let key = composite_key(rank_key, *friend_id);
      let asc = tree
        .rank_of_key(&key)
        .map_err(err)?
        .ok_or_else(|| "board map/tree divergence: mapped key missing".to_string())?;
      let entry = tree
        .scan_from_rank(asc, 1)
        .map_err(err)?
        .into_iter()
        .next()
        .ok_or_else(|| "board map/tree divergence: rank scan empty".to_string())?;
      friends.push(LbFriendRank {
        player_id: *friend_id,
        rank: total - 1 - asc,
        rank_key: *rank_key,
        display: decode_display(&entry.1),
      });
    }

    Ok(LbResult {
      total,
      player_rank,
      top: top_entries,
      bottom: bottom_entries,
      around,
      friends,
    })
  }

  fn apply_rule(&self, old_key: &[u8; 8], new_key: &[u8; 8]) -> bool {
    // Raw byte comparison is valid: rank-key encoding is order-
    // preserving, so byte order == numeric order.
    match self.def.rule {
      LbRule::Max => new_key > old_key,
      LbRule::Min => new_key < old_key,
      LbRule::Replace => new_key != old_key,
    }
  }
}

impl ResidentIndex for LbResidentBoard {
  fn apply(&mut self, _payload: &[u8]) -> IndexApplyOutcome {
    IndexApplyOutcome {
      violation: Some(
        "lb boards are maintained via execute ops, not framework index updates".to_string(),
      ),
      write_through: None,
    }
  }

  fn query(&self, query: &[u8]) -> Result<Vec<u8>, String> {
    let LbQueryReq {
      top,
      bottom,
      around_player,
      window,
      friend_ids,
    } = decode_lb_query_req(query).map_err(|e| e.to_string())?;
    let result = self.assemble(around_player, top, bottom, window, &friend_ids)?;
    Ok(encode_lb_result(&result))
  }

  fn execute(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
    match decode_lb_execute_op(payload).map_err(|e| e.to_string())? {
      LbExecuteOp::Update {
        player_id,
        display,
        rank_key,
        friend_ids,
        top,
        window,
      } => {
        let replace = match self.by_player.get(&player_id) {
          None => true,
          Some(old_key) => self.apply_rule(old_key, &rank_key),
        };
        if replace {
          let mut tree = self.tree.borrow_mut();
          if let Some(old_key) = self.by_player.get(&player_id) {
            tree
              .remove(&composite_key(old_key, player_id))
              .map_err(|e| format!("lb remove of old entry failed: {e}"))?;
          }
          tree
            .insert(
              &composite_key(&rank_key, player_id),
              &encode_display(&display, self.def.display_len),
            )
            .map_err(|e| format!("lb insert failed: {e}"))?;
          drop(tree);
          self.by_player.insert(player_id, rank_key);
        }
        let result = self.assemble(Some(player_id), top, 0, window, &friend_ids)?;
        Ok(encode_lb_result(&result))
      }
      LbExecuteOp::Remove { player_id } => {
        if let Some(old_key) = self.by_player.remove(&player_id) {
          self
            .tree
            .borrow_mut()
            .remove(&composite_key(&old_key, player_id))
            .map_err(|e| format!("lb remove failed: {e}"))?;
        }
        let result = self.assemble(None, 0, 0, 0, &[])?;
        Ok(encode_lb_result(&result))
      }
    }
  }
}

impl IndexKind for LbIndexKind {
  /// `stored` is ignored: lb persists in its own page file. The
  /// player map is rebuilt from a full scan — derivable state, never
  /// persisted.
  fn open(
    &self,
    target: DatumId,
    _stored: Option<Vec<u8>>,
  ) -> Result<Box<dyn ResidentIndex>, String> {
    let path = self.data_dir.join(file_name_for(target));
    let value_size = 2 + self.def.display_len as u32;
    let mut tree = if path.exists() {
      BPlusTree::open(&path)
    } else {
      std::fs::create_dir_all(&self.data_dir)
        .map_err(|e| format!("failed to create lb data dir {:?}: {e}", self.data_dir))?;
      BPlusTree::create(&path, 24, value_size, LB_PAGE_SIZE)
    }
    .map_err(|e| format!("failed to open lb board file {path:?}: {e}"))?;
    let len = tree.len();
    let mut by_player = HashMap::with_capacity(len);
    for (key, _) in tree
      .scan_from_rank(0, len)
      .map_err(|e| format!("failed to scan lb board {path:?}: {e}"))?
    {
      let rank_key: [u8; 8] = key[0..8].try_into().unwrap();
      let player_id = DatumId::from_bytes(key[8..24].try_into().unwrap());
      by_player.insert(player_id, rank_key);
    }
    Ok(Box::new(LbResidentBoard {
      def: self.def.clone(),
      tree: RefCell::new(tree),
      by_player,
    }))
  }
}

/// Registers one leaderboard class under kind `lb:{name}` — call once
/// at the composition root per class.
pub fn register_lb_class(registry: &mut IndexKindRegistry, def: LbClassDef, data_dir: PathBuf) {
  let kind = lb_kind_name(&def.name);
  registry.register(kind, Box::new(LbIndexKind::new(def, data_dir)));
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::field::FieldValue;
  use crate::lb::{encode_score, LbScoreType};
  use seisin_protocol::{decode_lb_result, encode_lb_execute_op, encode_lb_query_req};

  fn racing(rule: LbRule) -> LbClassDef {
    LbClassDef {
      name: "racing".to_string(),
      score_type: LbScoreType::I64,
      display_len: 16,
      rule,
    }
  }

  fn open_board(dir: &std::path::Path, rule: LbRule) -> Box<dyn ResidentIndex> {
    LbIndexKind::new(racing(rule), dir.to_path_buf())
      .open(DatumId::new(), None)
      .unwrap()
  }

  fn update(
    board: &mut dyn ResidentIndex,
    player: DatumId,
    display: &str,
    score: i64,
    friends: Vec<DatumId>,
  ) -> LbResult {
    let rank_key = encode_score(&racing(LbRule::Max), &FieldValue::I64(score)).unwrap();
    let payload = encode_lb_execute_op(&LbExecuteOp::Update {
      player_id: player,
      display: display.as_bytes().to_vec(),
      rank_key,
      friend_ids: friends,
      top: 10,
      window: 3,
    });
    decode_lb_result(&board.execute(&payload).unwrap()).unwrap()
  }

  #[test]
  fn a_fresh_update_inserts_and_reports_rank_zero_of_one() {
    let dir = tempfile::tempdir().unwrap();
    let mut board = open_board(dir.path(), LbRule::Max);
    let alice = DatumId::new();
    let result = update(board.as_mut(), alice, "Alice", 100, vec![]);
    assert_eq!(result.total, 1);
    assert_eq!(result.player_rank, Some(0));
    assert_eq!(result.top.len(), 1);
    assert_eq!(result.top[0].player_id, alice);
    assert_eq!(result.top[0].display, b"Alice".to_vec());
  }

  #[test]
  fn max_rule_keeps_the_better_score_and_replaces_a_worse_one() {
    let dir = tempfile::tempdir().unwrap();
    let mut board = open_board(dir.path(), LbRule::Max);
    let alice = DatumId::new();
    update(board.as_mut(), alice, "Alice", 300, vec![]);
    // A worse score changes nothing.
    let result = update(board.as_mut(), alice, "Alice", 200, vec![]);
    assert_eq!(result.total, 1);
    let key = result.top[0].rank_key;
    assert_eq!(
      crate::lb::decode_rank_key(&LbScoreType::I64, key),
      FieldValue::I64(300)
    );
    // A better score replaces (and does not duplicate).
    let result = update(board.as_mut(), alice, "Alice", 400, vec![]);
    assert_eq!(result.total, 1);
    assert_eq!(
      crate::lb::decode_rank_key(&LbScoreType::I64, result.top[0].rank_key),
      FieldValue::I64(400)
    );
  }

  #[test]
  fn min_rule_inverts_and_replace_rule_always_wins() {
    let dir = tempfile::tempdir().unwrap();
    let mut board = open_board(dir.path(), LbRule::Min);
    let alice = DatumId::new();
    update(board.as_mut(), alice, "Alice", 300, vec![]);
    let result = update(board.as_mut(), alice, "Alice", 200, vec![]); // better for Min
    assert_eq!(
      crate::lb::decode_rank_key(&LbScoreType::I64, result.top[0].rank_key),
      FieldValue::I64(200)
    );

    let dir2 = tempfile::tempdir().unwrap();
    let mut board2 = open_board(dir2.path(), LbRule::Replace);
    let bob = DatumId::new();
    update(board2.as_mut(), bob, "Bob", 300, vec![]);
    let result = update(board2.as_mut(), bob, "Bob", 100, vec![]); // worse, still wins
    assert_eq!(
      crate::lb::decode_rank_key(&LbScoreType::I64, result.top[0].rank_key),
      FieldValue::I64(100)
    );
  }

  #[test]
  fn ranks_top_order_and_friend_ranks_are_best_first() {
    let dir = tempfile::tempdir().unwrap();
    let mut board = open_board(dir.path(), LbRule::Max);
    let (a, b, c) = (DatumId::new(), DatumId::new(), DatumId::new());
    update(board.as_mut(), a, "A", 100, vec![]);
    update(board.as_mut(), b, "B", 300, vec![]);
    let result = update(board.as_mut(), c, "C", 200, vec![a, b, DatumId::new()]);
    assert_eq!(result.total, 3);
    assert_eq!(result.player_rank, Some(1)); // c is second-best
    let top_ids: Vec<DatumId> = result.top.iter().map(|e| e.player_id).collect();
    assert_eq!(top_ids, vec![b, c, a]);
    // Friends: a at rank 2, b at rank 0; the unknown id omitted.
    assert_eq!(result.friends.len(), 2);
    let find = |id: DatumId| result.friends.iter().find(|f| f.player_id == id).unwrap();
    assert_eq!(find(a).rank, 2);
    assert_eq!(find(b).rank, 0);
    assert_eq!(find(b).display, b"B".to_vec());
  }

  #[test]
  fn around_window_centers_on_the_player_in_best_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut board = open_board(dir.path(), LbRule::Max);
    let players: Vec<DatumId> = (0..7).map(|_| DatumId::new()).collect();
    for (i, p) in players.iter().enumerate() {
      update(board.as_mut(), *p, &format!("P{i}"), (i as i64 + 1) * 10, vec![]);
    }
    // players[3] (score 40) has best-rank 3; window 3 => ranks 2,3,4.
    let result = update(board.as_mut(), players[3], "P3", 40, vec![]);
    assert_eq!(result.player_rank, Some(3));
    let around_ids: Vec<DatumId> = result.around.iter().map(|e| e.player_id).collect();
    assert_eq!(around_ids, vec![players[4], players[3], players[2]]);
  }

  #[test]
  fn remove_deletes_the_entry_and_query_reflects_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut board = open_board(dir.path(), LbRule::Max);
    let (a, b) = (DatumId::new(), DatumId::new());
    update(board.as_mut(), a, "A", 100, vec![]);
    update(board.as_mut(), b, "B", 200, vec![]);
    let payload = encode_lb_execute_op(&LbExecuteOp::Remove { player_id: a });
    let result = decode_lb_result(&board.execute(&payload).unwrap()).unwrap();
    assert_eq!(result.total, 1);

    let query = encode_lb_query_req(&LbQueryReq {
      top: 10,
      bottom: 10,
      around_player: Some(b),
      window: 1,
      friend_ids: vec![a],
    });
    let result = decode_lb_result(&board.query(&query).unwrap()).unwrap();
    assert_eq!(result.total, 1);
    assert_eq!(result.top.len(), 1);
    assert_eq!(result.bottom.len(), 1);
    assert_eq!(result.player_rank, Some(0));
    assert!(result.friends.is_empty()); // a is gone
  }

  #[test]
  fn display_is_truncated_to_the_class_width_and_round_trips_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let mut board = open_board(dir.path(), LbRule::Max);
    let alice = DatumId::new();
    let long = "AVeryLongDisplayNameIndeed"; // 26 bytes > display_len 16
    let result = update(board.as_mut(), alice, long, 100, vec![]);
    assert_eq!(result.top[0].display, long.as_bytes()[..16].to_vec());
  }

  #[test]
  fn cold_reopen_rebuilds_the_player_map_from_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let target = DatumId::new();
    let kind = LbIndexKind::new(racing(LbRule::Max), dir.path().to_path_buf());
    let alice = DatumId::new();
    {
      let mut board = kind.open(target, None).unwrap();
      update(board.as_mut(), alice, "Alice", 300, vec![]);
    }
    let mut board = kind.open(target, None).unwrap();
    // Map rebuilt: a worse score under Max is still rejected.
    let result = update(board.as_mut(), alice, "Alice", 100, vec![]);
    assert_eq!(result.total, 1);
    assert_eq!(
      crate::lb::decode_rank_key(&LbScoreType::I64, result.top[0].rank_key),
      FieldValue::I64(300)
    );
  }

  #[test]
  fn apply_is_rejected_and_malformed_execute_is_an_error_not_a_panic() {
    let dir = tempfile::tempdir().unwrap();
    let mut board = open_board(dir.path(), LbRule::Max);
    assert!(board.apply(b"anything").violation.is_some());
    assert!(board.execute(&[0xFF, 0xFF]).is_err());
  }
}
```

- [ ] **Step 2: Run to verify compile failure**, then **Step 3: add `pub mod lb_kind;`** to `lib.rs` and fix whatever the compiler surfaces (e.g. import paths).

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p seisin-types && cargo clippy -p seisin-types --all-targets -- -D warnings`
Expected: all PASS, clippy clean.

- [ ] **Step 5: Commit**

```bash
git add crates/seisin-types
git commit -m "feat: add LbIndexKind/LbResidentBoard with declared update rules"
```

---

### Task 7: server routing for `LbExecute`/`LbQuery`

**Files:**
- Modify: `crates/seisin-node/src/server.rs`

**Interfaces:**
- Consumes: Task 4's wire variants + codecs, Task 3's `run_index_execute`, existing `run_index_query`.
- Produces: both lb requests served/redirected on the client listener. Verified end-to-end by Task 8's integration test (server.rs has no unit-test scaffolding; this matches how `RkQuery`'s routing was proven).

- [ ] **Step 1: Factor the redirect check** shared by rk and lb (three call sites now justify it). Add to `server.rs`:

```rust
/// `Some(response)` if `datum_id` isn't native here (a redirect, or an
/// error if the native node's address is unknown); `None` when this
/// node should serve the request itself.
fn redirect_if_foreign(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  datum_id: DatumId,
) -> Option<Response> {
  let native_node = ring.read().unwrap().native(datum_id).0;
  if native_node == self_node_id {
    return None;
  }
  Some(match address_book.get(&native_node) {
    Some(address) => Response::Redirect {
      address: address.clone(),
    },
    None => Response::OpError {
      message: format!("no known address for node {native_node:?}"),
    },
  })
}
```

and rewrite `handle_rk_query`'s redirect block to use it:

```rust
  if let Some(response) = redirect_if_foreign(self_node_id, ring, address_book, index_datum_id) {
    return response;
  }
```

- [ ] **Step 2: Add the lb handlers and match arms.** New arms in `handle_connection`'s match, before the `_ => return` catch-all:

```rust
      Request::LbExecute {
        board_id,
        class,
        op,
      } => handle_lb_execute(self_node_id, &ring, &address_book, &pool, board_id, class, op),
      Request::LbQuery {
        board_id,
        class,
        query,
      } => handle_lb_query(self_node_id, &ring, &address_book, &pool, board_id, class, query),
```

Handlers (the `class` field exists only to form the registry kind
string `lb:{class}` — this file stays semantics-agnostic):

```rust
fn handle_lb_execute(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  pool: &WorkerPool,
  board_id: DatumId,
  class: String,
  op: seisin_protocol::LbExecuteOp,
) -> Response {
  if let Some(response) = redirect_if_foreign(self_node_id, ring, address_book, board_id) {
    return response;
  }
  let payload = seisin_protocol::encode_lb_execute_op(&op);
  lb_result_response(pool.run_index_execute(board_id, format!("lb:{class}"), payload))
}

fn handle_lb_query(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  pool: &WorkerPool,
  board_id: DatumId,
  class: String,
  query: seisin_protocol::LbQueryReq,
) -> Response {
  if let Some(response) = redirect_if_foreign(self_node_id, ring, address_book, board_id) {
    return response;
  }
  let payload = seisin_protocol::encode_lb_query_req(&query);
  lb_result_response(pool.run_index_query(board_id, format!("lb:{class}"), payload))
}

fn lb_result_response(result: Result<Vec<u8>, String>) -> Response {
  match result {
    Ok(bytes) => match seisin_protocol::decode_lb_result(&bytes) {
      Ok(result) => Response::LbResult(result),
      Err(e) => Response::OpError {
        message: format!("malformed lb result: {e}"),
      },
    },
    Err(message) => Response::OpError { message },
  }
}
```

- [ ] **Step 3: Verify build + full suite**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: all PASS, clippy clean.

- [ ] **Step 4: Commit**

```bash
git add crates/seisin-node/src/server.rs
git commit -m "feat: route client LbExecute/LbQuery requests with shared redirect check"
```

---

### Task 8: end-to-end integration test, stress, docs

**Files:**
- Create: `crates/seisin-types/tests/integration_lb_boards.rs`
- Modify: `docs/superpowers/PROGRESS.md`

- [ ] **Step 1: Write the integration test** (bootstrap copied from `integration_rk_leaderboard.rs`; two boards prove independence; everything through the real wire):

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
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{LbExecuteOp, LbQueryReq, LbResult, Request, Response};
use seisin_ring::ring::Ring;
use seisin_types::field::FieldValue;
use seisin_types::lb::{encode_score, lb_board_key, LbClassDef, LbRule, LbScoreType};
use seisin_types::lb_kind::register_lb_class;

fn racing_class() -> LbClassDef {
  LbClassDef {
    name: "racing".to_string(),
    score_type: LbScoreType::I64,
    display_len: 32,
    rule: LbRule::Max,
  }
}

fn start_node(data_dir: std::path::PathBuf) -> String {
  let mut index_kinds = IndexKindRegistry::new();
  register_lb_class(&mut index_kinds, racing_class(), data_dir);

  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  let node_id = NodeId(1);
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_id, 2)])));
  let peer_link_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(OpRegistry::new()),
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

fn submit(
  addr: &str,
  board_id: DatumId,
  player: DatumId,
  display: &str,
  score: i64,
  friends: Vec<DatumId>,
) -> LbResult {
  let rank_key = encode_score(&racing_class(), &FieldValue::I64(score)).unwrap();
  let response = seisin_client::call(
    addr,
    Request::LbExecute {
      board_id,
      class: "racing".to_string(),
      op: LbExecuteOp::Update {
        player_id: player,
        display: display.as_bytes().to_vec(),
        rank_key,
        friend_ids: friends,
        top: 5,
        window: 3,
      },
    },
  )
  .unwrap();
  match response {
    Response::LbResult(result) => result,
    other => panic!("expected LbResult, got {other:?}"),
  }
}

fn query(addr: &str, board_id: DatumId, bottom: u32) -> LbResult {
  let response = seisin_client::call(
    addr,
    Request::LbQuery {
      board_id,
      class: "racing".to_string(),
      query: LbQueryReq {
        top: 5,
        bottom,
        around_player: None,
        window: 0,
        friend_ids: vec![],
      },
    },
  )
  .unwrap();
  match response {
    Response::LbResult(result) => result,
    other => panic!("expected LbResult, got {other:?}"),
  }
}

#[test]
fn boards_update_query_and_stay_independent_over_the_wire() {
  let data_dir = tempfile::tempdir().unwrap();
  let addr = start_node(data_dir.path().to_path_buf());
  let desert = lb_board_key("racing", "season1", "desert");
  let ice = lb_board_key("racing", "season1", "ice");

  let (alice, bob, carol) = (DatumId::new(), DatumId::new(), DatumId::new());

  submit(&addr, desert, alice, "Alice", 100, vec![]);
  submit(&addr, desert, bob, "Bob", 300, vec![]);
  let result = submit(&addr, desert, carol, "Carol", 200, vec![alice, bob]);

  assert_eq!(result.total, 3);
  assert_eq!(result.player_rank, Some(1));
  let top: Vec<&[u8]> = result.top.iter().map(|e| e.display.as_slice()).collect();
  assert_eq!(top, vec![b"Bob".as_slice(), b"Carol".as_slice(), b"Alice".as_slice()]);
  assert_eq!(result.friends.len(), 2);

  // Max rule over the wire: a worse score changes nothing.
  let result = submit(&addr, desert, bob, "Bob", 50, vec![]);
  assert_eq!(result.player_rank, Some(0));
  assert_eq!(result.total, 3);

  // Same players, different area config: an independent board.
  let result = submit(&addr, ice, alice, "Alice", 999, vec![bob]);
  assert_eq!(result.total, 1);
  assert_eq!(result.player_rank, Some(0));
  assert!(result.friends.is_empty()); // bob has no ice score

  // Read-only query with a bottom list.
  let result = query(&addr, desert, 2);
  assert_eq!(result.total, 3);
  assert_eq!(result.bottom.len(), 2);
  assert_eq!(result.bottom[0].display, b"Alice".to_vec()); // worst first

  // Removal over the wire.
  let response = seisin_client::call(
    &addr,
    Request::LbExecute {
      board_id: desert,
      class: "racing".to_string(),
      op: LbExecuteOp::Remove { player_id: alice },
    },
  )
  .unwrap();
  match response {
    Response::LbResult(result) => assert_eq!(result.total, 2),
    other => panic!("expected LbResult, got {other:?}"),
  }
}
```

- [ ] **Step 2: Run it**

Run: `cargo test -p seisin-types --test integration_lb_boards`
Expected: PASS. Debug via superpowers:systematic-debugging if not.

- [ ] **Step 3: Stress**

```bash
for i in $(seq 1 10); do cargo test -q -p seisin-types --test integration_lb_boards || break; done
for i in $(seq 1 20); do cargo test -q -p seisin-node --test integration_wound_wait --test integration_cross_node_wound_wait --test integration_op_collation || break; done
```

Expected: zero failures.

- [ ] **Step 4: Full gates**

Run: `cargo test --workspace && cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 5: Update `docs/superpowers/PROGRESS.md`** — a "Done" entry for the lb datum class following the established format (what shipped, bugs found during execution if any, new test count), and note the `execute` rail now exists for tk (Part 4) to reuse.

- [ ] **Step 6: Commit and push**

```bash
git add -A
git commit -m "feat: lb leaderboard datum class end-to-end"
git push
```

---

## Deliberately Out of Scope (from the spec — do not build)

- Elo or any rule needing opponent context; tie-policy upgrades; bottom lists in the update response; board wipe op; push/change notification; variable-length display (TOAST overflow — a named Storage Tier requirement).
