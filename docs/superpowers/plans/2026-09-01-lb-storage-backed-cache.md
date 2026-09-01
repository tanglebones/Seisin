# lb Storage-Backed Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: this repo's CLAUDE.md
> requires inline execution, never subagent dispatch — use
> superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `lb` leaderboard board data out of a compute node's local
disk and into the storage tier (replicated, durable), replacing the
fully-resident `BPlusTree` with a bounded compute-side cache (pinned
top/bottom windows + LRU) that queries storage for scans/samples/point
lookups it can't answer locally.

**Architecture:** A new content-agnostic "ordered collection" primitive
on the store wire protocol (`Create`/`Insert`/`Remove`/`Get`/
`ScanForward`/`ScanBackward`/`Sample`/`RankOfKey`/`ScanFromRank`/
`Count`), backed storage-side by the existing `BPlusTree` engine
(already has every one of these operations — no new tree logic, just
wiring). Writes replicate as logical ops fanned out to all N replicas
(not byte diffs). `lb` gets two collections per board (`rank`,
`by_player`) and a `CollectionStore` trait injected into `IndexKind`s
after `ClusterState` exists (mirrors how `WorkerPool` already takes an
injected `Arc<dyn Store>`).

**Tech Stack:** Rust, existing `seisin-storage::btree::BPlusTree`,
existing store-wire framing (`seisin-protocol::store_wire`).

**Spec:** `docs/superpowers/specs/2026-09-01-lb-storage-backed-cache-design.md`

## Global Constraints

- `cargo fmt --all --check` and `cargo clippy --workspace --all-targets
  -- -D warnings` clean, full `cargo test --workspace` green — after
  every task, not just at the end.
- 2-space indent (`rustfmt.toml`'s `tab_spaces = 2`).
- `anyhow::Result` + `bail!()`/`.context()` for new code that returns
  `Result` to its own crate's callers; the existing `Store`/wire-level
  APIs in this codebase return plain values or `String` errors (fail-
  stop internally) — match whichever pattern the surrounding code
  already uses, don't introduce a third style.
- `seisin-node/src/worker.rs` is not touched by this plan, but the
  standing 20× stress loop over `integration_wound_wait`,
  `integration_cross_node_wound_wait`, and `integration_op_collation`
  runs anyway in the final task, per house policy for anything touching
  the compute/storage dispatch path.
- Commit per task, directly on `main`, trailer `Co-Authored-By: Claude
  Sonnet 5 <noreply@anthropic.com>` (plus the session-link trailer if
  your commit tooling already adds one).
- `STORE_PROTOCOL_VERSION` bumps 3 → 4 (pre-first-release: drop the old
  decoder, no n±1 compatibility burden).

---

### Task 1: Store wire protocol — the ordered-collection primitive

**Files:**
- Modify: `crates/seisin-protocol/src/store_wire.rs`

**Interfaces:**
- Produces: `StoreRequest::{CollectionCreate, CollectionInsert,
  CollectionRemove, CollectionGet, CollectionScanForward,
  CollectionScanBackward, CollectionSample, CollectionRankOfKey,
  CollectionScanFromRank, CollectionCount}`, `StoreResponse::{
  CollectionEntry, CollectionEntries, CollectionRank, CollectionCount}`,
  `encode_store_request`/`decode_store_request`/`encode_store_response`/
  `decode_store_response` handling all of them, `STORE_PROTOCOL_VERSION
  = 4`.

- [ ] **Step 1: Add the new request/response variants and bump the version**

Add to the `StoreRequest` enum (after `Retire`, before `Identify` — or
anywhere in the enum; exact position doesn't matter):

```rust
  /// Creates the (collection_id) ordered collection if it doesn't
  /// already exist — idempotent, so replica catch-up and repeat calls
  /// are both safe. `key_size`/`value_size` are fixed for the
  /// collection's lifetime, matching `BPlusTree::create`.
  CollectionCreate {
    collection_id: DatumId,
    key_size: u32,
    value_size: u32,
  },
  CollectionInsert {
    collection_id: DatumId,
    key: Vec<u8>,
    value: Vec<u8>,
  },
  CollectionRemove {
    collection_id: DatumId,
    key: Vec<u8>,
  },
  CollectionGet {
    collection_id: DatumId,
    key: Vec<u8>,
  },
  /// Best-first bounded scan (ascending key order, from the end) —
  /// mirrors `BPlusTree::scan_backward_bounded`.
  CollectionScanForward {
    collection_id: DatumId,
    limit: u32,
  },
  /// Worst-first bounded scan — mirrors `BPlusTree::scan_forward_bounded`.
  CollectionScanBackward {
    collection_id: DatumId,
    limit: u32,
  },
  CollectionSample {
    collection_id: DatumId,
    k: u32,
  },
  CollectionRankOfKey {
    collection_id: DatumId,
    key: Vec<u8>,
  },
  CollectionScanFromRank {
    collection_id: DatumId,
    rank: u64,
    limit: u32,
  },
  CollectionCount {
    collection_id: DatumId,
  },
```

Add to `StoreResponse`:

```rust
  CollectionEntry {
    value: Option<Vec<u8>>,
  },
  CollectionEntries {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
  },
  CollectionRank {
    rank: Option<u64>,
  },
  CollectionCount {
    total: u64,
  },
```

Bump: `pub const STORE_PROTOCOL_VERSION: u8 = 4;` and extend its doc
comment: `// Bumped to 4 for the ordered-collection primitive (lb
storage-backed cache design) — new request/response kinds, no change
to existing ones.`

Add new tag constants after the existing `REQ_IDENTIFY`/`RESP_ERROR`
block:

```rust
const REQ_COLLECTION_CREATE: u8 = 11;
const REQ_COLLECTION_INSERT: u8 = 12;
const REQ_COLLECTION_REMOVE: u8 = 13;
const REQ_COLLECTION_GET: u8 = 14;
const REQ_COLLECTION_SCAN_FORWARD: u8 = 15;
const REQ_COLLECTION_SCAN_BACKWARD: u8 = 16;
const REQ_COLLECTION_SAMPLE: u8 = 17;
const REQ_COLLECTION_RANK_OF_KEY: u8 = 18;
const REQ_COLLECTION_SCAN_FROM_RANK: u8 = 19;
const REQ_COLLECTION_COUNT: u8 = 20;

const RESP_COLLECTION_ENTRY: u8 = 8;
const RESP_COLLECTION_ENTRIES: u8 = 9;
const RESP_COLLECTION_RANK: u8 = 10;
const RESP_COLLECTION_COUNT: u8 = 11;
```

- [ ] **Step 2: Encode the new requests**

In `encode_store_request`'s `match`, add:

```rust
    StoreRequest::CollectionCreate {
      collection_id,
      key_size,
      value_size,
    } => {
      buf.push(REQ_COLLECTION_CREATE);
      put_id(&mut buf, *collection_id);
      buf.extend_from_slice(&key_size.to_le_bytes());
      buf.extend_from_slice(&value_size.to_le_bytes());
    }
    StoreRequest::CollectionInsert {
      collection_id,
      key,
      value,
    } => {
      buf.push(REQ_COLLECTION_INSERT);
      put_id(&mut buf, *collection_id);
      put_bytes(&mut buf, key);
      put_bytes(&mut buf, value);
    }
    StoreRequest::CollectionRemove { collection_id, key } => {
      buf.push(REQ_COLLECTION_REMOVE);
      put_id(&mut buf, *collection_id);
      put_bytes(&mut buf, key);
    }
    StoreRequest::CollectionGet { collection_id, key } => {
      buf.push(REQ_COLLECTION_GET);
      put_id(&mut buf, *collection_id);
      put_bytes(&mut buf, key);
    }
    StoreRequest::CollectionScanForward { collection_id, limit } => {
      buf.push(REQ_COLLECTION_SCAN_FORWARD);
      put_id(&mut buf, *collection_id);
      buf.extend_from_slice(&limit.to_le_bytes());
    }
    StoreRequest::CollectionScanBackward { collection_id, limit } => {
      buf.push(REQ_COLLECTION_SCAN_BACKWARD);
      put_id(&mut buf, *collection_id);
      buf.extend_from_slice(&limit.to_le_bytes());
    }
    StoreRequest::CollectionSample { collection_id, k } => {
      buf.push(REQ_COLLECTION_SAMPLE);
      put_id(&mut buf, *collection_id);
      buf.extend_from_slice(&k.to_le_bytes());
    }
    StoreRequest::CollectionRankOfKey { collection_id, key } => {
      buf.push(REQ_COLLECTION_RANK_OF_KEY);
      put_id(&mut buf, *collection_id);
      put_bytes(&mut buf, key);
    }
    StoreRequest::CollectionScanFromRank {
      collection_id,
      rank,
      limit,
    } => {
      buf.push(REQ_COLLECTION_SCAN_FROM_RANK);
      put_id(&mut buf, *collection_id);
      buf.extend_from_slice(&rank.to_le_bytes());
      buf.extend_from_slice(&limit.to_le_bytes());
    }
    StoreRequest::CollectionCount { collection_id } => {
      buf.push(REQ_COLLECTION_COUNT);
      put_id(&mut buf, *collection_id);
    }
```

- [ ] **Step 3: Decode the new requests**

Find `decode_store_request` (the `match tag` after `check_version`) and
add matching arms, reading fields back in the same order they were
written:

```rust
    REQ_COLLECTION_CREATE => {
      let collection_id = take_id(buf, &mut offset)?;
      let key_size = take_u32(buf, &mut offset)?;
      let value_size = take_u32(buf, &mut offset)?;
      Ok(StoreRequest::CollectionCreate {
        collection_id,
        key_size,
        value_size,
      })
    }
    REQ_COLLECTION_INSERT => {
      let collection_id = take_id(buf, &mut offset)?;
      let key = take_bytes(buf, &mut offset)?;
      let value = take_bytes(buf, &mut offset)?;
      Ok(StoreRequest::CollectionInsert {
        collection_id,
        key,
        value,
      })
    }
    REQ_COLLECTION_REMOVE => {
      let collection_id = take_id(buf, &mut offset)?;
      let key = take_bytes(buf, &mut offset)?;
      Ok(StoreRequest::CollectionRemove { collection_id, key })
    }
    REQ_COLLECTION_GET => {
      let collection_id = take_id(buf, &mut offset)?;
      let key = take_bytes(buf, &mut offset)?;
      Ok(StoreRequest::CollectionGet { collection_id, key })
    }
    REQ_COLLECTION_SCAN_FORWARD => {
      let collection_id = take_id(buf, &mut offset)?;
      let limit = take_u32(buf, &mut offset)?;
      Ok(StoreRequest::CollectionScanForward { collection_id, limit })
    }
    REQ_COLLECTION_SCAN_BACKWARD => {
      let collection_id = take_id(buf, &mut offset)?;
      let limit = take_u32(buf, &mut offset)?;
      Ok(StoreRequest::CollectionScanBackward { collection_id, limit })
    }
    REQ_COLLECTION_SAMPLE => {
      let collection_id = take_id(buf, &mut offset)?;
      let k = take_u32(buf, &mut offset)?;
      Ok(StoreRequest::CollectionSample { collection_id, k })
    }
    REQ_COLLECTION_RANK_OF_KEY => {
      let collection_id = take_id(buf, &mut offset)?;
      let key = take_bytes(buf, &mut offset)?;
      Ok(StoreRequest::CollectionRankOfKey { collection_id, key })
    }
    REQ_COLLECTION_SCAN_FROM_RANK => {
      let collection_id = take_id(buf, &mut offset)?;
      let rank = take_u64(buf, &mut offset)?;
      let limit = take_u32(buf, &mut offset)?;
      Ok(StoreRequest::CollectionScanFromRank {
        collection_id,
        rank,
        limit,
      })
    }
    REQ_COLLECTION_COUNT => {
      let collection_id = take_id(buf, &mut offset)?;
      Ok(StoreRequest::CollectionCount { collection_id })
    }
```

(Check the existing `decode_store_request` body for whether it uses a
running `offset` cursor already initialized right after
`check_version` — it does, per the `take_*` helpers' signatures; reuse
that same `offset` variable, don't shadow it.)

- [ ] **Step 4: Encode/decode the new responses**

In `encode_store_response`:

```rust
    StoreResponse::CollectionEntry { value } => {
      buf.push(RESP_COLLECTION_ENTRY);
      match value {
        None => buf.push(0),
        Some(v) => {
          buf.push(1);
          put_bytes(&mut buf, v);
        }
      }
    }
    StoreResponse::CollectionEntries { entries } => {
      buf.push(RESP_COLLECTION_ENTRIES);
      buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
      for (key, value) in entries {
        put_bytes(&mut buf, key);
        put_bytes(&mut buf, value);
      }
    }
    StoreResponse::CollectionRank { rank } => {
      buf.push(RESP_COLLECTION_RANK);
      match rank {
        None => buf.push(0),
        Some(r) => {
          buf.push(1);
          buf.extend_from_slice(&r.to_le_bytes());
        }
      }
    }
    StoreResponse::CollectionCount { total } => {
      buf.push(RESP_COLLECTION_COUNT);
      buf.extend_from_slice(&total.to_le_bytes());
    }
```

In `decode_store_response`:

```rust
    RESP_COLLECTION_ENTRY => {
      let tag = *buf.get(offset).ok_or_else(|| anyhow::anyhow!("truncated CollectionEntry tag"))?;
      offset += 1;
      let value = if tag == 1 {
        Some(take_bytes(buf, &mut offset)?)
      } else {
        None
      };
      Ok(StoreResponse::CollectionEntry { value })
    }
    RESP_COLLECTION_ENTRIES => {
      let count = take_u32(buf, &mut offset)? as usize;
      let mut entries = Vec::with_capacity(count);
      for _ in 0..count {
        let key = take_bytes(buf, &mut offset)?;
        let value = take_bytes(buf, &mut offset)?;
        entries.push((key, value));
      }
      Ok(StoreResponse::CollectionEntries { entries })
    }
    RESP_COLLECTION_RANK => {
      let tag = *buf.get(offset).ok_or_else(|| anyhow::anyhow!("truncated CollectionRank tag"))?;
      offset += 1;
      let rank = if tag == 1 {
        Some(take_u64(buf, &mut offset)?)
      } else {
        None
      };
      Ok(StoreResponse::CollectionRank { rank })
    }
    RESP_COLLECTION_COUNT => {
      let total = take_u64(buf, &mut offset)?;
      Ok(StoreResponse::CollectionCount { total })
    }
```

(Match the exact `offset`-cursor idiom already used by the surrounding
`decode_store_response` arms — inspect the file for whether `offset` is
declared once at the top of the function, same as
`decode_store_request`.)

- [ ] **Step 5: Round-trip tests**

Add to the file's existing `#[cfg(test)] mod tests` block (find it at
the bottom of `store_wire.rs`, follow its existing naming/assert
style):

```rust
  #[test]
  fn collection_requests_round_trip() {
    let id = DatumId::new();
    let cases = vec![
      StoreRequest::CollectionCreate {
        collection_id: id,
        key_size: 24,
        value_size: 34,
      },
      StoreRequest::CollectionInsert {
        collection_id: id,
        key: vec![1, 2, 3],
        value: vec![4, 5],
      },
      StoreRequest::CollectionRemove {
        collection_id: id,
        key: vec![1, 2, 3],
      },
      StoreRequest::CollectionGet {
        collection_id: id,
        key: vec![9],
      },
      StoreRequest::CollectionScanForward {
        collection_id: id,
        limit: 10,
      },
      StoreRequest::CollectionScanBackward {
        collection_id: id,
        limit: 5,
      },
      StoreRequest::CollectionSample { collection_id: id, k: 3 },
      StoreRequest::CollectionRankOfKey {
        collection_id: id,
        key: vec![7, 7],
      },
      StoreRequest::CollectionScanFromRank {
        collection_id: id,
        rank: 42,
        limit: 6,
      },
      StoreRequest::CollectionCount { collection_id: id },
    ];
    for req in cases {
      let encoded = encode_store_request(&req);
      assert_eq!(decode_store_request(&encoded).unwrap(), req);
    }
  }

  #[test]
  fn collection_responses_round_trip() {
    let cases = vec![
      StoreResponse::CollectionEntry { value: None },
      StoreResponse::CollectionEntry {
        value: Some(vec![1, 2, 3]),
      },
      StoreResponse::CollectionEntries {
        entries: vec![(vec![1], vec![2]), (vec![3], vec![4])],
      },
      StoreResponse::CollectionRank { rank: None },
      StoreResponse::CollectionRank { rank: Some(9) },
      StoreResponse::CollectionCount { total: 123 },
    ];
    for resp in cases {
      let encoded = encode_store_response(&resp);
      assert_eq!(decode_store_response(&encoded).unwrap(), resp);
    }
  }
```

- [ ] **Step 6: Run the tests**

Run: `cargo test -p seisin-protocol`
Expected: PASS, including the two new tests.

- [ ] **Step 7: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy -p seisin-protocol --all-targets -- -D warnings
git add crates/seisin-protocol/src/store_wire.rs
git commit -m "$(cat <<'EOF'
feat: ordered-collection store-wire primitive (lb storage backing, part 1)

New CollectionCreate/Insert/Remove/Get/ScanForward/ScanBackward/Sample/
RankOfKey/ScanFromRank/Count request/response kinds on the compute<->
storage protocol, wrapping BPlusTree's existing operations. Bumps
STORE_PROTOCOL_VERSION to 4 (pre-first-release, old decoder dropped).

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Extract `ReplicaResolver` (shared by `RemoteStore` and the new `RemoteCollectionStore`)

**Files:**
- Create: `crates/seisin-node/src/replica_resolver.rs`
- Modify: `crates/seisin-node/src/remote_store.rs`
- Modify: `crates/seisin-node/src/lib.rs` (module declaration)

**Interfaces:**
- Produces: `pub(crate) struct ReplicaResolver` with `pub(crate) fn
  new(cluster: Arc<ClusterState>) -> Self`, `pub(crate) fn
  serving_replicas(&self, id: DatumId, n: u16) -> Vec<NodeId>`,
  `pub(crate) fn mark_stale(&self, node: NodeId)`, `pub(crate) fn
  halt_total_loss(&self, id: DatumId) -> !`.
- Consumes: `crate::gossip_state::ClusterState` (existing).

This is a pure extraction — `RemoteStore`'s existing behavior and its
existing tests must be unchanged after this task. `RemoteCollectionStore`
(Task 4) reuses it instead of duplicating the same three methods.

- [ ] **Step 1: Create the extracted module**

```rust
//! The replica-selection/failure-bookkeeping logic shared by every
//! networked store built on the storage ring (`RemoteStore` for blob
//! datums, `RemoteCollectionStore` for ordered collections) — pulled
//! out once both needed the identical "resolve serving replicas, mark
//! one stale, halt on total loss" behavior rather than duplicating it.

use std::sync::Arc;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;

use crate::gossip_state::ClusterState;

pub(crate) struct ReplicaResolver {
  cluster: Arc<ClusterState>,
}

impl ReplicaResolver {
  pub(crate) fn new(cluster: Arc<ClusterState>) -> Self {
    Self { cluster }
  }

  /// The id's replica set restricted to nodes that can actually serve
  /// it right now — in the ring, alive, and not stale — in rank order
  /// (rank 0, the primary, first).
  pub(crate) fn serving_replicas(&self, id: DatumId, n: u16) -> Vec<NodeId> {
    let replicas = self
      .cluster
      .storage_ring
      .read()
      .unwrap()
      .replicas(id, n as usize);
    let alive = self.cluster.storage_alive.read().unwrap();
    let stale = self.cluster.storage_stale.read().unwrap();
    replicas
      .into_iter()
      .filter(|node| alive.contains(node) && !stale.contains(node))
      .collect()
  }

  /// Excludes `node` from future serving until a driver re-replication
  /// re-admits it — used when a call to it fails mid-operation.
  pub(crate) fn mark_stale(&self, node: NodeId) {
    self.cluster.storage_stale.write().unwrap().insert(node);
  }

  /// Engages the coordinated whole-cluster halt for an id whose every
  /// replica is gone, then fail-stops this worker.
  pub(crate) fn halt_total_loss(&self, id: DatumId) -> ! {
    let reason =
      format!("cluster halted: every replica of {id:?} is unreachable — total shard loss");
    self.cluster.halt.halt(reason.clone());
    panic!("{reason}");
  }
}
```

- [ ] **Step 2: Declare the module**

In `crates/seisin-node/src/lib.rs`, add `pub(crate) mod
replica_resolver;` alongside the other `pub mod`/`mod` declarations
(match whichever visibility the file already uses for `remote_store` —
if `remote_store` is `pub mod`, use `mod replica_resolver;` since
nothing outside the crate needs it).

- [ ] **Step 3: Point `RemoteStore` at it**

In `crates/seisin-node/src/remote_store.rs`:
- Replace the `pub struct RemoteStore { cluster: Arc<ClusterState> }`
  field with `resolver: crate::replica_resolver::ReplicaResolver`.
- In `RemoteStore::new`, build it as `Self { resolver:
  crate::replica_resolver::ReplicaResolver::new(cluster) }`.
- Delete `RemoteStore`'s own `serving_replicas`/`mark_stale`/
  `halt_total_loss` methods.
- Update every call site in this file (`self.serving_replicas(...)` →
  `self.resolver.serving_replicas(...)`, same for the other two) — grep
  the file for `self.serving_replicas`, `self.mark_stale`,
  `self.halt_total_loss` to find them all.

- [ ] **Step 4: Run the existing RemoteStore tests to confirm no regression**

Run: `cargo test -p seisin-node remote_store`
Expected: PASS, identical results to before this task (behavior is
unchanged — this is a pure refactor).

- [ ] **Step 5: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy -p seisin-node --all-targets -- -D warnings
git add crates/seisin-node/src/replica_resolver.rs crates/seisin-node/src/remote_store.rs crates/seisin-node/src/lib.rs
git commit -m "$(cat <<'EOF'
refactor: extract ReplicaResolver from RemoteStore

Pulls the replica-selection/stale-marking/total-loss-halt logic into
its own module so the upcoming RemoteCollectionStore can reuse it
instead of duplicating it. No behavior change.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Storage-side collection dispatch

**Files:**
- Modify: `crates/seisin-node/src/store_server.rs`
- Modify: `crates/seisin-node/src/node.rs`

**Interfaces:**
- Produces: `StoreNode` gains `data_dir: PathBuf` and `collections:
  Mutex<HashMap<DatumId, BPlusTree>>`; `handle_connection` answers every
  `StoreRequest::Collection*` variant from Task 1.
- Consumes: `seisin_storage::btree::BPlusTree::{open, create, insert,
  remove, scan_forward_bounded, scan_backward_bounded, sample_by_rank,
  rank_of_key, scan_from_rank, len}` (all already exist, unmodified).

- [ ] **Step 1: Add fields to `StoreNode` and a collection-file helper**

In `crates/seisin-node/src/store_server.rs`, add to the `StoreNode`
struct (after the existing `transfers` field):

```rust
  /// Where this storage node's own collection files live — same
  /// directory the log lives under, one level up from wherever
  /// `datum_log.dlog` sits (the caller passes it explicitly; see
  /// `node::run`'s storage-role branch).
  pub data_dir: std::path::PathBuf,
  /// Resident collection files, opened lazily on first request and
  /// kept open for the process's lifetime — mirrors how the compute
  /// side already keeps `lb`'s tree resident per thread, just here
  /// it's per storage node instead.
  pub collections: Mutex<std::collections::HashMap<DatumId, BPlusTree>>,
```

Add the import: `use seisin_storage::btree::BPlusTree;` and `use
std::collections::HashMap;` (check the file's existing imports first —
`HashMap` may already be imported).

Add a private helper near the top of the file, next to `read_value`:

```rust
const COLLECTION_PAGE_SIZE: u32 = 4096;

fn collection_file_name(collection_id: DatumId) -> String {
  let hex: String = collection_id
    .as_bytes()
    .iter()
    .map(|b| format!("{b:02x}"))
    .collect();
  format!("collection_{hex}.btree")
}

/// Opens (or creates, if `key_size`/`value_size` are given and the file
/// doesn't exist yet) `collection_id`'s tree, inserting it into
/// `node.collections` if it wasn't already resident. `create` is `None`
/// for every request except `CollectionCreate` — a request against a
/// collection that was never created and isn't found on disk is an
/// error, not a silent auto-create, except for `CollectionCreate`
/// itself (which is the one idempotent "make it exist" entry point).
fn with_collection<T>(
  node: &StoreNode,
  collection_id: DatumId,
  create: Option<(u32, u32)>,
  f: impl FnOnce(&mut BPlusTree) -> Result<T, String>,
) -> Result<T, String> {
  let mut collections = node.collections.lock().unwrap();
  if !collections.contains_key(&collection_id) {
    let path = node.data_dir.join(collection_file_name(collection_id));
    let tree = if path.exists() {
      BPlusTree::open(&path)
    } else {
      match create {
        Some((key_size, value_size)) => {
          std::fs::create_dir_all(&node.data_dir)
            .map_err(|e| format!("failed to create data dir {:?}: {e}", node.data_dir))?;
          BPlusTree::create(&path, key_size, value_size, COLLECTION_PAGE_SIZE)
        }
        None => return Err(format!("collection {collection_id:?} does not exist")),
      }
    }
    .map_err(|e| format!("failed to open collection file {path:?}: {e}"))?;
    collections.insert(collection_id, tree);
  }
  f(collections.get_mut(&collection_id).unwrap())
}
```

- [ ] **Step 2: Dispatch the new requests in `handle_connection`**

Add arms to the `match request { ... }` block, after the existing
`StoreRequest::Retire { .. } => { ... }` arm:

```rust
      StoreRequest::CollectionCreate {
        collection_id,
        key_size,
        value_size,
      } => match with_collection(&node, collection_id, Some((key_size, value_size)), |_| Ok(())) {
        Ok(()) => StoreResponse::Ack,
        Err(message) => StoreResponse::Error { message },
      },
      StoreRequest::CollectionInsert {
        collection_id,
        key,
        value,
      } => match with_collection(&node, collection_id, None, |tree| {
        tree.insert(&key, &value).map_err(|e| e.to_string())
      }) {
        Ok(()) => StoreResponse::Ack,
        Err(message) => StoreResponse::Error { message },
      },
      StoreRequest::CollectionRemove { collection_id, key } => {
        match with_collection(&node, collection_id, None, |tree| {
          tree.remove(&key).map_err(|e| e.to_string())
        }) {
          Ok(_) => StoreResponse::Ack,
          Err(message) => StoreResponse::Error { message },
        }
      }
      StoreRequest::CollectionGet { collection_id, key } => {
        match with_collection(&node, collection_id, None, |tree| {
          tree
            .scan_from_rank(0, tree.len())
            .map_err(|e| e.to_string())
            .map(|entries| entries.into_iter().find(|(k, _)| k == &key).map(|(_, v)| v))
        }) {
          Ok(value) => StoreResponse::CollectionEntry { value },
          Err(message) => StoreResponse::Error { message },
        }
      }
      StoreRequest::CollectionScanForward { collection_id, limit } => {
        match with_collection(&node, collection_id, None, |tree| {
          tree.scan_backward_bounded(limit as usize).map_err(|e| e.to_string())
        }) {
          Ok(entries) => StoreResponse::CollectionEntries { entries },
          Err(message) => StoreResponse::Error { message },
        }
      }
      StoreRequest::CollectionScanBackward { collection_id, limit } => {
        match with_collection(&node, collection_id, None, |tree| {
          tree.scan_forward_bounded(limit as usize).map_err(|e| e.to_string())
        }) {
          Ok(entries) => StoreResponse::CollectionEntries { entries },
          Err(message) => StoreResponse::Error { message },
        }
      }
      StoreRequest::CollectionSample { collection_id, k } => {
        match with_collection(&node, collection_id, None, |tree| {
          tree.sample_by_rank(k as usize).map_err(|e| e.to_string())
        }) {
          Ok(entries) => StoreResponse::CollectionEntries { entries },
          Err(message) => StoreResponse::Error { message },
        }
      }
      StoreRequest::CollectionRankOfKey { collection_id, key } => {
        match with_collection(&node, collection_id, None, |tree| {
          tree.rank_of_key(&key).map_err(|e| e.to_string())
        }) {
          Ok(rank) => StoreResponse::CollectionRank { rank },
          Err(message) => StoreResponse::Error { message },
        }
      }
      StoreRequest::CollectionScanFromRank {
        collection_id,
        rank,
        limit,
      } => match with_collection(&node, collection_id, None, |tree| {
        tree.scan_from_rank(rank, limit as usize).map_err(|e| e.to_string())
      }) {
        Ok(entries) => StoreResponse::CollectionEntries { entries },
        Err(message) => StoreResponse::Error { message },
      },
      StoreRequest::CollectionCount { collection_id } => {
        match with_collection(&node, collection_id, None, |tree| Ok(tree.len() as u64)) {
          Ok(total) => StoreResponse::CollectionCount { total },
          Err(message) => StoreResponse::Error { message },
        }
      }
```

Note on `CollectionGet`: `BPlusTree` has no direct point-`get` method
(only `rank_of_key` + scan) — implemented above via a full
`scan_from_rank(0, tree.len())` plus a linear find. This is correct but
O(n) per point lookup; acceptable for now (`by_player` collections are
small — one entry per player on one board — and this is flagged, not
silently shipped). **If this shows up as a real bottleneck later, add a
proper `BPlusTree::get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>`
using `rank_of_key` + a single-entry `scan_from_rank`** — out of scope
for this task, but leave a `// PERF:` comment at the `CollectionGet` arm
pointing at this note.

- [ ] **Step 3: Wire `data_dir` through `node::run`'s storage-role branch**

In `crates/seisin-node/src/node.rs`, find the storage-role branch (the
`if self_member.role == NodeRole::Storage { ... }` block) and the
`StoreNode { ... }` construction inside it. Add `data_dir:
std::path::PathBuf::from(&config.data_dir), collections:
Mutex::new(std::collections::HashMap::new()),` to the struct literal.
(Check what's already imported at the top of `node.rs` — `Mutex` is
likely already in scope since `log` is already an `Arc<Mutex<...>>`.)

- [ ] **Step 4: A real end-to-end test**

Add to `store_server.rs`'s existing `#[cfg(test)] mod tests` (follow
its existing pattern — it already spins up a real `StoreNode` +
`TcpListener` + `serve_store` thread and calls `store_call`; find that
setup helper and reuse it, adding the new `data_dir`/`collections`
fields to whatever constructs `StoreNode` in tests):

```rust
  #[test]
  fn collection_create_insert_scan_and_rank_round_trip_over_the_wire() {
    let dir = tempfile::tempdir().unwrap();
    let addr = start_test_store_node(dir.path().to_path_buf()); // existing test helper — adapt name if different
    let collection_id = DatumId::new();

    assert_eq!(
      store_call(
        &addr,
        &StoreRequest::CollectionCreate {
          collection_id,
          key_size: 4,
          value_size: 4,
        },
      )
      .unwrap(),
      StoreResponse::Ack
    );

    for i in 0u32..5 {
      let bytes = i.to_be_bytes().to_vec(); // big-endian: byte order == numeric order, so scans come out sorted by i
      assert_eq!(
        store_call(
          &addr,
          &StoreRequest::CollectionInsert {
            collection_id,
            key: bytes.clone(),
            value: bytes,
          },
        )
        .unwrap(),
        StoreResponse::Ack
      );
    }

    match store_call(&addr, &StoreRequest::CollectionCount { collection_id }).unwrap() {
      StoreResponse::CollectionCount { total } => assert_eq!(total, 5),
      other => panic!("expected CollectionCount, got {other:?}"),
    }

    match store_call(
      &addr,
      &StoreRequest::CollectionScanForward {
        collection_id,
        limit: 2,
      },
    )
    .unwrap()
    {
      StoreResponse::CollectionEntries { entries } => {
        let keys: Vec<u32> = entries
          .iter()
          .map(|(k, _)| u32::from_be_bytes(k.clone().try_into().unwrap()))
          .collect();
        assert_eq!(keys, vec![4, 3]); // best-first: 4 is the highest key
      }
      other => panic!("expected CollectionEntries, got {other:?}"),
    }

    match store_call(
      &addr,
      &StoreRequest::CollectionRankOfKey {
        collection_id,
        key: 2u32.to_be_bytes().to_vec(),
      },
    )
    .unwrap()
    {
      StoreResponse::CollectionRank { rank } => assert_eq!(rank, Some(2)), // ascending rank: 0,1,2,3,4
      other => panic!("expected CollectionRank, got {other:?}"),
    }

    assert_eq!(
      store_call(
        &addr,
        &StoreRequest::CollectionRemove {
          collection_id,
          key: 2u32.to_be_bytes().to_vec(),
        },
      )
      .unwrap(),
      StoreResponse::Ack
    );
    match store_call(&addr, &StoreRequest::CollectionCount { collection_id }).unwrap() {
      StoreResponse::CollectionCount { total } => assert_eq!(total, 4),
      other => panic!("expected CollectionCount, got {other:?}"),
    }
  }
```

(If the file's existing tests construct `StoreNode` and start
`serve_store` inline rather than via a named helper, inline the same
setup here instead of calling a nonexistent `start_test_store_node` —
match whatever the file actually does; the point is: real `StoreNode`,
real socket, real `BPlusTree` file on a tempdir.)

- [ ] **Step 5: Run the tests**

Run: `cargo test -p seisin-node store_server`
Expected: PASS.

- [ ] **Step 6: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy -p seisin-node --all-targets -- -D warnings
git add crates/seisin-node/src/store_server.rs crates/seisin-node/src/node.rs
git commit -m "$(cat <<'EOF'
feat: storage-side dispatch for the ordered-collection primitive

StoreNode now hosts BPlusTree-backed collection files alongside the
datum log, answering the Collection* store-wire requests by calling
straight into BPlusTree's existing insert/remove/scan/sample/rank ops.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: `CollectionStore` trait + `RemoteCollectionStore`

**Files:**
- Create: `crates/seisin-node/src/collection_store.rs`
- Modify: `crates/seisin-node/src/lib.rs`

**Interfaces:**
- Consumes: `crate::replica_resolver::ReplicaResolver` (Task 2),
  `seisin_protocol::store_wire::{StoreRequest, StoreResponse,
  encode_store_request, decode_store_response}`, `crate::gossip_state::
  ClusterState`.
- Produces: `pub trait CollectionStore: Send + Sync` with methods
  `create`, `insert`, `remove`, `get`, `scan_forward`, `scan_backward`,
  `sample`, `rank_of_key`, `scan_from_rank`, `count` (exact signatures
  in Step 1 — `seisin-types` (Task 6) depends on these exactly), and
  `pub struct RemoteCollectionStore` implementing it.

- [ ] **Step 1: The trait**

```rust
//! `CollectionStore`: the compute-side interface to the storage tier's
//! ordered-collection primitive (Task 1/3) — `RemoteCollectionStore` is
//! the real networked implementation; a solution's `IndexKind`s (lb,
//! and later rk/tk) depend on this trait, not on `RemoteCollectionStore`
//! directly, the same way compute code already depends on `Store`
//! rather than `RemoteStore`.

use seisin_core::datum::DatumId;

pub trait CollectionStore: Send + Sync {
  /// Idempotent: creates the collection if it doesn't already exist.
  fn create(&self, collection_id: DatumId, key_size: u32, value_size: u32, n: u16);
  fn insert(&self, collection_id: DatumId, key: Vec<u8>, value: Vec<u8>, n: u16);
  fn remove(&self, collection_id: DatumId, key: Vec<u8>, n: u16);
  fn get(&self, collection_id: DatumId, key: Vec<u8>, n: u16) -> Option<Vec<u8>>;
  /// Best-first bounded scan.
  fn scan_forward(&self, collection_id: DatumId, limit: u32, n: u16) -> Vec<(Vec<u8>, Vec<u8>)>;
  /// Worst-first bounded scan.
  fn scan_backward(&self, collection_id: DatumId, limit: u32, n: u16) -> Vec<(Vec<u8>, Vec<u8>)>;
  fn sample(&self, collection_id: DatumId, k: u32, n: u16) -> Vec<(Vec<u8>, Vec<u8>)>;
  /// Ascending rank (0 = worst) of `key`, if present.
  fn rank_of_key(&self, collection_id: DatumId, key: Vec<u8>, n: u16) -> Option<u64>;
  fn scan_from_rank(
    &self,
    collection_id: DatumId,
    rank: u64,
    limit: u32,
    n: u16,
  ) -> Vec<(Vec<u8>, Vec<u8>)>;
  fn count(&self, collection_id: DatumId, n: u16) -> u64;
}
```

- [ ] **Step 2: `RemoteCollectionStore`, mirroring `RemoteStore`'s connection/failover shape**

```rust
use std::cell::RefCell;
use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::Arc;

use seisin_core::authority::NodeId;
use seisin_protocol::store_wire::{
  decode_store_response, encode_store_request, StoreRequest, StoreResponse,
};
use seisin_protocol::{read_frame, write_frame};

use crate::gossip_state::ClusterState;
use crate::replica_resolver::ReplicaResolver;

pub struct RemoteCollectionStore {
  resolver: ReplicaResolver,
}

thread_local! {
  static CONNECTIONS: RefCell<HashMap<u64, TcpStream>> = RefCell::new(HashMap::new());
}

impl RemoteCollectionStore {
  pub fn new(cluster: Arc<ClusterState>) -> Self {
    Self {
      resolver: ReplicaResolver::new(cluster),
    }
  }

  /// One request/response round trip on this thread's connection to
  /// `node`'s store address, reconnecting once on an IO error.
  fn try_call(&self, node: NodeId, address: &str, request: &StoreRequest) -> Result<StoreResponse, String> {
    let encoded = encode_store_request(request);
    for attempt in 0..2 {
      let result = CONNECTIONS.with(|conns| {
        let mut conns = conns.borrow_mut();
        let stream = match conns.entry(node.0) {
          std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
          std::collections::hash_map::Entry::Vacant(v) => match TcpStream::connect(address) {
            Ok(stream) => v.insert(stream),
            Err(e) => return Err(e.to_string()),
          },
        };
        if let Err(e) = write_frame(stream, &encoded) {
          conns.remove(&node.0);
          return Err(e.to_string());
        }
        match read_frame(stream) {
          Ok(payload) => Ok(payload),
          Err(e) => {
            conns.remove(&node.0);
            Err(e.to_string())
          }
        }
      });
      match result {
        Ok(payload) => return decode_store_response(&payload).map_err(|e| e.to_string()),
        Err(_) if attempt == 0 => continue,
        Err(e) => return Err(e),
      }
    }
    unreachable!("both attempts return")
  }

  fn address_of(&self, node: NodeId, cluster: &Arc<ClusterState>) -> Result<String, String> {
    cluster
      .store_addresses
      .read()
      .unwrap()
      .get(&node)
      .cloned()
      .ok_or_else(|| format!("no store address configured for storage node {node:?}"))
  }

  /// Sends `request` to every serving replica of `collection_id`; a
  /// node that fails is marked stale; total failure fail-stops. Used
  /// for `create`/`insert`/`remove` (write ops — logical-op
  /// replication, not byte diffs, per the design doc).
  fn write_all(&self, cluster: &Arc<ClusterState>, collection_id: DatumId, n: u16, request: &StoreRequest) {
    let targets = self.resolver.serving_replicas(collection_id, n);
    if targets.is_empty() {
      self.resolver.halt_total_loss(collection_id);
    }
    let mut acked = 0;
    for node in targets {
      let address = match self.address_of(node, cluster) {
        Ok(a) => a,
        Err(_) => {
          self.resolver.mark_stale(node);
          continue;
        }
      };
      match self.try_call(node, &address, request) {
        Ok(StoreResponse::Ack) => acked += 1,
        _ => self.resolver.mark_stale(node),
      }
    }
    if acked == 0 {
      self.resolver.halt_total_loss(collection_id);
    }
  }

  /// Reads from the primary replica, failing over to the next on error
  /// — mirrors `RemoteStore::get`'s failover shape.
  fn read_one(
    &self,
    cluster: &Arc<ClusterState>,
    collection_id: DatumId,
    n: u16,
    request: &StoreRequest,
  ) -> StoreResponse {
    let targets = self.resolver.serving_replicas(collection_id, n);
    if targets.is_empty() {
      self.resolver.halt_total_loss(collection_id);
    }
    for node in &targets {
      let address = match self.address_of(*node, cluster) {
        Ok(a) => a,
        Err(_) => {
          self.resolver.mark_stale(*node);
          continue;
        }
      };
      match self.try_call(*node, &address, request) {
        Ok(response) => return response,
        Err(_) => self.resolver.mark_stale(*node),
      }
    }
    self.resolver.halt_total_loss(collection_id);
  }
}
```

`ReplicaResolver` (Task 2) doesn't expose the `Arc<ClusterState>` it
wraps, but `RemoteCollectionStore` needs it directly for
`store_addresses` lookups. Add a `pub(crate) fn cluster(&self) ->
&Arc<ClusterState>` accessor to `ReplicaResolver` in
`replica_resolver.rs` as part of this task's Step 1 (small addition,
not a Task 2 revision — Task 2 already landed and is tested; this is
new, additive surface on it), and use `self.resolver.cluster()` in
place of the `cluster: &Arc<ClusterState>` parameters threaded through
`write_all`/`read_one`/`address_of` above (drop those parameters,
they're redundant once the accessor exists — simplify the three
methods to take just what's still needed).

- [ ] **Step 3: Implement the trait**

```rust
impl CollectionStore for RemoteCollectionStore {
  fn create(&self, collection_id: DatumId, key_size: u32, value_size: u32, n: u16) {
    self.write_all(
      collection_id,
      n,
      &StoreRequest::CollectionCreate {
        collection_id,
        key_size,
        value_size,
      },
    );
  }

  fn insert(&self, collection_id: DatumId, key: Vec<u8>, value: Vec<u8>, n: u16) {
    self.write_all(
      collection_id,
      n,
      &StoreRequest::CollectionInsert {
        collection_id,
        key,
        value,
      },
    );
  }

  fn remove(&self, collection_id: DatumId, key: Vec<u8>, n: u16) {
    self.write_all(collection_id, n, &StoreRequest::CollectionRemove { collection_id, key });
  }

  fn get(&self, collection_id: DatumId, key: Vec<u8>, n: u16) -> Option<Vec<u8>> {
    match self.read_one(collection_id, n, &StoreRequest::CollectionGet { collection_id, key }) {
      StoreResponse::CollectionEntry { value } => value,
      other => panic!("unexpected reply to CollectionGet: {other:?}"),
    }
  }

  fn scan_forward(&self, collection_id: DatumId, limit: u32, n: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
    match self.read_one(
      collection_id,
      n,
      &StoreRequest::CollectionScanForward { collection_id, limit },
    ) {
      StoreResponse::CollectionEntries { entries } => entries,
      other => panic!("unexpected reply to CollectionScanForward: {other:?}"),
    }
  }

  fn scan_backward(&self, collection_id: DatumId, limit: u32, n: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
    match self.read_one(
      collection_id,
      n,
      &StoreRequest::CollectionScanBackward { collection_id, limit },
    ) {
      StoreResponse::CollectionEntries { entries } => entries,
      other => panic!("unexpected reply to CollectionScanBackward: {other:?}"),
    }
  }

  fn sample(&self, collection_id: DatumId, k: u32, n: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
    match self.read_one(collection_id, n, &StoreRequest::CollectionSample { collection_id, k }) {
      StoreResponse::CollectionEntries { entries } => entries,
      other => panic!("unexpected reply to CollectionSample: {other:?}"),
    }
  }

  fn rank_of_key(&self, collection_id: DatumId, key: Vec<u8>, n: u16) -> Option<u64> {
    match self.read_one(collection_id, n, &StoreRequest::CollectionRankOfKey { collection_id, key }) {
      StoreResponse::CollectionRank { rank } => rank,
      other => panic!("unexpected reply to CollectionRankOfKey: {other:?}"),
    }
  }

  fn scan_from_rank(
    &self,
    collection_id: DatumId,
    rank: u64,
    limit: u32,
    n: u16,
  ) -> Vec<(Vec<u8>, Vec<u8>)> {
    match self.read_one(
      collection_id,
      n,
      &StoreRequest::CollectionScanFromRank {
        collection_id,
        rank,
        limit,
      },
    ) {
      StoreResponse::CollectionEntries { entries } => entries,
      other => panic!("unexpected reply to CollectionScanFromRank: {other:?}"),
    }
  }

  fn count(&self, collection_id: DatumId, n: u16) -> u64 {
    match self.read_one(collection_id, n, &StoreRequest::CollectionCount { collection_id }) {
      StoreResponse::CollectionCount { total } => total,
      other => panic!("unexpected reply to CollectionCount: {other:?}"),
    }
  }
}
```

(Adjust `write_all`/`read_one` call sites above once Step 2's
`cluster()`-accessor simplification lands — they take `(collection_id,
n, request)`, not `(cluster, collection_id, n, request)`.)

- [ ] **Step 4: Declare the module**

In `crates/seisin-node/src/lib.rs`, add `pub mod collection_store;`
(public — `seisin-types` needs `CollectionStore` and
`RemoteCollectionStore`).

- [ ] **Step 5: A failover test**

Add `#[cfg(test)] mod tests` to `collection_store.rs`, mirroring
whatever pattern `remote_store.rs`'s own tests already use for
"spin up two real storage nodes, kill one mid-test, confirm the other
still serves and the dead one gets marked stale" (read that file's
existing failover test first, then write this one against the same
harness shape — two real `StoreNode`s + `serve_store` threads +
`ClusterState` with both in the storage ring, `n: 2`):

```rust
  #[test]
  fn insert_survives_one_replica_down_and_marks_it_stale() {
    // Set up two real storage nodes (data dirs under tempfile::tempdir()),
    // a ClusterState with both in the storage ring at n=2, and a
    // RemoteCollectionStore over it — same setup shape as
    // remote_store.rs's own replica-failover test.
    // ...
    // let collection_id = DatumId::new();
    // store.create(collection_id, 4, 4, 2);
    // drop the second node's listener / stop its thread to simulate death
    // store.insert(collection_id, vec![1, 2, 3, 4], vec![5, 6, 7, 8], 2);
    // assert the surviving node has the entry (via a direct StoreRequest::CollectionGet call)
    // assert the dead node is now in cluster.storage_stale
  }
```

Write this test by copying `remote_store.rs`'s existing "one replica
down" test's setup verbatim (same helper functions for spinning up
`StoreNode`s/`ClusterState`, if any exist in that file or a shared test
module) and swapping the `Store`/`RemoteStore` calls for
`CollectionStore`/`RemoteCollectionStore` ones — don't invent a new
harness shape.

- [ ] **Step 6: Run the tests**

Run: `cargo test -p seisin-node collection_store`
Expected: PASS.

- [ ] **Step 7: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy -p seisin-node --all-targets -- -D warnings
git add crates/seisin-node/src/collection_store.rs crates/seisin-node/src/lib.rs crates/seisin-node/src/replica_resolver.rs
git commit -m "$(cat <<'EOF'
feat: CollectionStore trait + RemoteCollectionStore

The compute-side client of the ordered-collection primitive: fans
create/insert/remove out to every serving replica (logical-op
replication), reads the primary with failover — same policy RemoteStore
already applies to blob datums, via the shared ReplicaResolver.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Inject `CollectionStore` into `IndexKind`s

**Files:**
- Modify: `crates/seisin-node/src/index_handler.rs`
- Modify: `crates/seisin-node/src/node.rs`

**Interfaces:**
- Produces: `IndexKind::attach_collection_store(&self, store:
  Arc<dyn CollectionStore>)` (default no-op), `IndexKindRegistry::
  attach_collection_store(&self, store: Arc<dyn CollectionStore>)`.
- Consumes: `crate::collection_store::CollectionStore` (Task 4).

This is the composition-root wiring that lets `register_lb_class`
(called by a solution's binary *before* `node::run` builds
`ClusterState`) end up with a working `CollectionStore` anyway —
`node::run` attaches it after `ClusterState` exists, before the
`WorkerPool` (and therefore any client traffic) starts.

- [ ] **Step 1: Extend the trait and registry**

In `crates/seisin-node/src/index_handler.rs`, add to the `IndexKind`
trait (after `open`):

```rust
  /// Injects the storage-tier collection client, for kinds whose
  /// resident structure is storage-backed (lb; later rk/tk). Called
  /// once, after `ClusterState` exists and before any client traffic —
  /// see `node::run`. Kinds with no storage-backed state (sk) keep the
  /// default no-op.
  fn attach_collection_store(&self, store: std::sync::Arc<dyn crate::collection_store::CollectionStore>) {
    let _ = store;
  }
```

Add to `IndexKindRegistry`'s `impl` block:

```rust
  /// Calls `attach_collection_store` on every registered kind.
  pub fn attach_collection_store(&self, store: std::sync::Arc<dyn crate::collection_store::CollectionStore>) {
    for kind in self.kinds.values() {
      kind.attach_collection_store(std::sync::Arc::clone(&store));
    }
  }
```

- [ ] **Step 2: Wire it in `node::run`**

In `crates/seisin-node/src/node.rs`'s compute-role path, find where
`cluster` (the `Arc<ClusterState>`) is constructed and where
`WorkerPool::spawn` is called (the `let pool = Arc::new(WorkerPool::
spawn(...))` line). Immediately before that `WorkerPool::spawn` call,
insert:

```rust
  index_kinds.attach_collection_store(Arc::new(crate::collection_store::RemoteCollectionStore::new(
    Arc::clone(&cluster),
  )));
```

(`index_kinds` at this point is still the plain `IndexKindRegistry`
value passed into `node::run` — it gets wrapped in `Arc::new(index_kinds)`
only at the `WorkerPool::spawn(...)` call site itself, so this call
must land on the line right before that wrap, using the registry by
value/reference before it's moved.)

- [ ] **Step 3: A registry-level test**

Add to `index_handler.rs`'s existing `#[cfg(test)] mod tests` (which
already has `AppendKind`/`AppendResident` fixtures — add a tiny second
fixture that records whether `attach_collection_store` was called):

```rust
  struct RecordingKind {
    attached: std::sync::Arc<std::sync::atomic::AtomicBool>,
  }

  impl ResidentIndex for AppendResident {
    // (reuse the existing AppendResident impl already in this file — no change)
  }

  impl IndexKind for RecordingKind {
    fn open(&self, _target: DatumId, stored: Option<Vec<u8>>) -> Result<Box<dyn ResidentIndex>, String> {
      Ok(Box::new(AppendResident {
        bytes: stored.unwrap_or_default(),
      }))
    }
    fn attach_collection_store(&self, _store: std::sync::Arc<dyn crate::collection_store::CollectionStore>) {
      self.attached.store(true, std::sync::atomic::Ordering::SeqCst);
    }
  }

  struct NoopCollectionStore;
  impl crate::collection_store::CollectionStore for NoopCollectionStore {
    fn create(&self, _: DatumId, _: u32, _: u32, _: u16) {}
    fn insert(&self, _: DatumId, _: Vec<u8>, _: Vec<u8>, _: u16) {}
    fn remove(&self, _: DatumId, _: Vec<u8>, _: u16) {}
    fn get(&self, _: DatumId, _: Vec<u8>, _: u16) -> Option<Vec<u8>> {
      None
    }
    fn scan_forward(&self, _: DatumId, _: u32, _: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
      vec![]
    }
    fn scan_backward(&self, _: DatumId, _: u32, _: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
      vec![]
    }
    fn sample(&self, _: DatumId, _: u32, _: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
      vec![]
    }
    fn rank_of_key(&self, _: DatumId, _: Vec<u8>, _: u16) -> Option<u64> {
      None
    }
    fn scan_from_rank(&self, _: DatumId, _: u64, _: u32, _: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
      vec![]
    }
    fn count(&self, _: DatumId, _: u16) -> u64 {
      0
    }
  }

  #[test]
  fn attach_collection_store_reaches_every_registered_kind() {
    let attached = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut registry = IndexKindRegistry::new();
    registry.register(
      "recording",
      Box::new(RecordingKind {
        attached: std::sync::Arc::clone(&attached),
      }),
    );
    registry.attach_collection_store(std::sync::Arc::new(NoopCollectionStore));
    assert!(attached.load(std::sync::atomic::Ordering::SeqCst));
  }
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p seisin-node index_handler`
Expected: PASS.

- [ ] **Step 5: Full workspace build check** (this task touches
`node.rs`, worth a full build even though `worker.rs` itself isn't
modified)

Run: `cargo build --workspace`
Expected: builds clean — this confirms nothing downstream of
`node::run`'s signature/behavior broke.

- [ ] **Step 6: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
git add crates/seisin-node/src/index_handler.rs crates/seisin-node/src/node.rs
git commit -m "$(cat <<'EOF'
feat: inject CollectionStore into IndexKinds at composition-root time

IndexKind gains attach_collection_store (default no-op); node::run
builds a RemoteCollectionStore once ClusterState exists and attaches it
to every registered kind before starting the WorkerPool — lets
register_lb_class (called before node::run) end up with a working
storage client despite running before ClusterState is built.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: `LbCacheConfig` + `LbCache` scaffold (open/create, no read/write logic yet)

**Files:**
- Create: `crates/seisin-types/src/lb_cache.rs`
- Modify: `crates/seisin-types/src/lb_kind.rs`
- Modify: `crates/seisin-types/src/lib.rs` (module declaration)

**Interfaces:**
- Produces: `pub struct LbCacheConfig { pub pinned_top: usize, pub
  pinned_bottom: usize, pub max_cached_entries: usize }`, `pub struct
  LbCache` (fields below), `pub fn register_lb_class(registry: &mut
  IndexKindRegistry, def: LbClassDef, data_dir: PathBuf, cache_config:
  impl Fn(DatumId) -> LbCacheConfig + Send + Sync + 'static)` (signature
  change — `data_dir` becomes unused/removed, see note below).
- Consumes: `seisin_node::collection_store::CollectionStore` (Task 4),
  `seisin_node::index_handler::{IndexKind, ResidentIndex,
  IndexKindRegistry}`.

Note on `data_dir`: `LbIndexKind` no longer opens local files (storage
owns the collection files now), so `register_lb_class`'s `data_dir:
PathBuf` parameter is dead. Remove it from the signature entirely
(don't keep it as an unused parameter — GUIDELINES: no dead weight).

- [ ] **Step 1: The LRU-for-the-middle helper**

```rust
//! `LbCache`: the compute-side bounded cache in front of a
//! storage-backed lb board (`docs/superpowers/specs/
//! 2026-09-01-lb-storage-backed-cache-design.md`). Pinned top/bottom
//! windows plus an LRU for everything else fetched via point/around-
//! player/friend queries — middle entries are evicted first because
//! the LRU never holds a pinned entry in the first place.

use std::collections::HashMap;
use std::sync::Arc;

use seisin_core::datum::DatumId;
use seisin_node::collection_store::CollectionStore;
use seisin_protocol::LbEntry;

pub struct LbCacheConfig {
  pub pinned_top: usize,
  pub pinned_bottom: usize,
  /// Total entries the cache may hold, pinned windows included — the
  /// LRU's own capacity is `max_cached_entries.saturating_sub(pinned_top
  /// + pinned_bottom)`.
  pub max_cached_entries: usize,
}

/// A tiny manual LRU (linear-scan eviction) — deliberately not pulling
/// in an `lru` crate dependency for what's expected to be a small
/// (tens-to-low-hundreds of entries) cache per board.
pub(crate) struct LruMiddle {
  entries: HashMap<DatumId, (LbEntry, u64)>,
  next_seq: u64,
  cap: usize,
}

impl LruMiddle {
  pub(crate) fn new(cap: usize) -> Self {
    Self {
      entries: HashMap::new(),
      next_seq: 0,
      cap,
    }
  }

  pub(crate) fn touch(&mut self, entry: LbEntry) {
    let seq = self.next_seq;
    self.next_seq += 1;
    self.entries.insert(entry.player_id, (entry, seq));
    if self.cap > 0 && self.entries.len() > self.cap {
      if let Some(evict) = self
        .entries
        .iter()
        .min_by_key(|(_, (_, seq))| *seq)
        .map(|(id, _)| *id)
      {
        self.entries.remove(&evict);
      }
    } else if self.cap == 0 {
      self.entries.clear(); // a zero-capacity LRU caches nothing
    }
  }

  pub(crate) fn get(&mut self, player_id: DatumId) -> Option<LbEntry> {
    let seq = self.next_seq;
    self.next_seq += 1;
    let (entry, s) = self.entries.get_mut(&player_id)?;
    *s = seq;
    Some(entry.clone())
  }

  pub(crate) fn remove(&mut self, player_id: DatumId) {
    self.entries.remove(&player_id);
  }

  pub(crate) fn clear(&mut self) {
    self.entries.clear();
  }
}
```

- [ ] **Step 2: `LbCache` struct**

Append to the same file:

```rust
pub struct LbCache {
  pub(crate) def: crate::lb::LbClassDef,
  pub(crate) store: Arc<dyn CollectionStore>,
  pub(crate) rank_id: DatumId,
  pub(crate) by_player_id: DatumId,
  pub(crate) replication: u16,
  /// `None` means "needs a storage refresh before the next read that
  /// needs it" — writes invalidate broadly rather than patching the
  /// pinned windows in place (a correct, simple v1; incremental
  /// in-place patching on write is a natural later optimization, not
  /// required for correctness).
  pub(crate) pinned_top: Option<Vec<LbEntry>>,
  pub(crate) pinned_bottom: Option<Vec<LbEntry>>,
  pub(crate) total: Option<u64>,
  pub(crate) config: LbCacheConfig,
  pub(crate) middle: LruMiddle,
}

impl LbCache {
  pub(crate) fn new(
    def: crate::lb::LbClassDef,
    store: Arc<dyn CollectionStore>,
    board_id: DatumId,
    replication: u16,
    config: LbCacheConfig,
  ) -> Self {
    let rank_id = board_id;
    let by_player_id = DatumId::from_name(&board_id, b"by_player");
    let value_size = 2 + def.display_len as u32;
    store.create(rank_id, 24, value_size, replication);
    store.create(by_player_id, 16, 8, replication);
    let middle_cap = config
      .max_cached_entries
      .saturating_sub(config.pinned_top + config.pinned_bottom);
    Self {
      def,
      store,
      rank_id,
      by_player_id,
      replication,
      pinned_top: None,
      pinned_bottom: None,
      total: None,
      middle: LruMiddle::new(middle_cap),
      config,
    }
  }
}
```

(Read/write logic — `apply`/`query`/`execute`, `ResidentIndex`, and
`IndexKind` for `LbIndexKind` — is Task 7; this task only needs to
compile on its own, so add a temporary `impl seisin_node::index_handler
::ResidentIndex for LbCache {}` using the trait's defaults (`apply`
isn't defaulted — see `index_handler.rs`; if `apply` has no default,
add a minimal one here returning the "lb boards use execute, not
apply" violation exactly like today's `LbResidentBoard::apply`, copied
verbatim) so the crate builds; Task 7 replaces this stub with the real
`execute`/`query`.)

- [ ] **Step 3: `register_lb_class`'s new signature and `LbIndexKind`**

In `crates/seisin-types/src/lb_kind.rs`, replace the whole file's
`LbIndexKind` struct, its `impl IndexKind for LbIndexKind`, and
`register_lb_class` (delete `LbResidentBoard` and everything under it —
Task 7 rebuilds the execute/query logic on `LbCache` instead). Keep the
file's existing `file_name_for`/`composite_key`/`encode_display`/
`decode_display`/`entry_from` helpers — Task 7 reuses `composite_key`/
`encode_display`/`decode_display`/`entry_from` (delete `file_name_for`,
which was only for the now-gone local file):

```rust
pub struct LbIndexKind {
  def: LbClassDef,
  cache_config: Box<dyn Fn(DatumId) -> crate::lb_cache::LbCacheConfig + Send + Sync>,
  collection_store: std::sync::OnceLock<std::sync::Arc<dyn seisin_node::collection_store::CollectionStore>>,
}

/// Board replication factor — fixed for now (matches
/// `cluster_test_node`'s hardcoded `REPL`); no per-board configuration
/// surface yet, per the design doc.
const LB_REPLICATION: u16 = 2;

impl seisin_node::index_handler::IndexKind for LbIndexKind {
  fn open(
    &self,
    target: DatumId,
    _stored: Option<Vec<u8>>,
  ) -> Result<Box<dyn seisin_node::index_handler::ResidentIndex>, String> {
    let store = self
      .collection_store
      .get()
      .cloned()
      .ok_or_else(|| "lb: collection store not attached before first access".to_string())?;
    let config = (self.cache_config)(target);
    Ok(Box::new(crate::lb_cache::LbCache::new(
      self.def.clone(),
      store,
      target,
      LB_REPLICATION,
      config,
    )))
  }

  fn attach_collection_store(
    &self,
    store: std::sync::Arc<dyn seisin_node::collection_store::CollectionStore>,
  ) {
    let _ = self.collection_store.set(store); // idempotent: a repeat attach is ignored, not an error
  }
}

/// Registers one leaderboard class under kind `lb:{name}` — call once
/// at the composition root per class. `cache_config` resolves each
/// specific board's cache sizing the first time this compute node opens
/// it (the set of actual boards isn't fixed, so this can't be a static
/// table) — see the design doc's "Per-board cache configuration"
/// section.
pub fn register_lb_class(
  registry: &mut seisin_node::index_handler::IndexKindRegistry,
  def: LbClassDef,
  cache_config: impl Fn(DatumId) -> crate::lb_cache::LbCacheConfig + Send + Sync + 'static,
) {
  let kind = lb_kind_name(&def.name);
  registry.register(
    kind,
    Box::new(LbIndexKind {
      def,
      cache_config: Box::new(cache_config),
      collection_store: std::sync::OnceLock::new(),
    }),
  );
}
```

Remove the now-unused `use std::path::PathBuf;` and `LB_PAGE_SIZE`
constant from the top of the file if nothing else in it still uses
them (Task 7 will confirm — `composite_key`/`encode_display`/
`decode_display`/`entry_from` don't need either).

- [ ] **Step 4: Declare the module**

In `crates/seisin-types/src/lib.rs`, add `pub mod lb_cache;` next to
the existing `pub mod lb_kind;`.

- [ ] **Step 5: Confirm it compiles**

Run: `cargo build -p seisin-types`
Expected: builds clean (existing callers of `register_lb_class` — the
tests in `lb_kind.rs` and `integration_lb_boards.rs` — will now fail to
*compile* against the new signature; that's expected and fixed in Task
7/8, not this task. Confirm with `cargo build -p seisin-types --lib`
specifically, which builds the library without its tests, to isolate
"does the new code itself compile" from "do the old tests still match
the old signature.")

- [ ] **Step 6: Format, lint (library target only), commit**

```bash
cargo fmt --all
cargo clippy -p seisin-types --lib -- -D warnings
git add crates/seisin-types/src/lb_cache.rs crates/seisin-types/src/lb_kind.rs crates/seisin-types/src/lib.rs
git commit -m "$(cat <<'EOF'
feat: LbCache scaffold and register_lb_class's new callback signature

LbCacheConfig + a bounded LruMiddle helper, and LbIndexKind/
register_lb_class rewired onto CollectionStore (injected via
attach_collection_store) instead of a local BPlusTree file. Read/write
logic lands in the next commit — this one is scaffold-only, and
existing lb tests are expected to be red until then.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: `LbCache` read/write logic (the real `execute`/`query`)

**Files:**
- Modify: `crates/seisin-types/src/lb_cache.rs`
- Modify: `crates/seisin-types/src/lb_kind.rs` (remove the temporary stub `impl ResidentIndex for LbCache` from Task 6)

**Interfaces:**
- Consumes: `crate::lb::{LbClassDef, LbRule}`, `crate::lb_kind::
  {composite_key, encode_display, decode_display, entry_from}` (make
  these `pub(crate)` if not already, so `lb_cache.rs` can use them —
  they're currently private `fn`s in `lb_kind.rs`; add `pub(crate)` to
  each), `seisin_protocol::{LbEntry, LbExecuteOp, LbFriendRank,
  LbQueryReq, LbResult, decode_lb_execute_op, decode_lb_query_req,
  encode_lb_result}`.
- Produces: `impl ResidentIndex for LbCache` (`apply`/`query`/
  `execute`), replacing the Task 6 stub.

This task ports `LbResidentBoard::assemble`/`apply_rule`/`apply`/
`query`/`execute`'s *logic* (unchanged rules, unchanged wire contract)
onto `CollectionStore` calls instead of a local `RefCell<BPlusTree>`.

- [ ] **Step 1: `pinned_top`/`pinned_bottom` refresh + the write path**

Add to `lb_cache.rs`:

```rust
impl LbCache {
  fn ensure_top(&mut self) -> &[LbEntry] {
    if self.pinned_top.is_none() {
      let entries = self
        .store
        .scan_forward(self.rank_id, self.config.pinned_top as u32, self.replication)
        .iter()
        .map(|(k, v)| crate::lb_kind::entry_from(k, v))
        .collect();
      self.pinned_top = Some(entries);
    }
    self.pinned_top.as_deref().unwrap()
  }

  fn ensure_bottom(&mut self) -> &[LbEntry] {
    if self.pinned_bottom.is_none() {
      let entries = self
        .store
        .scan_backward(self.rank_id, self.config.pinned_bottom as u32, self.replication)
        .iter()
        .map(|(k, v)| crate::lb_kind::entry_from(k, v))
        .collect();
      self.pinned_bottom = Some(entries);
    }
    self.pinned_bottom.as_deref().unwrap()
  }

  fn ensure_total(&mut self) -> u64 {
    if self.total.is_none() {
      self.total = Some(self.store.count(self.rank_id, self.replication));
    }
    self.total.unwrap()
  }

  /// A board-write of any kind (Update/Remove) invalidates the pinned
  /// windows and the running total — the next read that needs them
  /// re-fetches from storage. Simple and correct; not the tightest
  /// possible (an update outside both windows doesn't actually need to
  /// invalidate either), left as a documented future refinement rather
  /// than complicating v1.
  fn invalidate(&mut self) {
    self.pinned_top = None;
    self.pinned_bottom = None;
    self.total = None;
  }

  fn apply_rule(&self, old_key: &[u8; 8], new_key: &[u8; 8]) -> bool {
    match self.def.rule {
      crate::lb::LbRule::Max => new_key > old_key,
      crate::lb::LbRule::Min => new_key < old_key,
      crate::lb::LbRule::Replace => new_key != old_key,
    }
  }

  /// Looks up a player's current `rank_key`: LRU first, `by_player`
  /// collection on miss (and caches the miss's resulting entry only
  /// when the caller already has enough context to build one — see
  /// call sites; this helper alone just answers the rank_key).
  fn player_rank_key(&mut self, player_id: DatumId) -> Option<[u8; 8]> {
    self
      .store
      .get(self.by_player_id, player_id.as_bytes().to_vec(), self.replication)
      .map(|v| v.try_into().unwrap())
  }

  /// Best-first entries window ±`half` around `player_id`'s current
  /// rank, plus that player's own best-first rank — `None` if the
  /// player isn't on the board. Ports `LbResidentBoard::assemble`'s
  /// "around" computation onto `rank_of_key`/`scan_from_rank`.
  fn around(&mut self, player_id: DatumId, window: u32) -> Result<Option<(u64, Vec<LbEntry>)>, String> {
    let Some(rank_key) = self.player_rank_key(player_id) else {
      return Ok(None);
    };
    let total = self.ensure_total();
    let key = crate::lb_kind::composite_key(&rank_key, player_id);
    let asc = self
      .store
      .rank_of_key(self.rank_id, key.to_vec(), self.replication)
      .ok_or_else(|| "board map/collection divergence: mapped key missing".to_string())?;
    let best_rank = total - 1 - asc;
    if window == 0 {
      return Ok(Some((best_rank, Vec::new())));
    }
    let half = (window / 2) as u64;
    let best_start = best_rank.saturating_sub(half);
    let best_end = (best_start + window as u64).min(total);
    let best_start = best_end.saturating_sub(window as u64);
    let asc_start = total - best_end;
    let mut entries: Vec<LbEntry> = self
      .store
      .scan_from_rank(self.rank_id, asc_start, (best_end - best_start) as u32, self.replication)
      .iter()
      .map(|(k, v)| crate::lb_kind::entry_from(k, v))
      .collect();
    entries.reverse();
    for entry in &entries {
      self.middle.touch(entry.clone());
    }
    Ok(Some((best_rank, entries)))
  }

  fn friend_ranks(&mut self, friend_ids: &[DatumId]) -> Result<Vec<LbFriendRank>, String> {
    let total = self.ensure_total();
    let mut friends = Vec::new();
    for friend_id in friend_ids {
      if let Some(cached) = self.middle.get(*friend_id) {
        let asc = self
          .store
          .rank_of_key(
            self.rank_id,
            crate::lb_kind::composite_key(&cached.rank_key, *friend_id).to_vec(),
            self.replication,
          )
          .ok_or_else(|| "board map/collection divergence: cached key missing".to_string())?;
        friends.push(LbFriendRank {
          player_id: *friend_id,
          rank: total - 1 - asc,
          rank_key: cached.rank_key,
          display: cached.display,
        });
        continue;
      }
      let Some(rank_key) = self.player_rank_key(*friend_id) else {
        continue; // not on this board — omitted per the design doc
      };
      let key = crate::lb_kind::composite_key(&rank_key, *friend_id);
      let asc = self
        .store
        .rank_of_key(self.rank_id, key.to_vec(), self.replication)
        .ok_or_else(|| "board map/collection divergence: mapped key missing".to_string())?;
      let (k, v) = self
        .store
        .scan_from_rank(self.rank_id, asc, 1, self.replication)
        .into_iter()
        .next()
        .ok_or_else(|| "board map/collection divergence: rank scan empty".to_string())?;
      let entry = crate::lb_kind::entry_from(&k, &v);
      self.middle.touch(entry.clone());
      friends.push(LbFriendRank {
        player_id: *friend_id,
        rank: total - 1 - asc,
        rank_key,
        display: entry.display,
      });
    }
    Ok(friends)
  }
}
```

- [ ] **Step 2: `ResidentIndex` — `query`**

```rust
impl seisin_node::index_handler::ResidentIndex for LbCache {
  fn apply(&mut self, _payload: &[u8]) -> seisin_node::index_handler::IndexApplyOutcome {
    seisin_node::index_handler::IndexApplyOutcome {
      violation: Some(
        "lb boards are maintained via execute ops, not framework index updates".to_string(),
      ),
      write_through: seisin_node::index_handler::WriteThrough::None,
    }
  }

  fn query(&self, query: &[u8]) -> Result<Vec<u8>, String> {
    // `query` takes `&self` per the trait, but every helper above needs
    // `&mut self` for cache population — same RefCell-vs-&self tension
    // `LbResidentBoard` already had, resolved the same way: interior
    // mutability. Wrap the cache-mutating fields in a `RefCell` instead
    // of taking `&mut self` directly on `LbCache` itself... 
```

Stop and reconsider before writing this arm: `ResidentIndex::query`
takes `&self`, but `ensure_top`/`ensure_bottom`/`around`/`friend_ranks`
all need `&mut self` to populate the cache. `LbResidentBoard` solved
this with `tree: RefCell<BPlusTree>`. Do the same here: wrap `LbCache`'s
five mutable fields (`pinned_top`, `pinned_bottom`, `total`, `middle`)
in one `RefCell<LbCacheState>` (a small nested struct), leaving `def`,
`store`, `rank_id`, `by_player_id`, `replication`, `config` as plain
`LbCache` fields outside the `RefCell` (they're set once at
construction and never mutated again). Go back to Task 6 Step 2 and
Task 7 Step 1 and thread this through:

```rust
pub(crate) struct LbCacheState {
  pinned_top: Option<Vec<LbEntry>>,
  pinned_bottom: Option<Vec<LbEntry>>,
  total: Option<u64>,
  middle: LruMiddle,
}

pub struct LbCache {
  def: crate::lb::LbClassDef,
  store: Arc<dyn CollectionStore>,
  rank_id: DatumId,
  by_player_id: DatumId,
  replication: u16,
  config: LbCacheConfig,
  state: std::cell::RefCell<LbCacheState>,
}
```

`LbCache::new` builds `state: std::cell::RefCell::new(LbCacheState {
pinned_top: None, pinned_bottom: None, total: None, middle:
LruMiddle::new(middle_cap) })`. Every helper method from Step 1
(`ensure_top`, `ensure_bottom`, `ensure_total`, `invalidate`,
`player_rank_key`, `around`, `friend_ranks`) becomes `&self` instead of
`&mut self`, opening `self.state.borrow_mut()` internally to reach its
fields (mirror `LbResidentBoard::assemble`'s existing `let mut tree =
self.tree.borrow_mut();` pattern exactly). Now finish Step 2:

```rust
  fn query(&self, query: &[u8]) -> Result<Vec<u8>, String> {
    let LbQueryReq {
      top,
      bottom,
      around_player,
      window,
      friend_ids,
    } = seisin_protocol::decode_lb_query_req(query).map_err(|e| e.to_string())?;
    let top_entries = self.ensure_top()[..(top as usize).min(self.config.pinned_top)].to_vec();
    let bottom_entries = self.ensure_bottom()[..(bottom as usize).min(self.config.pinned_bottom)].to_vec();
    let total = self.ensure_total();
    let (player_rank, around_entries) = match around_player {
      Some(player_id) => match self.around(player_id, window)? {
        Some((rank, entries)) => (Some(rank), entries),
        None => (None, Vec::new()),
      },
      None => (None, Vec::new()),
    };
    let friends = self.friend_ranks(&friend_ids)?;
    let result = LbResult {
      total,
      player_rank,
      top: top_entries,
      bottom: bottom_entries,
      around: around_entries,
      friends,
    };
    Ok(seisin_protocol::encode_lb_result(&result))
  }
```

Note: `top`/`bottom` in the query can legitimately ask for more entries
than `config.pinned_top`/`pinned_bottom` cover. For this task, cap the
response at the pinned-window size (as written above) — a query asking
for more than the configured pin size gets fewer entries than
requested rather than a fresh unbounded storage scan. Document this
precisely as a real, deliberate v1 limitation with a comment at the
`top_entries`/`bottom_entries` lines: `// v1 limitation: a query for
more than this board's pinned window returns only what's pinned —
growing the window on demand is a straightforward follow-up, not done
here.` (`LbResidentBoard` never had this limitation since it held the
whole tree; flag it rather than silently shipping reduced behavior.)

- [ ] **Step 3: `ResidentIndex` — `execute`**

```rust
  fn execute(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
    match seisin_protocol::decode_lb_execute_op(payload).map_err(|e| e.to_string())? {
      LbExecuteOp::Update {
        player_id,
        display,
        rank_key,
        friend_ids,
        top,
        window,
      } => {
        let old_key = self.player_rank_key(player_id);
        let replace = match old_key {
          None => true,
          Some(old_key) => self.apply_rule(&old_key, &rank_key),
        };
        if replace {
          if let Some(old_key) = old_key {
            self.store.remove(
              self.rank_id,
              crate::lb_kind::composite_key(&old_key, player_id).to_vec(),
              self.replication,
            );
          }
          self.store.insert(
            self.rank_id,
            crate::lb_kind::composite_key(&rank_key, player_id).to_vec(),
            crate::lb_kind::encode_display(&display, self.def.display_len),
            self.replication,
          );
          self.store.insert(
            self.by_player_id,
            player_id.as_bytes().to_vec(),
            rank_key.to_vec(),
            self.replication,
          );
          self.invalidate();
          self.middle.remove(player_id); // stale if it was cached under the old key
        }
        let query = LbQueryReq {
          top,
          bottom: 0,
          around_player: Some(player_id),
          window,
          friend_ids,
        };
        self.query(&seisin_protocol::encode_lb_query_req(&query))
      }
      LbExecuteOp::Remove { player_id } => {
        if let Some(old_key) = self.player_rank_key(player_id) {
          self.store.remove(
            self.rank_id,
            crate::lb_kind::composite_key(&old_key, player_id).to_vec(),
            self.replication,
          );
          self.store.remove(self.by_player_id, player_id.as_bytes().to_vec(), self.replication);
          self.invalidate();
          self.middle.remove(player_id);
        }
        let query = LbQueryReq {
          top: 0,
          bottom: 0,
          around_player: None,
          window: 0,
          friend_ids: vec![],
        };
        self.query(&seisin_protocol::encode_lb_query_req(&query))
      }
    }
  }
```

`execute` takes `&mut self` per the trait (unlike `query`) but every
call above goes through `&self` helpers via the `RefCell` — that's
fine, `&mut self` trivially reborrows as `&self`. `Update`/`Remove`
delegate their result assembly to `self.query(...)` re-encoding a
`LbQueryReq` — this exactly matches what `LbResidentBoard::execute`
already did by calling `self.assemble(...)` directly; going through
`query`'s public wire encoding instead of a shared private `assemble`
is a small deliberate simplification (one fewer near-duplicate
function) enabled by the fact `LbCache` no longer needs `assemble` to
also serve `IndexApplyOutcome`'s framework-diff path (lb never uses
that path — see `apply` above).

- [ ] **Step 4: Wire up `lb_kind.rs`'s exports and delete the Task 6 stub**

In `lb_kind.rs`: add `pub(crate)` to `composite_key`, `encode_display`,
`decode_display`, `entry_from` (they're used from `lb_cache.rs` now).
Delete the temporary `impl ResidentIndex for LbCache {}` stub Task 6
added — Step 2/3 above now provide the real impl in `lb_cache.rs`.

- [ ] **Step 5: Fix `lb_kind.rs`'s own `#[cfg(test)] mod tests`**

The existing tests in `lb_kind.rs` (`racing`, `open_board`, `update`,
and whatever else is in that module — inspect the file directly, the
version before Task 6's edits had a `#[cfg(test)] mod tests` block
using `LbIndexKind::new(...).open(...)` against a local tempdir) no
longer compile: `LbIndexKind` has no `new` anymore (only `register_lb_class`
constructs it), and `open` now needs an attached `CollectionStore`.
Rewrite this test module to:
1. Build an in-memory-ish test double implementing `CollectionStore`
   (a `HashMap<(DatumId, Vec<u8>), Vec<u8>>`-backed fake is simplest and
   is a legitimate fake here — the *real* storage-side implementation
   is already covered by Task 3's real-file test; this fake just needs
   to behave like an ordered collection for `LbCache`'s own unit tests).
2. Register a class via `register_lb_class`, call
   `registry.attach_collection_store(Arc::new(fake_store))`, then
   `registry.get("lb:racing").unwrap().open(board_id, None)`.
3. Keep the existing test *assertions* (same `update`/`query` call
   shapes, same expected `LbResult` contents) — only the setup changes.

```rust
#[cfg(test)]
mod tests {
  use std::collections::HashMap;
  use std::sync::{Arc, Mutex};

  use super::*;
  use crate::field::FieldValue;
  use crate::lb::{encode_score, LbScoreType};
  use crate::lb_cache::LbCacheConfig;
  use seisin_node::collection_store::CollectionStore;
  use seisin_node::index_handler::IndexKindRegistry;
  use seisin_protocol::{decode_lb_result, encode_lb_execute_op, encode_lb_query_req};

  /// An in-process ordered-collection fake: a plain sorted Vec per
  /// collection, linear scans throughout. Only for LbCache's own unit
  /// tests — the real storage-side path is exercised for real in
  /// Task 3's store_server.rs test and Task 8's integration test.
  #[derive(Default)]
  struct FakeCollectionStore {
    collections: Mutex<HashMap<DatumId, Vec<(Vec<u8>, Vec<u8>)>>>,
  }

  impl CollectionStore for FakeCollectionStore {
    fn create(&self, collection_id: DatumId, _key_size: u32, _value_size: u32, _n: u16) {
      self.collections.lock().unwrap().entry(collection_id).or_default();
    }
    fn insert(&self, collection_id: DatumId, key: Vec<u8>, value: Vec<u8>, _n: u16) {
      let mut collections = self.collections.lock().unwrap();
      let entries = collections.entry(collection_id).or_default();
      entries.retain(|(k, _)| k != &key);
      entries.push((key, value));
      entries.sort_by(|a, b| a.0.cmp(&b.0));
    }
    fn remove(&self, collection_id: DatumId, key: Vec<u8>, _n: u16) {
      if let Some(entries) = self.collections.lock().unwrap().get_mut(&collection_id) {
        entries.retain(|(k, _)| k != &key);
      }
    }
    fn get(&self, collection_id: DatumId, key: Vec<u8>, _n: u16) -> Option<Vec<u8>> {
      self
        .collections
        .lock()
        .unwrap()
        .get(&collection_id)?
        .iter()
        .find(|(k, _)| k == &key)
        .map(|(_, v)| v.clone())
    }
    fn scan_forward(&self, collection_id: DatumId, limit: u32, _n: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
      let collections = self.collections.lock().unwrap();
      let entries = collections.get(&collection_id).cloned().unwrap_or_default();
      entries.into_iter().rev().take(limit as usize).collect()
    }
    fn scan_backward(&self, collection_id: DatumId, limit: u32, _n: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
      let collections = self.collections.lock().unwrap();
      let entries = collections.get(&collection_id).cloned().unwrap_or_default();
      entries.into_iter().take(limit as usize).collect()
    }
    fn sample(&self, collection_id: DatumId, k: u32, _n: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
      self.scan_forward(collection_id, k, _n)
    }
    fn rank_of_key(&self, collection_id: DatumId, key: Vec<u8>, _n: u16) -> Option<u64> {
      let collections = self.collections.lock().unwrap();
      collections
        .get(&collection_id)?
        .iter()
        .position(|(k, _)| k == &key)
        .map(|p| p as u64)
    }
    fn scan_from_rank(&self, collection_id: DatumId, rank: u64, limit: u32, _n: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
      let collections = self.collections.lock().unwrap();
      let entries = collections.get(&collection_id).cloned().unwrap_or_default();
      entries.into_iter().skip(rank as usize).take(limit as usize).collect()
    }
    fn count(&self, collection_id: DatumId, _n: u16) -> u64 {
      self
        .collections
        .lock()
        .unwrap()
        .get(&collection_id)
        .map(|e| e.len() as u64)
        .unwrap_or(0)
    }
  }

  fn racing(rule: LbRule) -> LbClassDef {
    LbClassDef {
      name: "racing".to_string(),
      score_type: LbScoreType::I64,
      display_len: 16,
      rule,
    }
  }

  fn generous_config(_board_id: DatumId) -> LbCacheConfig {
    LbCacheConfig {
      pinned_top: 10,
      pinned_bottom: 10,
      max_cached_entries: 100,
    }
  }

  fn open_board(rule: LbRule) -> Box<dyn seisin_node::index_handler::ResidentIndex> {
    let mut registry = IndexKindRegistry::new();
    register_lb_class(&mut registry, racing(rule), generous_config);
    registry.attach_collection_store(Arc::new(FakeCollectionStore::default()));
    registry.get("lb:racing").unwrap().open(DatumId::new(), None).unwrap()
  }

  fn update(
    board: &mut dyn seisin_node::index_handler::ResidentIndex,
    player: DatumId,
    display: &str,
    score: i64,
    friends: Vec<DatumId>,
  ) -> seisin_protocol::LbResult {
    let rank_key = encode_score(&racing(LbRule::Max), &FieldValue::I64(score)).unwrap();
    let payload = encode_lb_execute_op(&LbExecuteOp::Update {
      player_id: player,
      display: display.as_bytes().to_vec(),
      rank_key,
      friend_ids: friends,
      top: 10,
      window: 10,
    });
    decode_lb_result(&board.execute(&payload).unwrap()).unwrap()
  }

  // Port the rest of the pre-existing tests in this module (whatever
  // is currently below the deleted `open_board`/`update` helpers —
  // read the file before this task to see their exact bodies) onto
  // these new helpers, keeping each test's assertions unchanged.
}
```

- [ ] **Step 6: Run the tests**

Run: `cargo test -p seisin-types lb`
Expected: PASS (`lb_kind::tests::*` and any `lb::tests::*`).

- [ ] **Step 7: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy -p seisin-types --all-targets -- -D warnings
git add crates/seisin-types/src/lb_cache.rs crates/seisin-types/src/lb_kind.rs
git commit -m "$(cat <<'EOF'
feat: LbCache read/write logic over CollectionStore

Ports LbResidentBoard's Update/Remove/query assembly onto the storage-
backed CollectionStore trait: pinned top/bottom windows refreshed on
invalidation, an LRU for point/around-player/friend lookups. Same wire
contract, same class-rule semantics, unit-tested against a fake
CollectionStore.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: Rewrite `integration_lb_boards.rs` against a real storage node

**Files:**
- Modify: `crates/seisin-types/tests/integration_lb_boards.rs`

**Interfaces:**
- Consumes: everything from Tasks 1–7 (`register_lb_class`'s new
  signature, `RemoteCollectionStore`, `attach_collection_store`).

- [ ] **Step 1: Start a real storage node alongside the compute node**

Rewrite `start_node` (currently: compute-only, `InMemoryStore`) to also
spin up a real `StoreNode` (mirror Task 3's test setup — real
`TcpListener`, `serve_store` thread, tempdir `data_dir`) and a
`ClusterState` with that storage node in the storage ring (mirror
`cluster_test_node`'s `put2`/`get2` pattern of `n: 2` — for this test,
`n: 1` is enough since there's only one storage node; use whatever `n`
`LB_REPLICATION` in `lb_kind.rs` is hardcoded to and start that many
storage nodes, so replication actually gets exercised at the configured
factor. If `LB_REPLICATION == 2`, start two storage nodes here.).
Build the compute node's `WorkerPool` with this `ClusterState`'s
`RemoteStore` as before (unchanged — regular datums still work exactly
as they did), and after building `IndexKindRegistry` +
`register_lb_class(&mut index_kinds, racing_class(), generous_config)`,
call `index_kinds.attach_collection_store(Arc::new(
RemoteCollectionStore::new(Arc::clone(&cluster))))` before passing
`index_kinds` into `WorkerPool::spawn`.

- [ ] **Step 2: Update the `cache_config` argument at the call site**

`register_lb_class`'s call in this file currently passes `data_dir` —
replace with a `cache_config` closure generous enough that none of the
existing assertions (which check specific `top`/rank contents) get
truncated by pin-size limits: `|_board_id| LbCacheConfig { pinned_top:
50, pinned_bottom: 50, max_cached_entries: 500 }` (the test boards in
this file have at most a handful of players).

- [ ] **Step 3: Run the existing test body unchanged**

The test function `boards_update_query_and_stay_independent_over_the_wire`
itself (its `submit`/`query` helpers and assertions) doesn't change —
it exercises the wire contract (`Request::LbExecute`/`LbQuery`), which
Task 7 guarantees is unchanged. If it doesn't compile/pass unmodified
after Steps 1–2, the wire contract broke somewhere in Tasks 6–7 — fix
the regression there, don't adjust this test's assertions to match
broken behavior.

- [ ] **Step 4: Run the test**

Run: `cargo test -p seisin-types --test integration_lb_boards`
Expected: PASS.

- [ ] **Step 5: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy -p seisin-types --all-targets -- -D warnings
git add crates/seisin-types/tests/integration_lb_boards.rs
git commit -m "$(cat <<'EOF'
test: integration_lb_boards against a real storage-backed cluster

Boots real storage node(s) alongside the compute node and wires
RemoteCollectionStore through attach_collection_store, proving the
Request::LbExecute/LbQuery wire contract is unchanged by the storage-
backed rewrite.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: Cache-eviction test

**Files:**
- Create: `crates/seisin-types/tests/integration_lb_cache_eviction.rs`

**Interfaces:**
- Consumes: same real-cluster setup as Task 8 (extract the `start_node`
  helper Task 8 wrote into a small shared test-support module if it's
  getting duplicated across these two files — `crates/seisin-types/
  tests/support/mod.rs` is the idiomatic place for a helper shared by
  multiple integration test binaries; only do this extraction if
  copy-pasting `start_node` verbatim would exceed ~40 lines of
  duplication, per the file-structure guidance to avoid unnecessary
  abstraction for a one-off).

- [ ] **Step 1: Insert more players than the cache limit, assert pinned windows stay correct**

```rust
#[test]
fn pinned_windows_survive_a_board_larger_than_the_cache() {
  let addr = start_node_with_cache_config(|_board_id| LbCacheConfig {
    pinned_top: 3,
    pinned_bottom: 3,
    max_cached_entries: 10, // leaves room for a 4-entry LRU
  });
  let board = lb_board_key("racing", "big", "default");

  let players: Vec<DatumId> = (0..20).map(|_| DatumId::new()).collect();
  for (i, player) in players.iter().enumerate() {
    submit(&addr, board, *player, &format!("p{i}"), i as i64, vec![]);
  }

  let result = query(&addr, board, 0); // top query, per this file's existing `query` helper shape (Task 8/mirroring integration_lb_boards.rs)
  assert_eq!(result.total, 20);
  let top_names: Vec<&[u8]> = result.top.iter().map(|e| e.display.as_slice()).collect();
  // Highest scores are players 19, 18, 17 (score == index).
  assert_eq!(top_names, vec![b"p19".as_slice(), b"p18".as_slice(), b"p17".as_slice()]);
}
```

- [ ] **Step 2: A point/around-player query for a middle player still resolves correctly (via a storage round trip, not a resident full board)**

```rust
#[test]
fn a_middle_player_not_in_either_pinned_window_still_resolves() {
  let addr = start_node_with_cache_config(|_board_id| LbCacheConfig {
    pinned_top: 2,
    pinned_bottom: 2,
    max_cached_entries: 6,
  });
  let board = lb_board_key("racing", "big", "default");

  let players: Vec<DatumId> = (0..10).map(|_| DatumId::new()).collect();
  for (i, player) in players.iter().enumerate() {
    submit(&addr, board, *player, &format!("p{i}"), i as i64, vec![]);
  }

  // Player 5 (0-indexed score 5) is in neither pinned-top (scores 9,8)
  // nor pinned-bottom (scores 0,1) — this must still work correctly,
  // proving the LRU/point-lookup path (not just the pinned windows)
  // is exercised, not just skipped.
  let around = query_around(&addr, board, players[5], 3); // helper: LbQuery with around_player + window
  assert_eq!(around.player_rank, Some(4)); // best-first: rank 0 = score 9, so score 5 is rank 4
  assert!(around.around.iter().any(|e| e.display == b"p5"));
}
```

(`query_around` is a small new local helper in this test file — same
shape as the existing `submit`/`query` helpers, just setting
`around_player`/`window` on the `LbQueryReq` instead of `top`.)

- [ ] **Step 3: Run the tests**

Run: `cargo test -p seisin-types --test integration_lb_cache_eviction`
Expected: PASS.

- [ ] **Step 4: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy -p seisin-types --all-targets -- -D warnings
git add crates/seisin-types/tests/integration_lb_cache_eviction.rs
git commit -m "$(cat <<'EOF'
test: lb cache stays bounded and still resolves evicted/middle players

Proves a board larger than its configured cache still answers top-N
correctly from the pinned windows, and a player in neither pinned
window still resolves correctly via a storage round trip.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 10: Final gates, stress loop, doc follow-up

**Files:**
- Modify: `docs/superpowers/specs/2026-09-01-leaderboard-example-design.md`
- Modify: `docs/superpowers/PROGRESS.md`

- [ ] **Step 1: Full workspace gates**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
Expected: all clean/green.

- [ ] **Step 2: Standing stress loop**

Since this plan's changes sit directly on the compute↔storage dispatch
path this repo's stress-discipline rule covers, run the standing 20×
loop:

```bash
for i in $(seq 1 20); do
  cargo test -p seisin-node --test integration_wound_wait &&
  cargo test -p seisin-node --test integration_cross_node_wound_wait &&
  cargo test -p seisin-node --test integration_op_collation || { echo "FAILED at iteration $i"; break; }
done
```
Expected: 20/20 clean. (Exact test-binary names: confirm against
`crates/seisin-node/tests/` — use whatever names actually exist there;
this plan's Task 5 didn't rename any of them.)

- [ ] **Step 3: Update the leaderboard-http example spec's stale Non-goal**

In `docs/superpowers/specs/2026-09-01-leaderboard-example-design.md`,
find the "Non-goals" bullet: `lb boards are node-resident indexes, not
yet routed through the storage tier...`. Replace it with: `lb boards
are now storage-backed (see 2026-09-01-lb-storage-backed-cache-design.md)
— the leaderboard-http crate's HTTP contract is unaffected; the
"storage1 isn't exercised by lb traffic" caveat from the original spec
no longer applies.`

- [ ] **Step 4: PROGRESS.md**

Add an entry to the "Done" section (find where Sub-project 5 / the
datum type system entries are recorded, follow the existing entry
style) noting: lb boards are now storage-tier-backed via a new
content-agnostic ordered-collection store-wire primitive
(`Create`/`Insert`/`Remove`/`Get`/`ScanForward`/`ScanBackward`/
`Sample`/`RankOfKey`/`ScanFromRank`/`Count`, `STORE_PROTOCOL_VERSION`
4), with a bounded compute-side cache (pinned top/bottom + LRU,
per-board cache sizing via a first-access resolver callback) replacing
the old fully-resident `BPlusTree`; remove the "tk/lb B+Tree-file
datum-grade durability" bullet from the "Not started" Storage Tier Part
C remainder list (lb's half is now done — leave tk's).

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/2026-09-01-leaderboard-example-design.md docs/superpowers/PROGRESS.md
git commit -m "$(cat <<'EOF'
docs: lb storage-backed cache landed — update PROGRESS.md and the leaderboard-http spec's stale non-goal

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

## Self-Review Notes

- **Spec coverage**: wire protocol (Task 1), replication model (Tasks
  2/4), storage-side dispatch (Task 3), per-board cache-config callback
  (Task 6), pinned-top/bottom + LRU-middle cache (Tasks 6/7), the
  authority-pinning/no-collation invariant (unchanged by construction —
  `IndexExecute`/`IndexQuery` dispatch isn't touched by any task here,
  called out explicitly in the spec, not a task since there's nothing
  to build for it), ownership handoff (a consequence of Task 7's cold-
  open design, not separately implemented), testing (Tasks 3/4/8/9).
  `CollectionGet`'s O(n) point-lookup implementation (Task 3) is flagged
  in-line as a known follow-up, not silently shipped.
- **Known follow-up work this plan deliberately leaves out** (call these
  out to the user if picking up further work afterward): a real
  `BPlusTree::get` instead of scan-and-find; incremental in-place cache
  patching on write instead of broad invalidation; growing a query's
  top/bottom window past the pinned-window size via an extra storage
  round trip instead of truncating. Each is flagged at its point of
  introduction above.
