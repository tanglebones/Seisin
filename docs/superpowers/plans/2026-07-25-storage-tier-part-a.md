# Storage Tier Part A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline execution — this project never uses subagents) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Durable storage-role nodes with the delta log, dedicated store wire protocol, and `RemoteStore` write-through-before-ack over a static capacity-weighted ring, per `docs/superpowers/specs/2026-07-25-storage-tier-part-a-design.md`.

**Architecture:** `seisin-storage` gains the pure pieces (delta codec, `DatumLog` — dependency-free, `[u8;16]` keys); `seisin-protocol` gains the independently-versioned `store_wire` module (depending on `seisin-storage` for the `Delta` type — storage is dep-free, no cycle); `seisin-node` gains `store_server` and `RemoteStore`; `seisin-core`'s `Store` trait grows a defaulted `put_with_previous` fed by `Cache`.

**Tech Stack:** Established. `crc32` hand-rolled (small table impl in `seisin-storage` — no new deps).

## Global Constraints

House rules as prior plans (fmt/clippy per task, commit trailer, loud errors, versioned encodings). Specific to this part: every ack follows an `fdatasync` (`File::sync_data`); torn-tail truncation must never drop an acked record; `Store`'s signatures stay infallible — `RemoteStore` panics on storage failure with node + datum named (documented fail-stop v1).

---

### Task 1: delta codec (`seisin-storage/src/delta.rs`)

**Produces:** `pub struct Delta { pub prefix_len: u32, pub suffix_len: u32, pub middle: Vec<u8>, pub new_total_len: u32 }` (derives Debug/Clone/PartialEq/Eq); `pub fn diff(old: &[u8], new: &[u8]) -> Delta` (longest common prefix, then longest common suffix of remainders); `pub fn apply(old: &[u8], delta: &Delta) -> anyhow::Result<Vec<u8>>` (strict bounds: `prefix+suffix <= old.len()`, `prefix + middle + suffix == new_total_len`); `pub fn encode_delta(&Delta) -> Vec<u8>` / `pub fn decode_delta(&[u8]) -> Result<Delta>` (u32 LE fields + middle, strict trailing check); `impl Delta { pub fn encoded_len(&self) -> usize }`.

- [ ] Red: known-answer trims (middle change; length-changing change with shifted suffix — `"aaBBcc"` -> `"aaBBBBcc"`; identical → empty middle; disjoint → whole-new middle); apply rejects `prefix+suffix > old.len()` and total-len mismatch; LCG property test `apply(old, diff(old,new)) == new` over ~500 random pairs (shared-region corpus). Green. Commit `feat: add prefix/suffix byte-delta codec`.

### Task 2: the delta log (`seisin-storage/src/datum_log.rs` + `crc.rs`)

**Produces:**

```rust
pub struct DatumLog { /* file, index: HashMap<[u8;16], LogRef>, ... */ }
pub enum PatchOutcome { Applied, NeedFull }
impl DatumLog {
  pub fn open(path: &Path) -> Result<Self>;            // creates or recovers (scan, CRC, torn-tail truncate)
  pub fn put_full(&mut self, id: [u8;16], bytes: &[u8]) -> Result<()>;      // append Full + fsync
  pub fn put_delta(&mut self, id: [u8;16], delta: &Delta) -> Result<PatchOutcome>; // unknown id => NeedFull (no append); else append Delta (or self-rebased Full) + fsync
  pub fn get(&mut self, id: [u8;16]) -> Result<Option<Vec<u8>>>;            // seek base + replay chain
  pub fn delete(&mut self, id: [u8;16]) -> Result<()>;                      // append Tombstone + fsync
  pub fn len(&self) -> usize; pub fn is_empty(&self) -> bool;
}
```

Format: header `magic "SDLG" + u16 version=1`; records `[u32 len][u8 kind: 1=Full 2=Delta 3=Tombstone][16-byte id][body][u32 crc32(kind+id+body)]`. `LogRef { base_offset: u64, deltas: Vec<u64>, materialized_len: u32, delta_bytes: u32 }`. Self-rebase inside `put_delta` when chain would exceed `MAX_DELTA_CHAIN = 8` or `delta_bytes + new_delta > materialized_len / 2`: materialize, append `Full` instead. `crc.rs`: table-based crc32 (IEEE), ~20 lines, known-answer tested (`crc32(b"123456789") == 0xCBF43926`).

- [ ] Red: round trips; delta chains materialize; both rebase triggers fire (observe: after N small deltas, `get` correct AND a reopened log's chain length resets — reopen sees the consolidating Full); `put_delta` on unknown id → `NeedFull`, nothing appended; reopen-recovery; torn tail (append garbage / truncate mid-record, reopen, acked prefix intact, tail gone); wrong-magic/version open errors. Green (fsync via `sync_data` after each append). Commit `feat: add crash-recoverable delta log with self-rebasing`.

### Task 3: store wire (`seisin-protocol/src/store_wire.rs`)

**Produces:** `pub const STORE_PROTOCOL_VERSION: u8 = 1;`

```rust
pub enum StoreRequest {
  Put { id: DatumId, bytes: Vec<u8> },
  Patch { id: DatumId, delta: Delta },
  Get { id: DatumId },
  Delete { id: DatumId },
}
pub enum StoreResponse { Ack, NeedFull, Value { bytes: Option<Vec<u8>> } }
pub fn encode_store_request/decode_store_request; encode_store_response/decode_store_response;
```

Leading version byte per frame (own `check_version` clone with "store" wording); reuse the crate's `read_frame`/`write_frame` for transport. `seisin-protocol` gains `seisin-storage = { path = ... }` dep (dep-free crate; no cycle). `lib.rs`: `pub mod store_wire;`.

- [ ] Red: round trips all variants incl. `Value { None }` and a Patch carrying a real Delta; unsupported version rejected; tag corruption rejected. Green. Commit `feat: add independently versioned storage wire protocol`.

### Task 4: `Store::put_with_previous` + `Cache` feed

**Files:** `crates/seisin-core/src/store.rs`, `crates/seisin-core/src/cache.rs`.

`Store` gains `fn put_with_previous(&self, id: DatumId, content: Vec<u8>, previous: Option<&[u8]>) { let _ = previous; self.put(id, content) }` (defaulted — `InMemoryStore` untouched). `Cache::put` captures its current entry for `id` (its own map — the value it is about to overwrite) and calls `put_with_previous`. (Check cache.rs's actual shape at implementation time; `Cache::delete`/invalidate unchanged.)

- [ ] Red: a spy `Store` in cache tests records the `previous` argument — first put sees `None`, second sees `Some(first_bytes)`, put-after-invalidate sees `None`. Green; full workspace tests (no behavior change for InMemoryStore paths). Commit `feat: thread previous bytes through Cache to Store for delta computation`.

### Task 5: store server, `RemoteStore`, role config

**Files:** `crates/seisin-node/src/store_server.rs` (new), `crates/seisin-node/src/remote_store.rs` (new), `crates/seisin-node/src/config.rs`, `crates/seisin-node/src/main.rs`, `crates/seisin-node/src/lib.rs`.

- `store_server::serve_store(listener: TcpListener, log: Arc<Mutex<DatumLog>>)` — accept loop, thread per connection, loop { read_frame, decode, lock log, execute (Put→put_full/Ack; Patch→put_delta→Ack|NeedFull; Get→Value; Delete→Ack), write_frame }. Malformed frame drops the connection. (One global log mutex is Part A honest — fsync dominates anyway; sharding is Part C.)
- `remote_store::RemoteStore { ring: Arc<RwLock<Ring>>, addresses: Arc<HashMap<NodeId, String>> }` implementing `Store`: thread-local `RefCell<HashMap<NodeId, TcpStream>>` connections (connect on demand, reconnect once on IO error, then panic); `get` → Get; `delete` → Delete; `put` → Put; `put_with_previous` → if `previous` is Some and `diff` yields `encoded_len < new/2` → Patch (on `NeedFull` reply, follow with Put) else Put. Panics name the storage node + datum id (documented fail-stop).
- Config: `MemberConfig` gains `role: String` ("compute"/"storage"), `store_address: Option<String>`, `capacity_weight: Option<u32>`; `NodeConfig` helpers `storage_ring_members() -> Vec<(NodeId, u32)>` and `store_address_book()`; existing sample configs updated (`role: "compute"`). `main.rs`: storage-role nodes bind `store_address`, open `DatumLog` under `data_dir`, run `serve_store`, and skip compute listeners; compute-role nodes with storage members configured build `RemoteStore` instead of `InMemoryStore`.

- [ ] Red where unit-testable (config parsing incl. roles/weights; RemoteStore round trip against an in-process `serve_store` on a tempdir log: put/get/delete, patch path via `put_with_previous` with a previous value, NeedFull fallback by patching an unknown id). Green; workspace gates. Commit `feat: add storage-role server, RemoteStore write-through, role config`.

### Task 6: integration, stress, docs

**Files:** `crates/seisin-node/tests/integration_storage_tier.rs`, `docs/superpowers/PROGRESS.md`.

Scenario: one storage "node" (in-process `serve_store` + tempdir log) + one real compute node whose pool uses `RemoteStore` (storage ring = 1 member); registered `put_first`/`get_first` byte ops:
- write through compute; **evict compute caches** (`pool.evict_non_native(|_| false)`); read again — served from storage.
- **durability**: stop the store listener thread, reopen the `DatumLog` from the same tempdir fresh (simulating restart), serve again; the acked write is still there.
- **delta amplification**: write a ~1 MiB datum, then rewrite it with a small middle change; assert the log file grew by ≪ datum size between the two writes.
- two-storage-node spread sanity: with weights (1, 3), a few hundred random ids map to both (ring function check, no I/O).
- Stress 10x; standing 20x compute suites; full gates; PROGRESS entry (Storage Tier Part A done; Parts B/C queued); push.

## Deferred (spec)
Part B (gossip/halt/migration), Part C (compaction, reweighting, tk/lb durability, CDC, group commit, hot LRU), copy/insert deltas, chunk-aware wire.
