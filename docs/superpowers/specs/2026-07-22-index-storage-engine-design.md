# Index Storage Engine (rk) — Design

## Overview & Goals

A standalone, disk-backed **counted B+Tree** engine for rk's ordered index
storage: a new crate, `seisin-storage`, with zero dependency on `DatumId`,
ring, gossip, or any node/placement concept. This sub-project builds and
tests the engine in isolation. Two follow-on pieces are explicitly deferred
to separate specs:

- **rk's `IndexKind` logic** (bounded declaration, load/apply against the
  Automatic Index Maintenance mechanism, query surface wiring) — built on
  top of this engine once it exists.
- **Node function model & per-function placement** (which node/thread's
  disk holds a given index's file — compute-only, storage-only, index-only,
  or any combination an operator chooses) — a separate change touching
  gossip's mutation log and sequencer (Sub-project 2b's machinery), since
  the engine itself has no opinion on where its files live.

**Scope boundary, explicit**: pk, sk, and tk are NOT part of this spec. sk
stays on its existing Part 2 (revised) bytes-based `IndexHandler` contract,
untouched. pk (point lookup only, never scanned) and tk (bitemporal,
per-entity) each have different access patterns and get their own
storage-engine decision later, evaluated against the research below on
their own terms — nothing here assumes they'll use this same engine.

## Why a Counted B+Tree

Backed by a dedicated research pass — see
`docs/superpowers/research/2026-07-22-index-storage-engine-choice.md` for
full findings, sources, and open questions. Summary: for rk's workload
(order-statistics/rank-augmented, insert/upsert-only, no crash-safety
requirement, single-writer, needs cheap bounded scans from both ends plus
cheap rank-based sampling), a counted B+Tree is the textbook-proven right
structure (CLRS-level: per-subtree counts give O(log n) `Select(i)`/`Rank(x)`
at no asymptotic cost to normal operations), with a real disk-backed
precedent (AELMDB, an order-statistics-augmented LMDB extension). LSM-trees
are architecturally hostile to rank queries (deferred, asynchronous
compaction destroys a global rank invariant — even the state-of-the-art LSM
range-query accelerator, REMIX, only speeds up scans, not rank/percentile
queries). Hash/radix structures have no ordering to support scans or rank
at all. Approximate quantile sketches (KLL, t-digest) are a legitimate
complementary technique for "roughly the median" but are unsuitable as a
*primary* index (no point upserts, no exact top-N, no per-key ordering).

## On-Disk Format

- **Fixed-size keys and values**, chosen at tree-creation time
  (`key_size`, `value_size` in bytes) and fixed for the tree's lifetime.
  rk's actual need is small and fixed (an 8-byte order-preserving numeric
  key, a 16-byte `DatumId` value) — this avoids the real complexity of
  variable-length records/overflow pages for no present benefit. A future
  consumer needing variable-length keys/values is out of scope; revisit
  if/when one exists.
- **Insert is upsert only — no delete** (matches "rk has no delete," an
  explicit scope decision this session). A real consequence: since nothing
  is ever freed, page allocation is a simple monotonically-growing counter
  — no free-list needed at all for v1.
- **Configurable page size, a power of 2, minimum 4096 bytes** — chosen at
  tree-creation time and fixed for the tree's lifetime (stored in the
  superblock, validated on `open`). 4096 matches the traditional OS page
  boundary, but modern storage hardware (SSDs with larger native
  erase-block/program-page sizes, NVMe devices) often performs better with
  larger pages; hardcoding 4096 would leave real throughput on the table.
  `create` takes an explicit `page_size` argument rather than defaulting
  silently, so a caller who hasn't decided yet has to either pass 4096 or
  make a real choice. **Auto-detection and a benchmark tool for the
  operator to determine the optimal page size on actual deployment
  hardware are explicitly deferred** — out of scope for this crate's v1 as
  long as `page_size` is configurable now; see "Open Questions Carried
  Forward" below.
- **Page 0 is a superblock**: magic bytes + format version + `page_size` +
  `key_size` + `value_size` + root page id + total entry count. `open()`
  validates the superblock (including that `page_size` matches what the
  file was actually created with) and returns an error (never a panic,
  never a silent misread) if it doesn't check out — the caller decides
  whether to treat that as "rebuild from a full datum scan" (see
  Durability below).
- **Leaf pages are sibling-linked** (prev/next page id) for bounded
  forward/backward scans without touching the whole tree.
- **Internal pages carry a subtree-entry-count alongside each child
  pointer** — the order-statistics augmentation that gives O(log n)
  rank-based descent (find the i-th smallest/largest entry without a full
  scan), which is what backs middle-sampling ("take every k-th entry").

## Operations

- `create(path, key_size, value_size, page_size)` — `page_size` must be a
  power of 2, `>= 4096`; rejected with an error otherwise (never silently
  rounded). `open(path)` — validates the superblock (including that
  `page_size` matches) and returns `Result`, not a panic, on
  mismatch/corruption.
- `insert(key: &[u8], value: &[u8]) -> Result<()>` — upsert; same key
  overwrites its value in place.
- `len() -> usize` — total entry count (tracked in the superblock/root, no
  full scan needed).
- `scan_forward_bounded(n: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>>` —
  walks leaf sibling links from the smallest key, up to `n` entries.
- `scan_backward_bounded(n: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>>` —
  same, from the largest key. Together these back top-N/bottom-N.
- `sample_by_rank(k: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>>` — k
  roughly-evenly-spaced rank-based lookups using subtree counts to descend
  directly to each target rank, without scanning the whole tree. Target
  ranks are 0-indexed, computed as `i * len() / k` for `i` in `0..k`
  (ascending order, evenly spaced across the tree's full 0-indexed range),
  each resolved via the same O(log n) counted descent as an individual
  rank lookup. Backs middle-sampling.
- `rebuild_from(entries: impl Iterator<Item = (Vec<u8>, Vec<u8>)>) ->
  Result<()>` — wipes the file and bulk-loads a balanced tree from the
  given entries. This is the recovery path: a caller who detects a
  missing/invalid file (via `open`'s error) re-derives entries from a full
  scan of the owning type's datums and calls this to rebuild.

## Durability Model

No WAL, no per-write fsync, no crash-safety machinery. This tree is a
rebuildable performance structure, not a source of truth — matches the
reasoning already established earlier in this project for why index writes
don't need to be fsynced before an op acks. The only safety net is the
superblock validity check on `open()`, so corruption is *detected* (an
`Err`, triggering the caller's rebuild path) rather than silently served as
wrong data forever.

## Testing Strategy

- Unit tests per operation against a temp file: create/open round-trip,
  insert/upsert semantics (same key twice doesn't grow `len()`), superblock
  validation rejects a corrupted/foreign file, `scan_forward_bounded`/
  `scan_backward_bounded` correctness against a range of tree sizes
  (including boundary cases: empty tree, single entry, `n` larger than
  `len()`), `sample_by_rank` correctness (verify sampled entries are at
  their claimed ranks), `rebuild_from` produces a tree equivalent (by full
  in-order traversal) to one built via sequential `insert` calls.
  Property-style tests inserting many entries in random order and
  verifying the tree's in-order traversal is sorted, entry count matches,
  and every inserted key is retrievable via a scan.
- `create` rejects a non-power-of-2 `page_size` and rejects `page_size <
  4096`; `open` rejects a file whose stored `page_size` doesn't match
  what's expected (a corrupted or mismatched-build-config file). Functional
  tests (correctness, not performance) run against at least two distinct
  valid page sizes (e.g. 4096 and 16384) to confirm the format and every
  operation are page-size-agnostic, not implicitly hardcoded to one value.
- No integration test against `seisin-node`/ring/gossip in this
  sub-project — this crate has no dependency on them, and none of its
  consumers (rk's `IndexKind`, node-function placement) exist yet.

## Open Questions Carried Forward

See the research doc's "Open questions surfaced, not yet resolved" section
for the fuller list. Most directly relevant to a future implementation
plan:

- Whether a fast-changing rk index needs a write-buffering front end
  (in-memory counted structure periodically flushed/merged into the
  on-disk tree) — not needed for v1; only revisit if a real workload
  demonstrates the plain counted B+Tree is insufficient. Note this shape
  is effectively a mini-LSM hybrid and would reintroduce the exact
  cross-run rank-reconciliation problem the research found LSM-trees
  handle poorly, so it is not a default direction.
- AELMDB's actual write-amplification/rebalancing cost under a
  fast-changing leaderboard workload is unmeasured in the literature;
  worth benchmarking Seisin's own implementation once built.
- Whether pk should eventually use a hash/radix engine (this crate does
  not build one) and whether sk or tk should eventually migrate onto this
  same counted B+Tree engine — both explicitly deferred, decided later
  against the research doc on each one's own access pattern.
- **Page-size auto-detection and an operator-facing benchmark tool** —
  explicitly deferred. `page_size` is configurable now (a required
  argument to `create`, validated as a power of 2 `>= 4096`), which is the
  part that must land in this plan; detecting a good default from the
  actual deployment hardware (OS page size, filesystem block size, SSD
  erase-block size) and a benchmark utility an operator can run to measure
  throughput/latency across candidate page sizes on their real hardware
  are both future work, tracked here rather than built now.
