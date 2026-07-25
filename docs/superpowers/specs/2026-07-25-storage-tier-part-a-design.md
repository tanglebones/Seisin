# Storage Tier Part A — Design

Date: 2026-07-25 (Sub-project 4, Part A of C)

## Overview & Scope

The durable source of truth for datum content, per the original design
doc's "Storage Tier" section: dedicated storage-role nodes on their own
capacity-weighted ring, compute writing through **before ack**. Part A
delivers the blob path end to end over a **static** storage ring:

- the delta-log disk store (fsync-before-ack, crash-recoverable),
- the dedicated storage wire protocol,
- `RemoteStore` on compute (the existing `Store` trait, unchanged),
- role/weight configuration.

Part B: storage-pool gossip, coordinated halt-on-shard-loss, node
add/remove migration. Part C: log compaction (with delta re-basing
policy tuning), capacity reweighting, tk/lb datum-grade durability for
compute-local B+Tree files, optional CDC dedup for full writes.

Unchanged invariants from the original doc: storage stores **content
only** (never ownership/authority); placement is a pure function of
`(datum_id, ring + weights)`; **no replication in v1** — a storage
crash is fail-stop for the cluster.

## Storage Stays Content-Agnostic (decision record)

Semantic field-path patches (`(inventory.0.count, 2)`) were considered
and rejected *at this layer* for two hard reasons:

1. **Deployment order**: storage rolls out first in the n -> n+1
   sequence precisely because it sits below the schema; a storage tier
   that interprets typed paths turns every schema migration into a
   storage deploy concern.
2. **No def to apply**: a datum id names no type (no type registry
   exists), so neither storage nor a cold reader can materialize
   `base + typed patches`. The tagless encoding is deliberate and
   isn't navigable without the schema.

Path-addressed updates belong in a future compute-side typed patch
surface (macro-DSL territory), compiling down to the byte deltas below
— same ergonomics, right layer.

## The Delta Log (new `seisin-storage` module: `datum_log`)

One append-only log file per storage node
(`{data_dir}/datum_log.dlog`), three record types, each framed
`[u32 len][u8 kind][body][u32 crc32-of-kind+body]`:

- `Full { id, bytes }` — a datum's complete content.
- `Delta { id, delta }` — a byte delta against the datum's current
  materialized value (see Delta Encoding).
- `Tombstone { id }` — deletion.

A superblock-style header (magic + format version u16) starts the file
— versioned like every other encoding in this project.

**In-memory state** (rebuilt by a full scan on open): `id ->
materialized bytes`? No — that's the whole dataset in memory. Instead:
`id -> LogRef { base_offset, delta_offsets: Vec<u64>, materialized_len
}` — reads seek the base and replay deltas. A small LRU of hot
materialized values is a Part C option, not v1 (the compute cache
already absorbs read traffic).

**Write path**: append record, `fdatasync`, then ack — the
write-before-ack rule, literally. Group commit is a contained later
optimization behind the same wire contract.

**Delta application is mechanical byte splicing** — no schema
knowledge. `Get` always returns fully materialized bytes.

**Self-rebasing**: when a Put-as-delta would make a datum's chain
exceed `MAX_DELTA_CHAIN = 8` deltas, or cumulative delta bytes exceed
half the materialized length, the store materializes and appends a
consolidating `Full` instead (one record, one fsync — same ack
latency class). Bounds read-time replay and pre-shrinks Part C's
compaction problem.

**Recovery**: scan from the header, verifying each record's CRC and
rebuilding the index. The first CRC/length failure truncates the log
there (a torn tail from a crash mid-append): everything acked was
fsynced and therefore precedes the tear; nothing unacked survives.
A delta whose base was truncated away cannot occur (bases precede
deltas by construction and truncation only removes a suffix).

## Delta Encoding (computed on compute, applied by storage)

v1 is prefix/suffix trim: compare old and new bytes, find the longest
common prefix and the longest common suffix of the remainder; the
delta is `{ prefix_len: u32, suffix_len: u32, middle: Vec<u8>,
new_total_len: u32 }`. Apply = `old[..prefix] ++ middle ++
old[old_len-suffix..]` (with strict bounds validation — a malformed
delta is a storage-side error, never a silent corruption).

This captures the driving cases: a fixed-width field change in a huge
datum is a few bytes; a length-changing field trims the shared prefix
and the shifted-but-identical suffix. Many scattered changes degrade
toward `middle ~= whole datum`, at which point compute sends `Full`
instead (see threshold below). Copy/insert (xdelta-style) is a
drop-in upgrade behind the same record kind, deferred.

**Compute side**: at write-through, if the worker's cache holds the
old bytes and `delta.encoded_len < new_len / 2`, send
`StorePatch { id, delta }`; otherwise `StorePut { id, bytes }` (cold
cache, new datum, or a poor delta). Deletes send `StoreDelete`.

## Storage Wire Protocol (new module `store_wire` in `seisin-protocol`)

Independently versioned (`STORE_PROTOCOL_VERSION: u8 = 1`, leading
byte per frame, same n±1 keep-the-old-decoder policy):

```
StoreRequest::Put    { id, bytes }   -> StoreResponse::Ack
StoreRequest::Patch  { id, delta }   -> StoreResponse::Ack | NeedFull
StoreRequest::Get    { id }          -> StoreResponse::Value { bytes: Option<Vec<u8>> }
StoreRequest::Delete { id }          -> StoreResponse::Ack
```

`NeedFull` covers the one legitimate divergence: a patch arriving for
an id the log doesn't know (e.g. compute cache believed in a value the
log never saw — must not silently apply a delta to nothing). Compute
answers by re-sending `Put` with full bytes.

Length-framed over plain TCP, **one connection per compute worker
thread**, blocking request/response — `Store` is a synchronous trait
and each thread owns its connection; no multiplexing needed (the
peer-link envelope machinery is deliberately not reused).

## Storage Ring & Configuration

`MemberConfig` gains `role: NodeRole` (`Compute | Storage`) and
storage members `store_address: String` + `capacity_weight: u32`
(virtual-bucket count). The storage ring reuses the existing `Ring`
with `capacity_weight` in place of thread counts — same
jump-consistent-hash placement, pure function of id + ring, no
directory service. Static in Part A; reweighting/migration are Part
B/C. Compute members keep their existing fields; the node binary
specializes by role at startup (a storage node runs only the store
listener + log; it joins no compute gossip in Part A).

## Compute Integration: `RemoteStore`

Implements `seisin-core::Store` unchanged: `get` -> storage-ring
lookup -> `StoreGet` round trip; `put` -> delta-or-full decision (the
old bytes come from the worker cache — `Cache` write-through calls
`Store::put` while still holding the previous value) -> blocking until
the fsynced ack; `delete` -> `StoreDelete`. Because `Cache` already
writes through synchronously on the owning thread, **write-before-ack
falls out with zero changes to worker/op-lifecycle code**.
`InMemoryStore` remains the unit-test store.

Note on the delta decision's data flow: `Cache::put(id, new)` must
pass the *previous* cached bytes to the store for delta computation —
`Store` gains a defaulted method `put_with_previous(id, new, previous:
Option<&[u8]>)` that `InMemoryStore` ignores (delegating to `put`) and
`RemoteStore` uses; `Cache` calls it. Existing `Store::put` callers
are unaffected.

## Failure Semantics (Part A)

Fail-stop, honestly minimal: any storage round-trip failure (connect
refused, disconnect mid-call, malformed or unexpected reply, `NeedFull`
loop) **panics the compute worker** with a message naming the storage
node and datum — v1 of "the cluster halts rather than serve from a
partially-lost dataset". Coordinated cluster-wide halt arrives with
Part B's storage-pool membership. The `Store` trait's infallible
signatures stay unchanged; the policy is documented on `RemoteStore`.

## Testing Strategy

- Delta codec: known-answer trims (fixed-width middle change,
  length-changing change with shifted suffix, identical inputs, fully
  different inputs), apply-side bounds validation rejecting malformed
  deltas loudly, round-trip property vs a model (`apply(old, diff(old,
  new)) == new` over an LCG-driven corpus).
- Datum log: put/get/delete round trips; delta chains materialize
  correctly; self-rebase triggers at both thresholds (chain length,
  cumulative size) and reads stay correct across it; reopen-recovery
  rebuilds the index; torn-tail truncation (corrupt/truncate the last
  record, reopen, acked prefix intact); format-version mismatch is a
  loud open error.
- Wire codec round trips incl. `NeedFull`; version byte enforced.
- Integration (`integration_storage_tier.rs`): a real storage process
  (thread with its own listener + tempdir log) + a compute node with
  `RemoteStore`: write ops through compute, evict the compute cache,
  read back through storage; **kill and restart the storage node's
  listener/log, verify every acked write survives** (the fsync
  contract); a large datum mutated in one field ships a small frame
  (observable: log file growth « datum size) and reads back exactly;
  two storage nodes with different capacity weights both receive data
  (ring spread sanity). Stress 10x + the standing 20x compute suites.

## Deferred

Part B: storage gossip pool, coordinated halt, add/remove migration.
Part C: compaction + rebasing-policy tuning, capacity reweighting,
tk/lb B+Tree durability, CDC dedup for full writes, chunk/delta-aware
replication hooks (double-write), group commit, hot-value LRU.
