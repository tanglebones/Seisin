# Storage Migration & Reweighting (Storage Tier Part C-1) — Design

One unified mechanism for storage node add, planned remove, and capacity
reweight: a client-side migration driver that drains shards live, briefly
pauses the cluster to catch the tail, flips the storage ring everywhere,
and resumes. Plus the two operational companions that make it whole: log
identity (a restarted or moved storage node is provably the same data)
and storage-side self-halt (an isolated storage node stops serving before
it can diverge).

**Not in scope**: replication — crashes still halt (Part B behavior). A
crash remains recoverable only by restoring the node's log directory and
running the resume flow below. Also out: log compaction, tk/lb B+Tree
datum-grade durability, group commit, copy/insert deltas, chunk-aware
wire, hot-value LRU — each is its own later part.

## 1. A ring change is one thing: old ring → proposed ring

Add, remove, and reweight are all "the storage ring's member/weight set
changes." The driver takes the current ring and a proposed ring and
computes the **moved set**: every datum whose owner differs between the
two rings. Remove is a proposal without the node; add is a proposal with
it; reweight is a proposal with new weights. One mechanism serves all
three.

**Part B behavior change — Join no longer auto-extends the storage
ring.** Extending a consistent-hash ring re-homes existing keys whose
bytes never moved, so reads at the new owner would miss; the Part B
auto-extend is a latent bug once real data exists. In this part, a
gossiped storage Join only records the node as *available*: its store
address and log id go into the address/identity books, but it enters the
storage ring exclusively via a driver-run migration.

**Planned removal avoids the halt for free.** A storage Leave halts only
if the departing node is still in the storage ring. A drained node was
removed from the ring at flip time, so its later Leave is ignored.

## 2. Driver protocol: plan → bulk copy → pause → tail → flip → resume

The driver is a client-side admin program, same philosophy as the scan
drivers: the cluster only executes explicit commands, never
self-initiates rebalancing. Storage nodes stay ring-ignorant — the driver
does all ring math and hands storage explicit id lists.

1. **Plan.** Fetch the current storage ring, address book, and identity
   book from a compute node (new admin request `GetClusterConfig`).
   Enumerate each source's datum ids (new store-wire `ListIds`). Apply
   the ring diff to produce, per (source, destination) pair, an explicit
   list of datum ids to move.
2. **Bulk copy.** Send each source `Transfer { transfer_id, ids,
   dest_address }`. The source streams the current value of each id
   directly to the destination over the existing store wire (`Put`),
   while tracking a **dirty set**: any id in the transfer set written
   again after its snapshot read. Client writes keep flowing normally
   the whole time. The driver polls `TransferStatus { transfer_id }`
   for progress (copied count, dirty count, done flag).
3. **Pause.** Tell every compute node to pause (`Pause { reason:
   "migrating" }`). In-flight ops settle; new client ops are rejected
   with a distinct retryable error (see §3).
4. **Tail.** Send each source `FinishTransfer { transfer_id }`: it
   re-sends the dirty set's current values to the destination, then
   acks. Tail size is proportional to write traffic during the copy,
   not to the dataset.
5. **Flip.** Send every compute node `InstallStorageRing { members:
   [(node_id, weight, store_address, log_id)] }`. The shared
   `Arc<RwLock<...>>` ring/address structures from Part B make this a
   swap — no restart. The pause is held until every compute node
   confirms the install.
6. **Resume.** Un-pause all compute nodes. Then, and only then, send
   each source `Retire { transfer_id }` — it deletes the transferred
   ids from its log (tombstones). A fully drained node may then be shut
   down; its Leave will not halt (§1).

**Driver crash safety.** A crashed driver leaves the cluster running on
the old ring with some extra copies at the destination — inert and safe
(the destination's copies are unreachable until a flip names it owner).
Re-running the driver is idempotent: transfers are Put-based (last write
per id wins), `Transfer` with a fresh `transfer_id` simply re-copies, and
the flip is held under the pause until every compute node confirms. The
driver follows the two-phase plan→execute guideline: it prints the full
plan (moved counts per pair) and requires an explicit `--apply` flag to
execute.

**Failure mid-migration.** A storage death during migration halts through
the pause (halt beats pause, §3). The driver aborts on any command error,
leaving the old ring live; `Retire` is the only destructive step and is
gated behind flip confirmation from every compute node.

## 3. Pause: the halt gate grows a resumable flavor

`HaltState` becomes a two-flavor gate:

- **Halt** — permanent, first-reason-wins, exactly today's semantics.
- **Pause** — resumable, driver-owned, carries its own reason.

Same single check point in `serve`, before dispatch. A paused node
rejects with a distinct retryable mnemonic (`OpError` message prefixed
`"cluster paused"` vs `"cluster halted"`) so clients can distinguish
"retry shortly" from "cluster is down." Halt takes precedence over pause:
if both are set, the halt reason is what clients see, and `Resume` does
not clear a halt.

## 4. Log identity and resume-after-halt

- `DatumLog` stamps a **log id (UUIDv7) at creation** in the log header.
  Recovery reads it back; it never changes for the life of the log
  directory.
- New store-wire request `Identify` returns `(node_id, log_id)`. The
  storage node's gossip Join carries the log id alongside
  `store_address`.
- Compute nodes keep an **identity book** (log id per storage ring
  member) next to the address book. It is installed by
  `InstallStorageRing` and extended by storage Joins.
- **Resume flow**: after a halt caused by a storage death, the operator
  restarts the storage node on its original log directory. The driver
  runs `resume`: it calls `Identify` on the returned node, verifies node
  id and log id match the identity book (via `GetClusterConfig` from a
  compute node — readable while halted), then sends every compute node
  `ClearHalt`. The append-only fsynced log guarantees every
  previously-acked write is served after recovery.
- **Impostor detection**: same node id but a different log id (blank or
  wrong disk) — `resume` refuses and the halt stands, because acked data
  is provably not there. `ClearHalt` is only ever sent by the driver
  after identity verification; compute nodes never clear a halt on their
  own.

## 5. Storage-side self-halt

Fail-stop symmetry: a storage node normally hears constant gossip probes.
If it hears **nothing for longer than the suspicion window**, it must
assume the cluster may have declared it dead and **stops acking** store
requests — error replies (`StoreResponse::Error` with a reason), not a
panic, because it should resume serving as soon as gossip contact
returns (compute-side halt is the real fence; self-halt just closes the
window where a partitioned storage node keeps acking writes from an
equally partitioned compute node).

Mechanism: the ack-only gossip responder records a last-heard timestamp;
the store server checks it against a configured threshold (default: the
suspicion timeout) before serving. Fresh boot counts as "just heard"
so a node can start before its first probe arrives.

`StoreResponse::Error` is new to the store wire; the compute-side
`RemoteStore` treats it like any other failure — panic naming node and
datum (fail-stop), which the failure detector then converts into the
cluster halt.

## 6. Wire and API additions

Store wire (`STORE_PROTOCOL_VERSION` bump):
- `ListIds` → streamed/chunked id list.
- `Transfer { transfer_id, ids, dest_address }` → `Ack` (starts async
  copy; the storage node runs the copy on a worker thread).
- `TransferStatus { transfer_id }` → `{ copied, dirty, done }`.
- `FinishTransfer { transfer_id }` → `Ack` after the dirty tail is
  re-sent.
- `Retire { transfer_id }` → `Ack` after tombstoning transferred ids.
- `Identify` → `{ node_id, log_id }`.
- `Error { message }` response (used by self-halt and bad requests).

Client/admin wire (`PROTOCOL_VERSION` bump):
- `GetClusterConfig` → storage ring members + address book + identity
  book (served even while halted/paused — it is read-only control
  plane).
- `Pause { reason }` / `Resume` / `ClearHalt` / `InstallStorageRing {
  members }` → `Ack`.

Admin requests are trusted-network commands, consistent with the rest of
the wire (no auth story yet anywhere — unchanged by this part).

## 7. Interaction with tk/lb resident files

tk/lb B+Tree files are node-local and index-grade (rebuildable).
Migration moves only datum-log content; tk/lb resident state rebuilds at
the destination the same way it does after any restart. Datum-grade
durability for those files is a separate later part.

## 8. Testing

Unit:
- Ring-diff / moved-set math (add, remove, reweight; empty diff).
- Transfer dirty-set tracking: write during copy lands in the tail.
- Pause vs halt precedence; resume clears pause but never halt.
- Log id: stamped at creation, stable across reopen, `Identify` round
  trip, mismatch detection.
- Self-halt timestamp gate: stale → Error, fresh boot → serves.

Integration:
- **Live add**: 2 storage nodes, write a corpus through compute, driver
  admits a third; every datum readable afterward; placement matches the
  new ring.
- **Planned remove**: drain a node; its Leave does not halt; all data
  readable from the survivors.
- **Reweight**: weight change moves data; corpus fully readable.
- **Concurrent writes**: writes issued during the bulk-copy phase are
  present after the flip (dirty-set tail proven end-to-end).
- **Halt + resume**: storage node dies (halt engages), restarts on the
  same log dir, driver resume clears the halt, every previously-acked
  write reads back.
- **Impostor**: restart on an empty dir; resume refuses; halt stands.

Stress: 10× each new integration suite; standing 20× wound-wait /
cross-node wound-wait / op-collation suites; full workspace gates
(`cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D
warnings`).
