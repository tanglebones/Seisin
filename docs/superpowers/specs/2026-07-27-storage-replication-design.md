# Storage Replication (Storage Tier Part C-2) — Design

Per-datum-type replication for the storage tier: an opted-in datum type is
persisted to N distinct storage nodes so a storage-node crash fails over to a
surviving replica instead of halting the cluster. Replication is **configured
per datum type** (in the type schema), not globally — app-aware, selective
replication is the thing Seisin can uniquely offer; whole-disk durability is
deliberately left to block-device replication (DRBD, cloud volumes) underneath
Seisin. Strong consistency is unchanged: the single owning compute thread
(wound-wait) remains the only serialization point; replication only adds
durable copies.

**Not in scope**: incremental catch-up of a returned replica (v1 does a full
resync via the driver); rack/zone-aware placement; read load-balancing across
replicas; changing a type's replication factor on already-written data;
whole-disk / block-device replication (external tooling, by design). Each is a
later part or an external concern.

## 1. Replication factor is a per-type schema property

- A datum type declares a `replication_factor` in its schema (`seisin-types`),
  defaulting to **1**. At N=1 the system is byte-for-byte today's behavior —
  single owner, fail-stop — so replication is a strict, opt-in extension, and
  every existing test of single-copy behavior must stay green unchanged.
- Untyped/raw datums and all index datums (sk/rk/tk/lb/partition — rebuildable,
  index-grade) are N=1. Only opted-in typed datum content is replicated.
- There is no global replication constant. N varies per datum, driven entirely
  by type.

## 2. N travels with the write and is stored per datum

Storage and the migration driver are type-blind (opaque bytes, raw-id
enumeration), but N is a per-type fact, so N must be recoverable where the type
is not known:

- The typed write path (which knows the type → N) tags every write with N. The
  storage node persists N as a small per-record **placement integer** in the
  `DatumLog` record header. Storage never acts on N — it stores it and reports
  it; all placement decisions stay compute-side.
- `ListIds` returns `(id, N)` pairs, so the migration/re-replication driver
  stays type-blind and uniform: for each `(id, N)` it compares the old and new
  replica sets.
- This keeps storage content-agnostic in spirit (N is durability/placement
  metadata, not content typing) at the cost of a 1–2 byte count on the store
  wire and in each log record.

## 3. Ring: `replicas(id, N)` — salted re-hash preserving the primary

`Ring` grows `replicas(&self, id, n) -> Vec<NodeId>` returning up to `n`
distinct nodes:

- Rank 0 is `native(id).0` — exactly today's owner, unchanged. `replicas(id, 1)
  == [native(id).0]`.
- For rank k≥1, hash a salted key derived from `(id, k)` into the ring, skip
  already-chosen nodes, advance the salt on a collision, until `n` distinct
  nodes are collected or the ring's distinct nodes are exhausted (a datum can
  have no more replicas than there are nodes).
- Deterministic, capacity-weight-biased (more slots → likelier at each rank),
  and additive: it does not touch `native`/`from_members`/`weights`, so the
  C-1 migration determinism and everything built on it are preserved.

The ordered list is the replica preference order: rank 0 is the "primary" (read
target), the rest are the failover order.

## 4. Write path: all-alive, ≥1 required

`RemoteStore` (which gains the replication factor per call and a handle to the
alive/stale sets and the `HaltState`) writes as:

```
targets = replicas(id, N) ∩ storage_alive ∩ ¬storage_stale
if targets is empty:  engage HaltState (total shard loss) + fail-stop the worker
fsync-write to every target (one connection per node on this worker thread)
ack once every target has acked
```

- Skipping a down replica is normal and does not halt; it leaves that replica
  stale, recovered later by the driver.
- The delta path (`put_with_previous`) applies per target independently: a
  target answering `NeedFull` falls back to a full `Put` for that target only.
- Because every write reaches all alive, non-stale replicas, each such replica
  is current.

## 5. Read path: primary, then fail over

Read from the first alive, non-stale replica in `replicas(id, N)` order (rank 0
first). On a connection failure mid-read, fail over to the next; engage the
halt only if none are reachable. Read-one is safe precisely because every write
reached all alive replicas, so any alive non-stale replica holds the current
value.

## 6. Membership, alive/stale sets, and point-of-use halt

- `ClusterState` gains two compute-side sets maintained by the gossip apply
  path: `storage_alive` (nodes gossip-up) and `storage_stale` (nodes confirmed
  dead and not yet re-replicated). A node is a valid serving replica only if it
  is in the ring **and** alive **and** not stale.
- **Confirmed storage death** (`apply_ready_mutations`, storage Leave): remove
  from `storage_alive`, add to `storage_stale`. It no longer halts at
  membership time.
- **A returned node** coming back `Alive` on gossip is **not** auto-cleared from
  `storage_stale` — this is what prevents a stale node from serving. Only the
  driver's re-replication clears it (§7).
- **The coordinated whole-cluster halt becomes point-of-use.** It moves from the
  membership path into `RemoteStore`: the first op whose every replica is gone
  engages `HaltState` (whole cluster, first-reason-wins, exactly as Part B/C-1)
  and fail-stops the worker. Effects:
  - A storage death no longer preemptively stops the cluster; it serves
    degraded, and only a genuine total loss of a shard trips the halt, on first
    access.
  - N=1 is unchanged in spirit: a single-copy datum on a dead node trips the
    halt the moment it is touched (its node was the only replica).
  - This is the one Part-B behavior shift — the halt fires on access to lost
    data, not at the instant of death — and it is required: with per-type N,
    compute cannot know at membership time whether a death caused unrecoverable
    loss (that depends on which datums the node held and their N).
- Resume-after-halt is unchanged from C-1: the driver verifies identity and
  clears the halt once the loss is recovered.

## 7. Recovery is the C-1 driver, generalized to replica sets

- `seisin-migrate`'s `plan_moves` generalizes from a single owner to the whole
  replica set: for each `(id, N)` from `ListIds`, compute `replicas(id, N)`
  under the old and proposed rings and move the datum to every newly-added
  replica node (a copy per new replica; the transfer engine already does
  per-id copies).
- Add / planned-remove / reweight all still work — they move replica *sets*
  instead of single owners. A migration that drops a node re-homes each of its
  replicas onto the next distinct node in `replicas(id, N)` order.
- **Recover-after-loss** is a `seisin-migrate recover` run: it proposes a ring
  without the dead node, restoring N onto survivors (and/or a replacement
  node), then flips. `InstallStorageRing` is extended to **clear its members
  from `storage_stale`**, so a re-replicated node is re-admitted as a current
  replica in the same atomic flip (`storage_alive` stays gossip-maintained).
- Consistent with the framework's stance: recovery is an operator-run driver
  program, never framework-self-initiated background work.

## 8. Wire and API additions

- **`Store` trait** (`seisin-core`): `put`, `put_with_previous`, and `get` gain
  a replication-factor parameter. `InMemoryStore` ignores it (N=1 semantics).
- **Store wire** (`STORE_PROTOCOL_VERSION` bump): `Put`/`Patch` carry N;
  `ListIds` result becomes `(id, N)` pairs. Pre-first-release: old decoder
  dropped, per the standing policy.
- **`DatumLog`** (`FORMAT_VERSION` bump): the record header stores N.
- **`Ring::replicas(id, n)`** (`seisin-ring`).
- **`seisin-types`**: a per-type `replication_factor` in the schema; the typed
  write/read drivers thread N into the `Store` calls.
- **`ClusterState`**: `storage_alive` and `storage_stale` sets; `RemoteStore`
  gains the alive/stale handles and an `Arc<HaltState>`.
- **Admin wire**: `InstallStorageRing` additionally clears its member set from
  `storage_stale` (re-admitting a re-replicated node). No new admin request is
  required beyond that.

## 9. Testing

Unit:
- `Ring::replicas`: rank 0 == native; N distinct nodes; determinism; graceful
  cap when N > node count; weight bias.
- Per-record N round-trips through the log and the store wire; `ListIds`
  returns `(id, N)`.
- Alive/stale set maintenance: death → alive-/stale+; a returned node stays
  stale until re-admitted.

Integration:
- **Replicated write + read-one**: a type with N=2 writes to two nodes; the
  datum reads back; both nodes independently hold it.
- **Read failover**: kill the primary; reads succeed from the secondary; no
  halt.
- **Degraded write**: with one replica down, a write acks to the survivor and
  the down node is left stale.
- **Total-loss halt (point-of-use)**: with N=2, kill both replicas of a shard;
  the first op touching it trips the whole-cluster halt.
- **N=1 unchanged**: a single-copy datum on a dead node trips the halt on
  access, exactly as C-1.
- **Driver re-replication**: kill a replica, run `recover`; N is restored on a
  survivor/replacement; the re-admitted node serves; the stale set is cleared.
- **Stale node not served**: a node that missed writes and returned is never
  read from until re-replicated.

Stress: 10× each new integration suite; standing 20× wound-wait /
cross-node-wound-wait / op-collation suites if `worker.rs` changes; full
workspace gates (`cargo fmt --check`, `cargo clippy --workspace --all-targets
-- -D warnings`).
