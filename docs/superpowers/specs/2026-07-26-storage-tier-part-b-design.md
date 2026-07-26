# Storage Tier Part B — Membership & Coordinated Halt — Design

Date: 2026-07-26 (Sub-project 4, Part B of C)

## Overview & Scope

Storage nodes join the **existing SWIM gossip network, role-tagged** —
one membership mechanism, not a second pool (the original doc's "two
independent membership pools" survives as two independent *rings* fed
by one gossip fabric). A storage member confirmed dead triggers the
**coordinated fail-stop halt**: every compute node independently stops
serving client traffic (each one's own failure detector converges on
the same confirmed-dead update, so no separate halt broadcast is
needed). Resume is operator restart — no auto-resume in v1, because
distinguishing "storage came back intact" from "came back empty" needs
log-identity machinery this part doesn't build. **Data migration on
add/remove is Part C** (the doc already unifies it with capacity
reweighting): a storage node *added* via gossip serves only new
placements in the interim (documented operator note: don't add storage
nodes until Part C lands); a storage node *removed without drain* is
exactly the halt case, correctly.

## Membership Changes

`MemberUpdate` gains `role: MemberRole { Compute | Storage }`,
`capacity_weight: u32`, and `store_address: String` (empty for compute
members). `GOSSIP_PROTOCOL_VERSION` bumps to **2** — the layout
changed; the keep-the-old-decoder n±1 policy binds from the first
deployed release, and there have been none, so v1 decoding is dropped
(recorded here deliberately).

## Mutation Routing by Role

`RingMutation` is unchanged; **application** becomes role-aware, using
the member table (the wire's `thread_count` doubles as the weight for
storage joins). A new `ClusterState` bundle replaces the bare compute
ring at the three `apply_ready_mutations` call sites:

- Compute `Join`/`Leave` → existing behavior (compute ring, eviction,
  lock release).
- Storage `Join` → `storage_ring.apply_join(node, capacity_weight)` +
  insert into the shared store-address book (now `RwLock`ed inside
  `RemoteStore`'s map so gossip can extend it).
- Storage `Leave` (confirmed dead) → **set the halt flag** with a
  reason naming the node; rings untouched (routing to a dead store is
  moot once nothing serves).

## The Halt

`HaltState { halted: AtomicBool, reason: Mutex<Option<String>> }`
(`seisin-node::halt`), shared with the client-facing server: once set,
every client request answers `OpError { "cluster halted: ..." }`
before any dispatch. In-flight storage round-trips keep Part A's panic
as the backstop. The flag is per-process but converges cluster-wide
via each node's own detector.

## Storage Nodes on the Gossip Fabric

A storage node runs a **gossip responder only**
(`serve_gossip_storage`): merge incoming piggybacks, reply `Ack` with
its own piggyback — no rings, no pool, no probing loop (being probed
and acking is sufficient for SWIM liveness; compute nodes do the
probing). Its member row (role, weight, store_address) is seeded from
static config on every node, as today.

## Testing

Wire/membership round trips with roles (v2). Routing unit tests:
storage join updates the storage ring + address book; storage leave
sets halt with the node named; compute leave behavior unchanged.
Server: a halted node answers ops with the halt error. Integration
(`integration_storage_halt.rs`, mirroring the existing gossip
failure-detection test): compute node + storage node with a gossip
responder; ops work; kill the storage node's gossip listener; the
compute failure detector confirms it dead; subsequent client ops get
"cluster halted"; the reason names the storage node. Stress per house
discipline.

## Deferred

Part C: migration/reweighting, compaction, tk/lb durability, group
commit. Auto-resume with log-identity verification. Storage-node-side
halt of its own store listener (harmless to keep serving; compute has
already stopped asking).
