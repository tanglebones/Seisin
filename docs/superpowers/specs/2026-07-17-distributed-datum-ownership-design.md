# Distributed Datum Ownership Architecture

Date: 2026-07-17

## Overview & Goals

Seisin is a cluster of servers that collectively own a keyed dataset and
serve client operations against it. The core idea under test (per the
project README): allocate each unit of data (a "datum") to a single owning
thread, and temporarily collate the datums involved in a multi-datum
operation onto one thread so the operation can execute without distributed
locking — then let ownership drift back to a natural home over time so no
single thread ends up owning everything.

This spec covers the v1 architecture: a real multi-node, multi-process
system (deployed via containers) that validates this ownership/routing/
collation model end-to-end. It intentionally excludes concerns unrelated to
proving the core model out (see Non-Goals).

**What Seisin actually ships (added 2026-07-20).** Seisin itself is not a
single running system — it's a set of base libraries (a server framework
and a paired client library) that a specific *solution* builds on top of.
A solution defines its datum types and the operations over them in code
using these libraries; that definition compiles into two deployable
pieces: a server executable (the compute/storage node binary, specialized
to that solution's datum types) and a client library tied to the same
datum/compute definitions. Everything in this doc describes the mechanics
those base libraries implement — the ring, gossip, collation, storage —
not a specific solution built on them.

## Non-Goals (v1)

- Authentication, authorization, or transport encryption. The system runs on
  a private network; if a connection is permitted by the network, the node
  trusts it.
- Storage replication / crash resilience for storage nodes (see "Storage
  Tier" — a storage-node crash is fail-stop for the whole cluster in v1).
  Double-write replication is a deferred future option.
- Cross-machine ops tooling/observability beyond what's needed to validate
  the protocol.
- ACID/multi-datum distributed transactions. Correctness for multi-datum
  operations comes from single-thread serialization after collation, not
  from a distributed commit protocol.

## Roles

A server is configured with either or both of two roles:

- **compute** — hosts owning threads. Each thread holds an in-memory working
  copy (cache) of the datums it currently owns and executes ops against them
  one at a time. Always accepts client connections (see below — there is no
  separate "endpoint" role).
- **storage** — durable source of truth for datum content, sharded across
  storage nodes via a capacity-weighted consistent-hash ring.

A node may run both roles. There is no separate client-facing "endpoint"
role: since clients connect directly to whatever node they believe is the
current owner (computed via hashing into the *compute* ring), every compute
node is a potential direct hash target for some datum_id and must accept
client connections regardless. An "endpoint" flag would either be redundant
(compute nodes need it always) or unreachable (nothing to route to without
compute) — so it collapses into the compute role entirely.

## Data Model & Addressing

- **datum_id**: a uuidv7 key identifying either:
  - a **primary-key datum** — a single keyed record, or
  - a **secondary-key (SK) datum** — a materialized index result, e.g.
    `sk:user.name:cliff` → `[(d_id, a_idx), ...]`. SK datums are lazily
    created on first query and invalidated (removed from both cache and
    storage) when the underlying indexed data changes. Because index
    contents can change concurrently, clients must treat SK results as
    best-effort and revalidate on any 1+N fetch pattern.
  - SK datums are **regular datums** in every respect — same ownership,
    caching, collation, and storage rules as primary-key datums. No
    special-casing anywhere in the ownership/routing machinery.
- **authority_idx**: identifies the current owner (node, thread) of a
  datum_id. It is a tagged value, not a bare hash bucket:
  - **Native** — resolved by looking up `hash(datum_id)` in the *current*
    compute consistent-hash ring → (node, thread). This is a live
    function of ring state and drifts correctly as ring membership changes.
  - **Foreign(node_id, thread_id)** — a direct pointer, stamped explicitly
    when a datum is collated onto a non-native thread. Never recomputed via
    hashing; this is what makes foreign-vs-native detection immune to ring
    membership changes happening concurrently with collation (a rehash-and-
    compare approach would be ambiguous under dynamic membership).
  - authority_idx is **never persisted to storage** — storage only ever
    knows datum content, never current ownership. On arrival back at its
    native-home thread, a datum's authority_idx is reset to `Native`
    (the `Foreign` pointer is simply discarded, not compared/reconciled).
- **op_id**: a uuidv7 generated client-side (not centrally assigned), used
  as the ordering token for wound-wait priority during collation contention.
  Because it's client-generated, ordering is time-*ish* but subject to
  clock drift across clients — sufficient to break livelock cycles, not a
  linearizability guarantee.

## Datum Type System (added 2026-07-20)

- Datums are **typed**, and each datum type is **homogeneous** — every
  datum of a given type has the same shape. Content is no longer just an
  opaque byte blob at the solution level (it's still opaque at the
  storage/replication layer — see "Storage Tier" — but a solution's
  server and client code operate on typed values, not raw bytes).
- A type definition's field types are: Rust's primitive types, arrays,
  and dictionaries (maps) whose keys are restricted to primitive types
  (values can presumably be any supported type, including nested
  arrays/dicts — exact nesting rules are still to be nailed down; see
  Open Questions).
- **Secondary indexes are declared as part of the type definition**, not
  constructed ad hoc by client queries. This ties the "which fields are
  indexable" decision to the schema itself, alongside the SK-datum
  mechanics already described above (an SK datum is still a regular
  datum; this section just fixes *where* the definition of one comes
  from — the owning type's schema, not caller-supplied index
  expressions).
- **Index types** (added 2026-07-23, to be detailed later): every datum
  type declares one or more indexes, each of a specific kind:
  - **pk** (primary key) — required on every datum type; this is the
    datum_id itself.
  - **sk** (secondary key) — the SK-datum mechanism already described
    above.
  - **rk** (stochastically ranked) — a ranked/sampled index; mechanics
    not yet designed.
  - **tk** (temporal) — a time-oriented index; mechanics not yet
    designed.
- **Relational constraints** (e.g. foreign-key-style references between
  datum types) exist in the type system, but full synchronous
  update-time enforcement may not be achievable given the ownership
  model — enforcement is likely **eventual or advisory** rather than a
  hard constraint checked on every write. The exact mechanism is still
  open — see Open Questions.

## Deployment & Schema Evolution (added 2026-07-20)

- A **central deployment management system** handles rolling out cluster
  updates. It is not part of the always-running cluster — it's only
  invoked when a deployment is actually happening.
- **Every node must remain compatible with the immediately prior version
  (n-1)** — a node on version `n` must be able to interoperate (wire
  protocol, gossip, datum type handling) with peers still on `n-1` during
  a rolling update.
- **A new deployment can only be started when every node in the cluster
  is currently on the same version `n`** — deployments don't stack; the
  cluster must fully settle on a uniform version before the next rollout
  begins.
- **Update order within a single deployment: storage nodes first, then
  compute nodes, then clients.** This ordering exists so that by the time
  compute nodes (and then clients) start expecting a schema/behavior
  change, the storage tier underneath them already supports it.
- **Datum type evolution rules**:
  - New datum types, and new fields on an existing type, can be added
    freely.
  - Existing types/fields can be deprecated, and deprecated types/fields
    can subsequently be removed — but only after having been deprecated
    first (no direct removal of a still-active type/field).
  - **No renames.** A rename is modeled as an alias on the original
    name, never as an in-place identity change — this keeps the n/n-1
    compatibility guarantee simple (an n-1 node or client that only knows
    the old name still resolves to the same underlying thing via the
    alias, rather than needing to understand a rename event).

## Routing & Ownership Protocol

- Clients hold a `(datum_id, authority_idx)` pair from whenever they last
  learned it (creation, a prior response, etc.) and connect directly to
  whatever node that resolves to. There is no client-facing routing tier
  distinct from compute nodes.
- When a node receives a request for a datum_id it doesn't currently own:
  - If it's the native home and knows the datum is currently transferred
    elsewhere, it relays to the (node, thread) it has on record.
  - If it's not the native home either (client's authority_idx is doubly
    stale, e.g. after a ring membership change), it forwards toward the
    native home, which relays onward if needed.
  - If it's the native home and has no transfer on record, it owns the
    datum natively — normal path (cache, or load from storage on miss).
- **Transfer bookkeeping** lives only at the native-home thread: "currently
  owned elsewhere, at (node, thread) X." Non-native holders track nothing
  beyond their own current holdings.
- **Lazy crash/drain recovery, no heartbeats for ownership**: if a relay/
  forward to X fails (connection refused/timeout), the native-home thread
  immediately reclaims authority and reloads from storage on next access.
  A clean node exit and a crash look identical from the ring's perspective
  — on shutdown, a compute node just flushes any dirty state (already
  guaranteed by write-before-ack) and exits; there is no active handoff
  step.
- **Livelock avoidance (wound-wait by op_id)**: when two ops' collation
  requests contend for the same datum, the request carrying the lower
  (older) op_id wins; the higher (newer) op backs off and retries. This
  prevents a datum ping-ponging indefinitely between two competing
  multi-datum ops.

## Collation & Op Execution

- Each owning thread has a single inbox/queue and executes exactly one op
  at a time against the datums it currently holds (native + foreign).
- **Op → thread assignment**: for a multi-datum op, pick the thread that
  already owns the most of the involved datums (native or already-foreign),
  minimizing the number of new foreign pulls needed.
- **Collation**: for each datum not already on the chosen thread, the
  thread requests a transfer (native home relays if the datum is currently
  elsewhere). On arrival, the datum is tagged `Foreign(self.node_id,
  self.thread_id)` — trivial to mint, since the collating thread always
  knows its own address; no search for a pre-existing usable authority_idx
  value is needed.
- **Post-op placement**: immediately after the op completes, every foreign
  datum used is sent back toward its native home — *unless* the thread's
  very next queued op already needs that same foreign datum, in which case
  it stays for that one hop. This is the anti-degeneration rule: without
  it, ownership tends to collapse onto a single thread over time.
- **Write-before-ack**: any datum mutated during the op is written through
  to storage before the op's response is acknowledged to the client. This
  is what makes lazy crash recovery safe — nothing acknowledged is ever
  only-in-memory.

## Storage Tier

- Storage nodes form their own consistent-hash ring, entirely independent
  of the compute ring's membership and keyspace mapping.
- The ring is **capacity-weighted**: because datums vary in size and pure
  hash-based placement would not yield even space usage, nodes with more
  free space claim proportionally more virtual buckets. Placement/lookup
  remains a pure function of `(datum_id, current ring + weights)` — no
  separate directory service is introduced. Capacity reweighting is handled
  by the same migration mechanism as membership changes, not a separate
  system.
- Storage never tracks current ownership/authority, only content — that
  state lives entirely in compute-side memory (see authority_idx above).
- On cache miss, a compute thread reads through to storage (hashing into
  the storage ring); on write, it writes through before ack.
- **No replication in v1.** A storage node crash is unrecoverable for the
  data it held. This is a deliberate fail-stop choice: **the whole cluster
  halts** (stops serving all client traffic) rather than risk serving from
  a partially-lost dataset. This is a deliberate asymmetry with compute
  nodes, whose crashes are fully recoverable via lazy reclaim.
- **Adding a storage node** triggers migration onto it per the reweighted
  ring, prioritizing datums currently involved in live ops first, so the
  cluster can ideally stay available through the migration.
- **Removing a storage node** must fully migrate its data off to other
  nodes before it's allowed to leave the ring — unlike compute's "just
  exit, lazy reclaim handles it," storage has no fallback if data
  disappears mid-removal.
- **Deferred to later (not v1)**: double-write replication (writing to two
  storage nodes) for crash resilience, at 2x storage cost and slower
  writes. The storage-node interface and placement scheme should stay open
  to this without being built now.

## Cluster Membership

- Two independent membership pools: the **compute ring** and the
  **storage ring**. A node advertises which role(s) it holds; role
  membership changes only affect the corresponding ring(s).
- Dynamic membership via gossip (SWIM-style): periodic ping/ping-req
  between random peers, suspicion timeout, then confirmed-dead broadcast.
  Chosen over static config despite v1's container-based deployment,
  because migrating a static-membership system to dynamic later has
  historically been a significant rewrite.
- Membership-confirmed-dead is a separate signal from the lazy per-datum
  reclaim path: gossip updates the relevant ring so *new* native-home
  hashing routes around a dead node going forward, but reclaiming any
  specific already-transferred datum still happens lazily, on relay/forward
  failure — not as a reaction to the gossip signal.

## Cache Invalidation on Ring Membership Change

Ring membership changes (compute node add/remove) shift native ownership
for roughly 1/n of keys, independent of any crash or collation transfer.
This is a distinct failure mode from the ones covered above and needs its
own rule: a node must never serve or build on a stale local cache entry
left over from a previous era of owning a key, and must not let unused
stale entries accumulate unboundedly.

Example: node B joins, becomes native home for datum X (previously A).
B loads X (cache miss from storage), commits a change to X (write-before-
ack), then B leaves. Native ownership for X reverts — potentially back to
A. If A still had a stale pre-B cache entry for X lying around and trusted
it instead of reloading, B's committed write would be silently lost.

**Revised during Sub-project 2b design.** The original plan below assumed
a vnode-style ring where `datum_id → vnode bucket` is stable across
membership changes and only `vnode bucket → node` shifts. That doesn't
hold for jump-consistent-hash: under the swap-with-last mutation (see
"Compute Ring Mechanics"), only the removed slot and the former
last-index slot actually change physical identity — every other slot
keeps both its index *and* its node — but there's no stable notion of
"which bucket did this key move out of" to partition a cache by, the way
an actual vnode ring would allow.

Re-examining what's actually required: since every request re-derives
`ring.native(datum_id)` fresh before deciding to serve or redirect (see
"Server relay logic" in Sub-project 2a), a stale cache entry for a key
this node no longer natively owns is harmless on its own — the node will
simply keep redirecting requests for it elsewhere and never touch that
entry again. The only genuinely dangerous case is the one below: a node
**regains** native status for a key without ever having noticed it lost
it, and serves the stale pre-handoff content straight from cache. So the
correctness fix only needs to prevent *that* — no cluster-keyspace-wide
bucket-partitioning apparatus is needed.

- **Correctness — regained native ownership is always a cache miss.**
  On each locally-applied ring mutation, a node scans its *own* cache
  (not the whole cluster's keyspace — just whatever it's actually holding)
  and evicts any entry whose native owner under the *new* ring is no
  longer itself. This guarantees that if this node later regains native
  status for that key, it's a hard cache miss — reload from storage —
  rather than serving a stale value that might be missing another node's
  write-before-ack'd change in the interim. This is O(this node's cache
  size) per mutation, not O(#reassigned slots) as originally hoped, but
  it's simple, obviously correct, and reasonable at this project's scale
  (container-based test clusters, not millions of entries per node).
  Bounding this more tightly than "scan my own cache" is left as future
  work if it ever becomes a real bottleneck.

## Execution Model & Wire Protocol

- **Execution model**: OS thread per authority slot. A compute node runs a
  fixed pool of real OS threads, each with its own inbox, executing one op
  at a time. This maps directly to "a thread owns a datum" and avoids the
  complexity of an async runtime.
- **Wire protocol**: a custom, minimal binary framing over raw TCP, used
  for client↔compute traffic, node↔node relay/transfer traffic, and gossip.
  No auth or encryption layer (see Non-Goals).

## Deployment & v1 Scope

- v1 target: a containerized multi-node deployment (e.g. docker-compose),
  each container running a node process with compute and/or storage roles
  enabled, communicating over a real network — validates the protocol over
  actual sockets rather than in-process simulation.

## Testing Strategy

Testing should exercise, at minimum:
- Ownership handoff correctness (native ↔ foreign transitions, relay
  chains, reset-to-native on return).
- Wound-wait livelock avoidance under deliberately contended multi-datum
  ops.
- Lazy reclaim after a node is killed (both compute and storage nodes),
  and confirmation that a clean exit behaves identically to a crash from
  the ring's perspective.
- Ring rebalancing on node join/leave, and on storage capacity reweighting.
- The anti-degeneration (datum-return-home) rule under sustained
  multi-datum op load, confirming ownership doesn't collapse onto a single
  thread over time.
- Node add/drop cycling: a node joins post-standup, takes native ownership
  of some live keys, commits changes to one, then drops — confirming the
  key's reverted native owner reloads from storage rather than serving a
  stale pre-join cache entry, and that bulk cache eviction on membership
  change stays cheap (not a full-cache scan) as node count grows.

## Compute Ring Mechanics (added during Sub-project 2 design)

The Routing & Ownership Protocol section above establishes *that* a native
authority is resolved via consistent hashing through the compute ring;
this section fixes the concrete mechanism, decided while designing
Sub-project 2 (Compute Ring & Routing) rather than in the original pass.

- **Thread count per node**: derived from the node's own available CPU
  core count at startup, not manually configured. A node announces
  `(NodeId, address, thread_count)` when it joins.
- **Ring algorithm**: jump-consistent-hash (the project's own
  `consistent_hash` crate, vendored — see `SUBMODULE.md`-independent
  copy-in per its README) over a flat `slots: Vec<(NodeId, ThreadId)>`
  array. `native(datum_id) = slots[hash(datum_id, slots.len())]`.
  - **Join**: append the new node's thread-slots to the end of the array.
  - **Leave**: for each of the departed node's slots, swap it with
    whatever currently occupies the last index, then truncate by one —
    applied one slot at a time in a fixed order (ascending index) so the
    result is identical regardless of which node computes it. This is the
    standard technique for handling arbitrary-node removal with a
    jump-consistent-hash family algorithm while preserving its minimal-
    remap guarantee; naively re-sorting live nodes by id and reassigning
    contiguous indices from scratch was considered and rejected — it
    converges correctly but reintroduces the same churn characteristics
    as plain modulo hashing (up to O(n) keys reshuffled for one node
    leaving), defeating the reason to use consistent hashing at all.
  - Rendezvous (HRW) hashing was also considered — it sidesteps ordering
    entirely by being a pure function of the live node *set* — but was
    rejected in favor of keeping O(1) lookups via jump-consistent-hash,
    given the ordering problem below has a clean fix.
- **Ordering (why the swap-with-last mutation is safe to trust)**: SWIM
  gossip (Cluster Membership section above) has no built-in global event
  order, but the array mutation above is order-sensitive — two nodes
  applying the same join/leave events in different orders would compute
  different arrays. This is fixed with a lightweight elected **epoch
  sequencer**: whichever live node currently has the lowest `NodeId` (the
  same deterministic, coordination-free selection Akka Cluster uses for
  its leader) assigns the next monotonic epoch number to each ring
  mutation and gossips `(epoch, mutation)`. The sequencer only orders
  mutations — every node still computes the array itself, replaying
  mutations strictly in epoch order (buffering any that arrive out of
  order). If the sequencer dies, the next-lowest-`NodeId` live node
  resumes sequencing from the last epoch it observed via anti-entropy; no
  separate failover protocol is needed since "who is the sequencer" is
  always a pure function of current membership.
- **Relay mechanism**: client-side redirect, not node-to-node proxying. A
  node that isn't the current owner of a requested datum replies with a
  `Redirect(address)` response naming the correct node; the client
  reconnects there itself. This avoids an extra node-to-node hop on every
  misrouted request, at the cost of the client needing a redirect-follow
  loop.

**Sub-project 2 is split into two plans** because of how much is bundled
above:
- **2a — Ring & redirect routing**: the ring data structure and mutation
  replay, `NodeId`/`ThreadId` addressing, the `Redirect` wire message, a
  redirect-following client helper, and relay logic — proven against a
  *static*, config-supplied initial membership (no runtime join/leave
  yet). The ring API is built around replaying an ordered mutation log
  from the start, so this isn't the same "static now, rewrite for
  dynamic later" trap as a hardcoded membership list would be.
- **2b — Dynamic gossip membership**: SWIM ping/ping-req/suspect/dead, the
  epoch sequencer, and the ring-epoch cache invalidation rules — layered
  on top of 2a's ring as a new *source* of mutations, without changing the
  ring or relay logic itself.

## Dynamic Gossip Membership Mechanics (added during Sub-project 2b design)

Concrete mechanics for the SWIM gossip layer and the epoch sequencer
introduced conceptually above.

- **Transport**: gossip uses the same custom binary framing as the
  client/relay protocol, but over a separate `gossip_address` per node
  (distinct from the client-facing address) — keeps the client wire
  protocol and the gossip wire protocol as two independent message
  namespaces rather than multiplexing both over one socket.
- **SWIM parameters**: 1-second probe interval, indirect-probe fanout of
  3 random peers on a direct-probe timeout, 5-second suspicion timeout
  before a suspected node is declared dead. These are plain constants for
  v1, not yet configurable.
- **Membership state**: each node tracks, per known member, an
  incarnation number and a status (`Alive` / `Suspect` / `Dead`), plus its
  client address, gossip address, and thread count. Status changes and
  incarnation bumps (a node refuting a false suspicion) are disseminated
  by piggybacking a bounded batch of recent updates onto every ping/ack
  message — standard SWIM infection-style dissemination.
- **Ring mutations are derived from membership transitions, not a
  separate message type**: a node's first-seen `Alive` status is a Join;
  a confirmed `Dead` status is a Leave. The elected epoch sequencer (the
  live member with the lowest `NodeId` — see "Compute Ring Mechanics")
  assigns the next epoch number to each such transition and piggybacks
  `(epoch, mutation)` records on the same gossip messages, alongside the
  raw membership updates. Every node applies mutations to its `Ring`
  strictly in epoch order, buffering any that arrive ahead of the next
  expected epoch.
- **`Ring` grows mutation methods** (`apply_join`, `apply_leave`) that
  implement the append / swap-with-last operations described in "Compute
  Ring Mechanics", replacing Sub-project 2a's build-once-from-static-list
  behavior. Existing `native()` lookups are unaffected — this was the
  point of shaping `Ring`'s API around a mutation log from the start.
- **Cache invalidation** on each applied mutation follows the revised
  rule in "Cache Invalidation on Ring Membership Change" above: scan this
  node's own cache and evict entries whose native owner under the new
  ring is no longer this node.

## Op Registry & Collation Mechanics (added during Sub-project 3 design)

Concrete mechanics for how a "multi-datum op" (referenced in "Collation &
Op Execution" above) is actually defined and invoked, decided while
designing Sub-project 3 — resolves the framework-shape question from the
2026-07-20 notes for this specific piece.

- **Ops are solution-defined Rust functions, not a generic wire-level
  batch operation.** A solution registers named operations (a function
  taking mutable access to the datums it needs, plus an op-specific
  payload) with the server framework at startup. The wire protocol
  carries an op identifier, the *caller-supplied* explicit list of
  datum_ids the op needs (the framework can't discover this by inspecting
  arbitrary Rust code, so the client must state it up front), and an
  opaque payload (the op's arguments, serialized however the solution
  chooses). The framework's job is purely collation + invocation +
  write-back — it never interprets the op's internal logic.
- **`OpContext`** is the interface an op function operates through:
  byte-level `get`/`put`/`delete` over exactly the collated datums (not
  the typed layer from the Datum Type System notes above — that's a
  separate, not-yet-designed layer a solution's own generated code would
  sit on top of; the framework itself stays type-agnostic).
- **Panic safety**: op invocation is wrapped so a panicking op function
  doesn't take down its owning thread (and every other datum that thread
  owns) — the panic is caught and converted into an error response,
  rather than being allowed to unwind through the framework's dispatch
  loop.
- **Thread assignment (v1 simplification)**: the chosen thread is
  whichever local thread *natively* owns the most of the op's requested
  datum_ids — a pure function of the ring, requiring no visibility into
  what any thread's cache currently happens to hold. The original
  design's "or already-foreign" refinement (preferring a thread that
  already has a datum on hand, even non-natively) is deferred; it would
  require a live reverse-index of cache contents that isn't needed to
  prove the core mechanism.
- **Cross-node transfer** reuses the existing client-facing wire
  protocol and port rather than standing up a third protocol: a new
  request variant lets one node ask another for a datum it needs to
  collate. The request always goes to whoever `ring.native(datum_id)`
  currently resolves to, exactly like a client request would — if that
  node currently holds the datum, it hands it over (evicting its own
  copy and, if it's the native home, recording "currently elsewhere");
  if not, it relays/redirects exactly as client requests already do.
  Reusing this machinery (rather than inventing a parallel one) is
  possible because "the current owner replies or points elsewhere" is
  already exactly what the client protocol does.
- **Sub-project 3 is split**: **3a** proves the op-registry, `OpContext`,
  thread-assignment, and write-back/anti-degeneration mechanics for the
  *single-node, uncontended* case (every requested datum_id already
  natively on the one running node, no concurrent op wants the same
  datum). **3b** adds cross-node transfer and wound-wait contention
  handling (the case where two ops' collation requests race for the same
  datum) on top, without changing 3a's op-invocation mechanics.

## Open Questions / Future Work

- Storage replication (double-write) for crash resilience.
- Whether/how to support internal-only compute nodes not directly
  reachable by clients, if that's ever needed (would require rethinking the
  "client hashes directly into the compute ring" assumption).
- **Relational constraint enforcement mechanism.** Whether constraints
  between datum types are checked eventually (a background
  reconciliation pass), advisory-only (surfaced but never blocking a
  write), or some mix per-constraint — not yet decided.
- **Exact type-system nesting rules.** Whether dictionary/array values
  can themselves be arrays/dicts (arbitrary nesting) or only primitives,
  and how deeply — not yet decided.
- **Deployment management system's own design** — how it detects "all
  nodes on version n," drives the storage→compute→client rollout order,
  and what happens if a rollout is interrupted partway through. Not yet
  designed at all; the rules above are constraints on it, not a design
  for it.
