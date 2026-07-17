# Prior Art Survey: Distributed Datum Ownership Architecture

Companion to `2026-07-17-distributed-datum-ownership-design.md`. Produced via
a multi-agent research pass (108 agents: search → fetch 25 sources →
adversarially verify 25 extracted claims, 22 confirmed / 3 refuted).

## 1. Single-writer-per-key ownership (virtual actor / grain model)

- **Microsoft Orleans**: grains activate on-demand, deactivate when idle,
  execute single-request-at-a-time (no in-grain locking needed) — same
  shape as this design's compute-thread ownership. Durability is pluggable,
  external storage-backed. Placement is entirely runtime-controlled via a
  **one-hop DHT directory keyed by actor GUID**, and this directory's size
  scales with actor count — an explicitly flagged scalability constraint.
  This design avoids a separate directory service entirely (native-home
  tracking lives at the hash-derived thread itself), which is an advantage
  over Orleans' approach.
- **Akka / Akka.NET Cluster Sharding**: enforces the same single-writer-
  per-entity invariant ("one entity instance may live only at one node at a
  time"), but its directory is bounded by **shard count**, not entity
  count. Notably, **on rebalancing Akka does NOT migrate live in-memory
  state** — it hands the entity off and relies on reloading from durable
  persistence on the new node. This design's direct in-memory Foreign-tag
  transfer during collation is a heavier-weight choice than Akka's, a
  deliberate trade-off (locality/performance vs. Akka's simpler "just
  reload") rather than an oversight.
- **Cloudflare Durable Objects**: explicitly modeled on Orleans; activates
  lazily, storage is the durable source of truth, in-memory state is not
  itself durable. Matches this design's storage-is-truth/compute-is-cache
  model closely.
- Ray, Temporal, and Erlang/BEAM did not surface usable sources in this
  pass — a gap for a follow-up search if desired.

## 2. Multi-key collation without 2PC

- **Lotus (VLDB 2022)** is the closest direct match: migrates/collates
  multi-partition transaction execution onto a single partition-owner
  thread, runs it via granule-level locking + deterministic batch-commit
  (MEST), avoiding 2PC entirely — the same core idea as this design's op
  collation, applied to statically partitioned databases rather than
  dynamically placed actors.
- **Calvin**: avoids 2PC via a **sequencer that pre-agrees a deterministic
  global commit order** in fixed epochs, with local lock managers acquiring
  locks in that agreed order. This does involve a coordinating sequencer
  layer — not "purely local ordering with zero coordination."
- **Orleans' own transaction support (VLDB'24)** notably does *not* use
  collation — it implements classic 2PL+2PC across actors, with a
  "reconnaissance query" pre-pass to prefetch state. Useful negative data
  point: even a mature virtual-actor system chose traditional distributed
  transactions over collation for multi-entity ops.

## 3. Wound-wait with loosely-synchronized timestamps

- Traces to **Rosenkrantz, Stearns & Lewis (1978)**, the origin of both
  wound-wait and wait-die: older (lower) timestamp preempts younger;
  younger waits for older; an aborted transaction **keeps its original
  timestamp on restart** — this is what guarantees no starvation. Confirm
  op_id retains identity across retries, not just op contents, for the
  same reason.
- **Google Cloud Spanner** uses wound-wait in production today, keyed on
  transaction age from earliest read/commit — evidence this scales to real
  distributed systems, not just textbook single-node lock managers.

## 4. Capacity-weighted consistent hashing

- **Dynamo** is the direct precedent for capacity-weighted virtual nodes:
  "node heterogeneity can be expressed by giving higher-capacity nodes more
  tokens" — matches this design's scheme. Its confirmed contribution is
  minimal key movement on membership change; no precedent was found
  specifically for a two-level "vnode-bucket → node" structure enabling
  O(#reassigned buckets) bulk cache eviction — that data-structure choice
  appears to be a novel contribution on top of the Dynamo idea.
- **Cassandra** (Dynamo-lineage) moved from random per-node token
  assignment to a deterministic allocation algorithm in 3.x+ for better
  balance with fewer tokens per node.
- **Local Rendezvous Hashing** (arXiv 2512.23434, recent): a fixed-size
  candidate-window scheme that remaps only keys whose winning node
  actually died (zero excess movement), faster than multi-probe consistent
  hashing. Explicitly doesn't address capacity-weighting or a two-level
  bucket structure either, so it doesn't solve the bulk-eviction need
  directly, but is a legitimate alternative to ring+vnodes worth
  evaluating.

## 5. Fail-stop, no-replication, halt-on-shard-loss

- **Redis Cluster** is the closest concrete precedent: when no replica is
  available for failover, it deliberately halts and stops serving entirely
  rather than degrading — and is unavailable for the whole minority side
  of a partition, not just the affected slot range (even more conservative
  than strictly necessary here). Real production validation of this
  design's storage trade-off.
- **Dynamo explicitly rejects this philosophy** — it favors always-
  accepting writes via sloppy quorum/hinted handoff over consistency. This
  design borrows Dynamo's *placement* mechanics while rejecting Dynamo's
  *availability* philosophy in favor of a Redis-Cluster-style CP stance —
  a real and somewhat unusual combination.
- The classical **fail-stop model (Schneider & Schlichting, 1983)** defines
  the property being relied on, but the paper's own way of achieving it is
  replicated memory + majority voting — the textbook answer to "how do you
  get fail-stop-safe" is redundancy, the opposite of this design's v1
  choice. This is a deliberate minority stance, not the literature's
  default answer, consistent with treating replication as explicit future
  work.

## 6. Gossip membership + per-key ownership

- **SWIM** (2002) and its 1998 Cornell predecessor (van Renesse/Minsky/
  Hayden) establish the O(n log n) detection time / no-leader gossip
  model being built on. Caveat: the basic/flat gossip protocol's bandwidth
  is roughly linear per-node but effectively quadratic cluster-wide at
  large N — the original paper itself motivates a hierarchical extension
  for this reason. Not a v1 concern at container-cluster scale, but worth
  remembering if the cluster grows large.
- **Consul/Serf** run SWIM with two separate gossip pools (per-datacenter
  LAN + global WAN) specifically to spread failure-detection load — a
  direct precedent for this design's two-pool (compute/storage) split,
  though Consul splits by network topology rather than by role.
- **Cassandra** and **Redis Cluster** both propagate per-key/token/slot
  ownership directly over gossip — precedent for carrying sharding info in
  the membership layer, though both use comparatively static ownership
  (fixed token ranges/slots) rather than this design's dynamic per-datum
  Native/Foreign handoff.
- **Akka Cluster** is notably more surgical about partial failure: on a
  split-brain/non-convergence, only the leader's membership-management
  duties halt — the rest of the cluster keeps serving. A contrast point
  given this design's much more conservative "halt everything" storage
  choice (a deliberate divergence, see §5).

## Closest overall prior art / what's genuinely novel

No single system combines all of this. The closest partial matches:

- **Compute tier** ≈ Orleans/Akka-style virtual-actor placement, but
  without a separate directory service (routing is derived, not looked
  up) — closer to the "no directory" ideal both systems fall short of.
- **Collation mechanism** ≈ Lotus's granule-collation-onto-single-thread
  idea, but applied to *dynamically placed* per-key actors rather than
  statically partitioned data. This combination (actor-style dynamic
  placement + Lotus-style deterministic single-thread collation instead of
  2PL/2PC) doesn't appear to have a direct precedent — even Orleans, which
  has both actors and transactions, uses classic 2PC for the latter rather
  than collation.
- **Storage tier** ≈ Dynamo's placement mechanics grafted onto Redis
  Cluster's fail-stop philosophy — an unusual combination (mixing an AP
  system's sharding math with a CP system's availability stance).
- **Membership** ≈ Consul's two-pool gossip pattern, adapted from
  topology-based to role-based splitting.

The wound-wait scheduling is the one piece that's simply a faithful,
low-risk application of a 45-year-old, still-production-proven (Spanner)
mechanism — nothing novel there, which is exactly what's wanted from that
piece.
