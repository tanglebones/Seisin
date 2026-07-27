@GUIDELINES.md

# Seisin — Project Context

Seisin is a Rust workspace implementing a distributed backend
data-processing toolkit: datum ownership allocated to threads/nodes via
consistent-hash rings, multi-datum ops collated onto owning threads
(wound-wait deadlock handling), caching, pre-baked ops and datum types
defined in code, and upgrades managed via code deployment with strict
n→n+1 update ordering (no version skipping). There is no SQL and no ORM
anywhere — all datum types, indexes, and constraints are defined in Rust
(a macro DSL for this is a planned future sub-project).

## Where to orient on resume

1. `docs/superpowers/PROGRESS.md` — the authoritative status ledger:
   every completed sub-project ("Done" entries), what is not started,
   and all deferred items. Read the tail of this first.
2. `git log --oneline -15` — the last few commits show what just
   happened; work is committed per task directly to `main`.
3. `docs/superpowers/specs/` and `docs/superpowers/plans/` — every
   feature goes brainstorm → spec → plan → execution. The newest spec
   without a matching plan (or plan with unchecked boxes) is the work
   in flight.

Snapshot as of 2026-07-26: sub-projects 1–3 (datum core, gossip/ring,
collation & wound-wait) and the datum type system (pk/sk/rk/tk/lb, FK
constraints, partition index) are done. Storage Tier Parts A (delta log,
store wire, RemoteStore), B (role-tagged gossip membership, coordinated
fail-stop halt), and C-1 (live add/remove/reweight migration via the
`seisin-migrate` driver, log identity, resumable pause, storage
self-halt, resume-after-halt with impostor detection) are done — 11
crates, 479 tests passing. Next up: Storage Tier Part C remainder
(replication, log compaction, tk/lb datum-grade durability, group
commit) and Sub-project 5 (containerized multi-node harness). No spec
written for either yet. PROGRESS.md supersedes this snapshot if they
disagree.

## Architecture quick map (crate → responsibility)

- `seisin-core` — NodeId/DatumId, `Store` trait (`put_with_previous`),
  cache.
- `seisin-ring` — capacity-weighted consistent-hash `Ring` (used for
  both compute threads and storage nodes).
- `seisin-gossip` — SWIM membership, role-tagged
  (`MemberRole::{Compute,Storage}`), `GOSSIP_PROTOCOL_VERSION`.
- `seisin-protocol` — client wire (`PROTOCOL_VERSION`) and store wire
  (`STORE_PROTOCOL_VERSION`), each independently versioned.
- `seisin-ops` — op registry; ops are closures over an `OpContext`.
- `seisin-types` — datum schema/encoding (u16 schema-version prefix),
  typed context, index kinds (sk/rk/tk/lb/partition), FK machinery,
  client-side scan drivers.
- `seisin-storage` — counted B+Tree engine; delta codec; append-only
  CRC-framed `DatumLog` (fsync-before-ack, self-rebasing).
- `seisin-node` — worker pool, servers (client/gossip/store),
  `ClusterState`, `HaltState`, `RemoteStore`; composition root in
  `main.rs` dispatches on `NodeRole::{Compute,Storage}`.
- `seisin-client` — blocking wire client used by tests and drivers.

Load-bearing design decisions (rationale lives in the specs):
- Storage is content-agnostic: it stores bytes and structure-blind byte
  deltas; all typing/semantics live compute-side.
- Indexes (sk/rk/partition) are rebuildable derived state — no WAL; tk
  decomposed fields and lb boards are currently index-grade too
  (datum-grade durability for them is a deferred part).
- No replication in v1: a confirmed-dead storage node fail-stops the
  whole cluster (`HaltState`) rather than serving partial data.
- The framework never self-initiates background work: validation
  rescans, revalidation, and migrations are all client-side driver
  programs.
- Wire compatibility: n±1 keep-old-decoder policy binds from the first
  deployed release; pre-first-release breaking changes are allowed and
  taken (bump the protocol version, drop the old decoder, note it).

## House workflow (differs from or sharpens GUIDELINES.md)

- Superpowers flow per feature: brainstorm → spec in
  `docs/superpowers/specs/YYYY-MM-DD-<topic>-design.md` → plan in
  `docs/superpowers/plans/` → execute **inline in the session, never
  via subagents** → update PROGRESS.md → commit + push.
- Commit per task, directly on `main`, trailer
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- Gates per task: `cargo fmt --all --check` and
  `cargo clippy --workspace --all-targets -- -D warnings` clean, full
  `cargo test --workspace` green.
- Stress discipline: run each new integration suite 10×; whenever
  `seisin-node/src/worker.rs` changes, also run the standing 20× loop
  over `integration_wound_wait`, `integration_cross_node_wound_wait`,
  and `integration_op_collation`.
- Crate versions stay `0.1.0` pre-first-release — an established,
  deliberate deviation from the version-bump guideline.
- Test-time constants for failure-detection tests: 20 ms probe
  interval/timeout, 40 ms suspicion (see the existing gossip
  integration tests for the pattern).
