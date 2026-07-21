# Datum Type System Design

Date: 2026-07-21

## Overview & Goals

This sub-project designs the datum type system referenced in the original
architecture doc's "Datum Type System" section: typed, homogeneous datum
types with declared indexes (pk/sk/rk/tk) and relational constraints. It
was re-sequenced ahead of Sub-project 4 (Storage Tier) because Storage
Tier's disk persistence format for indexes depends on how indexes actually
need to be structured and reconstructed — designing this first avoids a
storage-format rework later.

Everything here builds on Sub-projects 1-3 (fully implemented on `main`):
the wire protocol, `OpRegistry`/`OpContext`, `NativeLock`/collation, and
cross-node acquisition/wound-wait. Most of this spec needs no changes to
those mechanisms — the one exception, added in this revision, is
"Automatic Index Maintenance & Op Lifecycle" below, which extends
`worker.rs`'s op-record tracking and the wire protocol with a new
`IndexUpdate` message pair. Nothing about collation/wound-wait/crash
recovery itself changes; a new phase is added to an op's own lifecycle.

## Scope

In scope: schema declaration and field encoding; automatic, framework-
driven index maintenance via a new op-lifecycle phase (see "Automatic
Index Maintenance & Op Lifecycle"); the pk, sk, rk, and tk index kinds,
fully specified; uniqueness enforcement (synchronous, as part of that new
op-lifecycle phase) and relational (FK) constraint enforcement, which
references a declared index and — when a resolution strategy is
declared — allows a temporarily dangling reference rather than rejecting
it outright, tracked for a periodic eventual scan that invokes a
solution-declared conflict-resolution op only if the reference is still
missing when it runs. Out of scope, explicitly: transaction-time audit
logging (a separate, system-wide concern per the database guideline's own
split of the two time axes — not part of this sub-project); the
deployment/schema-evolution rollout system (already sketched separately in
the original doc, its own future sub-project); any actual disk persistence
(that's Storage Tier's job — this spec defines the *content model* each
index needs to persist, which Storage Tier will build its format around).

This spec covers all four index kinds in one document per an explicit
scope decision, but it is large — schema encoding, sk, rk (a splay tree
implementation), and tk (a bitemporal correction engine) are each
substantial on their own. Whether the implementation plan splits into
multiple parts (mirroring how Sub-project 3b was split into Parts 1/2a/2b)
is a decision for the writing-plans step, not this spec.

## Schema Declaration & Field Encoding

A solution declares a datum type via a runtime registration API, mirroring
`OpRegistry`'s existing pattern rather than introducing a proc-macro or
codegen pipeline (the "framework/codegen shape" is still an open,
unresolved question project-wide — this sub-project doesn't need to
resolve it, and a runtime-registration API works either way: it could
itself be the target of a future codegen layer without changing shape).

```rust
pub enum FieldType {
  Bool, I64, F64, String, Bytes,
  Array(Box<FieldType>),
  Dict(PrimitiveFieldType, Box<FieldType>), // keys restricted to primitives
}

/// The subset of `FieldType` allowed as a `Dict` key — everything except
/// `Array`/`Dict` themselves (a key must be directly comparable/hashable).
pub enum PrimitiveFieldType { Bool, I64, F64, String, Bytes }

pub struct DatumTypeDef {
  name: String,
  fields: Vec<(String, FieldType)>,
  indexes: Vec<IndexDef>,
}

pub enum IndexDef {
  Sk { field: String, unique: Option<ConflictOp> },
  Rk { field: String },  // field must be I64 or F64
  Tk { field: String },  // field can be any type; see "tk Index" below
}

/// Names a registered op (via `OpRegistry`, same mechanism as any domain
/// op) to call when the eventual/authoritative constraint check finds a
/// genuine violation the best-effort check missed. See "Constraint
/// Enforcement" below.
pub struct ConflictOp(pub String);
```

Content encodes as a hand-rolled, length-prefixed tag-value sequence in
declared field order — matching this project's existing style (see
`seisin-protocol`'s `encode_request`/`decode_request`, `seisin-core::sk`'s
`encode_sk_entries`), not a serde-based encoding. `encode_datum(def,
values) -> Vec<u8>` / `decode_datum(def, bytes) -> Result<Vec<FieldValue>>`
are the two entry points; a `FieldValue` enum mirrors `FieldType`'s shape
(`FieldValue::Bool(bool)`, `FieldValue::Array(Vec<FieldValue>)`, etc.).

A tk-indexed field's value is stored *only* in that field's tk index (see
below) — it is not duplicated in the type's own encoded content. This is
6NF-style decomposition (each independently time-varying attribute gets
its own storage), matching the database guideline's own framing of `_t_`
tables directly. All other fields (non-tk) live in the plain datum content
as normal.

## Automatic Index Maintenance & Op Lifecycle (revised 2026-07-21)

Supersedes the "two-round-trip, client-driven" update flow originally
described under sk/rk/tk below — those sections' "Update flow" bullets
now point here instead of describing their own bespoke flow. This is the
actual mechanism every index kind's writes go through.

**Motivation.** An index structure (especially rk's splay tree) must
never be rebuilt from serialized bytes on every single update, and must
never be collated onto a foreign thread the way a normal multi-datum op
pulls other datums together — an index's residency is pinned to its own
native-home thread, for as long as that thread runs, full stop.

**Field-level change tracking, no codegen yet.** An op's registered
handler operates through a typed accessor layer (`seisin-types`, built on
top of the existing byte-level `OpContext`) rather than raw
`ctx.get`/`ctx.put`. It reads/writes `FieldValue`s per `(datum_id,
field)`, and the *framework* — not the op author — detects which fields
actually changed by comparing what was read against what was written, so
an op author never writes index-maintenance code by hand. (Ergonomics
like `changeName(User u, String n) { u.name = n; }` are illustrative of
the eventual codegen layer this could support, not something this
increment builds — see "Framework/codegen shape" in `PROGRESS.md`.)

**Writes are staged, not immediate.** A typed write during an op's
business-logic phase does not commit to cache/storage right away — it's
held in memory until the op's overall outcome, including every index
update it triggers, is known.

**Three op lifecycle phases**, extending the existing `OpRecord`/
collation machinery (which already tracks in-flight `Acquire`s the same
non-blocking way — see `worker.rs`):

1. **Execute** — the op's handler runs, producing staged field changes;
   nothing is written through yet.
2. **Index update phase** — for every changed, indexed field, the
   executing thread sends a new `IndexUpdate` message to whichever
   thread natively owns that index datum:
   - Locally: a new `WorkerMessage::IndexUpdate { target: DatumId, op_id:
     DatumId, payload: Vec<u8> }` variant, following the exact
     `Acquire`/`Recall`/`Release` pattern already established.
   - Cross-node: a new `Request::IndexUpdate { target, op_id, payload }`
     / `Response::IndexUpdateResult(Result<(), String>)` wire pair
     (`payload` is opaque to the framework, exactly like `Request::Op`'s
     payload — interpreted by whichever index-kind logic owns `target`,
     which already knows what kind of index it is).
   The op's own record tracks how many `IndexUpdate` replies it's still
   waiting on, the same way it already tracks `still_needed` acquires —
   the executing *thread* keeps processing other work in the meantime;
   only this specific op isn't ready yet. The index-owning thread applies
   the change to its resident, never-rebuilt-from-scratch in-memory
   structure (loaded once on first access, kept live across every future
   update on that thread) and checks any declared uniqueness/FK
   constraint synchronously — it's local to that thread by construction
   — before replying success or violation.
3. **Commit or fail** — once every dispatched `IndexUpdate` reply is in:
   if all succeeded, the staged write actually lands (`ctx.put`,
   write-through-before-ack as already established) and the client gets
   `OpResult`; if any reported a violation, nothing is written at all and
   the client gets `OpError` — the whole op is discarded, not partially
   applied.

**Still best-effort, not a hard cross-op guarantee.** This makes
checking synchronous and reliable *within* a single op's index-update
phase — two ops racing to write the same new unique value both dispatch
to the same index-owning thread, which processes them one at a time in
message order, so that specific race resolves correctly. It does not
provide full distributed-transaction atomicity (an explicit non-goal of
this project already): a crash mid-sequence is handled by the existing
crash-detection/lock-release machinery from Sub-project 3b Part 2b at the
ownership level, not re-solved here.

**Deliberately not in this increment's scope**: which thread an op gets
*collated to* in the first place (native-majority heuristic, from
Sub-project 3's `pool.rs`) still doesn't know to prefer an index's native
thread over the pk datum's — a normal cross-node `IndexUpdate` dispatch
works correctly regardless of where the main op happens to run, so
revisiting that heuristic is a reasonable future throughput optimization,
not a correctness requirement here.

**Retrofit required.** Part 2 (sk index) shipped using the earlier
two-round-trip, client-driven design — client pre-reads the old value,
declares every sk datum_id up front, everything collated onto one
thread. That implementation needs reworking onto this mechanism before
rk (Part 3) is built on the same foundation; this is the first task of
the next implementation plan, not a separate one.

## pk Index

Trivial, unchanged: the datum_id itself is the primary key. No new
mechanism.

## sk Index (Secondary Key)

Builds on the existing `seisin-core::sk` entry-list encode/decode (already
implemented, Sub-project 1) — nothing populates or invalidates it
automatically today; this sub-project adds that.

- **Key**: `sk:{type_name}.{field_name}:{value}` (already the convention
  used by the design doc's own example, `sk:user.name:cliff`).
- **Content**: a list of `(DatumId, AuthorityIdx)` pairs (unchanged from
  the existing `encode_sk_entries`/`decode_sk_entries`).
- **Update flow**: via the "Automatic Index Maintenance & Op Lifecycle"
  mechanism above — the framework detects the indexed field changed,
  dispatches an `IndexUpdate` to the old sk key's owning thread (removing
  the entry, if the field had a prior value) and one to the new sk key's
  owning thread (appending `(pk_id, Native)`), and only commits the pk
  write once both replies are in.
- **Uniqueness (`unique: Some(ConflictOp)`)**: when applying an insert to
  a unique-flagged sk key, the owning thread checks whether the entry
  list already holds an entry for a *different* pk_id — if so, it replies
  with a violation instead of inserting a probable duplicate, which fails
  the whole op per the lifecycle above (no pk write, no index change).
  This is checked synchronously as part of the index-update phase, on the
  thread that actually owns the sk key, so it's authoritative for any
  contention on that specific key (the owning thread processes every
  `IndexUpdate` for it one at a time). It is still not a full
  cross-op guarantee — see the lifecycle section's "still best-effort"
  note.

## rk Index (Ranked/Sampled — Leaderboards)

A single global ordered structure per `type.field`, supporting top-N,
bottom-N, and percentile-sampled-range queries with configurable
precision — the leaderboard use case is primary, though the mechanism is
general to any "give me an approximate rank/percentile view over a
numeric field" need.

- **Key**: `rk:{type_name}.{field_name}` — one key per declared rk index,
  no value-based partitioning (unlike sk).
- **`rank_key` ordering**: `I64` compares normally; `F64` requires a total
  order for the splay tree's comparisons, so `F64` rank keys are compared
  via IEEE 754 total ordering (`f64::total_cmp`) rather than the partial
  `PartialOrd` — `NaN` is thus orderable (sorts consistently, if
  unusually) rather than rejected outright, keeping insert/rank-descent
  total and panic-free without adding input validation this project
  doesn't otherwise require at the type-system boundary.
- **In-memory structure**: a modified splay tree keyed by the field's
  numeric value (`rank_key`), each node augmented with a subtree-size
  weight. Subtree weights enable O(log n) amortized:
  - **Rank descent** — find the k-th smallest/largest entry by comparing
    `k` against the left subtree's size at each node, recursing
    left/right or stopping, adjusting `k` as it descends.
  - **Insert** — standard BST insert by `rank_key`, then splay the new
    node to the root (rotations along the path also update subtree
    sizes).
  - **Delete** — splay the target node to the root, splice it out (splay
    its in-order predecessor to the root of the left subtree, then attach
    the right subtree).
  - **Weighted random descent** — at each node, recurse left/right with
    probability proportional to that side's subtree size, enabling
    uniform-random sampling within a rank range without a full traversal.
- **Content (disk persistence)**: a sorted `Vec<(RankKey, DatumId)>`
  (row-store — each entry is one tuple), ascending by `rank_key`.
  Rebuilding the in-memory splay tree from this on load is O(n) (build a
  balanced tree from the sorted array directly, not n sequential inserts
  each triggering splay rotations). The sorted array can also be sampled
  directly without materializing the tree, for a lightweight query that
  doesn't need the full structure.
- **Storage Tier dependency, noted here for when that sub-project's disk
  engine is designed**: the "Automatic Index Maintenance" mechanism above
  keeps this splay tree resident in memory on its owning thread, so
  rebuilding it from bytes on every update is no longer a concern (it's
  built once, on first access). What's still Storage Tier's problem: an
  insert/delete anywhere in this structure logically touches the whole
  sorted-Vec disk representation, and Seisin's storage model otherwise
  treats a datum's content as one opaque blob rewritten in full on every
  `put`. For a large rk (or tk, see below) index, naively rewriting the
  entire blob on every single update would mean genuinely expensive disk
  I/O per write — Storage Tier's `DiskStore` needs an
  append-only-journal-with-periodic-compaction format (or equivalent) for
  large, frequently-updated datums like this, rather than whole-file
  rewrites, when that sub-project's disk format is designed. This spec
  deliberately does not solve that part — it's a disk-engine concern, not
  a content-model or in-memory-residency concern.
- **Update flow**: via the "Automatic Index Maintenance & Op Lifecycle"
  mechanism above — the framework detects the rk-indexed field changed
  and dispatches a single `IndexUpdate` to `rk:type.field`'s owning
  thread (remove the old rank_key entry if one existed, insert the new
  one), touching only this one datum since there's no value-keyed
  partitioning to move between (unlike sk).
- **Query surface**: `top_n(n)` / `bottom_n(n)` (bounded in-order walk
  from an end — no splay needed, a plain tree walk respecting current
  shape); `percentile_sample(p, k)` (k evenly-spaced or weighted-random
  rank descents around percentile `p`, for "here's roughly where you
  stand" style queries with configurable precision).
- **Known limitation, deliberately not solved here**: every write to a
  type's rk-indexed field funnels through this single datum's owning
  thread — a genuine throughput bottleneck under high write volume,
  structurally similar to "no replication in v1" for Storage Tier.
  Sharding an rk index across multiple datums (with a K-way merge for
  top-N/percentile queries) is a clear future extension, explicitly
  deferred rather than built now.

## tk Index (Bitemporal Valid-Time)

Models the valid-time half of the database guideline's `_t_` pattern:
non-overlapping, half-open `[lower, upper)` ranged versions of a field's
value per entity, corrected via split-and-insert rather than in-place
edit. Transaction-time audit (who changed what, when) is explicitly out
of scope here — a separate, system-wide concern, per the guideline's own
insistence that the two time axes are independent and shouldn't be
collapsed into one mechanism.

- **Key**: `tk:{type_name}.{field_name}:{pk_id}` — **per-entity**, unlike
  rk. Each pk datum has its own independent version history for that
  field; there is no cross-entity structure at all.
- **`Timestamp`**: `i64` milliseconds since the Unix epoch, sourced from a
  shared `ClockSource` at write time (the same fake-clock-testable
  abstraction pattern already established in `seisin-gossip`'s failure
  detector) — never the raw system clock read inline, so tests can inject
  a fixed/advancing clock instead of racing real time.
- **Content — column-store**: three parallel arrays, sorted ascending by
  `lower`:
  - `lowers: Vec<Timestamp>`
  - `uppers: Vec<Option<Timestamp>>` (`None` = open-ended, currently in
    effect)
  - `values: Vec<FieldValue>` (encoded per the field's declared
    `FieldType`)
  Column-store (separate contiguous arrays) rather than row-store (an
  array of `(lower, upper, value)` structs) because range queries
  ("what was in effect between X and Y") scan the bound columns without
  needing to pull values along — a genuinely different access pattern
  from rk's rank-lookup, which is why rk and tk don't share a storage
  engine despite both being "ordered by a comparable key." Per-entity
  scoping naturally bounds a tk datum's size to one entity's own
  correction history (not the type's whole population), so the whole-blob
  -rewrite-per-update concern noted under rk is far less severe here —
  but for an entity with a very long correction history, the same
  Storage Tier dependency applies (see rk's note above): a
  journal-with-compaction disk format avoids rewriting the whole history
  on every single correction.
- **Overlap invariant**: enforced by the index-owning thread itself, via
  the "Automatic Index Maintenance & Op Lifecycle" mechanism above —
  there's no database-level exclusion constraint available, so this is the
  correction-upsert logic applied inside the `IndexUpdate` handler for a
  tk key:
  1. Given `(pk_id, field, as_of, new_value)`, read the tk datum's own
     resident content (its `tk:type.field:pk_id` id is deterministic from
     `pk_id` alone — no old-value read needed to compute the target,
     unlike sk).
  2. Find the entry whose range covers `as_of` (normally the currently-
     open one, `upper == None`, for a forward-dated correction; could be
     a past closed entry for a backdated correction) and set its `upper =
     as_of`.
  3. Insert the new entry `(as_of, previous_upper, new_value)` at the
     correct sorted position.
  Dispatched the same way as sk/rk: the framework detects the tk-indexed
  field changed and sends one `IndexUpdate` to `tk:type.field:pk_id`'s
  owning thread, which stays resident there like any other index.
- **Query surface**: `as_of(pk_id, timestamp) -> Option<FieldValue>`
  (binary search the `lowers`/`uppers` columns for the covering range);
  `current(pk_id) -> Option<FieldValue>` (the entry with `upper == None`,
  if any — gaps are allowed by default, matching the guideline's base
  case, which only enforces "no gaps" as an explicit opt-in invariant on
  top of the overlap constraint, not a default); `history(pk_id) ->
  &[entries]` (the full version list for that entity/field).
- **No cross-entity contention, and free distribution across storage
  nodes.** Because tk is scoped per-`(type, field, pk_id)`, it's a
  regular, independent datum like any other — it distributes across
  compute/storage nodes via the exact same ring-based placement every
  other datum uses, with zero special-casing. This is a direct structural
  advantage over rk's single global bottleneck: tk parallelizes for free,
  rk does not.

## Constraint Enforcement (Uniqueness & Relational)

**Revised alongside "Automatic Index Maintenance" above.** The primary
enforcement mechanism for uniqueness is now the synchronous check inside
the index-update phase (see "sk Index"'s Uniqueness bullet) — since every
sk write is already dispatched to, and synchronously checked by, its
owning thread before the whole op is allowed to commit, there's no
separate "best-effort inline check, hope a background scan catches what
it missed" story needed for uniqueness anymore. It's not a full
cross-op/distributed-transaction guarantee (this project's explicit
non-goal), but it's substantially stronger than the original two-tier
design: a genuine violation fails the whole op before anything commits,
for any contention the index-update phase actually observes.

**Relational (FK) constraints must reference a declared index** — not a
bare `(type, field)` pair. This is what makes checking (and, more
importantly, *re*-checking) a reference efficient rather than an
arbitrary scan, and it's what lets a future compound/multi-field index
support FKs that match on a prefix of a compound key without redesigning
this shape:

```rust
pub enum IndexRef {
  Pk { type_name: String },                     // the default: references a bare pk_id
  Sk { type_name: String, field: String },       // references a declared *unique* sk index
}

pub struct RelationalConstraintDef {
  field: String,
  references: IndexRef,
  /// If `Some`, a dangling reference is flagged and the write is still
  /// allowed (see below) rather than rejected. If `None`, a dangling
  /// reference is a hard synchronous rejection — the default/fallback
  /// when a constraint hasn't opted into flagged handling.
  resolution: Option<ConflictOp>,
}
```

**A missing reference is not automatically a hard rejection.** Real data
sets legitimately need to insert entities out of order against
pre-assigned ids — bulk/batch imports, and especially the database
guideline's `_e_`-style *mandatory, mutual* 1:1 extension pattern (two
rows that each FK back to the other, so neither can be written
strictly before the other without the SQL-world's awkward
deferred-constraint/CTE dance, which doesn't even work on every engine —
see the `database` guideline's `_e_` section). Rather than requiring that
dance, a constraint that declares a `resolution` strategy allows the
write to proceed with a temporarily dangling reference:

1. **Existence check, at write time** — via the referenced index (a
   lightweight existence check dispatched to whichever thread owns the
   referenced pk_id or sk key, not a full `Acquire`/collation — exact
   wire-message shape is left to the implementation plan).
   - Reference exists: proceed normally, nothing flagged.
   - Reference missing, **no** `resolution` declared: reject the write
     synchronously (the original hard-check behavior, still the default).
   - Reference missing, `resolution` **is** declared: allow the write,
     and record the dangling reference in a pending-FK tracking
     structure (`fk_pending:{type}.{field}`, maintained the same way an
     sk entry list is — through the same `IndexUpdate` dispatch/resident-
     structure mechanism as any other index) so the eventual scan below
     only ever has to check what's actually pending, never the whole
     population.
2. **Eventual, authoritative check** — a periodic scan over each
   `fk_pending:{type}.{field}` structure: for every still-pending entry,
   re-check whether the reference now exists. If it does, the entry is
   just removed (resolved naturally — no violation, no op invocation). If
   it's still missing, the constraint's declared `ConflictOp` is invoked
   with the violating datum_id and the missing reference — the *policy*
   for resolving it (null out the reference, cascade-delete,
   flag-for-review, whatever fits the solution) is entirely up to that
   op's implementation, not prescribed here. The resolution op is *only*
   invoked from this scan, never synchronously at write time, even though
   the write itself proceeds immediately.

Full scheduling mechanics for the FK eventual/periodic scan (how often,
what triggers it, whether it's driven by a dedicated background thread or
piggybacks on an existing loop) are left for the implementation plan —
this spec establishes the enforcement *shape*, not a complete scheduler
design.

**Open question carried forward from this revision**: whether a similar
periodic defense-in-depth scan is still worth keeping for uniqueness too
(covering the crash-mid-index-update-phase case the synchronous check
can't observe) is left to the implementation plan to decide — the
synchronous check is the primary mechanism either way, so this would only
ever be a backstop, not required for the feature to work correctly in
the common case.

## Testing Strategy

- Schema/field encoding: round-trip tests per `FieldType` variant
  (including nested Array/Dict), matching the existing round-trip-test
  style used throughout (`seisin-protocol`, `seisin-core::sk`).
- Op lifecycle: unit tests for the three-phase state machine in
  isolation (execute → index update phase → commit/fail) — an op with
  no indexed fields changed skips straight to commit; an op with one
  changed indexed field waits for exactly one `IndexUpdate` reply before
  committing; a reported violation results in no write at all (`ctx.get`
  after the op still shows the pre-op state) and an `OpError` to the
  client. An integration test proving the same across a real cross-node
  `IndexUpdate` dispatch (index native to a different node than the main
  op), reusing the peer-link integration-test patterns from Sub-project
  3b.
- sk: unit tests for the update flow (create, update changing the
  indexed value, update leaving it unchanged, delete) via the new
  lifecycle; an integration test proving concurrent writers to the same
  sk key resolve correctly (the owning thread processes both
  `IndexUpdate`s one at a time); a unit test proving the uniqueness
  check rejects a second writer to the same already-populated unique sk
  key and that the whole op's write is discarded, not partially applied.
- rk: unit tests for the splay tree's insert/delete/rank-descent
  correctness against known sequences (matching the "known-answer test
  vectors, not fuzzing" convention favored elsewhere in this project);
  property-style tests confirming subtree-size invariants hold after
  arbitrary insert/delete sequences; a test proving the tree stays
  resident across multiple updates dispatched to the same thread (built
  once from bytes on first access, not rebuilt on the second update — the
  whole point of keeping it resident, per "Automatic Index Maintenance"
  above); a round-trip test for the sorted-Vec disk content format and
  O(n) initial build.
- tk: unit tests for the correction-upsert (forward correction, backdated
  correction, and the reject-if-genuinely-ambiguous case); round-trip
  tests for the column-store encoding; a test confirming two different
  entities' tk indexes never contend (no shared lock/datum, no shared
  `IndexUpdate` target).
- Constraint enforcement: a unit test proving a write referencing a
  never-existed pk_id is rejected synchronously when no `resolution` is
  declared; a unit test proving the same write is *allowed* (and the
  reference tracked in `fk_pending:{type}.{field}`) when a `resolution`
  is declared; a test proving a pending entry is silently cleared once
  the referenced entity is created before the scan runs; a test proving
  the eventual scan invokes the declared `ConflictOp` for an entry still
  missing when it runs.

## Open Questions Carried Forward

- **Exact nesting rules for Array/Dict field values** (e.g. how deep
  nesting is allowed, whether a Dict value can itself be a Dict) — not
  nailed down in the original doc either; deferred until a real solution
  needs it.
- **rk index sharding** — explicitly deferred (see "Known limitation"
  above under rk).
- **Compound/multi-field indexes, and FKs matching a prefix of one** —
  `IndexDef`/`IndexRef` are both scoped to a single field for now; the FK
  design's "can be shared or part of a prefix if compound" framing
  anticipates this but doesn't require building it yet. Deferred until a
  real solution needs a compound key.
- **Collation destination-thread preference** — whether `pool.rs`'s
  native-majority heuristic should learn to prefer an index's native
  thread over the pk datum's, as a throughput optimization — explicitly
  deferred (see "Automatic Index Maintenance"'s "Deliberately not in this
  increment's scope" note).
- **Resident index cache lifecycle** — whether/how a thread ever evicts a
  resident index structure (analogous to the existing cache-eviction-on-
  ring-mutation rule for regular datums) isn't nailed down here; left for
  the implementation plan, which should at minimum confirm the existing
  ring-mutation cache-eviction rule from Sub-project 2b extends naturally
  to this new resident-index cache rather than silently exempting it.
- **FK eventual-check scheduling mechanics** — how often the periodic FK
  scan runs and what drives it (dedicated thread vs. piggybacking on an
  existing loop) is left for the implementation plan (see "Constraint
  Enforcement" above).
- **Uniqueness defense-in-depth scan** — whether to add one at all, given
  the synchronous check is now the primary mechanism — left for the
  implementation plan (see "Constraint Enforcement" above).
- **Transaction-time audit mechanism** — explicitly out of scope for this
  sub-project; a separate, system-wide concern for a future pass.
