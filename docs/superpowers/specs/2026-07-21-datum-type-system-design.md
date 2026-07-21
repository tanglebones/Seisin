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
cross-node acquisition/wound-wait. No changes to those mechanisms are
required except where explicitly noted below (none are).

## Scope

In scope: schema declaration and field encoding; the pk, sk, rk, and tk
index kinds, fully specified; uniqueness and relational (FK) constraint
enforcement (best-effort inline check + eventual authoritative check
invoking a solution-declared conflict-resolution op). Out of scope,
explicitly: transaction-time audit
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
- **Update flow — two round trips, client-driven.** Updating an sk-indexed
  field requires removing the old entry from the old key's list, but the
  old value isn't known until the pk datum is actually read — and
  collation today requires an op to declare every `datum_id` it needs
  *before* execution starts (see `worker.rs`'s `RunOp` handler sending an
  `Acquire` for each declared id up front). Rather than extending
  collation to support discovering a new required datum mid-op (a real
  architectural change — op execution would need to pause and resume on a
  new grant), the typed-write helper does this instead:
  1. A plain read of the pk datum, to learn the current (soon-to-be-old)
     field value and derive `sk_old_key`.
  2. The actual write op, declaring `datum_ids: [pk_id, sk_new_key,
     sk_old_key]` (omitting `sk_old_key` on a fresh create, or when the
     value didn't change) — the op appends `(pk_id, Native)` to
     `sk_new_key`'s entry list and removes `pk_id`'s entry from
     `sk_old_key`'s list.
  This costs an extra round trip only when an sk-indexed field is actually
  changing, and keeps the collation/wound-wait model completely
  untouched. Contention on a shared sk key (e.g. two writers both naming
  an entity "cliff" concurrently) resolves via the exact same
  wound-wait/collation machinery already built for any other regular
  datum — no new concurrency primitive needed.
- **Uniqueness (`unique: Some(ConflictOp)`)**: before appending `(pk_id,
  Native)` to `sk_new_key`'s entry list in step 2 above, the op checks
  whether that list already holds an entry for a *different* pk_id — if
  so, it rejects the write (returns an error) instead of inserting a
  probable duplicate. This best-effort check is genuinely authoritative
  for the common case (two writers racing on the same sk key resolve
  through the same wound-wait collation as any other sk update, so
  whichever runs second always sees the first's already-committed insert)
  — but it is not a full guarantee: a crash of the owning thread/node
  between this check passing and the op's write-through-before-ack
  completing can still leave more than one entry behind after lazy
  recovery, since there's no cross-op transaction making check-then-
  commit atomic against a crash in between. See "Constraint Enforcement"
  below for the eventual/authoritative backstop.

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
- **Update flow**: same two-round-trip shape as sk (read old value if
  updating, derive whether the rank position needs to move), but touching
  only *one* datum (`rk:type.field`) rather than two, since there's no
  value-keyed partitioning to move between — the update is a
  remove-old-rank-key + insert-new-rank-key against the same structure.
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
  engine despite both being "ordered by a comparable key."
- **Overlap invariant**: enforced by the writing op itself, since there's
  no database-level exclusion constraint available. A correction-upsert:
  1. Given `(pk_id, field, as_of, new_value)`, read the tk datum's own
     content (already known deterministically from `pk_id` alone — no
     external lookup needed, unlike sk's old-value problem).
  2. Find the entry whose range covers `as_of` (normally the currently-
     open one, `upper == None`, for a forward-dated correction; could be
     a past closed entry for a backdated correction) and set its `upper =
     as_of`.
  3. Insert the new entry `(as_of, previous_upper, new_value)` at the
     correct sorted position.
  This is a **single-datum op** — `datum_ids: [tk:type.field:pk_id]` (or
  including `pk_id` too, if the same op also touches other fields on the
  entity) — no two-round-trip needed, since the tk key doesn't depend on
  any value that must first be read.
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

Neither uniqueness nor relational (FK) constraints can be a hard,
synchronous guarantee in this ownership model — there's no cross-op
transaction boundary spanning "check, then commit" (matches the original
design doc's explicit non-goal of distributed transactions), so both
share the same two-tier shape:

1. **Best-effort synchronous check**, at write time — cheap, uses data
   the op is already touching, catches the common case immediately, and
   is genuinely authoritative for same-datum contention (which resolves
   through existing wound-wait collation). It is not a full guarantee: a
   crash of the owning thread/node between the check passing and
   write-through-before-ack completing can still leave a real violation
   behind.
2. **Eventual, authoritative check** — a periodic background scan that is
   the real source of truth. Any genuine violation it finds invokes a
   **solution-declared conflict-resolution op** (`ConflictOp`, a name
   registered the same way as any other domain op via `OpRegistry`) —
   Seisin's part is detecting the violation and invoking the op; the
   *policy* for resolving it (keep-oldest, merge, cascade-delete,
   flag-for-review, whatever fits the solution) is entirely up to that
   op's implementation, not prescribed here.

**Uniqueness** (`IndexDef::Sk { unique: Some(ConflictOp), .. }`) — the
best-effort check is described under "sk Index" above. The eventual check
periodically scans sk indexes declared unique for any entry list with
more than one entry, and calls the declared `ConflictOp` with the full
list of conflicting `(DatumId, AuthorityIdx)` entries.

**Relational (FK) constraints** — a constraint declares a referencing
field on one type, the type it points to, and a `ConflictOp`:

```rust
pub struct RelationalConstraintDef {
  field: String,
  references_type: String,
  on_violation: ConflictOp,
}
```

- Best-effort check: at write time, a plain existence check (`ctx.get` on
  the referenced pk_id) before committing the write — rejected
  synchronously if absent, catching the common "reference to something
  that never existed" case immediately. Not a full guarantee: the
  referenced entity could be deleted concurrently with (or immediately
  after) this check, which no cross-datum transaction prevents.
- Eventual check: a periodic scan over declared FK fields finds dangling
  references (pointing to a pk_id that no longer exists) and calls the
  constraint's declared `ConflictOp` with the violating datum_id and the
  missing reference — replacing any notion of passive logging with the
  same detect-and-resolve-via-op mechanism uniqueness uses.

Full scheduling mechanics for the eventual/periodic scan (how often, what
triggers it, whether it's driven by a dedicated background thread or
piggybacks on an existing loop) are left for the implementation plan —
this spec establishes the enforcement *shape* (best-effort inline check +
eventual authoritative check invoking a solution-owned resolution op),
not a complete scheduler design.

## Testing Strategy

- Schema/field encoding: round-trip tests per `FieldType` variant
  (including nested Array/Dict), matching the existing round-trip-test
  style used throughout (`seisin-protocol`, `seisin-core::sk`).
- sk: unit tests for the two-round-trip update flow (create, update
  changing the indexed value, update leaving it unchanged, delete);
  an integration test proving concurrent writers to the same sk key
  collate/wound-wait correctly (reusing the existing wound-wait
  integration test pattern from Sub-project 3b); a unit test proving the
  uniqueness best-effort check rejects a second writer to the same
  already-populated unique sk key.
- rk: unit tests for the splay tree's insert/delete/rank-descent
  correctness against known sequences (matching the "known-answer test
  vectors, not fuzzing" convention favored elsewhere in this project);
  property-style tests confirming subtree-size invariants hold after
  arbitrary insert/delete sequences; a round-trip test for the sorted-Vec
  disk content format and O(n) rebuild.
- tk: unit tests for the correction-upsert (forward correction, backdated
  correction, and the reject-if-genuinely-ambiguous case); round-trip
  tests for the column-store encoding; a test confirming two different
  entities' tk indexes never contend (no shared lock/datum).
- Constraint enforcement: a unit test proving the FK best-effort check
  rejects a write referencing a pk_id that never existed; a unit test
  proving the eventual check detects a dangling reference (created via a
  concurrent delete the best-effort check couldn't have caught) and
  invokes the declared `ConflictOp`; the same shape repeated for the
  uniqueness eventual check (more than one entry in a unique sk list
  invokes its `ConflictOp`).

## Open Questions Carried Forward

- **Exact nesting rules for Array/Dict field values** (e.g. how deep
  nesting is allowed, whether a Dict value can itself be a Dict) — not
  nailed down in the original doc either; deferred until a real solution
  needs it.
- **rk index sharding** — explicitly deferred (see "Known limitation"
  above under rk).
- **Eventual-check scheduling mechanics** — how often the periodic
  uniqueness/FK scans run and what drives them (dedicated thread vs.
  piggybacking on an existing loop) is left for the implementation plan
  (see "Constraint Enforcement" above).
- **Transaction-time audit mechanism** — explicitly out of scope for this
  sub-project; a separate, system-wide concern for a future pass.
