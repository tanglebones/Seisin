# Partition Index & Validation Scan Order — Design

Date: 2026-07-25 (Part 5c — generalizes Part 5b's extent and rescan)

## Overview

Part 5b's extent generalizes into a **partition index**: a named,
pk-ordered subset of a type's datums. The full extent is the trivial
partition (`all`); the validation system's invalid-set is another
(`invalid`) — membership in it *is* the datum's valid/invalid flag, so
no flag lives in datum content (a validation verdict must not be a
content write churning indexes and schema versions). The checker then
runs against the invalid partition instead of full extents, and full
rescans process types in an order chosen to fix the most-depended-on
data first.

## The `"partition"` Kind

Part 5b's `"extent"` kind is renamed and extended in place (same
counted-B+Tree-of-pks file per partition datum, same paged reads):

- `partition_key(type_name, partition) -> DatumId` from
  `partition:{type}:{partition}`; `extent_key(type)` becomes
  `partition_key(type, "all")` and `invalid_key(type)` is
  `partition_key(type, "invalid")`.
- **Maintenance**: the `all` partition stays framework-maintained
  (`TypedOpContext` schedules Insert/Remove for `.track_extent()`
  types, kind string now `"partition"`). Other partitions are
  **driver/solution-maintained** via a new client-facing wire request:
  `Request::PartitionUpdate { partition_datum_id, op: ExtentOp }` →
  `Response::ExtentResult { total, pks: [] }` (the kind gains an
  `execute` handling Insert/Remove and returning the new total).
  `Request::ExtentQuery` already addresses any partition datum by id —
  unchanged, just documented as the partition page read.
- Ordering: pk-byte order (a set with deterministic pagination).
  Custom orderings are rk's territory, deferred.
- Same single-datum-per-partition write-funnel note as rk/extent.

## The `invalid` Partition Convention

- The **driver** inserts a pk into `invalid:{type}` when
  `validate_type` reports findings for it, and removes it when a
  re-validation passes. New driver helpers:
  - `mark_invalid(addr, def, pks)` / `clear_invalid(addr, def, pk)` —
    thin `PartitionUpdate` wrappers.
  - `revalidate_invalid(addr, def, read_op, page_size) ->
    Vec<ValidationFinding>` — pages the `invalid` partition,
    re-validates only those pks (same per-datum logic as
    `validate_type`), clears entries that now pass, and returns the
    still-failing findings. This is the checker's fast path; the full
    `validate_type` sweep remains the slow path that discovers new
    invalidity.
- Write-path note: a datum that commits through the typed layer passed
  every write-time check, so the write path never marks invalid;
  invalidity is only ever discovered by scans (or implied by delete
  markers) and only ever cleared by re-validation.

## Full-Rescan Type Order

`driver::scan_order(defs: &[DatumTypeDef]) -> Vec<usize>` (indices into
`defs`, most-urgent first): sort by

1. **Most incoming runtime references first** — incoming(T) = count of
   `PkUuid`/`SkUnique` constraints across all defs targeting T
   (`PkEnum` excluded everywhere: static refs can never dangle).
   Rationale: fixing the most-depended-on data first can resolve the
   references pointing at it before their own types are scanned.
2. Ties: **least outgoing** runtime constraints first (fewer of its own
   references to be broken — likelier to be genuinely fixable now).
3. Further ties: **lower derived type id** — `DatumId::from_name` over
   `type:{name}` compared as bytes (deterministic; a numeric type-id
   registry is future schema-registry/DSL work).

Pure schema-graph computation, no I/O — the driver applies it to both
full sweeps and invalid-partition passes.

## Testing

Partition kind: execute Insert/Remove over the wire round-trips and
updates total; the `all` partition still framework-maintains.
Driver: `scan_order` known-answer over a three-type graph exercising
all three tie-break levels; `mark_invalid` → `revalidate_invalid` on a
still-broken datum returns the finding and keeps membership; fixing
the datum then re-running clears membership. Integration extends
`integration_delete_side_and_rescan.rs`; stress per house discipline.

## Deferred

Custom partition orderings; predicate-declared (auto-maintained)
partitions — a real design (sk-style diff maintenance over arbitrary
predicates) deferred until a concrete need beyond `invalid`; numeric
type ids (schema registry / macro DSL).
