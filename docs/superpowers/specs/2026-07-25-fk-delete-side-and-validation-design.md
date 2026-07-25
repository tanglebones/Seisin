# Delete-Side FK Enforcement, Field Validation & Rescan — Design

Date: 2026-07-25 (Part 5b — completes `2026-07-24-fk-constraints-design.md`)

## Overview

Part 5 enforced references at *write time of the referencing datum*.
This part covers the other three ways a reference or value goes bad:

1. **Deletes of referenced datums** (delete `user`, `foo.user_id`
   dangles) — restrict or lazy-cascade semantics.
2. **Anything the write-time checks missed or that decayed later** — a
   declared, per-type **full-validation rescan rate** the driver runs.
3. **Value validity** beyond types — declared field checks
   (`value > 0`-style), enforced at write and re-verified by rescan.

SQL terms (restrict/cascade) are illustrative only: everything is
declared in code via the existing builder API — the eventual macro DSL
("framework/codegen shape") is the layer that will collapse the
declared-on-both-sides duplications this design accepts.

## PkEnum Is Append-Only

`PkKind::Enum` mnemonic sets may only **grow** across schema versions.
Removing a mnemonic is not a legal migration (it would orphan every
mnemonic FK with no tracking machinery to notice). Consequence: enum-pk
types need **no delete-side handling at all** — a mnemonic reference,
once statically valid, stays valid forever. Documented on `PkKind`.

## Retiring the Bytes-Exist Approximation: `WriteThrough`

`IndexApplyOutcome.write_through: Option<Vec<u8>>` becomes:

```rust
pub enum WriteThrough {
  None,           // self-persisted kinds (rk, lb, tk)
  Put(Vec<u8>),   // blob kinds with content
  Delete,         // blob kinds whose state became EMPTY
}
```

sk and fk_pending return `Delete` when their entry list empties, so an
emptied sk key or drained pending list **vanishes from storage**
instead of persisting as an empty blob. Bytes-exist checks are now
exact for every target class — Part 5's documented SkUnique
approximation is retired, and delete-side restrict probes (below) get
exact answers with no new query machinery.

## Delete-Side Enforcement

**Declared on the referenced type** (where the delete runs — the same
cross-def-knowledge compromise as `PkEnum`'s embedded mnemonics, and
the same thing the macro DSL later derives from one definition):

```rust
pub enum OnDelete {
  /// Reject the delete while any reference exists.
  Restrict,
  /// Allow the delete; leave a marker for the scan driver, which
  /// chains the referencing constraint's declared ConflictOp
  /// (cascade-delete / rewrite-to-valid are that op's policy).
  Track,
}
pub struct GuardRef {
  pub type_name: String, // the referencing type (e.g. "foo")
  pub field: String,     // its FK field (e.g. "user_id")
  pub on_delete: OnDelete,
}
// DatumTypeDef gains: pub guards: Vec<GuardRef>; builder .guard(GuardRef)
```

**Requirement (documented, not cross-validatable in v1):** the
referencing type must declare a (non-unique) **sk index on the FK
field** — `sk:foo.user_id:<referenced_pk>` is both the restrict probe
target and the cascade enumeration source. The rescan reports
misconfiguration (a guard whose sk key never exists while references
do) as a finding.

`TypedOpContext::delete(user)` schedules per guard, using the pk being
deleted:

- **Restrict** → an *inverted* exists check against
  `sk_key(guard.type_name, guard.field, Bytes(pk))`: the op fails if
  the target **exists**. `PendingExistsCheck` is generalized:

  ```rust
  pub struct PendingExistsCheck { pub target: DatumId, pub expect: Expectation }
  pub enum Expectation {
    Present { on_missing: FkMissingPolicy },   // Part 5's write-time checks
    Absent { message: String },                 // restrict: fail if present
  }
  ```

  Known race, recorded not solved: a concurrent new-ref insert can land
  after the probe (same best-effort class as uniqueness); declaring
  `Track` alongside on another guard—or the rescan—is the backstop.

- **Track** → one marker inserted via the existing IndexUpdate rail
  into `deleted_refs:{type_name}.{field}` (id via a new
  `fk_deleted_key(type, field)`), **reusing the `"fk_pending"` kind**
  (it is a generic pair-set kind; documented dual use). Entry:
  `(deleted_pk, sk_probe_key)` — everything the driver needs. The
  driver then: read markers → read the sk key's entry list (ordinary
  client read + `seisin-core::sk` decode) → invoke the referencing
  constraint's ConflictOp per entry → remove the marker when the sk
  list drains. Cascades chain one hop per scan pass by construction —
  no synchronous cascade storms through the wound-wait layer.

## Field Validation Checks

```rust
pub enum FieldCheck {
  Gt(FieldValue), Ge(FieldValue), Lt(FieldValue), Le(FieldValue), // numeric (I64/F64)
  MinLen(u32), MaxLen(u32),                                       // String/Bytes
}
pub struct FieldCheckDef { pub field: String, pub check: FieldCheck }
// DatumTypeDef gains: pub checks: Vec<FieldCheckDef>; builder .check(field, FieldCheck)
// Declaration-time validation panics on unknown field / type-incompatible check.
```

Enforced synchronously in `TypedOpContext::set` (loud errors), and
re-verified by the rescan (catching byte-level writes that bypassed the
typed layer, and pre-existing data after a check is tightened).

## Type Extent & the Rescan

Enumerating "all datums of type T" needs an extent index — none exists.
New self-persisted kind `"extent"` (counted B+Tree, key = pk (16),
value = 1 zero byte; registered with `data_dir` like rk): datum id
`extent:{type}` via `extent_key(type)`; opt-in per type via
`.track_extent()` on the builder (auto-scheduled Insert/Remove in
`TypedOpContext`'s drop for tracked types). Wire:
`Request::ExtentQuery { extent_datum_id, offset: u64, limit: u32 }` →
`Response::ExtentResult { total: u64, pks: Vec<DatumId> }` (a paged
`scan_from_rank`). One extent datum per type is a create/delete-time
write funnel — same documented single-datum limitation class as rk,
same future sharding answer.

**Rescan rate** is driver guidance declared on the type:
`DatumTypeDef.rescan_every_millis: Option<u64>` (builder
`.rescan_every_millis(n)`), meaningful for any type with outgoing
constraints, incoming guards, or field checks. The framework runs
nothing on a timer — cadence, batching, and acting on findings remain
driver decisions (Part 5's philosophy unchanged).

**Driver helper** (`seisin-types/src/driver.rs`, client-side —
validating incoming refs falls out of validating every type's
*outgoing* refs plus the deleted-ref markers):

```rust
pub struct ValidationFinding {
  pub pk: DatumId,
  pub field: String,
  pub problem: String, // dangling ref / failed check / undecodable
}
pub fn validate_type(
  addr: &str,
  def: &DatumTypeDef,
  read_op: &str,      // a solution-registered byte-read op name
  page_size: u32,
) -> anyhow::Result<Vec<ValidationFinding>>
```

Pages the extent; reads and decodes each datum (undecodable = a
finding); re-runs field checks and static enum-membership; probes
`PkUuid`/`SkUnique` targets via `Request::ExistsCheck`. Acting on
findings (ConflictOps, alerts) is the caller's loop.

## Testing Strategy

- `WriteThrough`: sk/fk_pending emptying deletes the stored datum
  (exists-probe goes false); rk/lb/tk unchanged (`None`).
- Restrict: delete with live references fails atomically; after the
  last reference is removed (sk list empties → datum deleted), the
  same delete succeeds.
- Track: delete leaves the marker; driver enumerates the sk list,
  invokes the ConflictOp (delete-flavored, proving one-hop chaining),
  drains, removes the marker.
- Field checks: declaration panics (unknown field, Gt on String,
  MinLen on I64); set-time accept/reject at boundaries.
- Extent: tracked create/delete maintain it; paging exact across
  pages; untracked types schedule nothing.
- Driver: `validate_type` on a healthy type returns no findings;
  seeded dangling ref and out-of-range value (written via the byte
  level to bypass set-time checks) each produce one finding.
- Integration end-to-end over the wire + stress 10x + the standing 20x
  suites (worker.rs's reply handling changes again).

## Deferred

- Macro DSL (single-site declarations eliminating mnemonic/guard
  duplication) — the codegen-shape work, now with three forcing
  functions.
- Extent sharding; framework-scheduled rescans; cross-def declaration
  validation; SetNull as a distinct built-in (it's a ConflictOp
  policy).
