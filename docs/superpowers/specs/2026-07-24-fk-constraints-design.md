# Relational (FK) Constraints & Pk Identity Discipline — Design

Date: 2026-07-24

## Overview & Goals

Datum Type System Part 5: relational constraint enforcement per the
"Constraint Enforcement" section of
`2026-07-21-datum-type-system-design.md`, revised for everything built
since (the `ResidentIndex` rail, the `execute`/`query` paths, blob
kinds, the no-nested-op-invocation decision), plus a new pk identity
discipline that makes one whole class of FKs free at runtime.

## Pk Identity Discipline

Every typed datum's pk is one of exactly two kinds, declared on the
type:

```rust
pub enum PkKind {
  /// The default: ids must be version-7 UUIDs (what `DatumId::new`
  /// produces) — time-ordered, non-guessable.
  Uuid,
  /// A closed set of well-known mnemonics ("active", "suspended",
  /// "closed", ...), each deterministically deriving its DatumId from
  /// `pk:{type}:{mnemonic}`. Extending the set is a schema migration —
  /// a code deploy under the n -> n+1 rollout model — never a runtime
  /// operation. The shared-status-set case: entities FK these by
  /// mnemonic, not by uuid.
  Enum(Vec<String>),
}
```

`DatumTypeDef` gains `pk: PkKind` (default `Uuid`; builder
`.pk(PkKind::...)`) and `seisin-types` gains
`enum_pk_id(type_name, mnemonic) -> DatumId` (UUIDv5 derivation via the
shared namespace). Enum-pk datums are **derived-on-demand**: the
derived id is a valid reference target by construction; content is
written like any datum when a solution stores something there. No
seeding step, nothing to sync at startup.

**Enforcement lives in `TypedOpContext`** (`set`/`delete`): Uuid types
reject any id whose version nibble isn't 7; Enum types reject any id
not in the derived set. Loud errors, never silent. The byte-level
`OpContext` stays unrestricted — framework internals (index datums,
derived keys) legitimately use non-v7 ids.

## Constraint Declaration

```rust
pub struct RelationalConstraintDef {
  pub field: String,
  pub references: FkTarget,
  /// None: a dangling reference is a hard synchronous rejection (the
  /// default). Some: the write is allowed, the dangling reference is
  /// tracked in fk_pending, and the named op is invoked by the scan
  /// driver if the reference is still missing when the scan runs.
  pub resolution: Option<ConflictOp>,
}

pub enum FkTarget {
  /// References a Uuid-pk type: runtime existence check against the
  /// referenced datum.
  PkUuid { type_name: String },
  /// References an Enum-pk type: validity is set membership against
  /// the declared mnemonics — a schema-local, synchronous check with
  /// NO runtime dispatch at all. The mnemonic set is embedded at the
  /// declaring site (solutions define the enum once as a shared const
  /// and use it in both the referenced type's PkKind and here); a
  /// cross-def type registry is future codegen-shape work.
  PkEnum { type_name: String, mnemonics: Vec<String> },
  /// References a *unique* sk index: runtime check against the derived
  /// sk key datum. See "Sk existence is approximate at write time".
  SkUnique { type_name: String, field: String },
}
```

`DatumTypeDef` gains `constraints: Vec<RelationalConstraintDef>`
(builder `.constraint(...)`). Declaration-time validation panics on a
schema bug (the rk/`self_address` policy): the constrained field must
be declared; `PkEnum` fields must be `FieldType::String`;
`PkUuid`/`SkUnique` fields must be `FieldType::Bytes` (holding a
16-byte `DatumId`).

## Write Path

- **`PkEnum` constraints** are validated inside `TypedOpContext::set`:
  the field's string must be a member of the embedded mnemonic set,
  and the write fails synchronously otherwise. Zero dispatches.
- **`PkUuid`/`SkUnique` constraints**: `set` validates shape (exactly
  16 bytes); the drop-diff schedules an existence check whenever the
  FK field changed (or the datum is new), via a new `OpContext`
  method:

  ```rust
  pub struct PendingExistsCheck {
    pub target: DatumId,          // referenced pk id, or derived sk key
    pub on_missing: FkMissingPolicy,
  }
  pub enum FkMissingPolicy {
    Reject,                        // resolution: None
    Track {                        // resolution: Some — payload for the
      pending_datum: DatumId,      // fk_pending:{type}.{field} datum
      entry: Vec<u8>,              // encoded (referencing_pk, target)
    },
  }
  ```

- **Lifecycle integration** mirrors `IndexUpdate` exactly: a new
  `WorkerMessage::ExistsCheck { target, op_id, reply }` /
  `ExistsCheckReplied { op_id, target, exists }` pair (plus
  `Request::ExistsCheck { datum_id }` / `Response::Exists { exists }`
  on the wire — node-to-node for this path, and client-facing for the
  scan driver, routed with the shared redirect check). The op record
  counts exists-check replies in the same pending state as index
  updates. On a reply:
  - exists → nothing more.
  - missing + `Reject` → violation; the whole op fails atomically
    (existing commit-or-fail machinery).
  - missing + `Track` → the reply handler dispatches one more
    `IndexUpdate` inserting the entry into the fk_pending datum
    (pending count grows by one mid-flight) and the op commits once
    that lands.

  Existence on the owning thread = the datum has stored/cached bytes.
  That is exact for pk datums (delete removes the bytes).

## Sk Existence Is Approximate at Write Time

The worker is type-agnostic and cannot decode sk entry lists, so the
write-time check for `SkUnique` targets is bytes-exist on the sk key
datum — a false positive is possible when every entry was removed but
the empty-list blob remains. This is a documented approximation, not
an oversight: teaching the resident rail an `exists` method for one
corner case wasn't worth the trait growth. The scan driver's re-check
is exact (it decodes the entry list client-side), so a false-positive
dangling reference is caught eventually; a hard-reject (`resolution:
None`) `SkUnique` constraint can therefore admit a write referencing
an emptied unique key until compaction/scan — acceptable for v1,
revisit if a real solution hits it.

## fk_pending Tracking & the Driver-Run Scan

`fk_pending:{type}.{field}` derives one datum id per constraint
(shared namespace). It is a blob-resident registered kind
(`"fk_pending"`, sk's mechanics: decoded entry list, write-through on
apply): `apply` handles Insert (from the write path's `IndexUpdate`)
and Remove; entries are `(referencing_pk: DatumId, target: DatumId)`
pairs. The kind's client-facing wire pair:

```rust
Request::FkPending { pending_datum_id, op: FkPendingOp }
enum FkPendingOp { List, Remove { referencing_pk: DatumId, target: DatumId } }
Response::FkPendingResult { entries: Vec<(DatumId, DatumId)> }
```

(`List` via the `query` method; `Remove` via `execute`.)

**The eventual scan is pure driver orchestration** — a solution-binary
loop or test harness, never a framework thread, preserving the
no-nested-op-invocation decision (the framework never calls a
`ConflictOp` itself):

1. `FkPending { List }` → the pending entries.
2. Per entry, probe the target: `ExistsCheck` for pk targets; for sk
   targets, read the sk datum (ordinary registered read op) and decode
   the entry list client-side — exact.
3. `FkPending { Remove }` for every entry whose reference now exists
   (resolved naturally — no violation, nothing invoked).
4. For every entry still missing, the driver invokes the constraint's
   declared `ConflictOp` (an ordinary `Request::Op`) with the
   referencing pk and missing target — the resolution policy (null the
   field, delete, flag) is entirely that op's business.

Cadence, batching, and retry are driver decisions, out of framework
scope.

## Explicitly Not Built

- **Uniqueness defense-in-depth scan** — the synchronous sk check is
  the primary mechanism; a crash-window backstop adds machinery for a
  case the ownership/crash-release layer already bounds.
- Compound keys / FK-on-prefix (carried forward from the original
  spec), cascade policies, a cross-def type registry (codegen-shape
  work), framework-invoked resolution ops.

## Testing Strategy

- Pk discipline: v7 ids accepted and non-v7 rejected on Uuid types;
  derived mnemonic ids accepted and everything else rejected on Enum
  types; `enum_pk_id` stability/distinctness.
- Declaration validation: unknown field, PkEnum on a non-String field,
  PkUuid/SkUnique on a non-Bytes field — all panic with clear messages.
- `set`-time checks: enum membership pass/fail; malformed (non-16-byte)
  uuid FK values rejected.
- Lifecycle unit tests: satisfied reference commits; dangling +
  `Reject` fails the whole op with no writes; dangling + `Track`
  commits and the fk_pending datum holds the entry; the exists-check
  reply race with a same-op index update resolves (pending count grows
  mid-flight).
- fk_pending kind: Insert/Remove/List round-trips; duplicate inserts
  idempotent.
- Integration over the wire (`integration_fk_constraints.rs`):
  out-of-order creation — a dangling tracked write, the referenced
  entity created afterward, the driver scan observing resolution and
  removing the entry; a never-resolved entry surfaced for ConflictOp
  invocation (the test acting as driver invokes it and observes the
  op run); an enum-pk status type referenced by mnemonic end-to-end,
  including a rejected unknown mnemonic; a hard-reject dangling
  reference failing atomically. Stress 10x; standing 20x suites.
