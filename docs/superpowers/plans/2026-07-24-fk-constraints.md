# FK Constraints & Pk Identity Discipline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline execution — this project never uses subagents) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Relational constraints (hard-reject or track-dangling with driver-run resolution) plus the UUIDv7-or-mnemonic-enum pk discipline, per `docs/superpowers/specs/2026-07-24-fk-constraints-design.md`.

**Architecture:** Enum-pk FKs validate statically inside `TypedOpContext::set`. Uuid/Sk FKs ride a new `ExistsCheck` message pair integrated into the op lifecycle's existing pending-replies state; a missing reference either fails the op (Reject) or dispatches an `IndexUpdate` into the blob-resident `fk_pending` kind (Track) before commit. The eventual scan is pure driver orchestration over client-facing `ExistsCheck` + `FkPending{List/Remove}` wire calls.

**Tech Stack:** Rust workspace, established rail patterns (five prior iterations), hand-rolled codecs.

## Global Constraints

- 2-space indent; `cargo fmt --check` / `cargo clippy --workspace --all-targets -- -D warnings` clean per task; commit per task with the `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` trailer.
- All pk/constraint violations are loud errors (`TypedOpContext` returns `Result`); declaration bugs panic (the rk/`self_address` policy).
- The framework never invokes a `ConflictOp` — resolution invocation is the scan driver's job.
- New wire variants inherit `PROTOCOL_VERSION` automatically.
- **Spec correction applied here** (fix the spec in Task 2's commit): `SkUnique`-constrained fields hold the referenced natural-key *value* — any sk-legal primitive (`Bool`/`I64`/`F64`/`String`/`Bytes`), with the check target derived via the existing `sk_key(type, field, value)`. Only `PkUuid` fields must be `FieldType::Bytes` holding 16 bytes.

---

### Task 1: Pk identity discipline (`seisin-types`)

**Files:**
- Modify: `crates/seisin-types/src/schema.rs` (PkKind, `DatumTypeDef.pk`, builder)
- Modify: `crates/seisin-types/src/typed_context.rs` (enforcement)
- Modify: `crates/seisin-types/src/sk_index.rs` only if `derived_id_namespace` needs re-export (it is already `pub(crate)`)

**Interfaces (produces):**

```rust
// schema.rs
pub enum PkKind { Uuid, Enum(Vec<String>) }   // derives Debug, Clone, PartialEq, Eq
// DatumTypeDef gains: pub pk: PkKind (default Uuid in new()); builder pub fn pk(mut self, pk: PkKind) -> Self
pub fn enum_pk_id(type_name: &str, mnemonic: &str) -> DatumId  // from_name over "pk:{type}:{mnemonic}"
```

Enforcement in `TypedOpContext`: a private `check_pk(&self, pk_id, def) -> Result<()>` called at the top of `set` and `delete`:

```rust
fn check_pk(pk_id: DatumId, def: &DatumTypeDef) -> Result<()> {
  match &def.pk {
    PkKind::Uuid => {
      // UUID version nibble: high 4 bits of byte 6.
      if pk_id.as_bytes()[6] >> 4 != 7 {
        bail!(
          "type {:?} declares Uuid pk identity: id {:?} is not a version-7 uuid",
          def.name, pk_id
        );
      }
    }
    PkKind::Enum(mnemonics) => {
      if !mnemonics.iter().any(|m| enum_pk_id(&def.name, m) == pk_id) {
        bail!(
          "type {:?} declares Enum pk identity: id {:?} matches none of its {} mnemonics",
          def.name, pk_id, mnemonics.len()
        );
      }
    }
  }
  Ok(())
}
```

(Free function in `schema.rs` next to `enum_pk_id`, so tests hit it directly; `TypedOpContext` calls it.)

- [ ] **Step 1: Failing tests.** `schema.rs`: `enum_pk_id` stable across calls, distinct per type/mnemonic; `check_pk` accepts `DatumId::new()` on Uuid types and rejects a `DatumId::from_name`-derived id; accepts each derived mnemonic id on an Enum type (`PkKind::Enum(vec!["active","closed"])`) and rejects `DatumId::new()` and a foreign mnemonic's id. `typed_context.rs`: `set` with a derived-id on a Uuid-pk type errors; `set` with `DatumId::new()` on an Enum-pk type errors; `set` with `enum_pk_id("status","active")` on the Enum-pk `status` type (one String field) succeeds; existing tests (all use `DatumId::new()` on default-Uuid types) stay green.
- [ ] **Step 2:** run `cargo test -p seisin-types` → new tests fail (missing types).
- [ ] **Step 3:** implement (`pk: PkKind` field breaks no existing struct literals — the codebase always builds `DatumTypeDef` via `new()`; update `new()` to set `pk: PkKind::Uuid`).
- [ ] **Step 4:** `cargo test -p seisin-types` → all PASS.
- [ ] **Step 5:** commit `feat: add UUIDv7-or-enum pk identity discipline`.

---

### Task 2: Constraint declaration + set-time checks (`seisin-types`)

**Files:**
- Modify: `crates/seisin-types/src/schema.rs`
- Modify: `crates/seisin-types/src/typed_context.rs`
- Modify: `docs/superpowers/specs/2026-07-24-fk-constraints-design.md` (SkUnique field-typing correction)

**Interfaces (produces):**

```rust
pub enum FkTarget {                             // derives Debug, Clone, PartialEq, Eq
  PkUuid { type_name: String },
  PkEnum { type_name: String, mnemonics: Vec<String> },
  SkUnique { type_name: String, field: String },
}
pub struct RelationalConstraintDef {            // derives Debug, Clone, PartialEq, Eq
  pub field: String,
  pub references: FkTarget,
  pub resolution: Option<ConflictOp>,
}
// DatumTypeDef gains: pub constraints: Vec<RelationalConstraintDef> (empty in new());
// builder pub fn constraint(mut self, c: RelationalConstraintDef) -> Self with validation:
//   - c.field must be declared;
//   - PkEnum  => field's FieldType == String;
//   - PkUuid  => field's FieldType == Bytes;
//   - SkUnique => field's FieldType is a primitive (not Array/Dict);
//   panics otherwise (schema declaration bug, process-start class).
```

Set-time checks in `TypedOpContext::set`, after `encode_datum` succeeds (per-constraint on the new values): `PkEnum` → the field's `String` must be in `mnemonics` (bail otherwise); `PkUuid` → the field's `Bytes` must be exactly 16 long. `SkUnique` values need no set-time shape check beyond the declaration-time primitive restriction.

- [ ] **Step 1: Failing tests.** Declaration panics: unknown field; `PkEnum` on an `I64` field; `PkUuid` on a `String` field; `SkUnique` on an `Array` field. Set-time: unknown mnemonic bails with the mnemonic in the message; known mnemonic succeeds; 15-byte `Bytes` value on a `PkUuid` field bails; 16-byte succeeds (with no exists-check scheduled yet — that's Task 5, assert `take_pending_index_updates` empty is fine here).
- [ ] **Step 2–4:** red → implement → green (`cargo test -p seisin-types`).
- [ ] **Step 5:** also apply the spec correction (replace the spec's "`PkUuid`/`SkUnique` fields must be `FieldType::Bytes`" sentence with the corrected per-target rules above, stating why: an SkUnique FK holds the referenced natural-key value, which is what `sk_key` derives the check target from). Commit `feat: add relational constraint declaration with static enum-FK checks`.

---

### Task 3: wire — `ExistsCheck`/`Exists`, `FkPending`/`FkPendingResult`

**Files:**
- Modify: `crates/seisin-protocol/src/lib.rs`
- Modify: `crates/seisin-node/src/pool.rs`

**Interfaces (produces):**

```rust
Request::ExistsCheck { datum_id: DatumId }          // opcode 11; node-to-node AND client-facing
Response::Exists { exists: bool }                    // resp 10; body = 1 flag byte
Request::FkPending { pending_datum_id: DatumId, op: FkPendingOp }  // opcode 12; client-facing only
pub enum FkPendingOp {                               // tag byte 0/1
  List,
  Remove { referencing_pk: DatumId, target: DatumId },
}
Response::FkPendingResult { entries: Vec<(DatumId, DatumId)> }     // resp 11
pub fn encode_fk_pending_op(op: &FkPendingOp) -> Vec<u8>;          // + decode
pub fn encode_fk_entries(entries: &[(DatumId, DatumId)]) -> Vec<u8>; // u32 count + 32-byte pairs; + decode (strict lengths)
```

`pool.rs` `on_request`: `ExistsCheck` maps to `WorkerMessage::ExistsCheck` with a Remote reply (Task 4 defines the message; write this arm in Task 4 — here add `Request::ExistsCheck { .. } | Request::FkPending { .. } => return,` temporarily so the workspace builds, replaced in Task 4).

- [ ] **Step 1: Failing tests** (protocol): round-trip `ExistsCheck` request; `Exists { exists: true/false }` responses; `FkPending` with both op variants; `FkPendingResult` with 0 and 2 entries; truncated `decode_fk_entries` rejected.
- [ ] **Step 2–4:** red → implement (cursor-helper style, strict trailing checks; `FkPending` decode: id + `decode_fk_pending_op(&buf[offset..])`) → `cargo test -p seisin-protocol && cargo build --workspace` green.
- [ ] **Step 5:** commit `feat: add ExistsCheck and FkPending wire pairs`.

---

### Task 4: op-lifecycle exists checks (`seisin-ops` + `seisin-node`)

**Files:**
- Modify: `crates/seisin-ops/src/context.rs`
- Modify: `crates/seisin-node/src/worker.rs`
- Modify: `crates/seisin-node/src/pool.rs`
- Modify: `crates/seisin-node/src/server.rs`

**Interfaces (produces):**

```rust
// seisin-ops context.rs
pub enum FkMissingPolicy {
  Reject,
  Track { pending_datum: DatumId, index_kind: String, entry: Vec<u8> },
}
pub struct PendingExistsCheck { pub target: DatumId, pub on_missing: FkMissingPolicy }
// OpContext gains: pub fn schedule_exists_check(&mut self, target: DatumId, on_missing: FkMissingPolicy)
//                  pub fn take_pending_exists_checks(&mut self) -> Vec<PendingExistsCheck>
// (Track carries index_kind — "fk_pending" — so seisin-ops stays string-agnostic like PendingIndexUpdate.)

// seisin-node
WorkerHandle::run_exists_check(&self, datum_id: DatumId) -> bool   // sync, for the server path
WorkerPool::run_exists_check(&self, datum_id: DatumId) -> bool     // routes via ring.native
```

Worker mechanics (mirrors `IndexUpdate` end to end):

1. `WorkerMessage::ExistsCheck { datum_id, op_id: DatumId, reply: ExistsReply }` where

```rust
pub(crate) enum ExistsReply {
  Local(Sender<WorkerMessage>),           // op lifecycle, same node
  Remote(Arc<PeerLink>, u64),             // op lifecycle, cross node
  Sync(Sender<bool>),                     // server's client-facing path
}
impl ExistsReply {
  fn respond(self, op_id: DatumId, target: DatumId, exists: bool) {
    match self {
      ExistsReply::Local(inbox) => {
        let _ = inbox.send(WorkerMessage::ExistsCheckReplied { op_id, target, exists });
      }
      ExistsReply::Remote(link, correlation_id) => {
        link.respond(correlation_id, seisin_protocol::Response::Exists { exists });
      }
      ExistsReply::Sync(tx) => {
        let _ = tx.send(exists);
      }
    }
  }
}
```

2. Handler arm: `let exists = cache.get(datum_id).is_some(); reply.respond(op_id, datum_id, exists);` (existence = stored/cached bytes — exact for pk datums; the documented sk approximation).

3. `IndexUpdateState` gains `exists_policies: HashMap<DatumId, seisin_ops::context::FkMissingPolicy>`. `try_run_if_ready` drains `take_pending_exists_checks()` alongside index updates; `pending = index_updates.len() + exists_checks.len()`; for each check it calls a new `dispatch_exists_check` (same local/remote split as `dispatch_index_update`, sending `Request::ExistsCheck` over the peer-link with the callback mapping `Response::Exists` — anything else counts as `exists: false`, the reactive-failure convention).

4. `WorkerMessage::ExistsCheckReplied { op_id, target, exists }` handler:

```rust
          WorkerMessage::ExistsCheckReplied { op_id, target, exists } => {
            if let Some(record) = op_records.get_mut(&op_id) {
              if let Some(state) = &mut record.index_update_state {
                state.pending -= 1;
                if !exists {
                  match state.exists_policies.remove(&target) {
                    Some(seisin_ops::context::FkMissingPolicy::Track {
                      pending_datum,
                      index_kind,
                      entry,
                    }) => {
                      // Dangling but tracked: record it in fk_pending and
                      // keep the op alive — pending grows mid-flight.
                      state.pending += 1;
                      dispatch_index_update(
                        &ring, &peers, &peer_links, self_node_id, op_id,
                        pending_datum, index_kind, entry, join_sender.clone(),
                      );
                    }
                    _ => {
                      if state.violation.is_none() {
                        state.violation =
                          Some(format!("dangling reference: {target:?} does not exist"));
                      }
                    }
                  }
                }
                if state.pending == 0 { /* identical commit-or-fail block to
                  IndexUpdateReplied — factor the existing block into a
                  helper fn finish_op_if_settled(op_id, ...) called from BOTH
                  reply handlers rather than duplicating it */ }
              }
            }
          }
```

  The factoring note is mandatory: extract the commit-or-fail tail of `IndexUpdateReplied` into `finish_op_if_settled(op_id, op_records, cache, ring, peers, peer_links, self_node_id)` and call it from both handlers.

5. `pool.rs` `on_request`: replace Task 3's temporary arm — `ExistsCheck` → `WorkerMessage::ExistsCheck { datum_id, op_id: DatumId::from_bytes([0;16]), reply: ExistsReply::Remote(link, cid) }` (op_id is echo-only for Remote replies; a nil id is fine and never read), routed to `target_thread`; `FkPending { .. } => return` (client-only).

6. `server.rs`: `Request::ExistsCheck` → redirect-or `Response::Exists { exists: pool.run_exists_check(datum_id) }`; `Request::FkPending` → redirect-or map `FkPendingOp::List` → `run_index_query(pending_datum_id, "fk_pending", encode_fk_pending_op(&op))`, `Remove` → `run_index_execute(...)`, both decoding `decode_fk_entries` into `Response::FkPendingResult`.

- [ ] **Step 1: Failing tests** (`worker.rs` tests): an op scheduling an exists check on a datum that has bytes commits (register a `put_first`-seeded datum, then an op whose handler calls `ctx.schedule_exists_check(existing_id, FkMissingPolicy::Reject)`); the same against a never-written id fails with "dangling reference"; a missing id with `Track` (registering the `FixedOutcomeKind` under `"fk_pending"` as the tracked kind for the test) commits AND the fk_pending target received an apply (`FixedOutcomeResident` write-through observable via a follow-up `run_index_query`); `run_exists_check` sync path true/false.
- [ ] **Step 2–4:** red → implement → `cargo test -p seisin-node && cargo build --workspace` green.
- [ ] **Step 5:** commit `feat: integrate exists checks into the op lifecycle with track-dangling dispatch`.

---

### Task 5: `fk_pending` kind + `TypedOpContext` scheduling (`seisin-types`)

**Files:**
- Create: `crates/seisin-types/src/fk.rs`
- Modify: `crates/seisin-types/src/lib.rs` (`pub mod fk;`)
- Modify: `crates/seisin-types/src/typed_context.rs` (Drop scheduling)

**Interfaces (produces):**

```rust
pub fn fk_pending_key(type_name: &str, field: &str) -> DatumId;   // "fk_pending:{type}.{field}"
pub struct FkPendingKind;                                          // blob-resident, sk mechanics
pub fn register_fk_pending_kind(registry: &mut IndexKindRegistry);
// Resident state: Vec<(DatumId, DatumId)> decoded once on open (undecodable stored bytes = open error).
// apply: payload = encode_fk_pending_insert(referencing, target) (tag 0 ++ 32 bytes) or the protocol's
//        FkPendingOp::Remove encoding — Insert is idempotent (skip if pair present); write_through = re-encoded list.
// query: payload = encode_fk_pending_op(List) -> encode_fk_entries(list)
// execute: payload = encode_fk_pending_op(Remove{..}) -> removes pair, returns encode_fk_entries(remaining)
// (Insert arrives via the write-path IndexUpdate; Remove via the driver's execute; one payload
//  codec family in seisin-protocol covers all three.)
```

Note: to keep one codec family, extend `FkPendingOp` in Task 3 with `Insert { referencing_pk: DatumId, target: DatumId }` (tag 2) used only as an apply payload (never on the `Request::FkPending` wire — `server.rs` maps only List/Remove; document on the enum).

`TypedOpContext::drop` gains constraint scheduling, after the per-index loop, for every tracked+touched datum with a new value: for each constraint whose field changed (or `before` is `None`):

```rust
        let target = match &constraint.references {
          FkTarget::PkEnum { .. } => continue, // validated at set() — no runtime check
          FkTarget::PkUuid { .. } => {
            let FieldValue::Bytes(bytes) = &new_values[field_idx] else { continue };
            DatumId::from_bytes(bytes.as_slice().try_into().expect("16 bytes checked at set"))
          }
          FkTarget::SkUnique { type_name, field } => {
            match sk_key(type_name, field, &new_values[field_idx]) {
              Ok(id) => id,
              Err(_) => continue, // non-primitive rejected at declaration
            }
          }
        };
        let on_missing = match &constraint.resolution {
          None => FkMissingPolicy::Reject,
          Some(_) => FkMissingPolicy::Track {
            pending_datum: fk_pending_key(&tracked.def.name, &constraint.field),
            index_kind: "fk_pending".to_string(),
            entry: encode_fk_pending_op(&FkPendingOp::Insert {
              referencing_pk: pk_id,
              target,
            }),
          },
        };
        self.ctx.schedule_exists_check(target, on_missing);
```

- [ ] **Step 1: Failing tests.** `fk.rs`: key derivation stable/distinct; kind apply Insert (cold + idempotent duplicate), Remove, List query, Remove execute, undecodable stored bytes error on open. `typed_context.rs`: a `PkUuid`-constrained write schedules one exists check with `Reject` when no resolution declared and `Track` (correct pending_datum + decodable Insert entry) when declared; an unchanged FK field schedules nothing; `PkEnum` constraints schedule nothing; an `SkUnique` constraint targets `sk_key(ref_type, ref_field, value)`.
- [ ] **Step 2–4:** red → implement → `cargo test -p seisin-types` green (plus the Task 3 `FkPendingOp::Insert` extension and its round-trip test in seisin-protocol).
- [ ] **Step 5:** commit `feat: add fk_pending kind and automatic exists-check scheduling`.

---

### Task 6: integration, stress, docs

**Files:**
- Create: `crates/seisin-types/tests/integration_fk_constraints.rs`
- Modify: `docs/superpowers/PROGRESS.md`

- [ ] **Step 1: Integration test** (lb/tk bootstrap pattern; registry gets `register_fk_pending_kind`; ops registry gets `write_order` — a `TypedOpContext` op over an `order` type with a `customer_id` FK (`PkUuid` → `customer`, `resolution: Some(ConflictOp("null_customer"))`), a `status` field (`PkEnum` → the `status` type's `["active","closed"]`), and a `write_customer` op; plus a `null_customer` op that blanks the field and a `read` op):
  - Enum path: an order with `status: "active"` commits; `status: "bogus"` gets `OpError` mentioning the mnemonic.
  - Hard-reject path: a second constraint variant (`resolution: None`) on another type/field — a dangling reference fails the op and a follow-up read shows nothing written.
  - Track path (the `_e_` motivation): write an order referencing a not-yet-created customer id → commits; `FkPending { List }` shows the entry; create the customer; driver re-check (`ExistsCheck` over the wire → true) → `FkPending { Remove }` → `List` empty.
  - Unresolved path: another order referencing a never-created id; driver probe false → driver invokes `null_customer` via `Request::Op`; read confirms the field was blanked; driver removes the pending entry.
- [ ] **Step 2:** run; systematic-debugging if needed.
- [ ] **Step 3: Stress** — 10x this test; 20x the standing wound-wait/collation suites (worker.rs's loop changed again).
- [ ] **Step 4: Gates** — full `cargo test --workspace && cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings`.
- [ ] **Step 5: PROGRESS.md** — Done entry (established format); mark the Datum Type System complete (Parts 1–5) and Storage Tier as next.
- [ ] **Step 6:** `git add -A && git commit -m "feat: FK constraints with pk identity discipline end-to-end" && git push`.

---

## Deliberately Out of Scope (from the spec — do not build)

- Uniqueness defense-in-depth scan; compound/prefix FKs; cascade policies; cross-def type registry; framework-invoked ConflictOps; exact write-time sk existence (documented approximation, exact at scan time).
