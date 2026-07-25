# Part 5b — Delete-Side FK, Field Checks, Extent & Rescan Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline execution — this project never uses subagents) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete-side FK enforcement (Restrict/Track), declared field checks, the type-extent index, and the driver-run full-validation rescan, per `docs/superpowers/specs/2026-07-25-fk-delete-side-and-validation-design.md`.

**Architecture:** Four rail-pattern extensions on established machinery: `WriteThrough` (Put/Delete/None) retires the empty-blob approximation; `Expectation` generalizes exists checks to both polarities; delete markers reuse the `fk_pending` pair-set kind; the extent is one more self-persisted B+Tree kind with a paged wire query; the driver helper composes existing client calls.

**Tech Stack:** Established (sixth rail iteration).

## Global Constraints

Same as Part 5's plan (2-space indent, fmt/clippy clean per task, commit trailer, loud errors, framework never invokes ConflictOps, PROTOCOL_VERSION automatic), plus: PkEnum append-only is a documentation change on `PkKind`, folded into Task 3's commit.

---

### Task 1: `WriteThrough` enum

**Files:** `crates/seisin-node/src/index_handler.rs`, `crates/seisin-node/src/worker.rs`, `crates/seisin-types/src/sk_index.rs`, `crates/seisin-types/src/fk.rs`, `crates/seisin-types/src/lb_kind.rs` (touch: outcome literals), test updates throughout.

**Interfaces:** `IndexApplyOutcome.write_through` becomes `pub enum WriteThrough { None, Put(Vec<u8>), Delete }`. Worker's `IndexUpdate` arm: `Put(bytes)` → `cache.put`, `Delete` → `cache.delete`, `None` → nothing (all only when `violation.is_none()`). sk returns `Delete` when `entries.is_empty()` (Remove path), else `Put`; fk_pending likewise; rk/lb/tk return `None` everywhere; test fixtures (`AppendResident`, `FixedOutcomeResident`) return `Put`.

- [ ] Red: sk test `removing_the_last_entry_deletes_the_stored_datum` (apply Insert then Remove; outcome is `WriteThrough::Delete`); fk test `draining_the_pending_list_deletes_the_stored_datum`. Green: implement enum + ripple (compiler-guided; ~10 sites). Full workspace tests. Commit `feat: add WriteThrough Delete so emptied blob indexes vanish from storage`.

### Task 2: `Expectation` on exists checks

**Files:** `crates/seisin-ops/src/context.rs`, `crates/seisin-node/src/worker.rs`, `crates/seisin-types/src/typed_context.rs` (call-site updates only).

**Interfaces:** `PendingExistsCheck { target, expect: Expectation }`; `pub enum Expectation { Present { on_missing: FkMissingPolicy }, Absent { message: String } }`; `schedule_exists_check(target, expect)`. Worker: `IndexUpdateState.exists_policies` becomes `HashMap<DatumId, Expectation>`; `ExistsCheckReplied` handler matches `(exists, expectation)`: `(true, Present)` ok; `(false, Present{Reject})` violation; `(false, Present{Track})` dispatch + pending+1; `(true, Absent{message})` violation = message; `(false, Absent)` ok. Part 5 call sites wrap policies in `Expectation::Present`.

- [ ] Red: worker test `an_absent_expectation_fails_when_the_target_exists` (and passes when missing) via `schedule_exists_check(id, Expectation::Absent{..})`. Green; existing Part 5 worker/typed tests updated mechanically. Commit `feat: generalize exists checks to present/absent expectations`.

### Task 3: delete-side guards

**Files:** `crates/seisin-types/src/schema.rs` (`OnDelete`, `GuardRef`, `guards`, `.guard()` with declaration validation: nonempty names; PkEnum append-only doc note on `PkKind`), `crates/seisin-types/src/fk.rs` (`fk_deleted_key(type, field)`), `crates/seisin-types/src/typed_context.rs` (`delete` schedules per guard).

**Scheduling in `delete` (after `ensure_tracked`, only when the datum currently exists — `before.is_some()` at drop time; implement in Drop alongside constraints, keyed off `after.is_none() && before.is_some()`):** per `GuardRef`: probe = `sk_key(guard.type_name, guard.field, &FieldValue::Bytes(pk_id.as_bytes().to_vec()))`. Restrict → `Expectation::Absent { message: format!("delete restricted: {}.{} still references {:?}", ...) }`. Track → `schedule_index_update(fk_deleted_key(..), "fk_pending", encode_fk_pending_op(&Insert { referencing_pk: pk_id, target: probe }))` (marker = (deleted_pk, sk_probe_key), per spec).

- [ ] Red: typed_context tests — a guarded delete schedules the Absent probe at the right sk key (Restrict) / the marker IndexUpdate at `fk_deleted_key` (Track); an unguarded delete schedules neither; a delete of a non-existent datum schedules nothing. Green. Commit `feat: add delete-side FK guards (restrict probes, track markers)`.

### Task 4: field checks

**Files:** `crates/seisin-types/src/schema.rs` (`FieldCheck`, `FieldCheckDef`, `checks`, `.check(field, FieldCheck)` — panics: unknown field; Gt/Ge/Lt/Le require the field and bound both I64 or both F64; Min/MaxLen require String or Bytes), `crates/seisin-types/src/typed_context.rs` (`check_static_constraints` gains the check loop: numeric compares via i64/f64 `>` etc.; lengths via `s.len()`/`b.len()`).

- [ ] Red: declaration panics (3 cases); set-time boundary tests (Gt(0) rejects 0 accepts 1; MinLen(1) rejects ""; Le(F64) at boundary accepts). Green. Commit `feat: add declared field validation checks enforced at set time`.

### Task 5: extent kind + wire + rescan declaration

**Files:** `crates/seisin-types/src/extent.rs` (new: `extent_key(type)`, `ExtentKind { data_dir }` — B+Tree key 16/value 1/page 4096, file `extent_<hex>.btree`; apply payload `ExtentOp::{Insert,Remove}{pk}` codec local to seisin-protocol; `query` payload = 12 bytes offset u64 ++ limit u32 → `scan_from_rank`, result = `encode_extent_result(total, pks)`; `register_extent_kind(registry, data_dir)`), `crates/seisin-protocol/src/lib.rs` (`Request::ExtentQuery { extent_datum_id, offset: u64, limit: u32 }` opcode 13, `Response::ExtentResult { total: u64, pks: Vec<DatumId> }` resp 12, `ExtentOp` + codecs `encode_extent_op`/`decode_extent_op`/`encode_extent_result`/`decode_extent_result`), `crates/seisin-node/src/pool.rs` (client-only arm), `crates/seisin-node/src/server.rs` (redirect-or `run_index_query(extent_datum_id, "extent", 12-byte payload)` → decode → `ExtentResult`), `crates/seisin-types/src/schema.rs` (`track_extent: bool` + `.track_extent()`; `rescan_every_millis: Option<u64>` + `.rescan_every_millis(n)`), `crates/seisin-types/src/typed_context.rs` (drop: tracked types schedule `ExtentOp::Insert` on create — `before.is_none() && after.is_some()` — and `Remove` on delete, target `extent_key(type)`).

- [ ] Red: protocol round-trips; extent kind unit tests (insert/remove/paging exactness across a forced multi-page tree, reopen); typed_context scheduling tests (create/delete/untracked). Green; workspace build. Commit `feat: add type extent kind, paged wire query, and rescan-rate declaration`.

### Task 6: driver helper

**Files:** `crates/seisin-types/src/driver.rs` (new), `crates/seisin-types/Cargo.toml` (move `seisin-client` from dev-dependencies to dependencies — the driver is a client-side library function).

**Interfaces:** per spec — `ValidationFinding { pk, field, problem }`, `validate_type(addr, def, read_op, page_size) -> anyhow::Result<Vec<ValidationFinding>>`: page `ExtentQuery`; per pk call `read_op`, decode (undecodable → finding, continue); re-run field checks + PkEnum membership; probe PkUuid (`ExistsCheck` on the Bytes id) and SkUnique (`ExistsCheck` on `sk_key(...)`) — misses → findings. No unit tests here (needs a live node); proven in Task 7.

- [ ] Implement; `cargo build -p seisin-types` clean. Commit `feat: add driver-side validate_type full-scan helper`.

### Task 7: integration, stress, docs

**Files:** `crates/seisin-types/tests/integration_delete_side_and_rescan.rs`, `docs/superpowers/PROGRESS.md`.

Scenario (single node; types: `user` (track_extent, guarded by `foo.user_id` — one Restrict user-type variant and one Track flavor via two guard entries is overkill: use Track on `user`, Restrict on a second type `team` guarded by `foo.team_id`); `foo` (track_extent, sk indexes on `user_id` and `team_id`, PkUuid constraints with `resolution: Some(drop_foo)`); ops: write/read/delete per type + `drop_foo`):
- Restrict: delete `team` while a foo references it → OpError containing "delete restricted"; delete the foo (sk list empties → WriteThrough::Delete) → team delete succeeds.
- Track: delete `user` → marker in `deleted_refs:foo.user_id` (`FkPending{List}` on `fk_deleted_key`); driver: read sk list via `read` op + `decode_sk_entries`, invoke `drop_foo` per entry, `FkPending{Remove}` the marker; foo gone.
- Rescan: `validate_type(foo)` clean before; seed a bad datum via a byte-level op (out-of-range check value + dangling uuid) → two findings; extent paging covers it.
- Stress 10x + standing 20x suites; full gates; PROGRESS entry (Part 5b closes the FK/validation story; next: Storage Tier); `git push`.

## Deferred (spec)
Macro DSL; extent sharding; framework-scheduled rescans; cross-def declaration validation; SetNull built-in.
