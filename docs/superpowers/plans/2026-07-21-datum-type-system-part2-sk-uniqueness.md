# Datum Type System, Part 2: sk Index & Uniqueness Constraint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Automatically maintain sk (secondary-key) index entries when a typed datum's indexed field changes, and enforce uniqueness constraints on sk indexes — a best-effort synchronous check at write time, plus a standalone violation-detection helper for eventual/defensive re-checking.

**Architecture:** Everything lives in `seisin-types` (Part 1's crate) except one small, genuinely reusable addition to `seisin-core`: `DatumId::from_name`, a deterministic (UUIDv5, name-based) id constructor — needed because sk keys must resolve to the *same* datum_id every time the same `(type, field, value)` combination is written, unlike `DatumId::new()`'s time-based randomness. The write path is a client-driven two-round-trip flow (a plain read, then the actual write op declaring every datum it touches up front), matching the design doc's resolution of the "op needs a datum it hasn't read yet" tension — no changes to collation/wound-wait. A detected uniqueness violation is never auto-resolved in-process (there's no nested-op-invocation mechanism in this framework, and adding one is explicitly out of scope here) — it's surfaced back to the client as data, and the client-side helper makes an ordinary follow-up call to the solution's declared conflict-resolution op.

**Tech Stack:** Rust, hand-rolled encoding (matching Part 1). New dependency: `seisin-core`'s `uuid` dependency gains the `v5` feature (already depends on `uuid`, just enabling another cargo feature — not a new crate).

## Global Constraints

- 2-space indentation.
- Commit and push after every task.
- No serde or other external encoding dependency.
- This plan covers only Part 2 of 5 (see `docs/superpowers/specs/2026-07-21-datum-type-system-design.md`'s "sk Index" section and the sk half of "Constraint Enforcement"). Parts 3 (rk), 4 (tk), 5 (relational/FK constraints) are separate, later plans.
- **Explicitly out of scope, by design decision**: automatically invoking a solution's declared `ConflictOp` in-process when a violation is detected. There is no nested-op-invocation mechanism in this framework (`OpHandler`'s signature has no way to call another named op), and adding one is a real, separate framework change not needed for this plan — a detected violation is surfaced as data; the client-side helper makes an ordinary follow-up call instead.

---

### Task 1: `DatumId::from_name` — deterministic, name-based ids

**Files:**
- Modify: `crates/seisin-core/Cargo.toml`
- Modify: `crates/seisin-core/src/datum.rs`

**Interfaces:**
- Produces: `DatumId::from_name(namespace: &DatumId, name: &[u8]) -> DatumId`.

- [ ] **Step 1: Write the failing tests**

Add to `crates/seisin-core/src/datum.rs`'s `mod tests`:

```rust
  #[test]
  fn from_name_is_deterministic() {
    let ns = DatumId::new();
    let a = DatumId::from_name(&ns, b"sk:user.name:cliff");
    let b = DatumId::from_name(&ns, b"sk:user.name:cliff");
    assert_eq!(a, b);
  }

  #[test]
  fn from_name_differs_for_different_names() {
    let ns = DatumId::new();
    let a = DatumId::from_name(&ns, b"sk:user.name:cliff");
    let b = DatumId::from_name(&ns, b"sk:user.name:someone_else");
    assert_ne!(a, b);
  }

  #[test]
  fn from_name_differs_across_namespaces() {
    let a = DatumId::from_name(&DatumId::new(), b"same name");
    let b = DatumId::from_name(&DatumId::new(), b"same name");
    assert_ne!(a, b);
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-core --lib datum::tests::from_name`
Expected: FAIL with "no function or associated item named `from_name` found"

- [ ] **Step 3: Enable the `v5` uuid feature**

In `crates/seisin-core/Cargo.toml`, change:

```toml
uuid = { version = "1", features = ["v7"] }
```

to:

```toml
uuid = { version = "1", features = ["v7", "v5"] }
```

- [ ] **Step 4: Implement `DatumId::from_name`**

Add to `impl DatumId` in `crates/seisin-core/src/datum.rs`, after `from_bytes`:

```rust
  /// Deterministically derives an id from `namespace` and `name` — the
  /// same `(namespace, name)` pair always produces the same `DatumId`,
  /// unlike `new()`'s time-based randomness. Used for keys that must
  /// resolve to the same datum every time (sk/rk/tk index keys), backed
  /// by UUIDv5 (the standard name-based UUID scheme).
  pub fn from_name(namespace: &DatumId, name: &[u8]) -> Self {
    DatumId(Uuid::new_v5(&namespace.0, name))
  }
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p seisin-core --lib datum::`
Expected: PASS (6 tests: 3 existing + 3 new).

- [ ] **Step 6: Commit and push**

```bash
git add crates/seisin-core/Cargo.toml crates/seisin-core/src/datum.rs
git commit -m "feat: add DatumId::from_name for deterministic index-key ids"
git push
```

---

### Task 2: `sk_key` — deterministic sk datum_id derivation

**Files:**
- Create: `crates/seisin-types/src/sk_index.rs`
- Modify: `crates/seisin-types/src/lib.rs`

**Interfaces:**
- Consumes: `DatumId::from_name` (Task 1), `FieldValue` (Part 1).
- Produces: `sk_key(type_name: &str, field_name: &str, value: &FieldValue) -> anyhow::Result<DatumId>`.

sk indexes are restricted to primitive-valued fields (`Bool`/`I64`/`F64`/`String`/`Bytes`) — an `Array`/`Dict` value has no canonical, unambiguous byte representation for keying, and the design doc's own `sk:user.name:cliff` example is inherently a scalar-value key. `sk_key` rejects non-primitive values.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/seisin-types/src/sk_index.rs, at the bottom
#[cfg(test)]
mod tests {
  use super::*;
  use crate::field::FieldValue;

  #[test]
  fn same_type_field_and_value_always_derive_the_same_key() {
    let a = sk_key("user", "name", &FieldValue::String("cliff".to_string())).unwrap();
    let b = sk_key("user", "name", &FieldValue::String("cliff".to_string())).unwrap();
    assert_eq!(a, b);
  }

  #[test]
  fn a_different_value_derives_a_different_key() {
    let a = sk_key("user", "name", &FieldValue::String("cliff".to_string())).unwrap();
    let b = sk_key("user", "name", &FieldValue::String("someone_else".to_string())).unwrap();
    assert_ne!(a, b);
  }

  #[test]
  fn a_different_field_derives_a_different_key_for_the_same_value() {
    let a = sk_key("user", "name", &FieldValue::String("x".to_string())).unwrap();
    let b = sk_key("user", "nickname", &FieldValue::String("x".to_string())).unwrap();
    assert_ne!(a, b);
  }

  #[test]
  fn a_different_type_derives_a_different_key_for_the_same_field_and_value() {
    let a = sk_key("user", "name", &FieldValue::String("x".to_string())).unwrap();
    let b = sk_key("widget", "name", &FieldValue::String("x".to_string())).unwrap();
    assert_ne!(a, b);
  }

  #[test]
  fn numeric_and_bool_values_derive_keys_too() {
    assert!(sk_key("user", "age", &FieldValue::I64(41)).is_ok());
    assert!(sk_key("user", "score", &FieldValue::F64(1.5)).is_ok());
    assert!(sk_key("user", "active", &FieldValue::Bool(true)).is_ok());
    assert!(sk_key("user", "avatar", &FieldValue::Bytes(vec![1, 2])).is_ok());
  }

  #[test]
  fn array_and_dict_values_are_rejected() {
    assert!(sk_key("user", "tags", &FieldValue::Array(vec![])).is_err());
    assert!(sk_key("user", "meta", &FieldValue::Dict(vec![])).is_err());
  }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-types --lib sk_index::`
Expected: FAIL to compile — `sk_key` doesn't exist yet.

- [ ] **Step 3: Implement `sk_key`**

```rust
// crates/seisin-types/src/sk_index.rs, above the tests module
//! sk (secondary-key) index maintenance: deriving a stable datum_id for
//! a `(type, field, value)` key, and keeping its entry list in sync with
//! writes. See the design doc's "sk Index" section.

use anyhow::{bail, Result};
use seisin_core::datum::DatumId;

use crate::field::FieldValue;

/// A fixed, arbitrary namespace for every `DatumId::from_name` call this
/// crate makes (sk/rk/tk keys alike) — the name strings themselves are
/// already distinguished by their `sk:`/`rk:`/`tk:` prefixes, so a single
/// shared namespace is sufficient; it exists only because UUIDv5 requires
/// one, not to partition anything. `DatumId::from_bytes` isn't `const
/// fn`, so this is a plain function (cheap — it's just constructing a
/// 16-byte value) rather than a `const`.
fn derived_id_namespace() -> DatumId {
  DatumId::from_bytes([
    0x5e, 0x15, 0x1a, 0x00, 0xd1, 0xd5, 0x4e, 0x1d, 0x9a, 0x53, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
  ])
}

/// Derives the stable `DatumId` for the sk datum holding entries for
/// `type_name.field_name` at `value` — the same triple always derives
/// the same id. `value` must be a primitive (`Bool`/`I64`/`F64`/
/// `String`/`Bytes`); `Array`/`Dict` values have no canonical byte
/// representation to key on and are rejected.
pub fn sk_key(type_name: &str, field_name: &str, value: &FieldValue) -> Result<DatumId> {
  let mut name = format!("sk:{type_name}.{field_name}:").into_bytes();
  match value {
    FieldValue::Bool(b) => name.push(u8::from(*b)),
    FieldValue::I64(i) => name.extend_from_slice(&i.to_le_bytes()),
    FieldValue::F64(f) => name.extend_from_slice(&f.to_le_bytes()),
    FieldValue::String(s) => name.extend_from_slice(s.as_bytes()),
    FieldValue::Bytes(b) => name.extend_from_slice(b),
    FieldValue::Array(_) | FieldValue::Dict(_) => {
      bail!("sk index values must be primitive; got a non-primitive value for {type_name}.{field_name}")
    }
  }
  Ok(DatumId::from_name(&derived_id_namespace(), &name))
}
```

Add `pub mod sk_index;` to `crates/seisin-types/src/lib.rs`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-types --lib sk_index::`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-types/src/sk_index.rs crates/seisin-types/src/lib.rs
git commit -m "feat: add deterministic sk_key derivation"
git push
```

---

### Task 3: `IndexDef`/`ConflictOp` and `DatumTypeDef.indexes`

**Files:**
- Modify: `crates/seisin-types/src/schema.rs`

**Interfaces:**
- Produces: `ConflictOp(pub String)`, `IndexDef::Sk { field: String, unique: Option<ConflictOp> }`, `DatumTypeDef.indexes: Vec<IndexDef>`, `DatumTypeDef::index(self, index: IndexDef) -> Self` (builder).

- [ ] **Step 1: Write the failing tests**

Add to `crates/seisin-types/src/schema.rs`'s `mod tests`:

```rust
  #[test]
  fn builder_accumulates_indexes_in_declared_order() {
    let def = DatumTypeDef::new("user")
      .field("name", FieldType::String)
      .index(IndexDef::Sk {
        field: "name".to_string(),
        unique: None,
      });
    assert_eq!(
      def.indexes,
      vec![IndexDef::Sk {
        field: "name".to_string(),
        unique: None,
      }]
    );
  }

  #[test]
  fn a_unique_index_carries_its_conflict_op_name() {
    let def = DatumTypeDef::new("user")
      .field("email", FieldType::String)
      .index(IndexDef::Sk {
        field: "email".to_string(),
        unique: Some(ConflictOp("resolve_duplicate_email".to_string())),
      });
    match &def.indexes[0] {
      IndexDef::Sk { unique: Some(op), .. } => assert_eq!(op.0, "resolve_duplicate_email"),
      other => panic!("expected a unique Sk index, got {other:?}"),
    }
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-types --lib schema::tests::builder_accumulates_indexes_in_declared_order`
Expected: FAIL to compile — `IndexDef`/`ConflictOp`/`.index(...)`/`.indexes` don't exist yet.

- [ ] **Step 3: Implement `IndexDef`, `ConflictOp`, and `DatumTypeDef.indexes`**

Change `DatumTypeDef` in `crates/seisin-types/src/schema.rs` from:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct DatumTypeDef {
  pub name: String,
  pub fields: Vec<(String, FieldType)>,
}

impl DatumTypeDef {
  pub fn new(name: impl Into<String>) -> Self {
    Self {
      name: name.into(),
      fields: Vec::new(),
    }
  }

  /// Appends a field to the type, in declaration order — that order is
  /// what `encode_datum`/`decode_datum` use, not the field name.
  pub fn field(mut self, name: impl Into<String>, ty: FieldType) -> Self {
    self.fields.push((name.into(), ty));
    self
  }
}
```

to:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct DatumTypeDef {
  pub name: String,
  pub fields: Vec<(String, FieldType)>,
  pub indexes: Vec<IndexDef>,
}

impl DatumTypeDef {
  pub fn new(name: impl Into<String>) -> Self {
    Self {
      name: name.into(),
      fields: Vec::new(),
      indexes: Vec::new(),
    }
  }

  /// Appends a field to the type, in declaration order — that order is
  /// what `encode_datum`/`decode_datum` use, not the field name.
  pub fn field(mut self, name: impl Into<String>, ty: FieldType) -> Self {
    self.fields.push((name.into(), ty));
    self
  }

  /// Declares an index on this type — see `IndexDef`.
  pub fn index(mut self, index: IndexDef) -> Self {
    self.indexes.push(index);
    self
  }
}

/// Names a registered op (via `OpRegistry`, same mechanism as any domain
/// op) to call when a constraint violation is detected — see the design
/// doc's "Constraint Enforcement" section. Nothing in this crate invokes
/// it automatically; it's data a caller (the client-side typed-write
/// helper, in this plan) uses to make its own follow-up call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictOp(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexDef {
  Sk {
    field: String,
    unique: Option<ConflictOp>,
  },
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-types --lib schema::`
Expected: PASS (7 tests: 5 existing + 2 new).

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-types/src/schema.rs
git commit -m "feat: add IndexDef::Sk with uniqueness and DatumTypeDef.indexes"
git push
```

---

### Task 4: sk entry list maintenance and uniqueness violation detection

**Files:**
- Modify: `crates/seisin-types/src/sk_index.rs`

**Interfaces:**
- Consumes: `sk_key` (this file, Task 2), `seisin_core::sk::{encode_sk_entries, decode_sk_entries}` (existing, Sub-project 1), `seisin_core::authority::AuthorityIdx`.
- Produces: `insert_sk_entry(ctx: &mut OpContext, sk_key: DatumId, pk_id: DatumId) -> anyhow::Result<Option<UniquenessViolation>>`, `remove_sk_entry(ctx: &mut OpContext, sk_key: DatumId, pk_id: DatumId)`, `UniquenessViolation { pub sk_key: DatumId, pub conflict_op: String }`.

`insert_sk_entry` is where the best-effort uniqueness check lives: it's only ever called with a `unique` flag's `ConflictOp` in hand from the caller (Task 5 wires this) — but to keep this function's own contract simple and independently testable, it always performs the check-and-report; callers that don't care about uniqueness simply ignore a `None` result (an unflagged sk index never has a `ConflictOp` to check against, so Task 5 only passes one in when the index is actually declared unique).

- [ ] **Step 1: Write the failing tests**

Add to `crates/seisin-types/src/sk_index.rs`'s `mod tests`:

```rust
  use seisin_core::cache::Cache;
  use seisin_core::store::InMemoryStore;
  use seisin_ops::context::OpContext;
  use std::sync::Arc;

  #[test]
  fn insert_then_remove_round_trips_through_a_real_cache() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let key = sk_key("user", "name", &FieldValue::String("cliff".to_string())).unwrap();
    let pk_id = DatumId::new();

    let violation = insert_sk_entry(&mut ctx, key, pk_id, None).unwrap();
    assert!(violation.is_none());

    remove_sk_entry(&mut ctx, key, pk_id);
    let bytes = ctx.get(key).unwrap();
    assert_eq!(seisin_core::sk::decode_sk_entries(&bytes).unwrap(), vec![]);
  }

  #[test]
  fn a_second_insert_of_a_different_pk_id_is_flagged_as_a_violation() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let key = sk_key("user", "email", &FieldValue::String("a@example.com".to_string())).unwrap();
    let first_pk = DatumId::new();
    let second_pk = DatumId::new();

    let first = insert_sk_entry(&mut ctx, key, first_pk, Some("resolve".to_string())).unwrap();
    assert!(first.is_none());

    let second = insert_sk_entry(&mut ctx, key, second_pk, Some("resolve".to_string())).unwrap();
    let violation = second.expect("a second distinct pk_id must be flagged");
    assert_eq!(violation.sk_key, key);
    assert_eq!(violation.conflict_op, "resolve");
  }

  #[test]
  fn inserting_the_same_pk_id_twice_is_not_a_violation() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let key = sk_key("user", "email", &FieldValue::String("a@example.com".to_string())).unwrap();
    let pk_id = DatumId::new();

    insert_sk_entry(&mut ctx, key, pk_id, Some("resolve".to_string())).unwrap();
    let second = insert_sk_entry(&mut ctx, key, pk_id, Some("resolve".to_string())).unwrap();
    assert!(second.is_none(), "re-inserting the same pk_id is not a duplicate");
  }

  #[test]
  fn remove_on_a_missing_key_is_a_no_op() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let key = sk_key("user", "name", &FieldValue::String("nobody".to_string())).unwrap();
    remove_sk_entry(&mut ctx, key, DatumId::new()); // must not panic
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-types --lib sk_index::tests::a_second_insert`
Expected: FAIL to compile — `insert_sk_entry`/`remove_sk_entry`/`UniquenessViolation` don't exist yet, and `seisin-types` doesn't depend on `seisin-ops` yet.

- [ ] **Step 3: Add the `seisin-ops` dev-dependency and implement `insert_sk_entry`/`remove_sk_entry`**

`seisin-types`'s production code (Task 5 next) needs `OpContext` too, so add `seisin-ops` as a normal dependency now, not a dev-dependency. In `crates/seisin-types/Cargo.toml`:

```toml
[package]
name = "seisin-types"
version = "0.1.0"
edition = "2021"

[dependencies]
seisin-core = { path = "../seisin-core" }
seisin-ops = { path = "../seisin-ops" }
anyhow = "1"
```

Add to `crates/seisin-types/src/sk_index.rs`, above the tests module:

```rust
use seisin_core::authority::AuthorityIdx;
use seisin_core::sk::{decode_sk_entries, encode_sk_entries};
use seisin_ops::context::OpContext;

/// A detected uniqueness violation: `sk_key` now holds more than one
/// distinct pk_id, and `conflict_op` names the op the caller declared
/// for resolving it. Nothing in this crate invokes `conflict_op` — see
/// the "Constraint Enforcement" section of the design doc and this
/// plan's Global Constraints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniquenessViolation {
  pub sk_key: DatumId,
  pub conflict_op: String,
}

/// Appends `(pk_id, AuthorityIdx::Native)` to `sk_key`'s entry list if
/// `pk_id` isn't already present (re-inserting the same pk_id is a
/// no-op, not a duplicate). If `unique_conflict_op` is `Some`, checks
/// whether the list already holds a *different* pk_id — if so, this is
/// the best-effort uniqueness violation, reported (not rejected here;
/// see Task 5 for where a caller actually decides to reject the write).
pub fn insert_sk_entry(
  ctx: &mut OpContext,
  sk_key: DatumId,
  pk_id: DatumId,
  unique_conflict_op: Option<String>,
) -> Result<Option<UniquenessViolation>> {
  let mut entries = match ctx.get(sk_key) {
    Some(bytes) => decode_sk_entries(&bytes)?,
    None => Vec::new(),
  };

  if let Some(conflict_op) = &unique_conflict_op {
    if let Some((existing_id, _)) = entries.iter().find(|(id, _)| *id != pk_id) {
      let _ = existing_id;
      ctx.put(sk_key, encode_sk_entries(&entries));
      return Ok(Some(UniquenessViolation {
        sk_key,
        conflict_op: conflict_op.clone(),
      }));
    }
  }

  if !entries.iter().any(|(id, _)| *id == pk_id) {
    entries.push((pk_id, AuthorityIdx::Native));
  }
  ctx.put(sk_key, encode_sk_entries(&entries));
  Ok(None)
}

/// Removes `pk_id`'s entry from `sk_key`'s list, if present. A missing
/// `sk_key` datum (never written, or already empty) is a no-op.
pub fn remove_sk_entry(ctx: &mut OpContext, sk_key: DatumId, pk_id: DatumId) {
  let Some(bytes) = ctx.get(sk_key) else {
    return;
  };
  let Ok(mut entries) = decode_sk_entries(&bytes) else {
    return;
  };
  entries.retain(|(id, _)| *id != pk_id);
  ctx.put(sk_key, encode_sk_entries(&entries));
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-types --lib sk_index::`
Expected: PASS (10 tests: 6 from Task 2 + 4 new).

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-types/Cargo.toml crates/seisin-types/src/sk_index.rs
git commit -m "feat: add sk entry insert/remove with uniqueness violation detection"
git push
```

---

### Task 5: `write_typed_datum` and `delete_typed_datum`

**Files:**
- Create: `crates/seisin-types/src/typed_write.rs`
- Modify: `crates/seisin-types/src/lib.rs`

**Interfaces:**
- Consumes: `DatumTypeDef`/`IndexDef`/`ConflictOp` (Task 3), `sk_key`/`insert_sk_entry`/`remove_sk_entry`/`UniquenessViolation` (Tasks 2 & 4), `encode_datum`/`decode_datum` (Part 1).
- Produces: `write_typed_datum(ctx: &mut OpContext, def: &DatumTypeDef, pk_id: DatumId, values: &[FieldValue]) -> anyhow::Result<WriteTypedResult>`, `delete_typed_datum(ctx: &mut OpContext, def: &DatumTypeDef, pk_id: DatumId)`, `WriteTypedResult { pub violation: Option<UniquenessViolation> }`, `encode_write_result(result: &WriteTypedResult) -> Vec<u8>`, `decode_write_result(bytes: &[u8]) -> anyhow::Result<WriteTypedResult>`.

Per the design doc's two-round-trip flow: this function assumes `ctx` already holds every datum it needs (the pk datum plus every relevant sk key, old and new) — declaring the right `datum_ids` up front is the *caller's* job (Task 7's client helper). This function itself does no network calls and doesn't read anything outside what `ctx.get` already gives it.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/seisin-types/src/typed_write.rs, at the bottom
#[cfg(test)]
mod tests {
  use super::*;
  use crate::field::FieldType;
  use crate::schema::ConflictOp;
  use seisin_core::cache::Cache;
  use seisin_core::store::InMemoryStore;
  use seisin_ops::context::OpContext;
  use std::sync::Arc;

  fn user_type() -> DatumTypeDef {
    DatumTypeDef::new("user")
      .field("name", FieldType::String)
      .field("age", FieldType::I64)
      .index(IndexDef::Sk {
        field: "name".to_string(),
        unique: None,
      })
  }

  fn unique_user_type() -> DatumTypeDef {
    DatumTypeDef::new("user").field("email", FieldType::String).index(IndexDef::Sk {
      field: "email".to_string(),
      unique: Some(ConflictOp("resolve_duplicate_email".to_string())),
    })
  }

  #[test]
  fn a_fresh_create_writes_the_datum_and_populates_its_sk_index() {
    let def = user_type();
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let pk_id = DatumId::new();
    let values = vec![FieldValue::String("cliff".to_string()), FieldValue::I64(41)];

    let key = sk_key(&def.name, "name", &values[0]).unwrap();
    ctx.get(key); // simulate the caller having declared/acquired it, per the framework's collation model

    let result = write_typed_datum(&mut ctx, &def, pk_id, &values).unwrap();
    assert!(result.violation.is_none());

    let decoded = decode_datum(&def, &ctx.get(pk_id).unwrap()).unwrap();
    assert_eq!(decoded, values);

    let entries = seisin_core::sk::decode_sk_entries(&ctx.get(key).unwrap()).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, pk_id);
  }

  #[test]
  fn updating_the_indexed_field_moves_the_entry_between_sk_keys() {
    let def = user_type();
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let pk_id = DatumId::new();

    let first_values = vec![FieldValue::String("cliff".to_string()), FieldValue::I64(41)];
    write_typed_datum(&mut ctx, &def, pk_id, &first_values).unwrap();

    let second_values = vec![FieldValue::String("clifford".to_string()), FieldValue::I64(41)];
    write_typed_datum(&mut ctx, &def, pk_id, &second_values).unwrap();

    let old_key = sk_key(&def.name, "name", &FieldValue::String("cliff".to_string())).unwrap();
    let new_key = sk_key(&def.name, "name", &FieldValue::String("clifford".to_string())).unwrap();
    let old_entries = seisin_core::sk::decode_sk_entries(&ctx.get(old_key).unwrap()).unwrap();
    let new_entries = seisin_core::sk::decode_sk_entries(&ctx.get(new_key).unwrap()).unwrap();
    assert_eq!(old_entries, vec![]);
    assert_eq!(new_entries.len(), 1);
    assert_eq!(new_entries[0].0, pk_id);
  }

  #[test]
  fn writing_the_same_indexed_value_again_does_not_disturb_its_sk_entry() {
    let def = user_type();
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let pk_id = DatumId::new();
    let values = vec![FieldValue::String("cliff".to_string()), FieldValue::I64(41)];

    write_typed_datum(&mut ctx, &def, pk_id, &values).unwrap();
    write_typed_datum(&mut ctx, &def, pk_id, &values).unwrap();

    let key = sk_key(&def.name, "name", &values[0]).unwrap();
    let entries = seisin_core::sk::decode_sk_entries(&ctx.get(key).unwrap()).unwrap();
    assert_eq!(entries.len(), 1);
  }

  #[test]
  fn a_uniqueness_violation_is_reported_but_the_write_still_completes() {
    let def = unique_user_type();
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let first_pk = DatumId::new();
    let second_pk = DatumId::new();
    let values = vec![FieldValue::String("a@example.com".to_string())];

    write_typed_datum(&mut ctx, &def, first_pk, &values).unwrap();
    let result = write_typed_datum(&mut ctx, &def, second_pk, &values).unwrap();

    let violation = result.violation.expect("a second writer to the same unique value must be flagged");
    assert_eq!(violation.conflict_op, "resolve_duplicate_email");
  }

  #[test]
  fn delete_removes_the_pk_content_and_its_sk_entry() {
    let def = user_type();
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let pk_id = DatumId::new();
    let values = vec![FieldValue::String("cliff".to_string()), FieldValue::I64(41)];
    write_typed_datum(&mut ctx, &def, pk_id, &values).unwrap();

    delete_typed_datum(&mut ctx, &def, pk_id);

    assert_eq!(ctx.get(pk_id), None);
    let key = sk_key(&def.name, "name", &values[0]).unwrap();
    let entries = seisin_core::sk::decode_sk_entries(&ctx.get(key).unwrap()).unwrap();
    assert_eq!(entries, vec![]);
  }

  #[test]
  fn write_result_round_trips_through_encoding() {
    let with_violation = WriteTypedResult {
      violation: Some(UniquenessViolation {
        sk_key: DatumId::new(),
        conflict_op: "resolve".to_string(),
      }),
    };
    let decoded = decode_write_result(&encode_write_result(&with_violation)).unwrap();
    assert_eq!(decoded, with_violation);

    let without = WriteTypedResult { violation: None };
    let decoded_without = decode_write_result(&encode_write_result(&without)).unwrap();
    assert_eq!(decoded_without, without);
  }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-types --lib typed_write::`
Expected: FAIL to compile — nothing in this file exists yet.

- [ ] **Step 3: Implement `write_typed_datum`, `delete_typed_datum`, `WriteTypedResult`, and its encoding**

```rust
// crates/seisin-types/src/typed_write.rs, above the tests module
//! Whole-op write/delete for a typed datum, including sk index
//! maintenance and best-effort uniqueness checking. See the design
//! doc's "sk Index" and "Constraint Enforcement" sections.

use anyhow::{bail, Result};
use seisin_core::datum::DatumId;
use seisin_ops::context::OpContext;

use crate::field::FieldValue;
use crate::schema::{DatumTypeDef, IndexDef};
use crate::sk_index::{insert_sk_entry, remove_sk_entry, sk_key, UniquenessViolation};
use crate::{decode_datum, encode_datum};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteTypedResult {
  pub violation: Option<UniquenessViolation>,
}

pub fn encode_write_result(result: &WriteTypedResult) -> Vec<u8> {
  match &result.violation {
    None => vec![0],
    Some(v) => {
      let mut buf = vec![1];
      buf.extend_from_slice(&v.sk_key.as_bytes());
      let op_bytes = v.conflict_op.as_bytes();
      buf.extend_from_slice(&(op_bytes.len() as u32).to_le_bytes());
      buf.extend_from_slice(op_bytes);
      buf
    }
  }
}

pub fn decode_write_result(bytes: &[u8]) -> Result<WriteTypedResult> {
  if bytes.is_empty() {
    bail!("write result bytes are empty");
  }
  match bytes[0] {
    0 => Ok(WriteTypedResult { violation: None }),
    1 => {
      if bytes.len() < 1 + 16 + 4 {
        bail!("write result claims a violation but is too short");
      }
      let sk_key_bytes: [u8; 16] = bytes[1..17].try_into().unwrap();
      let sk_key = DatumId::from_bytes(sk_key_bytes);
      let op_len = u32::from_le_bytes(bytes[17..21].try_into().unwrap()) as usize;
      if bytes.len() != 21 + op_len {
        bail!("write result's conflict_op length prefix doesn't match the remaining bytes");
      }
      let conflict_op = String::from_utf8(bytes[21..21 + op_len].to_vec())
        .map_err(|_| anyhow::anyhow!("conflict_op was not valid utf8"))?;
      Ok(WriteTypedResult {
        violation: Some(UniquenessViolation { sk_key, conflict_op }),
      })
    }
    tag => bail!("unknown write result tag: {tag}"),
  }
}

/// Writes `pk_id`'s content and maintains every declared sk index. The
/// caller must have already arranged (via normal collation, declaring
/// every relevant datum_id up front) for `ctx` to hold `pk_id` and every
/// sk key this write touches — see this plan's Task 7 for the
/// client-side helper that computes and declares them.
pub fn write_typed_datum(
  ctx: &mut OpContext,
  def: &DatumTypeDef,
  pk_id: DatumId,
  values: &[FieldValue],
) -> Result<WriteTypedResult> {
  let old_values = match ctx.get(pk_id) {
    Some(bytes) => Some(decode_datum(def, &bytes)?),
    None => None,
  };

  let mut violation = None;
  for index in &def.indexes {
    let IndexDef::Sk { field, unique } = index;
    let field_idx = def
      .fields
      .iter()
      .position(|(name, _)| name == field)
      .ok_or_else(|| anyhow::anyhow!("index declared on unknown field {field:?}"))?;
    let new_key = sk_key(&def.name, field, &values[field_idx])?;
    let old_key = old_values
      .as_ref()
      .map(|old| sk_key(&def.name, field, &old[field_idx]))
      .transpose()?;

    if old_key != Some(new_key) {
      if let Some(old_key) = old_key {
        remove_sk_entry(ctx, old_key, pk_id);
      }
      let conflict_op = unique.as_ref().map(|op| op.0.clone());
      if let Some(found) = insert_sk_entry(ctx, new_key, pk_id, conflict_op)? {
        violation = Some(found);
      }
    }
  }

  ctx.put(pk_id, encode_datum(def, values)?);
  Ok(WriteTypedResult { violation })
}

/// Deletes `pk_id`'s content and removes its entry from every declared
/// sk index. Same up-front-declaration requirement as `write_typed_datum`.
pub fn delete_typed_datum(ctx: &mut OpContext, def: &DatumTypeDef, pk_id: DatumId) {
  let Some(bytes) = ctx.get(pk_id) else {
    return;
  };
  let Ok(old_values) = decode_datum(def, &bytes) else {
    ctx.delete(pk_id);
    return;
  };
  for index in &def.indexes {
    let IndexDef::Sk { field, .. } = index;
    let Some(field_idx) = def.fields.iter().position(|(name, _)| name == field) else {
      continue;
    };
    if let Ok(key) = sk_key(&def.name, field, &old_values[field_idx]) {
      remove_sk_entry(ctx, key, pk_id);
    }
  }
  ctx.delete(pk_id);
}
```

Add `pub mod typed_write;` to `crates/seisin-types/src/lib.rs`. Also re-export `encode_datum`/`decode_datum` at the crate root if they aren't already — check `crates/seisin-types/src/lib.rs`'s current contents; if `schema::encode_datum`/`schema::decode_datum` aren't re-exported, add `pub use schema::{decode_datum, encode_datum};` so `typed_write.rs`'s `use crate::{decode_datum, encode_datum};` resolves (adjust the import path to `crate::schema::{decode_datum, encode_datum}` instead if re-exporting isn't wanted — either is fine, but pick one and make sure it compiles).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-types --lib typed_write::`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-types/src/typed_write.rs crates/seisin-types/src/lib.rs
git commit -m "feat: add write_typed_datum/delete_typed_datum with sk maintenance"
git push
```

---

### Task 6: Client-side typed-write helper (two-round-trip orchestration)

**Files:**
- Create: `crates/seisin-types/src/client.rs`
- Modify: `crates/seisin-types/Cargo.toml`
- Modify: `crates/seisin-types/src/lib.rs`

**Interfaces:**
- Consumes: `DatumTypeDef`/`IndexDef` (Task 3), `sk_key` (Task 2), `encode_datum`/`decode_datum` (Part 1), `decode_write_result` (Task 5), `seisin_client::call`, `seisin_protocol::{Request, Response}`.
- Produces: `write_typed_datum_client(addr: &str, read_op_name: &str, write_op_name: &str, def: &DatumTypeDef, pk_id: DatumId, values: &[FieldValue]) -> anyhow::Result<Option<(String, DatumId)>>` — returns `Some((conflict_op_name, sk_key))` if the write op reported a uniqueness violation and the caller should make its own follow-up call (this function does not make that follow-up call itself — see the step 3 rationale below for why).

- [ ] **Step 1: Add the `seisin-client`/`seisin-protocol` dependencies**

```toml
# crates/seisin-types/Cargo.toml
[package]
name = "seisin-types"
version = "0.1.0"
edition = "2021"

[dependencies]
seisin-core = { path = "../seisin-core" }
seisin-ops = { path = "../seisin-ops" }
seisin-client = { path = "../seisin-client" }
seisin-protocol = { path = "../seisin-protocol" }
anyhow = "1"
```

- [ ] **Step 2: Write the failing integration test**

```rust
// crates/seisin-types/tests/integration_typed_write_client.rs
use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::InMemoryStore;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ops::context::OpContext;
use seisin_ops::registry::OpRegistry;
use seisin_ring::ring::Ring;
use seisin_types::field::FieldType;
use seisin_types::field::FieldValue;
use seisin_types::schema::{ConflictOp, DatumTypeDef, IndexDef};
use seisin_types::typed_write::{decode_write_result, encode_write_result, write_typed_datum};
use seisin_types::{decode_datum, encode_datum};

fn user_type() -> DatumTypeDef {
  DatumTypeDef::new("user")
    .field("email", FieldType::String)
    .index(IndexDef::Sk {
      field: "email".to_string(),
      unique: Some(ConflictOp("resolve_duplicate_email".to_string())),
    })
}

fn start_node() -> String {
  let def = user_type();
  let mut ops = OpRegistry::new();

  ops.register(
    "read_user",
    Box::new(move |ctx: &mut OpContext, ids, _payload| ctx.get(ids[0]).unwrap_or_default()),
  );

  let write_def = def.clone();
  ops.register(
    "write_user",
    Box::new(move |ctx: &mut OpContext, ids, payload| {
      let values = decode_datum(&write_def, payload).unwrap();
      let result = write_typed_datum(ctx, &write_def, ids[0], &values).unwrap();
      encode_write_result(&result)
    }),
  );

  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  let node_id = NodeId(1);
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_id, 1)])));
  let peer_link_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    1,
    Arc::new(ops),
    Arc::clone(&ring),
    node_id,
    peer_link_listener,
    Arc::new(std::collections::HashMap::new()),
  ));
  let address_book = Arc::new(std::collections::HashMap::new());
  thread::spawn(move || serve(listener, node_id, ring, address_book, pool));
  thread::sleep(std::time::Duration::from_millis(100));
  addr
}

#[test]
fn a_second_write_of_the_same_unique_value_is_reported_for_follow_up() {
  let addr = start_node();
  let def = user_type();

  let first_pk = DatumId::new();
  let second_pk = DatumId::new();
  let values = vec![FieldValue::String("a@example.com".to_string())];

  let first = seisin_types::client::write_typed_datum_client(
    &addr,
    "read_user",
    "write_user",
    &def,
    first_pk,
    &values,
  )
  .unwrap();
  assert_eq!(first, None, "the first writer must not see a violation");

  let second = seisin_types::client::write_typed_datum_client(
    &addr,
    "read_user",
    "write_user",
    &def,
    second_pk,
    &values,
  )
  .unwrap();
  let (conflict_op, _sk_key) = second.expect("the second writer must see a violation");
  assert_eq!(conflict_op, "resolve_duplicate_email");
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p seisin-types --test integration_typed_write_client`
Expected: FAIL to compile — `seisin_types::client`/`write_typed_datum_client` don't exist yet.

- [ ] **Step 4: Implement `write_typed_datum_client`**

```rust
// crates/seisin-types/src/client.rs
//! Client-side orchestration for a typed write: the two-round-trip flow
//! (read the old value, then issue the actual write declaring every
//! datum it touches up front) the design doc's "sk Index" section
//! settles on, since collation requires an op to declare every datum_id
//! it needs before execution starts.

use anyhow::{bail, Result};
use seisin_core::datum::DatumId;
use seisin_protocol::{Request, Response};

use crate::decode_datum;
use crate::field::FieldValue;
use crate::schema::{DatumTypeDef, IndexDef};
use crate::sk_index::sk_key;
use crate::typed_write::decode_write_result;

/// Performs a typed write of `pk_id` against the server at `addr`.
/// `read_op_name` must be a registered op that returns `pk_id`'s raw
/// content (or empty bytes if it doesn't exist yet) given `datum_ids:
/// [pk_id]`; `write_op_name` must be one whose handler decodes its
/// payload via `decode_datum` and calls `write_typed_datum`, returning
/// `encode_write_result`'s bytes as its own response payload — see
/// `crates/seisin-types/tests/integration_typed_write_client.rs` for a
/// worked example of registering both.
///
/// Returns `Some((conflict_op_name, sk_key))` if the write reported a
/// uniqueness violation. This function does **not** make the follow-up
/// call itself — deciding whether/how to call the resolution op is left
/// to the caller (there's no framework mechanism for one op to invoke
/// another in-process; see this plan's Global Constraints), so a caller
/// that wants a fully automatic resolution issues its own
/// `seisin_client::call` to `conflict_op_name` afterward.
pub fn write_typed_datum_client(
  addr: &str,
  read_op_name: &str,
  write_op_name: &str,
  def: &DatumTypeDef,
  pk_id: DatumId,
  values: &[FieldValue],
) -> Result<Option<(String, DatumId)>> {
  let read_response = seisin_client::call(
    addr,
    Request::Op {
      op_id: DatumId::new(),
      op_name: read_op_name.to_string(),
      datum_ids: vec![pk_id],
      payload: vec![],
    },
  )?;
  let old_bytes = match read_response {
    Response::OpResult { payload } if payload.is_empty() => None,
    Response::OpResult { payload } => Some(payload),
    Response::OpError { message } => bail!("read op {read_op_name:?} failed: {message}"),
    other => bail!("unexpected response from read op {read_op_name:?}: {other:?}"),
  };
  let old_values = old_bytes.map(|bytes| decode_datum(def, &bytes)).transpose()?;

  let mut datum_ids = vec![pk_id];
  for index in &def.indexes {
    let IndexDef::Sk { field, .. } = index;
    let field_idx = def
      .fields
      .iter()
      .position(|(name, _)| name == field)
      .ok_or_else(|| anyhow::anyhow!("index declared on unknown field {field:?}"))?;
    let new_key = sk_key(&def.name, field, &values[field_idx])?;
    datum_ids.push(new_key);
    if let Some(old_values) = &old_values {
      let old_key = sk_key(&def.name, field, &old_values[field_idx])?;
      if old_key != new_key {
        datum_ids.push(old_key);
      }
    }
  }

  let payload = crate::encode_datum(def, values)?;
  let write_response = seisin_client::call(
    addr,
    Request::Op {
      op_id: DatumId::new(),
      op_name: write_op_name.to_string(),
      datum_ids,
      payload,
    },
  )?;
  match write_response {
    Response::OpResult { payload } => {
      let result = decode_write_result(&payload)?;
      Ok(result.violation.map(|v| (v.conflict_op, v.sk_key)))
    }
    Response::OpError { message } => bail!("write op {write_op_name:?} failed: {message}"),
    other => bail!("unexpected response from write op {write_op_name:?}: {other:?}"),
  }
}
```

Add `pub mod client;` to `crates/seisin-types/src/lib.rs`.

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p seisin-types --test integration_typed_write_client`
Expected: PASS (1 test).

- [ ] **Step 6: Run it repeatedly to check for flakiness**

```bash
for i in $(seq 1 10); do
  cargo test -p seisin-types --test integration_typed_write_client 2>&1 | tail -3
done
```
Expected: PASS every time — this test starts a real node and makes real TCP calls, matching the stress-testing discipline used throughout this project for anything touching real concurrency/networking.

- [ ] **Step 7: Commit and push**

```bash
git add crates/seisin-types/Cargo.toml crates/seisin-types/src/client.rs crates/seisin-types/src/lib.rs crates/seisin-types/tests/integration_typed_write_client.rs
git commit -m "feat: add client-side typed-write helper with uniqueness follow-up signaling"
git push
```

---

### Task 7: Quality gate and progress tracker update

**Files:** `docs/superpowers/PROGRESS.md` (modify).

- [ ] **Step 1: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS — including every earlier sub-project's tests.

- [ ] **Step 2: Run fmt and clippy**

Run: `cargo fmt --check`
Expected: no output; if it reports diffs, run `cargo fmt` and re-check.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings/errors.

- [ ] **Step 3: Update `docs/superpowers/PROGRESS.md`**

Add an entry under "Done" for this plan (Datum Type System, Part 2), summarizing `DatumId::from_name`, `sk_key`, `insert_sk_entry`/`remove_sk_entry`/`UniquenessViolation`, `write_typed_datum`/`delete_typed_datum`/`WriteTypedResult`, and `write_typed_datum_client`. Note the explicit scope decision that automatic `ConflictOp` invocation is a client-driven follow-up call, not in-process dispatch. Note that Part 3 (rk index) is next.

- [ ] **Step 4: Commit and push**

```bash
git add -A
git commit -m "chore: fmt/clippy fixes for datum type system part 2; update progress tracker"
git push
```

---

## Self-Review Notes

- **Spec coverage**: "sk Index" ✓ (Tasks 2, 4, 5 — key derivation, entry maintenance, two-round-trip write flow via Task 6's client helper); the sk half of "Constraint Enforcement" (uniqueness) ✓ (Tasks 3-6 — `unique: Option<ConflictOp>`, best-effort detection in `insert_sk_entry`, violation surfaced through `WriteTypedResult` to the client for a follow-up call). Explicitly scoped out: automatic in-process `ConflictOp` dispatch (see Global Constraints) — this is a deliberate simplification, not a gap, decided before writing this plan.
- **Placeholder scan**: an earlier draft of Task 2's `DERIVED_ID_NAMESPACE` used a `const` binding that would have failed to compile (`DatumId::from_bytes` isn't `const fn`) — fixed to a plain function before finalizing. An earlier draft of Task 4's tests included an unused `new_ctx` helper left over from drafting — removed before finalizing. No other TBD/TODO placeholders remain.
- **Type consistency**: `DatumId::from_name` (Task 1) is consumed by `sk_key` (Task 2) unchanged. `IndexDef::Sk`/`ConflictOp` (Task 3) match exactly what `write_typed_datum`/`write_typed_datum_client` (Tasks 5-6) destructure. `UniquenessViolation`/`WriteTypedResult` (Tasks 4-5) match exactly what `decode_write_result`/`write_typed_datum_client` (Tasks 5-6) consume.
