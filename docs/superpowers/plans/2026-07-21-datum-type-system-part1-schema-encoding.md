# Datum Type System, Part 1: Schema Declaration & Field Encoding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give a solution a way to declare a typed, homogeneous datum type (fields + their types) and encode/decode values of that type to/from bytes — the foundation every later part (sk, rk, tk, constraint enforcement) builds on.

**Architecture:** A new crate, `seisin-types`, following this project's existing many-small-crates pattern (see `seisin-ops`). `FieldType`/`PrimitiveFieldType` describe a type's shape; `FieldValue` is a concrete value matching some `FieldType`; `DatumTypeDef` names a type and its ordered fields; `encode_datum`/`decode_datum` convert a `DatumTypeDef` + `Vec<FieldValue>` to/from bytes. No per-value type tags are needed on the wire — the declared schema tells the decoder what to expect at each position, recursively (including inside `Array`/`Dict`), matching how `seisin-protocol` avoids redundant tagging wherever the shape is already known.

**Tech Stack:** Rust, hand-rolled binary encoding (no serde), matching `seisin-protocol`/`seisin-core::sk`'s existing style. Depends on `seisin-core` (`DatumId`) and `anyhow` (error handling), same as `seisin-protocol`.

## Global Constraints

- 2-space indentation.
- Commit and push after every task.
- No serde or other external encoding dependency — hand-rolled length-prefixed binary encoding, matching this project's existing style.
- This plan covers only Part 1 of 5 (see `docs/superpowers/specs/2026-07-21-datum-type-system-design.md`'s "Schema Declaration & Field Encoding" and "pk Index" sections). Parts 2 (sk + uniqueness), 3 (rk), 4 (tk), 5 (relational constraints) are separate, later plans.
- pk needs no code in this plan at all — it's the existing `DatumId`, already implemented.

---

### Task 1: `seisin-types` crate scaffolding, `FieldType`/`PrimitiveFieldType`/`FieldValue`

**Files:**
- Create: `crates/seisin-types/Cargo.toml`
- Create: `crates/seisin-types/src/lib.rs`
- Create: `crates/seisin-types/src/field.rs`
- Modify: `Cargo.toml` (workspace members)

**Interfaces:**
- Produces: `FieldType` (`Bool`/`I64`/`F64`/`String`/`Bytes`/`Array(Box<FieldType>)`/`Dict(PrimitiveFieldType, Box<FieldType>)`), `PrimitiveFieldType` (`Bool`/`I64`/`F64`/`String`/`Bytes`), `FieldValue` (mirrors `FieldType`'s shape, `Dict` variant holds `Vec<(FieldValue, FieldValue)>`), `value_matches_type(value: &FieldValue, ty: &FieldType) -> bool`.

- [ ] **Step 1: Create the crate and wire it into the workspace**

```toml
# crates/seisin-types/Cargo.toml
[package]
name = "seisin-types"
version = "0.1.0"
edition = "2021"

[dependencies]
seisin-core = { path = "../seisin-core" }
anyhow = "1"
```

Modify `/Users/cliff/play/seisin/Cargo.toml`'s `members` list to add `"crates/seisin-types"`:

```toml
[workspace]
resolver = "2"
members = ["crates/seisin-core", "crates/seisin-protocol", "crates/seisin-node", "crates/seisin-ring", "crates/seisin-client", "crates/seisin-gossip", "crates/seisin-ops", "crates/seisin-types"]
```

```rust
// crates/seisin-types/src/lib.rs
pub mod field;
```

- [ ] **Step 2: Write the failing tests**

```rust
// crates/seisin-types/src/field.rs, at the bottom
#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn a_bool_value_matches_a_bool_type() {
    assert!(value_matches_type(&FieldValue::Bool(true), &FieldType::Bool));
  }

  #[test]
  fn a_bool_value_does_not_match_an_i64_type() {
    assert!(!value_matches_type(&FieldValue::Bool(true), &FieldType::I64));
  }

  #[test]
  fn every_primitive_kind_matches_its_own_type() {
    assert!(value_matches_type(&FieldValue::I64(7), &FieldType::I64));
    assert!(value_matches_type(&FieldValue::F64(1.5), &FieldType::F64));
    assert!(value_matches_type(
      &FieldValue::String("x".to_string()),
      &FieldType::String
    ));
    assert!(value_matches_type(
      &FieldValue::Bytes(vec![1, 2]),
      &FieldType::Bytes
    ));
  }

  #[test]
  fn an_array_value_matches_when_every_element_matches_the_inner_type() {
    let ty = FieldType::Array(Box::new(FieldType::I64));
    let value = FieldValue::Array(vec![FieldValue::I64(1), FieldValue::I64(2)]);
    assert!(value_matches_type(&value, &ty));
  }

  #[test]
  fn an_array_value_does_not_match_if_one_element_has_the_wrong_type() {
    let ty = FieldType::Array(Box::new(FieldType::I64));
    let value = FieldValue::Array(vec![
      FieldValue::I64(1),
      FieldValue::String("oops".to_string()),
    ]);
    assert!(!value_matches_type(&value, &ty));
  }

  #[test]
  fn a_dict_value_matches_when_every_key_and_value_match() {
    let ty = FieldType::Dict(PrimitiveFieldType::String, Box::new(FieldType::I64));
    let value = FieldValue::Dict(vec![(
      FieldValue::String("a".to_string()),
      FieldValue::I64(1),
    )]);
    assert!(value_matches_type(&value, &ty));
  }

  #[test]
  fn a_dict_value_does_not_match_if_a_key_has_the_wrong_primitive_type() {
    let ty = FieldType::Dict(PrimitiveFieldType::String, Box::new(FieldType::I64));
    let value = FieldValue::Dict(vec![(FieldValue::I64(9), FieldValue::I64(1))]);
    assert!(!value_matches_type(&value, &ty));
  }

  #[test]
  fn a_nested_array_of_dicts_matches_recursively() {
    let ty = FieldType::Array(Box::new(FieldType::Dict(
      PrimitiveFieldType::String,
      Box::new(FieldType::Bool),
    )));
    let value = FieldValue::Array(vec![FieldValue::Dict(vec![(
      FieldValue::String("k".to_string()),
      FieldValue::Bool(false),
    )])]);
    assert!(value_matches_type(&value, &ty));
  }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p seisin-types --lib`
Expected: FAIL to compile — `FieldType`, `PrimitiveFieldType`, `FieldValue`, `value_matches_type` don't exist yet.

- [ ] **Step 4: Implement `FieldType`, `PrimitiveFieldType`, `FieldValue`, `value_matches_type`**

```rust
// crates/seisin-types/src/field.rs, above the tests module
//! A datum type's field shapes (`FieldType`) and concrete values
//! (`FieldValue`) matching those shapes. See the design doc's "Schema
//! Declaration & Field Encoding" section.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
  Bool,
  I64,
  F64,
  String,
  Bytes,
  Array(Box<FieldType>),
  /// Keys are restricted to `PrimitiveFieldType` — a Dict key must be
  /// directly comparable/hashable, unlike an arbitrary `FieldType`.
  Dict(PrimitiveFieldType, Box<FieldType>),
}

/// The subset of `FieldType` allowed as a `Dict` key — everything except
/// `Array`/`Dict` themselves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrimitiveFieldType {
  Bool,
  I64,
  F64,
  String,
  Bytes,
}

impl From<PrimitiveFieldType> for FieldType {
  fn from(primitive: PrimitiveFieldType) -> Self {
    match primitive {
      PrimitiveFieldType::Bool => FieldType::Bool,
      PrimitiveFieldType::I64 => FieldType::I64,
      PrimitiveFieldType::F64 => FieldType::F64,
      PrimitiveFieldType::String => FieldType::String,
      PrimitiveFieldType::Bytes => FieldType::Bytes,
    }
  }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
  Bool(bool),
  I64(i64),
  F64(f64),
  String(String),
  Bytes(Vec<u8>),
  Array(Vec<FieldValue>),
  Dict(Vec<(FieldValue, FieldValue)>),
}

/// Recursively checks that `value`'s runtime shape matches `ty`,
/// including a `Dict`'s keys matching its declared `PrimitiveFieldType`.
pub fn value_matches_type(value: &FieldValue, ty: &FieldType) -> bool {
  match (value, ty) {
    (FieldValue::Bool(_), FieldType::Bool) => true,
    (FieldValue::I64(_), FieldType::I64) => true,
    (FieldValue::F64(_), FieldType::F64) => true,
    (FieldValue::String(_), FieldType::String) => true,
    (FieldValue::Bytes(_), FieldType::Bytes) => true,
    (FieldValue::Array(items), FieldType::Array(inner)) => {
      items.iter().all(|item| value_matches_type(item, inner))
    }
    (FieldValue::Dict(entries), FieldType::Dict(key_ty, val_ty)) => {
      let key_ty: FieldType = (*key_ty).into();
      entries.iter().all(|(k, v)| {
        value_matches_type(k, &key_ty) && value_matches_type(v, val_ty)
      })
    }
    _ => false,
  }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p seisin-types --lib`
Expected: PASS (8 tests).

- [ ] **Step 6: Commit and push**

```bash
git add Cargo.toml crates/seisin-types/Cargo.toml crates/seisin-types/src/lib.rs crates/seisin-types/src/field.rs
git commit -m "feat: add seisin-types crate with FieldType/FieldValue and shape matching"
git push
```

---

### Task 2: Field value encoding — `encode_field_value`/`decode_field_value`

**Files:**
- Create: `crates/seisin-types/src/encoding.rs`
- Modify: `crates/seisin-types/src/lib.rs`

**Interfaces:**
- Consumes: `FieldType`, `PrimitiveFieldType`, `FieldValue`, `value_matches_type` from Task 1.
- Produces: `encode_field_value(value: &FieldValue, buf: &mut Vec<u8>)`, `decode_field_value(ty: &FieldType, buf: &[u8], offset: &mut usize) -> anyhow::Result<FieldValue>`.

No per-value type tags are written — the caller already knows the expected `FieldType` at every position (recursively, including inside `Array`/`Dict`), so encoding only needs length prefixes for variable-length data (`String`/`Bytes`/`Array`/`Dict`), not tags identifying which variant follows.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/seisin-types/src/encoding.rs, at the bottom
#[cfg(test)]
mod tests {
  use super::*;
  use crate::field::{FieldType, FieldValue, PrimitiveFieldType};

  fn round_trip(ty: &FieldType, value: &FieldValue) {
    let mut buf = Vec::new();
    encode_field_value(value, &mut buf);
    let mut offset = 0;
    let decoded = decode_field_value(ty, &buf, &mut offset).unwrap();
    assert_eq!(&decoded, value);
    assert_eq!(offset, buf.len(), "decode must consume exactly the encoded bytes");
  }

  #[test]
  fn round_trips_bool() {
    round_trip(&FieldType::Bool, &FieldValue::Bool(true));
    round_trip(&FieldType::Bool, &FieldValue::Bool(false));
  }

  #[test]
  fn round_trips_i64_including_negative() {
    round_trip(&FieldType::I64, &FieldValue::I64(-42));
    round_trip(&FieldType::I64, &FieldValue::I64(i64::MAX));
  }

  #[test]
  fn round_trips_f64() {
    round_trip(&FieldType::F64, &FieldValue::F64(3.5));
  }

  #[test]
  fn round_trips_string() {
    round_trip(&FieldType::String, &FieldValue::String("hello".to_string()));
    round_trip(&FieldType::String, &FieldValue::String(String::new()));
  }

  #[test]
  fn round_trips_bytes() {
    round_trip(&FieldType::Bytes, &FieldValue::Bytes(vec![1, 2, 3]));
  }

  #[test]
  fn round_trips_an_array_of_i64() {
    let ty = FieldType::Array(Box::new(FieldType::I64));
    let value = FieldValue::Array(vec![FieldValue::I64(1), FieldValue::I64(2), FieldValue::I64(3)]);
    round_trip(&ty, &value);
  }

  #[test]
  fn round_trips_an_empty_array() {
    let ty = FieldType::Array(Box::new(FieldType::String));
    round_trip(&ty, &FieldValue::Array(vec![]));
  }

  #[test]
  fn round_trips_a_dict_of_string_to_i64() {
    let ty = FieldType::Dict(PrimitiveFieldType::String, Box::new(FieldType::I64));
    let value = FieldValue::Dict(vec![
      (FieldValue::String("a".to_string()), FieldValue::I64(1)),
      (FieldValue::String("b".to_string()), FieldValue::I64(2)),
    ]);
    round_trip(&ty, &value);
  }

  #[test]
  fn round_trips_a_nested_array_of_dicts() {
    let ty = FieldType::Array(Box::new(FieldType::Dict(
      PrimitiveFieldType::String,
      Box::new(FieldType::Bool),
    )));
    let value = FieldValue::Array(vec![
      FieldValue::Dict(vec![(FieldValue::String("k".to_string()), FieldValue::Bool(true))]),
      FieldValue::Dict(vec![]),
    ]);
    round_trip(&ty, &value);
  }

  #[test]
  fn decode_rejects_a_truncated_string_length_prefix() {
    let buf = vec![0u8, 1]; // claims a 2-byte len prefix worth of u32 but only has 2 bytes total
    let mut offset = 0;
    assert!(decode_field_value(&FieldType::String, &buf, &mut offset).is_err());
  }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-types --lib encoding::`
Expected: FAIL to compile — `encode_field_value`/`decode_field_value` don't exist yet.

- [ ] **Step 3: Implement `encode_field_value`/`decode_field_value`**

```rust
// crates/seisin-types/src/encoding.rs, above the tests module
//! Hand-rolled binary encoding for `FieldValue`s, driven by the declared
//! `FieldType` at each position — no per-value type tags are written,
//! since the schema already tells the decoder what to expect. Matches
//! this project's existing style (`seisin-protocol`, `seisin-core::sk`),
//! not a serde-based encoding.

use anyhow::{bail, Context, Result};

use crate::field::{FieldType, FieldValue, PrimitiveFieldType};

pub fn encode_field_value(value: &FieldValue, buf: &mut Vec<u8>) {
  match value {
    FieldValue::Bool(b) => buf.push(u8::from(*b)),
    FieldValue::I64(i) => buf.extend_from_slice(&i.to_le_bytes()),
    FieldValue::F64(f) => buf.extend_from_slice(&f.to_le_bytes()),
    FieldValue::String(s) => encode_len_prefixed(s.as_bytes(), buf),
    FieldValue::Bytes(b) => encode_len_prefixed(b, buf),
    FieldValue::Array(items) => {
      buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
      for item in items {
        encode_field_value(item, buf);
      }
    }
    FieldValue::Dict(entries) => {
      buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
      for (k, v) in entries {
        encode_field_value(k, buf);
        encode_field_value(v, buf);
      }
    }
  }
}

fn encode_len_prefixed(bytes: &[u8], buf: &mut Vec<u8>) {
  buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
  buf.extend_from_slice(bytes);
}

pub fn decode_field_value(ty: &FieldType, buf: &[u8], offset: &mut usize) -> Result<FieldValue> {
  match ty {
    FieldType::Bool => {
      let b = read_bytes(buf, offset, 1)?[0];
      Ok(FieldValue::Bool(b != 0))
    }
    FieldType::I64 => {
      let bytes: [u8; 8] = read_bytes(buf, offset, 8)?.try_into().unwrap();
      Ok(FieldValue::I64(i64::from_le_bytes(bytes)))
    }
    FieldType::F64 => {
      let bytes: [u8; 8] = read_bytes(buf, offset, 8)?.try_into().unwrap();
      Ok(FieldValue::F64(f64::from_le_bytes(bytes)))
    }
    FieldType::String => {
      let bytes = decode_len_prefixed(buf, offset)?;
      Ok(FieldValue::String(
        String::from_utf8(bytes).context("string field was not valid utf8")?,
      ))
    }
    FieldType::Bytes => Ok(FieldValue::Bytes(decode_len_prefixed(buf, offset)?)),
    FieldType::Array(inner) => {
      let count = read_u32(buf, offset)?;
      let mut items = Vec::with_capacity(count as usize);
      for _ in 0..count {
        items.push(decode_field_value(inner, buf, offset)?);
      }
      Ok(FieldValue::Array(items))
    }
    FieldType::Dict(key_ty, val_ty) => {
      let key_ty: FieldType = (*key_ty).into();
      let count = read_u32(buf, offset)?;
      let mut entries = Vec::with_capacity(count as usize);
      for _ in 0..count {
        let key = decode_field_value(&key_ty, buf, offset)?;
        let value = decode_field_value(val_ty, buf, offset)?;
        entries.push((key, value));
      }
      Ok(FieldValue::Dict(entries))
    }
  }
}

fn read_bytes<'a>(buf: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8]> {
  if *offset + len > buf.len() {
    bail!(
      "buffer truncated: need {len} bytes at offset {offset}, only {} remain",
      buf.len() - *offset
    );
  }
  let slice = &buf[*offset..*offset + len];
  *offset += len;
  Ok(slice)
}

fn read_u32(buf: &[u8], offset: &mut usize) -> Result<u32> {
  let bytes: [u8; 4] = read_bytes(buf, offset, 4)?.try_into().unwrap();
  Ok(u32::from_le_bytes(bytes))
}

fn decode_len_prefixed(buf: &[u8], offset: &mut usize) -> Result<Vec<u8>> {
  let len = read_u32(buf, offset)? as usize;
  Ok(read_bytes(buf, offset, len)?.to_vec())
}
```

Add `pub mod encoding;` to `crates/seisin-types/src/lib.rs`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-types --lib`
Expected: PASS (18 tests: 8 from Task 1 + 10 new).

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-types/src/encoding.rs crates/seisin-types/src/lib.rs
git commit -m "feat: add schema-driven FieldValue binary encoding"
git push
```

---

### Task 3: `DatumTypeDef` and whole-datum `encode_datum`/`decode_datum`

**Files:**
- Create: `crates/seisin-types/src/schema.rs`
- Modify: `crates/seisin-types/src/lib.rs`

**Interfaces:**
- Consumes: `FieldType`, `FieldValue`, `value_matches_type` (Task 1), `encode_field_value`/`decode_field_value` (Task 2).
- Produces: `DatumTypeDef { name: String, fields: Vec<(String, FieldType)> }`, `DatumTypeDef::new(name: impl Into<String>) -> Self`, `DatumTypeDef::field(self, name: impl Into<String>, ty: FieldType) -> Self` (builder), `encode_datum(def: &DatumTypeDef, values: &[FieldValue]) -> anyhow::Result<Vec<u8>>`, `decode_datum(def: &DatumTypeDef, bytes: &[u8]) -> anyhow::Result<Vec<FieldValue>>`.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/seisin-types/src/schema.rs, at the bottom
#[cfg(test)]
mod tests {
  use super::*;
  use crate::field::FieldValue;

  fn user_type() -> DatumTypeDef {
    DatumTypeDef::new("user")
      .field("name", FieldType::String)
      .field("age", FieldType::I64)
  }

  #[test]
  fn builder_accumulates_fields_in_declared_order() {
    let def = user_type();
    assert_eq!(def.name, "user");
    assert_eq!(
      def.fields,
      vec![
        ("name".to_string(), FieldType::String),
        ("age".to_string(), FieldType::I64),
      ]
    );
  }

  #[test]
  fn round_trips_a_simple_datum() {
    let def = user_type();
    let values = vec![
      FieldValue::String("cliff".to_string()),
      FieldValue::I64(41),
    ];
    let encoded = encode_datum(&def, &values).unwrap();
    let decoded = decode_datum(&def, &encoded).unwrap();
    assert_eq!(decoded, values);
  }

  #[test]
  fn encode_rejects_the_wrong_number_of_values() {
    let def = user_type();
    let values = vec![FieldValue::String("cliff".to_string())]; // missing "age"
    assert!(encode_datum(&def, &values).is_err());
  }

  #[test]
  fn encode_rejects_a_value_that_does_not_match_its_fields_declared_type() {
    let def = user_type();
    let values = vec![
      FieldValue::String("cliff".to_string()),
      FieldValue::String("not a number".to_string()), // "age" is declared I64
    ];
    assert!(encode_datum(&def, &values).is_err());
  }

  #[test]
  fn decode_rejects_bytes_with_a_trailing_garbage() {
    let def = user_type();
    let values = vec![
      FieldValue::String("cliff".to_string()),
      FieldValue::I64(41),
    ];
    let mut encoded = encode_datum(&def, &values).unwrap();
    encoded.push(0xFF); // trailing byte no field consumes
    assert!(decode_datum(&def, &encoded).is_err());
  }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p seisin-types --lib schema::`
Expected: FAIL to compile — `DatumTypeDef`/`encode_datum`/`decode_datum` don't exist yet.

- [ ] **Step 3: Implement `DatumTypeDef`, `encode_datum`, `decode_datum`**

```rust
// crates/seisin-types/src/schema.rs, above the tests module
//! A solution's declared datum type: its name and ordered fields. See
//! the design doc's "Schema Declaration & Field Encoding" section.

use anyhow::{bail, Result};

use crate::encoding::{decode_field_value, encode_field_value};
use crate::field::{value_matches_type, FieldType, FieldValue};

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

/// Encodes `values` (one per field, in `def.fields`' declared order) into
/// a single byte buffer. Fails if the count doesn't match the schema or
/// any value doesn't match its field's declared type.
pub fn encode_datum(def: &DatumTypeDef, values: &[FieldValue]) -> Result<Vec<u8>> {
  if values.len() != def.fields.len() {
    bail!(
      "datum type {:?} has {} fields but {} values were given",
      def.name,
      def.fields.len(),
      values.len()
    );
  }
  let mut buf = Vec::new();
  for ((field_name, field_ty), value) in def.fields.iter().zip(values) {
    if !value_matches_type(value, field_ty) {
      bail!(
        "value for field {:?} on datum type {:?} does not match its declared type {:?}",
        field_name,
        def.name,
        field_ty
      );
    }
    encode_field_value(value, &mut buf);
  }
  Ok(buf)
}

/// Decodes `bytes` into one `FieldValue` per field, in `def.fields`'
/// declared order. Fails if the bytes don't cleanly decode into exactly
/// that many fields with nothing left over.
pub fn decode_datum(def: &DatumTypeDef, bytes: &[u8]) -> Result<Vec<FieldValue>> {
  let mut offset = 0;
  let mut values = Vec::with_capacity(def.fields.len());
  for (_, field_ty) in &def.fields {
    values.push(decode_field_value(field_ty, bytes, &mut offset)?);
  }
  if offset != bytes.len() {
    bail!(
      "datum type {:?} decoded {} of {} bytes; {} trailing bytes unaccounted for",
      def.name,
      offset,
      bytes.len(),
      bytes.len() - offset
    );
  }
  Ok(values)
}
```

Add `pub mod schema;` to `crates/seisin-types/src/lib.rs`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p seisin-types --lib`
Expected: PASS (23 tests: 18 from Tasks 1-2 + 5 new).

- [ ] **Step 5: Commit and push**

```bash
git add crates/seisin-types/src/schema.rs crates/seisin-types/src/lib.rs
git commit -m "feat: add DatumTypeDef and whole-datum encode/decode"
git push
```

---

### Task 4: Quality gate and progress tracker update

**Files:** `docs/superpowers/PROGRESS.md` (modify).

- [ ] **Step 1: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS — including every earlier sub-project's tests, confirming the new `seisin-types` crate hasn't disturbed anything (it has no dependents yet, so this is mostly a sanity check that the workspace still builds).

- [ ] **Step 2: Run fmt and clippy**

Run: `cargo fmt --check`
Expected: no output; if it reports diffs, run `cargo fmt` and re-check.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings/errors.

- [ ] **Step 3: Update `docs/superpowers/PROGRESS.md`**

Add an entry under "Done" for this plan (Datum Type System, Part 1), summarizing `seisin-types`'s `FieldType`/`FieldValue`/`value_matches_type`, `encode_field_value`/`decode_field_value`, `DatumTypeDef`/`encode_datum`/`decode_datum`. Note that Part 2 (sk index + uniqueness constraint) is next, per `docs/superpowers/specs/2026-07-21-datum-type-system-design.md`.

- [ ] **Step 4: Commit and push**

```bash
git add -A
git commit -m "chore: fmt/clippy fixes for datum type system part 1; update progress tracker"
git push
```

---

## Self-Review Notes

- **Spec coverage**: "Schema Declaration & Field Encoding" ✓ (Tasks 1-3: `FieldType`/`PrimitiveFieldType`/`FieldValue`, schema-driven encoding with no redundant tags, `DatumTypeDef` + whole-datum encode/decode); "pk Index" ✓ (no code needed — already `DatumId`, noted in Global Constraints rather than given a no-op task). Everything else in the design doc (sk/rk/tk/constraints) is explicitly out of scope for this plan — see Parts 2-5.
- **Placeholder scan**: no TBD/TODO; every step has complete code. No other placeholders found.
- **Type consistency**: `FieldType`/`PrimitiveFieldType`/`FieldValue` (Task 1) are consumed unchanged by `encode_field_value`/`decode_field_value` (Task 2) and `DatumTypeDef`/`encode_datum`/`decode_datum` (Task 3) — names and shapes match exactly across all three tasks.
