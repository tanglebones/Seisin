//! A solution's declared datum type: its name and ordered fields. See
//! the design doc's "Schema Declaration & Field Encoding" section.

use anyhow::{bail, Result};

use crate::encoding::{decode_field_value, encode_field_value};
use crate::field::{value_matches_type, FieldType, FieldValue};

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
    let values = vec![FieldValue::String("cliff".to_string()), FieldValue::I64(41)];
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
      IndexDef::Sk {
        unique: Some(op), ..
      } => assert_eq!(op.0, "resolve_duplicate_email"),
      other => panic!("expected a unique Sk index, got {other:?}"),
    }
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
    let values = vec![FieldValue::String("cliff".to_string()), FieldValue::I64(41)];
    let mut encoded = encode_datum(&def, &values).unwrap();
    encoded.push(0xFF); // trailing byte no field consumes
    assert!(decode_datum(&def, &encoded).is_err());
  }
}
