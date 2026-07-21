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
      entries
        .iter()
        .all(|(k, v)| value_matches_type(k, &key_ty) && value_matches_type(v, val_ty))
    }
    _ => false,
  }
}

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
