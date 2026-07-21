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
      bail!(
        "sk index values must be primitive; got a non-primitive value for {type_name}.{field_name}"
      )
    }
  }
  Ok(DatumId::from_name(&derived_id_namespace(), &name))
}

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
