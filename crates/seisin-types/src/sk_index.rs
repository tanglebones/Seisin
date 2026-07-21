//! sk (secondary-key) index maintenance: deriving a stable datum_id for
//! a `(type, field, value)` key, and keeping its entry list in sync with
//! writes. See the design doc's "sk Index" section.

use anyhow::{bail, Result};
use seisin_core::authority::AuthorityIdx;
use seisin_core::datum::DatumId;
use seisin_core::sk::{decode_sk_entries, encode_sk_entries};
use seisin_ops::context::OpContext;

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
/// see `write_typed_datum` for where a caller actually decides to reject
/// the write).
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
    if entries.iter().any(|(id, _)| *id != pk_id) {
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
}
