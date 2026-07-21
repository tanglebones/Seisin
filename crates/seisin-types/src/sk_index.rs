//! sk (secondary-key) index maintenance: deriving a stable datum_id for
//! a `(type, field, value)` key, and the `"sk"` `IndexHandler` that
//! keeps that key's entry list in sync with writes. See the design
//! doc's "sk Index" section and "Automatic Index Maintenance & Op
//! Lifecycle".

use anyhow::{bail, Context, Result};
use seisin_core::authority::AuthorityIdx;
use seisin_core::datum::DatumId;
use seisin_core::sk::{decode_sk_entries, encode_sk_entries};
use seisin_node::index_handler::IndexHandlerRegistry;

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

/// One update to apply to an sk index's entry list — the payload
/// `IndexHandlerRegistry` dispatches for the `"sk"` kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkIndexOp {
  Insert {
    pk_id: DatumId,
    unique_conflict_op: Option<String>,
  },
  Remove {
    pk_id: DatumId,
  },
}

const SK_OP_INSERT: u8 = 0;
const SK_OP_REMOVE: u8 = 1;

pub fn encode_sk_index_op(op: &SkIndexOp) -> Vec<u8> {
  let mut buf = Vec::new();
  match op {
    SkIndexOp::Insert {
      pk_id,
      unique_conflict_op,
    } => {
      buf.push(SK_OP_INSERT);
      buf.extend_from_slice(&pk_id.as_bytes());
      match unique_conflict_op {
        None => buf.push(0),
        Some(name) => {
          buf.push(1);
          let name_bytes = name.as_bytes();
          buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
          buf.extend_from_slice(name_bytes);
        }
      }
    }
    SkIndexOp::Remove { pk_id } => {
      buf.push(SK_OP_REMOVE);
      buf.extend_from_slice(&pk_id.as_bytes());
    }
  }
  buf
}

pub fn decode_sk_index_op(buf: &[u8]) -> Result<SkIndexOp> {
  if buf.is_empty() {
    bail!("sk index op payload is empty");
  }
  if buf.len() < 1 + 16 {
    bail!("sk index op payload too short for a pk_id");
  }
  let pk_id = DatumId::from_bytes(buf[1..17].try_into().unwrap());
  match buf[0] {
    SK_OP_REMOVE => Ok(SkIndexOp::Remove { pk_id }),
    SK_OP_INSERT => {
      if buf.len() < 18 {
        bail!("sk insert op payload too short for its conflict_op flag");
      }
      let unique_conflict_op = match buf[17] {
        0 => None,
        1 => {
          if buf.len() < 22 {
            bail!("sk insert op payload too short for its conflict_op length");
          }
          let name_len = u32::from_le_bytes(buf[18..22].try_into().unwrap()) as usize;
          if buf.len() != 22 + name_len {
            bail!("sk insert op payload length mismatch for its conflict_op name");
          }
          Some(
            String::from_utf8(buf[22..22 + name_len].to_vec())
              .context("sk insert op's conflict_op name was not valid utf8")?,
          )
        }
        flag => bail!("unknown sk insert op conflict_op flag: {flag}"),
      };
      Ok(SkIndexOp::Insert {
        pk_id,
        unique_conflict_op,
      })
    }
    tag => bail!("unknown sk index op tag: {tag}"),
  }
}

/// The `IndexHandler` for the `"sk"` kind — applies one `SkIndexOp`
/// against `current`'s decoded entry list (empty if `None`, a cold sk
/// key with nothing resident/stored yet).
pub fn apply_sk_index_update(current: Option<&[u8]>, payload: &[u8]) -> (Vec<u8>, Option<String>) {
  let mut entries = match current {
    Some(bytes) => decode_sk_entries(bytes).unwrap_or_default(),
    None => Vec::new(),
  };
  let op = match decode_sk_index_op(payload) {
    Ok(op) => op,
    Err(e) => {
      return (
        encode_sk_entries(&entries),
        Some(format!("malformed sk index payload: {e}")),
      )
    }
  };
  match op {
    SkIndexOp::Remove { pk_id } => {
      entries.retain(|(id, _)| *id != pk_id);
      (encode_sk_entries(&entries), None)
    }
    SkIndexOp::Insert {
      pk_id,
      unique_conflict_op,
    } => {
      if let Some(conflict_op) = &unique_conflict_op {
        if entries.iter().any(|(id, _)| *id != pk_id) {
          return (encode_sk_entries(&entries), Some(conflict_op.clone()));
        }
      }
      if !entries.iter().any(|(id, _)| *id == pk_id) {
        entries.push((pk_id, AuthorityIdx::Native));
      }
      (encode_sk_entries(&entries), None)
    }
  }
}

/// Registers the `"sk"` kind's `IndexHandler` — call once at startup,
/// alongside registering a solution's ops.
pub fn register_sk_index_handler(registry: &mut IndexHandlerRegistry) {
  registry.register("sk", Box::new(apply_sk_index_update));
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn same_type_field_and_value_always_derive_the_same_key() {
    let a = sk_key("user", "name", &FieldValue::String("cliff".to_string())).unwrap();
    let b = sk_key("user", "name", &FieldValue::String("cliff".to_string())).unwrap();
    assert_eq!(a, b);
  }

  #[test]
  fn a_different_value_derives_a_different_key() {
    let a = sk_key("user", "name", &FieldValue::String("cliff".to_string())).unwrap();
    let b = sk_key(
      "user",
      "name",
      &FieldValue::String("someone_else".to_string()),
    )
    .unwrap();
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

  #[test]
  fn encode_decode_round_trips_an_insert_op() {
    let op = SkIndexOp::Insert {
      pk_id: DatumId::new(),
      unique_conflict_op: Some("resolve".to_string()),
    };
    assert_eq!(decode_sk_index_op(&encode_sk_index_op(&op)).unwrap(), op);
  }

  #[test]
  fn encode_decode_round_trips_an_insert_op_without_a_conflict_op() {
    let op = SkIndexOp::Insert {
      pk_id: DatumId::new(),
      unique_conflict_op: None,
    };
    assert_eq!(decode_sk_index_op(&encode_sk_index_op(&op)).unwrap(), op);
  }

  #[test]
  fn encode_decode_round_trips_a_remove_op() {
    let op = SkIndexOp::Remove {
      pk_id: DatumId::new(),
    };
    assert_eq!(decode_sk_index_op(&encode_sk_index_op(&op)).unwrap(), op);
  }

  #[test]
  fn apply_insert_on_a_cold_target_creates_a_single_entry_list() {
    let pk_id = DatumId::new();
    let payload = encode_sk_index_op(&SkIndexOp::Insert {
      pk_id,
      unique_conflict_op: None,
    });
    let (new_bytes, violation) = apply_sk_index_update(None, &payload);
    assert!(violation.is_none());
    let entries = seisin_core::sk::decode_sk_entries(&new_bytes).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, pk_id);
  }

  #[test]
  fn apply_remove_on_an_existing_entry_removes_it() {
    let pk_id = DatumId::new();
    let insert_payload = encode_sk_index_op(&SkIndexOp::Insert {
      pk_id,
      unique_conflict_op: None,
    });
    let (after_insert, _) = apply_sk_index_update(None, &insert_payload);
    let remove_payload = encode_sk_index_op(&SkIndexOp::Remove { pk_id });
    let (after_remove, violation) = apply_sk_index_update(Some(&after_insert), &remove_payload);
    assert!(violation.is_none());
    assert_eq!(
      seisin_core::sk::decode_sk_entries(&after_remove).unwrap(),
      vec![]
    );
  }

  #[test]
  fn apply_insert_of_the_same_pk_id_twice_is_not_a_violation() {
    let pk_id = DatumId::new();
    let payload = encode_sk_index_op(&SkIndexOp::Insert {
      pk_id,
      unique_conflict_op: Some("resolve".to_string()),
    });
    let (first, _) = apply_sk_index_update(None, &payload);
    let (second, violation) = apply_sk_index_update(Some(&first), &payload);
    assert!(violation.is_none());
    assert_eq!(
      seisin_core::sk::decode_sk_entries(&second).unwrap().len(),
      1
    );
  }

  #[test]
  fn apply_insert_of_a_different_pk_id_when_unique_reports_a_violation() {
    let first_pk = DatumId::new();
    let second_pk = DatumId::new();
    let first_payload = encode_sk_index_op(&SkIndexOp::Insert {
      pk_id: first_pk,
      unique_conflict_op: Some("resolve".to_string()),
    });
    let (after_first, _) = apply_sk_index_update(None, &first_payload);
    let second_payload = encode_sk_index_op(&SkIndexOp::Insert {
      pk_id: second_pk,
      unique_conflict_op: Some("resolve".to_string()),
    });
    let (_, violation) = apply_sk_index_update(Some(&after_first), &second_payload);
    assert_eq!(violation, Some("resolve".to_string()));
  }

  #[test]
  fn apply_insert_of_a_different_pk_id_without_unique_is_not_a_violation() {
    let first_pk = DatumId::new();
    let second_pk = DatumId::new();
    let first_payload = encode_sk_index_op(&SkIndexOp::Insert {
      pk_id: first_pk,
      unique_conflict_op: None,
    });
    let (after_first, _) = apply_sk_index_update(None, &first_payload);
    let second_payload = encode_sk_index_op(&SkIndexOp::Insert {
      pk_id: second_pk,
      unique_conflict_op: None,
    });
    let (after_second, violation) = apply_sk_index_update(Some(&after_first), &second_payload);
    assert!(violation.is_none());
    assert_eq!(
      seisin_core::sk::decode_sk_entries(&after_second)
        .unwrap()
        .len(),
      2
    );
  }

  #[test]
  fn register_sk_index_handler_wires_the_kind_name_correctly() {
    let mut registry = IndexHandlerRegistry::new();
    register_sk_index_handler(&mut registry);
    let pk_id = DatumId::new();
    let payload = encode_sk_index_op(&SkIndexOp::Insert {
      pk_id,
      unique_conflict_op: None,
    });
    let (new_bytes, violation) = registry.apply("sk", None, &payload).unwrap();
    assert!(violation.is_none());
    assert_eq!(
      seisin_core::sk::decode_sk_entries(&new_bytes).unwrap()[0].0,
      pk_id
    );
  }
}
