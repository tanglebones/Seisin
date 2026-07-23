//! sk (secondary-key) index maintenance: deriving a stable datum_id for
//! a `(type, field, value)` key, and the `"sk"` index kind that keeps
//! that key's entry list in sync with writes. See the design doc's "sk
//! Index" section and "Automatic Index Maintenance & Op Lifecycle".

use anyhow::{bail, Context, Result};
use seisin_core::authority::AuthorityIdx;
use seisin_core::datum::DatumId;
use seisin_core::sk::{decode_sk_entries, encode_sk_entries};
use seisin_node::index_handler::{IndexApplyOutcome, IndexKind, IndexKindRegistry, ResidentIndex};

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
/// `IndexKindRegistry` dispatches for the `"sk"` kind.
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

/// The `"sk"` kind's resident structure: one key's decoded entry list,
/// kept live on the owning thread — decoded once on cold miss, mutated
/// in place per update, re-encoded only for the write-through.
pub struct SkResidentIndex {
  entries: Vec<(DatumId, AuthorityIdx)>,
}

impl ResidentIndex for SkResidentIndex {
  fn apply(&mut self, payload: &[u8]) -> IndexApplyOutcome {
    let op = match decode_sk_index_op(payload) {
      Ok(op) => op,
      Err(e) => {
        return IndexApplyOutcome {
          violation: Some(format!("malformed sk index payload: {e}")),
          write_through: None,
        }
      }
    };
    match op {
      SkIndexOp::Remove { pk_id } => {
        self.entries.retain(|(id, _)| *id != pk_id);
      }
      SkIndexOp::Insert {
        pk_id,
        unique_conflict_op,
      } => {
        if let Some(conflict_op) = &unique_conflict_op {
          if self.entries.iter().any(|(id, _)| *id != pk_id) {
            return IndexApplyOutcome {
              violation: Some(conflict_op.clone()),
              write_through: None,
            };
          }
        }
        if !self.entries.iter().any(|(id, _)| *id == pk_id) {
          self.entries.push((pk_id, AuthorityIdx::Native));
        }
      }
    }
    IndexApplyOutcome {
      violation: None,
      write_through: Some(encode_sk_entries(&self.entries)),
    }
  }
}

/// The `"sk"` `IndexKind`: blob-persisted, so `open` decodes whatever
/// is currently stored. Stored bytes that fail to decode are an error,
/// not an empty index — silently starting fresh would drop every
/// existing entry for that key on the next write-through.
pub struct SkIndexKind;

impl IndexKind for SkIndexKind {
  fn open(
    &self,
    target: DatumId,
    stored: Option<Vec<u8>>,
  ) -> std::result::Result<Box<dyn ResidentIndex>, String> {
    let entries = match stored {
      Some(bytes) => decode_sk_entries(&bytes)
        .map_err(|e| format!("stored sk entries for {target:?} failed to decode: {e}"))?,
      None => Vec::new(),
    };
    Ok(Box::new(SkResidentIndex { entries }))
  }
}

/// Registers the `"sk"` index kind — call once at startup, alongside
/// registering a solution's ops.
pub fn register_sk_index_kind(registry: &mut IndexKindRegistry) {
  registry.register("sk", Box::new(SkIndexKind));
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

  /// Cold-opens the "sk" kind over `stored`, mirroring what the owning
  /// thread's worker does on a cold miss.
  fn open_sk(stored: Option<Vec<u8>>) -> Box<dyn ResidentIndex> {
    SkIndexKind.open(DatumId::new(), stored).unwrap()
  }

  #[test]
  fn apply_insert_on_a_cold_target_creates_a_single_entry_list() {
    let pk_id = DatumId::new();
    let payload = encode_sk_index_op(&SkIndexOp::Insert {
      pk_id,
      unique_conflict_op: None,
    });
    let mut resident = open_sk(None);
    let outcome = resident.apply(&payload);
    assert!(outcome.violation.is_none());
    let entries = seisin_core::sk::decode_sk_entries(&outcome.write_through.unwrap()).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, pk_id);
  }

  #[test]
  fn apply_remove_on_an_existing_entry_removes_it() {
    let pk_id = DatumId::new();
    let mut resident = open_sk(None);
    resident.apply(&encode_sk_index_op(&SkIndexOp::Insert {
      pk_id,
      unique_conflict_op: None,
    }));
    let outcome = resident.apply(&encode_sk_index_op(&SkIndexOp::Remove { pk_id }));
    assert!(outcome.violation.is_none());
    assert_eq!(
      seisin_core::sk::decode_sk_entries(&outcome.write_through.unwrap()).unwrap(),
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
    let mut resident = open_sk(None);
    resident.apply(&payload);
    let outcome = resident.apply(&payload);
    assert!(outcome.violation.is_none());
    assert_eq!(
      seisin_core::sk::decode_sk_entries(&outcome.write_through.unwrap())
        .unwrap()
        .len(),
      1
    );
  }

  #[test]
  fn apply_insert_of_a_different_pk_id_when_unique_reports_a_violation() {
    let mut resident = open_sk(None);
    resident.apply(&encode_sk_index_op(&SkIndexOp::Insert {
      pk_id: DatumId::new(),
      unique_conflict_op: Some("resolve".to_string()),
    }));
    let outcome = resident.apply(&encode_sk_index_op(&SkIndexOp::Insert {
      pk_id: DatumId::new(),
      unique_conflict_op: Some("resolve".to_string()),
    }));
    assert_eq!(outcome.violation, Some("resolve".to_string()));
    // A rejected update writes nothing through — resident/stored state
    // must be left untouched.
    assert!(outcome.write_through.is_none());
  }

  #[test]
  fn apply_insert_of_a_different_pk_id_without_unique_is_not_a_violation() {
    let mut resident = open_sk(None);
    resident.apply(&encode_sk_index_op(&SkIndexOp::Insert {
      pk_id: DatumId::new(),
      unique_conflict_op: None,
    }));
    let outcome = resident.apply(&encode_sk_index_op(&SkIndexOp::Insert {
      pk_id: DatumId::new(),
      unique_conflict_op: None,
    }));
    assert!(outcome.violation.is_none());
    assert_eq!(
      seisin_core::sk::decode_sk_entries(&outcome.write_through.unwrap())
        .unwrap()
        .len(),
      2
    );
  }

  #[test]
  fn open_seeds_the_resident_entries_from_stored_bytes() {
    let pk_id = DatumId::new();
    let stored = encode_sk_entries(&[(pk_id, AuthorityIdx::Native)]);
    let mut resident = open_sk(Some(stored));
    let outcome = resident.apply(&encode_sk_index_op(&SkIndexOp::Remove { pk_id }));
    assert_eq!(
      seisin_core::sk::decode_sk_entries(&outcome.write_through.unwrap()).unwrap(),
      vec![]
    );
  }

  #[test]
  fn open_on_undecodable_stored_bytes_is_an_error_not_an_empty_index() {
    assert!(SkIndexKind
      .open(DatumId::new(), Some(vec![0xFF, 0xFF, 0xFF]))
      .is_err());
  }

  #[test]
  fn register_sk_index_kind_wires_the_kind_name_correctly() {
    let mut registry = IndexKindRegistry::new();
    register_sk_index_kind(&mut registry);
    let pk_id = DatumId::new();
    let payload = encode_sk_index_op(&SkIndexOp::Insert {
      pk_id,
      unique_conflict_op: None,
    });
    let mut resident = registry
      .get("sk")
      .unwrap()
      .open(DatumId::new(), None)
      .unwrap();
    let outcome = resident.apply(&payload);
    assert!(outcome.violation.is_none());
    assert_eq!(
      seisin_core::sk::decode_sk_entries(&outcome.write_through.unwrap()).unwrap()[0].0,
      pk_id
    );
  }
}
