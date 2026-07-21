//! Whole-op write/delete for a typed datum, including sk index
//! maintenance and best-effort uniqueness checking. See the design
//! doc's "sk Index" and "Constraint Enforcement" sections.

use anyhow::{bail, Result};
use seisin_core::datum::DatumId;
use seisin_ops::context::OpContext;

use crate::field::FieldValue;
use crate::schema::{decode_datum, encode_datum, DatumTypeDef, IndexDef};
use crate::sk_index::{insert_sk_entry, remove_sk_entry, sk_key, UniquenessViolation};

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
/// sk key this write touches — see this plan's Task 6 for the
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

    let violation = result
      .violation
      .expect("a second writer to the same unique value must be flagged");
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
