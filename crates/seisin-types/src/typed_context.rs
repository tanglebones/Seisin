//! A typed accessor wrapping `OpContext`, used by a solution's op
//! handler instead of raw `ctx.get`/`ctx.put`. Field-level changes are
//! detected automatically on drop and turned into scheduled index
//! updates — the op author never writes index-maintenance code by
//! hand. See the design doc's "Automatic Index Maintenance & Op
//! Lifecycle" section.

use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use seisin_core::datum::DatumId;
use seisin_ops::context::{FkMissingPolicy, OpContext};
use seisin_protocol::{encode_fk_pending_op, FkPendingOp};

use crate::field::FieldValue;
use crate::fk::fk_pending_key;
use crate::rk_index::{encode_rank_key, encode_rk_index_op, rk_key, RkIndexOp};
use crate::schema::{decode_datum, encode_datum, DatumTypeDef, IndexDef};
use crate::sk_index::{encode_sk_index_op, sk_key, SkIndexOp};

struct TrackedDatum {
  def: DatumTypeDef,
  before: Option<Vec<FieldValue>>,
  after: Option<Vec<FieldValue>>,
  touched: bool,
}

pub struct TypedOpContext<'a, 'b> {
  ctx: &'b mut OpContext<'a>,
  tracked: HashMap<DatumId, TrackedDatum>,
}

impl<'a, 'b> TypedOpContext<'a, 'b> {
  pub fn new(ctx: &'b mut OpContext<'a>) -> Self {
    Self {
      ctx,
      tracked: HashMap::new(),
    }
  }

  /// Reads `pk_id`'s current typed value, decoding via `def`. Remembers
  /// it as the "before" snapshot for diffing on drop, if `pk_id` hasn't
  /// been tracked yet this op. Existing bytes that fail to decode are an
  /// error, not `None` — treating corrupt/mismatched content as absent
  /// would let an op silently overwrite real data and compute index
  /// diffs from a false "before" state, stranding stale index entries.
  pub fn get(&mut self, pk_id: DatumId, def: &DatumTypeDef) -> Result<Option<Vec<FieldValue>>> {
    let values = match self.ctx.get(pk_id) {
      Some(bytes) => Some(
        decode_datum(def, &bytes)
          .with_context(|| format!("existing content for datum {pk_id:?} failed to decode"))?,
      ),
      None => None,
    };
    self.tracked.entry(pk_id).or_insert_with(|| TrackedDatum {
      def: def.clone(),
      before: values.clone(),
      after: values.clone(),
      touched: false,
    });
    Ok(values)
  }

  /// Writes `pk_id`'s new typed value. The byte write is staged
  /// immediately via the underlying `OpContext`; index maintenance is
  /// computed automatically on drop. An encode failure (type mismatch,
  /// wrong field count) fails the call before anything is staged or
  /// tracked — the datum and its indexes never diverge.
  pub fn set(&mut self, pk_id: DatumId, def: &DatumTypeDef, values: Vec<FieldValue>) -> Result<()> {
    crate::schema::check_pk(pk_id, def)?;
    check_static_constraints(def, &values)?;
    let bytes = encode_datum(def, &values)?;
    self.ensure_tracked(pk_id, def)?;
    self.ctx.put(pk_id, bytes);
    let entry = self.tracked.get_mut(&pk_id).unwrap();
    entry.after = Some(values);
    entry.touched = true;
    Ok(())
  }

  /// Deletes `pk_id`. Same tracking/diffing as `set`, but with an
  /// `after` of `None` — every declared sk index gets a remove
  /// scheduled for whatever the "before" value was.
  pub fn delete(&mut self, pk_id: DatumId, def: &DatumTypeDef) -> Result<()> {
    crate::schema::check_pk(pk_id, def)?;
    self.ensure_tracked(pk_id, def)?;
    self.ctx.delete(pk_id);
    let entry = self.tracked.get_mut(&pk_id).unwrap();
    entry.after = None;
    entry.touched = true;
    Ok(())
  }

  fn ensure_tracked(&mut self, pk_id: DatumId, def: &DatumTypeDef) -> Result<()> {
    if self.tracked.contains_key(&pk_id) {
      return Ok(());
    }
    let before = match self.ctx.get(pk_id) {
      Some(bytes) => Some(
        decode_datum(def, &bytes)
          .with_context(|| format!("existing content for datum {pk_id:?} failed to decode"))?,
      ),
      None => None,
    };
    self.tracked.insert(
      pk_id,
      TrackedDatum {
        def: def.clone(),
        before,
        after: None,
        touched: false,
      },
    );
    Ok(())
  }
}

/// The schema-local constraint checks that need no runtime dispatch —
/// enforced synchronously at `set` time, before anything is staged:
/// `PkEnum` membership (the whole point of enum pks — FK validity is
/// static) and `PkUuid` value shape (exactly 16 bytes, a DatumId).
fn check_static_constraints(def: &DatumTypeDef, values: &[FieldValue]) -> Result<()> {
  for constraint in &def.constraints {
    let Some(field_idx) = def
      .fields
      .iter()
      .position(|(name, _)| name == &constraint.field)
    else {
      continue; // declaration validation guarantees presence
    };
    match &constraint.references {
      crate::schema::FkTarget::PkEnum {
        type_name,
        mnemonics,
      } => {
        if let Some(FieldValue::String(value)) = values.get(field_idx) {
          if !mnemonics.contains(value) {
            bail!(
              "field {:?} references enum-pk type {:?} but {:?} is not a declared mnemonic",
              constraint.field,
              type_name,
              value
            );
          }
        }
      }
      crate::schema::FkTarget::PkUuid { type_name } => {
        if let Some(FieldValue::Bytes(bytes)) = values.get(field_idx) {
          if bytes.len() != 16 {
            bail!(
              "field {:?} references pk type {:?} and must hold a 16-byte DatumId, got {} bytes",
              constraint.field,
              type_name,
              bytes.len()
            );
          }
        }
      }
      crate::schema::FkTarget::SkUnique { .. } => {}
    }
  }
  Ok(())
}

impl<'a, 'b> Drop for TypedOpContext<'a, 'b> {
  fn drop(&mut self) {
    for (pk_id, tracked) in self.tracked.drain() {
      if !tracked.touched {
        continue;
      }
      for index in &tracked.def.indexes {
        match index {
          IndexDef::Sk { field, unique } => {
            let Some(field_idx) = tracked
              .def
              .fields
              .iter()
              .position(|(name, _)| name == field)
            else {
              continue;
            };
            let old_value = tracked.before.as_ref().map(|v| v[field_idx].clone());
            let new_value = tracked.after.as_ref().map(|v| v[field_idx].clone());
            if old_value == new_value {
              continue;
            }
            if let Some(old_value) = &old_value {
              if let Ok(old_key) = sk_key(&tracked.def.name, field, old_value) {
                let payload = encode_sk_index_op(&SkIndexOp::Remove { pk_id });
                self.ctx.schedule_index_update(old_key, "sk", payload);
              }
            }
            if let Some(new_value) = &new_value {
              if let Ok(new_key) = sk_key(&tracked.def.name, field, new_value) {
                let conflict_op = unique.as_ref().map(|op| op.0.clone());
                let payload = encode_sk_index_op(&SkIndexOp::Insert {
                  pk_id,
                  unique_conflict_op: conflict_op,
                });
                self.ctx.schedule_index_update(new_key, "sk", payload);
              }
            }
          }
          IndexDef::Rk { field } => {
            let Some(field_idx) = tracked
              .def
              .fields
              .iter()
              .position(|(name, _)| name == field)
            else {
              continue;
            };
            let old_value = tracked.before.as_ref().map(|v| v[field_idx].clone());
            let new_value = tracked.after.as_ref().map(|v| v[field_idx].clone());
            if old_value == new_value {
              continue;
            }
            // Declaration-time validation (schema.rs) guarantees the
            // field is numeric, so encode_rank_key cannot fail here.
            let old_rank_key = old_value.as_ref().and_then(|v| encode_rank_key(v).ok());
            let new_rank_key = new_value.as_ref().and_then(|v| encode_rank_key(v).ok());
            if old_rank_key.is_none() && new_rank_key.is_none() {
              continue;
            }
            let payload = encode_rk_index_op(&RkIndexOp {
              pk_id,
              old_rank_key,
              new_rank_key,
            });
            let target = rk_key(&tracked.def.name, field);
            self.ctx.schedule_index_update(target, "rk", payload);
          }
        }
      }

      // Relational constraints: schedule an existence check whenever a
      // constrained field changed (or the datum is new) and holds a
      // value. PkEnum needs no runtime check — membership was already
      // validated synchronously at set().
      let Some(new_values) = tracked.after.as_ref() else {
        continue; // deletes don't create references
      };
      for constraint in &tracked.def.constraints {
        let Some(field_idx) = tracked
          .def
          .fields
          .iter()
          .position(|(name, _)| name == &constraint.field)
        else {
          continue;
        };
        let old_value = tracked.before.as_ref().map(|v| v[field_idx].clone());
        if old_value.as_ref() == Some(&new_values[field_idx]) {
          continue; // unchanged reference — already checked when set
        }
        let target = match &constraint.references {
          crate::schema::FkTarget::PkEnum { .. } => continue,
          crate::schema::FkTarget::PkUuid { .. } => {
            let FieldValue::Bytes(bytes) = &new_values[field_idx] else {
              continue;
            };
            let Ok(raw) = <[u8; 16]>::try_from(bytes.as_slice()) else {
              continue; // shape enforced at set(); belt and braces
            };
            DatumId::from_bytes(raw)
          }
          crate::schema::FkTarget::SkUnique { type_name, field } => {
            match sk_key(type_name, field, &new_values[field_idx]) {
              Ok(id) => id,
              Err(_) => continue, // non-primitive rejected at declaration
            }
          }
        };
        let on_missing = match &constraint.resolution {
          None => FkMissingPolicy::Reject,
          Some(_) => FkMissingPolicy::Track {
            pending_datum: fk_pending_key(&tracked.def.name, &constraint.field),
            index_kind: "fk_pending".to_string(),
            entry: encode_fk_pending_op(&FkPendingOp::Insert {
              referencing_pk: pk_id,
              target,
            }),
          },
        };
        self.ctx.schedule_exists_check(target, on_missing);
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::field::FieldType;
  use crate::rk_index::{decode_rk_index_op, encode_rank_key, rk_key};
  use crate::schema::{ConflictOp, DatumTypeDef, IndexDef};
  use crate::sk_index::{decode_sk_index_op, sk_key, SkIndexOp};
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

  #[test]
  fn a_fresh_create_schedules_one_insert_and_no_remove() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let def = user_type();
    let pk_id = DatumId::new();
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx
        .set(
          pk_id,
          &def,
          vec![FieldValue::String("cliff".to_string()), FieldValue::I64(41)],
        )
        .unwrap();
    } // tctx dropped here — diffing happens now
    let updates = ctx.take_pending_index_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].index_kind, "sk");
    let expected_key = sk_key("user", "name", &FieldValue::String("cliff".to_string())).unwrap();
    assert_eq!(updates[0].target, expected_key);
    match decode_sk_index_op(&updates[0].payload).unwrap() {
      SkIndexOp::Insert { pk_id: id, .. } => assert_eq!(id, pk_id),
      other => panic!("expected an Insert op, got {other:?}"),
    }
  }

  #[test]
  fn updating_the_indexed_field_schedules_a_remove_and_an_insert() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let def = user_type();
    let pk_id = DatumId::new();

    // First write: establishes the pk datum's initial content directly
    // via the underlying OpContext (simulating an earlier op).
    {
      let mut ctx = OpContext::new(&mut cache);
      ctx.put(
        pk_id,
        crate::encode_datum(
          &def,
          &[FieldValue::String("cliff".to_string()), FieldValue::I64(41)],
        )
        .unwrap(),
      );
      for (id, content) in ctx.take_staged_writes() {
        if let Some(bytes) = content {
          cache.put(id, bytes);
        }
      }
    }

    let mut ctx = OpContext::new(&mut cache);
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.get(pk_id, &def).unwrap();
      tctx
        .set(
          pk_id,
          &def,
          vec![
            FieldValue::String("clifford".to_string()),
            FieldValue::I64(41),
          ],
        )
        .unwrap();
    }
    let updates = ctx.take_pending_index_updates();
    assert_eq!(updates.len(), 2);
    let old_key = sk_key("user", "name", &FieldValue::String("cliff".to_string())).unwrap();
    let new_key = sk_key("user", "name", &FieldValue::String("clifford".to_string())).unwrap();
    assert!(updates.iter().any(|u| u.target == old_key
      && matches!(
        decode_sk_index_op(&u.payload).unwrap(),
        SkIndexOp::Remove { .. }
      )));
    assert!(updates.iter().any(|u| u.target == new_key
      && matches!(
        decode_sk_index_op(&u.payload).unwrap(),
        SkIndexOp::Insert { .. }
      )));
  }

  #[test]
  fn writing_the_same_indexed_value_again_schedules_nothing() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let def = user_type();
    let pk_id = DatumId::new();
    let values = vec![FieldValue::String("cliff".to_string()), FieldValue::I64(41)];
    {
      let mut ctx = OpContext::new(&mut cache);
      ctx.put(pk_id, crate::encode_datum(&def, &values).unwrap());
      for (id, content) in ctx.take_staged_writes() {
        if let Some(bytes) = content {
          cache.put(id, bytes);
        }
      }
    }

    let mut ctx = OpContext::new(&mut cache);
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.get(pk_id, &def).unwrap();
      tctx.set(pk_id, &def, values).unwrap();
    }
    assert_eq!(ctx.take_pending_index_updates().len(), 0);
  }

  #[test]
  fn a_plain_get_with_no_set_schedules_nothing() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let def = user_type();
    let pk_id = DatumId::new();
    let mut ctx = OpContext::new(&mut cache);
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.get(pk_id, &def).unwrap();
    }
    assert_eq!(ctx.take_pending_index_updates().len(), 0);
  }

  #[test]
  fn delete_schedules_a_remove_from_every_declared_sk_index() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let def = user_type();
    let pk_id = DatumId::new();
    let values = vec![FieldValue::String("cliff".to_string()), FieldValue::I64(41)];
    {
      let mut ctx = OpContext::new(&mut cache);
      ctx.put(pk_id, crate::encode_datum(&def, &values).unwrap());
      for (id, content) in ctx.take_staged_writes() {
        if let Some(bytes) = content {
          cache.put(id, bytes);
        }
      }
    }

    let mut ctx = OpContext::new(&mut cache);
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.get(pk_id, &def).unwrap();
      tctx.delete(pk_id, &def).unwrap();
    }
    let updates = ctx.take_pending_index_updates();
    assert_eq!(updates.len(), 1);
    let key = sk_key("user", "name", &FieldValue::String("cliff".to_string())).unwrap();
    assert_eq!(updates[0].target, key);
    assert!(matches!(
      decode_sk_index_op(&updates[0].payload).unwrap(),
      SkIndexOp::Remove { .. }
    ));
  }

  #[test]
  fn a_unique_index_carries_its_conflict_op_into_the_scheduled_insert() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let def = DatumTypeDef::new("user")
      .field("email", FieldType::String)
      .index(IndexDef::Sk {
        field: "email".to_string(),
        unique: Some(ConflictOp("resolve".to_string())),
      });
    let pk_id = DatumId::new();
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx
        .set(
          pk_id,
          &def,
          vec![FieldValue::String("a@example.com".to_string())],
        )
        .unwrap();
    }
    let updates = ctx.take_pending_index_updates();
    match decode_sk_index_op(&updates[0].payload).unwrap() {
      SkIndexOp::Insert {
        unique_conflict_op, ..
      } => assert_eq!(unique_conflict_op, Some("resolve".to_string())),
      other => panic!("expected an Insert op, got {other:?}"),
    }
  }

  fn player_type() -> DatumTypeDef {
    DatumTypeDef::new("player")
      .field("score", FieldType::I64)
      .index(IndexDef::Rk {
        field: "score".to_string(),
      })
  }

  fn commit_initial(cache: &mut Cache, def: &DatumTypeDef, pk_id: DatumId, values: &[FieldValue]) {
    let mut ctx = OpContext::new(cache);
    ctx.put(pk_id, crate::encode_datum(def, values).unwrap());
    let staged = ctx.take_staged_writes();
    for (id, content) in staged {
      if let Some(bytes) = content {
        cache.put(id, bytes);
      }
    }
  }

  #[test]
  fn a_fresh_rk_write_schedules_one_insert_with_no_old_key() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let def = player_type();
    let pk_id = DatumId::new();
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.set(pk_id, &def, vec![FieldValue::I64(100)]).unwrap();
    }
    let updates = ctx.take_pending_index_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].index_kind, "rk");
    assert_eq!(updates[0].target, rk_key("player", "score"));
    let op = decode_rk_index_op(&updates[0].payload).unwrap();
    assert_eq!(op.pk_id, pk_id);
    assert_eq!(op.old_rank_key, None);
    assert_eq!(
      op.new_rank_key,
      Some(encode_rank_key(&FieldValue::I64(100)).unwrap())
    );
  }

  #[test]
  fn an_rk_score_change_schedules_one_update_carrying_old_and_new_keys() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let def = player_type();
    let pk_id = DatumId::new();
    commit_initial(&mut cache, &def, pk_id, &[FieldValue::I64(100)]);
    let mut ctx = OpContext::new(&mut cache);
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.get(pk_id, &def).unwrap();
      tctx.set(pk_id, &def, vec![FieldValue::I64(250)]).unwrap();
    }
    let updates = ctx.take_pending_index_updates();
    assert_eq!(updates.len(), 1); // one target datum, unlike sk's two
    let op = decode_rk_index_op(&updates[0].payload).unwrap();
    assert_eq!(
      op.old_rank_key,
      Some(encode_rank_key(&FieldValue::I64(100)).unwrap())
    );
    assert_eq!(
      op.new_rank_key,
      Some(encode_rank_key(&FieldValue::I64(250)).unwrap())
    );
  }

  #[test]
  fn deleting_an_rk_indexed_datum_schedules_a_remove_only_update() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let def = player_type();
    let pk_id = DatumId::new();
    commit_initial(&mut cache, &def, pk_id, &[FieldValue::I64(100)]);
    let mut ctx = OpContext::new(&mut cache);
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.delete(pk_id, &def).unwrap();
    }
    let updates = ctx.take_pending_index_updates();
    assert_eq!(updates.len(), 1);
    let op = decode_rk_index_op(&updates[0].payload).unwrap();
    assert!(op.old_rank_key.is_some());
    assert_eq!(op.new_rank_key, None);
  }

  #[test]
  fn an_unchanged_rk_score_schedules_nothing() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let def = player_type();
    let pk_id = DatumId::new();
    commit_initial(&mut cache, &def, pk_id, &[FieldValue::I64(100)]);
    let mut ctx = OpContext::new(&mut cache);
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.get(pk_id, &def).unwrap();
      tctx.set(pk_id, &def, vec![FieldValue::I64(100)]).unwrap();
    }
    assert_eq!(ctx.take_pending_index_updates().len(), 0);
  }

  #[test]
  fn set_rejects_a_non_v7_id_on_a_uuid_pk_type_and_wrong_ids_on_enum_pk_types() {
    use crate::schema::{enum_pk_id, PkKind};
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let def = user_type();
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      // A derived id is not v7 — rejected on the default Uuid-pk type.
      let result = tctx.set(
        enum_pk_id("status", "active"),
        &def,
        vec![FieldValue::String("x".to_string()), FieldValue::I64(1)],
      );
      assert!(result.is_err());
    }

    let status = DatumTypeDef::new("status")
      .pk(PkKind::Enum(vec!["active".to_string()]))
      .field("label", FieldType::String);
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      // A random v7 id is not a declared mnemonic — rejected.
      assert!(tctx
        .set(
          DatumId::new(),
          &status,
          vec![FieldValue::String("Active".to_string())]
        )
        .is_err());
      // The derived mnemonic id is accepted.
      assert!(tctx
        .set(
          enum_pk_id("status", "active"),
          &status,
          vec![FieldValue::String("Active".to_string())]
        )
        .is_ok());
    }
  }

  #[test]
  fn set_time_enum_membership_and_uuid_shape_checks() {
    use crate::schema::{FkTarget, RelationalConstraintDef};
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let order = DatumTypeDef::new("order")
      .field("status", FieldType::String)
      .field("customer_id", FieldType::Bytes)
      .constraint(RelationalConstraintDef {
        field: "status".to_string(),
        references: FkTarget::PkEnum {
          type_name: "status".to_string(),
          mnemonics: vec!["active".to_string(), "closed".to_string()],
        },
        resolution: None,
      })
      .constraint(RelationalConstraintDef {
        field: "customer_id".to_string(),
        references: FkTarget::PkUuid {
          type_name: "customer".to_string(),
        },
        resolution: None,
      });
    let mut tctx = TypedOpContext::new(&mut ctx);
    // Unknown mnemonic rejected, message names it.
    let err = tctx
      .set(
        DatumId::new(),
        &order,
        vec![
          FieldValue::String("bogus".to_string()),
          FieldValue::Bytes(vec![0u8; 16]),
        ],
      )
      .unwrap_err();
    assert!(err.to_string().contains("bogus"), "{err}");
    // Wrong-length uuid bytes rejected.
    assert!(tctx
      .set(
        DatumId::new(),
        &order,
        vec![
          FieldValue::String("active".to_string()),
          FieldValue::Bytes(vec![0u8; 15]),
        ],
      )
      .is_err());
    // Valid mnemonic + 16-byte id accepted.
    assert!(tctx
      .set(
        DatumId::new(),
        &order,
        vec![
          FieldValue::String("active".to_string()),
          FieldValue::Bytes(DatumId::new().as_bytes().to_vec()),
        ],
      )
      .is_ok());
  }

  #[test]
  fn fk_constraints_schedule_exists_checks_with_the_right_policy() {
    use crate::fk::fk_pending_key;
    use crate::schema::{FkTarget, RelationalConstraintDef};
    use seisin_ops::context::FkMissingPolicy;

    let customer_id = DatumId::new();
    let order_reject = DatumTypeDef::new("order")
      .field("customer_id", FieldType::Bytes)
      .constraint(RelationalConstraintDef {
        field: "customer_id".to_string(),
        references: FkTarget::PkUuid {
          type_name: "customer".to_string(),
        },
        resolution: None,
      });
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx
        .set(
          DatumId::new(),
          &order_reject,
          vec![FieldValue::Bytes(customer_id.as_bytes().to_vec())],
        )
        .unwrap();
    }
    let checks = ctx.take_pending_exists_checks();
    assert_eq!(checks.len(), 1);
    assert_eq!(checks[0].target, customer_id);
    assert!(matches!(checks[0].on_missing, FkMissingPolicy::Reject));

    // With a resolution declared: Track, aimed at the right pending
    // datum, carrying a decodable Insert entry.
    let order_track = DatumTypeDef::new("order")
      .field("customer_id", FieldType::Bytes)
      .constraint(RelationalConstraintDef {
        field: "customer_id".to_string(),
        references: FkTarget::PkUuid {
          type_name: "customer".to_string(),
        },
        resolution: Some(ConflictOp("null_customer".to_string())),
      });
    let pk = DatumId::new();
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx
        .set(
          pk,
          &order_track,
          vec![FieldValue::Bytes(customer_id.as_bytes().to_vec())],
        )
        .unwrap();
    }
    let checks = ctx.take_pending_exists_checks();
    assert_eq!(checks.len(), 1);
    match &checks[0].on_missing {
      FkMissingPolicy::Track {
        pending_datum,
        index_kind,
        entry,
      } => {
        assert_eq!(*pending_datum, fk_pending_key("order", "customer_id"));
        assert_eq!(index_kind, "fk_pending");
        match seisin_protocol::decode_fk_pending_op(entry).unwrap() {
          seisin_protocol::FkPendingOp::Insert {
            referencing_pk,
            target,
          } => {
            assert_eq!(referencing_pk, pk);
            assert_eq!(target, customer_id);
          }
          other => panic!("expected Insert, got {other:?}"),
        }
      }
      other => panic!("expected Track, got {other:?}"),
    }
  }

  #[test]
  fn unchanged_and_enum_fk_fields_schedule_no_exists_checks() {
    use crate::schema::{FkTarget, RelationalConstraintDef};
    let def = DatumTypeDef::new("order")
      .field("status", FieldType::String)
      .field("customer_id", FieldType::Bytes)
      .constraint(RelationalConstraintDef {
        field: "status".to_string(),
        references: FkTarget::PkEnum {
          type_name: "status".to_string(),
          mnemonics: vec!["active".to_string()],
        },
        resolution: None,
      })
      .constraint(RelationalConstraintDef {
        field: "customer_id".to_string(),
        references: FkTarget::PkUuid {
          type_name: "customer".to_string(),
        },
        resolution: None,
      });
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let pk = DatumId::new();
    let values = vec![
      FieldValue::String("active".to_string()),
      FieldValue::Bytes(DatumId::new().as_bytes().to_vec()),
    ];
    commit_initial(&mut cache, &def, pk, &values);
    let mut ctx = OpContext::new(&mut cache);
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx.get(pk, &def).unwrap();
      tctx.set(pk, &def, values).unwrap(); // nothing changed
    }
    // Enum constraint never schedules; unchanged uuid FK skipped.
    assert!(ctx.take_pending_exists_checks().is_empty());
  }

  #[test]
  fn sk_unique_constraints_target_the_derived_sk_key() {
    use crate::schema::{FkTarget, RelationalConstraintDef};
    use seisin_ops::context::FkMissingPolicy;
    let def = DatumTypeDef::new("order")
      .field("user_email", FieldType::String)
      .constraint(RelationalConstraintDef {
        field: "user_email".to_string(),
        references: FkTarget::SkUnique {
          type_name: "user".to_string(),
          field: "email".to_string(),
        },
        resolution: None,
      });
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      tctx
        .set(
          DatumId::new(),
          &def,
          vec![FieldValue::String("a@example.com".to_string())],
        )
        .unwrap();
    }
    let checks = ctx.take_pending_exists_checks();
    assert_eq!(checks.len(), 1);
    let expected = sk_key(
      "user",
      "email",
      &FieldValue::String("a@example.com".to_string()),
    )
    .unwrap();
    assert_eq!(checks[0].target, expected);
    assert!(matches!(checks[0].on_missing, FkMissingPolicy::Reject));
  }

  #[test]
  fn a_set_with_a_type_mismatched_value_fails_and_stages_nothing() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let def = user_type();
    let pk_id = DatumId::new();
    {
      let mut tctx = TypedOpContext::new(&mut ctx);
      // "age" is declared I64 — encoding must fail, and the failure must
      // not leave a staged write or a scheduled index update behind.
      let result = tctx.set(
        pk_id,
        &def,
        vec![
          FieldValue::String("cliff".to_string()),
          FieldValue::String("not a number".to_string()),
        ],
      );
      assert!(result.is_err());
    }
    assert!(ctx.take_staged_writes().is_empty());
    assert!(ctx.take_pending_index_updates().is_empty());
  }

  #[test]
  fn a_get_over_undecodable_existing_content_is_an_error_not_absence() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let pk_id = DatumId::new();
    cache.put(pk_id, vec![0xFF, 0xFF, 0xFF]); // garbage no schema decodes
    let mut ctx = OpContext::new(&mut cache);
    let def = user_type();
    let mut tctx = TypedOpContext::new(&mut ctx);
    assert!(tctx.get(pk_id, &def).is_err());
  }
}
