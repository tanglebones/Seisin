//! A typed accessor wrapping `OpContext`, used by a solution's op
//! handler instead of raw `ctx.get`/`ctx.put`. Field-level changes are
//! detected automatically on drop and turned into scheduled index
//! updates — the op author never writes index-maintenance code by
//! hand. See the design doc's "Automatic Index Maintenance & Op
//! Lifecycle" section.

use std::collections::HashMap;

use anyhow::{Context, Result};
use seisin_core::datum::DatumId;
use seisin_ops::context::OpContext;

use crate::field::FieldValue;
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
