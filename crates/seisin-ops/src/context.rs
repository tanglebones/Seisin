//! `OpContext` is the interface a solution-defined operation function
//! uses to read/write the datums the framework has already collated
//! onto this thread. It operates at the byte level — the typed datum
//! layer from the design doc's "Datum Type System" notes is a separate
//! layer a solution's own generated code would sit on top of; the
//! framework itself stays type-agnostic.
//!
//! Writes are staged, not immediate: an op's `put`/`delete` calls are
//! held in memory (read-your-own-writes within the same op, via `get`)
//! until the framework (`worker.rs`) decides it's safe to commit them —
//! immediately, for an op that scheduled no index updates, or once
//! every scheduled index update has succeeded. See the design doc's
//! "Automatic Index Maintenance & Op Lifecycle" section.

use std::collections::HashMap;

use seisin_core::cache::Cache;
use seisin_core::datum::DatumId;

/// One index update an op wants applied, once its own effects are known
/// to be safe to commit. Framework-internal: solution op authors never
/// construct this directly — a typed accessor layer (`seisin-types`)
/// calls `OpContext::schedule_index_update` on their behalf.
pub struct PendingIndexUpdate {
  pub target: DatumId,
  pub index_kind: String,
  pub payload: Vec<u8>,
}

pub struct OpContext<'a> {
  cache: &'a mut Cache,
  staged: HashMap<DatumId, Option<Vec<u8>>>,
  pending_index_updates: Vec<PendingIndexUpdate>,
}

impl<'a> OpContext<'a> {
  pub fn new(cache: &'a mut Cache) -> Self {
    Self {
      cache,
      staged: HashMap::new(),
      pending_index_updates: Vec::new(),
    }
  }

  /// Reads `id`'s current value. A value staged by an earlier `put`/
  /// `delete` in this same op is visible immediately (read-your-own-
  /// writes), even though nothing is actually committed to the
  /// underlying cache/storage until the framework says so.
  pub fn get(&mut self, id: DatumId) -> Option<Vec<u8>> {
    if let Some(staged) = self.staged.get(&id) {
      return staged.clone();
    }
    self.cache.get(id)
  }

  pub fn put(&mut self, id: DatumId, content: Vec<u8>) {
    self.staged.insert(id, Some(content));
  }

  pub fn delete(&mut self, id: DatumId) {
    self.staged.insert(id, None);
  }

  /// Schedules an index update to be dispatched, once this op's
  /// business logic finishes, to whichever thread natively owns
  /// `target`. Framework-internal.
  pub fn schedule_index_update(
    &mut self,
    target: DatumId,
    index_kind: impl Into<String>,
    payload: Vec<u8>,
  ) {
    self.pending_index_updates.push(PendingIndexUpdate {
      target,
      index_kind: index_kind.into(),
      payload,
    });
  }

  /// Drains every staged write from this op. Framework-internal — the
  /// caller (`worker.rs`) is responsible for actually committing these
  /// to the underlying cache once it's known safe to do so.
  pub fn take_staged_writes(&mut self) -> Vec<(DatumId, Option<Vec<u8>>)> {
    std::mem::take(&mut self.staged).into_iter().collect()
  }

  /// Drains every index update this op scheduled. Framework-internal.
  pub fn take_pending_index_updates(&mut self) -> Vec<PendingIndexUpdate> {
    std::mem::take(&mut self.pending_index_updates)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::Arc;

  use seisin_core::store::InMemoryStore;

  #[test]
  fn put_then_get_round_trips_through_the_context() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let id = DatumId::new();
    ctx.put(id, b"hello".to_vec());
    assert_eq!(ctx.get(id), Some(b"hello".to_vec()));
  }

  #[test]
  fn delete_removes_the_entry() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let id = DatumId::new();
    ctx.put(id, b"hello".to_vec());
    ctx.delete(id);
    assert_eq!(ctx.get(id), None);
  }

  #[test]
  fn get_on_unknown_datum_returns_none() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    assert_eq!(ctx.get(DatumId::new()), None);
  }

  #[test]
  fn a_staged_write_is_not_visible_on_the_underlying_cache_until_taken_and_committed() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let id = DatumId::new();
    {
      let mut ctx = OpContext::new(&mut cache);
      ctx.put(id, b"staged".to_vec());
      assert_eq!(ctx.get(id), Some(b"staged".to_vec())); // read-your-own-write
    }
    assert_eq!(cache.get(id), None); // never committed
  }

  #[test]
  fn take_staged_writes_returns_every_put_and_delete() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let put_id = DatumId::new();
    let delete_id = DatumId::new();
    ctx.put(put_id, b"hello".to_vec());
    ctx.delete(delete_id);
    let mut writes = ctx.take_staged_writes();
    writes.sort_by_key(|(id, _)| *id);
    let mut expected = vec![(put_id, Some(b"hello".to_vec())), (delete_id, None)];
    expected.sort_by_key(|(id, _)| *id);
    assert_eq!(writes, expected);
  }

  #[test]
  fn take_staged_writes_drains_so_a_second_call_is_empty() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    ctx.put(DatumId::new(), b"x".to_vec());
    assert_eq!(ctx.take_staged_writes().len(), 1);
    assert_eq!(ctx.take_staged_writes().len(), 0);
  }

  #[test]
  fn schedule_index_update_is_collected_by_take_pending_index_updates() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let target = DatumId::new();
    ctx.schedule_index_update(target, "sk", vec![9, 9]);
    let updates = ctx.take_pending_index_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].target, target);
    assert_eq!(updates[0].index_kind, "sk");
    assert_eq!(updates[0].payload, vec![9, 9]);
    assert_eq!(ctx.take_pending_index_updates().len(), 0);
  }
}
