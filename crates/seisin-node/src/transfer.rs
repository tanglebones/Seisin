//! The storage node's live-migration transfer engine. A `Transfer`
//! request records a set of ids to copy to a destination and kicks off
//! an async snapshot copy; client writes keep flowing the whole time,
//! and any write to a transfer id marks it *dirty* so `FinishTransfer`
//! can re-send the tail. Dirty tracking is deliberately over-inclusive
//! (a write to a transfer id *before* its snapshot copy still re-sends
//! it) — correct, and simpler than per-id copy timestamps. `Retire`
//! tombstones the moved ids once the ring has flipped.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use seisin_core::datum::DatumId;

struct TransferState {
  ids: HashSet<DatumId>,
  dest: String,
  copied: u64,
  dirty: HashSet<DatumId>,
  done: bool,
}

#[derive(Default)]
pub struct TransferManager {
  inner: Mutex<HashMap<DatumId, TransferState>>,
}

impl TransferManager {
  /// Records a new transfer (ids to move to `dest`); the async copy is
  /// driven by the store server on a worker thread.
  pub fn start(&self, transfer_id: DatumId, ids: Vec<DatumId>, dest: String) {
    self.inner.lock().unwrap().insert(
      transfer_id,
      TransferState {
        ids: ids.into_iter().collect(),
        dest,
        copied: 0,
        dirty: HashSet::new(),
        done: false,
      },
    );
  }

  /// Marks `id` dirty in every active transfer whose set contains it —
  /// called after any successful Put/Patch/Delete on the source.
  pub fn note_write(&self, id: DatumId) {
    let mut inner = self.inner.lock().unwrap();
    for state in inner.values_mut() {
      if state.ids.contains(&id) {
        state.dirty.insert(id);
      }
    }
  }

  pub fn dest(&self, transfer_id: DatumId) -> Option<String> {
    self
      .inner
      .lock()
      .unwrap()
      .get(&transfer_id)
      .map(|s| s.dest.clone())
  }

  pub fn ids(&self, transfer_id: DatumId) -> Vec<DatumId> {
    self
      .inner
      .lock()
      .unwrap()
      .get(&transfer_id)
      .map(|s| s.ids.iter().copied().collect())
      .unwrap_or_default()
  }

  pub fn bump_copied(&self, transfer_id: DatumId, n: u64) {
    if let Some(state) = self.inner.lock().unwrap().get_mut(&transfer_id) {
      state.copied += n;
    }
  }

  pub fn mark_done(&self, transfer_id: DatumId) {
    if let Some(state) = self.inner.lock().unwrap().get_mut(&transfer_id) {
      state.done = true;
    }
  }

  /// `(copied, dirty, done)` for an active transfer, or `None` if unknown.
  pub fn status(&self, transfer_id: DatumId) -> Option<(u64, u64, bool)> {
    self
      .inner
      .lock()
      .unwrap()
      .get(&transfer_id)
      .map(|s| (s.copied, s.dirty.len() as u64, s.done))
  }

  /// Drains and returns the dirty set for the tail re-send.
  pub fn take_dirty(&self, transfer_id: DatumId) -> Vec<DatumId> {
    self
      .inner
      .lock()
      .unwrap()
      .get_mut(&transfer_id)
      .map(|s| s.dirty.drain().collect())
      .unwrap_or_default()
  }

  /// Removes the transfer and returns its id set (to tombstone).
  pub fn retire(&self, transfer_id: DatumId) -> Vec<DatumId> {
    self
      .inner
      .lock()
      .unwrap()
      .remove(&transfer_id)
      .map(|s| s.ids.into_iter().collect())
      .unwrap_or_default()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn start_records_ids_and_dest() {
    let m = TransferManager::default();
    let tid = DatumId::new();
    let a = DatumId::new();
    let b = DatumId::new();
    m.start(tid, vec![a, b], "dest:1".to_string());
    assert_eq!(m.dest(tid), Some("dest:1".to_string()));
    let mut ids = m.ids(tid);
    ids.sort();
    let mut expected = vec![a, b];
    expected.sort();
    assert_eq!(ids, expected);
    assert_eq!(m.status(tid), Some((0, 0, false)));
  }

  #[test]
  fn note_write_only_dirties_ids_in_the_transfer_set() {
    let m = TransferManager::default();
    let tid = DatumId::new();
    let a = DatumId::new();
    let outsider = DatumId::new();
    m.start(tid, vec![a], "d".to_string());
    m.note_write(outsider); // not in the set — ignored
    assert_eq!(m.status(tid).unwrap().1, 0);
    m.note_write(a);
    m.note_write(a); // idempotent (a set)
    assert_eq!(m.status(tid).unwrap().1, 1);
  }

  #[test]
  fn copied_and_done_progress_and_take_dirty_drains() {
    let m = TransferManager::default();
    let tid = DatumId::new();
    let a = DatumId::new();
    m.start(tid, vec![a], "d".to_string());
    m.bump_copied(tid, 1);
    m.note_write(a);
    m.mark_done(tid);
    assert_eq!(m.status(tid), Some((1, 1, true)));
    assert_eq!(m.take_dirty(tid), vec![a]);
    assert_eq!(m.status(tid).unwrap().1, 0); // drained
  }

  #[test]
  fn retire_returns_the_ids_and_forgets_the_transfer() {
    let m = TransferManager::default();
    let tid = DatumId::new();
    let a = DatumId::new();
    m.start(tid, vec![a], "d".to_string());
    assert_eq!(m.retire(tid), vec![a]);
    assert_eq!(m.status(tid), None); // gone
    assert_eq!(m.retire(tid), Vec::<DatumId>::new()); // idempotent
  }
}
