//! The `"rk"` `IndexKind`: a disk-backed counted B+Tree per declared rk
//! index, resident as an open `seisin_storage::BPlusTree` handle on the
//! owning thread. Self-persisted — every apply outcome carries
//! `write_through: None`, because the tree's own page file is the
//! storage; there is no blob to hand back to the datum cache.

use std::cell::RefCell;
use std::path::PathBuf;

use seisin_core::datum::DatumId;
use seisin_node::index_handler::{IndexApplyOutcome, IndexKind, IndexKindRegistry, ResidentIndex};
use seisin_protocol::{decode_rk_query_kind, encode_rk_entries, RkQueryKind};
use seisin_storage::btree::BPlusTree;

use crate::rk_index::decode_rk_index_op;

/// key = rank_key (8) ++ pk_id (16): pk_id is a deterministic tiebreaker
/// so ties at the same score never collide/overwrite under upsert (real
/// data loss for a leaderboard, where ties are expected).
const RK_KEY_SIZE: u32 = 24;
const RK_VALUE_SIZE: u32 = 16;
const RK_PAGE_SIZE: u32 = 4096;

pub struct RkIndexKind {
  data_dir: PathBuf,
}

impl RkIndexKind {
  pub fn new(data_dir: PathBuf) -> Self {
    Self { data_dir }
  }
}

/// Files are named by the index datum's id, not the `type.field` pair —
/// `IndexKind::open` only receives the `DatumId`, which is already the
/// stable, collision-free derivation of `rk:{type}.{field}`.
fn file_name_for(target: DatumId) -> String {
  let hex: String = target
    .as_bytes()
    .iter()
    .map(|b| format!("{b:02x}"))
    .collect();
  format!("rk_{hex}.btree")
}

fn composite_key(rank_key: &[u8; 8], pk_id: DatumId) -> [u8; 24] {
  let mut key = [0u8; 24];
  key[0..8].copy_from_slice(rank_key);
  key[8..24].copy_from_slice(&pk_id.as_bytes());
  key
}

pub struct RkResidentIndex {
  // RefCell because `ResidentIndex::query` takes `&self` (a read in
  // spirit) while `BPlusTree`'s page reads need `&mut self` (they seek
  // the underlying file). Single-threaded by construction: a resident
  // index lives on exactly one owning thread's map.
  tree: RefCell<BPlusTree>,
}

impl ResidentIndex for RkResidentIndex {
  fn apply(&mut self, payload: &[u8]) -> IndexApplyOutcome {
    let op = match decode_rk_index_op(payload) {
      Ok(op) => op,
      Err(e) => {
        return IndexApplyOutcome {
          violation: Some(format!("malformed rk index payload: {e}")),
          write_through: None,
        }
      }
    };
    let tree = self.tree.get_mut();
    if let Some(old) = &op.old_rank_key {
      if let Err(e) = tree.remove(&composite_key(old, op.pk_id)) {
        return IndexApplyOutcome {
          violation: Some(format!("rk remove failed: {e}")),
          write_through: None,
        };
      }
    }
    if let Some(new) = &op.new_rank_key {
      if let Err(e) = tree.insert(&composite_key(new, op.pk_id), &op.pk_id.as_bytes()) {
        return IndexApplyOutcome {
          violation: Some(format!("rk insert failed: {e}")),
          write_through: None,
        };
      }
    }
    IndexApplyOutcome {
      violation: None,
      write_through: None,
    }
  }

  fn query(&self, query: &[u8]) -> Result<Vec<u8>, String> {
    let kind = decode_rk_query_kind(query).map_err(|e| e.to_string())?;
    let mut tree = self.tree.borrow_mut();
    let raw = match kind {
      RkQueryKind::TopN(n) => tree.scan_backward_bounded(n as usize),
      RkQueryKind::BottomN(n) => tree.scan_forward_bounded(n as usize),
      RkQueryKind::PercentileSample(k) => tree.sample_by_rank(k as usize),
    }
    .map_err(|e| e.to_string())?;
    let entries: Vec<(Vec<u8>, DatumId)> = raw
      .into_iter()
      .map(|(key, value)| {
        let pk_id = DatumId::from_bytes(value.try_into().expect("rk values are 16 bytes"));
        (key[0..8].to_vec(), pk_id)
      })
      .collect();
    Ok(encode_rk_entries(&entries))
  }
}

impl IndexKind for RkIndexKind {
  /// `stored` is ignored: rk persists in its own page file, not as
  /// datum-cache bytes.
  fn open(
    &self,
    target: DatumId,
    _stored: Option<Vec<u8>>,
  ) -> Result<Box<dyn ResidentIndex>, String> {
    let path = self.data_dir.join(file_name_for(target));
    let tree = if path.exists() {
      BPlusTree::open(&path)
    } else {
      std::fs::create_dir_all(&self.data_dir)
        .map_err(|e| format!("failed to create rk data dir {:?}: {e}", self.data_dir))?;
      BPlusTree::create(&path, RK_KEY_SIZE, RK_VALUE_SIZE, RK_PAGE_SIZE)
    }
    .map_err(|e| format!("failed to open rk index file {path:?}: {e}"))?;
    Ok(Box::new(RkResidentIndex {
      tree: RefCell::new(tree),
    }))
  }
}

/// Registers the `"rk"` index kind — call once at startup with the
/// directory this node's rk index files live in.
pub fn register_rk_index_kind(registry: &mut IndexKindRegistry, data_dir: PathBuf) {
  registry.register("rk", Box::new(RkIndexKind::new(data_dir)));
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::field::FieldValue;
  use crate::rk_index::{encode_rank_key, encode_rk_index_op, RkIndexOp};
  use seisin_protocol::{decode_rk_entries, encode_rk_query_kind};

  fn open_rk(dir: &std::path::Path) -> (DatumId, Box<dyn ResidentIndex>) {
    let target = DatumId::new();
    let kind = RkIndexKind::new(dir.to_path_buf());
    (target, kind.open(target, None).unwrap())
  }

  fn insert_score(resident: &mut dyn ResidentIndex, pk_id: DatumId, score: i64) {
    let payload = encode_rk_index_op(&RkIndexOp {
      pk_id,
      old_rank_key: None,
      new_rank_key: Some(encode_rank_key(&FieldValue::I64(score)).unwrap()),
    });
    let outcome = resident.apply(&payload);
    assert!(outcome.violation.is_none());
    assert!(outcome.write_through.is_none()); // self-persisted
  }

  fn top_n(resident: &dyn ResidentIndex, n: u32) -> Vec<DatumId> {
    let bytes = resident
      .query(&encode_rk_query_kind(&RkQueryKind::TopN(n)))
      .unwrap();
    decode_rk_entries(&bytes)
      .unwrap()
      .into_iter()
      .map(|(_, id)| id)
      .collect()
  }

  #[test]
  fn inserts_then_top_n_returns_highest_scores_first() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut resident) = open_rk(dir.path());
    let (a, b, c) = (DatumId::new(), DatumId::new(), DatumId::new());
    insert_score(resident.as_mut(), a, 100);
    insert_score(resident.as_mut(), b, 300);
    insert_score(resident.as_mut(), c, 200);
    assert_eq!(top_n(resident.as_ref(), 2), vec![b, c]);
  }

  #[test]
  fn a_score_change_moves_the_entry_not_duplicates_it() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut resident) = open_rk(dir.path());
    let (a, b) = (DatumId::new(), DatumId::new());
    insert_score(resident.as_mut(), a, 100);
    insert_score(resident.as_mut(), b, 200);
    let payload = encode_rk_index_op(&RkIndexOp {
      pk_id: a,
      old_rank_key: Some(encode_rank_key(&FieldValue::I64(100)).unwrap()),
      new_rank_key: Some(encode_rank_key(&FieldValue::I64(300)).unwrap()),
    });
    assert!(resident.apply(&payload).violation.is_none());
    assert_eq!(top_n(resident.as_ref(), 10), vec![a, b]); // exactly two entries
  }

  #[test]
  fn a_delete_removes_the_entry() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut resident) = open_rk(dir.path());
    let a = DatumId::new();
    insert_score(resident.as_mut(), a, 100);
    let payload = encode_rk_index_op(&RkIndexOp {
      pk_id: a,
      old_rank_key: Some(encode_rank_key(&FieldValue::I64(100)).unwrap()),
      new_rank_key: None,
    });
    assert!(resident.apply(&payload).violation.is_none());
    assert!(top_n(resident.as_ref(), 10).is_empty());
  }

  #[test]
  fn tied_scores_keep_both_entries() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut resident) = open_rk(dir.path());
    let (a, b) = (DatumId::new(), DatumId::new());
    insert_score(resident.as_mut(), a, 100);
    insert_score(resident.as_mut(), b, 100);
    assert_eq!(top_n(resident.as_ref(), 10).len(), 2);
  }

  #[test]
  fn bottom_n_and_percentile_sample_answer_from_the_same_tree() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut resident) = open_rk(dir.path());
    for score in [10i64, 20, 30, 40, 50] {
      insert_score(resident.as_mut(), DatumId::new(), score);
    }
    let bottom = resident
      .query(&encode_rk_query_kind(&RkQueryKind::BottomN(2)))
      .unwrap();
    assert_eq!(decode_rk_entries(&bottom).unwrap().len(), 2);
    let sample = resident
      .query(&encode_rk_query_kind(&RkQueryKind::PercentileSample(3)))
      .unwrap();
    assert_eq!(decode_rk_entries(&sample).unwrap().len(), 3);
  }

  #[test]
  fn reopening_the_same_target_reuses_the_existing_file() {
    let dir = tempfile::tempdir().unwrap();
    let target = DatumId::new();
    let kind = RkIndexKind::new(dir.path().to_path_buf());
    let a = DatumId::new();
    {
      let mut resident = kind.open(target, None).unwrap();
      insert_score(resident.as_mut(), a, 100);
    }
    let resident = kind.open(target, None).unwrap();
    assert_eq!(top_n(resident.as_ref(), 10), vec![a]);
  }

  #[test]
  fn a_malformed_payload_is_a_violation_not_a_panic() {
    let dir = tempfile::tempdir().unwrap();
    let (_, mut resident) = open_rk(dir.path());
    let outcome = resident.apply(&[0xFF, 0xFF]);
    assert!(outcome.violation.is_some());
  }
}
