//! The `"extent"` kind: one counted-B+Tree file per tracked type
//! listing that type's datum pks — the rescan driver's enumeration
//! surface. Maintained automatically by `TypedOpContext` for types
//! declaring `.track_extent()`; paged via `Request::ExtentQuery`. One
//! extent datum per type is a create/delete-time write funnel — the
//! same documented single-datum limitation class as rk, same future
//! sharding answer.

use std::cell::RefCell;
use std::path::PathBuf;

use seisin_core::datum::DatumId;
use seisin_node::index_handler::{
  IndexApplyOutcome, IndexKind, IndexKindRegistry, ResidentIndex, WriteThrough,
};
use seisin_protocol::{decode_extent_op, decode_extent_page, encode_extent_result, ExtentOp};
use seisin_storage::btree::BPlusTree;

use crate::sk_index::derived_id_namespace;

const EXTENT_PAGE_SIZE: u32 = 4096;

/// One extent datum per tracked type.
pub fn extent_key(type_name: &str) -> DatumId {
  let name = format!("extent:{type_name}");
  DatumId::from_name(&derived_id_namespace(), name.as_bytes())
}

pub struct ExtentKind {
  data_dir: PathBuf,
}

impl ExtentKind {
  pub fn new(data_dir: PathBuf) -> Self {
    Self { data_dir }
  }
}

fn file_name_for(target: DatumId) -> String {
  let hex: String = target
    .as_bytes()
    .iter()
    .map(|b| format!("{b:02x}"))
    .collect();
  format!("extent_{hex}.btree")
}

pub struct ExtentResident {
  // RefCell for the same reason as rk/lb/tk: `query` takes `&self`
  // while B+Tree page reads need `&mut`. Single-threaded by
  // construction.
  tree: RefCell<BPlusTree>,
}

impl ResidentIndex for ExtentResident {
  fn apply(&mut self, payload: &[u8]) -> IndexApplyOutcome {
    let op = match decode_extent_op(payload) {
      Ok(op) => op,
      Err(e) => {
        return IndexApplyOutcome {
          violation: Some(format!("malformed extent payload: {e}")),
          write_through: WriteThrough::None,
        }
      }
    };
    let tree = self.tree.get_mut();
    let result = match op {
      ExtentOp::Insert { pk } => tree.insert(&pk.as_bytes(), &[0u8]),
      ExtentOp::Remove { pk } => tree.remove(&pk.as_bytes()).map(|_| ()),
    };
    IndexApplyOutcome {
      violation: result.err().map(|e| format!("extent apply failed: {e}")),
      write_through: WriteThrough::None, // self-persisted
    }
  }

  fn query(&self, query: &[u8]) -> Result<Vec<u8>, String> {
    let (offset, limit) = decode_extent_page(query).map_err(|e| e.to_string())?;
    let mut tree = self.tree.borrow_mut();
    let total = tree.len() as u64;
    let pks: Vec<DatumId> = tree
      .scan_from_rank(offset, limit as usize)
      .map_err(|e| e.to_string())?
      .into_iter()
      .map(|(key, _)| DatumId::from_bytes(key.try_into().expect("extent keys are 16 bytes")))
      .collect();
    Ok(encode_extent_result(total, &pks))
  }
}

impl IndexKind for ExtentKind {
  /// `stored` is ignored: the extent persists in its own page file.
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
        .map_err(|e| format!("failed to create extent data dir {:?}: {e}", self.data_dir))?;
      BPlusTree::create(&path, 16, 1, EXTENT_PAGE_SIZE)
    }
    .map_err(|e| format!("failed to open extent file {path:?}: {e}"))?;
    Ok(Box::new(ExtentResident {
      tree: RefCell::new(tree),
    }))
  }
}

/// Registers the `"extent"` kind — call once at the composition root
/// wherever `.track_extent()` types exist.
pub fn register_extent_kind(registry: &mut IndexKindRegistry, data_dir: PathBuf) {
  registry.register("extent", Box::new(ExtentKind::new(data_dir)));
}

#[cfg(test)]
mod tests {
  use super::*;
  use seisin_protocol::{decode_extent_result, encode_extent_op, encode_extent_page};

  fn open_extent(dir: &std::path::Path) -> Box<dyn ResidentIndex> {
    ExtentKind::new(dir.to_path_buf())
      .open(DatumId::new(), None)
      .unwrap()
  }

  fn page(resident: &dyn ResidentIndex, offset: u64, limit: u32) -> (u64, Vec<DatumId>) {
    decode_extent_result(&resident.query(&encode_extent_page(offset, limit)).unwrap()).unwrap()
  }

  #[test]
  fn inserts_and_removes_maintain_the_population() {
    let dir = tempfile::tempdir().unwrap();
    let mut extent = open_extent(dir.path());
    let pks: Vec<DatumId> = (0..5).map(|_| DatumId::new()).collect();
    for pk in &pks {
      let outcome = extent.apply(&encode_extent_op(&ExtentOp::Insert { pk: *pk }));
      assert!(outcome.violation.is_none());
    }
    let (total, listed) = page(extent.as_ref(), 0, 100);
    assert_eq!(total, 5);
    assert_eq!(listed.len(), 5);
    extent.apply(&encode_extent_op(&ExtentOp::Remove { pk: pks[0] }));
    let (total, listed) = page(extent.as_ref(), 0, 100);
    assert_eq!(total, 4);
    assert!(!listed.contains(&pks[0]));
  }

  #[test]
  fn paging_is_exact_and_disjoint() {
    let dir = tempfile::tempdir().unwrap();
    let mut extent = open_extent(dir.path());
    let mut pks: Vec<DatumId> = (0..10).map(|_| DatumId::new()).collect();
    for pk in &pks {
      extent.apply(&encode_extent_op(&ExtentOp::Insert { pk: *pk }));
    }
    pks.sort(); // extent orders by pk bytes
    let (_, first) = page(extent.as_ref(), 0, 4);
    let (_, second) = page(extent.as_ref(), 4, 4);
    let (_, third) = page(extent.as_ref(), 8, 4);
    let mut all = first;
    all.extend(second);
    all.extend(third);
    assert_eq!(all, pks);
    let (_, past_end) = page(extent.as_ref(), 10, 4);
    assert!(past_end.is_empty());
  }

  #[test]
  fn cold_reopen_answers_from_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let target = DatumId::new();
    let kind = ExtentKind::new(dir.path().to_path_buf());
    let pk = DatumId::new();
    {
      let mut extent = kind.open(target, None).unwrap();
      extent.apply(&encode_extent_op(&ExtentOp::Insert { pk }));
    }
    let extent = kind.open(target, None).unwrap();
    let (total, listed) = page(extent.as_ref(), 0, 10);
    assert_eq!(total, 1);
    assert_eq!(listed, vec![pk]);
  }

  #[test]
  fn malformed_payloads_error_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    let mut extent = open_extent(dir.path());
    assert!(extent.apply(&[0xFF, 0xFF]).violation.is_some());
    assert!(extent.query(&[0x01]).is_err());
  }
}
