//! The counted B+Tree itself: create/open a tree backed by a single
//! page file, insert (upsert-only, no delete), and the bounded-scan/
//! rank-sampling/rebuild operations built in later tasks. See the
//! design doc's "Operations" section.

use std::fs::File;
use std::io::Read;
use std::path::Path;

use anyhow::{bail, Result};

use crate::node::{
  decode_internal, decode_leaf, encode_internal, encode_leaf, max_internal_entries,
  max_leaf_entries, page_type, InternalNode, LeafNode, PageType,
};
use crate::page_store::PageStore;
use crate::superblock::Superblock;
use crate::{PageId, NULL_PAGE};

pub struct BPlusTree {
  store: PageStore,
  page_size: u32,
  key_size: u32,
  value_size: u32,
  root_page_id: PageId,
  total_count: u64,
  next_page_id: PageId,
  max_leaf_entries: usize,
  max_internal_entries: usize,
}

impl BPlusTree {
  pub fn create(path: &Path, key_size: u32, value_size: u32, page_size: u32) -> Result<Self> {
    Superblock::validate_page_size(page_size)?;
    let max_leaf = max_leaf_entries(page_size, key_size, value_size);
    let max_internal = max_internal_entries(page_size, key_size);
    if max_leaf == 0 {
      bail!("key_size + value_size too large for page_size {page_size}");
    }
    if max_internal == 0 {
      bail!("key_size too large for page_size {page_size} (internal node capacity)");
    }
    let mut store = PageStore::create(path, page_size)?;
    let root_id: PageId = 1;
    let empty_leaf = LeafNode { prev: NULL_PAGE, next: NULL_PAGE, entries: vec![] };
    store.write_page(root_id, &encode_leaf(&empty_leaf, page_size, key_size, value_size))?;
    let mut tree = Self {
      store,
      page_size,
      key_size,
      value_size,
      root_page_id: root_id,
      total_count: 0,
      next_page_id: 2,
      max_leaf_entries: max_leaf,
      max_internal_entries: max_internal,
    };
    tree.write_superblock()?;
    Ok(tree)
  }

  pub fn open(path: &Path) -> Result<Self> {
    let mut header_buf = vec![0u8; 44];
    let mut file = File::open(path)?;
    file.read_exact(&mut header_buf)?;
    let sb = Superblock::decode(&header_buf)?;
    drop(file);
    let store = PageStore::open(path, sb.page_size)?;
    let max_leaf = max_leaf_entries(sb.page_size, sb.key_size, sb.value_size);
    let max_internal = max_internal_entries(sb.page_size, sb.key_size);
    Ok(Self {
      store,
      page_size: sb.page_size,
      key_size: sb.key_size,
      value_size: sb.value_size,
      root_page_id: sb.root_page_id,
      total_count: sb.total_count,
      next_page_id: sb.next_page_id,
      max_leaf_entries: max_leaf,
      max_internal_entries: max_internal,
    })
  }

  fn write_superblock(&mut self) -> Result<()> {
    let sb = Superblock {
      page_size: self.page_size,
      key_size: self.key_size,
      value_size: self.value_size,
      root_page_id: self.root_page_id,
      total_count: self.total_count,
      next_page_id: self.next_page_id,
    };
    self.store.write_page(0, &sb.encode(self.page_size))
  }

  fn allocate_page(&mut self) -> PageId {
    let id = self.next_page_id;
    self.next_page_id += 1;
    id
  }

  fn read_leaf(&mut self, id: PageId) -> Result<LeafNode> {
    let bytes = self.store.read_page(id)?;
    decode_leaf(&bytes, self.key_size, self.value_size)
  }

  fn write_leaf(&mut self, id: PageId, node: &LeafNode) -> Result<()> {
    let bytes = encode_leaf(node, self.page_size, self.key_size, self.value_size);
    self.store.write_page(id, &bytes)
  }

  fn read_internal(&mut self, id: PageId) -> Result<InternalNode> {
    let bytes = self.store.read_page(id)?;
    decode_internal(&bytes, self.key_size)
  }

  fn write_internal(&mut self, id: PageId, node: &InternalNode) -> Result<()> {
    let bytes = encode_internal(node, self.page_size, self.key_size);
    self.store.write_page(id, &bytes)
  }

  pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
    if key.len() != self.key_size as usize {
      bail!("key must be exactly {} bytes, got {}", self.key_size, key.len());
    }
    if value.len() != self.value_size as usize {
      bail!("value must be exactly {} bytes, got {}", self.value_size, value.len());
    }
    let root = self.root_page_id;
    let mut node = self.read_leaf(root)?;
    let is_new = match node.entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
      Ok(i) => {
        node.entries[i].1 = value.to_vec();
        false
      }
      Err(i) => {
        node.entries.insert(i, (key.to_vec(), value.to_vec()));
        true
      }
    };
    if node.entries.len() > self.max_leaf_entries {
      bail!("leaf overflow: splitting is not implemented yet (Task 6)");
    }
    self.write_leaf(root, &node)?;
    if is_new {
      self.total_count += 1;
    }
    self.write_superblock()?;
    Ok(())
  }

  pub fn len(&self) -> usize {
    self.total_count as usize
  }

  pub fn is_empty(&self) -> bool {
    self.total_count == 0
  }
}

#[cfg(test)]
impl BPlusTree {
  /// Full in-order traversal via recursive descent — test-only, used to
  /// verify tree structure/correctness directly. The public API's
  /// `scan_forward_bounded`/`scan_backward_bounded` (Task 8) use the
  /// leaf sibling-link walk instead (bounded, not a full descent).
  fn all_entries_for_test(&mut self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let root = self.root_page_id;
    self.collect_all_for_test(root)
  }

  fn collect_all_for_test(&mut self, page_id: PageId) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let bytes = self.store.read_page(page_id)?;
    match page_type(&bytes)? {
      PageType::Leaf => {
        let node = decode_leaf(&bytes, self.key_size, self.value_size)?;
        Ok(node.entries)
      }
      PageType::Internal => {
        let node = decode_internal(&bytes, self.key_size)?;
        let mut all = Vec::new();
        for (_, child, _) in &node.entries {
          all.extend(self.collect_all_for_test(*child)?);
        }
        all.extend(self.collect_all_for_test(node.rightmost_child)?);
        Ok(all)
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use tempfile::NamedTempFile;

  #[test]
  fn create_then_open_round_trips_an_empty_tree() {
    let tmp = NamedTempFile::new().unwrap();
    {
      let tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
      assert_eq!(tree.len(), 0);
      assert!(tree.is_empty());
    }
    let tree = BPlusTree::open(tmp.path()).unwrap();
    assert_eq!(tree.len(), 0);
  }

  #[test]
  fn create_rejects_an_invalid_page_size() {
    let tmp = NamedTempFile::new().unwrap();
    assert!(BPlusTree::create(tmp.path(), 8, 8, 2048).is_err());
  }

  #[test]
  fn insert_then_reopen_preserves_entries() {
    let tmp = NamedTempFile::new().unwrap();
    {
      let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
      tree.insert(&[1; 8], &[10; 8]).unwrap();
      tree.insert(&[2; 8], &[20; 8]).unwrap();
      assert_eq!(tree.len(), 2);
    }
    let mut tree = BPlusTree::open(tmp.path()).unwrap();
    assert_eq!(tree.len(), 2);
    let mut all = tree.all_entries_for_test().unwrap();
    all.sort();
    assert_eq!(all, vec![(vec![1; 8], vec![10; 8]), (vec![2; 8], vec![20; 8])]);
  }

  #[test]
  fn inserting_the_same_key_twice_overwrites_and_does_not_grow_len() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    tree.insert(&[1; 8], &[10; 8]).unwrap();
    tree.insert(&[1; 8], &[99; 8]).unwrap();
    assert_eq!(tree.len(), 1);
    let all = tree.all_entries_for_test().unwrap();
    assert_eq!(all, vec![(vec![1; 8], vec![99; 8])]);
  }

  #[test]
  fn insert_rejects_a_wrong_sized_key_or_value() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    assert!(tree.insert(&[1; 4], &[10; 8]).is_err());
    assert!(tree.insert(&[1; 8], &[10; 4]).is_err());
  }

  #[test]
  fn overflowing_a_single_leaf_before_splitting_is_implemented_returns_an_error() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    // (4096 - 32) / 16 = 254 entries fit; the 255th must fail until Task 6.
    for i in 0..254u64 {
      tree.insert(&i.to_le_bytes(), &i.to_le_bytes()).unwrap();
    }
    let result = tree.insert(&254u64.to_le_bytes(), &254u64.to_le_bytes());
    assert!(result.is_err());
  }
}
