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

  pub fn len(&self) -> usize {
    self.total_count as usize
  }

  pub fn is_empty(&self) -> bool {
    self.total_count == 0
  }

  pub fn scan_forward_bounded(&mut self, n: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    if n == 0 {
      return Ok(Vec::new());
    }
    let mut page_id = self.leftmost_leaf_id()?;
    let mut results = Vec::with_capacity(n.min(self.total_count as usize));
    while page_id != NULL_PAGE && results.len() < n {
      let node = self.read_leaf(page_id)?;
      for entry in &node.entries {
        if results.len() >= n {
          break;
        }
        results.push(entry.clone());
      }
      page_id = node.next;
    }
    Ok(results)
  }

  pub fn scan_backward_bounded(&mut self, n: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    if n == 0 {
      return Ok(Vec::new());
    }
    let mut page_id = self.rightmost_leaf_id()?;
    let mut results = Vec::with_capacity(n.min(self.total_count as usize));
    while page_id != NULL_PAGE && results.len() < n {
      let node = self.read_leaf(page_id)?;
      for entry in node.entries.iter().rev() {
        if results.len() >= n {
          break;
        }
        results.push(entry.clone());
      }
      page_id = node.prev;
    }
    Ok(results)
  }

  fn leftmost_leaf_id(&mut self) -> Result<PageId> {
    let mut page_id = self.root_page_id;
    loop {
      let bytes = self.store.read_page(page_id)?;
      match page_type(&bytes)? {
        PageType::Leaf => return Ok(page_id),
        PageType::Internal => {
          let node = decode_internal(&bytes, self.key_size)?;
          page_id = match node.entries.first() {
            Some((_, child, _)) => *child,
            None => node.rightmost_child,
          };
        }
      }
    }
  }

  fn rightmost_leaf_id(&mut self) -> Result<PageId> {
    let mut page_id = self.root_page_id;
    loop {
      let bytes = self.store.read_page(page_id)?;
      match page_type(&bytes)? {
        PageType::Leaf => return Ok(page_id),
        PageType::Internal => {
          let node = decode_internal(&bytes, self.key_size)?;
          page_id = node.rightmost_child;
        }
      }
    }
  }

  pub fn sample_by_rank(&mut self, k: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let total = self.total_count as usize;
    if total == 0 || k == 0 {
      return Ok(Vec::new());
    }
    let mut results = Vec::with_capacity(k);
    for i in 0..k {
      let rank = (i * total) / k;
      results.push(self.entry_at_rank(rank)?);
    }
    Ok(results)
  }

  fn entry_at_rank(&mut self, mut rank: usize) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut page_id = self.root_page_id;
    loop {
      let bytes = self.store.read_page(page_id)?;
      match page_type(&bytes)? {
        PageType::Leaf => {
          let node = decode_leaf(&bytes, self.key_size, self.value_size)?;
          if rank >= node.entries.len() {
            bail!("rank {rank} out of bounds for a leaf with {} entries", node.entries.len());
          }
          return Ok(node.entries[rank].clone());
        }
        PageType::Internal => {
          let node = decode_internal(&bytes, self.key_size)?;
          let mut descended = false;
          for (_, child, count) in &node.entries {
            let count = *count as usize;
            if rank < count {
              page_id = *child;
              descended = true;
              break;
            }
            rank -= count;
          }
          if !descended {
            page_id = node.rightmost_child;
          }
        }
      }
    }
  }
}

enum InsertOutcome {
  NoSplit { is_new: bool },
  Split { is_new: bool, separator_key: Vec<u8>, new_page_id: PageId, new_page_count: u64 },
}

impl BPlusTree {
  pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
    if key.len() != self.key_size as usize {
      bail!("key must be exactly {} bytes, got {}", self.key_size, key.len());
    }
    if value.len() != self.value_size as usize {
      bail!("value must be exactly {} bytes, got {}", self.value_size, value.len());
    }
    let root = self.root_page_id;
    match self.insert_into(root, key, value)? {
      InsertOutcome::NoSplit { is_new } => {
        if is_new {
          self.total_count += 1;
        }
      }
      InsertOutcome::Split { is_new, separator_key, new_page_id, new_page_count } => {
        let old_root_count = self.total_count + if is_new { 1 } else { 0 } - new_page_count;
        let new_root_id = self.allocate_page();
        let new_root = InternalNode {
          entries: vec![(separator_key, root, old_root_count)],
          rightmost_child: new_page_id,
          rightmost_count: new_page_count,
        };
        self.write_internal(new_root_id, &new_root)?;
        self.root_page_id = new_root_id;
        if is_new {
          self.total_count += 1;
        }
      }
    }
    self.write_superblock()?;
    Ok(())
  }

  fn insert_into(&mut self, page_id: PageId, key: &[u8], value: &[u8]) -> Result<InsertOutcome> {
    let bytes = self.store.read_page(page_id)?;
    match page_type(&bytes)? {
      PageType::Leaf => self.insert_into_leaf(page_id, key, value),
      PageType::Internal => self.insert_into_internal(page_id, key, value),
    }
  }

  fn insert_into_internal(&mut self, page_id: PageId, key: &[u8], value: &[u8]) -> Result<InsertOutcome> {
    let mut node = self.read_internal(page_id)?;
    let child_idx = node.entries.iter().position(|(k, _, _)| key < k.as_slice());
    let child_id = match child_idx {
      Some(i) => node.entries[i].1,
      None => node.rightmost_child,
    };
    let result = self.insert_into(child_id, key, value)?;
    match result {
      InsertOutcome::NoSplit { is_new } => {
        if is_new {
          match child_idx {
            Some(i) => node.entries[i].2 += 1,
            None => node.rightmost_count += 1,
          }
          self.write_internal(page_id, &node)?;
        }
        Ok(InsertOutcome::NoSplit { is_new })
      }
      InsertOutcome::Split { is_new, separator_key, new_page_id, new_page_count } => {
        match child_idx {
          Some(i) => {
            // child_id (the smaller-key half, unchanged page id) now
            // gets the NEW, smaller separator; new_page_id (the
            // larger-key half) takes over child_id's OLD separator.
            let old_separator = node.entries[i].0.clone();
            let adjusted_count = node.entries[i].2 + if is_new { 1 } else { 0 } - new_page_count;
            node.entries[i] = (separator_key, child_id, adjusted_count);
            node.entries.insert(i + 1, (old_separator, new_page_id, new_page_count));
          }
          None => {
            // the rightmost child split: it keeps the smaller keys and
            // becomes a regular bounded entry; new_page_id becomes the
            // new rightmost.
            let adjusted_count = node.rightmost_count + if is_new { 1 } else { 0 } - new_page_count;
            node.entries.push((separator_key, node.rightmost_child, adjusted_count));
            node.rightmost_child = new_page_id;
            node.rightmost_count = new_page_count;
          }
        }
        if node.entries.len() <= self.max_internal_entries {
          self.write_internal(page_id, &node)?;
          return Ok(InsertOutcome::NoSplit { is_new });
        }
        let mid = node.entries.len() / 2;
        let promoted_key = node.entries[mid].0.clone();
        let promoted_child = node.entries[mid].1;
        let promoted_count = node.entries[mid].2;
        let right_entries = node.entries.split_off(mid + 1);
        node.entries.truncate(mid);
        let new_page_id2 = self.allocate_page();
        let new_internal_count: u64 =
          right_entries.iter().map(|(_, _, c)| *c).sum::<u64>() + node.rightmost_count;
        let new_node = InternalNode {
          entries: right_entries,
          rightmost_child: node.rightmost_child,
          rightmost_count: node.rightmost_count,
        };
        node.rightmost_child = promoted_child;
        node.rightmost_count = promoted_count;
        self.write_internal(page_id, &node)?;
        self.write_internal(new_page_id2, &new_node)?;
        Ok(InsertOutcome::Split {
          is_new,
          separator_key: promoted_key,
          new_page_id: new_page_id2,
          new_page_count: new_internal_count,
        })
      }
    }
  }

  fn insert_into_leaf(&mut self, page_id: PageId, key: &[u8], value: &[u8]) -> Result<InsertOutcome> {
    let mut node = self.read_leaf(page_id)?;
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
    if node.entries.len() <= self.max_leaf_entries {
      self.write_leaf(page_id, &node)?;
      return Ok(InsertOutcome::NoSplit { is_new });
    }
    let mid = node.entries.len() / 2;
    let right_entries = node.entries.split_off(mid);
    let new_page_id = self.allocate_page();
    let separator_key = right_entries[0].0.clone();
    let new_page_count = right_entries.len() as u64;
    let old_next = node.next;
    node.next = new_page_id;
    let new_node = LeafNode { prev: page_id, next: old_next, entries: right_entries };
    if old_next != NULL_PAGE {
      let mut next_node = self.read_leaf(old_next)?;
      next_node.prev = new_page_id;
      self.write_leaf(old_next, &next_node)?;
    }
    self.write_leaf(page_id, &node)?;
    self.write_leaf(new_page_id, &new_node)?;
    Ok(InsertOutcome::Split { is_new, separator_key, new_page_id, new_page_count })
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
  fn overflowing_a_single_leaf_splits_into_two_leaves_and_a_new_internal_root() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    // (4096 - 32) / 16 = 254 fit in one leaf; insert one more to force a split.
    for i in 0..255u64 {
      tree.insert(&i.to_le_bytes(), &i.to_le_bytes()).unwrap();
    }
    assert_eq!(tree.len(), 255);
    let mut all = tree.all_entries_for_test().unwrap();
    all.sort();
    let expected: Vec<(Vec<u8>, Vec<u8>)> = (0..255u64)
      .map(|i| (i.to_le_bytes().to_vec(), i.to_le_bytes().to_vec()))
      .collect();
    assert_eq!(all, expected);
  }

  #[test]
  fn a_deep_tree_with_small_capacity_pages_stays_correct_across_many_splits() {
    let tmp = NamedTempFile::new().unwrap();
    // key_size=value_size=1000 on a 4096-byte page gives max_leaf_entries=2
    // and max_internal_entries=(4096-32)/(1000+16)=4 — a handful of inserts
    // is enough to force splits several levels deep.
    let mut tree = BPlusTree::create(tmp.path(), 1000, 1000, 4096).unwrap();
    let mut keys: Vec<u64> = (0..200).collect();
    keys.reverse(); // deterministic non-ascending insertion order
    for i in &keys {
      let mut key = vec![0u8; 1000];
      key[0..8].copy_from_slice(&i.to_le_bytes());
      let mut value = vec![0u8; 1000];
      value[0..8].copy_from_slice(&i.to_le_bytes());
      tree.insert(&key, &value).unwrap();
    }
    assert_eq!(tree.len(), 200);
    let mut all = tree.all_entries_for_test().unwrap();
    all.sort();
    for (i, (key, value)) in all.iter().enumerate() {
      let mut expected_key = vec![0u8; 1000];
      expected_key[0..8].copy_from_slice(&(i as u64).to_le_bytes());
      assert_eq!(key, &expected_key);
      assert_eq!(value, &expected_key); // value mirrors key in this test
    }
  }

  #[test]
  fn upserting_an_existing_key_in_a_deep_tree_does_not_change_len() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 1000, 1000, 4096).unwrap();
    for i in 0..200u64 {
      let mut key = vec![0u8; 1000];
      key[0..8].copy_from_slice(&i.to_le_bytes());
      tree.insert(&key, &vec![1u8; 1000]).unwrap();
    }
    let mut key = vec![0u8; 1000];
    key[0..8].copy_from_slice(&50u64.to_le_bytes());
    tree.insert(&key, &vec![2u8; 1000]).unwrap();
    assert_eq!(tree.len(), 200);
    let all = tree.all_entries_for_test().unwrap();
    let updated = all.iter().find(|(k, _)| k == &key).unwrap();
    assert_eq!(updated.1, vec![2u8; 1000]);
  }

  #[test]
  fn a_split_tree_survives_reopening() {
    // Keys use to_be_bytes(), not to_le_bytes(): byte-lexicographic
    // order (what the tree sorts by) only matches numeric order for
    // big-endian encodings once values reach 256 and need more than one
    // significant byte.
    let tmp = NamedTempFile::new().unwrap();
    {
      let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
      for i in 0..300u64 {
        tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
      }
    }
    let mut tree = BPlusTree::open(tmp.path()).unwrap();
    assert_eq!(tree.len(), 300);
    let mut all = tree.all_entries_for_test().unwrap();
    all.sort();
    assert_eq!(all.len(), 300);
    assert_eq!(all[0], (0u64.to_be_bytes().to_vec(), 0u64.to_be_bytes().to_vec()));
    assert_eq!(all[299], (299u64.to_be_bytes().to_vec(), 299u64.to_be_bytes().to_vec()));
  }

  #[test]
  fn inserting_out_of_order_still_produces_a_correctly_sorted_tree() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    let mut keys: Vec<u64> = (0..300).collect();
    keys.reverse();
    for i in &keys {
      tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }
    let mut all = tree.all_entries_for_test().unwrap();
    all.sort();
    let expected: Vec<(Vec<u8>, Vec<u8>)> = (0..300u64)
      .map(|i| (i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec()))
      .collect();
    assert_eq!(all, expected);
  }

  #[test]
  fn scan_forward_bounded_returns_the_smallest_n_entries_in_ascending_order() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    let mut keys: Vec<u64> = (0..300).collect();
    keys.reverse();
    for i in &keys {
      tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }
    let result = tree.scan_forward_bounded(5).unwrap();
    let expected: Vec<(Vec<u8>, Vec<u8>)> = (0..5u64)
      .map(|i| (i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec()))
      .collect();
    assert_eq!(result, expected);
  }

  #[test]
  fn scan_backward_bounded_returns_the_largest_n_entries_in_descending_order() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    for i in 0..300u64 {
      tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }
    let result = tree.scan_backward_bounded(5).unwrap();
    let expected: Vec<(Vec<u8>, Vec<u8>)> = (295..300u64)
      .rev()
      .map(|i| (i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec()))
      .collect();
    assert_eq!(result, expected);
  }

  #[test]
  fn scan_forward_bounded_with_n_larger_than_len_returns_everything() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    for i in 0..3u64 {
      tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }
    let result = tree.scan_forward_bounded(1000).unwrap();
    assert_eq!(result.len(), 3);
  }

  #[test]
  fn scan_forward_bounded_on_an_empty_tree_returns_nothing() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    assert_eq!(tree.scan_forward_bounded(10).unwrap(), vec![]);
    assert_eq!(tree.scan_backward_bounded(10).unwrap(), vec![]);
  }

  #[test]
  fn scan_bounded_with_n_zero_returns_nothing() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    tree.insert(&1u64.to_be_bytes(), &1u64.to_be_bytes()).unwrap();
    assert_eq!(tree.scan_forward_bounded(0).unwrap(), vec![]);
    assert_eq!(tree.scan_backward_bounded(0).unwrap(), vec![]);
  }

  #[test]
  fn scan_forward_bounded_spans_multiple_leaves_via_sibling_links() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    // 300 entries with max_leaf_entries=254 forces at least one split,
    // so this exercises walking across a leaf sibling link.
    for i in 0..300u64 {
      tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }
    let result = tree.scan_forward_bounded(260).unwrap();
    assert_eq!(result.len(), 260);
    assert_eq!(result[0].0, 0u64.to_be_bytes().to_vec());
    assert_eq!(result[259].0, 259u64.to_be_bytes().to_vec());
  }

  #[test]
  fn sample_by_rank_returns_entries_at_the_expected_evenly_spaced_ranks() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    for i in 0..300u64 {
      tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }
    // ranks = i * 300 / 5 for i in 0..5 => 0, 60, 120, 180, 240
    let result = tree.sample_by_rank(5).unwrap();
    let expected_ranks = [0u64, 60, 120, 180, 240];
    assert_eq!(result.len(), 5);
    for (entry, rank) in result.iter().zip(expected_ranks.iter()) {
      assert_eq!(entry.0, rank.to_be_bytes().to_vec());
    }
  }

  #[test]
  fn sample_by_rank_on_an_empty_tree_returns_nothing() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    assert_eq!(tree.sample_by_rank(5).unwrap(), vec![]);
  }

  #[test]
  fn sample_by_rank_with_k_zero_returns_nothing() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    tree.insert(&1u64.to_be_bytes(), &1u64.to_be_bytes()).unwrap();
    assert_eq!(tree.sample_by_rank(0).unwrap(), vec![]);
  }

  #[test]
  fn sample_by_rank_works_across_a_multi_level_tree() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 1000, 1000, 4096).unwrap();
    for i in 0..200u64 {
      let mut key = vec![0u8; 1000];
      key[0..8].copy_from_slice(&i.to_le_bytes());
      tree.insert(&key, &key).unwrap();
    }
    let result = tree.sample_by_rank(4).unwrap();
    let expected_ranks = [0u64, 50, 100, 150];
    assert_eq!(result.len(), 4);
    for (entry, rank) in result.iter().zip(expected_ranks.iter()) {
      let mut expected_key = vec![0u8; 1000];
      expected_key[0..8].copy_from_slice(&rank.to_le_bytes());
      assert_eq!(entry.0, expected_key);
    }
  }
}
