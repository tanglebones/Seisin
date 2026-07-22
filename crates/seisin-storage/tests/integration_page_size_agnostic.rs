//! Confirms the on-disk format and every operation are agnostic of the
//! configured page size — the same test logic runs against two distinct
//! valid page sizes (4096 and 16384), per the design doc's Testing
//! Strategy.

use seisin_storage::btree::BPlusTree;
use tempfile::NamedTempFile;

fn exercise_a_tree_at(page_size: u32) {
  // to_be_bytes(), not to_le_bytes(): byte-lexicographic order (what the
  // tree sorts by) only matches numeric order for big-endian encodings
  // once keys reach 256 — see btree.rs's tests for the same note.
  let tmp = NamedTempFile::new().unwrap();
  let mut tree = BPlusTree::create(tmp.path(), 8, 8, page_size).unwrap();
  let mut keys: Vec<u64> = (0..500).collect();
  keys.reverse();
  for i in &keys {
    tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }
  assert_eq!(tree.len(), 500);

  let forward = tree.scan_forward_bounded(10).unwrap();
  let expected_forward: Vec<(Vec<u8>, Vec<u8>)> = (0..10u64)
    .map(|i| (i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec()))
    .collect();
  assert_eq!(forward, expected_forward);

  let backward = tree.scan_backward_bounded(10).unwrap();
  let expected_backward: Vec<(Vec<u8>, Vec<u8>)> = (490..500u64)
    .rev()
    .map(|i| (i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec()))
    .collect();
  assert_eq!(backward, expected_backward);

  let sampled = tree.sample_by_rank(5).unwrap();
  assert_eq!(sampled.len(), 5);
  assert_eq!(sampled[0].0, 0u64.to_be_bytes().to_vec());

  // Overwrite an existing key: len() must not grow.
  tree
    .insert(&250u64.to_be_bytes(), &999u64.to_be_bytes())
    .unwrap();
  assert_eq!(tree.len(), 500);

  // rebuild_from round-trips at this page size too.
  let all_entries: Vec<(Vec<u8>, Vec<u8>)> = (0..500u64)
    .map(|i| (i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec()))
    .collect();
  tree.rebuild_from(all_entries.clone().into_iter()).unwrap();
  assert_eq!(tree.len(), 500);
  let forward_after_rebuild = tree.scan_forward_bounded(500).unwrap();
  assert_eq!(forward_after_rebuild, all_entries);
}

#[test]
fn the_engine_behaves_identically_at_the_minimum_page_size() {
  exercise_a_tree_at(4096);
}

#[test]
fn the_engine_behaves_identically_at_a_larger_page_size() {
  exercise_a_tree_at(16384);
}

#[test]
fn create_rejects_a_non_power_of_two_page_size() {
  let tmp = NamedTempFile::new().unwrap();
  assert!(BPlusTree::create(tmp.path(), 8, 8, 5000).is_err());
}

#[test]
fn create_rejects_a_page_size_below_the_minimum() {
  let tmp = NamedTempFile::new().unwrap();
  assert!(BPlusTree::create(tmp.path(), 8, 8, 2048).is_err());
}

#[test]
fn open_rejects_a_file_with_a_corrupted_superblock() {
  let tmp = NamedTempFile::new().unwrap();
  {
    let _tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
  }
  std::fs::write(tmp.path(), [0xFFu8; 4096]).unwrap();
  assert!(BPlusTree::open(tmp.path()).is_err());
}
