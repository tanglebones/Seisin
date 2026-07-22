//! In-memory decoded representations of a page's content (leaf or
//! internal), and their fixed-size on-disk encoding. Records are
//! fixed-size (`key_size`/`value_size` chosen at tree-creation time), so
//! a page's capacity is a pure function of `page_size`/`key_size`/
//! `value_size`. See the design doc's "On-Disk Format" section.

use anyhow::{bail, Result};

use crate::PageId;

pub(crate) const NODE_HEADER_LEN: usize = 32;
pub(crate) const LEAF_PAGE_TYPE: u8 = 0;

/// How many `(key, value)` entries fit in one leaf page after its
/// header.
pub fn max_leaf_entries(page_size: u32, key_size: u32, value_size: u32) -> usize {
  let record_len = key_size as usize + value_size as usize;
  (page_size as usize - NODE_HEADER_LEN) / record_len
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafNode {
  pub prev: PageId,
  pub next: PageId,
  /// Sorted ascending by key.
  pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

pub fn encode_leaf(node: &LeafNode, page_size: u32, key_size: u32, value_size: u32) -> Vec<u8> {
  let mut buf = vec![0u8; page_size as usize];
  buf[0] = LEAF_PAGE_TYPE;
  buf[8..16].copy_from_slice(&node.prev.to_le_bytes());
  buf[16..24].copy_from_slice(&node.next.to_le_bytes());
  buf[24..28].copy_from_slice(&(node.entries.len() as u32).to_le_bytes());
  let record_len = key_size as usize + value_size as usize;
  let mut offset = NODE_HEADER_LEN;
  for (key, value) in &node.entries {
    buf[offset..offset + key_size as usize].copy_from_slice(key);
    buf[offset + key_size as usize..offset + record_len].copy_from_slice(value);
    offset += record_len;
  }
  buf
}

pub fn decode_leaf(bytes: &[u8], key_size: u32, value_size: u32) -> Result<LeafNode> {
  if bytes.first() != Some(&LEAF_PAGE_TYPE) {
    bail!("page is not a leaf page (wrong page_type byte)");
  }
  let prev = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
  let next = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
  let entry_count = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
  let record_len = key_size as usize + value_size as usize;
  let mut entries = Vec::with_capacity(entry_count);
  let mut offset = NODE_HEADER_LEN;
  for _ in 0..entry_count {
    let key = bytes[offset..offset + key_size as usize].to_vec();
    let value = bytes[offset + key_size as usize..offset + record_len].to_vec();
    entries.push((key, value));
    offset += record_len;
  }
  Ok(LeafNode { prev, next, entries })
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn round_trips_an_empty_leaf() {
    let node = LeafNode { prev: 0, next: 0, entries: vec![] };
    let encoded = encode_leaf(&node, 4096, 8, 8);
    assert_eq!(decode_leaf(&encoded, 8, 8).unwrap(), node);
  }

  #[test]
  fn round_trips_a_leaf_with_entries_and_sibling_links() {
    let node = LeafNode {
      prev: 3,
      next: 7,
      entries: vec![(vec![1; 8], vec![10; 8]), (vec![2; 8], vec![20; 8])],
    };
    let encoded = encode_leaf(&node, 4096, 8, 8);
    assert_eq!(decode_leaf(&encoded, 8, 8).unwrap(), node);
  }

  #[test]
  fn decode_rejects_a_non_leaf_page_type_byte() {
    let mut bytes = vec![0u8; 4096];
    bytes[0] = 1; // not LEAF_PAGE_TYPE
    assert!(decode_leaf(&bytes, 8, 8).is_err());
  }

  #[test]
  fn max_leaf_entries_matches_expected_capacity() {
    // (4096 - 32) / (8 + 8) = 254
    assert_eq!(max_leaf_entries(4096, 8, 8), 254);
  }

  #[test]
  fn max_leaf_entries_shrinks_with_larger_records() {
    // (4096 - 32) / (1000 + 1000) = 2
    assert_eq!(max_leaf_entries(4096, 1000, 1000), 2);
  }
}
