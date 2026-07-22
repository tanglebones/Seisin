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
  Ok(LeafNode {
    prev,
    next,
    entries,
  })
}

pub(crate) const INTERNAL_PAGE_TYPE: u8 = 1;

/// How many `(separator_key, child, count)` entries fit in one internal
/// page after its header (the header itself already holds the
/// rightmost child/count, so this is purely about the entries array).
pub fn max_internal_entries(page_size: u32, key_size: u32) -> usize {
  let record_len = key_size as usize + 16; // child: u64, count: u64
  (page_size as usize - NODE_HEADER_LEN) / record_len
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalNode {
  /// Sorted ascending by separator key. `entries[i].1` holds all keys
  /// strictly less than `entries[i].0`.
  pub entries: Vec<(Vec<u8>, PageId, u64)>,
  /// Holds all keys greater than or equal to the last entry's
  /// separator (or literally all keys, if `entries` is empty).
  pub rightmost_child: PageId,
  pub rightmost_count: u64,
}

pub fn encode_internal(node: &InternalNode, page_size: u32, key_size: u32) -> Vec<u8> {
  let mut buf = vec![0u8; page_size as usize];
  buf[0] = INTERNAL_PAGE_TYPE;
  buf[8..16].copy_from_slice(&node.rightmost_child.to_le_bytes());
  buf[16..24].copy_from_slice(&node.rightmost_count.to_le_bytes());
  buf[24..28].copy_from_slice(&(node.entries.len() as u32).to_le_bytes());
  let record_len = key_size as usize + 16;
  let mut offset = NODE_HEADER_LEN;
  for (key, child, count) in &node.entries {
    buf[offset..offset + key_size as usize].copy_from_slice(key);
    let child_offset = offset + key_size as usize;
    buf[child_offset..child_offset + 8].copy_from_slice(&child.to_le_bytes());
    buf[child_offset + 8..child_offset + 16].copy_from_slice(&count.to_le_bytes());
    offset += record_len;
  }
  buf
}

pub fn decode_internal(bytes: &[u8], key_size: u32) -> Result<InternalNode> {
  if bytes.first() != Some(&INTERNAL_PAGE_TYPE) {
    bail!("page is not an internal page (wrong page_type byte)");
  }
  let rightmost_child = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
  let rightmost_count = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
  let entry_count = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
  let record_len = key_size as usize + 16;
  let mut entries = Vec::with_capacity(entry_count);
  let mut offset = NODE_HEADER_LEN;
  for _ in 0..entry_count {
    let key = bytes[offset..offset + key_size as usize].to_vec();
    let child_offset = offset + key_size as usize;
    let child = u64::from_le_bytes(bytes[child_offset..child_offset + 8].try_into().unwrap());
    let count = u64::from_le_bytes(
      bytes[child_offset + 8..child_offset + 16]
        .try_into()
        .unwrap(),
    );
    entries.push((key, child, count));
    offset += record_len;
  }
  Ok(InternalNode {
    entries,
    rightmost_child,
    rightmost_count,
  })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
  Leaf,
  Internal,
}

pub fn page_type(bytes: &[u8]) -> Result<PageType> {
  match bytes.first() {
    Some(&LEAF_PAGE_TYPE) => Ok(PageType::Leaf),
    Some(&INTERNAL_PAGE_TYPE) => Ok(PageType::Internal),
    _ => bail!("unrecognized page type byte"),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn round_trips_an_empty_leaf() {
    let node = LeafNode {
      prev: 0,
      next: 0,
      entries: vec![],
    };
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

  #[test]
  fn round_trips_an_internal_node_with_no_entries() {
    let node = InternalNode {
      entries: vec![],
      rightmost_child: 9,
      rightmost_count: 3,
    };
    let encoded = encode_internal(&node, 4096, 8);
    assert_eq!(decode_internal(&encoded, 8).unwrap(), node);
  }

  #[test]
  fn round_trips_an_internal_node_with_entries() {
    let node = InternalNode {
      entries: vec![(vec![5; 8], 2, 10), (vec![9; 8], 3, 20)],
      rightmost_child: 4,
      rightmost_count: 30,
    };
    let encoded = encode_internal(&node, 4096, 8);
    assert_eq!(decode_internal(&encoded, 8).unwrap(), node);
  }

  #[test]
  fn decode_internal_rejects_a_leaf_page_type_byte() {
    let mut bytes = vec![0u8; 4096];
    bytes[0] = LEAF_PAGE_TYPE;
    assert!(decode_internal(&bytes, 8).is_err());
  }

  #[test]
  fn max_internal_entries_matches_expected_capacity() {
    // (4096 - 32) / (8 + 16) = 169
    assert_eq!(max_internal_entries(4096, 8), 169);
  }

  #[test]
  fn page_type_identifies_leaf_and_internal_pages() {
    let leaf_bytes = encode_leaf(
      &LeafNode {
        prev: 0,
        next: 0,
        entries: vec![],
      },
      4096,
      8,
      8,
    );
    let internal_bytes = encode_internal(
      &InternalNode {
        entries: vec![],
        rightmost_child: 1,
        rightmost_count: 0,
      },
      4096,
      8,
    );
    assert_eq!(page_type(&leaf_bytes).unwrap(), PageType::Leaf);
    assert_eq!(page_type(&internal_bytes).unwrap(), PageType::Internal);
  }

  #[test]
  fn page_type_rejects_an_unrecognized_byte() {
    let mut bytes = vec![0u8; 4096];
    bytes[0] = 0xFF;
    assert!(page_type(&bytes).is_err());
  }
}
