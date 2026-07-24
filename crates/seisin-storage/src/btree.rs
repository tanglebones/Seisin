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
    let empty_leaf = LeafNode {
      prev: NULL_PAGE,
      next: NULL_PAGE,
      entries: vec![],
    };
    store.write_page(
      root_id,
      &encode_leaf(&empty_leaf, page_size, key_size, value_size),
    )?;
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
            bail!(
              "rank {rank} out of bounds for a leaf with {} entries",
              node.entries.len()
            );
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

  pub fn rebuild_from(&mut self, entries: impl Iterator<Item = (Vec<u8>, Vec<u8>)>) -> Result<()> {
    let mut sorted: Vec<(Vec<u8>, Vec<u8>)> = entries.collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    self.store.truncate()?;
    self.next_page_id = 1;
    self.total_count = 0;
    if sorted.is_empty() {
      let root_id = self.allocate_page();
      let empty_leaf = LeafNode {
        prev: NULL_PAGE,
        next: NULL_PAGE,
        entries: vec![],
      };
      self.write_leaf(root_id, &empty_leaf)?;
      self.root_page_id = root_id;
      self.write_superblock()?;
      return Ok(());
    }
    let leaf_chunk_size = self.max_leaf_entries.max(1);
    let leaf_chunks: Vec<&[(Vec<u8>, Vec<u8>)]> = sorted.chunks(leaf_chunk_size).collect();
    let mut level: Vec<(Vec<u8>, PageId, u64)> = Vec::with_capacity(leaf_chunks.len());
    let mut leaf_ids = Vec::with_capacity(leaf_chunks.len());
    for chunk in &leaf_chunks {
      let id = self.allocate_page();
      leaf_ids.push(id);
      level.push((chunk[0].0.clone(), id, chunk.len() as u64));
    }
    for (i, chunk) in leaf_chunks.iter().enumerate() {
      let prev = if i == 0 { NULL_PAGE } else { leaf_ids[i - 1] };
      let next = if i + 1 < leaf_ids.len() {
        leaf_ids[i + 1]
      } else {
        NULL_PAGE
      };
      let node = LeafNode {
        prev,
        next,
        entries: chunk.to_vec(),
      };
      self.write_leaf(leaf_ids[i], &node)?;
    }
    self.total_count = sorted.len() as u64;
    self.root_page_id = if level.len() == 1 {
      level[0].1
    } else {
      self.build_internal_levels(level)?
    };
    self.write_superblock()?;
    Ok(())
  }

  /// Builds internal levels bottom-up from `level` (a sequence of
  /// `(smallest_key_in_subtree, child_page_id, count)` triples covering
  /// the whole key range in ascending order) until exactly one page
  /// remains, returning its id as the new root.
  fn build_internal_levels(&mut self, mut level: Vec<(Vec<u8>, PageId, u64)>) -> Result<PageId> {
    while level.len() > 1 {
      let group_size = self.max_internal_entries + 1; // each internal page holds this many children
      let mut next_level = Vec::new();
      let mut i = 0;
      while i < level.len() {
        let end = (i + group_size).min(level.len());
        let group = &level[i..end];
        // The last child in the group becomes this page's rightmost
        // child (no upper bound needed within the page); every other
        // child gets an entry whose separator is the NEXT child's
        // smallest key (the exclusive upper bound for that child).
        let mut entries: Vec<(Vec<u8>, PageId, u64)> = Vec::with_capacity(group.len() - 1);
        for j in 0..group.len() - 1 {
          entries.push((group[j + 1].0.clone(), group[j].1, group[j].2));
        }
        let rightmost = group.last().unwrap();
        let total_count: u64 = entries.iter().map(|(_, _, c)| *c).sum::<u64>() + rightmost.2;
        let page_id = self.allocate_page();
        let node = InternalNode {
          entries,
          rightmost_child: rightmost.1,
          rightmost_count: rightmost.2,
        };
        self.write_internal(page_id, &node)?;
        next_level.push((group[0].0.clone(), page_id, total_count));
        i = end;
      }
      level = next_level;
    }
    Ok(level[0].1)
  }
}

enum InsertOutcome {
  NoSplit {
    is_new: bool,
  },
  Split {
    is_new: bool,
    separator_key: Vec<u8>,
    new_page_id: PageId,
    new_page_count: u64,
  },
}

impl BPlusTree {
  pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
    if key.len() != self.key_size as usize {
      bail!(
        "key must be exactly {} bytes, got {}",
        self.key_size,
        key.len()
      );
    }
    if value.len() != self.value_size as usize {
      bail!(
        "value must be exactly {} bytes, got {}",
        self.value_size,
        value.len()
      );
    }
    let root = self.root_page_id;
    match self.insert_into(root, key, value)? {
      InsertOutcome::NoSplit { is_new } => {
        if is_new {
          self.total_count += 1;
        }
      }
      InsertOutcome::Split {
        is_new,
        separator_key,
        new_page_id,
        new_page_count,
      } => {
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

  fn insert_into_internal(
    &mut self,
    page_id: PageId,
    key: &[u8],
    value: &[u8],
  ) -> Result<InsertOutcome> {
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
      InsertOutcome::Split {
        is_new,
        separator_key,
        new_page_id,
        new_page_count,
      } => {
        match child_idx {
          Some(i) => {
            // child_id (the smaller-key half, unchanged page id) now
            // gets the NEW, smaller separator; new_page_id (the
            // larger-key half) takes over child_id's OLD separator.
            let old_separator = node.entries[i].0.clone();
            let adjusted_count = node.entries[i].2 + if is_new { 1 } else { 0 } - new_page_count;
            node.entries[i] = (separator_key, child_id, adjusted_count);
            node
              .entries
              .insert(i + 1, (old_separator, new_page_id, new_page_count));
          }
          None => {
            // the rightmost child split: it keeps the smaller keys and
            // becomes a regular bounded entry; new_page_id becomes the
            // new rightmost.
            let adjusted_count = node.rightmost_count + if is_new { 1 } else { 0 } - new_page_count;
            node
              .entries
              .push((separator_key, node.rightmost_child, adjusted_count));
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

  fn insert_into_leaf(
    &mut self,
    page_id: PageId,
    key: &[u8],
    value: &[u8],
  ) -> Result<InsertOutcome> {
    let mut node = self.read_leaf(page_id)?;
    let is_new = match node
      .entries
      .binary_search_by(|(k, _)| k.as_slice().cmp(key))
    {
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
    let new_node = LeafNode {
      prev: page_id,
      next: old_next,
      entries: right_entries,
    };
    if old_next != NULL_PAGE {
      let mut next_node = self.read_leaf(old_next)?;
      next_node.prev = new_page_id;
      self.write_leaf(old_next, &next_node)?;
    }
    self.write_leaf(page_id, &node)?;
    self.write_leaf(new_page_id, &new_node)?;
    Ok(InsertOutcome::Split {
      is_new,
      separator_key,
      new_page_id,
      new_page_count,
    })
  }
}

impl BPlusTree {
  /// Removes `key` if present, returning whether it was found. Fixes
  /// every ancestor internal node's subtree count on the way down (so
  /// rank descent and sampling stay correct) via a presence check
  /// first, then a decrementing descent — counts are only touched when
  /// the key is known to exist. Does NOT merge or rebalance an
  /// underfull page afterward: pages can go sparse (even empty) over a
  /// delete-heavy history — never incorrect, just less space-efficient;
  /// an accepted, documented limitation revisited only if a real
  /// workload shows it matters.
  pub fn remove(&mut self, key: &[u8]) -> Result<bool> {
    if key.len() != self.key_size as usize {
      bail!(
        "key must be exactly {} bytes, got {}",
        self.key_size,
        key.len()
      );
    }
    if !self.contains(key)? {
      return Ok(false);
    }
    let mut page_id = self.root_page_id;
    loop {
      let bytes = self.store.read_page(page_id)?;
      match page_type(&bytes)? {
        PageType::Leaf => {
          let mut node = decode_leaf(&bytes, self.key_size, self.value_size)?;
          if let Ok(i) = node
            .entries
            .binary_search_by(|(k, _)| k.as_slice().cmp(key))
          {
            node.entries.remove(i);
            self.write_leaf(page_id, &node)?;
          }
          break;
        }
        PageType::Internal => {
          let mut node = decode_internal(&bytes, self.key_size)?;
          let child_idx = node.entries.iter().position(|(k, _, _)| key < k.as_slice());
          let next = match child_idx {
            Some(i) => {
              node.entries[i].2 -= 1;
              node.entries[i].1
            }
            None => {
              node.rightmost_count -= 1;
              node.rightmost_child
            }
          };
          self.write_internal(page_id, &node)?;
          page_id = next;
        }
      }
    }
    self.total_count -= 1;
    self.write_superblock()?;
    Ok(true)
  }

  fn contains(&mut self, key: &[u8]) -> Result<bool> {
    let mut page_id = self.root_page_id;
    loop {
      let bytes = self.store.read_page(page_id)?;
      match page_type(&bytes)? {
        PageType::Leaf => {
          let node = decode_leaf(&bytes, self.key_size, self.value_size)?;
          return Ok(
            node
              .entries
              .binary_search_by(|(k, _)| k.as_slice().cmp(key))
              .is_ok(),
          );
        }
        PageType::Internal => {
          let node = decode_internal(&bytes, self.key_size)?;
          page_id = match node.entries.iter().find(|(k, _, _)| key < k.as_slice()) {
            Some((_, child, _)) => *child,
            None => node.rightmost_child,
          };
        }
      }
    }
  }

  /// 0-based ascending rank of `key` (None if absent) — the mirror of
  /// `entry_at_rank`: descend toward the key, accumulating the counts
  /// of every subtree passed on the left, then add the position inside
  /// the leaf.
  pub fn rank_of_key(&mut self, key: &[u8]) -> Result<Option<u64>> {
    if key.len() != self.key_size as usize {
      bail!(
        "key must be exactly {} bytes, got {}",
        self.key_size,
        key.len()
      );
    }
    let mut page_id = self.root_page_id;
    let mut passed: u64 = 0;
    loop {
      let bytes = self.store.read_page(page_id)?;
      match page_type(&bytes)? {
        PageType::Leaf => {
          let node = decode_leaf(&bytes, self.key_size, self.value_size)?;
          return Ok(
            node
              .entries
              .binary_search_by(|(k, _)| k.as_slice().cmp(key))
              .ok()
              .map(|i| passed + i as u64),
          );
        }
        PageType::Internal => {
          let node = decode_internal(&bytes, self.key_size)?;
          let mut next = node.rightmost_child;
          for (separator, child, count) in &node.entries {
            if key < separator.as_slice() {
              next = *child;
              break;
            }
            passed += count;
          }
          page_id = next;
        }
      }
    }
  }

  /// Up to `n` entries starting at ascending rank `rank`: one counted
  /// descent to the holding leaf, then a sibling-link walk — no
  /// per-entry descents, so a neighbors window costs one descent total.
  pub fn scan_from_rank(&mut self, rank: u64, n: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    if n == 0 || rank >= self.total_count {
      return Ok(Vec::new());
    }
    let mut remaining_rank = rank as usize;
    let mut page_id = self.root_page_id;
    loop {
      let bytes = self.store.read_page(page_id)?;
      match page_type(&bytes)? {
        PageType::Leaf => break,
        PageType::Internal => {
          let node = decode_internal(&bytes, self.key_size)?;
          let mut next = node.rightmost_child;
          for (_, child, count) in &node.entries {
            let count = *count as usize;
            if remaining_rank < count {
              next = *child;
              break;
            }
            remaining_rank -= count;
          }
          page_id = next;
        }
      }
    }
    let mut results = Vec::with_capacity(n);
    while page_id != NULL_PAGE && results.len() < n {
      let node = self.read_leaf(page_id)?;
      for entry in node.entries.iter().skip(remaining_rank) {
        if results.len() >= n {
          break;
        }
        results.push(entry.clone());
      }
      remaining_rank = 0;
      page_id = node.next;
    }
    Ok(results)
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
    assert_eq!(
      all,
      vec![(vec![1; 8], vec![10; 8]), (vec![2; 8], vec![20; 8])]
    );
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
    assert_eq!(
      all[0],
      (0u64.to_be_bytes().to_vec(), 0u64.to_be_bytes().to_vec())
    );
    assert_eq!(
      all[299],
      (299u64.to_be_bytes().to_vec(), 299u64.to_be_bytes().to_vec())
    );
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
    tree
      .insert(&1u64.to_be_bytes(), &1u64.to_be_bytes())
      .unwrap();
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
    tree
      .insert(&1u64.to_be_bytes(), &1u64.to_be_bytes())
      .unwrap();
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

  #[test]
  fn remove_deletes_an_existing_key_and_returns_true() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    for i in 0..10u64 {
      tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }
    assert!(tree.remove(&5u64.to_be_bytes()).unwrap());
    assert_eq!(tree.len(), 9);
    let all = tree.all_entries_for_test().unwrap();
    assert!(!all.iter().any(|(k, _)| k == &5u64.to_be_bytes().to_vec()));
  }

  #[test]
  fn remove_on_a_missing_key_is_a_noop_returning_false() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    tree
      .insert(&1u64.to_be_bytes(), &1u64.to_be_bytes())
      .unwrap();
    assert!(!tree.remove(&2u64.to_be_bytes()).unwrap());
    assert_eq!(tree.len(), 1);
  }

  #[test]
  fn remove_rejects_a_wrong_sized_key() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    assert!(tree.remove(&[1u8; 4]).is_err());
  }

  #[test]
  fn remove_keeps_rank_descent_correct_across_a_multi_level_tree() {
    let tmp = NamedTempFile::new().unwrap();
    // key/value_size 1000 on 4096-byte pages: max_leaf_entries=2 — a
    // multi-level tree from a handful of inserts (same trick as the
    // deep-tree insert test above).
    let mut tree = BPlusTree::create(tmp.path(), 1000, 1000, 4096).unwrap();
    let make = |i: u64| {
      let mut k = vec![0u8; 1000];
      k[0..8].copy_from_slice(&i.to_be_bytes());
      k
    };
    for i in 0..50u64 {
      tree.insert(&make(i), &make(i)).unwrap();
    }
    // Remove every even key; odd keys remain at ranks 0..25.
    for i in (0..50u64).step_by(2) {
      assert!(tree.remove(&make(i)).unwrap());
    }
    assert_eq!(tree.len(), 25);
    let sample = tree.sample_by_rank(5).unwrap();
    // ranks 0,5,10,15,20 over remaining keys 1,3,5,... => key = 2*rank+1
    let expected: Vec<u64> = [0u64, 5, 10, 15, 20].iter().map(|r| 2 * r + 1).collect();
    for (entry, want) in sample.iter().zip(expected.iter()) {
      assert_eq!(entry.0, make(*want));
    }
    let bottom = tree.scan_forward_bounded(3).unwrap();
    assert_eq!(bottom[0].0, make(1));
    assert_eq!(bottom[2].0, make(5));
    let top = tree.scan_backward_bounded(1).unwrap();
    assert_eq!(top[0].0, make(49));
  }

  #[test]
  fn interleaved_inserts_and_removes_stay_consistent() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    // Deterministic pseudo-random walk (hand-rolled LCG, no rand dep):
    // mirror every operation into a BTreeMap and compare at the end.
    let mut model = std::collections::BTreeMap::new();
    let mut state = 0x2545F4914F6CDD1Du64;
    for _ in 0..2000 {
      state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
      let key = ((state >> 33) % 500).to_be_bytes();
      if (state >> 20) & 1 == 0 {
        tree.insert(&key, &key).unwrap();
        model.insert(key.to_vec(), key.to_vec());
      } else {
        let expected = model.remove(key.as_slice()).is_some();
        assert_eq!(tree.remove(&key).unwrap(), expected);
      }
    }
    assert_eq!(tree.len(), model.len());
    let mut all = tree.all_entries_for_test().unwrap();
    all.sort();
    let expected: Vec<(Vec<u8>, Vec<u8>)> = model.into_iter().collect();
    assert_eq!(all, expected);
  }

  #[test]
  fn a_tree_with_removes_survives_reopening() {
    let tmp = NamedTempFile::new().unwrap();
    {
      let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
      for i in 0..300u64 {
        tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
      }
      for i in 100..200u64 {
        assert!(tree.remove(&i.to_be_bytes()).unwrap());
      }
    }
    let mut tree = BPlusTree::open(tmp.path()).unwrap();
    assert_eq!(tree.len(), 200);
    assert_eq!(
      tree.scan_backward_bounded(1).unwrap()[0].0,
      299u64.to_be_bytes().to_vec()
    );
  }

  #[test]
  fn rank_of_key_matches_insertion_order_and_misses_return_none() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    for i in 0..300u64 {
      tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }
    assert_eq!(tree.rank_of_key(&0u64.to_be_bytes()).unwrap(), Some(0));
    assert_eq!(tree.rank_of_key(&157u64.to_be_bytes()).unwrap(), Some(157));
    assert_eq!(tree.rank_of_key(&299u64.to_be_bytes()).unwrap(), Some(299));
    assert_eq!(tree.rank_of_key(&999u64.to_be_bytes()).unwrap(), None);
  }

  #[test]
  fn rank_of_key_stays_correct_after_removes_and_across_levels() {
    let tmp = NamedTempFile::new().unwrap();
    // key/value_size 1000 on 4096-byte pages: multi-level tree cheaply.
    let mut tree = BPlusTree::create(tmp.path(), 1000, 1000, 4096).unwrap();
    let make = |i: u64| {
      let mut k = vec![0u8; 1000];
      k[0..8].copy_from_slice(&i.to_be_bytes());
      k
    };
    for i in 0..60u64 {
      tree.insert(&make(i), &make(i)).unwrap();
    }
    for i in (0..60u64).step_by(3) {
      assert!(tree.remove(&make(i)).unwrap());
    }
    let remaining: Vec<u64> = (0..60u64).filter(|i| i % 3 != 0).collect();
    for (rank, i) in remaining.iter().enumerate() {
      assert_eq!(tree.rank_of_key(&make(*i)).unwrap(), Some(rank as u64));
    }
    assert_eq!(tree.rank_of_key(&make(0)).unwrap(), None); // removed
  }

  #[test]
  fn scan_from_rank_returns_entries_from_that_rank_crossing_leaves() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    // 300 entries, max_leaf_entries=254 — rank 250..260 crosses a leaf link.
    for i in 0..300u64 {
      tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }
    let window = tree.scan_from_rank(250, 10).unwrap();
    assert_eq!(window.len(), 10);
    for (offset, (key, _)) in window.iter().enumerate() {
      assert_eq!(key, &(250 + offset as u64).to_be_bytes().to_vec());
    }
  }

  #[test]
  fn scan_from_rank_clamps_at_the_end_and_returns_nothing_past_it() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    for i in 0..10u64 {
      tree.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }
    assert_eq!(tree.scan_from_rank(7, 100).unwrap().len(), 3);
    assert_eq!(tree.scan_from_rank(10, 5).unwrap(), vec![]);
    assert_eq!(tree.scan_from_rank(0, 0).unwrap(), vec![]);
  }

  #[test]
  fn rebuild_from_produces_a_tree_equivalent_to_sequential_inserts() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..300u64)
      .map(|i| (i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec()))
      .collect();
    // Feed rebuild_from in a shuffled (reversed) order — it must sort
    // internally rather than assume sorted input.
    let mut shuffled = entries.clone();
    shuffled.reverse();
    tree.rebuild_from(shuffled.into_iter()).unwrap();
    assert_eq!(tree.len(), 300);
    let mut all = tree.all_entries_for_test().unwrap();
    all.sort();
    assert_eq!(all, entries);
  }

  #[test]
  fn rebuild_from_an_empty_iterator_produces_an_empty_tree() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    tree
      .insert(&1u64.to_be_bytes(), &1u64.to_be_bytes())
      .unwrap();
    tree.rebuild_from(std::iter::empty()).unwrap();
    assert_eq!(tree.len(), 0);
    assert_eq!(tree.all_entries_for_test().unwrap(), vec![]);
  }

  #[test]
  fn rebuild_from_leaves_a_usable_tree_that_still_supports_insert_and_scans() {
    let tmp = NamedTempFile::new().unwrap();
    let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..300u64)
      .map(|i| (i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec()))
      .collect();
    tree.rebuild_from(entries.into_iter()).unwrap();
    tree
      .insert(&999u64.to_be_bytes(), &999u64.to_be_bytes())
      .unwrap();
    assert_eq!(tree.len(), 301);
    let top = tree.scan_backward_bounded(1).unwrap();
    assert_eq!(top[0].0, 999u64.to_be_bytes().to_vec());
    let sample = tree.sample_by_rank(3).unwrap();
    assert_eq!(sample.len(), 3);
  }

  #[test]
  fn rebuild_from_survives_reopening() {
    let tmp = NamedTempFile::new().unwrap();
    {
      let mut tree = BPlusTree::create(tmp.path(), 8, 8, 4096).unwrap();
      let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..300u64)
        .map(|i| (i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec()))
        .collect();
      tree.rebuild_from(entries.into_iter()).unwrap();
    }
    let mut tree = BPlusTree::open(tmp.path()).unwrap();
    assert_eq!(tree.len(), 300);
    let mut all = tree.all_entries_for_test().unwrap();
    all.sort();
    assert_eq!(all.len(), 300);
  }
}
