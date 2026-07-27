//! The storage node's append-only delta log: the durable source of
//! truth for datum content. Three record kinds (Full/Delta/Tombstone),
//! each CRC-framed; every append is `fdatasync`ed before the caller
//! acks — the write-before-ack rule, literally. Keys are opaque
//! 16-byte ids (this crate stays dependency-free of DatumId, like the
//! B+Tree). Recovery is a full scan rebuilding the in-memory index,
//! truncating a torn tail at the first CRC/length failure: everything
//! acked was fsynced and precedes any tear; nothing unacked survives.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::crc::crc32;
use crate::delta::{apply, decode_delta, encode_delta, Delta};

const MAGIC: &[u8; 4] = b"SDLG";
// Bumped to 2 when the header gained a 16-byte log id (Storage Tier
// Part C-1), then to 3 when each record gained a u16 replication factor
// (Storage Tier Part C-2). The keep-the-old-decoder n±1 policy binds
// from the first deployed release; there have been none, so an older
// log is simply rejected on open rather than migrated.
const FORMAT_VERSION: u16 = 3;
/// MAGIC(4) ++ FORMAT_VERSION:u16 ++ log_id:[u8;16].
const HEADER_LEN: u64 = 22;

const KIND_FULL: u8 = 1;
const KIND_DELTA: u8 = 2;
const KIND_TOMBSTONE: u8 = 3;

/// Self-rebase thresholds: a chain longer than this, or cumulative
/// delta bytes above half the materialized length, consolidates into a
/// fresh Full record instead of appending another delta — bounding
/// read-time replay and pre-shrinking compaction (Part C).
const MAX_DELTA_CHAIN: usize = 8;

/// A decoded record: (kind, id, replication_factor, body, next_offset).
type RawRecord = (u8, [u8; 16], u16, Vec<u8>, u64);

pub enum PatchOutcome {
  Applied,
  /// The log has no base for this id — the caller must send full bytes
  /// (a delta must never be applied to nothing).
  NeedFull,
}

struct LogRef {
  base_offset: u64,
  deltas: Vec<u64>,
  materialized_len: u32,
  delta_bytes: u32,
  /// The datum's replication factor — placement metadata compute set on
  /// write, reported back via `list_ids`/`n_of` so the type-blind
  /// migration driver can restore the right number of replicas.
  n: u16,
}

pub struct DatumLog {
  file: File,
  index: HashMap<[u8; 16], LogRef>,
  end_offset: u64,
  /// Stamped once at creation, read back on every reopen; never changes
  /// for the life of the log directory. Proves a restarted/moved storage
  /// node holds the same data (see the migration design's log-identity
  /// section).
  log_id: [u8; 16],
}

impl DatumLog {
  /// Opens (creating if absent) and recovers: scans every record,
  /// rebuilding the index; the first CRC/length failure truncates the
  /// log there.
  pub fn open(path: &Path) -> Result<Self> {
    let mut file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .truncate(false)
      .open(path)
      .with_context(|| format!("failed to open datum log {path:?}"))?;
    let file_len = file.metadata()?.len();
    let log_id: [u8; 16] = if file_len == 0 {
      let fresh = *uuid::Uuid::now_v7().as_bytes();
      file.write_all(MAGIC)?;
      file.write_all(&FORMAT_VERSION.to_le_bytes())?;
      file.write_all(&fresh)?;
      file.sync_data()?;
      fresh
    } else {
      let mut header = [0u8; HEADER_LEN as usize];
      file.seek(SeekFrom::Start(0))?;
      file.read_exact(&mut header)?;
      if &header[0..4] != MAGIC {
        bail!("{path:?} is not a datum log (bad magic)");
      }
      let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
      if version != FORMAT_VERSION {
        bail!("datum log {path:?} is format version {version}; this build reads {FORMAT_VERSION}");
      }
      header[6..22].try_into().unwrap()
    };

    let mut log = Self {
      file,
      index: HashMap::new(),
      end_offset: HEADER_LEN,
      log_id,
    };
    log.recover(file_len.max(HEADER_LEN))?;
    Ok(log)
  }

  fn recover(&mut self, file_len: u64) -> Result<()> {
    let mut offset = HEADER_LEN;
    while offset < file_len {
      match self.read_record_at(offset, file_len) {
        Ok(Some((kind, id, n, body, next_offset))) => {
          match kind {
            KIND_FULL => {
              self.index.insert(
                id,
                LogRef {
                  base_offset: offset,
                  deltas: Vec::new(),
                  materialized_len: body.len() as u32,
                  delta_bytes: 0,
                  n,
                },
              );
            }
            KIND_DELTA => {
              // A delta for an unknown id can't be applied — treat it
              // like a tear (it can only arise from external
              // corruption; appends never write one).
              let Some(entry) = self.index.get_mut(&id) else {
                break;
              };
              let Ok(delta) = decode_delta(&body) else {
                break;
              };
              entry.deltas.push(offset);
              entry.delta_bytes += body.len() as u32;
              entry.materialized_len = delta.new_total_len;
              entry.n = n;
            }
            KIND_TOMBSTONE => {
              self.index.remove(&id);
            }
            _ => break, // unknown kind: treat as a tear
          }
          offset = next_offset;
        }
        Ok(None) | Err(_) => break, // torn tail
      }
    }
    self.end_offset = offset;
    self.file.set_len(offset)?; // drop the torn tail, if any
    self.file.seek(SeekFrom::Start(offset))?;
    Ok(())
  }

  /// Reads the record at `offset`, returning `(kind, id, n, body,
  /// next_offset)` — `None`/`Err` for anything torn or corrupt. The
  /// record layout is `[kind:u8][id:16][n:u16 LE][body]`.
  fn read_record_at(&mut self, offset: u64, file_len: u64) -> Result<Option<RawRecord>> {
    if offset + 4 > file_len {
      return Ok(None);
    }
    self.file.seek(SeekFrom::Start(offset))?;
    let mut len_buf = [0u8; 4];
    self.file.read_exact(&mut len_buf)?;
    let payload_len = u32::from_le_bytes(len_buf) as u64; // kind + id + n + body
    if payload_len < 19 || offset + 4 + payload_len + 4 > file_len {
      return Ok(None);
    }
    let mut payload = vec![0u8; payload_len as usize];
    self.file.read_exact(&mut payload)?;
    let mut crc_buf = [0u8; 4];
    self.file.read_exact(&mut crc_buf)?;
    if crc32(&payload) != u32::from_le_bytes(crc_buf) {
      return Ok(None);
    }
    let kind = payload[0];
    let id: [u8; 16] = payload[1..17].try_into().unwrap();
    let n = u16::from_le_bytes(payload[17..19].try_into().unwrap());
    let body = payload[19..].to_vec();
    Ok(Some((kind, id, n, body, offset + 4 + payload_len + 4)))
  }

  /// Appends one record and fsyncs — the caller may ack after this
  /// returns. Returns the record's offset.
  fn append(&mut self, kind: u8, id: [u8; 16], n: u16, body: &[u8]) -> Result<u64> {
    let offset = self.end_offset;
    let mut payload = Vec::with_capacity(19 + body.len());
    payload.push(kind);
    payload.extend_from_slice(&id);
    payload.extend_from_slice(&n.to_le_bytes());
    payload.extend_from_slice(body);
    self.file.seek(SeekFrom::Start(offset))?;
    self.file.write_all(&(payload.len() as u32).to_le_bytes())?;
    self.file.write_all(&payload)?;
    self.file.write_all(&crc32(&payload).to_le_bytes())?;
    self.file.sync_data()?;
    self.end_offset = offset + 4 + payload.len() as u64 + 4;
    Ok(offset)
  }

  pub fn put_full(&mut self, id: [u8; 16], bytes: &[u8], n: u16) -> Result<()> {
    let offset = self.append(KIND_FULL, id, n, bytes)?;
    self.index.insert(
      id,
      LogRef {
        base_offset: offset,
        deltas: Vec::new(),
        materialized_len: bytes.len() as u32,
        delta_bytes: 0,
        n,
      },
    );
    Ok(())
  }

  pub fn put_delta(&mut self, id: [u8; 16], delta: &Delta, n: u16) -> Result<PatchOutcome> {
    if !self.index.contains_key(&id) {
      return Ok(PatchOutcome::NeedFull);
    }
    let body = encode_delta(delta);
    let (chain_len, delta_bytes, materialized_len) = {
      let entry = self.index.get(&id).unwrap();
      (
        entry.deltas.len(),
        entry.delta_bytes as usize,
        entry.materialized_len as usize,
      )
    };
    if chain_len + 1 > MAX_DELTA_CHAIN || delta_bytes + body.len() > materialized_len.max(1) / 2 {
      // Self-rebase: materialize current + delta, consolidate as Full —
      // same single append + fsync, so the same ack latency class.
      let current = self
        .get(id)?
        .expect("index entry implies a materializable value");
      let new_value = apply(&current, delta)?;
      self.put_full(id, &new_value, n)?;
      return Ok(PatchOutcome::Applied);
    }
    let offset = self.append(KIND_DELTA, id, n, &body)?;
    let entry = self.index.get_mut(&id).unwrap();
    entry.deltas.push(offset);
    entry.delta_bytes += body.len() as u32;
    entry.materialized_len = delta.new_total_len;
    entry.n = n;
    Ok(PatchOutcome::Applied)
  }

  pub fn get(&mut self, id: [u8; 16]) -> Result<Option<Vec<u8>>> {
    let Some((base_offset, delta_offsets)) = self
      .index
      .get(&id)
      .map(|r| (r.base_offset, r.deltas.clone()))
    else {
      return Ok(None);
    };
    let end = self.end_offset;
    let (_, _, _, mut value, _) = self
      .read_record_at(base_offset, end)?
      .context("indexed base record unreadable")?;
    for delta_offset in delta_offsets {
      let (_, _, _, body, _) = self
        .read_record_at(delta_offset, end)?
        .context("indexed delta record unreadable")?;
      let delta = decode_delta(&body)?;
      value = apply(&value, &delta)?;
    }
    Ok(Some(value))
  }

  pub fn delete(&mut self, id: [u8; 16]) -> Result<()> {
    self.append(KIND_TOMBSTONE, id, 0, &[])?;
    self.index.remove(&id);
    Ok(())
  }

  /// The stored replication factor for `id`, or `None` if absent.
  pub fn n_of(&self, id: [u8; 16]) -> Option<u16> {
    self.index.get(&id).map(|r| r.n)
  }

  pub fn len(&self) -> usize {
    self.index.len()
  }

  pub fn is_empty(&self) -> bool {
    self.index.is_empty()
  }

  /// The immutable 16-byte identity stamped into this log at creation.
  pub fn log_id(&self) -> [u8; 16] {
    self.log_id
  }

  /// The `(id, replication_factor)` pairs currently present, sorted
  /// ascending by id, strictly greater than `after`, capped at `limit`.
  /// The caller pages by passing the last returned id back as the next
  /// `after`, and knows it is done when a page comes back shorter than
  /// `limit`. The replication factor lets the type-blind migration
  /// driver restore the right number of replicas per datum.
  pub fn list_ids(&self, after: Option<[u8; 16]>, limit: usize) -> Vec<([u8; 16], u16)> {
    let mut ids: Vec<[u8; 16]> = self
      .index
      .keys()
      .copied()
      .filter(|id| after.map(|a| *id > a).unwrap_or(true))
      .collect();
    ids.sort_unstable();
    ids.truncate(limit);
    ids.into_iter().map(|id| (id, self.index[&id].n)).collect()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::delta::diff;
  use tempfile::TempDir;

  fn log_path(dir: &TempDir) -> std::path::PathBuf {
    dir.path().join("datum_log.dlog")
  }

  fn id(n: u8) -> [u8; 16] {
    [n; 16]
  }

  #[test]
  fn put_get_delete_round_trip() {
    let dir = TempDir::new().unwrap();
    let mut log = DatumLog::open(&log_path(&dir)).unwrap();
    log.put_full(id(1), b"hello", 1).unwrap();
    assert_eq!(log.get(id(1)).unwrap(), Some(b"hello".to_vec()));
    assert_eq!(log.len(), 1);
    log.delete(id(1)).unwrap();
    assert_eq!(log.get(id(1)).unwrap(), None);
    assert!(log.is_empty());
  }

  #[test]
  fn delta_chains_materialize_correctly() {
    let dir = TempDir::new().unwrap();
    let mut log = DatumLog::open(&log_path(&dir)).unwrap();
    let v1 = b"aaaaBBBBcccc".to_vec();
    log.put_full(id(1), &v1, 1).unwrap();
    let v2 = b"aaaaXXXXcccc".to_vec();
    assert!(matches!(
      log.put_delta(id(1), &diff(&v1, &v2), 1).unwrap(),
      PatchOutcome::Applied
    ));
    let v3 = b"aaaaXXXXccccdd".to_vec();
    log.put_delta(id(1), &diff(&v2, &v3), 1).unwrap();
    assert_eq!(log.get(id(1)).unwrap(), Some(v3));
  }

  #[test]
  fn a_delta_for_an_unknown_id_is_need_full_and_appends_nothing() {
    let dir = TempDir::new().unwrap();
    let path = log_path(&dir);
    let mut log = DatumLog::open(&path).unwrap();
    let before = std::fs::metadata(&path).unwrap().len();
    assert!(matches!(
      log.put_delta(id(9), &diff(b"a", b"b"), 1).unwrap(),
      PatchOutcome::NeedFull
    ));
    assert_eq!(std::fs::metadata(&path).unwrap().len(), before);
  }

  #[test]
  fn chain_length_rebase_consolidates_into_a_full_record() {
    let dir = TempDir::new().unwrap();
    let path = log_path(&dir);
    let mut log = DatumLog::open(&path).unwrap();
    // A large base so the size threshold never fires first.
    let mut value = vec![7u8; 4096];
    log.put_full(id(1), &value, 1).unwrap();
    for i in 0..12u8 {
      let mut next = value.clone();
      next[100] = i;
      log.put_delta(id(1), &diff(&value, &next), 1).unwrap();
      value = next;
    }
    assert_eq!(log.get(id(1)).unwrap(), Some(value.clone()));
    // Reopen: recovery rebuilds; the consolidating Full must have reset
    // the chain (otherwise replay would need > MAX_DELTA_CHAIN deltas).
    drop(log);
    let mut log = DatumLog::open(&path).unwrap();
    assert_eq!(log.get(id(1)).unwrap(), Some(value));
    let entry_chain = log.index.get(&id(1)).unwrap().deltas.len();
    assert!(entry_chain <= MAX_DELTA_CHAIN, "chain {entry_chain}");
  }

  #[test]
  fn cumulative_size_rebase_triggers_on_large_deltas() {
    let dir = TempDir::new().unwrap();
    let mut log = DatumLog::open(&log_path(&dir)).unwrap();
    let value = vec![1u8; 100];
    log.put_full(id(1), &value, 1).unwrap();
    // One delta bigger than half the value: must consolidate, not chain.
    let mut new = value.clone();
    for byte in new.iter_mut().take(80) {
      *byte = 2;
    }
    log.put_delta(id(1), &diff(&value, &new), 1).unwrap();
    assert_eq!(log.index.get(&id(1)).unwrap().deltas.len(), 0);
    assert_eq!(log.get(id(1)).unwrap(), Some(new));
  }

  #[test]
  fn reopen_recovers_the_full_index() {
    let dir = TempDir::new().unwrap();
    let path = log_path(&dir);
    {
      let mut log = DatumLog::open(&path).unwrap();
      log.put_full(id(1), b"one", 1).unwrap();
      log.put_full(id(2), b"two", 1).unwrap();
      log.delete(id(1)).unwrap();
    }
    let mut log = DatumLog::open(&path).unwrap();
    assert_eq!(log.get(id(1)).unwrap(), None);
    assert_eq!(log.get(id(2)).unwrap(), Some(b"two".to_vec()));
  }

  #[test]
  fn a_torn_tail_is_truncated_and_the_acked_prefix_survives() {
    let dir = TempDir::new().unwrap();
    let path = log_path(&dir);
    {
      let mut log = DatumLog::open(&path).unwrap();
      log.put_full(id(1), b"acked", 1).unwrap();
    }
    // Simulate a crash mid-append: garbage after the acked record.
    {
      let mut file = OpenOptions::new().append(true).open(&path).unwrap();
      file.write_all(&[0xAB; 7]).unwrap(); // not even a full record
    }
    let mut log = DatumLog::open(&path).unwrap();
    assert_eq!(log.get(id(1)).unwrap(), Some(b"acked".to_vec()));
    // The tail was truncated: a fresh append lands cleanly and survives
    // another reopen.
    log.put_full(id(2), b"after", 1).unwrap();
    drop(log);
    let mut log = DatumLog::open(&path).unwrap();
    assert_eq!(log.get(id(2)).unwrap(), Some(b"after".to_vec()));
  }

  #[test]
  fn a_corrupted_record_body_truncates_from_that_record() {
    let dir = TempDir::new().unwrap();
    let path = log_path(&dir);
    let second_offset;
    {
      let mut log = DatumLog::open(&path).unwrap();
      log.put_full(id(1), b"first", 1).unwrap();
      second_offset = log.end_offset;
      log.put_full(id(2), b"second", 1).unwrap();
    }
    // Flip a byte inside the second record's body.
    {
      let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
      // record = [len:4][kind:1][id:16][n:2][body] — body starts at +4+19.
      file.seek(SeekFrom::Start(second_offset + 4 + 19)).unwrap();
      file.write_all(&[0xFF]).unwrap();
    }
    let mut log = DatumLog::open(&path).unwrap();
    assert_eq!(log.get(id(1)).unwrap(), Some(b"first".to_vec()));
    assert_eq!(log.get(id(2)).unwrap(), None); // truncated away
  }

  #[test]
  fn log_id_is_stamped_at_creation_and_stable_across_reopen() {
    let dir = TempDir::new().unwrap();
    let path = log_path(&dir);
    let first_id = {
      let log = DatumLog::open(&path).unwrap();
      log.log_id()
    };
    // A freshly-created log has a non-zero id...
    assert_ne!(first_id, [0u8; 16]);
    // ...that survives a reopen unchanged (it lives in the header).
    let reopened = DatumLog::open(&path).unwrap();
    assert_eq!(reopened.log_id(), first_id);
    // ...and a different log directory gets a distinct id.
    let other_dir = TempDir::new().unwrap();
    let other = DatumLog::open(&log_path(&other_dir)).unwrap();
    assert_ne!(other.log_id(), first_id);
  }

  #[test]
  fn list_ids_pages_in_ascending_order() {
    let dir = TempDir::new().unwrap();
    let mut log = DatumLog::open(&log_path(&dir)).unwrap();
    for n in [3u8, 1, 4, 2] {
      log.put_full(id(n), b"v", 1).unwrap();
    }
    // Page with limit 2, walking `after`, gathering the full set.
    let mut seen: Vec<[u8; 16]> = Vec::new();
    let mut after = None;
    loop {
      let page = log.list_ids(after, 2);
      if page.is_empty() {
        break;
      }
      assert!(page.len() <= 2);
      after = Some(page.last().unwrap().0);
      seen.extend(page.into_iter().map(|(id, _)| id));
    }
    assert_eq!(seen, vec![id(1), id(2), id(3), id(4)]); // ascending, no dupes/gaps
                                                        // A tombstoned id drops out of enumeration.
    log.delete(id(2)).unwrap();
    let ids: Vec<[u8; 16]> = log
      .list_ids(None, 10)
      .into_iter()
      .map(|(id, _)| id)
      .collect();
    assert_eq!(ids, vec![id(1), id(3), id(4)]);
  }

  #[test]
  fn n_round_trips_through_a_reopen_and_list_ids() {
    let dir = TempDir::new().unwrap();
    let path = log_path(&dir);
    {
      let mut log = DatumLog::open(&path).unwrap();
      log.put_full(id(1), b"v", 3).unwrap();
      log.put_full(id(2), b"w", 1).unwrap();
      // A delta write keeps the id's replication factor.
      log.put_delta(id(1), &diff(b"v", b"z"), 3).unwrap();
      assert_eq!(log.n_of(id(1)), Some(3));
      assert_eq!(log.list_ids(None, 10), vec![(id(1), 3), (id(2), 1)]);
    }
    // The stored factor survives a recovery scan.
    let log = DatumLog::open(&path).unwrap();
    assert_eq!(log.n_of(id(1)), Some(3));
    assert_eq!(log.n_of(id(2)), Some(1));
    assert_eq!(log.n_of(id(9)), None);
  }

  #[test]
  fn wrong_magic_and_wrong_version_are_loud_open_errors() {
    let dir = TempDir::new().unwrap();
    let path = log_path(&dir);
    std::fs::write(&path, b"NOPE\x01\x00").unwrap();
    assert!(DatumLog::open(&path).is_err());
    let mut bytes = MAGIC.to_vec();
    bytes.extend_from_slice(&99u16.to_le_bytes());
    std::fs::write(&path, bytes).unwrap();
    assert!(DatumLog::open(&path).is_err());
  }
}
