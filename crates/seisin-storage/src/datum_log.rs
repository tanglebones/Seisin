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
const FORMAT_VERSION: u16 = 1;
const HEADER_LEN: u64 = 6;

const KIND_FULL: u8 = 1;
const KIND_DELTA: u8 = 2;
const KIND_TOMBSTONE: u8 = 3;

/// Self-rebase thresholds: a chain longer than this, or cumulative
/// delta bytes above half the materialized length, consolidates into a
/// fresh Full record instead of appending another delta — bounding
/// read-time replay and pre-shrinking compaction (Part C).
const MAX_DELTA_CHAIN: usize = 8;

/// A decoded record: (kind, id, body, next_offset).
type RawRecord = (u8, [u8; 16], Vec<u8>, u64);

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
}

pub struct DatumLog {
  file: File,
  index: HashMap<[u8; 16], LogRef>,
  end_offset: u64,
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
    if file_len == 0 {
      file.write_all(MAGIC)?;
      file.write_all(&FORMAT_VERSION.to_le_bytes())?;
      file.sync_data()?;
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
    }

    let mut log = Self {
      file,
      index: HashMap::new(),
      end_offset: HEADER_LEN,
    };
    log.recover(file_len.max(HEADER_LEN))?;
    Ok(log)
  }

  fn recover(&mut self, file_len: u64) -> Result<()> {
    let mut offset = HEADER_LEN;
    while offset < file_len {
      match self.read_record_at(offset, file_len) {
        Ok(Some((kind, id, body, next_offset))) => {
          match kind {
            KIND_FULL => {
              self.index.insert(
                id,
                LogRef {
                  base_offset: offset,
                  deltas: Vec::new(),
                  materialized_len: body.len() as u32,
                  delta_bytes: 0,
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

  /// Reads the record at `offset`, returning `(kind, id, body,
  /// next_offset)` — `None`/`Err` for anything torn or corrupt.
  fn read_record_at(&mut self, offset: u64, file_len: u64) -> Result<Option<RawRecord>> {
    if offset + 4 > file_len {
      return Ok(None);
    }
    self.file.seek(SeekFrom::Start(offset))?;
    let mut len_buf = [0u8; 4];
    self.file.read_exact(&mut len_buf)?;
    let payload_len = u32::from_le_bytes(len_buf) as u64; // kind + id + body
    if payload_len < 17 || offset + 4 + payload_len + 4 > file_len {
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
    let body = payload[17..].to_vec();
    Ok(Some((kind, id, body, offset + 4 + payload_len + 4)))
  }

  /// Appends one record and fsyncs — the caller may ack after this
  /// returns. Returns the record's offset.
  fn append(&mut self, kind: u8, id: [u8; 16], body: &[u8]) -> Result<u64> {
    let offset = self.end_offset;
    let mut payload = Vec::with_capacity(17 + body.len());
    payload.push(kind);
    payload.extend_from_slice(&id);
    payload.extend_from_slice(body);
    self.file.seek(SeekFrom::Start(offset))?;
    self.file.write_all(&(payload.len() as u32).to_le_bytes())?;
    self.file.write_all(&payload)?;
    self.file.write_all(&crc32(&payload).to_le_bytes())?;
    self.file.sync_data()?;
    self.end_offset = offset + 4 + payload.len() as u64 + 4;
    Ok(offset)
  }

  pub fn put_full(&mut self, id: [u8; 16], bytes: &[u8]) -> Result<()> {
    let offset = self.append(KIND_FULL, id, bytes)?;
    self.index.insert(
      id,
      LogRef {
        base_offset: offset,
        deltas: Vec::new(),
        materialized_len: bytes.len() as u32,
        delta_bytes: 0,
      },
    );
    Ok(())
  }

  pub fn put_delta(&mut self, id: [u8; 16], delta: &Delta) -> Result<PatchOutcome> {
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
      self.put_full(id, &new_value)?;
      return Ok(PatchOutcome::Applied);
    }
    let offset = self.append(KIND_DELTA, id, &body)?;
    let entry = self.index.get_mut(&id).unwrap();
    entry.deltas.push(offset);
    entry.delta_bytes += body.len() as u32;
    entry.materialized_len = delta.new_total_len;
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
    let (_, _, mut value, _) = self
      .read_record_at(base_offset, end)?
      .context("indexed base record unreadable")?;
    for delta_offset in delta_offsets {
      let (_, _, body, _) = self
        .read_record_at(delta_offset, end)?
        .context("indexed delta record unreadable")?;
      let delta = decode_delta(&body)?;
      value = apply(&value, &delta)?;
    }
    Ok(Some(value))
  }

  pub fn delete(&mut self, id: [u8; 16]) -> Result<()> {
    self.append(KIND_TOMBSTONE, id, &[])?;
    self.index.remove(&id);
    Ok(())
  }

  pub fn len(&self) -> usize {
    self.index.len()
  }

  pub fn is_empty(&self) -> bool {
    self.index.is_empty()
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
    log.put_full(id(1), b"hello").unwrap();
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
    log.put_full(id(1), &v1).unwrap();
    let v2 = b"aaaaXXXXcccc".to_vec();
    assert!(matches!(
      log.put_delta(id(1), &diff(&v1, &v2)).unwrap(),
      PatchOutcome::Applied
    ));
    let v3 = b"aaaaXXXXccccdd".to_vec();
    log.put_delta(id(1), &diff(&v2, &v3)).unwrap();
    assert_eq!(log.get(id(1)).unwrap(), Some(v3));
  }

  #[test]
  fn a_delta_for_an_unknown_id_is_need_full_and_appends_nothing() {
    let dir = TempDir::new().unwrap();
    let path = log_path(&dir);
    let mut log = DatumLog::open(&path).unwrap();
    let before = std::fs::metadata(&path).unwrap().len();
    assert!(matches!(
      log.put_delta(id(9), &diff(b"a", b"b")).unwrap(),
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
    log.put_full(id(1), &value).unwrap();
    for i in 0..12u8 {
      let mut next = value.clone();
      next[100] = i;
      log.put_delta(id(1), &diff(&value, &next)).unwrap();
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
    log.put_full(id(1), &value).unwrap();
    // One delta bigger than half the value: must consolidate, not chain.
    let mut new = value.clone();
    for byte in new.iter_mut().take(80) {
      *byte = 2;
    }
    log.put_delta(id(1), &diff(&value, &new)).unwrap();
    assert_eq!(log.index.get(&id(1)).unwrap().deltas.len(), 0);
    assert_eq!(log.get(id(1)).unwrap(), Some(new));
  }

  #[test]
  fn reopen_recovers_the_full_index() {
    let dir = TempDir::new().unwrap();
    let path = log_path(&dir);
    {
      let mut log = DatumLog::open(&path).unwrap();
      log.put_full(id(1), b"one").unwrap();
      log.put_full(id(2), b"two").unwrap();
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
      log.put_full(id(1), b"acked").unwrap();
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
    log.put_full(id(2), b"after").unwrap();
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
      log.put_full(id(1), b"first").unwrap();
      second_offset = log.end_offset;
      log.put_full(id(2), b"second").unwrap();
    }
    // Flip a byte inside the second record's body.
    {
      let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
      file.seek(SeekFrom::Start(second_offset + 4 + 17)).unwrap();
      file.write_all(&[0xFF]).unwrap();
    }
    let mut log = DatumLog::open(&path).unwrap();
    assert_eq!(log.get(id(1)).unwrap(), Some(b"first".to_vec()));
    assert_eq!(log.get(id(2)).unwrap(), None); // truncated away
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
