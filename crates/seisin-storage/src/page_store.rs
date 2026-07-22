//! Raw fixed-size page I/O over a single file. Agnostic of superblock/
//! B+Tree semantics — just reads and writes whole `page_size`-byte pages
//! by page id. Page id 0 is where `seisin-storage`'s superblock always
//! lives, but `PageStore` itself doesn't know or care about that.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{bail, Result};

use crate::PageId;

pub struct PageStore {
  file: File,
  page_size: u32,
}

impl PageStore {
  /// Creates a new, empty page file at `path`, truncating it if it
  /// already exists.
  pub fn create(path: &Path, page_size: u32) -> Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .truncate(true)
      .open(path)?;
    Ok(Self { file, page_size })
  }

  /// Opens an existing page file at `path`. `page_size` must already be
  /// known (read from the superblock's first page by the caller before
  /// calling this) since page offsets depend on it.
  pub fn open(path: &Path, page_size: u32) -> Result<Self> {
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    Ok(Self { file, page_size })
  }

  pub fn page_size(&self) -> u32 {
    self.page_size
  }

  /// Reads page `id`'s full `page_size` bytes. A page beyond the current
  /// end of file (e.g. one never written) reads back as all zeros.
  pub fn read_page(&mut self, id: PageId) -> Result<Vec<u8>> {
    let offset = id * self.page_size as u64;
    let mut buf = vec![0u8; self.page_size as usize];
    let file_len = self.file.metadata()?.len();
    if offset >= file_len {
      return Ok(buf);
    }
    self.file.seek(SeekFrom::Start(offset))?;
    let to_read = self.page_size as u64;
    if offset + to_read > file_len {
      let available = (file_len - offset) as usize;
      self.file.read_exact(&mut buf[..available])?;
    } else {
      self.file.read_exact(&mut buf)?;
    }
    Ok(buf)
  }

  /// Writes `bytes` (must be exactly `page_size` long) as page `id`,
  /// extending the file if `id` is beyond its current end.
  pub fn write_page(&mut self, id: PageId, bytes: &[u8]) -> Result<()> {
    if bytes.len() != self.page_size as usize {
      bail!(
        "write_page expected exactly {} bytes, got {}",
        self.page_size,
        bytes.len()
      );
    }
    let offset = id * self.page_size as u64;
    self.file.seek(SeekFrom::Start(offset))?;
    self.file.write_all(bytes)?;
    Ok(())
  }

  /// Truncates the file to zero length — used by `rebuild_from` to wipe
  /// and start over.
  pub fn truncate(&mut self) -> Result<()> {
    self.file.set_len(0)?;
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use tempfile::NamedTempFile;

  #[test]
  fn writes_and_reads_back_a_page() {
    let tmp = NamedTempFile::new().unwrap();
    let mut store = PageStore::create(tmp.path(), 4096).unwrap();
    let mut page = vec![0u8; 4096];
    page[0] = 42;
    store.write_page(3, &page).unwrap();
    let read_back = store.read_page(3).unwrap();
    assert_eq!(read_back, page);
  }

  #[test]
  fn reading_an_unwritten_page_returns_zeros() {
    let tmp = NamedTempFile::new().unwrap();
    let mut store = PageStore::create(tmp.path(), 4096).unwrap();
    let page = store.read_page(5).unwrap();
    assert_eq!(page, vec![0u8; 4096]);
  }

  #[test]
  fn write_page_rejects_the_wrong_length() {
    let tmp = NamedTempFile::new().unwrap();
    let mut store = PageStore::create(tmp.path(), 4096).unwrap();
    assert!(store.write_page(0, &[0u8; 100]).is_err());
  }

  #[test]
  fn open_reads_back_pages_written_before_reopening() {
    let tmp = NamedTempFile::new().unwrap();
    {
      let mut store = PageStore::create(tmp.path(), 4096).unwrap();
      let mut page = vec![0u8; 4096];
      page[10] = 7;
      store.write_page(2, &page).unwrap();
    }
    let mut store = PageStore::open(tmp.path(), 4096).unwrap();
    let page = store.read_page(2).unwrap();
    assert_eq!(page[10], 7);
  }

  #[test]
  fn truncate_empties_the_file() {
    let tmp = NamedTempFile::new().unwrap();
    let mut store = PageStore::create(tmp.path(), 4096).unwrap();
    store.write_page(0, &vec![9u8; 4096]).unwrap();
    store.truncate().unwrap();
    let page = store.read_page(0).unwrap();
    assert_eq!(page, vec![0u8; 4096]);
  }
}
