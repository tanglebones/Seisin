//! The B+Tree file's page-0 header: format identification, the fixed
//! key/value/page sizes chosen at creation time, and the tree's current
//! root/count/next-allocation state. See the design doc's "On-Disk
//! Format" section.

use anyhow::{bail, Result};

const MAGIC: &[u8; 8] = b"SEISNBT1";
const HEADER_LEN: usize = 44;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Superblock {
  pub page_size: u32,
  pub key_size: u32,
  pub value_size: u32,
  pub root_page_id: u64,
  pub total_count: u64,
  pub next_page_id: u64,
}

impl Superblock {
  pub fn validate_page_size(page_size: u32) -> Result<()> {
    if page_size < 4096 {
      bail!("page_size must be at least 4096, got {page_size}");
    }
    if !page_size.is_power_of_two() {
      bail!("page_size must be a power of 2, got {page_size}");
    }
    Ok(())
  }

  pub fn encode(&self, page_size: u32) -> Vec<u8> {
    let mut buf = vec![0u8; page_size as usize];
    buf[0..8].copy_from_slice(MAGIC);
    buf[8..12].copy_from_slice(&self.page_size.to_le_bytes());
    buf[12..16].copy_from_slice(&self.key_size.to_le_bytes());
    buf[16..20].copy_from_slice(&self.value_size.to_le_bytes());
    buf[20..28].copy_from_slice(&self.root_page_id.to_le_bytes());
    buf[28..36].copy_from_slice(&self.total_count.to_le_bytes());
    buf[36..44].copy_from_slice(&self.next_page_id.to_le_bytes());
    buf
  }

  pub fn decode(bytes: &[u8]) -> Result<Self> {
    if bytes.len() < HEADER_LEN {
      bail!("superblock page is too short to contain a header");
    }
    if &bytes[0..8] != MAGIC {
      bail!("not a seisin-storage B+Tree file (magic mismatch)");
    }
    let page_size = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let key_size = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
    let value_size = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
    let root_page_id = u64::from_le_bytes(bytes[20..28].try_into().unwrap());
    let total_count = u64::from_le_bytes(bytes[28..36].try_into().unwrap());
    let next_page_id = u64::from_le_bytes(bytes[36..44].try_into().unwrap());
    Self::validate_page_size(page_size)?;
    Ok(Self {
      page_size,
      key_size,
      value_size,
      root_page_id,
      total_count,
      next_page_id,
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn sample() -> Superblock {
    Superblock {
      page_size: 4096,
      key_size: 8,
      value_size: 16,
      root_page_id: 1,
      total_count: 0,
      next_page_id: 2,
    }
  }

  #[test]
  fn round_trips_through_bytes() {
    let sb = sample();
    let encoded = sb.encode(4096);
    assert_eq!(Superblock::decode(&encoded).unwrap(), sb);
  }

  #[test]
  fn decode_rejects_wrong_magic() {
    let sb = sample();
    let mut encoded = sb.encode(4096);
    encoded[0] = 0xFF;
    assert!(Superblock::decode(&encoded).is_err());
  }

  #[test]
  fn decode_rejects_too_short_input() {
    assert!(Superblock::decode(&[0u8; 10]).is_err());
  }

  #[test]
  fn validate_page_size_rejects_below_4096() {
    assert!(Superblock::validate_page_size(2048).is_err());
  }

  #[test]
  fn validate_page_size_rejects_non_power_of_two() {
    assert!(Superblock::validate_page_size(5000).is_err());
  }

  #[test]
  fn validate_page_size_accepts_4096_and_larger_powers_of_two() {
    assert!(Superblock::validate_page_size(4096).is_ok());
    assert!(Superblock::validate_page_size(8192).is_ok());
    assert!(Superblock::validate_page_size(65536).is_ok());
  }
}
