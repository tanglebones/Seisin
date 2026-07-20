//! Encode/decode for secondary-key datum content.
//!
//! A secondary-key datum (e.g. `sk:user.name:cliff`) is a regular datum —
//! same collate-then-run op path as any primary-key datum — whose content
//! happens to be an encoded list of `(DatumId, AuthorityIdx)` pairs for
//! the primary datums it currently matches. The wire/server layer never
//! needs to know this; it just sees bytes.

use anyhow::{bail, Result};

use crate::authority::{AuthorityIdx, ENCODED_LEN as AUTHORITY_LEN};
use crate::datum::DatumId;

const ENTRY_LEN: usize = 16 + AUTHORITY_LEN;

pub fn encode_sk_entries(entries: &[(DatumId, AuthorityIdx)]) -> Vec<u8> {
  let mut buf = Vec::with_capacity(4 + entries.len() * ENTRY_LEN);
  buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
  for (id, authority) in entries {
    buf.extend_from_slice(&id.as_bytes());
    buf.extend_from_slice(&authority.encode());
  }
  buf
}

pub fn decode_sk_entries(bytes: &[u8]) -> Result<Vec<(DatumId, AuthorityIdx)>> {
  if bytes.len() < 4 {
    bail!("sk entry list truncated: missing count prefix");
  }
  let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
  let expected_len = 4 + count * ENTRY_LEN;
  if bytes.len() != expected_len {
    bail!(
      "sk entry list length mismatch: expected {expected_len} bytes for {count} entries, got {}",
      bytes.len()
    );
  }
  let mut entries = Vec::with_capacity(count);
  let mut offset = 4;
  for _ in 0..count {
    let id_bytes: [u8; 16] = bytes[offset..offset + 16].try_into().unwrap();
    let authority_bytes: [u8; AUTHORITY_LEN] =
      bytes[offset + 16..offset + ENTRY_LEN].try_into().unwrap();
    entries.push((
      DatumId::from_bytes(id_bytes),
      AuthorityIdx::decode(authority_bytes)?,
    ));
    offset += ENTRY_LEN;
  }
  Ok(entries)
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::authority::{NodeId, ThreadId};

  #[test]
  fn round_trips_empty_list() {
    let encoded = encode_sk_entries(&[]);
    assert_eq!(decode_sk_entries(&encoded).unwrap(), vec![]);
  }

  #[test]
  fn round_trips_multiple_entries() {
    let entries = vec![
      (DatumId::new(), AuthorityIdx::Native),
      (
        DatumId::new(),
        AuthorityIdx::Foreign {
          node_id: NodeId(7),
          thread_id: ThreadId(3),
        },
      ),
    ];
    let encoded = encode_sk_entries(&entries);
    assert_eq!(decode_sk_entries(&encoded).unwrap(), entries);
  }

  #[test]
  fn rejects_truncated_input() {
    let entries = vec![(DatumId::new(), AuthorityIdx::Native)];
    let mut encoded = encode_sk_entries(&entries);
    encoded.truncate(encoded.len() - 1);
    assert!(decode_sk_entries(&encoded).is_err());
  }
}
