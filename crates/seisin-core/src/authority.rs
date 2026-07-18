//! The tagged Native/Foreign authority_idx representation.
//!
//! `Native` is resolved by hashing the datum_id through the current
//! consistent-hash ring (added in a later sub-project); `Foreign` is a
//! direct pointer stamped when a datum is collated onto a non-native
//! thread. The tag is fixed at assignment time and never reinterpreted by
//! rehashing, so it stays correct even if ring membership changes
//! concurrently — see the design doc's "Cache Invalidation on Ring
//! Membership Change" section.

use anyhow::{bail, Result};

/// Identifies a compute node within the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u64);

/// Identifies one owning thread (authority slot) within a compute node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ThreadId(pub u32);

/// Current owner of a datum: either its hash-derived native home, or a
/// thread it has been temporarily collated onto.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityIdx {
  Native,
  Foreign {
    node_id: NodeId,
    thread_id: ThreadId,
  },
}

/// Wire size of an encoded `AuthorityIdx`: 1 tag byte + 8-byte node id + 4-byte thread id.
pub const ENCODED_LEN: usize = 13;

const TAG_NATIVE: u8 = 0;
const TAG_FOREIGN: u8 = 1;

impl AuthorityIdx {
  pub fn encode(&self) -> [u8; ENCODED_LEN] {
    let mut buf = [0u8; ENCODED_LEN];
    match self {
      AuthorityIdx::Native => buf[0] = TAG_NATIVE,
      AuthorityIdx::Foreign { node_id, thread_id } => {
        buf[0] = TAG_FOREIGN;
        buf[1..9].copy_from_slice(&node_id.0.to_le_bytes());
        buf[9..13].copy_from_slice(&thread_id.0.to_le_bytes());
      }
    }
    buf
  }

  pub fn decode(buf: [u8; ENCODED_LEN]) -> Result<Self> {
    match buf[0] {
      TAG_NATIVE => Ok(AuthorityIdx::Native),
      TAG_FOREIGN => {
        let node_id = NodeId(u64::from_le_bytes(buf[1..9].try_into().unwrap()));
        let thread_id = ThreadId(u32::from_le_bytes(buf[9..13].try_into().unwrap()));
        Ok(AuthorityIdx::Foreign { node_id, thread_id })
      }
      tag => bail!("invalid authority_idx tag byte: {tag}"),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  const VECTORS: &[(AuthorityIdx, [u8; ENCODED_LEN])] = &[
    (
      AuthorityIdx::Native,
      [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    (
      AuthorityIdx::Foreign {
        node_id: NodeId(1),
        thread_id: ThreadId(2),
      },
      [1, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0],
    ),
  ];

  #[test]
  fn encodes_known_vectors() {
    for (value, expected_bytes) in VECTORS {
      assert_eq!(&value.encode(), expected_bytes);
    }
  }

  #[test]
  fn decodes_known_vectors() {
    for (expected_value, bytes) in VECTORS {
      assert_eq!(AuthorityIdx::decode(*bytes).unwrap(), *expected_value);
    }
  }

  #[test]
  fn rejects_invalid_tag() {
    let mut bytes = [0u8; ENCODED_LEN];
    bytes[0] = 2;
    assert!(AuthorityIdx::decode(bytes).is_err());
  }
}
