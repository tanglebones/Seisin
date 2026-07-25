//! Structure-blind byte deltas: computed on the compute side (which
//! holds old and new bytes at write-through time), applied mechanically
//! by the storage tier — no schema knowledge on either side of the
//! apply, which is what keeps storage content-agnostic (see the Storage
//! Tier Part A design doc's decision record). v1 is prefix/suffix trim;
//! copy/insert (xdelta-style) is a drop-in upgrade behind the same
//! record kind.

use anyhow::{bail, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delta {
  pub prefix_len: u32,
  pub suffix_len: u32,
  pub middle: Vec<u8>,
  pub new_total_len: u32,
}

impl Delta {
  /// The delta's encoded wire/log size — what the compute side compares
  /// against the full value to decide Patch vs Put.
  pub fn encoded_len(&self) -> usize {
    16 + self.middle.len()
  }
}

/// Longest-common-prefix + longest-common-suffix trim. The middle is
/// whatever remains of `new` between the shared ends.
pub fn diff(old: &[u8], new: &[u8]) -> Delta {
  let mut prefix = 0usize;
  let max_prefix = old.len().min(new.len());
  while prefix < max_prefix && old[prefix] == new[prefix] {
    prefix += 1;
  }
  let mut suffix = 0usize;
  let max_suffix = max_prefix - prefix;
  while suffix < max_suffix && old[old.len() - 1 - suffix] == new[new.len() - 1 - suffix] {
    suffix += 1;
  }
  Delta {
    prefix_len: prefix as u32,
    suffix_len: suffix as u32,
    middle: new[prefix..new.len() - suffix].to_vec(),
    new_total_len: new.len() as u32,
  }
}

/// Mechanical splice — strict bounds validation: a malformed delta is a
/// loud error, never silent corruption.
pub fn apply(old: &[u8], delta: &Delta) -> Result<Vec<u8>> {
  let prefix = delta.prefix_len as usize;
  let suffix = delta.suffix_len as usize;
  if prefix + suffix > old.len() {
    bail!(
      "delta prefix {} + suffix {} exceed the base's {} bytes",
      prefix,
      suffix,
      old.len()
    );
  }
  if prefix + delta.middle.len() + suffix != delta.new_total_len as usize {
    bail!(
      "delta reassembles to {} bytes but declares {}",
      prefix + delta.middle.len() + suffix,
      delta.new_total_len
    );
  }
  let mut out = Vec::with_capacity(delta.new_total_len as usize);
  out.extend_from_slice(&old[..prefix]);
  out.extend_from_slice(&delta.middle);
  out.extend_from_slice(&old[old.len() - suffix..]);
  Ok(out)
}

pub fn encode_delta(delta: &Delta) -> Vec<u8> {
  let mut buf = Vec::with_capacity(delta.encoded_len());
  buf.extend_from_slice(&delta.prefix_len.to_le_bytes());
  buf.extend_from_slice(&delta.suffix_len.to_le_bytes());
  buf.extend_from_slice(&delta.new_total_len.to_le_bytes());
  buf.extend_from_slice(&(delta.middle.len() as u32).to_le_bytes());
  buf.extend_from_slice(&delta.middle);
  buf
}

pub fn decode_delta(buf: &[u8]) -> Result<Delta> {
  if buf.len() < 16 {
    bail!("delta too short for its fixed fields: {} bytes", buf.len());
  }
  let prefix_len = u32::from_le_bytes(buf[0..4].try_into().unwrap());
  let suffix_len = u32::from_le_bytes(buf[4..8].try_into().unwrap());
  let new_total_len = u32::from_le_bytes(buf[8..12].try_into().unwrap());
  let middle_len = u32::from_le_bytes(buf[12..16].try_into().unwrap()) as usize;
  if buf.len() != 16 + middle_len {
    bail!(
      "delta declares a {}-byte middle but carries {}",
      middle_len,
      buf.len() - 16
    );
  }
  Ok(Delta {
    prefix_len,
    suffix_len,
    middle: buf[16..].to_vec(),
    new_total_len,
  })
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn a_fixed_width_middle_change_trims_to_the_changed_bytes() {
    let old = b"aa11cc";
    let new = b"aa22cc";
    let delta = diff(old, new);
    assert_eq!(delta.prefix_len, 2);
    assert_eq!(delta.suffix_len, 2);
    assert_eq!(delta.middle, b"22");
    assert_eq!(apply(old, &delta).unwrap(), new);
  }

  #[test]
  fn a_length_changing_field_keeps_the_shifted_suffix() {
    let old = b"aaBBcc";
    let new = b"aaBBBBcc";
    let delta = diff(old, new);
    assert_eq!(delta.prefix_len, 4); // "aaBB" shared
    assert_eq!(delta.suffix_len, 2); // "cc" shared despite the shift
    assert_eq!(apply(old, &delta).unwrap(), new);
  }

  #[test]
  fn identical_inputs_yield_an_empty_middle() {
    let old = b"same";
    let delta = diff(old, old);
    assert!(delta.middle.is_empty());
    assert_eq!(apply(old, &delta).unwrap(), old.to_vec());
  }

  #[test]
  fn disjoint_inputs_carry_the_whole_new_value() {
    let delta = diff(b"abc", b"xyz!");
    assert_eq!(delta.prefix_len, 0);
    assert_eq!(delta.suffix_len, 0);
    assert_eq!(delta.middle, b"xyz!");
    assert_eq!(apply(b"abc", &delta).unwrap(), b"xyz!".to_vec());
  }

  #[test]
  fn apply_rejects_malformed_deltas_loudly() {
    let bad = Delta {
      prefix_len: 3,
      suffix_len: 3,
      middle: vec![],
      new_total_len: 6,
    };
    assert!(apply(b"abcd", &bad).is_err()); // prefix+suffix > base
    let mismatch = Delta {
      prefix_len: 1,
      suffix_len: 1,
      middle: b"x".to_vec(),
      new_total_len: 99,
    };
    assert!(apply(b"abc", &mismatch).is_err()); // total-len mismatch
  }

  #[test]
  fn codec_round_trips_and_rejects_truncation() {
    let delta = diff(b"hello world", b"hello brave world");
    let encoded = encode_delta(&delta);
    assert_eq!(decode_delta(&encoded).unwrap(), delta);
    assert!(decode_delta(&encoded[..encoded.len() - 1]).is_err());
  }

  #[test]
  fn apply_of_diff_round_trips_over_a_random_corpus() {
    // Hand-rolled LCG, no rand dep — pairs share a common region so the
    // trim paths are actually exercised.
    let mut state = 0x9E3779B97F4A7C15u64;
    let mut next = move || {
      state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
      state
    };
    for _ in 0..500 {
      let base_len = (next() % 200) as usize;
      let base: Vec<u8> = (0..base_len).map(|_| (next() % 256) as u8).collect();
      let mut new = base.clone();
      // Random splice: replace a random region with random bytes.
      if !new.is_empty() {
        let start = (next() as usize) % new.len();
        let end = start + ((next() as usize) % (new.len() - start + 1));
        let replacement: Vec<u8> = (0..(next() % 32) as usize)
          .map(|_| (next() % 256) as u8)
          .collect();
        new.splice(start..end, replacement);
      }
      let delta = diff(&base, &new);
      assert_eq!(apply(&base, &delta).unwrap(), new);
    }
  }
}
