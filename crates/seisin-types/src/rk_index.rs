//! rk (ranked/leaderboard) index maintenance: deriving the one stable
//! datum_id per declared rk index, order-preserving rank-key encoding,
//! and the update-payload codec. The resident-structure side
//! (`RkIndexKind`) is `rk_kind.rs`. See the rk design doc.

use anyhow::{bail, Result};
use seisin_core::datum::DatumId;

use crate::field::FieldValue;
use crate::sk_index::derived_id_namespace;

/// Derives the stable `DatumId` for `rk:{type_name}.{field_name}` — one
/// single id per declared rk index (no value-based partitioning,
/// unlike sk's per-distinct-value keys).
pub fn rk_key(type_name: &str, field_name: &str) -> DatumId {
  let name = format!("rk:{type_name}.{field_name}");
  DatumId::from_name(&derived_id_namespace(), name.as_bytes())
}

/// Encodes a numeric field value as 8 bytes whose raw byte-lexicographic
/// order (what the B+Tree compares) equals numeric total order.
pub fn encode_rank_key(value: &FieldValue) -> Result<[u8; 8]> {
  match value {
    FieldValue::I64(v) => {
      // Two's-complement order doesn't match unsigned byte order —
      // flipping the sign bit does: negatives (high bit set) become
      // the smaller unsigned range, positives the larger.
      Ok(((*v as u64) ^ 0x8000_0000_0000_0000).to_be_bytes())
    }
    FieldValue::F64(v) => {
      // Matches core::f64::total_cmp's own bit transform exactly, so
      // byte order equals total_cmp order (NaN included — orderable
      // rather than rejected).
      let bits = v.to_bits();
      let mask = ((bits as i64) >> 63) as u64 | 0x8000_0000_0000_0000;
      Ok((bits ^ mask).to_be_bytes())
    }
    other => bail!("rk rank_key must be I64 or F64, got {other:?}"),
  }
}

/// One update to an rk index: move `pk_id` from `old_rank_key` (absent
/// on first insert) to `new_rank_key` (absent on entity delete).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RkIndexOp {
  pub pk_id: DatumId,
  pub old_rank_key: Option<[u8; 8]>,
  pub new_rank_key: Option<[u8; 8]>,
}

fn push_optional_key(buf: &mut Vec<u8>, key: &Option<[u8; 8]>) {
  match key {
    None => buf.push(0),
    Some(k) => {
      buf.push(1);
      buf.extend_from_slice(k);
    }
  }
}

fn read_optional_key(buf: &[u8], offset: &mut usize) -> Result<Option<[u8; 8]>> {
  if buf.len() < *offset + 1 {
    bail!("rk index op truncated at an option flag");
  }
  let flag = buf[*offset];
  *offset += 1;
  match flag {
    0 => Ok(None),
    1 => {
      if buf.len() < *offset + 8 {
        bail!("rk index op truncated inside a rank key");
      }
      let key: [u8; 8] = buf[*offset..*offset + 8].try_into().unwrap();
      *offset += 8;
      Ok(Some(key))
    }
    f => bail!("unknown rk index op option flag: {f}"),
  }
}

pub fn encode_rk_index_op(op: &RkIndexOp) -> Vec<u8> {
  let mut buf = op.pk_id.as_bytes().to_vec();
  push_optional_key(&mut buf, &op.old_rank_key);
  push_optional_key(&mut buf, &op.new_rank_key);
  buf
}

pub fn decode_rk_index_op(buf: &[u8]) -> Result<RkIndexOp> {
  if buf.len() < 16 {
    bail!("rk index op payload too short for a pk_id");
  }
  let pk_id = DatumId::from_bytes(buf[0..16].try_into().unwrap());
  let mut offset = 16;
  let old_rank_key = read_optional_key(buf, &mut offset)?;
  let new_rank_key = read_optional_key(buf, &mut offset)?;
  if offset != buf.len() {
    bail!("rk index op has {} trailing bytes", buf.len() - offset);
  }
  Ok(RkIndexOp {
    pk_id,
    old_rank_key,
    new_rank_key,
  })
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn rk_key_is_stable_and_distinguishes_type_and_field() {
    assert_eq!(rk_key("user", "score"), rk_key("user", "score"));
    assert_ne!(rk_key("user", "score"), rk_key("user", "age"));
    assert_ne!(rk_key("user", "score"), rk_key("game", "score"));
  }

  #[test]
  fn i64_rank_keys_sort_byte_lexicographically_in_numeric_order() {
    let values = [i64::MIN, -300, -1, 0, 1, 256, 300, i64::MAX];
    let keys: Vec<[u8; 8]> = values
      .iter()
      .map(|v| encode_rank_key(&FieldValue::I64(*v)).unwrap())
      .collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
  }

  #[test]
  fn f64_rank_keys_sort_exactly_like_total_cmp() {
    let values = [
      f64::NEG_INFINITY,
      -1.5,
      -0.0,
      0.0,
      1.5,
      f64::INFINITY,
      f64::NAN,
      -f64::NAN,
    ];
    let mut by_bytes: Vec<f64> = values.to_vec();
    by_bytes.sort_by_key(|v| encode_rank_key(&FieldValue::F64(*v)).unwrap());
    let mut by_total_cmp: Vec<f64> = values.to_vec();
    by_total_cmp.sort_by(|a, b| a.total_cmp(b));
    let bits = |v: &f64| v.to_bits();
    assert_eq!(
      by_bytes.iter().map(bits).collect::<Vec<_>>(),
      by_total_cmp.iter().map(bits).collect::<Vec<_>>()
    );
  }

  #[test]
  fn non_numeric_values_are_rejected() {
    assert!(encode_rank_key(&FieldValue::String("x".to_string())).is_err());
    assert!(encode_rank_key(&FieldValue::Bool(true)).is_err());
  }

  #[test]
  fn rk_index_op_round_trips_all_option_combinations() {
    for (old, new) in [
      (None, Some([1u8; 8])),
      (Some([2u8; 8]), Some([3u8; 8])),
      (Some([4u8; 8]), None),
      (None, None),
    ] {
      let op = RkIndexOp {
        pk_id: DatumId::new(),
        old_rank_key: old,
        new_rank_key: new,
      };
      assert_eq!(decode_rk_index_op(&encode_rk_index_op(&op)).unwrap(), op);
    }
  }

  #[test]
  fn decode_rejects_truncated_or_padded_payloads() {
    let op = RkIndexOp {
      pk_id: DatumId::new(),
      old_rank_key: Some([1u8; 8]),
      new_rank_key: None,
    };
    let mut buf = encode_rk_index_op(&op);
    buf.push(0xFF);
    assert!(decode_rk_index_op(&buf).is_err());
    let buf = encode_rk_index_op(&op);
    assert!(decode_rk_index_op(&buf[..buf.len() - 1]).is_err());
  }
}
