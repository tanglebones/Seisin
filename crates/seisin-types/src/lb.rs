//! lb (leaderboard) class declaration and identity: the per-class
//! definition (score type, display width, update rule), board datum-id
//! derivation, and the rank-key decode inverse. The resident-board
//! side (`LbIndexKind`) is `lb_kind.rs`. See the lb design doc.

use anyhow::{bail, Result};
use seisin_core::datum::DatumId;

use crate::field::FieldValue;
use crate::rk_index::encode_rank_key;
use crate::sk_index::derived_id_namespace;

#[derive(Debug, Clone, PartialEq)]
pub enum LbScoreType {
  I64,
  F64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LbRule {
  /// Keep the better (higher) score.
  Max,
  /// Keep the better (lower) score.
  Min,
  /// Last write wins.
  Replace,
}

/// One leaderboard class. Registered as registry kind `lb:{name}` —
/// one kind per class, because `IndexKind::open` only receives a
/// `DatumId` and must learn the rule/display width from the registered
/// kind itself.
#[derive(Debug, Clone, PartialEq)]
pub struct LbClassDef {
  pub name: String,
  pub score_type: LbScoreType,
  pub display_len: u16,
  pub rule: LbRule,
}

pub fn lb_kind_name(class: &str) -> String {
  format!("lb:{class}")
}

/// Board identity: class + leaderboard + area configuration, all
/// normalized into the datum id — never repeated per entry. A season
/// or reset is a new `leaderboard_id`.
pub fn lb_board_key(class: &str, leaderboard_id: &str, area_config_id: &str) -> DatumId {
  let name = format!("lb:{class}:{leaderboard_id}:{area_config_id}");
  DatumId::from_name(&derived_id_namespace(), name.as_bytes())
}

/// Encodes a score for `def`, rejecting a value that doesn't match the
/// class's declared score type (the rk transforms are shared — order-
/// preserving byte encodings for I64/F64).
pub fn encode_score(def: &LbClassDef, value: &FieldValue) -> Result<[u8; 8]> {
  match (&def.score_type, value) {
    (LbScoreType::I64, FieldValue::I64(_)) | (LbScoreType::F64, FieldValue::F64(_)) => {
      encode_rank_key(value)
    }
    (expected, got) => bail!(
      "lb class {:?} declares score type {:?} but got {:?}",
      def.name,
      expected,
      got
    ),
  }
}

/// The inverse of `encode_rank_key` — bijective, so clients can render
/// scores from wire-level rank keys.
pub fn decode_rank_key(score_type: &LbScoreType, key: [u8; 8]) -> FieldValue {
  let enc = u64::from_be_bytes(key);
  match score_type {
    LbScoreType::I64 => FieldValue::I64((enc ^ 0x8000_0000_0000_0000) as i64),
    LbScoreType::F64 => {
      let bits = if enc & 0x8000_0000_0000_0000 != 0 {
        enc ^ 0x8000_0000_0000_0000
      } else {
        !enc
      };
      FieldValue::F64(f64::from_bits(bits))
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn racing() -> LbClassDef {
    LbClassDef {
      name: "racing".to_string(),
      score_type: LbScoreType::I64,
      display_len: 32,
      rule: LbRule::Max,
    }
  }

  #[test]
  fn board_key_is_stable_and_distinguishes_every_component() {
    let a = lb_board_key("racing", "season1", "desert");
    assert_eq!(a, lb_board_key("racing", "season1", "desert"));
    assert_ne!(a, lb_board_key("racing", "season2", "desert"));
    assert_ne!(a, lb_board_key("racing", "season1", "ice"));
    assert_ne!(a, lb_board_key("arena", "season1", "desert"));
  }

  #[test]
  fn encode_score_rejects_a_type_mismatch() {
    assert!(encode_score(&racing(), &FieldValue::F64(1.5)).is_err());
    assert!(encode_score(&racing(), &FieldValue::I64(100)).is_ok());
  }

  #[test]
  fn i64_rank_keys_round_trip_through_decode() {
    let def = racing();
    for v in [i64::MIN, -300, -1, 0, 1, 300, i64::MAX] {
      let key = encode_score(&def, &FieldValue::I64(v)).unwrap();
      assert_eq!(decode_rank_key(&def.score_type, key), FieldValue::I64(v));
    }
  }

  #[test]
  fn f64_rank_keys_round_trip_through_decode_bitwise() {
    let ty = LbScoreType::F64;
    for v in [
      f64::NEG_INFINITY,
      -1.5,
      -0.0,
      0.0,
      1.5,
      f64::INFINITY,
      f64::NAN,
    ] {
      let key = encode_rank_key(&FieldValue::F64(v)).unwrap();
      match decode_rank_key(&ty, key) {
        FieldValue::F64(out) => assert_eq!(out.to_bits(), v.to_bits()),
        other => panic!("expected F64, got {other:?}"),
      }
    }
  }

  #[test]
  fn kind_name_prefixes_the_class() {
    assert_eq!(lb_kind_name("racing"), "lb:racing");
  }
}
