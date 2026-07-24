//! tk (bitemporal valid-time) class declaration and identity: the
//! per-class definition (value type/width, sub-key width), entity
//! datum-id derivation, the order-preserving timestamp transform, and
//! the wall clock the server uses to stamp `as_of: None` writes. The
//! resident-history side (`TkIndexKind`) is `tk_kind.rs`. See the tk
//! design doc.

use std::time::{SystemTime, UNIX_EPOCH};

use seisin_core::datum::DatumId;

use crate::field::FieldType;
use crate::sk_index::derived_id_namespace;

/// One tk class. Registered as registry kind `tk:{name}` — one kind
/// per class, because `IndexKind::open` only receives a `DatumId`.
#[derive(Debug, Clone, PartialEq)]
pub struct TkClassDef {
  pub name: String,
  pub value_type: FieldType,
  /// Hard cap on the encoded value, bytes. tk is primary data — an
  /// oversized value is REJECTED, never truncated (TOAST overflow at
  /// the Storage Tier lifts this later).
  pub value_width: u16,
  /// Width of the sub-key prefix, bytes; 0 = the class tracks the
  /// entity itself. Non-zero = independent histories per sub-part of
  /// the entity (e.g. entity = account, sub_key = investment id).
  pub sub_key_width: u16,
}

pub fn tk_kind_name(class: &str) -> String {
  format!("tk:{class}")
}

/// One tk datum per (class, entity): identity normalized into the
/// datum id, entities distributed by ordinary ring placement.
pub fn tk_entity_key(class: &str, entity: DatumId) -> DatumId {
  let hex: String = entity
    .as_bytes()
    .iter()
    .map(|b| format!("{b:02x}"))
    .collect();
  let name = format!("tk:{class}:{hex}");
  DatumId::from_name(&derived_id_namespace(), name.as_bytes())
}

/// i64 epoch-millis -> order-preserving 8 bytes (the I64 sign-flip
/// big-endian transform; pre-1970 backdates order correctly).
pub fn encode_ts(t: i64) -> [u8; 8] {
  ((t as u64) ^ 0x8000_0000_0000_0000).to_be_bytes()
}

pub fn decode_ts(key: [u8; 8]) -> i64 {
  (u64::from_be_bytes(key) ^ 0x8000_0000_0000_0000) as i64
}

/// Wall-clock seam for stamping `as_of: None` writes — gossip's
/// `ClockSource` is monotonic-`Instant`-based, the wrong tool for
/// epoch millis. Tests inject a fixed fake.
pub trait WallClock: Send + Sync {
  fn now_millis(&self) -> i64;
}

pub struct SystemWallClock;

impl WallClock for SystemWallClock {
  fn now_millis(&self) -> i64 {
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .expect("system clock before the unix epoch")
      .as_millis() as i64
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn entity_key_is_stable_and_distinguishes_class_and_entity() {
    let e1 = DatumId::new();
    let e2 = DatumId::new();
    assert_eq!(tk_entity_key("holdings", e1), tk_entity_key("holdings", e1));
    assert_ne!(tk_entity_key("holdings", e1), tk_entity_key("holdings", e2));
    assert_ne!(tk_entity_key("holdings", e1), tk_entity_key("prices", e1));
  }

  #[test]
  fn timestamps_round_trip_and_sort_byte_lexicographically() {
    let values = [i64::MIN, -1_000_000, -1, 0, 1, 1_000_000, i64::MAX];
    for v in values {
      assert_eq!(decode_ts(encode_ts(v)), v);
    }
    let keys: Vec<[u8; 8]> = values.iter().map(|v| encode_ts(*v)).collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
  }

  #[test]
  fn system_wall_clock_returns_a_plausible_now() {
    // 2020-01-01 in millis — anything after this is "plausible".
    assert!(SystemWallClock.now_millis() > 1_577_836_800_000);
  }

  #[test]
  fn kind_name_prefixes_the_class() {
    assert_eq!(tk_kind_name("holdings"), "tk:holdings");
  }
}
