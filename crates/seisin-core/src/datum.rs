//! Identity for a single datum — the unit of ownership in the system.

use uuid::Uuid;

/// A globally unique identifier for a datum (primary-key or secondary-key).
/// Backed by a UUIDv7 so ids are k-sortable by creation time, which the
/// wound-wait collation scheme (a later sub-project) relies on for op
/// ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatumId(Uuid);

impl DatumId {
  /// Generates a fresh UUIDv7-backed id from the current time.
  pub fn new() -> Self {
    DatumId(Uuid::now_v7())
  }

  /// Reconstructs an id from its 16-byte wire representation.
  pub fn from_bytes(bytes: [u8; 16]) -> Self {
    DatumId(Uuid::from_bytes(bytes))
  }

  /// Returns the 16-byte wire representation of this id.
  pub fn as_bytes(&self) -> [u8; 16] {
    *self.0.as_bytes()
  }

  /// Deterministically derives an id from `namespace` and `name` — the
  /// same `(namespace, name)` pair always produces the same `DatumId`,
  /// unlike `new()`'s time-based randomness. Used for keys that must
  /// resolve to the same datum every time (sk/rk/tk index keys), backed
  /// by UUIDv5 (the standard name-based UUID scheme).
  pub fn from_name(namespace: &DatumId, name: &[u8]) -> Self {
    DatumId(Uuid::new_v5(&namespace.0, name))
  }
}

impl Default for DatumId {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn round_trips_through_bytes() {
    let id = DatumId::new();
    let restored = DatumId::from_bytes(id.as_bytes());
    assert_eq!(id, restored);
  }

  #[test]
  fn new_ids_are_distinct() {
    assert_ne!(DatumId::new(), DatumId::new());
  }

  #[test]
  fn ordering_matches_creation_order() {
    let first = DatumId::new();
    let second = DatumId::new();
    assert!(
      first < second,
      "a later-created UUIDv7 id must sort greater"
    );
  }

  #[test]
  fn from_name_is_deterministic() {
    let ns = DatumId::new();
    let a = DatumId::from_name(&ns, b"sk:user.name:cliff");
    let b = DatumId::from_name(&ns, b"sk:user.name:cliff");
    assert_eq!(a, b);
  }

  #[test]
  fn from_name_differs_for_different_names() {
    let ns = DatumId::new();
    let a = DatumId::from_name(&ns, b"sk:user.name:cliff");
    let b = DatumId::from_name(&ns, b"sk:user.name:someone_else");
    assert_ne!(a, b);
  }

  #[test]
  fn from_name_differs_across_namespaces() {
    let a = DatumId::from_name(&DatumId::new(), b"same name");
    let b = DatumId::from_name(&DatumId::new(), b"same name");
    assert_ne!(a, b);
  }
}
