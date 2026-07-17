//! Identity for a single datum — the unit of ownership in the system.

use uuid::Uuid;

/// A globally unique identifier for a datum (primary-key or secondary-key).
/// Backed by a UUIDv7 so ids are k-sortable by creation time, which the
/// wound-wait collation scheme (a later sub-project) relies on for op
/// ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
}
