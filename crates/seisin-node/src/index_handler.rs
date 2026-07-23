//! The registry of index kinds — parallel to
//! `seisin_ops::registry::OpRegistry`, but for index-kind logic rather
//! than solution ops. Registration happens once at startup (at the
//! composition root — `seisin-node` itself stays agnostic of what
//! "sk"/"rk"/"tk" even mean); lookup happens on each owning thread's
//! first `IndexUpdate` for a given index datum, which builds that
//! index's resident structure via `IndexKind::open`. Every later update
//! goes straight to the already-resident `ResidentIndex`.
//!
//! The resident structure is a trait object, not a byte blob, because
//! index kinds legitimately differ in representation: sk's entry list
//! is a small decoded `Vec` re-encoded per write-through, while rk's is
//! a live disk-backed B+Tree file handle that manages its own
//! persistence. What they share is the dispatch/lifecycle rail
//! (`IndexUpdate` → apply on the owning thread → pass/violation reply),
//! which is exactly what this module models. See the design doc's
//! "Automatic Index Maintenance & Op Lifecycle" section.

use std::collections::HashMap;

use seisin_core::datum::DatumId;

/// The outcome of applying one update to a resident index.
pub struct IndexApplyOutcome {
  /// `Some(message)` if the update was rejected (e.g. a uniqueness
  /// violation) — a rejected update must leave the resident structure
  /// untouched, since the caller keeps it resident for future updates.
  pub violation: Option<String>,
  /// New serialized content for the index datum, for kinds whose
  /// persistence is blob-shaped (sk): the worker writes it through to
  /// cache/storage. Kinds that manage their own persistence (rk's
  /// B+Tree file) return `None` and the worker writes nothing.
  pub write_through: Option<Vec<u8>>,
}

/// One index's live, per-owning-thread resident structure. Built once
/// on cold miss via `IndexKind::open`, kept resident and mutated in
/// place by every later update on that thread — never rebuilt from
/// bytes per update.
pub trait ResidentIndex: Send {
  fn apply(&mut self, payload: &[u8]) -> IndexApplyOutcome;
}

/// A registered index kind: knows how to build its resident structure
/// from whatever is currently stored for an index datum.
pub trait IndexKind: Send + Sync {
  /// Builds the resident structure for `target` on a cold miss.
  /// `stored` is the index datum's currently stored bytes, if any —
  /// blob-persisted kinds (sk) decode it; self-persisted kinds (rk)
  /// may ignore it and open their own backing file instead. A decode
  /// failure is an error, never silently treated as an empty index.
  fn open(
    &self,
    target: DatumId,
    stored: Option<Vec<u8>>,
  ) -> Result<Box<dyn ResidentIndex>, String>;
}

#[derive(Default)]
pub struct IndexKindRegistry {
  kinds: HashMap<String, Box<dyn IndexKind>>,
}

impl IndexKindRegistry {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn register(&mut self, kind: impl Into<String>, index_kind: Box<dyn IndexKind>) {
    self.kinds.insert(kind.into(), index_kind);
  }

  /// Looks up `kind`. Returns `Err` if it was never registered.
  pub fn get(&self, kind: &str) -> Result<&dyn IndexKind, String> {
    self
      .kinds
      .get(kind)
      .map(|k| k.as_ref())
      .ok_or_else(|| format!("no index kind registered for {kind:?}"))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// A test kind whose resident state is a plain byte accumulator —
  /// enough to prove open/apply/write-through mechanics without any
  /// real index semantics.
  struct AppendKind;
  struct AppendResident {
    bytes: Vec<u8>,
  }

  impl ResidentIndex for AppendResident {
    fn apply(&mut self, payload: &[u8]) -> IndexApplyOutcome {
      if payload == b"reject" {
        return IndexApplyOutcome {
          violation: Some("rejected".to_string()),
          write_through: None,
        };
      }
      self.bytes.extend_from_slice(payload);
      IndexApplyOutcome {
        violation: None,
        write_through: Some(self.bytes.clone()),
      }
    }
  }

  impl IndexKind for AppendKind {
    fn open(
      &self,
      _target: DatumId,
      stored: Option<Vec<u8>>,
    ) -> Result<Box<dyn ResidentIndex>, String> {
      Ok(Box::new(AppendResident {
        bytes: stored.unwrap_or_default(),
      }))
    }
  }

  #[test]
  fn get_returns_the_registered_kind() {
    let mut registry = IndexKindRegistry::new();
    registry.register("append", Box::new(AppendKind));
    assert!(registry.get("append").is_ok());
  }

  #[test]
  fn get_on_an_unregistered_kind_is_an_error() {
    let registry = IndexKindRegistry::new();
    assert!(registry.get("nope").is_err());
  }

  #[test]
  fn open_seeds_the_resident_structure_from_stored_bytes() {
    let mut registry = IndexKindRegistry::new();
    registry.register("append", Box::new(AppendKind));
    let kind = registry.get("append").unwrap();
    let mut resident = kind.open(DatumId::new(), Some(b"warm".to_vec())).unwrap();
    let outcome = resident.apply(b"+more");
    assert!(outcome.violation.is_none());
    assert_eq!(outcome.write_through, Some(b"warm+more".to_vec()));
  }

  #[test]
  fn a_resident_structure_accumulates_state_across_applies() {
    let mut registry = IndexKindRegistry::new();
    registry.register("append", Box::new(AppendKind));
    let kind = registry.get("append").unwrap();
    let mut resident = kind.open(DatumId::new(), None).unwrap();
    resident.apply(b"a");
    let outcome = resident.apply(b"b");
    assert_eq!(outcome.write_through, Some(b"ab".to_vec()));
  }

  #[test]
  fn apply_can_report_a_violation_without_a_write_through() {
    let mut registry = IndexKindRegistry::new();
    registry.register("append", Box::new(AppendKind));
    let kind = registry.get("append").unwrap();
    let mut resident = kind.open(DatumId::new(), None).unwrap();
    let outcome = resident.apply(b"reject");
    assert_eq!(outcome.violation, Some("rejected".to_string()));
    assert!(outcome.write_through.is_none());
  }
}
