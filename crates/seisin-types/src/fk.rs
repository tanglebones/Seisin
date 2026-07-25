//! FK dangling-reference tracking: the `fk_pending:{type}.{field}`
//! datum id derivation and the `"fk_pending"` blob-resident kind
//! holding `(referencing_pk, missing_target)` pairs. Inserts arrive
//! from the write path (a tracked dangling reference dispatched as an
//! ordinary `IndexUpdate`); List/Remove are the scan driver's surface
//! (`Request::FkPending`). The framework never resolves entries itself
//! — the driver probes, removes the resolved, and invokes the declared
//! ConflictOp for the still-missing.

use seisin_core::datum::DatumId;
use seisin_node::index_handler::{
  IndexApplyOutcome, IndexKind, IndexKindRegistry, ResidentIndex, WriteThrough,
};
use seisin_protocol::{decode_fk_entries, decode_fk_pending_op, encode_fk_entries, FkPendingOp};

use crate::sk_index::derived_id_namespace;

/// One pending-tracking datum per declared constraint.
pub fn fk_pending_key(type_name: &str, field: &str) -> DatumId {
  let name = format!("fk_pending:{type_name}.{field}");
  DatumId::from_name(&derived_id_namespace(), name.as_bytes())
}

pub struct FkPendingKind;

pub struct FkPendingResident {
  entries: Vec<(DatumId, DatumId)>,
}

impl FkPendingResident {
  fn outcome(&self) -> IndexApplyOutcome {
    IndexApplyOutcome {
      violation: None,
      // A drained pending list DELETES the stored datum — exists-probes
      // and storage stay exact, same policy as sk.
      write_through: if self.entries.is_empty() {
        WriteThrough::Delete
      } else {
        WriteThrough::Put(encode_fk_entries(&self.entries))
      },
    }
  }
}

impl ResidentIndex for FkPendingResident {
  fn apply(&mut self, payload: &[u8]) -> IndexApplyOutcome {
    let op = match decode_fk_pending_op(payload) {
      Ok(op) => op,
      Err(e) => {
        return IndexApplyOutcome {
          violation: Some(format!("malformed fk_pending payload: {e}")),
          write_through: WriteThrough::None,
        }
      }
    };
    match op {
      FkPendingOp::Insert {
        referencing_pk,
        target,
      } => {
        let pair = (referencing_pk, target);
        if !self.entries.contains(&pair) {
          self.entries.push(pair); // idempotent re-insert
        }
        self.outcome()
      }
      FkPendingOp::Remove {
        referencing_pk,
        target,
      } => {
        self
          .entries
          .retain(|pair| *pair != (referencing_pk, target));
        self.outcome()
      }
      FkPendingOp::List => IndexApplyOutcome {
        violation: Some("fk_pending List is a query, not an apply".to_string()),
        write_through: WriteThrough::None,
      },
    }
  }

  fn query(&self, query: &[u8]) -> Result<Vec<u8>, String> {
    match decode_fk_pending_op(query).map_err(|e| e.to_string())? {
      FkPendingOp::List => Ok(encode_fk_entries(&self.entries)),
      other => Err(format!(
        "fk_pending query supports only List, got {other:?}"
      )),
    }
  }

  fn execute(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
    // The driver's Remove mutates resident state only — execute has no
    // write-through channel, so a restart before the next apply's
    // write-through can resurrect removed entries. Acceptable here
    // (and only here): fk_pending's ground truth is re-derivable by
    // re-probing references, so a resurrected entry just gets resolved
    // and removed again by the next scan.
    match decode_fk_pending_op(payload).map_err(|e| e.to_string())? {
      FkPendingOp::Remove {
        referencing_pk,
        target,
      } => {
        self
          .entries
          .retain(|pair| *pair != (referencing_pk, target));
        Ok(encode_fk_entries(&self.entries))
      }
      other => Err(format!(
        "fk_pending execute supports only Remove, got {other:?}"
      )),
    }
  }
}

impl IndexKind for FkPendingKind {
  /// Blob-persisted (sk's mechanics): stored bytes are the encoded
  /// entry list; undecodable stored bytes are an open error, never a
  /// silently empty list.
  fn open(
    &self,
    target: DatumId,
    stored: Option<Vec<u8>>,
  ) -> Result<Box<dyn ResidentIndex>, String> {
    let entries = match stored {
      Some(bytes) => decode_fk_entries(&bytes)
        .map_err(|e| format!("stored fk_pending entries for {target:?} failed to decode: {e}"))?,
      None => Vec::new(),
    };
    Ok(Box::new(FkPendingResident { entries }))
  }
}

/// Registers the `"fk_pending"` kind — call once at the composition
/// root wherever relational constraints with a resolution are in play.
pub fn register_fk_pending_kind(registry: &mut IndexKindRegistry) {
  registry.register("fk_pending", Box::new(FkPendingKind));
}

#[cfg(test)]
mod tests {
  use super::*;
  use seisin_protocol::encode_fk_pending_op;

  fn open_pending() -> Box<dyn ResidentIndex> {
    FkPendingKind.open(DatumId::new(), None).unwrap()
  }

  fn list(resident: &dyn ResidentIndex) -> Vec<(DatumId, DatumId)> {
    decode_fk_entries(
      &resident
        .query(&encode_fk_pending_op(&FkPendingOp::List))
        .unwrap(),
    )
    .unwrap()
  }

  #[test]
  fn insert_is_idempotent_and_listable() {
    let mut resident = open_pending();
    let pair = (DatumId::new(), DatumId::new());
    let payload = encode_fk_pending_op(&FkPendingOp::Insert {
      referencing_pk: pair.0,
      target: pair.1,
    });
    assert!(resident.apply(&payload).violation.is_none());
    assert!(resident.apply(&payload).violation.is_none()); // duplicate
    assert_eq!(list(resident.as_ref()), vec![pair]);
  }

  #[test]
  fn remove_via_execute_returns_the_remainder() {
    let mut resident = open_pending();
    let (a, b) = (
      (DatumId::new(), DatumId::new()),
      (DatumId::new(), DatumId::new()),
    );
    for pair in [a, b] {
      resident.apply(&encode_fk_pending_op(&FkPendingOp::Insert {
        referencing_pk: pair.0,
        target: pair.1,
      }));
    }
    let remaining = decode_fk_entries(
      &resident
        .execute(&encode_fk_pending_op(&FkPendingOp::Remove {
          referencing_pk: a.0,
          target: a.1,
        }))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(remaining, vec![b]);
    assert_eq!(list(resident.as_ref()), vec![b]);
  }

  #[test]
  fn open_seeds_from_stored_bytes_and_rejects_garbage() {
    let pair = (DatumId::new(), DatumId::new());
    let stored = encode_fk_entries(&[pair]);
    let resident = FkPendingKind.open(DatumId::new(), Some(stored)).unwrap();
    assert_eq!(list(resident.as_ref()), vec![pair]);
    assert!(FkPendingKind
      .open(DatumId::new(), Some(vec![0xFF; 3]))
      .is_err());
  }

  #[test]
  fn draining_the_pending_list_deletes_the_stored_datum() {
    let mut resident = open_pending();
    let pair = (DatumId::new(), DatumId::new());
    resident.apply(&encode_fk_pending_op(&FkPendingOp::Insert {
      referencing_pk: pair.0,
      target: pair.1,
    }));
    let outcome = resident.apply(&encode_fk_pending_op(&FkPendingOp::Remove {
      referencing_pk: pair.0,
      target: pair.1,
    }));
    assert!(matches!(outcome.write_through, WriteThrough::Delete));
  }

  #[test]
  fn key_derivation_is_stable_and_distinct() {
    assert_eq!(
      fk_pending_key("order", "customer_id"),
      fk_pending_key("order", "customer_id")
    );
    assert_ne!(
      fk_pending_key("order", "customer_id"),
      fk_pending_key("order", "vendor_id")
    );
  }
}
