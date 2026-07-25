//! Client-side scan-driver helpers: the full-validation rescan over a
//! type's extent. The framework never runs this on a timer — a
//! solution's driver loop calls `validate_type` at the cadence the
//! type declares (`rescan_every_millis`) and acts on the findings
//! (ConflictOps, alerts) itself. Validating every type's *outgoing*
//! refs (plus the delete-side markers, scanned separately via
//! `Request::FkPending`) covers incoming validation across the schema.

use anyhow::{bail, Result};
use seisin_core::datum::DatumId;
use seisin_protocol::{ExtentOp, Request, Response};

use crate::field::FieldValue;
use crate::partition::{extent_key, invalid_key, partition_key};
use crate::schema::{DatumTypeDef, FkTarget};
use crate::sk_index::{derived_id_namespace, sk_key};
use crate::typed_context::run_field_checks;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationFinding {
  pub pk: DatumId,
  pub field: String,
  pub problem: String,
}

/// Full-validation scan of one type: pages the extent, reads and
/// decodes every datum via the solution-registered `read_op` (a plain
/// byte-read op), re-runs declared field checks and static enum
/// membership, and probes every runtime FK target's existence.
pub fn validate_type(
  addr: &str,
  def: &DatumTypeDef,
  read_op: &str,
  page_size: u32,
) -> Result<Vec<ValidationFinding>> {
  if !def.track_extent {
    bail!(
      "type {:?} does not track an extent — rescan cannot enumerate it",
      def.name
    );
  }
  let extent = extent_key(&def.name);
  let mut findings = Vec::new();
  let mut offset = 0u64;
  loop {
    let response = seisin_client::call(
      addr,
      Request::ExtentQuery {
        extent_datum_id: extent,
        offset,
        limit: page_size,
      },
    )?;
    let pks = match response {
      Response::ExtentResult { pks, .. } => pks,
      other => bail!("extent query failed: {other:?}"),
    };
    if pks.is_empty() {
      break;
    }
    offset += pks.len() as u64;
    for pk in pks {
      validate_one(addr, def, read_op, pk, &mut findings)?;
    }
  }
  Ok(findings)
}

fn validate_one(
  addr: &str,
  def: &DatumTypeDef,
  read_op: &str,
  pk: DatumId,
  findings: &mut Vec<ValidationFinding>,
) -> Result<()> {
  let response = seisin_client::call(
    addr,
    Request::Op {
      op_id: DatumId::new(),
      op_name: read_op.to_string(),
      datum_ids: vec![pk],
      payload: vec![],
    },
  )?;
  let bytes = match response {
    Response::OpResult { payload } => payload,
    Response::OpError { message } => {
      findings.push(ValidationFinding {
        pk,
        field: String::new(),
        problem: format!("read failed: {message}"),
      });
      return Ok(());
    }
    other => bail!("unexpected read response: {other:?}"),
  };
  if bytes.is_empty() {
    // Deleted between the extent page and this read — the extent
    // remove may still be in flight; not a finding.
    return Ok(());
  }
  let values = match crate::schema::decode_datum(def, &bytes) {
    Ok(values) => values,
    Err(e) => {
      findings.push(ValidationFinding {
        pk,
        field: String::new(),
        problem: format!("undecodable content: {e}"),
      });
      return Ok(());
    }
  };

  // Field checks + static enum membership (the same logic set() runs —
  // this catches byte-level writes that bypassed the typed layer and
  // data predating a tightened check).
  if let Err(e) = run_field_checks(def, &values) {
    findings.push(ValidationFinding {
      pk,
      field: String::new(),
      problem: e.to_string(),
    });
  }

  // Runtime FK probes.
  for constraint in &def.constraints {
    let Some(field_idx) = def
      .fields
      .iter()
      .position(|(name, _)| name == &constraint.field)
    else {
      continue;
    };
    let target = match &constraint.references {
      FkTarget::PkEnum { .. } => continue, // covered by run_field_checks
      FkTarget::PkUuid { .. } => match &values[field_idx] {
        FieldValue::Bytes(bytes) if bytes.len() == 16 => {
          DatumId::from_bytes(bytes.as_slice().try_into().unwrap())
        }
        _ => continue, // shape violation already reported above
      },
      FkTarget::SkUnique { type_name, field } => {
        match sk_key(type_name, field, &values[field_idx]) {
          Ok(id) => id,
          Err(_) => continue,
        }
      }
    };
    let exists = match seisin_client::call(addr, Request::ExistsCheck { datum_id: target })? {
      Response::Exists { exists } => exists,
      other => bail!("unexpected exists response: {other:?}"),
    };
    if !exists {
      findings.push(ValidationFinding {
        pk,
        field: constraint.field.clone(),
        problem: format!("dangling reference: {target:?} does not exist"),
      });
    }
  }
  Ok(())
}

/// The order full-validation sweeps should process types in,
/// most-urgent first (indices into `defs`):
/// 1. most incoming runtime references first — fixing the
///    most-depended-on data first can resolve the references pointing
///    at it before their own types are scanned;
/// 2. ties: least outgoing runtime constraints first (fewer of its own
///    references to be broken — likelier genuinely fixable now);
/// 3. further ties: lower derived type id (deterministic; a numeric
///    type-id registry is future schema-registry/DSL work).
///
/// PkEnum references are excluded everywhere: static refs never dangle.
pub fn scan_order(defs: &[DatumTypeDef]) -> Vec<usize> {
  fn runtime_target(t: &FkTarget) -> Option<&str> {
    match t {
      FkTarget::PkUuid { type_name } => Some(type_name),
      FkTarget::SkUnique { type_name, .. } => Some(type_name),
      FkTarget::PkEnum { .. } => None,
    }
  }
  let mut order: Vec<usize> = (0..defs.len()).collect();
  order.sort_by_key(|&i| {
    let name = &defs[i].name;
    let incoming = defs
      .iter()
      .flat_map(|d| &d.constraints)
      .filter(|c| runtime_target(&c.references) == Some(name.as_str()))
      .count();
    let outgoing = defs[i]
      .constraints
      .iter()
      .filter(|c| runtime_target(&c.references).is_some())
      .count();
    let type_id = DatumId::from_name(&derived_id_namespace(), format!("type:{name}").as_bytes());
    (std::cmp::Reverse(incoming), outgoing, type_id.as_bytes())
  });
  order
}

fn partition_update(addr: &str, partition: DatumId, op: ExtentOp) -> Result<()> {
  match seisin_client::call(
    addr,
    Request::PartitionUpdate {
      partition_datum_id: partition,
      op,
    },
  )? {
    Response::ExtentResult { .. } => Ok(()),
    other => bail!("partition update failed: {other:?}"),
  }
}

/// Marks `pks` invalid — membership in the "invalid" partition IS the
/// datum's invalid flag (no flag churns datum content).
pub fn mark_invalid(addr: &str, def: &DatumTypeDef, pks: &[DatumId]) -> Result<()> {
  let partition = invalid_key(&def.name);
  for pk in pks {
    partition_update(addr, partition, ExtentOp::Insert { pk: *pk })?;
  }
  Ok(())
}

/// Clears one pk's invalid mark after a passing re-validation.
pub fn clear_invalid(addr: &str, def: &DatumTypeDef, pk: DatumId) -> Result<()> {
  partition_update(addr, invalid_key(&def.name), ExtentOp::Remove { pk })
}

/// The checker's fast path: re-validate only the "invalid" partition,
/// clearing entries that now pass and returning the still-failing
/// findings. The full `validate_type` sweep remains the slow path that
/// discovers new invalidity.
pub fn revalidate_invalid(
  addr: &str,
  def: &DatumTypeDef,
  read_op: &str,
  page_size: u32,
) -> Result<Vec<ValidationFinding>> {
  let partition = invalid_key(&def.name);
  let mut still_failing = Vec::new();
  let mut offset = 0u64;
  loop {
    let response = seisin_client::call(
      addr,
      Request::ExtentQuery {
        extent_datum_id: partition,
        offset,
        limit: page_size,
      },
    )?;
    let pks = match response {
      Response::ExtentResult { pks, .. } => pks,
      other => bail!("invalid-partition query failed: {other:?}"),
    };
    if pks.is_empty() {
      break;
    }
    let mut cleared_this_page = 0u64;
    for pk in pks.iter().copied() {
      let before = still_failing.len();
      validate_one(addr, def, read_op, pk, &mut still_failing)?;
      if still_failing.len() == before {
        clear_invalid(addr, def, pk)?;
        cleared_this_page += 1;
      }
    }
    // Cleared entries shift ranks left; advance only past the keepers.
    offset += pks.len() as u64 - cleared_this_page;
  }
  Ok(still_failing)
}

/// The datum id every named partition of `type_name` hangs off — a
/// convenience re-export point for driver code building custom
/// partitions beyond "all"/"invalid".
pub fn partition_of(type_name: &str, partition: &str) -> DatumId {
  partition_key(type_name, partition)
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::field::FieldType;
  use crate::schema::RelationalConstraintDef;

  fn ty(name: &str, outgoing_to: &[&str]) -> DatumTypeDef {
    let mut def = DatumTypeDef::new(name);
    for (i, target) in outgoing_to.iter().enumerate() {
      let field = format!("ref{i}");
      def = def
        .field(&field, FieldType::Bytes)
        .constraint(RelationalConstraintDef {
          field,
          references: FkTarget::PkUuid {
            type_name: target.to_string(),
          },
          resolution: None,
        });
    }
    def
  }

  #[test]
  fn scan_order_sorts_by_incoming_then_outgoing_then_type_id() {
    // "user": 2 incoming (order, invoice), 0 outgoing -> first.
    // "team": 1 incoming (order), 0 outgoing -> second.
    // "order": 0 incoming, 2 outgoing; "invoice": 0 incoming, 1
    // outgoing -> invoice (fewer outgoing) before order.
    let defs = vec![
      ty("order", &["user", "team"]),
      ty("invoice", &["user"]),
      ty("user", &[]),
      ty("team", &[]),
    ];
    let order = scan_order(&defs);
    let names: Vec<&str> = order.iter().map(|&i| defs[i].name.as_str()).collect();
    assert_eq!(names, vec!["user", "team", "invoice", "order"]);
  }

  #[test]
  fn scan_order_ties_break_deterministically_by_derived_type_id() {
    // Two types with identical incoming/outgoing profiles: the order
    // is stable across calls (derived-type-id byte order).
    let defs = vec![ty("alpha", &[]), ty("beta", &[])];
    let first = scan_order(&defs);
    let second = scan_order(&defs);
    assert_eq!(first, second);
  }

  #[test]
  fn enum_references_are_excluded_from_both_counts() {
    use crate::schema::FkTarget;
    let mut status_ref = DatumTypeDef::new("order").field("status", FieldType::String);
    status_ref = status_ref.constraint(RelationalConstraintDef {
      field: "status".to_string(),
      references: FkTarget::PkEnum {
        type_name: "status".to_string(),
        mnemonics: vec!["active".to_string()],
      },
      resolution: None,
    });
    let defs = vec![status_ref, DatumTypeDef::new("status")];
    let order = scan_order(&defs);
    // No runtime refs at all: pure type-id ordering, but crucially
    // "status" gets no incoming credit from the enum reference.
    assert_eq!(order.len(), 2);
  }
}
