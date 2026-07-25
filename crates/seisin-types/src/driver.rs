//! Client-side scan-driver helpers: the full-validation rescan over a
//! type's extent. The framework never runs this on a timer — a
//! solution's driver loop calls `validate_type` at the cadence the
//! type declares (`rescan_every_millis`) and acts on the findings
//! (ConflictOps, alerts) itself. Validating every type's *outgoing*
//! refs (plus the delete-side markers, scanned separately via
//! `Request::FkPending`) covers incoming validation across the schema.

use anyhow::{bail, Result};
use seisin_core::datum::DatumId;
use seisin_protocol::{Request, Response};

use crate::extent::extent_key;
use crate::field::FieldValue;
use crate::schema::{DatumTypeDef, FkTarget};
use crate::sk_index::sk_key;
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
