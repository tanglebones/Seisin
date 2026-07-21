//! Client-side orchestration for a typed write: the two-round-trip flow
//! (read the old value, then issue the actual write declaring every
//! datum it touches up front) the design doc's "sk Index" section
//! settles on, since collation requires an op to declare every datum_id
//! it needs before execution starts.

use anyhow::{bail, Result};
use seisin_core::datum::DatumId;
use seisin_protocol::{Request, Response};

use crate::decode_datum;
use crate::field::FieldValue;
use crate::schema::{DatumTypeDef, IndexDef};
use crate::sk_index::sk_key;
use crate::typed_write::decode_write_result;

/// Performs a typed write of `pk_id` against the server at `addr`.
/// `read_op_name` must be a registered op that returns `pk_id`'s raw
/// content (or empty bytes if it doesn't exist yet) given `datum_ids:
/// [pk_id]`; `write_op_name` must be one whose handler decodes its
/// payload via `decode_datum` and calls `write_typed_datum`, returning
/// `encode_write_result`'s bytes as its own response payload — see
/// `crates/seisin-types/tests/integration_typed_write_client.rs` for a
/// worked example of registering both.
///
/// Returns `Some((conflict_op_name, sk_key))` if the write reported a
/// uniqueness violation. This function does **not** make the follow-up
/// call itself — deciding whether/how to call the resolution op is left
/// to the caller (there's no framework mechanism for one op to invoke
/// another in-process; see this plan's Global Constraints), so a caller
/// that wants a fully automatic resolution issues its own
/// `seisin_client::call` to `conflict_op_name` afterward.
pub fn write_typed_datum_client(
  addr: &str,
  read_op_name: &str,
  write_op_name: &str,
  def: &DatumTypeDef,
  pk_id: DatumId,
  values: &[FieldValue],
) -> Result<Option<(String, DatumId)>> {
  let read_response = seisin_client::call(
    addr,
    Request::Op {
      op_id: DatumId::new(),
      op_name: read_op_name.to_string(),
      datum_ids: vec![pk_id],
      payload: vec![],
    },
  )?;
  let old_bytes = match read_response {
    Response::OpResult { payload } if payload.is_empty() => None,
    Response::OpResult { payload } => Some(payload),
    Response::OpError { message } => bail!("read op {read_op_name:?} failed: {message}"),
    other => bail!("unexpected response from read op {read_op_name:?}: {other:?}"),
  };
  let old_values = old_bytes.map(|bytes| decode_datum(def, &bytes)).transpose()?;

  let mut datum_ids = vec![pk_id];
  for index in &def.indexes {
    let IndexDef::Sk { field, .. } = index;
    let field_idx = def
      .fields
      .iter()
      .position(|(name, _)| name == field)
      .ok_or_else(|| anyhow::anyhow!("index declared on unknown field {field:?}"))?;
    let new_key = sk_key(&def.name, field, &values[field_idx])?;
    datum_ids.push(new_key);
    if let Some(old_values) = &old_values {
      let old_key = sk_key(&def.name, field, &old_values[field_idx])?;
      if old_key != new_key {
        datum_ids.push(old_key);
      }
    }
  }

  let payload = crate::encode_datum(def, values)?;
  let write_response = seisin_client::call(
    addr,
    Request::Op {
      op_id: DatumId::new(),
      op_name: write_op_name.to_string(),
      datum_ids,
      payload,
    },
  )?;
  match write_response {
    Response::OpResult { payload } => {
      let result = decode_write_result(&payload)?;
      Ok(result.violation.map(|v| (v.conflict_op, v.sk_key)))
    }
    Response::OpError { message } => bail!("write op {write_op_name:?} failed: {message}"),
    other => bail!("unexpected response from write op {write_op_name:?}: {other:?}"),
  }
}
