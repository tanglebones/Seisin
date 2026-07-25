//! The compute<->storage wire protocol — deliberately separate from
//! the client/compute protocol so the two tiers version independently
//! (storage rolls out first in the n -> n+1 sequence precisely because
//! it sits below the schema). Same versioning policy: leading version
//! byte per frame, keep the version-n decoder for one release after
//! bumping. Transport reuses the crate's `read_frame`/`write_frame`
//! over one plain blocking TCP connection per compute worker thread.

use anyhow::{bail, Context, Result};

use seisin_core::datum::DatumId;
use seisin_storage::delta::{decode_delta, encode_delta, Delta};

pub const STORE_PROTOCOL_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreRequest {
  /// Full content write — fsynced before the ack.
  Put {
    id: DatumId,
    bytes: Vec<u8>,
  },
  /// Byte-delta write against the current value — fsynced before the
  /// ack. `NeedFull` answers a patch for an id the log doesn't know.
  Patch {
    id: DatumId,
    delta: Delta,
  },
  Get {
    id: DatumId,
  },
  Delete {
    id: DatumId,
  },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreResponse {
  Ack,
  /// The log has no base for the patched id — resend as `Put`.
  NeedFull,
  Value {
    bytes: Option<Vec<u8>>,
  },
}

const REQ_PUT: u8 = 1;
const REQ_PATCH: u8 = 2;
const REQ_GET: u8 = 3;
const REQ_DELETE: u8 = 4;

const RESP_ACK: u8 = 1;
const RESP_NEED_FULL: u8 = 2;
const RESP_VALUE: u8 = 3;

fn check_version<'a>(buf: &'a [u8], what: &str) -> Result<&'a [u8]> {
  match buf.first() {
    None => bail!("empty store {what} payload"),
    Some(&v) if v == STORE_PROTOCOL_VERSION => Ok(&buf[1..]),
    Some(&v) => bail!(
      "unsupported store {what} protocol version {v}; this node speaks {STORE_PROTOCOL_VERSION}"
    ),
  }
}

pub fn encode_store_request(req: &StoreRequest) -> Vec<u8> {
  let mut buf = vec![STORE_PROTOCOL_VERSION];
  match req {
    StoreRequest::Put { id, bytes } => {
      buf.push(REQ_PUT);
      buf.extend_from_slice(&id.as_bytes());
      buf.extend_from_slice(bytes);
    }
    StoreRequest::Patch { id, delta } => {
      buf.push(REQ_PATCH);
      buf.extend_from_slice(&id.as_bytes());
      buf.extend_from_slice(&encode_delta(delta));
    }
    StoreRequest::Get { id } => {
      buf.push(REQ_GET);
      buf.extend_from_slice(&id.as_bytes());
    }
    StoreRequest::Delete { id } => {
      buf.push(REQ_DELETE);
      buf.extend_from_slice(&id.as_bytes());
    }
  }
  buf
}

pub fn decode_store_request(buf: &[u8]) -> Result<StoreRequest> {
  let buf = check_version(buf, "request")?;
  if buf.len() < 17 {
    bail!("store request too short: {} bytes", buf.len());
  }
  let id = DatumId::from_bytes(buf[1..17].try_into().unwrap());
  let body = &buf[17..];
  match buf[0] {
    REQ_PUT => Ok(StoreRequest::Put {
      id,
      bytes: body.to_vec(),
    }),
    REQ_PATCH => Ok(StoreRequest::Patch {
      id,
      delta: decode_delta(body).context("store patch carried a malformed delta")?,
    }),
    REQ_GET => {
      if !body.is_empty() {
        bail!("store get has {} trailing bytes", body.len());
      }
      Ok(StoreRequest::Get { id })
    }
    REQ_DELETE => {
      if !body.is_empty() {
        bail!("store delete has {} trailing bytes", body.len());
      }
      Ok(StoreRequest::Delete { id })
    }
    tag => bail!("unknown store request tag: {tag}"),
  }
}

pub fn encode_store_response(resp: &StoreResponse) -> Vec<u8> {
  let mut buf = vec![STORE_PROTOCOL_VERSION];
  match resp {
    StoreResponse::Ack => buf.push(RESP_ACK),
    StoreResponse::NeedFull => buf.push(RESP_NEED_FULL),
    StoreResponse::Value { bytes } => {
      buf.push(RESP_VALUE);
      match bytes {
        None => buf.push(0),
        Some(bytes) => {
          buf.push(1);
          buf.extend_from_slice(bytes);
        }
      }
    }
  }
  buf
}

pub fn decode_store_response(buf: &[u8]) -> Result<StoreResponse> {
  let buf = check_version(buf, "response")?;
  if buf.is_empty() {
    bail!("empty store response payload");
  }
  match buf[0] {
    RESP_ACK => Ok(StoreResponse::Ack),
    RESP_NEED_FULL => Ok(StoreResponse::NeedFull),
    RESP_VALUE => {
      if buf.len() < 2 {
        bail!("store value response missing its presence flag");
      }
      match buf[1] {
        0 => Ok(StoreResponse::Value { bytes: None }),
        1 => Ok(StoreResponse::Value {
          bytes: Some(buf[2..].to_vec()),
        }),
        flag => bail!("unknown store value flag: {flag}"),
      }
    }
    tag => bail!("unknown store response tag: {tag}"),
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use seisin_storage::delta::diff;

  #[test]
  fn round_trips_every_request_variant() {
    for req in [
      StoreRequest::Put {
        id: DatumId::new(),
        bytes: b"content".to_vec(),
      },
      StoreRequest::Patch {
        id: DatumId::new(),
        delta: diff(b"hello world", b"hello brave world"),
      },
      StoreRequest::Get { id: DatumId::new() },
      StoreRequest::Delete { id: DatumId::new() },
    ] {
      assert_eq!(
        decode_store_request(&encode_store_request(&req)).unwrap(),
        req
      );
    }
  }

  #[test]
  fn round_trips_every_response_variant() {
    for resp in [
      StoreResponse::Ack,
      StoreResponse::NeedFull,
      StoreResponse::Value { bytes: None },
      StoreResponse::Value {
        bytes: Some(b"content".to_vec()),
      },
    ] {
      assert_eq!(
        decode_store_response(&encode_store_response(&resp)).unwrap(),
        resp
      );
    }
  }

  #[test]
  fn rejects_unsupported_versions_and_unknown_tags() {
    let mut buf = encode_store_request(&StoreRequest::Get { id: DatumId::new() });
    buf[0] = STORE_PROTOCOL_VERSION + 1;
    assert!(decode_store_request(&buf).is_err());
    let mut buf = encode_store_response(&StoreResponse::Ack);
    buf[1] = 99;
    assert!(decode_store_response(&buf).is_err());
  }
}
