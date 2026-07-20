//! The custom binary wire protocol between clients and compute nodes (and,
//! in later sub-projects, between nodes themselves). No auth/encryption
//! layer by design — the system trusts the network boundary.

use std::io::{self, Read, Write};

use anyhow::{bail, Context, Result};

use seisin_core::authority::{AuthorityIdx, ENCODED_LEN as AUTHORITY_LEN};
use seisin_core::datum::DatumId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
  Get {
    id: DatumId,
  },
  Put {
    id: DatumId,
    content: Vec<u8>,
  },
  Delete {
    id: DatumId,
  },
  Op {
    op_name: String,
    datum_ids: Vec<DatumId>,
    payload: Vec<u8>,
  },
}

impl Request {
  /// The datum_id every single-datum `Request` variant carries.
  ///
  /// # Panics
  /// Panics on `Request::Op`, which carries multiple datum_ids —
  /// callers must match on the variant directly before calling this.
  pub fn datum_id(&self) -> DatumId {
    match self {
      Request::Get { id } | Request::Put { id, .. } | Request::Delete { id } => *id,
      Request::Op { .. } => {
        panic!("Request::Op has no single datum_id; match on the variant directly")
      }
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
  Value {
    content: Vec<u8>,
    authority: AuthorityIdx,
  },
  NotFound,
  Ok,
  Redirect {
    address: String,
  },
  OpResult {
    payload: Vec<u8>,
  },
  OpError {
    message: String,
  },
}

const OP_GET: u8 = 1;
const OP_PUT: u8 = 2;
const OP_DELETE: u8 = 3;
const OP_OP: u8 = 4;

const RESP_VALUE: u8 = 0;
const RESP_NOT_FOUND: u8 = 1;
const RESP_OK: u8 = 2;
const RESP_REDIRECT: u8 = 3;
const RESP_OP_RESULT: u8 = 4;
const RESP_OP_ERROR: u8 = 5;

const ID_LEN: usize = 16;

pub fn encode_request(req: &Request) -> Vec<u8> {
  let mut buf = Vec::new();
  match req {
    Request::Get { id } => {
      buf.push(OP_GET);
      buf.extend_from_slice(&id.as_bytes());
    }
    Request::Put { id, content } => {
      buf.push(OP_PUT);
      buf.extend_from_slice(&id.as_bytes());
      buf.extend_from_slice(content);
    }
    Request::Delete { id } => {
      buf.push(OP_DELETE);
      buf.extend_from_slice(&id.as_bytes());
    }
    Request::Op {
      op_name,
      datum_ids,
      payload,
    } => {
      buf.push(OP_OP);
      let name_bytes = op_name.as_bytes();
      buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
      buf.extend_from_slice(name_bytes);
      buf.extend_from_slice(&(datum_ids.len() as u32).to_le_bytes());
      for id in datum_ids {
        buf.extend_from_slice(&id.as_bytes());
      }
      buf.extend_from_slice(payload);
    }
  }
  buf
}

pub fn decode_request(buf: &[u8]) -> Result<Request> {
  if buf.is_empty() {
    bail!("empty request payload");
  }
  if buf[0] == OP_OP {
    return decode_op_request(buf);
  }
  if buf.len() < 1 + ID_LEN {
    bail!("request payload too short for an id: {} bytes", buf.len());
  }
  let id = DatumId::from_bytes(buf[1..1 + ID_LEN].try_into().unwrap());
  match buf[0] {
    OP_GET => Ok(Request::Get { id }),
    OP_PUT => Ok(Request::Put {
      id,
      content: buf[1 + ID_LEN..].to_vec(),
    }),
    OP_DELETE => Ok(Request::Delete { id }),
    op => bail!("unknown request opcode: {op}"),
  }
}

fn decode_op_request(buf: &[u8]) -> Result<Request> {
  if buf.len() < 5 {
    bail!(
      "op request too short for a name length: {} bytes",
      buf.len()
    );
  }
  let name_len = u32::from_le_bytes(buf[1..5].try_into().unwrap()) as usize;
  let mut offset = 5;
  if buf.len() < offset + name_len {
    bail!("op request too short for its name: expected {name_len} bytes");
  }
  let op_name = String::from_utf8(buf[offset..offset + name_len].to_vec())
    .context("op name was not valid utf8")?;
  offset += name_len;
  if buf.len() < offset + 4 {
    bail!("op request too short for a datum_ids count");
  }
  let id_count = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
  offset += 4;
  let mut datum_ids = Vec::with_capacity(id_count);
  for _ in 0..id_count {
    if buf.len() < offset + ID_LEN {
      bail!("op request truncated in datum_ids list");
    }
    let id_bytes: [u8; ID_LEN] = buf[offset..offset + ID_LEN].try_into().unwrap();
    datum_ids.push(DatumId::from_bytes(id_bytes));
    offset += ID_LEN;
  }
  let payload = buf[offset..].to_vec();
  Ok(Request::Op {
    op_name,
    datum_ids,
    payload,
  })
}

pub fn encode_response(resp: &Response) -> Vec<u8> {
  let mut buf = Vec::new();
  match resp {
    Response::Value { content, authority } => {
      buf.push(RESP_VALUE);
      buf.extend_from_slice(&authority.encode());
      buf.extend_from_slice(content);
    }
    Response::NotFound => buf.push(RESP_NOT_FOUND),
    Response::Ok => buf.push(RESP_OK),
    Response::Redirect { address } => {
      buf.push(RESP_REDIRECT);
      let addr_bytes = address.as_bytes();
      buf.extend_from_slice(&(addr_bytes.len() as u32).to_le_bytes());
      buf.extend_from_slice(addr_bytes);
    }
    Response::OpResult { payload } => {
      buf.push(RESP_OP_RESULT);
      buf.extend_from_slice(payload);
    }
    Response::OpError { message } => {
      buf.push(RESP_OP_ERROR);
      buf.extend_from_slice(message.as_bytes());
    }
  }
  buf
}

pub fn decode_response(buf: &[u8]) -> Result<Response> {
  if buf.is_empty() {
    bail!("empty response payload");
  }
  match buf[0] {
    RESP_VALUE => {
      if buf.len() < 1 + AUTHORITY_LEN {
        bail!(
          "value response too short for an authority_idx: {} bytes",
          buf.len()
        );
      }
      let authority_bytes: [u8; AUTHORITY_LEN] = buf[1..1 + AUTHORITY_LEN].try_into().unwrap();
      let authority = AuthorityIdx::decode(authority_bytes)?;
      Ok(Response::Value {
        content: buf[1 + AUTHORITY_LEN..].to_vec(),
        authority,
      })
    }
    RESP_NOT_FOUND => Ok(Response::NotFound),
    RESP_OK => Ok(Response::Ok),
    RESP_REDIRECT => {
      if buf.len() < 5 {
        bail!(
          "redirect response too short for an address length: {} bytes",
          buf.len()
        );
      }
      let addr_len = u32::from_le_bytes(buf[1..5].try_into().unwrap()) as usize;
      if buf.len() != 5 + addr_len {
        bail!(
          "redirect response length mismatch: expected {} bytes, got {}",
          5 + addr_len,
          buf.len()
        );
      }
      let address =
        String::from_utf8(buf[5..].to_vec()).context("redirect address was not valid utf8")?;
      Ok(Response::Redirect { address })
    }
    RESP_OP_RESULT => Ok(Response::OpResult {
      payload: buf[1..].to_vec(),
    }),
    RESP_OP_ERROR => {
      let message =
        String::from_utf8(buf[1..].to_vec()).context("op error message was not valid utf8")?;
      Ok(Response::OpError { message })
    }
    op => bail!("unknown response opcode: {op}"),
  }
}

/// Writes a length-prefixed frame: a 4-byte little-endian length followed
/// by `payload`.
pub fn write_frame<W: Write>(w: &mut W, payload: &[u8]) -> io::Result<()> {
  let len = u32::try_from(payload.len()).map_err(|_| {
    io::Error::new(
      io::ErrorKind::InvalidInput,
      "payload too large for a u32 frame length",
    )
  })?;
  w.write_all(&len.to_le_bytes())?;
  w.write_all(payload)?;
  w.flush()
}

/// Frames larger than this are rejected outright rather than allocated —
/// caps how much memory a single malformed or malicious length prefix can
/// make a connection handler allocate before any content is even read.
pub const MAX_FRAME_LEN: u32 = 64 * 1024 * 1024;

/// Reads a single length-prefixed frame written by `write_frame`.
pub fn read_frame<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
  let mut len_buf = [0u8; 4];
  r.read_exact(&mut len_buf)?;
  let len = u32::from_le_bytes(len_buf);
  if len > MAX_FRAME_LEN {
    return Err(io::Error::new(
      io::ErrorKind::InvalidData,
      format!("frame length {len} exceeds MAX_FRAME_LEN ({MAX_FRAME_LEN})"),
    ));
  }
  let mut payload = vec![0u8; len as usize];
  r.read_exact(&mut payload)?;
  Ok(payload)
}

#[cfg(test)]
mod tests {
  use super::*;
  use seisin_core::authority::{NodeId, ThreadId};
  use std::io::Cursor;

  #[test]
  fn round_trips_get_request() {
    let req = Request::Get { id: DatumId::new() };
    assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
  }

  #[test]
  fn round_trips_put_request() {
    let req = Request::Put {
      id: DatumId::new(),
      content: b"hello".to_vec(),
    };
    assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
  }

  #[test]
  fn round_trips_delete_request() {
    let req = Request::Delete { id: DatumId::new() };
    assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
  }

  #[test]
  fn rejects_unknown_request_opcode() {
    let mut buf = encode_request(&Request::Get { id: DatumId::new() });
    buf[0] = 99;
    assert!(decode_request(&buf).is_err());
  }

  #[test]
  fn round_trips_value_response() {
    let resp = Response::Value {
      content: b"hello".to_vec(),
      authority: AuthorityIdx::Foreign {
        node_id: NodeId(1),
        thread_id: ThreadId(2),
      },
    };
    assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
  }

  #[test]
  fn round_trips_not_found_response() {
    assert_eq!(
      decode_response(&encode_response(&Response::NotFound)).unwrap(),
      Response::NotFound
    );
  }

  #[test]
  fn round_trips_ok_response() {
    assert_eq!(
      decode_response(&encode_response(&Response::Ok)).unwrap(),
      Response::Ok
    );
  }

  #[test]
  fn frame_round_trips_over_a_buffer() {
    let mut buf = Vec::new();
    write_frame(&mut buf, b"payload bytes").unwrap();
    let mut cursor = Cursor::new(buf);
    assert_eq!(read_frame(&mut cursor).unwrap(), b"payload bytes");
  }

  #[test]
  fn rejects_a_frame_length_over_the_max() {
    let oversized_len = MAX_FRAME_LEN + 1;
    let mut cursor = Cursor::new(oversized_len.to_le_bytes().to_vec());
    let err = read_frame(&mut cursor).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
  }

  #[test]
  fn round_trips_redirect_response() {
    let resp = Response::Redirect {
      address: "127.0.0.1:7879".to_string(),
    };
    assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
  }

  #[test]
  fn rejects_redirect_with_invalid_utf8_address() {
    let mut buf = encode_response(&Response::Redirect {
      address: "x".to_string(),
    });
    // Corrupt the one address byte into an invalid UTF-8 continuation byte.
    *buf.last_mut().unwrap() = 0x80;
    assert!(decode_response(&buf).is_err());
  }

  #[test]
  fn datum_id_returns_the_id_for_every_variant() {
    let id = DatumId::new();
    assert_eq!(Request::Get { id }.datum_id(), id);
    assert_eq!(
      Request::Put {
        id,
        content: vec![]
      }
      .datum_id(),
      id
    );
    assert_eq!(Request::Delete { id }.datum_id(), id);
  }

  #[test]
  fn round_trips_op_request_with_no_datum_ids() {
    let req = Request::Op {
      op_name: "noop".to_string(),
      datum_ids: vec![],
      payload: vec![],
    };
    assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
  }

  #[test]
  fn round_trips_op_request_with_datum_ids_and_payload() {
    let req = Request::Op {
      op_name: "transfer".to_string(),
      datum_ids: vec![DatumId::new(), DatumId::new()],
      payload: b"amount:100".to_vec(),
    };
    assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
  }

  #[test]
  fn round_trips_op_result_response() {
    let resp = Response::OpResult {
      payload: b"ok".to_vec(),
    };
    assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
  }

  #[test]
  fn round_trips_op_error_response() {
    let resp = Response::OpError {
      message: "unknown op: foo".to_string(),
    };
    assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
  }

  #[test]
  #[should_panic]
  fn datum_id_panics_on_op_request() {
    Request::Op {
      op_name: "x".to_string(),
      datum_ids: vec![],
      payload: vec![],
    }
    .datum_id();
  }
}
