//! The custom binary wire protocol between clients and compute nodes (and,
//! in later sub-projects, between nodes themselves). No auth/encryption
//! layer by design — the system trusts the network boundary.

use std::io::{self, Read, Write};

use anyhow::{bail, Result};

use seisin_core::authority::{AuthorityIdx, ENCODED_LEN as AUTHORITY_LEN};
use seisin_core::datum::DatumId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
  Get { id: DatumId },
  Put { id: DatumId, content: Vec<u8> },
  Delete { id: DatumId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
  Value {
    content: Vec<u8>,
    authority: AuthorityIdx,
  },
  NotFound,
  Ok,
}

const OP_GET: u8 = 1;
const OP_PUT: u8 = 2;
const OP_DELETE: u8 = 3;

const RESP_VALUE: u8 = 0;
const RESP_NOT_FOUND: u8 = 1;
const RESP_OK: u8 = 2;

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
  }
  buf
}

pub fn decode_request(buf: &[u8]) -> Result<Request> {
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
}
