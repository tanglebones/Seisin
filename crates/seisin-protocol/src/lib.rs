//! The custom binary wire protocol between clients and compute nodes (and,
//! in later sub-projects, between nodes themselves). No auth/encryption
//! layer by design — the system trusts the network boundary.
//!
//! Every request is an `Op` — a plain read or write is just a trivially-
//! registered op, no different in kind from an arbitrary domain op (see
//! the design doc's "Why Get/Put/Delete Disappear as Wire Variants"
//! section). Every op carries a client-generated `op_id`, the ordering
//! token wound-wait collation (a later sub-project) uses to resolve
//! contention between two ops' collation attempts.

use std::io::{self, Read, Write};

use anyhow::{bail, Context, Result};

use seisin_core::authority::{NodeId, ThreadId};
use seisin_core::datum::DatumId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
  Op {
    op_id: DatumId,
    op_name: String,
    datum_ids: Vec<DatumId>,
    payload: Vec<u8>,
  },
  /// Node-to-node only: requests the wound-wait lock on `datum_id` at
  /// whichever thread this frame's peer-link envelope targets, on
  /// behalf of `op_id`. Never sent by a client.
  Acquire {
    op_id: DatumId,
    datum_id: DatumId,
    requester_node: NodeId,
    requester_thread: ThreadId,
  },
  /// Node-to-node only: asks the envelope-targeted thread to evict and
  /// release `datum_id` right now.
  Recall { datum_id: DatumId },
  /// Node-to-node only: tells whichever thread is native home for
  /// `datum_id` that the sender is done with it after a normal
  /// (non-recalled) op completion — grants it to the oldest waiter, if
  /// any, exactly like the local same-node release path. Acked with
  /// `Response::Released`, though the sender doesn't need to wait for
  /// it for correctness (this is fire-and-forget, same as the local
  /// case).
  Release { datum_id: DatumId },
  /// Node-to-node only: applies one update to the index datum this
  /// frame's peer-link envelope targets, on behalf of `op_id`. `payload`
  /// is opaque to the framework — interpreted by whichever
  /// `IndexHandler` is registered for `index_kind`. Never sent by a
  /// client.
  IndexUpdate {
    target: DatumId,
    op_id: DatumId,
    index_kind: String,
    payload: Vec<u8>,
  },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
  Redirect {
    address: String,
  },
  OpResult {
    payload: Vec<u8>,
  },
  OpError {
    message: String,
  },
  /// Reply to a granted `Acquire` — no content, see the design doc's
  /// "No Content In Transfer Messages" section.
  Granted,
  /// Reply to an acknowledged `Recall`.
  Released,
  /// Reply to an `IndexUpdate` — `violation` is `Some(message)` if the
  /// update was rejected (e.g. a uniqueness constraint), in which case
  /// the index's resident/stored state was left untouched.
  IndexUpdateResult {
    violation: Option<String>,
  },
}

const OP_OP: u8 = 1;
const OP_ACQUIRE: u8 = 2;
const OP_RECALL: u8 = 3;
const OP_RELEASE: u8 = 4;
const OP_INDEX_UPDATE: u8 = 5;

const RESP_REDIRECT: u8 = 1;
const RESP_OP_RESULT: u8 = 2;
const RESP_OP_ERROR: u8 = 3;
const RESP_GRANTED: u8 = 4;
const RESP_RELEASED: u8 = 5;
const RESP_INDEX_UPDATE_RESULT: u8 = 6;

const ID_LEN: usize = 16;

pub fn encode_request(req: &Request) -> Vec<u8> {
  let mut buf = Vec::new();
  match req {
    Request::Op {
      op_id,
      op_name,
      datum_ids,
      payload,
    } => {
      buf.push(OP_OP);
      buf.extend_from_slice(&op_id.as_bytes());
      let name_bytes = op_name.as_bytes();
      buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
      buf.extend_from_slice(name_bytes);
      buf.extend_from_slice(&(datum_ids.len() as u32).to_le_bytes());
      for id in datum_ids {
        buf.extend_from_slice(&id.as_bytes());
      }
      buf.extend_from_slice(payload);
    }
    Request::Acquire {
      op_id,
      datum_id,
      requester_node,
      requester_thread,
    } => {
      buf.push(OP_ACQUIRE);
      buf.extend_from_slice(&op_id.as_bytes());
      buf.extend_from_slice(&datum_id.as_bytes());
      buf.extend_from_slice(&requester_node.0.to_le_bytes());
      buf.extend_from_slice(&requester_thread.0.to_le_bytes());
    }
    Request::Recall { datum_id } => {
      buf.push(OP_RECALL);
      buf.extend_from_slice(&datum_id.as_bytes());
    }
    Request::Release { datum_id } => {
      buf.push(OP_RELEASE);
      buf.extend_from_slice(&datum_id.as_bytes());
    }
    Request::IndexUpdate {
      target,
      op_id,
      index_kind,
      payload,
    } => {
      buf.push(OP_INDEX_UPDATE);
      buf.extend_from_slice(&target.as_bytes());
      buf.extend_from_slice(&op_id.as_bytes());
      let kind_bytes = index_kind.as_bytes();
      buf.extend_from_slice(&(kind_bytes.len() as u32).to_le_bytes());
      buf.extend_from_slice(kind_bytes);
      buf.extend_from_slice(payload);
    }
  }
  buf
}

pub fn decode_request(buf: &[u8]) -> Result<Request> {
  if buf.is_empty() {
    bail!("empty request payload");
  }
  match buf[0] {
    OP_OP => decode_op_request(buf),
    OP_ACQUIRE => decode_acquire_request(buf),
    OP_RECALL => decode_recall_request(buf),
    OP_RELEASE => decode_release_request(buf),
    OP_INDEX_UPDATE => decode_index_update_request(buf),
    op => bail!("unknown request opcode: {op}"),
  }
}

fn decode_acquire_request(buf: &[u8]) -> Result<Request> {
  if buf.len() != 1 + ID_LEN + ID_LEN + 8 + 4 {
    bail!(
      "acquire request has the wrong length: expected {} bytes, got {}",
      1 + ID_LEN + ID_LEN + 8 + 4,
      buf.len()
    );
  }
  let op_id = DatumId::from_bytes(buf[1..1 + ID_LEN].try_into().unwrap());
  let datum_id = DatumId::from_bytes(buf[1 + ID_LEN..1 + 2 * ID_LEN].try_into().unwrap());
  let node_offset = 1 + 2 * ID_LEN;
  let requester_node = NodeId(u64::from_le_bytes(
    buf[node_offset..node_offset + 8].try_into().unwrap(),
  ));
  let thread_offset = node_offset + 8;
  let requester_thread = ThreadId(u32::from_le_bytes(
    buf[thread_offset..thread_offset + 4].try_into().unwrap(),
  ));
  Ok(Request::Acquire {
    op_id,
    datum_id,
    requester_node,
    requester_thread,
  })
}

fn decode_recall_request(buf: &[u8]) -> Result<Request> {
  if buf.len() != 1 + ID_LEN {
    bail!(
      "recall request has the wrong length: expected {} bytes, got {}",
      1 + ID_LEN,
      buf.len()
    );
  }
  let datum_id = DatumId::from_bytes(buf[1..1 + ID_LEN].try_into().unwrap());
  Ok(Request::Recall { datum_id })
}

fn decode_release_request(buf: &[u8]) -> Result<Request> {
  if buf.len() != 1 + ID_LEN {
    bail!(
      "release request has the wrong length: expected {} bytes, got {}",
      1 + ID_LEN,
      buf.len()
    );
  }
  let datum_id = DatumId::from_bytes(buf[1..1 + ID_LEN].try_into().unwrap());
  Ok(Request::Release { datum_id })
}

fn decode_index_update_request(buf: &[u8]) -> Result<Request> {
  if buf.len() < 1 + ID_LEN + ID_LEN + 4 {
    bail!("index update request too short for its fixed fields");
  }
  let target = DatumId::from_bytes(buf[1..1 + ID_LEN].try_into().unwrap());
  let op_id_offset = 1 + ID_LEN;
  let op_id = DatumId::from_bytes(
    buf[op_id_offset..op_id_offset + ID_LEN]
      .try_into()
      .unwrap(),
  );
  let kind_len_offset = op_id_offset + ID_LEN;
  let kind_len =
    u32::from_le_bytes(buf[kind_len_offset..kind_len_offset + 4].try_into().unwrap()) as usize;
  let kind_offset = kind_len_offset + 4;
  if buf.len() < kind_offset + kind_len {
    bail!("index update request too short for its index_kind");
  }
  let index_kind = String::from_utf8(buf[kind_offset..kind_offset + kind_len].to_vec())
    .context("index_kind was not valid utf8")?;
  let payload = buf[kind_offset + kind_len..].to_vec();
  Ok(Request::IndexUpdate {
    target,
    op_id,
    index_kind,
    payload,
  })
}

fn decode_op_request(buf: &[u8]) -> Result<Request> {
  if buf.len() < 1 + ID_LEN {
    bail!("op request too short for an op_id: {} bytes", buf.len());
  }
  let op_id = DatumId::from_bytes(buf[1..1 + ID_LEN].try_into().unwrap());
  let mut offset = 1 + ID_LEN;

  if buf.len() < offset + 4 {
    bail!("op request too short for a name length");
  }
  let name_len = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
  offset += 4;
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
    op_id,
    op_name,
    datum_ids,
    payload,
  })
}

pub fn encode_response(resp: &Response) -> Vec<u8> {
  let mut buf = Vec::new();
  match resp {
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
    Response::Granted => buf.push(RESP_GRANTED),
    Response::Released => buf.push(RESP_RELEASED),
    Response::IndexUpdateResult { violation } => {
      buf.push(RESP_INDEX_UPDATE_RESULT);
      match violation {
        None => buf.push(0),
        Some(message) => {
          buf.push(1);
          buf.extend_from_slice(message.as_bytes());
        }
      }
    }
  }
  buf
}

pub fn decode_response(buf: &[u8]) -> Result<Response> {
  if buf.is_empty() {
    bail!("empty response payload");
  }
  match buf[0] {
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
    RESP_GRANTED => Ok(Response::Granted),
    RESP_RELEASED => Ok(Response::Released),
    RESP_INDEX_UPDATE_RESULT => {
      if buf.len() < 2 {
        bail!("index update result too short for its flag byte");
      }
      let violation = match buf[1] {
        0 => None,
        1 => Some(
          String::from_utf8(buf[2..].to_vec())
            .context("index update violation message was not valid utf8")?,
        ),
        flag => bail!("unknown index update result flag: {flag}"),
      };
      Ok(Response::IndexUpdateResult { violation })
    }
    op => bail!("unknown response opcode: {op}"),
  }
}

/// Which of the two peer-link message flows this envelope carries —
/// peer-link connections are bidirectional and multiplexed, so an
/// incoming frame could be either a response to one of *our* earlier
/// calls or a fresh incoming request from the peer; the reader needs
/// this tag to know which.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvelopeKind {
  Request,
  Response,
}

/// A peer-link frame: `body` is an encoded `Request` or `Response`
/// (per `kind`), tagged with a `correlation_id` so many concurrent
/// calls can share one connection, and (for requests only) a
/// `target_thread` naming which local worker thread on the receiving
/// node should handle it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
  pub correlation_id: u64,
  pub kind: EnvelopeKind,
  pub target_thread: ThreadId,
  pub body: Vec<u8>,
}

const ENVELOPE_KIND_REQUEST: u8 = 0;
const ENVELOPE_KIND_RESPONSE: u8 = 1;

pub fn encode_envelope(env: &Envelope) -> Vec<u8> {
  let mut buf = Vec::with_capacity(13 + env.body.len());
  buf.extend_from_slice(&env.correlation_id.to_le_bytes());
  buf.push(match env.kind {
    EnvelopeKind::Request => ENVELOPE_KIND_REQUEST,
    EnvelopeKind::Response => ENVELOPE_KIND_RESPONSE,
  });
  buf.extend_from_slice(&env.target_thread.0.to_le_bytes());
  buf.extend_from_slice(&env.body);
  buf
}

pub fn decode_envelope(buf: &[u8]) -> Result<Envelope> {
  if buf.len() < 13 {
    bail!("envelope too short for its header: {} bytes", buf.len());
  }
  let correlation_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
  let kind = match buf[8] {
    ENVELOPE_KIND_REQUEST => EnvelopeKind::Request,
    ENVELOPE_KIND_RESPONSE => EnvelopeKind::Response,
    tag => bail!("invalid envelope kind tag: {tag}"),
  };
  let target_thread = ThreadId(u32::from_le_bytes(buf[9..13].try_into().unwrap()));
  let body = buf[13..].to_vec();
  Ok(Envelope {
    correlation_id,
    kind,
    target_thread,
    body,
  })
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
  use std::io::Cursor;

  #[test]
  fn round_trips_op_request_with_no_datum_ids() {
    let req = Request::Op {
      op_id: DatumId::new(),
      op_name: "noop".to_string(),
      datum_ids: vec![],
      payload: vec![],
    };
    assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
  }

  #[test]
  fn round_trips_op_request_with_datum_ids_and_payload() {
    let req = Request::Op {
      op_id: DatumId::new(),
      op_name: "transfer".to_string(),
      datum_ids: vec![DatumId::new(), DatumId::new()],
      payload: b"amount:100".to_vec(),
    };
    assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
  }

  #[test]
  fn rejects_unknown_request_opcode() {
    let mut buf = encode_request(&Request::Op {
      op_id: DatumId::new(),
      op_name: "noop".to_string(),
      datum_ids: vec![],
      payload: vec![],
    });
    buf[0] = 99;
    assert!(decode_request(&buf).is_err());
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
  fn round_trips_acquire_request() {
    let req = Request::Acquire {
      op_id: DatumId::new(),
      datum_id: DatumId::new(),
      requester_node: seisin_core::authority::NodeId(7),
      requester_thread: seisin_core::authority::ThreadId(3),
    };
    assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
  }

  #[test]
  fn round_trips_recall_request() {
    let req = Request::Recall {
      datum_id: DatumId::new(),
    };
    assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
  }

  #[test]
  fn round_trips_release_request() {
    let req = Request::Release {
      datum_id: DatumId::new(),
    };
    assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
  }

  #[test]
  fn round_trips_granted_response() {
    assert_eq!(
      decode_response(&encode_response(&Response::Granted)).unwrap(),
      Response::Granted
    );
  }

  #[test]
  fn round_trips_released_response() {
    assert_eq!(
      decode_response(&encode_response(&Response::Released)).unwrap(),
      Response::Released
    );
  }

  #[test]
  fn rejects_a_truncated_acquire_request() {
    let mut buf = encode_request(&Request::Acquire {
      op_id: DatumId::new(),
      datum_id: DatumId::new(),
      requester_node: seisin_core::authority::NodeId(1),
      requester_thread: seisin_core::authority::ThreadId(0),
    });
    buf.truncate(buf.len() - 1);
    assert!(decode_request(&buf).is_err());
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
  fn round_trips_a_request_envelope() {
    let env = Envelope {
      correlation_id: 42,
      kind: EnvelopeKind::Request,
      target_thread: seisin_core::authority::ThreadId(3),
      body: encode_request(&Request::Recall {
        datum_id: DatumId::new(),
      }),
    };
    assert_eq!(decode_envelope(&encode_envelope(&env)).unwrap(), env);
  }

  #[test]
  fn round_trips_a_response_envelope() {
    let env = Envelope {
      correlation_id: 7,
      kind: EnvelopeKind::Response,
      target_thread: seisin_core::authority::ThreadId(0),
      body: encode_response(&Response::Granted),
    };
    assert_eq!(decode_envelope(&encode_envelope(&env)).unwrap(), env);
  }

  #[test]
  fn rejects_an_envelope_too_short_for_its_header() {
    assert!(decode_envelope(&[0u8; 5]).is_err());
  }

  #[test]
  fn rejects_an_envelope_with_an_invalid_kind_tag() {
    let mut buf = encode_envelope(&Envelope {
      correlation_id: 1,
      kind: EnvelopeKind::Request,
      target_thread: seisin_core::authority::ThreadId(0),
      body: vec![],
    });
    buf[8] = 99;
    assert!(decode_envelope(&buf).is_err());
  }

  #[test]
  fn round_trips_index_update_request() {
    let req = Request::IndexUpdate {
      target: DatumId::new(),
      op_id: DatumId::new(),
      index_kind: "sk".to_string(),
      payload: vec![1, 2, 3],
    };
    let encoded = encode_request(&req);
    assert_eq!(decode_request(&encoded).unwrap(), req);
  }

  #[test]
  fn round_trips_index_update_request_with_empty_payload() {
    let req = Request::IndexUpdate {
      target: DatumId::new(),
      op_id: DatumId::new(),
      index_kind: "sk".to_string(),
      payload: vec![],
    };
    let encoded = encode_request(&req);
    assert_eq!(decode_request(&encoded).unwrap(), req);
  }

  #[test]
  fn round_trips_index_update_result_without_violation() {
    let resp = Response::IndexUpdateResult { violation: None };
    let encoded = encode_response(&resp);
    assert_eq!(decode_response(&encoded).unwrap(), resp);
  }

  #[test]
  fn round_trips_index_update_result_with_violation() {
    let resp = Response::IndexUpdateResult {
      violation: Some("duplicate value".to_string()),
    };
    let encoded = encode_response(&resp);
    assert_eq!(decode_response(&encoded).unwrap(), resp);
  }
}
