//! The compute<->storage wire protocol — deliberately separate from
//! the client/compute protocol so the two tiers version independently
//! (storage rolls out first in the n -> n+1 sequence precisely because
//! it sits below the schema). Same versioning policy: leading version
//! byte per frame, keep the version-n decoder for one release after
//! bumping. Transport reuses the crate's `read_frame`/`write_frame`
//! over one plain blocking TCP connection per compute worker thread.

use std::net::TcpStream;

use anyhow::{bail, Context, Result};

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_storage::delta::{decode_delta, encode_delta, Delta};

use crate::{read_frame, write_frame};

// Bumped to 2 for the migration surface (Storage Tier Part C-1), then to
// 3 when writes gained a replication factor and `IdList` became `(id, n)`
// pairs (Storage Tier Part C-2). The keep-the-old-decoder n±1 policy
// binds from the first deployed release; there have been none, so older
// decoders are dropped rather than preserved.
pub const STORE_PROTOCOL_VERSION: u8 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreRequest {
  /// Full content write — fsynced before the ack. `n` is the datum's
  /// replication factor, persisted per record.
  Put {
    id: DatumId,
    bytes: Vec<u8>,
    n: u16,
  },
  /// Byte-delta write against the current value — fsynced before the
  /// ack. `NeedFull` answers a patch for an id the log doesn't know.
  Patch {
    id: DatumId,
    delta: Delta,
    n: u16,
  },
  Get {
    id: DatumId,
  },
  Delete {
    id: DatumId,
  },
  /// A page of the id enumeration (ascending, ids strictly greater than
  /// `after`, up to `limit`) — the migration driver's per-source scan.
  ListIds {
    after: Option<DatumId>,
    limit: u32,
  },
  /// Start an async snapshot copy of `ids` to `dest_address`. Acked
  /// immediately; progress is polled via `TransferStatus`.
  Transfer {
    transfer_id: DatumId,
    ids: Vec<DatumId>,
    dest_address: String,
  },
  TransferStatus {
    transfer_id: DatumId,
  },
  /// Re-send the dirty tail (ids written during the copy) then ack.
  FinishTransfer {
    transfer_id: DatumId,
  },
  /// Tombstone the transferred ids on the source, then ack.
  Retire {
    transfer_id: DatumId,
  },
  /// Return this node's (node_id, log_id).
  Identify,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreResponse {
  Ack,
  /// The log has no base for the patched id — resend as `Put`.
  NeedFull,
  Value {
    bytes: Option<Vec<u8>>,
  },
  /// Reply to `ListIds`; each entry is `(id, replication_factor)`, and
  /// `done` is true when this page exhausted the ids.
  IdList {
    ids: Vec<(DatumId, u16)>,
    done: bool,
  },
  /// Reply to `TransferStatus`.
  TransferProgress {
    copied: u64,
    dirty: u64,
    done: bool,
  },
  /// Reply to `Identify`.
  Identity {
    node_id: NodeId,
    log_id: DatumId,
  },
  /// A refusal (self-halt) or a bad request — the compute side treats it
  /// like any other non-`Ack`/`Value` reply: fail-stop.
  Error {
    message: String,
  },
}

const REQ_PUT: u8 = 1;
const REQ_PATCH: u8 = 2;
const REQ_GET: u8 = 3;
const REQ_DELETE: u8 = 4;
const REQ_LIST_IDS: u8 = 5;
const REQ_TRANSFER: u8 = 6;
const REQ_TRANSFER_STATUS: u8 = 7;
const REQ_FINISH_TRANSFER: u8 = 8;
const REQ_RETIRE: u8 = 9;
const REQ_IDENTIFY: u8 = 10;

const RESP_ACK: u8 = 1;
const RESP_NEED_FULL: u8 = 2;
const RESP_VALUE: u8 = 3;
const RESP_ID_LIST: u8 = 4;
const RESP_TRANSFER_PROGRESS: u8 = 5;
const RESP_IDENTITY: u8 = 6;
const RESP_ERROR: u8 = 7;

const ID_LEN: usize = 16;

// --- cursor-style codec helpers (mirroring the ones in lib.rs) ---

fn put_id(buf: &mut Vec<u8>, id: DatumId) {
  buf.extend_from_slice(&id.as_bytes());
}

fn take_id(buf: &[u8], offset: &mut usize) -> Result<DatumId> {
  if buf.len() < *offset + ID_LEN {
    bail!("truncated datum id at offset {offset}");
  }
  let id = DatumId::from_bytes(buf[*offset..*offset + ID_LEN].try_into().unwrap());
  *offset += ID_LEN;
  Ok(id)
}

fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
  buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
  buf.extend_from_slice(bytes);
}

fn take_bytes(buf: &[u8], offset: &mut usize) -> Result<Vec<u8>> {
  let len = take_u32(buf, offset)? as usize;
  if buf.len() < *offset + len {
    bail!("truncated byte field at offset {offset}: expected {len} bytes");
  }
  let bytes = buf[*offset..*offset + len].to_vec();
  *offset += len;
  Ok(bytes)
}

fn take_u16(buf: &[u8], offset: &mut usize) -> Result<u16> {
  if buf.len() < *offset + 2 {
    bail!("truncated u16 at offset {offset}");
  }
  let v = u16::from_le_bytes(buf[*offset..*offset + 2].try_into().unwrap());
  *offset += 2;
  Ok(v)
}

fn take_u32(buf: &[u8], offset: &mut usize) -> Result<u32> {
  if buf.len() < *offset + 4 {
    bail!("truncated u32 at offset {offset}");
  }
  let v = u32::from_le_bytes(buf[*offset..*offset + 4].try_into().unwrap());
  *offset += 4;
  Ok(v)
}

fn take_u64(buf: &[u8], offset: &mut usize) -> Result<u64> {
  if buf.len() < *offset + 8 {
    bail!("truncated u64 at offset {offset}");
  }
  let v = u64::from_le_bytes(buf[*offset..*offset + 8].try_into().unwrap());
  *offset += 8;
  Ok(v)
}

fn put_id_list(buf: &mut Vec<u8>, ids: &[DatumId]) {
  buf.extend_from_slice(&(ids.len() as u32).to_le_bytes());
  for id in ids {
    put_id(buf, *id);
  }
}

fn take_id_list(buf: &[u8], offset: &mut usize) -> Result<Vec<DatumId>> {
  let count = take_u32(buf, offset)? as usize;
  let mut ids = Vec::with_capacity(count);
  for _ in 0..count {
    ids.push(take_id(buf, offset)?);
  }
  Ok(ids)
}

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
    StoreRequest::Put { id, bytes, n } => {
      buf.push(REQ_PUT);
      put_id(&mut buf, *id);
      buf.extend_from_slice(&n.to_le_bytes());
      buf.extend_from_slice(bytes);
    }
    StoreRequest::Patch { id, delta, n } => {
      buf.push(REQ_PATCH);
      put_id(&mut buf, *id);
      buf.extend_from_slice(&n.to_le_bytes());
      buf.extend_from_slice(&encode_delta(delta));
    }
    StoreRequest::Get { id } => {
      buf.push(REQ_GET);
      put_id(&mut buf, *id);
    }
    StoreRequest::Delete { id } => {
      buf.push(REQ_DELETE);
      put_id(&mut buf, *id);
    }
    StoreRequest::ListIds { after, limit } => {
      buf.push(REQ_LIST_IDS);
      match after {
        None => buf.push(0),
        Some(id) => {
          buf.push(1);
          put_id(&mut buf, *id);
        }
      }
      buf.extend_from_slice(&limit.to_le_bytes());
    }
    StoreRequest::Transfer {
      transfer_id,
      ids,
      dest_address,
    } => {
      buf.push(REQ_TRANSFER);
      put_id(&mut buf, *transfer_id);
      put_id_list(&mut buf, ids);
      put_bytes(&mut buf, dest_address.as_bytes());
    }
    StoreRequest::TransferStatus { transfer_id } => {
      buf.push(REQ_TRANSFER_STATUS);
      put_id(&mut buf, *transfer_id);
    }
    StoreRequest::FinishTransfer { transfer_id } => {
      buf.push(REQ_FINISH_TRANSFER);
      put_id(&mut buf, *transfer_id);
    }
    StoreRequest::Retire { transfer_id } => {
      buf.push(REQ_RETIRE);
      put_id(&mut buf, *transfer_id);
    }
    StoreRequest::Identify => {
      buf.push(REQ_IDENTIFY);
    }
  }
  buf
}

pub fn decode_store_request(buf: &[u8]) -> Result<StoreRequest> {
  let buf = check_version(buf, "request")?;
  if buf.is_empty() {
    bail!("empty store request payload");
  }
  let tag = buf[0];
  let mut offset = 1;
  match tag {
    REQ_PUT => {
      let id = take_id(buf, &mut offset)?;
      let n = take_u16(buf, &mut offset)?;
      Ok(StoreRequest::Put {
        id,
        bytes: buf[offset..].to_vec(),
        n,
      })
    }
    REQ_PATCH => {
      let id = take_id(buf, &mut offset)?;
      let n = take_u16(buf, &mut offset)?;
      Ok(StoreRequest::Patch {
        id,
        delta: decode_delta(&buf[offset..]).context("store patch carried a malformed delta")?,
        n,
      })
    }
    REQ_GET => {
      let id = take_id(buf, &mut offset)?;
      expect_end(buf, offset, "get")?;
      Ok(StoreRequest::Get { id })
    }
    REQ_DELETE => {
      let id = take_id(buf, &mut offset)?;
      expect_end(buf, offset, "delete")?;
      Ok(StoreRequest::Delete { id })
    }
    REQ_LIST_IDS => {
      if buf.len() < offset + 1 {
        bail!("list ids request truncated at the after flag");
      }
      let after = match buf[offset] {
        0 => {
          offset += 1;
          None
        }
        1 => {
          offset += 1;
          Some(take_id(buf, &mut offset)?)
        }
        f => bail!("unknown list ids after flag: {f}"),
      };
      let limit = take_u32(buf, &mut offset)?;
      expect_end(buf, offset, "list ids")?;
      Ok(StoreRequest::ListIds { after, limit })
    }
    REQ_TRANSFER => {
      let transfer_id = take_id(buf, &mut offset)?;
      let ids = take_id_list(buf, &mut offset)?;
      let dest_address = String::from_utf8(take_bytes(buf, &mut offset)?)
        .context("transfer dest_address was not valid utf8")?;
      Ok(StoreRequest::Transfer {
        transfer_id,
        ids,
        dest_address,
      })
    }
    REQ_TRANSFER_STATUS => {
      let transfer_id = take_id(buf, &mut offset)?;
      expect_end(buf, offset, "transfer status")?;
      Ok(StoreRequest::TransferStatus { transfer_id })
    }
    REQ_FINISH_TRANSFER => {
      let transfer_id = take_id(buf, &mut offset)?;
      expect_end(buf, offset, "finish transfer")?;
      Ok(StoreRequest::FinishTransfer { transfer_id })
    }
    REQ_RETIRE => {
      let transfer_id = take_id(buf, &mut offset)?;
      expect_end(buf, offset, "retire")?;
      Ok(StoreRequest::Retire { transfer_id })
    }
    REQ_IDENTIFY => {
      expect_end(buf, offset, "identify")?;
      Ok(StoreRequest::Identify)
    }
    tag => bail!("unknown store request tag: {tag}"),
  }
}

fn expect_end(buf: &[u8], offset: usize, what: &str) -> Result<()> {
  if offset != buf.len() {
    bail!("store {what} has {} trailing bytes", buf.len() - offset);
  }
  Ok(())
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
    StoreResponse::IdList { ids, done } => {
      buf.push(RESP_ID_LIST);
      buf.push(u8::from(*done));
      buf.extend_from_slice(&(ids.len() as u32).to_le_bytes());
      for (id, n) in ids {
        put_id(&mut buf, *id);
        buf.extend_from_slice(&n.to_le_bytes());
      }
    }
    StoreResponse::TransferProgress {
      copied,
      dirty,
      done,
    } => {
      buf.push(RESP_TRANSFER_PROGRESS);
      buf.extend_from_slice(&copied.to_le_bytes());
      buf.extend_from_slice(&dirty.to_le_bytes());
      buf.push(u8::from(*done));
    }
    StoreResponse::Identity { node_id, log_id } => {
      buf.push(RESP_IDENTITY);
      buf.extend_from_slice(&node_id.0.to_le_bytes());
      put_id(&mut buf, *log_id);
    }
    StoreResponse::Error { message } => {
      buf.push(RESP_ERROR);
      buf.extend_from_slice(message.as_bytes());
    }
  }
  buf
}

pub fn decode_store_response(buf: &[u8]) -> Result<StoreResponse> {
  let buf = check_version(buf, "response")?;
  if buf.is_empty() {
    bail!("empty store response payload");
  }
  let tag = buf[0];
  let mut offset = 1;
  match tag {
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
    RESP_ID_LIST => {
      if buf.len() < offset + 1 {
        bail!("id list response truncated at the done flag");
      }
      let done = buf[offset] != 0;
      offset += 1;
      let count = take_u32(buf, &mut offset)? as usize;
      let mut ids = Vec::with_capacity(count);
      for _ in 0..count {
        let id = take_id(buf, &mut offset)?;
        let n = take_u16(buf, &mut offset)?;
        ids.push((id, n));
      }
      Ok(StoreResponse::IdList { ids, done })
    }
    RESP_TRANSFER_PROGRESS => {
      let copied = take_u64(buf, &mut offset)?;
      let dirty = take_u64(buf, &mut offset)?;
      if buf.len() < offset + 1 {
        bail!("transfer progress truncated at the done flag");
      }
      let done = buf[offset] != 0;
      Ok(StoreResponse::TransferProgress {
        copied,
        dirty,
        done,
      })
    }
    RESP_IDENTITY => {
      let node_id = NodeId(take_u64(buf, &mut offset)?);
      let log_id = take_id(buf, &mut offset)?;
      Ok(StoreResponse::Identity { node_id, log_id })
    }
    RESP_ERROR => {
      let message =
        String::from_utf8(buf[offset..].to_vec()).context("store error message not utf8")?;
      Ok(StoreResponse::Error { message })
    }
    tag => bail!("unknown store response tag: {tag}"),
  }
}

/// One request/response round trip to a storage node over a fresh
/// connection — the storage→storage transfer path and the migration
/// driver both use this. Callers that need connection reuse (the
/// per-worker `RemoteStore`) do their own pooling.
pub fn store_call(address: &str, request: &StoreRequest) -> Result<StoreResponse> {
  let mut stream =
    TcpStream::connect(address).with_context(|| format!("connecting to storage node {address}"))?;
  write_frame(&mut stream, &encode_store_request(request))?;
  let payload = read_frame(&mut stream)?;
  decode_store_response(&payload)
}

#[cfg(test)]
mod tests {
  use super::*;
  use seisin_storage::delta::diff;
  use std::net::TcpListener;
  use std::thread;

  #[test]
  fn round_trips_every_request_variant() {
    for req in [
      StoreRequest::Put {
        id: DatumId::new(),
        bytes: b"content".to_vec(),
        n: 1,
      },
      StoreRequest::Put {
        id: DatumId::new(),
        bytes: b"replicated".to_vec(),
        n: 3,
      },
      StoreRequest::Patch {
        id: DatumId::new(),
        delta: diff(b"hello world", b"hello brave world"),
        n: 2,
      },
      StoreRequest::Get { id: DatumId::new() },
      StoreRequest::Delete { id: DatumId::new() },
      StoreRequest::ListIds {
        after: None,
        limit: 100,
      },
      StoreRequest::ListIds {
        after: Some(DatumId::new()),
        limit: 5,
      },
      StoreRequest::Transfer {
        transfer_id: DatumId::new(),
        ids: vec![DatumId::new(), DatumId::new()],
        dest_address: "127.0.0.1:6999".to_string(),
      },
      StoreRequest::Transfer {
        transfer_id: DatumId::new(),
        ids: vec![],
        dest_address: String::new(),
      },
      StoreRequest::TransferStatus {
        transfer_id: DatumId::new(),
      },
      StoreRequest::FinishTransfer {
        transfer_id: DatumId::new(),
      },
      StoreRequest::Retire {
        transfer_id: DatumId::new(),
      },
      StoreRequest::Identify,
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
      StoreResponse::IdList {
        ids: vec![(DatumId::new(), 1), (DatumId::new(), 3)],
        done: true,
      },
      StoreResponse::IdList {
        ids: vec![],
        done: false,
      },
      StoreResponse::TransferProgress {
        copied: 7,
        dirty: 2,
        done: false,
      },
      StoreResponse::Identity {
        node_id: NodeId(42),
        log_id: DatumId::new(),
      },
      StoreResponse::Error {
        message: "self-halted".to_string(),
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

  #[test]
  fn store_call_round_trips_against_a_tiny_echo_listener() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    thread::spawn(move || {
      let (mut stream, _) = listener.accept().unwrap();
      let _req = read_frame(&mut stream).unwrap();
      write_frame(&mut stream, &encode_store_response(&StoreResponse::Ack)).unwrap();
    });
    let resp = store_call(&addr, &StoreRequest::Identify).unwrap();
    assert_eq!(resp, StoreResponse::Ack);
  }
}
