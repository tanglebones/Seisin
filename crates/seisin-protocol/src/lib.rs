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
  /// A read-only query against one rk index datum — client-facing,
  /// routed exactly like `Request::Op` (redirect if the receiving node
  /// isn't native for `index_datum_id`), but bypassing op collation
  /// entirely: a query touches exactly one datum and is answered
  /// synchronously by its owning thread. See the rk design doc's "Read
  /// Path" section.
  RkQuery {
    index_datum_id: DatumId,
    query: RkQueryKind,
  },
  /// A solution-called, mutating leaderboard op (update/remove) —
  /// client-facing, routed like `Op`/`RkQuery` (redirect if the
  /// receiving node isn't native for `board_id`). `class` exists only
  /// to form the registry kind string `lb:{class}`; the framework
  /// never interprets lb semantics. See the lb design doc.
  LbExecute {
    board_id: DatumId,
    class: String,
    op: LbExecuteOp,
  },
  /// A read-only leaderboard query — same routing as `LbExecute`.
  LbQuery {
    board_id: DatumId,
    class: String,
    query: LbQueryReq,
  },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LbExecuteOp {
  Update {
    player_id: DatumId,
    display: Vec<u8>,
    rank_key: [u8; 8],
    friend_ids: Vec<DatumId>,
    top: u32,
    window: u32,
  },
  Remove {
    player_id: DatumId,
  },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LbQueryReq {
  pub top: u32,
  pub bottom: u32,
  pub around_player: Option<DatumId>,
  pub window: u32,
  pub friend_ids: Vec<DatumId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LbEntry {
  pub rank_key: [u8; 8],
  pub player_id: DatumId,
  pub display: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LbFriendRank {
  pub player_id: DatumId,
  /// 0-based from best (rank 0 = highest score).
  pub rank: u64,
  pub rank_key: [u8; 8],
  pub display: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LbResult {
  pub total: u64,
  /// 0-based from best; None for a Remove result or a query without
  /// `around_player`.
  pub player_rank: Option<u64>,
  pub top: Vec<LbEntry>,
  /// Empty on execute results — bottom lists are a query concern.
  pub bottom: Vec<LbEntry>,
  pub around: Vec<LbEntry>,
  pub friends: Vec<LbFriendRank>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RkQueryKind {
  TopN(u32),
  BottomN(u32),
  PercentileSample(u32),
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
  /// Reply to an `RkQuery`: `(rank_key bytes, pk_id)` pairs in the
  /// order the query implies (descending for TopN, ascending for
  /// BottomN/PercentileSample). Rank keys are always exactly 8 bytes.
  RkQueryResult {
    entries: Vec<(Vec<u8>, DatumId)>,
  },
  /// Reply to `LbExecute`/`LbQuery`.
  LbResult(LbResult),
}

/// The wire protocol version, carried as the first byte of every
/// encoded `Request`/`Response` (and therefore inside every peer-link
/// envelope body too). Deployment policy is strict n -> n+1 rolling
/// updates with no version skipping, so when this is bumped to n+1 the
/// decoder for version n must be kept alive for one release — during a
/// rollout, version-n and version-n+1 nodes and clients coexist and
/// must decode each other's frames. Only one version exists so far, so
/// today's decoders accept exactly `PROTOCOL_VERSION`.
pub const PROTOCOL_VERSION: u8 = 1;

/// Checks and strips the leading version byte. Kept as the single
/// place a future n/n-1 dual-decode dispatch would live.
fn check_version<'a>(buf: &'a [u8], what: &str) -> Result<&'a [u8]> {
  match buf.first() {
    None => bail!("empty {what} payload"),
    Some(&v) if v == PROTOCOL_VERSION => Ok(&buf[1..]),
    Some(&v) => {
      bail!("unsupported {what} protocol version {v}; this node speaks version {PROTOCOL_VERSION}")
    }
  }
}

const OP_OP: u8 = 1;
const OP_ACQUIRE: u8 = 2;
const OP_RECALL: u8 = 3;
const OP_RELEASE: u8 = 4;
const OP_INDEX_UPDATE: u8 = 5;
const OP_RK_QUERY: u8 = 6;
const OP_LB_EXECUTE: u8 = 7;
const OP_LB_QUERY: u8 = 8;

const LB_OP_UPDATE: u8 = 0;
const LB_OP_REMOVE: u8 = 1;

const RK_QUERY_TOP_N: u8 = 0;
const RK_QUERY_BOTTOM_N: u8 = 1;
const RK_QUERY_PERCENTILE_SAMPLE: u8 = 2;
const RK_RANK_KEY_LEN: usize = 8;

const RESP_REDIRECT: u8 = 1;
const RESP_OP_RESULT: u8 = 2;
const RESP_OP_ERROR: u8 = 3;
const RESP_GRANTED: u8 = 4;
const RESP_RELEASED: u8 = 5;
const RESP_INDEX_UPDATE_RESULT: u8 = 6;
const RESP_RK_QUERY_RESULT: u8 = 7;
const RESP_LB_RESULT: u8 = 8;

const ID_LEN: usize = 16;

pub fn encode_request(req: &Request) -> Vec<u8> {
  let mut buf = vec![PROTOCOL_VERSION];
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
    Request::RkQuery {
      index_datum_id,
      query,
    } => {
      buf.push(OP_RK_QUERY);
      buf.extend_from_slice(&index_datum_id.as_bytes());
      buf.extend_from_slice(&encode_rk_query_kind(query));
    }
    Request::LbExecute {
      board_id,
      class,
      op,
    } => {
      buf.push(OP_LB_EXECUTE);
      buf.extend_from_slice(&board_id.as_bytes());
      put_bytes(&mut buf, class.as_bytes());
      buf.extend_from_slice(&encode_lb_execute_op(op));
    }
    Request::LbQuery {
      board_id,
      class,
      query,
    } => {
      buf.push(OP_LB_QUERY);
      buf.extend_from_slice(&board_id.as_bytes());
      put_bytes(&mut buf, class.as_bytes());
      buf.extend_from_slice(&encode_lb_query_req(query));
    }
  }
  buf
}

pub fn decode_request(buf: &[u8]) -> Result<Request> {
  let buf = check_version(buf, "request")?;
  if buf.is_empty() {
    bail!("empty request payload");
  }
  match buf[0] {
    OP_OP => decode_op_request(buf),
    OP_ACQUIRE => decode_acquire_request(buf),
    OP_RECALL => decode_recall_request(buf),
    OP_RELEASE => decode_release_request(buf),
    OP_INDEX_UPDATE => decode_index_update_request(buf),
    OP_RK_QUERY => decode_rk_query_request(buf),
    OP_LB_EXECUTE => decode_lb_execute_request(buf),
    OP_LB_QUERY => decode_lb_query_request(buf),
    op => bail!("unknown request opcode: {op}"),
  }
}

fn decode_lb_execute_request(buf: &[u8]) -> Result<Request> {
  let mut offset = 1;
  let board_id = take_id(buf, &mut offset)?;
  let class = String::from_utf8(take_bytes(buf, &mut offset)?)
    .context("lb class was not valid utf8")?;
  let op = decode_lb_execute_op(&buf[offset..])?;
  Ok(Request::LbExecute {
    board_id,
    class,
    op,
  })
}

fn decode_lb_query_request(buf: &[u8]) -> Result<Request> {
  let mut offset = 1;
  let board_id = take_id(buf, &mut offset)?;
  let class = String::from_utf8(take_bytes(buf, &mut offset)?)
    .context("lb class was not valid utf8")?;
  let query = decode_lb_query_req(&buf[offset..])?;
  Ok(Request::LbQuery {
    board_id,
    class,
    query,
  })
}

fn decode_rk_query_request(buf: &[u8]) -> Result<Request> {
  if buf.len() != 1 + ID_LEN + 5 {
    bail!(
      "rk query request has the wrong length: expected {} bytes, got {}",
      1 + ID_LEN + 5,
      buf.len()
    );
  }
  let index_datum_id = DatumId::from_bytes(buf[1..1 + ID_LEN].try_into().unwrap());
  let query = decode_rk_query_kind(&buf[1 + ID_LEN..])?;
  Ok(Request::RkQuery {
    index_datum_id,
    query,
  })
}

/// The rk query/entry codecs are public standalone functions (not just
/// baked into `encode_request`) because the worker treats query/result
/// bytes as opaque (`ResidentIndex::query`): the rk implementation in
/// `seisin-types` decodes the query bytes and encodes the result bytes
/// with these same functions, so the byte layout is defined exactly
/// once, here.
pub fn encode_rk_query_kind(q: &RkQueryKind) -> Vec<u8> {
  let (tag, n) = match q {
    RkQueryKind::TopN(n) => (RK_QUERY_TOP_N, *n),
    RkQueryKind::BottomN(n) => (RK_QUERY_BOTTOM_N, *n),
    RkQueryKind::PercentileSample(n) => (RK_QUERY_PERCENTILE_SAMPLE, *n),
  };
  let mut buf = vec![tag];
  buf.extend_from_slice(&n.to_le_bytes());
  buf
}

pub fn decode_rk_query_kind(buf: &[u8]) -> Result<RkQueryKind> {
  if buf.len() != 5 {
    bail!("rk query kind must be exactly 5 bytes, got {}", buf.len());
  }
  let n = u32::from_le_bytes(buf[1..5].try_into().unwrap());
  match buf[0] {
    RK_QUERY_TOP_N => Ok(RkQueryKind::TopN(n)),
    RK_QUERY_BOTTOM_N => Ok(RkQueryKind::BottomN(n)),
    RK_QUERY_PERCENTILE_SAMPLE => Ok(RkQueryKind::PercentileSample(n)),
    tag => bail!("unknown rk query kind tag: {tag}"),
  }
}

/// `entries` rank keys must be exactly 8 bytes each (the fixed
/// `encode_rank_key` width) — enforced by the producers (rk's resident
/// index), validated strictly by `decode_rk_entries`.
pub fn encode_rk_entries(entries: &[(Vec<u8>, DatumId)]) -> Vec<u8> {
  let mut buf = (entries.len() as u32).to_le_bytes().to_vec();
  for (rank_key, pk_id) in entries {
    buf.extend_from_slice(rank_key);
    buf.extend_from_slice(&pk_id.as_bytes());
  }
  buf
}

pub fn decode_rk_entries(buf: &[u8]) -> Result<Vec<(Vec<u8>, DatumId)>> {
  if buf.len() < 4 {
    bail!("rk entries buffer too short for a count");
  }
  let count = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
  let entry_len = RK_RANK_KEY_LEN + ID_LEN;
  if buf.len() != 4 + count * entry_len {
    bail!(
      "rk entries length mismatch: {} entries need {} bytes, got {}",
      count,
      4 + count * entry_len,
      buf.len()
    );
  }
  let mut entries = Vec::with_capacity(count);
  for i in 0..count {
    let start = 4 + i * entry_len;
    let rank_key = buf[start..start + RK_RANK_KEY_LEN].to_vec();
    let pk_id = DatumId::from_bytes(
      buf[start + RK_RANK_KEY_LEN..start + entry_len]
        .try_into()
        .unwrap(),
    );
    entries.push((rank_key, pk_id));
  }
  Ok(entries)
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
  let op_id = DatumId::from_bytes(buf[op_id_offset..op_id_offset + ID_LEN].try_into().unwrap());
  let kind_len_offset = op_id_offset + ID_LEN;
  let kind_len = u32::from_le_bytes(
    buf[kind_len_offset..kind_len_offset + 4]
      .try_into()
      .unwrap(),
  ) as usize;
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

// --- lb codec building blocks (shared cursor-style helpers) ---

fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
  buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
  buf.extend_from_slice(bytes);
}

fn take_bytes(buf: &[u8], offset: &mut usize) -> Result<Vec<u8>> {
  if buf.len() < *offset + 4 {
    bail!("truncated length prefix at offset {offset}");
  }
  let len = u32::from_le_bytes(buf[*offset..*offset + 4].try_into().unwrap()) as usize;
  *offset += 4;
  if buf.len() < *offset + len {
    bail!("truncated byte field at offset {offset}: expected {len} bytes");
  }
  let bytes = buf[*offset..*offset + len].to_vec();
  *offset += len;
  Ok(bytes)
}

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

fn take_rank_key(buf: &[u8], offset: &mut usize) -> Result<[u8; 8]> {
  if buf.len() < *offset + 8 {
    bail!("truncated rank key at offset {offset}");
  }
  let key: [u8; 8] = buf[*offset..*offset + 8].try_into().unwrap();
  *offset += 8;
  Ok(key)
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

/// The lb codecs are public standalone functions (the rk precedent):
/// the worker treats op/query/result bytes as opaque, so the lb
/// implementation in `seisin-types` and `server.rs`'s routing both use
/// these — the byte layout is defined exactly once, here.
pub fn encode_lb_execute_op(op: &LbExecuteOp) -> Vec<u8> {
  let mut buf = Vec::new();
  match op {
    LbExecuteOp::Update {
      player_id,
      display,
      rank_key,
      friend_ids,
      top,
      window,
    } => {
      buf.push(LB_OP_UPDATE);
      put_id(&mut buf, *player_id);
      buf.extend_from_slice(rank_key);
      put_bytes(&mut buf, display);
      put_id_list(&mut buf, friend_ids);
      buf.extend_from_slice(&top.to_le_bytes());
      buf.extend_from_slice(&window.to_le_bytes());
    }
    LbExecuteOp::Remove { player_id } => {
      buf.push(LB_OP_REMOVE);
      put_id(&mut buf, *player_id);
    }
  }
  buf
}

pub fn decode_lb_execute_op(buf: &[u8]) -> Result<LbExecuteOp> {
  if buf.is_empty() {
    bail!("empty lb execute op");
  }
  let mut offset = 1;
  let op = match buf[0] {
    LB_OP_UPDATE => {
      let player_id = take_id(buf, &mut offset)?;
      let rank_key = take_rank_key(buf, &mut offset)?;
      let display = take_bytes(buf, &mut offset)?;
      let friend_ids = take_id_list(buf, &mut offset)?;
      let top = take_u32(buf, &mut offset)?;
      let window = take_u32(buf, &mut offset)?;
      LbExecuteOp::Update {
        player_id,
        display,
        rank_key,
        friend_ids,
        top,
        window,
      }
    }
    LB_OP_REMOVE => LbExecuteOp::Remove {
      player_id: take_id(buf, &mut offset)?,
    },
    tag => bail!("unknown lb execute op tag: {tag}"),
  };
  if offset != buf.len() {
    bail!("lb execute op has {} trailing bytes", buf.len() - offset);
  }
  Ok(op)
}

pub fn encode_lb_query_req(q: &LbQueryReq) -> Vec<u8> {
  let mut buf = Vec::new();
  buf.extend_from_slice(&q.top.to_le_bytes());
  buf.extend_from_slice(&q.bottom.to_le_bytes());
  match q.around_player {
    None => buf.push(0),
    Some(id) => {
      buf.push(1);
      put_id(&mut buf, id);
    }
  }
  buf.extend_from_slice(&q.window.to_le_bytes());
  put_id_list(&mut buf, &q.friend_ids);
  buf
}

pub fn decode_lb_query_req(buf: &[u8]) -> Result<LbQueryReq> {
  let mut offset = 0;
  let top = take_u32(buf, &mut offset)?;
  let bottom = take_u32(buf, &mut offset)?;
  if buf.len() < offset + 1 {
    bail!("lb query truncated at around_player flag");
  }
  let flag = buf[offset];
  offset += 1;
  let around_player = match flag {
    0 => None,
    1 => Some(take_id(buf, &mut offset)?),
    f => bail!("unknown around_player flag: {f}"),
  };
  let window = take_u32(buf, &mut offset)?;
  let friend_ids = take_id_list(buf, &mut offset)?;
  if offset != buf.len() {
    bail!("lb query has {} trailing bytes", buf.len() - offset);
  }
  Ok(LbQueryReq {
    top,
    bottom,
    around_player,
    window,
    friend_ids,
  })
}

fn put_lb_entries(buf: &mut Vec<u8>, entries: &[LbEntry]) {
  buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
  for entry in entries {
    buf.extend_from_slice(&entry.rank_key);
    put_id(buf, entry.player_id);
    put_bytes(buf, &entry.display);
  }
}

fn take_lb_entries(buf: &[u8], offset: &mut usize) -> Result<Vec<LbEntry>> {
  let count = take_u32(buf, offset)? as usize;
  let mut entries = Vec::with_capacity(count);
  for _ in 0..count {
    entries.push(LbEntry {
      rank_key: take_rank_key(buf, offset)?,
      player_id: take_id(buf, offset)?,
      display: take_bytes(buf, offset)?,
    });
  }
  Ok(entries)
}

pub fn encode_lb_result(result: &LbResult) -> Vec<u8> {
  let mut buf = result.total.to_le_bytes().to_vec();
  match result.player_rank {
    None => buf.push(0),
    Some(rank) => {
      buf.push(1);
      buf.extend_from_slice(&rank.to_le_bytes());
    }
  }
  put_lb_entries(&mut buf, &result.top);
  put_lb_entries(&mut buf, &result.bottom);
  put_lb_entries(&mut buf, &result.around);
  buf.extend_from_slice(&(result.friends.len() as u32).to_le_bytes());
  for friend in &result.friends {
    put_id(&mut buf, friend.player_id);
    buf.extend_from_slice(&friend.rank.to_le_bytes());
    buf.extend_from_slice(&friend.rank_key);
    put_bytes(&mut buf, &friend.display);
  }
  buf
}

pub fn decode_lb_result(buf: &[u8]) -> Result<LbResult> {
  let mut offset = 0;
  let total = take_u64(buf, &mut offset)?;
  if buf.len() < offset + 1 {
    bail!("lb result truncated at player_rank flag");
  }
  let flag = buf[offset];
  offset += 1;
  let player_rank = match flag {
    0 => None,
    1 => Some(take_u64(buf, &mut offset)?),
    f => bail!("unknown player_rank flag: {f}"),
  };
  let top = take_lb_entries(buf, &mut offset)?;
  let bottom = take_lb_entries(buf, &mut offset)?;
  let around = take_lb_entries(buf, &mut offset)?;
  let friend_count = take_u32(buf, &mut offset)? as usize;
  let mut friends = Vec::with_capacity(friend_count);
  for _ in 0..friend_count {
    friends.push(LbFriendRank {
      player_id: take_id(buf, &mut offset)?,
      rank: take_u64(buf, &mut offset)?,
      rank_key: take_rank_key(buf, &mut offset)?,
      display: take_bytes(buf, &mut offset)?,
    });
  }
  if offset != buf.len() {
    bail!("lb result has {} trailing bytes", buf.len() - offset);
  }
  Ok(LbResult {
    total,
    player_rank,
    top,
    bottom,
    around,
    friends,
  })
}

pub fn encode_response(resp: &Response) -> Vec<u8> {
  let mut buf = vec![PROTOCOL_VERSION];
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
    Response::RkQueryResult { entries } => {
      buf.push(RESP_RK_QUERY_RESULT);
      buf.extend_from_slice(&encode_rk_entries(entries));
    }
    Response::LbResult(result) => {
      buf.push(RESP_LB_RESULT);
      buf.extend_from_slice(&encode_lb_result(result));
    }
  }
  buf
}

pub fn decode_response(buf: &[u8]) -> Result<Response> {
  let buf = check_version(buf, "response")?;
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
    RESP_RK_QUERY_RESULT => Ok(Response::RkQueryResult {
      entries: decode_rk_entries(&buf[1..])?,
    }),
    RESP_LB_RESULT => Ok(Response::LbResult(decode_lb_result(&buf[1..])?)),
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
  fn round_trips_an_rk_query_request() {
    for query in [
      RkQueryKind::TopN(10),
      RkQueryKind::BottomN(3),
      RkQueryKind::PercentileSample(7),
    ] {
      let req = Request::RkQuery {
        index_datum_id: DatumId::new(),
        query: query.clone(),
      };
      assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
    }
  }

  #[test]
  fn round_trips_an_rk_query_result_response() {
    let resp = Response::RkQueryResult {
      entries: vec![
        (vec![1u8; 8], DatumId::new()),
        (vec![2u8; 8], DatumId::new()),
      ],
    };
    assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
  }

  #[test]
  fn round_trips_an_empty_rk_query_result() {
    let resp = Response::RkQueryResult { entries: vec![] };
    assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
  }

  #[test]
  fn rk_entry_codec_rejects_a_truncated_buffer() {
    assert!(decode_rk_entries(&encode_rk_entries(&[]))
      .unwrap()
      .is_empty());
    let mut buf = encode_rk_entries(&[(vec![1u8; 8], DatumId::new())]);
    buf.truncate(buf.len() - 1); // corrupt: short by one byte
    assert!(decode_rk_entries(&buf).is_err());
  }

  fn sample_lb_result() -> LbResult {
    LbResult {
      total: 42,
      player_rank: Some(7),
      top: vec![LbEntry {
        rank_key: [9u8; 8],
        player_id: DatumId::new(),
        display: b"Alice".to_vec(),
      }],
      bottom: vec![],
      around: vec![LbEntry {
        rank_key: [3u8; 8],
        player_id: DatumId::new(),
        display: b"Bob".to_vec(),
      }],
      friends: vec![LbFriendRank {
        player_id: DatumId::new(),
        rank: 11,
        rank_key: [2u8; 8],
        display: b"Carol".to_vec(),
      }],
    }
  }

  #[test]
  fn round_trips_lb_execute_requests() {
    for op in [
      LbExecuteOp::Update {
        player_id: DatumId::new(),
        display: b"Alice".to_vec(),
        rank_key: [5u8; 8],
        friend_ids: vec![DatumId::new(), DatumId::new()],
        top: 10,
        window: 5,
      },
      LbExecuteOp::Remove {
        player_id: DatumId::new(),
      },
    ] {
      let req = Request::LbExecute {
        board_id: DatumId::new(),
        class: "racing".to_string(),
        op: op.clone(),
      };
      assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
    }
  }

  #[test]
  fn round_trips_lb_query_requests_with_and_without_around_player() {
    for around in [None, Some(DatumId::new())] {
      let req = Request::LbQuery {
        board_id: DatumId::new(),
        class: "racing".to_string(),
        query: LbQueryReq {
          top: 12,
          bottom: 12,
          around_player: around,
          window: 4,
          friend_ids: vec![DatumId::new()],
        },
      };
      assert_eq!(decode_request(&encode_request(&req)).unwrap(), req);
    }
  }

  #[test]
  fn round_trips_an_lb_result_response() {
    let resp = Response::LbResult(sample_lb_result());
    assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
  }

  #[test]
  fn round_trips_an_empty_lb_result() {
    let resp = Response::LbResult(LbResult {
      total: 0,
      player_rank: None,
      top: vec![],
      bottom: vec![],
      around: vec![],
      friends: vec![],
    });
    assert_eq!(decode_response(&encode_response(&resp)).unwrap(), resp);
  }

  #[test]
  fn lb_result_codec_rejects_a_truncated_buffer() {
    let mut buf = encode_lb_result(&sample_lb_result());
    buf.truncate(buf.len() - 1);
    assert!(decode_lb_result(&buf).is_err());
  }

  #[test]
  fn every_encoded_frame_starts_with_the_protocol_version() {
    let req = Request::Recall {
      datum_id: DatumId::new(),
    };
    assert_eq!(encode_request(&req)[0], PROTOCOL_VERSION);
    assert_eq!(encode_response(&Response::Granted)[0], PROTOCOL_VERSION);
  }

  #[test]
  fn decode_rejects_an_unsupported_protocol_version() {
    let mut buf = encode_request(&Request::Recall {
      datum_id: DatumId::new(),
    });
    buf[0] = PROTOCOL_VERSION + 1;
    let err = decode_request(&buf).unwrap_err();
    assert!(err.to_string().contains("protocol version"), "{err}");

    let mut buf = encode_response(&Response::Granted);
    buf[0] = PROTOCOL_VERSION + 1;
    assert!(decode_response(&buf).is_err());
  }

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
    buf[1] = 99; // buf[0] is the version byte; the opcode follows it
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
