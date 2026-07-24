//! Accepts client TCP connections and routes each `Request::Op`: serve
//! directly if every one of its datum_ids natively belongs to this
//! node, redirect if they all belong to exactly one other node (the
//! same idea as before 3b, just generalized from a single datum_id to a
//! list) — and, once an op's datums are genuinely spread across more
//! than one node, dispatch locally anyway, relying on the destination
//! thread's own Acquire/Recall machinery (see `worker.rs`/`peer_link.rs`)
//! to pull the remote ones in over the wire.

use std::collections::{HashMap, HashSet};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_protocol::{
  decode_request, encode_response, read_frame, write_frame, Request, Response,
};
use seisin_ring::ring::Ring;

use crate::pool::WorkerPool;

/// Runs the accept loop on `listener`, spawning one handler thread per
/// connection, until the listener errors out (e.g. the socket is closed).
pub fn serve(
  listener: TcpListener,
  self_node_id: NodeId,
  ring: Arc<RwLock<Ring>>,
  address_book: Arc<HashMap<NodeId, String>>,
  pool: Arc<WorkerPool>,
) {
  for stream in listener.incoming() {
    let stream = match stream {
      Ok(s) => s,
      Err(_) => continue,
    };
    let ring = Arc::clone(&ring);
    let address_book = Arc::clone(&address_book);
    let pool = Arc::clone(&pool);
    thread::spawn(move || handle_connection(stream, self_node_id, ring, address_book, pool));
  }
}

fn handle_connection(
  mut stream: TcpStream,
  self_node_id: NodeId,
  ring: Arc<RwLock<Ring>>,
  address_book: Arc<HashMap<NodeId, String>>,
  pool: Arc<WorkerPool>,
) {
  loop {
    let payload = match read_frame(&mut stream) {
      Ok(p) => p,
      Err(_) => return, // connection closed or errored
    };
    let request = match decode_request(&payload) {
      Ok(r) => r,
      Err(_) => return, // malformed request: drop the connection
    };
    let response = match request {
      Request::Op {
        op_id,
        op_name,
        datum_ids,
        payload,
      } => handle_op_request(
        self_node_id,
        &ring,
        &address_book,
        &pool,
        op_id,
        op_name,
        datum_ids,
        payload,
      ),
      Request::RkQuery {
        index_datum_id,
        query,
      } => handle_rk_query(
        self_node_id,
        &ring,
        &address_book,
        &pool,
        index_datum_id,
        query,
      ),
      Request::LbExecute {
        board_id,
        class,
        op,
      } => handle_lb_execute(
        self_node_id,
        &ring,
        &address_book,
        &pool,
        board_id,
        class,
        op,
      ),
      Request::LbQuery {
        board_id,
        class,
        query,
      } => handle_lb_query(
        self_node_id,
        &ring,
        &address_book,
        &pool,
        board_id,
        class,
        query,
      ),
      // Acquire/Recall/Release/IndexUpdate are node-to-node only,
      // carried over a peer-link connection (see peer_link.rs) — a
      // client should never send one on this client-facing connection.
      _ => return,
    };
    if write_frame(&mut stream, &encode_response(&response)).is_err() {
      return;
    }
  }
}

/// Resolves every datum_id's native node. If they're all this node,
/// runs the op locally. If they're all exactly one *other* node,
/// redirects there (the client reconnects and retries — the same
/// mechanism a single-datum request used before 3b, just generalized).
/// Otherwise (spread across more than one node), dispatches locally —
/// the destination thread pulls in whatever it doesn't already have.
#[allow(clippy::too_many_arguments)]
fn handle_op_request(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  pool: &WorkerPool,
  op_id: DatumId,
  op_name: String,
  datum_ids: Vec<DatumId>,
  payload: Vec<u8>,
) -> Response {
  let native_nodes: HashSet<NodeId> = {
    let ring = ring.read().unwrap();
    datum_ids.iter().map(|id| ring.native(*id).0).collect()
  };

  // A single-node op whose one native node isn't this one still takes
  // the cheaper redirect path (the client reconnects directly to the
  // node that already has everything it needs) — this is unchanged
  // from Part 1. Only once an op's datums are genuinely spread across
  // more than one node does it now fall through to local dispatch,
  // relying on the destination thread's own Acquire/Recall machinery
  // to pull the remote ones in — no more outright rejection.
  if native_nodes.len() == 1 {
    let only_node = *native_nodes.iter().next().unwrap();
    if only_node != self_node_id {
      return match address_book.get(&only_node) {
        Some(address) => Response::Redirect {
          address: address.clone(),
        },
        None => Response::OpError {
          message: format!("no known address for node {only_node:?}"),
        },
      };
    }
  }

  match pool.run_op(op_id, op_name, datum_ids, payload) {
    Ok(payload) => Response::OpResult { payload },
    Err(message) => Response::OpError { message },
  }
}

/// Routes a client rk query: redirect if `index_datum_id` isn't native
/// here (same check as `handle_op_request`), else answer synchronously
/// from the owning thread's resident tree. The query kind is re-encoded
/// to the protocol's standalone codec bytes because the worker treats
/// query/result bytes as opaque (`ResidentIndex::query`) — the byte
/// layout is defined once, in seisin-protocol, shared with the rk
/// impl's decoder in seisin-types.
/// `Some(response)` if `datum_id` isn't native here (a redirect, or an
/// error if the native node's address is unknown); `None` when this
/// node should serve the request itself.
fn redirect_if_foreign(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  datum_id: DatumId,
) -> Option<Response> {
  let native_node = ring.read().unwrap().native(datum_id).0;
  if native_node == self_node_id {
    return None;
  }
  Some(match address_book.get(&native_node) {
    Some(address) => Response::Redirect {
      address: address.clone(),
    },
    None => Response::OpError {
      message: format!("no known address for node {native_node:?}"),
    },
  })
}

fn handle_rk_query(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  pool: &WorkerPool,
  index_datum_id: DatumId,
  query: seisin_protocol::RkQueryKind,
) -> Response {
  if let Some(response) = redirect_if_foreign(self_node_id, ring, address_book, index_datum_id) {
    return response;
  }
  let query_bytes = seisin_protocol::encode_rk_query_kind(&query);
  match pool.run_index_query(index_datum_id, "rk".to_string(), query_bytes) {
    Ok(result_bytes) => match seisin_protocol::decode_rk_entries(&result_bytes) {
      Ok(entries) => Response::RkQueryResult { entries },
      Err(e) => Response::OpError {
        message: format!("malformed rk query result: {e}"),
      },
    },
    Err(message) => Response::OpError { message },
  }
}

/// Routes a client lb execute op: redirect if `board_id` isn't native
/// here, else run it on the owning thread. The `class` field exists
/// only to form the registry kind string `lb:{class}` — this file
/// stays semantics-agnostic about what lb ops mean.
#[allow(clippy::too_many_arguments)]
fn handle_lb_execute(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  pool: &WorkerPool,
  board_id: DatumId,
  class: String,
  op: seisin_protocol::LbExecuteOp,
) -> Response {
  if let Some(response) = redirect_if_foreign(self_node_id, ring, address_book, board_id) {
    return response;
  }
  let payload = seisin_protocol::encode_lb_execute_op(&op);
  lb_result_response(pool.run_index_execute(board_id, format!("lb:{class}"), payload))
}

/// Routes a client lb query — same shape as `handle_lb_execute`, on
/// the read-only index-query path.
#[allow(clippy::too_many_arguments)]
fn handle_lb_query(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  pool: &WorkerPool,
  board_id: DatumId,
  class: String,
  query: seisin_protocol::LbQueryReq,
) -> Response {
  if let Some(response) = redirect_if_foreign(self_node_id, ring, address_book, board_id) {
    return response;
  }
  let payload = seisin_protocol::encode_lb_query_req(&query);
  lb_result_response(pool.run_index_query(board_id, format!("lb:{class}"), payload))
}

fn lb_result_response(result: Result<Vec<u8>, String>) -> Response {
  match result {
    Ok(bytes) => match seisin_protocol::decode_lb_result(&bytes) {
      Ok(result) => Response::LbResult(result),
      Err(e) => Response::OpError {
        message: format!("malformed lb result: {e}"),
      },
    },
    Err(message) => Response::OpError { message },
  }
}
