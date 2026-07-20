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
    let Request::Op {
      op_id,
      op_name,
      datum_ids,
      payload,
    } = request
    else {
      // Acquire/Recall are node-to-node only, carried over a
      // peer-link connection (see peer_link.rs) — a client should
      // never send one on this client-facing connection.
      return;
    };
    let response = handle_op_request(
      self_node_id,
      &ring,
      &address_book,
      &pool,
      op_id,
      op_name,
      datum_ids,
      payload,
    );
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
