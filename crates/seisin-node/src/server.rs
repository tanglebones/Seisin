//! Accepts client TCP connections and routes each request: serve
//! directly if this node is the datum's native owner per the current
//! ring, otherwise reply `Redirect` naming the owner's address.

use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::{NodeId, ThreadId};
use seisin_core::datum::DatumId;
use seisin_protocol::{decode_request, encode_response, read_frame, write_frame, Request, Response};
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
    let response = match &request {
      Request::Op {
        op_name,
        datum_ids,
        payload,
      } => handle_op_request(
        self_node_id,
        &ring,
        &pool,
        op_name.clone(),
        datum_ids.clone(),
        payload.clone(),
      ),
      _ => {
        let (owner_node, thread_id) = ring.read().unwrap().native(request.datum_id());
        if owner_node == self_node_id {
          pool.submit(thread_id, request)
        } else {
          match address_book.get(&owner_node) {
            Some(address) => Response::Redirect {
              address: address.clone(),
            },
            // Ring and address book disagree — a static-config bug in
            // this plan's scope; drop the connection rather than lie
            // to the client with a bogus redirect.
            None => return,
          }
        }
      }
    };
    if write_frame(&mut stream, &encode_response(&response)).is_err() {
      return;
    }
  }
}

/// Dispatches a `Request::Op`: resolves every datum_id's native owner,
/// rejects the request if any resolve to a different node (cross-node
/// collation is Sub-project 3b), picks the local thread that natively
/// owns the most of them, evicts the rest from wherever they currently
/// sit, runs the op there, and evicts anything foreign left behind
/// afterward (a simplified anti-degeneration with no peek-ahead at the
/// next queued request — see the design doc's "Op Registry & Collation
/// Mechanics" section).
fn handle_op_request(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  pool: &WorkerPool,
  op_name: String,
  datum_ids: Vec<DatumId>,
  payload: Vec<u8>,
) -> Response {
  let owners: Vec<(NodeId, ThreadId)> = {
    let ring = ring.read().unwrap();
    datum_ids.iter().map(|id| ring.native(*id)).collect()
  };

  if owners.iter().any(|(node, _)| *node != self_node_id) {
    return Response::OpError {
      message: "cross-node collation is not supported in this version".to_string(),
    };
  }

  let mut counts: HashMap<ThreadId, usize> = HashMap::new();
  for (_, thread_id) in &owners {
    *counts.entry(*thread_id).or_insert(0) += 1;
  }
  // An op with no datum_ids (e.g. one that only ever fails an unknown-
  // name lookup) has no owner to pick a destination from — thread 0 is
  // as good as any since no cache entry will be touched.
  let destination = *counts
    .iter()
    .max_by_key(|(thread_id, count)| (**count, std::cmp::Reverse(thread_id.0)))
    .map(|(thread_id, _)| thread_id)
    .unwrap_or(&ThreadId(0));

  for (id, (_, thread_id)) in datum_ids.iter().zip(owners.iter()) {
    if *thread_id != destination {
      pool.evict_single(*thread_id, *id);
    }
  }

  let result = pool.run_op(destination, op_name, datum_ids, payload);

  let ring_for_predicate = Arc::clone(ring);
  pool.evict_non_native_for(
    destination,
    Arc::new(move |id| ring_for_predicate.read().unwrap().native(id) == (self_node_id, destination)),
  );

  match result {
    Ok(payload) => Response::OpResult { payload },
    Err(message) => Response::OpError { message },
  }
}
