//! Accepts client TCP connections and routes each request: serve
//! directly if this node is the datum's native owner per the current
//! ring, otherwise reply `Redirect` naming the owner's address.

use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use seisin_core::authority::NodeId;
use seisin_protocol::{decode_request, encode_response, read_frame, write_frame, Response};
use seisin_ring::ring::Ring;

use crate::pool::WorkerPool;

/// Runs the accept loop on `listener`, spawning one handler thread per
/// connection, until the listener errors out (e.g. the socket is closed).
pub fn serve(
  listener: TcpListener,
  self_node_id: NodeId,
  ring: Arc<Ring>,
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
  ring: Arc<Ring>,
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
    let (owner_node, thread_id) = ring.native(request.datum_id());
    let response = if owner_node == self_node_id {
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
    };
    if write_frame(&mut stream, &encode_response(&response)).is_err() {
      return;
    }
  }
}
