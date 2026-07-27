//! The storage-role node's request loop: accept, decode, apply to the
//! shared delta log, reply. One global log mutex is deliberate for
//! Part A — the per-record fsync dominates latency anyway; sharding
//! the log is Part C's business. A malformed frame drops the
//! connection (the compute side treats that as fail-stop).

use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use seisin_protocol::store_wire::{
  decode_store_request, encode_store_response, StoreRequest, StoreResponse,
};
use seisin_protocol::{read_frame, write_frame};
use seisin_storage::datum_log::{DatumLog, PatchOutcome};

pub fn serve_store(listener: TcpListener, log: Arc<Mutex<DatumLog>>) {
  for stream in listener.incoming() {
    let stream = match stream {
      Ok(s) => s,
      Err(_) => continue,
    };
    let log = Arc::clone(&log);
    thread::spawn(move || handle_connection(stream, log));
  }
}

fn handle_connection(mut stream: TcpStream, log: Arc<Mutex<DatumLog>>) {
  loop {
    let payload = match read_frame(&mut stream) {
      Ok(p) => p,
      Err(_) => return, // connection closed
    };
    let request = match decode_store_request(&payload) {
      Ok(r) => r,
      Err(_) => return, // malformed: drop the connection
    };
    let response = {
      let mut log = log.lock().unwrap();
      match request {
        StoreRequest::Put { id, bytes } => match log.put_full(id.as_bytes(), &bytes) {
          Ok(()) => StoreResponse::Ack,
          Err(_) => return, // disk failure: fail-stop, drop the conn
        },
        StoreRequest::Patch { id, delta } => match log.put_delta(id.as_bytes(), &delta) {
          Ok(PatchOutcome::Applied) => StoreResponse::Ack,
          Ok(PatchOutcome::NeedFull) => StoreResponse::NeedFull,
          Err(_) => return,
        },
        StoreRequest::Get { id } => match log.get(id.as_bytes()) {
          Ok(bytes) => StoreResponse::Value { bytes },
          Err(_) => return,
        },
        StoreRequest::Delete { id } => match log.delete(id.as_bytes()) {
          Ok(()) => StoreResponse::Ack,
          Err(_) => return,
        },
        // The migration surface (Identify/ListIds/Transfer/...) is wired
        // in Storage C-1 Tasks 3–4; until then it is not served.
        _ => StoreResponse::Error {
          message: "store request not supported until Storage C-1 Tasks 3-4".to_string(),
        },
      }
    };
    if write_frame(&mut stream, &encode_store_response(&response)).is_err() {
      return;
    }
  }
}
