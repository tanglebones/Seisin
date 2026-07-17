//! Accepts client TCP connections and bridges the wire protocol to the
//! worker thread. One thread per connection; each request blocks on the
//! worker's reply before the next request on that connection is read.

use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use seisin_protocol::{decode_request, encode_response, read_frame, write_frame};

use crate::worker::WorkerHandle;

/// Runs the accept loop on `listener`, spawning one handler thread per
/// connection, until the listener errors out (e.g. the socket is closed).
pub fn serve(listener: TcpListener, worker: Arc<WorkerHandle>) {
  for stream in listener.incoming() {
    let stream = match stream {
      Ok(s) => s,
      Err(_) => continue,
    };
    let worker = Arc::clone(&worker);
    thread::spawn(move || handle_connection(stream, worker));
  }
}

fn handle_connection(mut stream: TcpStream, worker: Arc<WorkerHandle>) {
  loop {
    let payload = match read_frame(&mut stream) {
      Ok(p) => p,
      Err(_) => return, // connection closed or errored
    };
    let request = match decode_request(&payload) {
      Ok(r) => r,
      Err(_) => return, // malformed request: drop the connection
    };
    let response = worker.submit(request);
    let response_bytes = encode_response(&response);
    if write_frame(&mut stream, &response_bytes).is_err() {
      return;
    }
  }
}
