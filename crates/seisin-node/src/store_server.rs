//! The storage-role node's request loop: accept, decode, apply to the
//! shared delta log, reply. One global log mutex is deliberate for
//! Part A — the per-record fsync dominates latency anyway; sharding
//! the log is Part C's business. A malformed frame drops the
//! connection (the compute side treats that as fail-stop).
//!
//! State beyond the log arrives with Storage Tier Part C-1: the node's
//! own id and log identity (for `Identify`) and a self-halt heartbeat
//! (before serving anything, refuse if gossip contact has gone stale).

use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_protocol::store_wire::{
  decode_store_request, encode_store_response, StoreRequest, StoreResponse,
};
use seisin_protocol::{read_frame, write_frame};
use seisin_storage::datum_log::{DatumLog, PatchOutcome};

use crate::heartbeat::Heartbeat;

/// Everything the store request loop needs beyond a single connection.
pub struct StoreNode {
  pub log: Arc<Mutex<DatumLog>>,
  pub node_id: NodeId,
  pub heartbeat: Arc<Heartbeat>,
  /// If no gossip contact within this window, the node self-halts
  /// (answers `Error` instead of serving). Default: the suspicion
  /// timeout.
  pub self_halt_threshold: Duration,
}

pub fn serve_store(listener: TcpListener, node: Arc<StoreNode>) {
  for stream in listener.incoming() {
    let stream = match stream {
      Ok(s) => s,
      Err(_) => continue,
    };
    let node = Arc::clone(&node);
    thread::spawn(move || handle_connection(stream, node));
  }
}

fn handle_connection(mut stream: TcpStream, node: Arc<StoreNode>) {
  loop {
    let payload = match read_frame(&mut stream) {
      Ok(p) => p,
      Err(_) => return, // connection closed
    };
    let request = match decode_store_request(&payload) {
      Ok(r) => r,
      Err(_) => return, // malformed: drop the connection
    };
    // Self-halt: if this node has heard no gossip within the suspicion
    // window it may have been declared dead — stop acking rather than
    // risk serving from behind a partition. It resumes serving as soon
    // as contact returns (this is a per-request check, not a latch).
    if node.heartbeat.is_stale(node.self_halt_threshold) {
      let message = format!(
        "storage node {:?} self-halted: no gossip contact within {}ms",
        node.node_id,
        node.self_halt_threshold.as_millis()
      );
      if write_frame(
        &mut stream,
        &encode_store_response(&StoreResponse::Error { message }),
      )
      .is_err()
      {
        return;
      }
      continue;
    }
    let response = {
      let mut log = node.log.lock().unwrap();
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
        StoreRequest::Identify => StoreResponse::Identity {
          node_id: node.node_id,
          log_id: DatumId::from_bytes(log.log_id()),
        },
        // The transfer surface (ListIds/Transfer/TransferStatus/
        // FinishTransfer/Retire) is wired in Storage C-1 Task 4.
        _ => StoreResponse::Error {
          message: "store transfer surface not supported until Storage C-1 Task 4".to_string(),
        },
      }
    };
    if write_frame(&mut stream, &encode_store_response(&response)).is_err() {
      return;
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use seisin_protocol::store_wire::store_call;
  use seisin_storage::datum_log::DatumLog;

  /// Boots an in-process `StoreNode` on a fresh tempdir log and returns
  /// (address, node, tempdir-kept-alive). A huge self-halt threshold so
  /// tests without a gossip responder never spuriously self-halt.
  fn store_node(node_id: NodeId) -> (String, Arc<StoreNode>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let log = Arc::new(Mutex::new(
      DatumLog::open(&dir.path().join("datum_log.dlog")).unwrap(),
    ));
    let node = Arc::new(StoreNode {
      log,
      node_id,
      heartbeat: Arc::new(Heartbeat::new()),
      self_halt_threshold: Duration::from_secs(3600),
    });
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serving = Arc::clone(&node);
    thread::spawn(move || serve_store(listener, serving));
    (addr, node, dir)
  }

  #[test]
  fn identify_returns_node_id_and_log_id() {
    let (addr, node, _dir) = store_node(NodeId(7));
    let expected_log_id = DatumId::from_bytes(node.log.lock().unwrap().log_id());
    match store_call(&addr, &StoreRequest::Identify).unwrap() {
      StoreResponse::Identity { node_id, log_id } => {
        assert_eq!(node_id, NodeId(7));
        assert_eq!(log_id, expected_log_id);
      }
      other => panic!("expected Identity, got {other:?}"),
    }
  }

  #[test]
  fn a_fresh_heartbeat_serves() {
    let (addr, _node, _dir) = store_node(NodeId(1));
    let id = DatumId::new();
    assert_eq!(
      store_call(
        &addr,
        &StoreRequest::Put {
          id,
          bytes: b"v".to_vec()
        }
      )
      .unwrap(),
      StoreResponse::Ack
    );
  }

  #[test]
  fn a_stale_heartbeat_answers_error() {
    let dir = tempfile::tempdir().unwrap();
    let log = Arc::new(Mutex::new(
      DatumLog::open(&dir.path().join("datum_log.dlog")).unwrap(),
    ));
    // Threshold 0: any elapsed time since construction is already stale.
    let node = Arc::new(StoreNode {
      log,
      node_id: NodeId(9),
      heartbeat: Arc::new(Heartbeat::new()),
      self_halt_threshold: Duration::from_millis(0),
    });
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    thread::spawn(move || serve_store(listener, node));
    match store_call(&addr, &StoreRequest::Get { id: DatumId::new() }).unwrap() {
      StoreResponse::Error { message } => assert!(message.contains("self-halted"), "{message}"),
      other => panic!("expected Error, got {other:?}"),
    }
  }
}
