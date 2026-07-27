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
  decode_store_request, encode_store_response, store_call, StoreRequest, StoreResponse,
};
use seisin_protocol::{read_frame, write_frame};
use seisin_storage::datum_log::{DatumLog, PatchOutcome};

use crate::heartbeat::Heartbeat;
use crate::transfer::TransferManager;

/// Everything the store request loop needs beyond a single connection.
pub struct StoreNode {
  pub log: Arc<Mutex<DatumLog>>,
  pub node_id: NodeId,
  pub heartbeat: Arc<Heartbeat>,
  /// If no gossip contact within this window, the node self-halts
  /// (answers `Error` instead of serving). Default: the suspicion
  /// timeout.
  pub self_halt_threshold: Duration,
  pub transfers: Arc<TransferManager>,
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
    // The log lock is taken per-arm (not across the whole match) so the
    // transfer arms — which lock the log themselves via helpers — never
    // deadlock against an outer guard.
    let response = match request {
      StoreRequest::Put { id, bytes, n } => {
        match node.log.lock().unwrap().put_full(id.as_bytes(), &bytes, n) {
          Ok(()) => {
            node.transfers.note_write(id);
            StoreResponse::Ack
          }
          Err(_) => return, // disk failure: fail-stop, drop the conn
        }
      }
      StoreRequest::Patch { id, delta, n } => {
        match node.log.lock().unwrap().put_delta(id.as_bytes(), &delta, n) {
          Ok(PatchOutcome::Applied) => {
            node.transfers.note_write(id);
            StoreResponse::Ack
          }
          Ok(PatchOutcome::NeedFull) => StoreResponse::NeedFull,
          Err(_) => return,
        }
      }
      StoreRequest::Get { id } => match node.log.lock().unwrap().get(id.as_bytes()) {
        Ok(bytes) => StoreResponse::Value { bytes },
        Err(_) => return,
      },
      StoreRequest::Delete { id } => match node.log.lock().unwrap().delete(id.as_bytes()) {
        Ok(()) => {
          node.transfers.note_write(id);
          StoreResponse::Ack
        }
        Err(_) => return,
      },
      StoreRequest::Identify => StoreResponse::Identity {
        node_id: node.node_id,
        log_id: DatumId::from_bytes(node.log.lock().unwrap().log_id()),
      },
      StoreRequest::ListIds { after, limit } => {
        let after_bytes = after.map(|id| id.as_bytes());
        let ids = node
          .log
          .lock()
          .unwrap()
          .list_ids(after_bytes, limit as usize);
        let done = ids.len() < limit as usize;
        StoreResponse::IdList {
          ids: ids
            .into_iter()
            .map(|(id, n)| (DatumId::from_bytes(id), n))
            .collect(),
          done,
        }
      }
      StoreRequest::Transfer {
        transfer_id,
        ids,
        dest_address,
      } => {
        node.transfers.start(transfer_id, ids, dest_address);
        let worker = Arc::clone(&node);
        thread::spawn(move || run_transfer_copy(worker, transfer_id));
        StoreResponse::Ack
      }
      StoreRequest::TransferStatus { transfer_id } => match node.transfers.status(transfer_id) {
        Some((copied, dirty, done)) => StoreResponse::TransferProgress {
          copied,
          dirty,
          done,
        },
        None => StoreResponse::Error {
          message: format!("unknown transfer {transfer_id:?}"),
        },
      },
      StoreRequest::FinishTransfer { transfer_id } => finish_transfer(&node, transfer_id),
      StoreRequest::Retire { transfer_id } => {
        for id in node.transfers.retire(transfer_id) {
          if node.log.lock().unwrap().delete(id.as_bytes()).is_err() {
            return; // disk failure: fail-stop
          }
        }
        StoreResponse::Ack
      }
    };
    if write_frame(&mut stream, &encode_store_response(&response)).is_err() {
      return;
    }
  }
}

/// Reads the current value of `id` from the log (materializing any delta
/// chain), releasing the lock before the caller does any network I/O.
fn read_value(node: &StoreNode, id: DatumId) -> Option<Vec<u8>> {
  node.log.lock().unwrap().get(id.as_bytes()).ok().flatten()
}

/// The async bulk copy: snapshot-read each transfer id and `Put` it to
/// the destination over the store wire. A best-effort copy — the dirty
/// tail (`FinishTransfer`) re-sends anything written during it, and a
/// crashed driver simply re-runs the whole transfer.
fn run_transfer_copy(node: Arc<StoreNode>, transfer_id: DatumId) {
  let Some(dest) = node.transfers.dest(transfer_id) else {
    return;
  };
  for id in node.transfers.ids(transfer_id) {
    if let Some(bytes) = read_value(&node, id) {
      // Task 3 bridge: n=1 here; Task 4 preserves the source's stored
      // replication factor (via the log's `n_of`).
      let _ = store_call(&dest, &StoreRequest::Put { id, bytes, n: 1 });
    }
    node.transfers.bump_copied(transfer_id, 1);
  }
  node.transfers.mark_done(transfer_id);
}

/// Re-sends the dirty tail: for each id written during the copy, `Put`
/// its current value (or `Delete` it if it was removed) to the
/// destination, so the destination ends the pause holding the latest
/// value of every moved id.
fn finish_transfer(node: &Arc<StoreNode>, transfer_id: DatumId) -> StoreResponse {
  let Some(dest) = node.transfers.dest(transfer_id) else {
    return StoreResponse::Error {
      message: format!("unknown transfer {transfer_id:?}"),
    };
  };
  for id in node.transfers.take_dirty(transfer_id) {
    let request = match read_value(node, id) {
      Some(bytes) => StoreRequest::Put { id, bytes, n: 1 }, // Task 3 bridge (see run_transfer_copy)
      None => StoreRequest::Delete { id },
    };
    let _ = store_call(&dest, &request);
  }
  StoreResponse::Ack
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
      transfers: Arc::new(TransferManager::default()),
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
          bytes: b"v".to_vec(),
          n: 1,
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
      transfers: Arc::new(TransferManager::default()),
    });
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    thread::spawn(move || serve_store(listener, node));
    match store_call(&addr, &StoreRequest::Get { id: DatumId::new() }).unwrap() {
      StoreResponse::Error { message } => assert!(message.contains("self-halted"), "{message}"),
      other => panic!("expected Error, got {other:?}"),
    }
  }

  #[test]
  fn transfer_copies_then_tails_a_dirty_write() {
    let (src_addr, _src, _src_dir) = store_node(NodeId(1));
    let (dst_addr, _dst, _dst_dir) = store_node(NodeId(2));

    // Seed the source with three datums.
    let ids: Vec<DatumId> = (0..3).map(|_| DatumId::new()).collect();
    for id in &ids {
      assert_eq!(
        store_call(
          &src_addr,
          &StoreRequest::Put {
            id: *id,
            bytes: b"v0".to_vec(),
            n: 1,
          }
        )
        .unwrap(),
        StoreResponse::Ack
      );
    }

    // Start the transfer and wait for the async bulk copy to finish.
    let transfer_id = DatumId::new();
    assert_eq!(
      store_call(
        &src_addr,
        &StoreRequest::Transfer {
          transfer_id,
          ids: ids.clone(),
          dest_address: dst_addr.clone(),
        }
      )
      .unwrap(),
      StoreResponse::Ack
    );
    wait_until(|| {
      matches!(
        store_call(&src_addr, &StoreRequest::TransferStatus { transfer_id }).unwrap(),
        StoreResponse::TransferProgress { done: true, .. }
      )
    });

    // A concurrent write to one transferred id lands in the dirty tail.
    assert_eq!(
      store_call(
        &src_addr,
        &StoreRequest::Put {
          id: ids[0],
          bytes: b"v1".to_vec(),
          n: 1,
        }
      )
      .unwrap(),
      StoreResponse::Ack
    );
    assert_eq!(
      store_call(&src_addr, &StoreRequest::FinishTransfer { transfer_id }).unwrap(),
      StoreResponse::Ack
    );

    // Destination now holds every id, with the tailed value for ids[0].
    assert_eq!(get(&dst_addr, ids[0]), Some(b"v1".to_vec()));
    assert_eq!(get(&dst_addr, ids[1]), Some(b"v0".to_vec()));
    assert_eq!(get(&dst_addr, ids[2]), Some(b"v0".to_vec()));

    // Retire tombstones the moved ids on the source.
    assert_eq!(
      store_call(&src_addr, &StoreRequest::Retire { transfer_id }).unwrap(),
      StoreResponse::Ack
    );
    for id in &ids {
      assert_eq!(get(&src_addr, *id), None);
    }
  }

  fn get(addr: &str, id: DatumId) -> Option<Vec<u8>> {
    match store_call(addr, &StoreRequest::Get { id }).unwrap() {
      StoreResponse::Value { bytes } => bytes,
      other => panic!("expected Value, got {other:?}"),
    }
  }

  /// Polls `cond` up to ~2s (the async copy is fast; this just avoids a
  /// fixed sleep). Uses a bounded spin rather than a real clock — good
  /// enough for a hermetic in-process test.
  fn wait_until(cond: impl Fn() -> bool) {
    for _ in 0..2000 {
      if cond() {
        return;
      }
      std::thread::sleep(Duration::from_millis(1));
    }
    panic!("condition not met within the deadline");
  }
}
