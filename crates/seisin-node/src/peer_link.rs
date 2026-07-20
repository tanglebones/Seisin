//! A single persistent, multiplexed connection to one remote node,
//! carrying `Acquire`/`Recall` traffic for every local thread's
//! cross-node collation needs — see the design doc's "Server-to-Server
//! Connection Multiplexing" section. One `PeerLink` per node pair,
//! never one per thread, keeps connection count at O(servers), not
//! O(threads^2).
//!
//! This module is deliberately unaware of `WorkerMessage` — callers
//! supply an `on_request` callback that translates an incoming
//! `Request` into whatever local dispatch they need; `PeerLink` itself
//! only ever deals in `Request`/`Response`/`Envelope`.

use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use seisin_core::authority::ThreadId;
use seisin_protocol::{
  decode_envelope, decode_request, decode_response, encode_envelope, encode_request,
  encode_response, read_frame, write_frame, Envelope, EnvelopeKind, Request, Response,
};

/// A pending call's completion callback, invoked with the eventual
/// `Response` (or a synthetic `Response::OpError` if the link drops
/// first).
type ResponseCallback = Box<dyn FnOnce(Response) + Send>;

pub struct PeerLink {
  writer_tx: Sender<Vec<u8>>,
  pending: Mutex<HashMap<u64, ResponseCallback>>,
  next_correlation_id: AtomicU64,
}

impl PeerLink {
  /// Sends `request` to `target_thread` on the peer, invoking
  /// `on_response` once a reply arrives — or, if the connection drops
  /// before one does, with a synthetic `Response::OpError`. Never
  /// blocks the caller; `on_response` runs on this link's reader
  /// thread, so it should do nothing more than post a message
  /// somewhere (matching the same pattern `NativeLock`'s `on_granted`
  /// callback already uses).
  pub fn call(
    &self,
    target_thread: ThreadId,
    request: Request,
    on_response: Box<dyn FnOnce(Response) + Send>,
  ) {
    let correlation_id = self.next_correlation_id.fetch_add(1, Ordering::Relaxed);
    self
      .pending
      .lock()
      .unwrap()
      .insert(correlation_id, on_response);
    let envelope = Envelope {
      correlation_id,
      kind: EnvelopeKind::Request,
      target_thread,
      body: encode_request(&request),
    };
    let _ = self.writer_tx.send(encode_envelope(&envelope));
  }

  /// Replies to a request this link's `on_request` callback was
  /// previously handed, tagged with the same `correlation_id` it
  /// arrived with.
  pub fn respond(&self, correlation_id: u64, response: Response) {
    let envelope = Envelope {
      correlation_id,
      kind: EnvelopeKind::Response,
      target_thread: ThreadId(0), // unused for responses
      body: encode_response(&response),
    };
    let _ = self.writer_tx.send(encode_envelope(&envelope));
  }
}

/// Spawns the writer and reader threads for an already-connected
/// `stream` and returns the shared handle both sides use.
/// `on_request` is invoked (from the reader thread) for every incoming
/// request frame, with `(target_thread, request, link, correlation_id)`
/// — implementations should dispatch locally and eventually call
/// `link.respond(correlation_id, ...)`.
pub fn spawn(
  stream: TcpStream,
  on_request: Arc<dyn Fn(ThreadId, Request, Arc<PeerLink>, u64) + Send + Sync>,
) -> Arc<PeerLink> {
  let (writer_tx, writer_rx) = mpsc::channel::<Vec<u8>>();
  let link = Arc::new(PeerLink {
    writer_tx,
    pending: Mutex::new(HashMap::new()),
    next_correlation_id: AtomicU64::new(0),
  });

  let mut write_stream = stream
    .try_clone()
    .expect("failed to clone peer-link stream for its writer half");
  thread::spawn(move || {
    for envelope_bytes in writer_rx {
      if write_frame(&mut write_stream, &envelope_bytes).is_err() {
        break;
      }
    }
  });

  let mut read_stream = stream;
  let reader_link = Arc::clone(&link);
  thread::spawn(move || {
    while let Ok(frame) = read_frame(&mut read_stream) {
      let envelope = match decode_envelope(&frame) {
        Ok(e) => e,
        Err(_) => continue,
      };
      match envelope.kind {
        EnvelopeKind::Response => {
          let callback = reader_link
            .pending
            .lock()
            .unwrap()
            .remove(&envelope.correlation_id);
          if let Some(callback) = callback {
            if let Ok(response) = decode_response(&envelope.body) {
              callback(response);
            }
          }
        }
        EnvelopeKind::Request => {
          if let Ok(request) = decode_request(&envelope.body) {
            on_request(
              envelope.target_thread,
              request,
              Arc::clone(&reader_link),
              envelope.correlation_id,
            );
          }
        }
      }
    }
    // Connection dropped: fail every still-pending call so callers
    // don't hang forever waiting for a response that will never
    // arrive now. (Proactive crash detection beyond this reactive
    // backstop is Part 2b's scope.)
    let pending = std::mem::take(&mut *reader_link.pending.lock().unwrap());
    for (_, callback) in pending {
      callback(Response::OpError {
        message: "peer link disconnected".to_string(),
      });
    }
  });

  link
}

use std::collections::HashMap as StdHashMap;
use std::io::{Read, Write};
use std::net::TcpListener;

use seisin_core::authority::NodeId;

/// One `PeerLink` per remote node, dialed eagerly at startup rather
/// than lazily on first need — a worker thread calling `get` must
/// never block on a fresh `TcpStream::connect`, so every connection
/// this node will ever originate is already established before any
/// traffic flows. The lower `NodeId` in a pair always dials the higher
/// one; the higher one only ever accepts, so exactly one connection
/// exists per pair regardless of which side needs it first.
pub struct PeerLinkRegistry {
  links: StdHashMap<NodeId, Arc<PeerLink>>,
}

impl PeerLinkRegistry {
  /// Dials every peer in `address_book` with a larger `NodeId` than
  /// `self_node_id`, then starts accepting inbound connections from
  /// smaller-`NodeId` peers on `listener` — the deterministic "lower
  /// dials higher" rule below is what guarantees exactly one
  /// connection per pair regardless of which side needs it first.
  /// Returns once every outbound dial has completed; the inbound
  /// accept loop keeps running in the background afterward.
  pub fn start(
    listener: TcpListener,
    self_node_id: NodeId,
    address_book: Arc<StdHashMap<NodeId, String>>,
    on_request: Arc<dyn Fn(ThreadId, Request, Arc<PeerLink>, u64) + Send + Sync>,
  ) -> Arc<Mutex<PeerLinkRegistry>> {
    let registry = Arc::new(Mutex::new(PeerLinkRegistry {
      links: StdHashMap::new(),
    }));

    // Dial every peer with a larger NodeId — the deterministic
    // "who dials whom" rule that avoids both sides racing to connect.
    // A peer that isn't reachable yet (not started, or started after
    // this node) is skipped rather than treated as fatal — this node
    // still needs to finish starting up. It simply won't have a link
    // to that peer until something re-establishes one; a later
    // Acquire/Recall targeting it panics at `get` (below), an accepted
    // v1 gap — reconnection/backoff is Part 2b's crash-handling scope,
    // not this plan's.
    for (&peer_id, address) in address_book.iter() {
      if peer_id <= self_node_id {
        continue;
      }
      let Ok(mut stream) = TcpStream::connect(address) else {
        continue;
      };
      if stream.write_all(&self_node_id.0.to_le_bytes()).is_err() {
        continue;
      }
      let link = spawn(stream, Arc::clone(&on_request));
      registry.lock().unwrap().links.insert(peer_id, link);
    }

    // Accept inbound connections from every peer with a smaller
    // NodeId, reading each one's handshake preamble to learn who it is.
    {
      let registry = Arc::clone(&registry);
      thread::spawn(move || {
        for stream in listener.incoming() {
          let mut stream = match stream {
            Ok(s) => s,
            Err(_) => continue,
          };
          let mut preamble = [0u8; 8];
          if stream.read_exact(&mut preamble).is_err() {
            continue;
          }
          let peer_id = NodeId(u64::from_le_bytes(preamble));
          let link = spawn(stream, Arc::clone(&on_request));
          registry.lock().unwrap().links.insert(peer_id, link);
        }
      });
    }

    registry
  }
}

impl PeerLinkRegistry {
  /// Returns the link to `node_id`.
  ///
  /// # Panics
  /// Panics if no link to `node_id` exists — expected to normally be
  /// dialed or accepted at startup (see `start`), but a peer that was
  /// unreachable at dial time, or joined dynamically after this node
  /// started, has no entry either. Either way, an `Acquire`/`Recall`
  /// that hits this is a case this plan doesn't yet handle gracefully
  /// — reconnection/backoff is Part 2b's crash-handling scope.
  pub fn get(&self, node_id: NodeId) -> Arc<PeerLink> {
    Arc::clone(
      self
        .links
        .get(&node_id)
        .unwrap_or_else(|| panic!("no peer-link connection to node {node_id:?}")),
    )
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::net::TcpListener as StdTcpListener;
  use std::sync::mpsc as std_mpsc;
  use std::time::Duration;

  use seisin_core::datum::DatumId;

  /// Connects a real loopback TCP pair and spawns a `PeerLink` on each
  /// end, letting `on_request_b` decide how side B answers incoming
  /// requests.
  fn connected_pair(
    on_request_a: Arc<dyn Fn(ThreadId, Request, Arc<PeerLink>, u64) + Send + Sync>,
    on_request_b: Arc<dyn Fn(ThreadId, Request, Arc<PeerLink>, u64) + Send + Sync>,
  ) -> (Arc<PeerLink>, Arc<PeerLink>) {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let accept_thread = thread::spawn(move || {
      let (stream, _) = listener.accept().unwrap();
      spawn(stream, on_request_b)
    });
    let client_stream = TcpStream::connect(addr).unwrap();
    let link_a = spawn(client_stream, on_request_a);
    let link_b = accept_thread.join().unwrap();
    (link_a, link_b)
  }

  fn no_op_dispatch() -> Arc<dyn Fn(ThreadId, Request, Arc<PeerLink>, u64) + Send + Sync> {
    Arc::new(|_thread, _request, _link, _cid| {})
  }

  #[test]
  fn a_call_reaches_the_peers_on_request_callback() {
    let (tx, rx) = std_mpsc::channel();
    let on_request_b: Arc<dyn Fn(ThreadId, Request, Arc<PeerLink>, u64) + Send + Sync> =
      Arc::new(move |thread, request, link, cid| {
        let _ = tx.send((thread, request));
        link.respond(cid, Response::Granted);
      });
    let (link_a, _link_b) = connected_pair(no_op_dispatch(), on_request_b);

    let (reply_tx, reply_rx) = std_mpsc::channel();
    let datum_id = DatumId::new();
    let op_id = DatumId::new();
    link_a.call(
      ThreadId(3),
      Request::Acquire {
        op_id,
        datum_id,
        requester_node: seisin_core::authority::NodeId(1),
        requester_thread: ThreadId(0),
      },
      Box::new(move |response| {
        let _ = reply_tx.send(response);
      }),
    );

    let (received_thread, received_request) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(received_thread, ThreadId(3));
    assert_eq!(
      received_request,
      Request::Acquire {
        op_id,
        datum_id,
        requester_node: seisin_core::authority::NodeId(1),
        requester_thread: ThreadId(0),
      }
    );
    assert_eq!(
      reply_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
      Response::Granted
    );
  }

  #[test]
  fn many_concurrent_calls_each_get_routed_back_to_the_right_caller() {
    let on_request_b: Arc<dyn Fn(ThreadId, Request, Arc<PeerLink>, u64) + Send + Sync> =
      Arc::new(|_thread, _request, link, cid| {
        link.respond(cid, Response::Granted);
      });
    let (link_a, _link_b) = connected_pair(no_op_dispatch(), on_request_b);

    let mut receivers = Vec::new();
    for _ in 0..20 {
      let (tx, rx) = std_mpsc::channel();
      link_a.call(
        ThreadId(0),
        Request::Recall {
          datum_id: DatumId::new(),
        },
        Box::new(move |response| {
          let _ = tx.send(response);
        }),
      );
      receivers.push(rx);
    }
    for rx in receivers {
      assert_eq!(
        rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        Response::Granted
      );
    }
  }

  #[test]
  fn a_dropped_connection_fails_pending_calls_instead_of_hanging() {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let accept_thread = thread::spawn(move || {
      let (stream, _) = listener.accept().unwrap();
      stream
    });
    let client_stream = TcpStream::connect(addr).unwrap();
    let link = spawn(client_stream, no_op_dispatch());
    let server_stream = accept_thread.join().unwrap();

    // Make the call while the connection is still up, so it's
    // genuinely in flight (registered in the pending map) when the
    // peer vanishes — the ordering that actually matters here: a call
    // made *after* a disconnect is already detected is a different,
    // not-yet-handled case (reconnection is Part 2b's scope).
    let (tx, rx) = std_mpsc::channel();
    link.call(
      ThreadId(0),
      Request::Recall {
        datum_id: DatumId::new(),
      },
      Box::new(move |response| {
        let _ = tx.send(response);
      }),
    );
    drop(server_stream);

    match rx.recv_timeout(Duration::from_secs(5)).unwrap() {
      Response::OpError { .. } => {}
      other => panic!("expected OpError on disconnect, got {other:?}"),
    }
  }

  #[test]
  fn registry_connects_every_pair_regardless_of_dial_direction() {
    use seisin_core::authority::NodeId;

    let node_a = NodeId(1);
    let node_b = NodeId(2);
    let node_c = NodeId(3);

    let listener_a = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let listener_b = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let listener_c = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr_a = listener_a.local_addr().unwrap().to_string();
    let addr_b = listener_b.local_addr().unwrap().to_string();
    let addr_c = listener_c.local_addr().unwrap().to_string();

    let mut address_book = StdHashMap::new();
    address_book.insert(node_a, addr_a);
    address_book.insert(node_b, addr_b);
    address_book.insert(node_c, addr_c);
    let address_book = Arc::new(address_book);

    // Start c and b first (they only accept from a smaller NodeId, so
    // nothing needs to dial yet), then a last (which dials both).
    let registry_c = PeerLinkRegistry::start(
      listener_c,
      node_c,
      Arc::clone(&address_book),
      no_op_dispatch(),
    );
    let registry_b = PeerLinkRegistry::start(
      listener_b,
      node_b,
      Arc::clone(&address_book),
      no_op_dispatch(),
    );
    let registry_a = PeerLinkRegistry::start(listener_a, node_a, address_book, no_op_dispatch());

    // Give the accept threads a moment to register the inbound
    // handshakes that a's and b's dial-outs just triggered.
    thread::sleep(Duration::from_millis(200));

    // Every pair connects, regardless of which side happened to dial:
    // a dials both b and c (1 < 2, 1 < 3); b dials c (2 < 3); c dials
    // no one (nothing in the set is greater than 3).
    registry_a.lock().unwrap().get(node_b);
    registry_a.lock().unwrap().get(node_c);
    registry_b.lock().unwrap().get(node_a);
    registry_b.lock().unwrap().get(node_c);
    registry_c.lock().unwrap().get(node_a);
    registry_c.lock().unwrap().get(node_b);
  }
}
