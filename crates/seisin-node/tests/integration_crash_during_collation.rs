use std::collections::HashMap;
use std::io::Read;
use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::{NodeId, ThreadId};
use seisin_core::datum::DatumId;
use seisin_core::store::InMemoryStore;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{
  decode_envelope, decode_request, decode_response, encode_envelope, encode_request, read_frame,
  write_frame, Envelope, EnvelopeKind, Request, Response,
};
use seisin_ring::ring::Ring;

#[test]
fn a_recall_against_a_peer_that_dies_mid_flight_still_releases_via_the_reactive_backstop() {
  // Single-member ring: node A is native home for everything, so there's
  // no ambiguity about which thread a datum belongs to (always thread 0).
  // "node B" only ever plays a *requester* role here, never a native
  // home, so it doesn't need to be a ring member at all.
  let node_a = NodeId(1);
  let node_b = NodeId(2);
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_a, 1)])));

  let listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr_a = listener_a.local_addr().unwrap().to_string();
  let peer_link_listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
  let fake_node_b_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let fake_node_b_addr = fake_node_b_listener.local_addr().unwrap().to_string();

  let mut peer_link_address_book = HashMap::new();
  peer_link_address_book.insert(node_b, fake_node_b_addr);
  let peer_link_address_book = Arc::new(peer_link_address_book);

  let datum_id = DatumId::new();
  let op1 = DatumId::new(); // older — created first, so its UUIDv7 sorts lower
  let op2 = DatumId::new(); // younger — the fake holder's op

  // Fake "node B": accepts node A's eager dial-out (node A dials since
  // node_b's id is larger), completes the handshake, then plays the
  // role of a remote requester who already holds `datum_id` — it
  // sends its own Acquire (impersonating op2) and gets granted
  // immediately since node A is idle. When node A later needs to
  // recall it (because op1, older, wants the same datum), this thread
  // reads that incoming Recall request and then just drops the
  // connection instead of ever acking it — simulating node B crashing
  // at the exact moment it was asked to give the datum back.
  let fake_node_b = thread::spawn(move || {
    let (mut stream, _) = fake_node_b_listener.accept().unwrap();
    let mut preamble = [0u8; 8];
    stream.read_exact(&mut preamble).unwrap();

    write_frame(
      &mut stream,
      &encode_envelope(&Envelope {
        correlation_id: 1,
        kind: EnvelopeKind::Request,
        target_thread: ThreadId(0),
        body: encode_request(&Request::Acquire {
          op_id: op2,
          datum_id,
          requester_node: node_b,
          requester_thread: ThreadId(0),
        }),
      }),
    )
    .unwrap();

    let response_frame = read_frame(&mut stream).unwrap();
    let response_envelope = decode_envelope(&response_frame).unwrap();
    assert_eq!(response_envelope.correlation_id, 1);
    assert_eq!(
      decode_response(&response_envelope.body).unwrap(),
      Response::Granted
    );

    // Node A's Recall arrives once op1 shows up wanting the same
    // datum — read it, then vanish without acking.
    let recall_frame = read_frame(&mut stream).unwrap();
    let recall_envelope = decode_envelope(&recall_frame).unwrap();
    assert!(matches!(
      decode_request(&recall_envelope.body).unwrap(),
      Request::Recall { datum_id: recalled } if recalled == datum_id
    ));
    drop(stream);
  });

  let mut ops = OpRegistry::new();
  ops.register(
    "touch",
    Box::new(|ctx, ids, _payload| {
      ctx.put(ids[0], b"touched".to_vec());
      vec![]
    }),
  );
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    1,
    Arc::new(ops),
    Arc::clone(&ring),
    node_a,
    peer_link_listener_a,
    peer_link_address_book,
    Arc::new(seisin_node::index_handler::IndexKindRegistry::new()),
  ));
  let address_book = Arc::new(HashMap::new());
  thread::spawn(move || serve(listener_a, node_a, ring, address_book, pool));

  // Give the eager dial-out, handshake, and fake op2 grant time to
  // land before op1 arrives.
  thread::sleep(std::time::Duration::from_millis(200));

  let response = seisin_client::call(
    &addr_a,
    Request::Op {
      op_id: op1,
      op_name: "touch".to_string(),
      datum_ids: vec![datum_id],
      payload: vec![],
    },
  )
  .unwrap();
  assert_eq!(response, Response::OpResult { payload: vec![] });

  fake_node_b.join().unwrap();
}

fn build_registry() -> OpRegistry {
  let mut ops = OpRegistry::new();
  ops.register(
    "touch_both",
    Box::new(|ctx, ids, _payload| {
      ctx.put(ids[0], b"touched".to_vec());
      ctx.put(ids[1], b"touched".to_vec());
      vec![]
    }),
  );
  ops
}

/// Starts node A only, wired with a peer-link address book entry for
/// node B that nothing is actually listening on — its dial-out at
/// startup simply fails and is skipped (Part 2a's dial-skip fix), so
/// node A ends up with no peer-link connection to B at all. This is
/// the "never connected" flavor of unreachability (distinct from the
/// test above's "connected, then went silent"), and it's what proves
/// the bounded-retry-then-fail path: every `Acquire` attempt targeting
/// B hits the same missing connection.
fn start_node_a_with_unreachable_peer(
  node_a: NodeId,
  node_b: NodeId,
) -> (String, Arc<RwLock<Ring>>) {
  let listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr_a = listener_a.local_addr().unwrap().to_string();
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_a, 2), (node_b, 2)])));

  let address_book = Arc::new(HashMap::new());

  let peer_link_listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
  // node_b's peer-link address is reserved then immediately dropped —
  // nothing is listening, so node A's eager dial-out to it at startup
  // fails and is silently skipped.
  let unreachable_peer_link_addr_b = TcpListener::bind("127.0.0.1:0")
    .unwrap()
    .local_addr()
    .unwrap()
    .to_string();
  let mut peer_link_address_book = HashMap::new();
  peer_link_address_book.insert(node_b, unreachable_peer_link_addr_b);
  let peer_link_address_book = Arc::new(peer_link_address_book);

  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(build_registry()),
    Arc::clone(&ring),
    node_a,
    peer_link_listener_a,
    peer_link_address_book,
    Arc::new(seisin_node::index_handler::IndexKindRegistry::new()),
  ));

  let serve_ring = Arc::clone(&ring);
  thread::spawn(move || serve(listener_a, node_a, serve_ring, address_book, pool));
  (addr_a, ring)
}

#[test]
fn an_op_needing_an_unreachable_cross_node_datum_fails_instead_of_hanging() {
  let node_a = NodeId(1);
  let node_b = NodeId(2);
  let (addr_a, ring) = start_node_a_with_unreachable_peer(node_a, node_b);

  // Find two ids, one native to each node — node A's op needs both,
  // but node B's peer-link was never reachable, so acquiring the
  // node-B-native one will retry `MAX_ACQUIRE_RETRIES` times and then
  // fail the whole op.
  let (local_id, remote_id) = loop {
    let x = DatumId::new();
    let y = DatumId::new();
    if ring.read().unwrap().native(x).0 == node_a && ring.read().unwrap().native(y).0 == node_b {
      break (x, y);
    }
  };

  let response = seisin_client::call(
    &addr_a,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "touch_both".to_string(),
      datum_ids: vec![local_id, remote_id],
      payload: vec![],
    },
  )
  .unwrap();
  match response {
    Response::OpError { message } => {
      assert!(
        message.contains("after 3 retries") || message.contains("retries"),
        "expected a retry-exhaustion message, got: {message}"
      );
    }
    other => panic!("expected OpError, got {other:?}"),
  }
}

#[test]
fn a_local_datum_stays_available_after_a_different_op_fails_on_the_remote_one() {
  // Proves fail_op's release path actually frees whatever the failed
  // op had already acquired — run the failing op once (as above),
  // then confirm a fresh op touching only the *local* datum still
  // works normally afterward (not stuck as an orphaned hold).
  let node_a = NodeId(1);
  let node_b = NodeId(2);
  let (addr_a, ring) = start_node_a_with_unreachable_peer(node_a, node_b);

  let (local_id, remote_id) = loop {
    let x = DatumId::new();
    let y = DatumId::new();
    if ring.read().unwrap().native(x).0 == node_a && ring.read().unwrap().native(y).0 == node_b {
      break (x, y);
    }
  };

  let _ = seisin_client::call(
    &addr_a,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "touch_both".to_string(),
      datum_ids: vec![local_id, remote_id],
      payload: vec![],
    },
  )
  .unwrap();

  // A second op touching only the local datum (paired with a fresh
  // local-only partner id) must still succeed — proving `local_id`
  // wasn't left stuck as an orphaned hold by the first op's failure.
  let another_local_id = loop {
    let candidate = DatumId::new();
    if ring.read().unwrap().native(candidate).0 == node_a {
      break candidate;
    }
  };
  let response = seisin_client::call(
    &addr_a,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "touch_both".to_string(),
      datum_ids: vec![local_id, another_local_id],
      payload: vec![],
    },
  )
  .unwrap();
  assert_eq!(response, Response::OpResult { payload: vec![] });
}
