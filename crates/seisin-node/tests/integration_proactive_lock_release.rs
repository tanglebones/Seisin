use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::InMemoryStore;
use seisin_gossip::membership::{Incarnation, MemberStatus, MemberUpdate};
use seisin_node::gossip_client::run_gossip_loop;
use seisin_node::gossip_server::serve_gossip;
use seisin_node::gossip_state::GossipState;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{Request, Response};
use seisin_ring::ring::Ring;

const PROBE_INTERVAL_MILLIS: u64 = 20;
const PROBE_TIMEOUT_MILLIS: u64 = 20;
const SUSPICION_TIMEOUT_MILLIS: u64 = 40;

/// Reserves a real, currently-unused address by binding then dropping
/// the listener — nothing will be listening there afterward.
fn reserve_silent_address() -> String {
  TcpListener::bind("127.0.0.1:0")
    .unwrap()
    .local_addr()
    .unwrap()
    .to_string()
}

/// Starts a single real node (client server, gossip server, probing
/// loop) against the given member list — `members` entries are
/// `(node_id, thread_count, client_address, gossip_address,
/// peer_link_address)`.
fn start_node(
  node_id: NodeId,
  members: &[(NodeId, u32, String, String, String)],
) -> Arc<RwLock<Ring>> {
  let this = members.iter().find(|m| m.0 == node_id).unwrap();
  let client_listener = TcpListener::bind(&this.2).unwrap();
  let gossip_listener = TcpListener::bind(&this.3).unwrap();
  let peer_link_listener = TcpListener::bind(&this.4).unwrap();

  let ring_members: Vec<(NodeId, u32)> = members.iter().map(|m| (m.0, m.1)).collect();
  let ring = Arc::new(RwLock::new(Ring::from_members(&ring_members)));

  let address_book: HashMap<NodeId, String> =
    members.iter().map(|m| (m.0, m.2.clone())).collect();
  let address_book = Arc::new(address_book);

  let peer_link_address_book: HashMap<NodeId, String> =
    members.iter().map(|m| (m.0, m.4.clone())).collect();
  let peer_link_address_book = Arc::new(peer_link_address_book);

  let gossip = Arc::new(GossipState::new());
  {
    let mut table = gossip.member_table.lock().unwrap();
    for member in members {
      table.merge_update(MemberUpdate {
        node_id: member.0,
        incarnation: Incarnation(0),
        status: MemberStatus::Alive,
        client_address: member.2.clone(),
        gossip_address: member.3.clone(),
        thread_count: member.1,
      });
    }
  }

  let mut ops = OpRegistry::new();
  ops.register(
    "touch_both",
    Box::new(|ctx, ids, _payload| {
      ctx.put(ids[0], b"touched".to_vec());
      ctx.put(ids[1], b"touched".to_vec());
      vec![]
    }),
  );

  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    this.1,
    Arc::new(ops),
    Arc::clone(&ring),
    node_id,
    peer_link_listener,
    peer_link_address_book,
  ));

  {
    let ring = Arc::clone(&ring);
    let address_book = Arc::clone(&address_book);
    let pool = Arc::clone(&pool);
    thread::spawn(move || serve(client_listener, node_id, ring, address_book, pool));
  }
  {
    let gossip = Arc::clone(&gossip);
    let ring = Arc::clone(&ring);
    let pool = Arc::clone(&pool);
    thread::spawn(move || serve_gossip(gossip_listener, node_id, gossip, ring, pool));
  }
  {
    let ring_for_loop = Arc::clone(&ring);
    thread::spawn(move || {
      run_gossip_loop(
        node_id,
        gossip,
        ring_for_loop,
        pool,
        PROBE_INTERVAL_MILLIS,
        PROBE_TIMEOUT_MILLIS,
        SUSPICION_TIMEOUT_MILLIS,
      )
    });
  }

  ring
}

#[test]
fn a_lock_held_by_a_node_that_goes_silent_is_released_once_gossip_confirms_it_dead() {
  let node_a = NodeId(1);
  let node_b = NodeId(2);

  let addr_a = reserve_silent_address();
  let gossip_addr_a = reserve_silent_address();
  let peer_link_addr_a = reserve_silent_address();
  let addr_b = reserve_silent_address();
  let silent_gossip_addr_b = reserve_silent_address();
  let silent_peer_link_addr_b = reserve_silent_address();

  let members = vec![
    (
      node_a,
      2u32,
      addr_a.clone(),
      gossip_addr_a,
      peer_link_addr_a,
    ),
    (
      node_b,
      2u32,
      addr_b,
      silent_gossip_addr_b,
      silent_peer_link_addr_b,
    ),
  ];

  let ring = start_node(node_a, &members);
  // Node B is deliberately never started — its gossip and peer-link
  // addresses are reserved-but-silent, simulating a node that crashed
  // before ever responding to anything, including any peer-link
  // traffic node A's dial-out at startup would have sent it (which,
  // per Part 2a's dial-skip-on-failure fix, just means node A has no
  // peer-link connection to node B at all — this test proves the
  // *proactive gossip path* still resolves things correctly even so,
  // since it only involves NativeLock bookkeeping local to node A,
  // not needing to reach node B over any connection to release it).

  // Find two ids that are BOTH currently native to node B (before it's
  // declared dead) — used to prove node A's own NativeLock state
  // (which only exists once something asks about a datum) reflects
  // the departure correctly. Since nothing has asked about them yet,
  // this loop just needs ids whose native node is node_b under the
  // *starting* 2-node ring.
  let (a, b) = loop {
    let x = DatumId::new();
    let y = DatumId::new();
    if ring.read().unwrap().native(x).0 == node_b && ring.read().unwrap().native(y).0 == node_b {
      break (x, y);
    }
  };

  // Give node A's gossip loop enough cycles to converge B to Dead and
  // shrink the ring: at least one probe timeout, one suspicion
  // timeout, plus generous slack for scheduling jitter under test load.
  thread::sleep(Duration::from_millis(
    PROBE_INTERVAL_MILLIS + PROBE_TIMEOUT_MILLIS * 2 + SUSPICION_TIMEOUT_MILLIS + 500,
  ));

  // Once the ring has shrunk to just node A, `a`/`b` now resolve
  // locally — proving the op completes without ever needing to reach
  // (the now-removed) node B, and without hanging on any stale lock
  // state left over from when they were native to B.
  assert_eq!(ring.read().unwrap().native(a).0, node_a);
  assert_eq!(ring.read().unwrap().native(b).0, node_a);

  let response = seisin_client::call(
    &addr_a,
    Request::Op {
      op_id: DatumId::new(),
      op_name: "touch_both".to_string(),
      datum_ids: vec![a, b],
      payload: vec![],
    },
  )
  .unwrap();
  assert_eq!(response, Response::OpResult { payload: vec![] });
}
