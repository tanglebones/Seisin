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
use seisin_protocol::{Request, Response};
use seisin_ring::ring::Ring;

const PROBE_INTERVAL_MILLIS: u64 = 20;
const PROBE_TIMEOUT_MILLIS: u64 = 20;
const SUSPICION_TIMEOUT_MILLIS: u64 = 40;

/// Starts node `node_id`'s client server, gossip server, and probing
/// loop against the given member list. Every member's `gossip_address`
/// must actually have something listening for this node to be reachable
/// via gossip — `reserve_silent_address` below is how the test creates a
/// member entry that looks valid in config but has nothing listening,
/// simulating a node that's stopped responding.
fn start_node(node_id: NodeId, members: &[(NodeId, u32, String, String)]) {
  let this = members.iter().find(|m| m.0 == node_id).unwrap();
  let client_listener = TcpListener::bind(&this.2).unwrap();
  let gossip_listener = TcpListener::bind(&this.3).unwrap();

  let ring_members: Vec<(NodeId, u32)> = members.iter().map(|m| (m.0, m.1)).collect();
  let ring = Arc::new(RwLock::new(Ring::from_members(&ring_members)));

  let address_book: HashMap<NodeId, String> = members.iter().map(|m| (m.0, m.2.clone())).collect();
  let address_book = Arc::new(address_book);

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

  let pool = Arc::new(WorkerPool::spawn(Arc::new(InMemoryStore::new()), this.1));

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
    thread::spawn(move || {
      run_gossip_loop(
        node_id,
        gossip,
        ring,
        pool,
        PROBE_INTERVAL_MILLIS,
        PROBE_TIMEOUT_MILLIS,
        SUSPICION_TIMEOUT_MILLIS,
      )
    });
  }
}

/// Reserves a real, currently-unused address by binding then immediately
/// dropping the listener — nothing will be listening there, so any
/// connection attempt to it fails, simulating a node that has gone
/// silent on gossip.
fn reserve_silent_address() -> String {
  TcpListener::bind("127.0.0.1:0")
    .unwrap()
    .local_addr()
    .unwrap()
    .to_string()
}

#[test]
fn a_node_that_stops_responding_on_gossip_is_eventually_removed_from_the_ring() {
  let node_a = NodeId(1);
  let node_b = NodeId(2);

  let addr_a = reserve_silent_address();
  let gossip_addr_a = reserve_silent_address();
  let addr_b = reserve_silent_address();
  // Node B's gossip address is reserved but nothing will ever listen on
  // it — simulating node B having stopped responding on gossip from the
  // very start, while its identity still exists in the initial config
  // both nodes are seeded with.
  let silent_gossip_addr_b = reserve_silent_address();

  let members = vec![
    (node_a, 2u32, addr_a.clone(), gossip_addr_a.clone()),
    (node_b, 2u32, addr_b.clone(), silent_gossip_addr_b),
  ];

  start_node(node_a, &members);
  // Node B's client/gossip servers and probing loop are deliberately
  // never started — only node A runs, discovering B purely through its
  // seeded initial config, then failing to ever reach it on gossip. Some
  // requests routed toward B before convergence would fail (its client
  // port is unreachable too, just like a genuinely dead node's would be)
  // — that's expected and not what this test checks; it only asserts the
  // steady state once the ring has had time to converge.

  // Give node A's gossip loop enough cycles to converge B to Dead and
  // shrink the ring: at least one probe timeout, one suspicion timeout,
  // plus generous slack for scheduling jitter under test load.
  thread::sleep(Duration::from_millis(
    PROBE_INTERVAL_MILLIS + PROBE_TIMEOUT_MILLIS * 2 + SUSPICION_TIMEOUT_MILLIS + 500,
  ));

  // Every request now, regardless of datum_id, must be served directly
  // by node A rather than redirected to node B — proving the ring only
  // has node A's slots left.
  for _ in 0..20 {
    let id = DatumId::new();
    let response = seisin_client::call(
      &addr_a,
      Request::Put {
        id,
        content: b"x".to_vec(),
      },
    )
    .unwrap();
    assert_eq!(
      response,
      Response::Ok,
      "expected a direct Ok, not a Redirect, once node B is removed from the ring"
    );
  }
}
