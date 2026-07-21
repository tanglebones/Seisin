use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::InMemoryStore;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{Request, Response};
use seisin_ring::ring::Ring;

fn start_two_node_cluster() -> (String, String, Arc<RwLock<Ring>>) {
  let listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
  let listener_b = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr_a = listener_a.local_addr().unwrap().to_string();
  let addr_b = listener_b.local_addr().unwrap().to_string();

  let node_a = NodeId(1);
  let node_b = NodeId(2);
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_a, 2), (node_b, 2)])));

  let mut address_book = HashMap::new();
  address_book.insert(node_a, addr_a.clone());
  address_book.insert(node_b, addr_b.clone());
  let address_book = Arc::new(address_book);

  let peer_link_listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
  let peer_link_listener_b = TcpListener::bind("127.0.0.1:0").unwrap();
  let peer_link_addr_a = peer_link_listener_a.local_addr().unwrap().to_string();
  let peer_link_addr_b = peer_link_listener_b.local_addr().unwrap().to_string();
  let mut peer_link_address_book = HashMap::new();
  peer_link_address_book.insert(node_a, peer_link_addr_a);
  peer_link_address_book.insert(node_b, peer_link_addr_b);
  let peer_link_address_book = Arc::new(peer_link_address_book);

  let mut ops = OpRegistry::new();
  ops.register(
    "touch_both",
    Box::new(|ctx, ids, _payload| {
      ctx.put(ids[0], b"touched".to_vec());
      ctx.put(ids[1], b"touched".to_vec());
      vec![]
    }),
  );
  let mut ops_b = OpRegistry::new();
  ops_b.register(
    "touch_both",
    Box::new(|ctx, ids, _payload| {
      ctx.put(ids[0], b"touched".to_vec());
      ctx.put(ids[1], b"touched".to_vec());
      vec![]
    }),
  );

  let pool_a = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(ops),
    Arc::clone(&ring),
    node_a,
    peer_link_listener_a,
    Arc::clone(&peer_link_address_book),
    Arc::new(seisin_node::index_handler::IndexHandlerRegistry::new()),
  ));
  let pool_b = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(ops_b),
    Arc::clone(&ring),
    node_b,
    peer_link_listener_b,
    peer_link_address_book,
    Arc::new(seisin_node::index_handler::IndexHandlerRegistry::new()),
  ));

  {
    let ring = Arc::clone(&ring);
    let address_book = Arc::clone(&address_book);
    thread::spawn(move || serve(listener_a, node_a, ring, address_book, pool_a));
  }
  {
    let ring = Arc::clone(&ring);
    let address_book = Arc::clone(&address_book);
    thread::spawn(move || serve(listener_b, node_b, ring, address_book, pool_b));
  }

  (addr_a, addr_b, ring)
}

#[test]
fn an_op_collates_datums_natively_owned_by_different_nodes() {
  let (addr_a, _addr_b, ring) = start_two_node_cluster();

  // Find two ids whose native homes are on different *nodes* (not just
  // different threads), so this test actually exercises a real
  // peer-link Acquire round trip.
  let (a, b) = loop {
    let x = DatumId::new();
    let y = DatumId::new();
    if ring.read().unwrap().native(x).0 != ring.read().unwrap().native(y).0 {
      break (x, y);
    }
  };

  // Contact node A regardless of where `a`/`b` actually live — proving
  // this doesn't matter now: whichever node ends up running the op
  // pulls in whatever it doesn't already have.
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
