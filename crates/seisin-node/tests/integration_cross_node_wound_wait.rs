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

  let pool_a = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(build_registry()),
    Arc::clone(&ring),
    node_a,
    peer_link_listener_a,
    Arc::clone(&peer_link_address_book),
    Arc::new(seisin_node::index_handler::IndexKindRegistry::new()),
  ));
  let pool_b = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(build_registry()),
    Arc::clone(&ring),
    node_b,
    peer_link_listener_b,
    peer_link_address_book,
    Arc::new(seisin_node::index_handler::IndexKindRegistry::new()),
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
fn two_ops_needing_cross_node_datums_in_opposite_order_both_complete() {
  let (addr_a, addr_b, ring) = start_two_node_cluster();

  let (a, b) = loop {
    let x = DatumId::new();
    let y = DatumId::new();
    if ring.read().unwrap().native(x).0 != ring.read().unwrap().native(y).0 {
      break (x, y);
    }
  };

  let op1 = DatumId::new(); // older
  let op2 = DatumId::new(); // younger

  let thread1 = thread::spawn(move || {
    seisin_client::call(
      &addr_a,
      Request::Op {
        op_id: op1,
        op_name: "touch_both".to_string(),
        datum_ids: vec![a, b],
        payload: vec![],
      },
    )
  });
  let thread2 = thread::spawn(move || {
    seisin_client::call(
      &addr_b,
      Request::Op {
        op_id: op2,
        op_name: "touch_both".to_string(),
        datum_ids: vec![b, a],
        payload: vec![],
      },
    )
  });

  let result1 = thread1.join().unwrap().unwrap();
  let result2 = thread2.join().unwrap().unwrap();
  assert_eq!(result1, Response::OpResult { payload: vec![] });
  assert_eq!(result2, Response::OpResult { payload: vec![] });
}
