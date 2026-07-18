use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::InMemoryStore;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_protocol::{Request, Response};
use seisin_ring::ring::Ring;

fn start_two_node_cluster() -> (String, String) {
  let listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
  let listener_b = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr_a = listener_a.local_addr().unwrap().to_string();
  let addr_b = listener_b.local_addr().unwrap().to_string();

  let node_a = NodeId(1);
  let node_b = NodeId(2);
  let ring = Arc::new(std::sync::RwLock::new(Ring::from_members(&[(node_a, 2), (node_b, 2)])));

  let mut address_book = HashMap::new();
  address_book.insert(node_a, addr_a.clone());
  address_book.insert(node_b, addr_b.clone());
  let address_book = Arc::new(address_book);

  let pool_a = Arc::new(WorkerPool::spawn(Arc::new(InMemoryStore::new()), 2));
  let pool_b = Arc::new(WorkerPool::spawn(Arc::new(InMemoryStore::new()), 2));

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

  (addr_a, addr_b)
}

#[test]
fn put_and_get_route_correctly_across_two_nodes_regardless_of_entry_point() {
  let (addr_a, addr_b) = start_two_node_cluster();

  for _ in 0..20 {
    let id = DatumId::new();
    let content = format!("value-for-{id:?}").into_bytes();

    // Always PUT via node A's address, regardless of who actually owns it.
    let put_resp = seisin_client::call(
      &addr_a,
      Request::Put {
        id,
        content: content.clone(),
      },
    )
    .unwrap();
    assert_eq!(put_resp, Response::Ok);

    // Always GET via node B's address.
    let get_resp = seisin_client::call(&addr_b, Request::Get { id }).unwrap();
    match get_resp {
      Response::Value { content: got, .. } => assert_eq!(got, content),
      other => panic!("expected Value, got {other:?}"),
    }
  }
}
