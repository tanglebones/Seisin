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

fn start_single_node_server(thread_count: u32) -> (String, Arc<RwLock<Ring>>, NodeId) {
  let node_id = NodeId(1);
  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_id, thread_count)])));
  let address_book = Arc::new(HashMap::new());

  let mut ops = OpRegistry::new();
  ops.register(
    "touch_both",
    Box::new(|ctx, ids, _payload| {
      ctx.put(ids[0], b"touched".to_vec());
      ctx.put(ids[1], b"touched".to_vec());
      vec![]
    }),
  );

  let peer_link_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    thread_count,
    Arc::new(ops),
    Arc::clone(&ring),
    node_id,
    peer_link_listener,
    Arc::new(HashMap::new()),
    Arc::new(seisin_node::index_handler::IndexKindRegistry::new()),
  ));
  let serve_ring = Arc::clone(&ring);
  thread::spawn(move || serve(listener, node_id, serve_ring, address_book, pool));
  (addr, ring, node_id)
}

#[test]
fn two_ops_needing_the_same_two_datums_in_opposite_order_both_complete() {
  let (addr, ring, _node_id) = start_single_node_server(4);

  // Find two ids whose native homes differ, so the two ops below
  // actually contend across threads rather than trivially both running
  // on the same one.
  let (a, b) = loop {
    let x = DatumId::new();
    let y = DatumId::new();
    if ring.read().unwrap().native(x) != ring.read().unwrap().native(y) {
      break (x, y);
    }
  };

  let addr1 = addr.clone();
  let addr2 = addr;
  let op1 = DatumId::new(); // older
  let op2 = DatumId::new(); // younger

  let thread1 = thread::spawn(move || {
    seisin_client::call(
      &addr1,
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
      &addr2,
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
