use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::store::InMemoryStore;
use seisin_node::index_handler::IndexKindRegistry;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{Request, Response, TkOp, TkQueryReq, TkSpan};
use seisin_ring::ring::Ring;
use seisin_types::encoding::encode_field_value;
use seisin_types::field::{FieldType, FieldValue};
use seisin_types::tk::{tk_entity_key, SystemWallClock, TkClassDef};
use seisin_types::tk_kind::register_tk_class;

fn holdings_class() -> TkClassDef {
  TkClassDef {
    name: "holdings".to_string(),
    value_type: FieldType::F64,
    value_width: 16,
    sub_key_width: 16,
  }
}

fn start_node(data_dir: std::path::PathBuf) -> String {
  let mut index_kinds = IndexKindRegistry::new();
  register_tk_class(
    &mut index_kinds,
    holdings_class(),
    data_dir,
    Arc::new(SystemWallClock),
  );

  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  let node_id = NodeId(1);
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_id, 2)])));
  let peer_link_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(OpRegistry::new()),
    Arc::clone(&ring),
    node_id,
    peer_link_listener,
    Arc::new(std::collections::HashMap::new()),
    Arc::new(index_kinds),
  ));
  let address_book = Arc::new(std::collections::HashMap::new());
  thread::spawn(move || {
    serve(
      listener,
      node_id,
      ring,
      address_book,
      pool,
      Arc::new(seisin_node::halt::HaltState::new()),
    )
  });
  thread::sleep(std::time::Duration::from_millis(100));
  addr
}

fn val(x: f64) -> Vec<u8> {
  let mut buf = Vec::new();
  encode_field_value(&FieldValue::F64(x), &mut buf);
  buf
}

fn execute(addr: &str, entity: DatumId, op: TkOp) -> Response {
  seisin_client::call(
    addr,
    Request::TkExecute {
      entity_datum_id: tk_entity_key("holdings", entity),
      class: "holdings".to_string(),
      op,
    },
  )
  .unwrap()
}

fn set(addr: &str, entity: DatumId, sub_key: &[u8], as_of: Option<i64>, x: f64) -> Vec<TkSpan> {
  match execute(
    addr,
    entity,
    TkOp::Set {
      sub_key: sub_key.to_vec(),
      as_of,
      value: val(x),
    },
  ) {
    Response::TkResult(result) => result.spans,
    other => panic!("expected TkResult, got {other:?}"),
  }
}

fn query(addr: &str, entity: DatumId, q: TkQueryReq) -> Vec<TkSpan> {
  let response = seisin_client::call(
    addr,
    Request::TkQuery {
      entity_datum_id: tk_entity_key("holdings", entity),
      class: "holdings".to_string(),
      query: q,
    },
  )
  .unwrap();
  match response {
    Response::TkResult(result) => result.spans,
    other => panic!("expected TkResult, got {other:?}"),
  }
}

#[test]
fn histories_correct_query_and_stay_independent_over_the_wire() {
  let data_dir = tempfile::tempdir().unwrap();
  let addr = start_node(data_dir.path().to_path_buf());

  let account_a = DatumId::new();
  let account_b = DatumId::new();
  let inv1 = [1u8; 16];
  let inv2 = [2u8; 16];

  // Account A, investment 1: buy at t=100, adjust at t=300.
  set(&addr, account_a, &inv1, Some(100), 10.0);
  set(&addr, account_a, &inv1, Some(300), 25.0);
  // Account A, investment 2: buy at t=200.
  set(&addr, account_a, &inv2, Some(200), 5.0);
  // Account B, investment 1: independent entity/file entirely.
  set(&addr, account_b, &inv1, Some(150), 99.0);

  // Backdated correction: A/inv1 actually held 12.0 from t=150.
  let spans = set(&addr, account_a, &inv1, Some(150), 12.0);
  assert_eq!((spans[0].lower, spans[0].upper), (150, Some(300)));

  // AsOf per sub-part.
  let at_120 = query(
    &addr,
    account_a,
    TkQueryReq::AsOf {
      sub_key: inv1.to_vec(),
      t: 120,
    },
  );
  assert_eq!(at_120[0].value, val(10.0)); // pre-correction value preserved
  let at_200 = query(
    &addr,
    account_a,
    TkQueryReq::AsOf {
      sub_key: inv1.to_vec(),
      t: 200,
    },
  );
  assert_eq!(at_200[0].value, val(12.0)); // corrected

  // Whole-account snapshot at t=250: inv1=12.0, inv2=5.0.
  let snapshot = query(&addr, account_a, TkQueryReq::SnapshotAt { t: 250 });
  assert_eq!(snapshot.len(), 2);
  assert_eq!(snapshot[0].sub_key, inv1.to_vec());
  assert_eq!(snapshot[0].value, val(12.0));
  assert_eq!(snapshot[1].sub_key, inv2.to_vec());
  assert_eq!(snapshot[1].value, val(5.0));

  // Account B is untouched by all of A's activity.
  let b_history = query(
    &addr,
    account_b,
    TkQueryReq::History {
      sub_key: inv1.to_vec(),
    },
  );
  assert_eq!(b_history.len(), 1);
  assert_eq!(b_history[0].value, val(99.0));

  // Clear inv2 (position closed) at t=400; snapshot at 500 shows inv1 only.
  match execute(
    &addr,
    account_a,
    TkOp::Clear {
      sub_key: inv2.to_vec(),
      as_of: Some(400),
    },
  ) {
    Response::TkResult(result) => {
      assert_eq!(
        (result.spans[0].lower, result.spans[0].upper),
        (200, Some(400))
      );
    }
    other => panic!("expected TkResult, got {other:?}"),
  }
  let snapshot = query(&addr, account_a, TkQueryReq::SnapshotAt { t: 500 });
  assert_eq!(snapshot.len(), 1);
  assert_eq!(snapshot[0].sub_key, inv1.to_vec());

  // Range over A/inv1 spans the whole history.
  let range = query(
    &addr,
    account_a,
    TkQueryReq::Range {
      sub_key: inv1.to_vec(),
      from: 120,
      to: 1_000,
    },
  );
  let lowers: Vec<i64> = range.iter().map(|s| s.lower).collect();
  assert_eq!(lowers, vec![100, 150, 300]);

  // Server-stamped write: as_of None lands at a plausible "now".
  let spans = set(&addr, account_a, &inv1, None, 30.0);
  assert!(spans[0].lower > 1_577_836_800_000); // after 2020-01-01
  let current = query(
    &addr,
    account_a,
    TkQueryReq::Current {
      sub_key: inv1.to_vec(),
    },
  );
  assert_eq!(current[0].value, val(30.0));

  // An oversized value is rejected loudly over the wire.
  let mut big = Vec::new();
  encode_field_value(&FieldValue::F64(1.0), &mut big);
  big.extend_from_slice(&[0u8; 32]); // trailing garbage also fails decode
  match execute(
    &addr,
    account_a,
    TkOp::Set {
      sub_key: inv1.to_vec(),
      as_of: Some(600),
      value: big,
    },
  ) {
    Response::OpError { message } => assert!(!message.is_empty()),
    other => panic!("expected OpError, got {other:?}"),
  }
}
