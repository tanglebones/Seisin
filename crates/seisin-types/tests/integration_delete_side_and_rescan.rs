use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_core::sk::decode_sk_entries;
use seisin_core::store::InMemoryStore;
use seisin_node::index_handler::IndexKindRegistry;
use seisin_node::pool::WorkerPool;
use seisin_node::server::serve;
use seisin_ops::context::OpContext;
use seisin_ops::registry::OpRegistry;
use seisin_protocol::{FkPendingOp, Request, Response};
use seisin_ring::ring::Ring;
use seisin_types::driver::{
  clear_invalid, mark_invalid, revalidate_invalid, scan_order, validate_type,
};
use seisin_types::field::{FieldType, FieldValue};
use seisin_types::fk::{fk_deleted_key, register_fk_pending_kind};
use seisin_types::partition::register_partition_kind;
use seisin_types::schema::{
  ConflictOp, DatumTypeDef, FieldCheck, FkTarget, GuardRef, IndexDef, OnDelete,
  RelationalConstraintDef,
};
use seisin_types::sk_index::{register_sk_index_kind, sk_key};
use seisin_types::typed_context::TypedOpContext;
use seisin_types::{decode_datum, encode_datum};

/// foo.user_id -> user (Track + resolution drop_foo);
/// foo.team_id -> team (hard constraint isn't the point here — the
/// guards on user/team are); foo declares sk indexes on both FK fields
/// (the guard requirement) and an amount > 0 check.
fn foo_type() -> DatumTypeDef {
  DatumTypeDef::new("foo")
    .field("user_id", FieldType::Bytes)
    .field("team_id", FieldType::Bytes)
    .field("amount", FieldType::I64)
    .index(IndexDef::Sk {
      field: "user_id".to_string(),
      unique: None,
    })
    .index(IndexDef::Sk {
      field: "team_id".to_string(),
      unique: None,
    })
    .constraint(RelationalConstraintDef {
      field: "user_id".to_string(),
      references: FkTarget::PkUuid {
        type_name: "user".to_string(),
      },
      resolution: Some(ConflictOp("drop_foo".to_string())),
    })
    .constraint(RelationalConstraintDef {
      field: "team_id".to_string(),
      references: FkTarget::PkUuid {
        type_name: "team".to_string(),
      },
      resolution: Some(ConflictOp("drop_foo".to_string())),
    })
    .check("amount", FieldCheck::Gt(FieldValue::I64(0)))
    .track_extent()
    .rescan_every_millis(60_000)
}

fn user_type() -> DatumTypeDef {
  DatumTypeDef::new("user")
    .field("name", FieldType::String)
    .guard(GuardRef {
      type_name: "foo".to_string(),
      field: "user_id".to_string(),
      on_delete: OnDelete::Track,
    })
}

fn team_type() -> DatumTypeDef {
  DatumTypeDef::new("team")
    .field("name", FieldType::String)
    .guard(GuardRef {
      type_name: "foo".to_string(),
      field: "team_id".to_string(),
      on_delete: OnDelete::Restrict,
    })
}

fn start_node(data_dir: std::path::PathBuf) -> String {
  let mut ops = OpRegistry::new();
  for (name, def) in [
    ("write_foo", foo_type()),
    ("write_user", user_type()),
    ("write_team", team_type()),
  ] {
    ops.register(
      name,
      Box::new(move |ctx: &mut OpContext, ids, payload| {
        let values = decode_datum(&def, payload).unwrap();
        let mut tctx = TypedOpContext::new(ctx);
        tctx.get(ids[0], &def).unwrap();
        tctx.set(ids[0], &def, values).unwrap();
        vec![]
      }),
    );
  }
  for (name, def) in [
    ("delete_foo", foo_type()),
    ("delete_user", user_type()),
    ("delete_team", team_type()),
  ] {
    ops.register(
      name,
      Box::new(move |ctx: &mut OpContext, ids, _payload| {
        let mut tctx = TypedOpContext::new(ctx);
        tctx.get(ids[0], &def).unwrap();
        tctx.delete(ids[0], &def).unwrap();
        vec![]
      }),
    );
  }
  // The declared resolution op for foo's constraints.
  let drop_def = foo_type();
  ops.register(
    "drop_foo",
    Box::new(move |ctx: &mut OpContext, ids, _payload| {
      let mut tctx = TypedOpContext::new(ctx);
      tctx.get(ids[0], &drop_def).unwrap();
      tctx.delete(ids[0], &drop_def).unwrap();
      vec![]
    }),
  );
  ops.register(
    "read",
    Box::new(|ctx: &mut OpContext, ids, _payload| ctx.get(ids[0]).unwrap_or_default()),
  );
  // A byte-level writer that bypasses every typed-layer check — the
  // rescan must catch what it writes.
  ops.register(
    "raw_put",
    Box::new(|ctx: &mut OpContext, ids, payload| {
      ctx.put(ids[0], payload.to_vec());
      vec![]
    }),
  );

  let mut index_kinds = IndexKindRegistry::new();
  register_sk_index_kind(&mut index_kinds);
  register_fk_pending_kind(&mut index_kinds);
  register_partition_kind(&mut index_kinds, data_dir);

  let listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  let node_id = NodeId(1);
  let ring = Arc::new(RwLock::new(Ring::from_members(&[(node_id, 2)])));
  let peer_link_listener = TcpListener::bind("127.0.0.1:0").unwrap();
  let pool = Arc::new(WorkerPool::spawn(
    Arc::new(InMemoryStore::new()),
    2,
    Arc::new(ops),
    Arc::clone(&ring),
    node_id,
    peer_link_listener,
    Arc::new(std::collections::HashMap::new()),
    Arc::new(index_kinds),
  ));
  let address_book = Arc::new(std::collections::HashMap::new());
  thread::spawn(move || serve(listener, node_id, ring, address_book, pool));
  thread::sleep(std::time::Duration::from_millis(100));
  addr
}

fn run_op(addr: &str, op_name: &str, ids: Vec<DatumId>, payload: Vec<u8>) -> Response {
  seisin_client::call(
    addr,
    Request::Op {
      op_id: DatumId::new(),
      op_name: op_name.to_string(),
      datum_ids: vec![ids[0]],
      payload,
    },
  )
  .unwrap()
}

fn name_payload(def: &DatumTypeDef, name: &str) -> Vec<u8> {
  encode_datum(def, &[FieldValue::String(name.to_string())]).unwrap()
}

fn foo_payload(user: DatumId, team: DatumId, amount: i64) -> Vec<u8> {
  encode_datum(
    &foo_type(),
    &[
      FieldValue::Bytes(user.as_bytes().to_vec()),
      FieldValue::Bytes(team.as_bytes().to_vec()),
      FieldValue::I64(amount),
    ],
  )
  .unwrap()
}

fn ok(response: Response) {
  assert!(
    matches!(response, Response::OpResult { .. }),
    "expected OpResult, got {response:?}"
  );
}

#[test]
fn delete_guards_and_rescan_work_over_the_wire() {
  let data_dir = tempfile::tempdir().unwrap();
  let addr = start_node(data_dir.path().to_path_buf());

  let (user, team, foo) = (DatumId::new(), DatumId::new(), DatumId::new());
  ok(run_op(
    &addr,
    "write_user",
    vec![user],
    name_payload(&user_type(), "U"),
  ));
  ok(run_op(
    &addr,
    "write_team",
    vec![team],
    name_payload(&team_type(), "T"),
  ));
  ok(run_op(
    &addr,
    "write_foo",
    vec![foo],
    foo_payload(user, team, 5),
  ));

  // RESTRICT: team is still referenced — delete rejected.
  match run_op(&addr, "delete_team", vec![team], vec![]) {
    Response::OpError { message } => {
      assert!(message.contains("delete restricted"), "{message}")
    }
    other => panic!("expected restriction, got {other:?}"),
  }

  // TRACK: user delete succeeds and leaves a marker.
  ok(run_op(&addr, "delete_user", vec![user], vec![]));
  let deleted_marker = fk_deleted_key("foo", "user_id");
  let entries = match seisin_client::call(
    &addr,
    Request::FkPending {
      pending_datum_id: deleted_marker,
      op: FkPendingOp::List,
    },
  )
  .unwrap()
  {
    Response::FkPendingResult { entries } => entries,
    other => panic!("expected FkPendingResult, got {other:?}"),
  };
  assert_eq!(entries.len(), 1);
  let (deleted_pk, probe) = entries[0];
  assert_eq!(deleted_pk, user);

  // DRIVER cascade: read the sk list at the probe key, drop each
  // referencing foo via the declared ConflictOp, then clear the marker.
  let sk_bytes = match run_op(&addr, "read", vec![probe], vec![]) {
    Response::OpResult { payload } => payload,
    other => panic!("expected OpResult, got {other:?}"),
  };
  let referencing = decode_sk_entries(&sk_bytes).unwrap();
  assert_eq!(referencing.len(), 1);
  assert_eq!(referencing[0].0, foo);
  ok(run_op(&addr, "drop_foo", vec![foo], vec![]));
  match run_op(&addr, "read", vec![foo], vec![]) {
    Response::OpResult { payload } => assert!(payload.is_empty()),
    other => panic!("expected OpResult, got {other:?}"),
  }
  seisin_client::call(
    &addr,
    Request::FkPending {
      pending_datum_id: deleted_marker,
      op: FkPendingOp::Remove {
        referencing_pk: deleted_pk,
        target: probe,
      },
    },
  )
  .unwrap();

  // The cascade emptied foo's sk lists (WriteThrough::Delete) — team's
  // restricted delete now succeeds.
  ok(run_op(&addr, "delete_team", vec![team], vec![]));

  // RESCAN: healthy state first (the cascaded foo is out of the extent).
  let findings = validate_type(&addr, &foo_type(), "read", 2).unwrap();
  assert!(findings.is_empty(), "{findings:?}");

  // Seed a bad foo via the byte level (bypasses set-time checks):
  // amount 0 violates Gt(0), and both refs dangle. Also insert it into
  // the extent the honest way — write a GOOD foo first (which tracks
  // it), then raw-overwrite its bytes with bad content.
  let (user2, team2, foo2) = (DatumId::new(), DatumId::new(), DatumId::new());
  ok(run_op(
    &addr,
    "write_user",
    vec![user2],
    name_payload(&user_type(), "U2"),
  ));
  ok(run_op(
    &addr,
    "write_team",
    vec![team2],
    name_payload(&team_type(), "T2"),
  ));
  ok(run_op(
    &addr,
    "write_foo",
    vec![foo2],
    foo_payload(user2, team2, 5),
  ));
  let ghost = DatumId::new();
  let bad_bytes = encode_datum(
    &foo_type(),
    &[
      FieldValue::Bytes(ghost.as_bytes().to_vec()),
      FieldValue::Bytes(ghost.as_bytes().to_vec()),
      FieldValue::I64(0),
    ],
  )
  .unwrap();
  ok(run_op(&addr, "raw_put", vec![foo2], bad_bytes));

  let findings = validate_type(&addr, &foo_type(), "read", 2).unwrap();
  // One check finding (amount) + two dangling refs (user_id, team_id).
  assert_eq!(findings.len(), 3, "{findings:?}");
  assert!(findings.iter().any(|f| f.problem.contains("check")));
  assert_eq!(
    findings
      .iter()
      .filter(|f| f.problem.contains("dangling"))
      .count(),
    2
  );
  assert!(findings.iter().all(|f| f.pk == foo2));

  // INVALID PARTITION: mark the bad datum from the findings; the fast
  // path re-checks only that partition, still fails, keeps membership.
  mark_invalid(&addr, &foo_type(), &[foo2]).unwrap();
  let still = revalidate_invalid(&addr, &foo_type(), "read", 2).unwrap();
  assert_eq!(still.len(), 3);
  assert!(still.iter().all(|f| f.pk == foo2));
  // Fix the datum (typed write restores validity), re-run: cleared.
  ok(run_op(
    &addr,
    "write_foo",
    vec![foo2],
    foo_payload(user2, team2, 7),
  ));
  let still = revalidate_invalid(&addr, &foo_type(), "read", 2).unwrap();
  assert!(still.is_empty(), "{still:?}");
  // Membership cleared: another pass sees an empty partition.
  let still = revalidate_invalid(&addr, &foo_type(), "read", 2).unwrap();
  assert!(still.is_empty());
  let _ = clear_invalid; // (exercised inside revalidate_invalid)

  // SCAN ORDER: user and team (referenced) come before foo (referencer).
  let defs = vec![foo_type(), user_type(), team_type()];
  let order = scan_order(&defs);
  let names: Vec<&str> = order.iter().map(|&i| defs[i].name.as_str()).collect();
  assert_eq!(names[2], "foo");

  // Guard-requirement sanity: the probe key derivation the guards use
  // matches sk reality (foo declared the sk index on user_id).
  let expected_probe = sk_key(
    "foo",
    "user_id",
    &FieldValue::Bytes(user.as_bytes().to_vec()),
  )
  .unwrap();
  assert_eq!(probe, expected_probe);
}
