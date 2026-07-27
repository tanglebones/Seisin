//! Accepts client TCP connections and routes each `Request::Op`: serve
//! directly if every one of its datum_ids natively belongs to this
//! node, redirect if they all belong to exactly one other node (the
//! same idea as before 3b, just generalized from a single datum_id to a
//! list) — and, once an op's datums are genuinely spread across more
//! than one node, dispatch locally anyway, relying on the destination
//! thread's own Acquire/Recall machinery (see `worker.rs`/`peer_link.rs`)
//! to pull the remote ones in over the wire.

use std::collections::{HashMap, HashSet};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread;

use seisin_core::authority::NodeId;
use seisin_core::datum::DatumId;
use seisin_protocol::{
  decode_request, encode_response, read_frame, write_frame, Request, Response, StorageMember,
};
use seisin_ring::ring::Ring;

use crate::gossip_state::ClusterState;
use crate::pool::WorkerPool;

/// Runs the accept loop on `listener`, spawning one handler thread per
/// connection, until the listener errors out (e.g. the socket is closed).
pub fn serve(
  listener: TcpListener,
  self_node_id: NodeId,
  cluster: Arc<ClusterState>,
  address_book: Arc<HashMap<NodeId, String>>,
  pool: Arc<WorkerPool>,
) {
  for stream in listener.incoming() {
    let stream = match stream {
      Ok(s) => s,
      Err(_) => continue,
    };
    let cluster = Arc::clone(&cluster);
    let address_book = Arc::clone(&address_book);
    let pool = Arc::clone(&pool);
    thread::spawn(move || handle_connection(stream, self_node_id, cluster, address_book, pool));
  }
}

/// Admin control-plane requests are served regardless of halt/pause —
/// `GetClusterConfig` is read-only control plane (readable while
/// halted, which the resume flow needs), and the pause/halt mutators
/// plus the ring flip must run precisely while the gate is engaged.
fn is_admin(request: &Request) -> bool {
  matches!(
    request,
    Request::GetClusterConfig
      | Request::Pause { .. }
      | Request::Resume
      | Request::ClearHalt
      | Request::InstallStorageRing { .. }
  )
}

fn handle_connection(
  mut stream: TcpStream,
  self_node_id: NodeId,
  cluster: Arc<ClusterState>,
  address_book: Arc<HashMap<NodeId, String>>,
  pool: Arc<WorkerPool>,
) {
  let ring = Arc::clone(&cluster.compute_ring);
  loop {
    let payload = match read_frame(&mut stream) {
      Ok(p) => p,
      Err(_) => return, // connection closed or errored
    };
    let request = match decode_request(&payload) {
      Ok(r) => r,
      Err(_) => return, // malformed request: drop the connection
    };
    // The serving gate: op traffic is rejected while halted (a storage
    // member confirmed dead — fail-stop) or paused (a live migration),
    // with a distinct message per flavor. Admin requests bypass it.
    if !is_admin(&request) {
      if let Some(message) = cluster.halt.gate() {
        if write_frame(
          &mut stream,
          &encode_response(&Response::OpError { message }),
        )
        .is_err()
        {
          return;
        }
        continue;
      }
    }
    let response = match request {
      Request::Op {
        op_id,
        op_name,
        datum_ids,
        payload,
      } => handle_op_request(
        self_node_id,
        &ring,
        &address_book,
        &pool,
        op_id,
        op_name,
        datum_ids,
        payload,
      ),
      Request::RkQuery {
        index_datum_id,
        query,
      } => handle_rk_query(
        self_node_id,
        &ring,
        &address_book,
        &pool,
        index_datum_id,
        query,
      ),
      Request::LbExecute {
        board_id,
        class,
        op,
      } => handle_lb_execute(
        self_node_id,
        &ring,
        &address_book,
        &pool,
        board_id,
        class,
        op,
      ),
      Request::LbQuery {
        board_id,
        class,
        query,
      } => handle_lb_query(
        self_node_id,
        &ring,
        &address_book,
        &pool,
        board_id,
        class,
        query,
      ),
      Request::TkExecute {
        entity_datum_id,
        class,
        op,
      } => handle_tk_execute(
        self_node_id,
        &ring,
        &address_book,
        &pool,
        entity_datum_id,
        class,
        op,
      ),
      Request::TkQuery {
        entity_datum_id,
        class,
        query,
      } => handle_tk_query(
        self_node_id,
        &ring,
        &address_book,
        &pool,
        entity_datum_id,
        class,
        query,
      ),
      Request::ExistsCheck { datum_id } => {
        handle_exists_check(self_node_id, &ring, &address_book, &pool, datum_id)
      }
      Request::FkPending {
        pending_datum_id,
        op,
      } => handle_fk_pending(
        self_node_id,
        &ring,
        &address_book,
        &pool,
        pending_datum_id,
        op,
      ),
      Request::ExtentQuery {
        extent_datum_id,
        offset,
        limit,
      } => handle_extent_query(
        self_node_id,
        &ring,
        &address_book,
        &pool,
        extent_datum_id,
        offset,
        limit,
      ),
      Request::PartitionUpdate {
        partition_datum_id,
        op,
      } => handle_partition_update(
        self_node_id,
        &ring,
        &address_book,
        &pool,
        partition_datum_id,
        op,
      ),
      // --- admin control plane (Storage Tier Part C-1) ---
      Request::GetClusterConfig => handle_get_cluster_config(&cluster),
      Request::Pause { reason } => {
        cluster.halt.pause(reason);
        Response::Ack
      }
      Request::Resume => {
        cluster.halt.resume();
        Response::Ack
      }
      Request::ClearHalt => {
        cluster.halt.clear_halt();
        Response::Ack
      }
      Request::InstallStorageRing { members } => handle_install_storage_ring(&cluster, members),
      // Acquire/Recall/Release/IndexUpdate are node-to-node only,
      // carried over a peer-link connection (see peer_link.rs) — a
      // client should never send one on this client-facing connection.
      _ => return,
    };
    if write_frame(&mut stream, &encode_response(&response)).is_err() {
      return;
    }
  }
}

/// Resolves every datum_id's native node. If they're all this node,
/// runs the op locally. If they're all exactly one *other* node,
/// redirects there (the client reconnects and retries — the same
/// mechanism a single-datum request used before 3b, just generalized).
/// Otherwise (spread across more than one node), dispatches locally —
/// the destination thread pulls in whatever it doesn't already have.
#[allow(clippy::too_many_arguments)]
fn handle_op_request(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  pool: &WorkerPool,
  op_id: DatumId,
  op_name: String,
  datum_ids: Vec<DatumId>,
  payload: Vec<u8>,
) -> Response {
  let native_nodes: HashSet<NodeId> = {
    let ring = ring.read().unwrap();
    datum_ids.iter().map(|id| ring.native(*id).0).collect()
  };

  // A single-node op whose one native node isn't this one still takes
  // the cheaper redirect path (the client reconnects directly to the
  // node that already has everything it needs) — this is unchanged
  // from Part 1. Only once an op's datums are genuinely spread across
  // more than one node does it now fall through to local dispatch,
  // relying on the destination thread's own Acquire/Recall machinery
  // to pull the remote ones in — no more outright rejection.
  if native_nodes.len() == 1 {
    let only_node = *native_nodes.iter().next().unwrap();
    if only_node != self_node_id {
      return match address_book.get(&only_node) {
        Some(address) => Response::Redirect {
          address: address.clone(),
        },
        None => Response::OpError {
          message: format!("no known address for node {only_node:?}"),
        },
      };
    }
  }

  match pool.run_op(op_id, op_name, datum_ids, payload) {
    Ok(payload) => Response::OpResult { payload },
    Err(message) => Response::OpError { message },
  }
}

/// Reports the storage ring's members with their store addresses and
/// log ids — the migration driver's planning input and the resume
/// flow's identity source. Served even while halted/paused (read-only).
fn handle_get_cluster_config(cluster: &ClusterState) -> Response {
  let weights = cluster.storage_ring.read().unwrap().weights();
  let addresses = cluster.store_addresses.read().unwrap();
  let identity = cluster.identity_book.read().unwrap();
  let members = weights
    .into_iter()
    .map(|(node_id, weight)| StorageMember {
      node_id,
      weight,
      store_address: addresses.get(&node_id).cloned().unwrap_or_default(),
      log_id: identity
        .get(&node_id)
        .copied()
        .unwrap_or_else(|| DatumId::from_bytes([0u8; 16])),
    })
    .collect();
  Response::ClusterConfig { members }
}

/// The migration flip: swap the shared storage ring (and its address /
/// identity books) to exactly `members`, in wire order — the same order
/// the driver used to compute the moved set, so placement agrees. The
/// shared `Arc<RwLock<Ring>>` is the one `RemoteStore` reads, so this is
/// a live swap with no restart.
fn handle_install_storage_ring(cluster: &ClusterState, members: Vec<StorageMember>) -> Response {
  let ring_members: Vec<(NodeId, u32)> = members.iter().map(|m| (m.node_id, m.weight)).collect();
  *cluster.storage_ring.write().unwrap() = Ring::from_members(&ring_members);
  {
    let mut addresses = cluster.store_addresses.write().unwrap();
    let mut identity = cluster.identity_book.write().unwrap();
    addresses.clear();
    identity.clear();
    for m in &members {
      if !m.store_address.is_empty() {
        addresses.insert(m.node_id, m.store_address.clone());
      }
      identity.insert(m.node_id, m.log_id);
    }
  }
  Response::Ack
}

/// `Some(response)` if `datum_id` isn't native here (a redirect, or an
/// error if the native node's address is unknown); `None` when this
/// node should serve the request itself.
fn redirect_if_foreign(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  datum_id: DatumId,
) -> Option<Response> {
  let native_node = ring.read().unwrap().native(datum_id).0;
  if native_node == self_node_id {
    return None;
  }
  Some(match address_book.get(&native_node) {
    Some(address) => Response::Redirect {
      address: address.clone(),
    },
    None => Response::OpError {
      message: format!("no known address for node {native_node:?}"),
    },
  })
}

/// Routes a client rk query: redirect if `index_datum_id` isn't native
/// here, else answer synchronously from the owning thread's resident
/// tree. The query kind is re-encoded to the protocol's standalone
/// codec bytes because the worker treats query/result bytes as opaque
/// (`ResidentIndex::query`) — the byte layout is defined once, in
/// seisin-protocol, shared with the rk impl's decoder in seisin-types.
fn handle_rk_query(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  pool: &WorkerPool,
  index_datum_id: DatumId,
  query: seisin_protocol::RkQueryKind,
) -> Response {
  if let Some(response) = redirect_if_foreign(self_node_id, ring, address_book, index_datum_id) {
    return response;
  }
  let query_bytes = seisin_protocol::encode_rk_query_kind(&query);
  match pool.run_index_query(index_datum_id, "rk".to_string(), query_bytes) {
    Ok(result_bytes) => match seisin_protocol::decode_rk_entries(&result_bytes) {
      Ok(entries) => Response::RkQueryResult { entries },
      Err(e) => Response::OpError {
        message: format!("malformed rk query result: {e}"),
      },
    },
    Err(message) => Response::OpError { message },
  }
}

/// Routes a client lb execute op: redirect if `board_id` isn't native
/// here, else run it on the owning thread. The `class` field exists
/// only to form the registry kind string `lb:{class}` — this file
/// stays semantics-agnostic about what lb ops mean.
#[allow(clippy::too_many_arguments)]
fn handle_lb_execute(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  pool: &WorkerPool,
  board_id: DatumId,
  class: String,
  op: seisin_protocol::LbExecuteOp,
) -> Response {
  if let Some(response) = redirect_if_foreign(self_node_id, ring, address_book, board_id) {
    return response;
  }
  let payload = seisin_protocol::encode_lb_execute_op(&op);
  lb_result_response(pool.run_index_execute(board_id, format!("lb:{class}"), payload))
}

/// Routes a client lb query — same shape as `handle_lb_execute`, on
/// the read-only index-query path.
#[allow(clippy::too_many_arguments)]
fn handle_lb_query(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  pool: &WorkerPool,
  board_id: DatumId,
  class: String,
  query: seisin_protocol::LbQueryReq,
) -> Response {
  if let Some(response) = redirect_if_foreign(self_node_id, ring, address_book, board_id) {
    return response;
  }
  let payload = seisin_protocol::encode_lb_query_req(&query);
  lb_result_response(pool.run_index_query(board_id, format!("lb:{class}"), payload))
}

fn lb_result_response(result: Result<Vec<u8>, String>) -> Response {
  match result {
    Ok(bytes) => match seisin_protocol::decode_lb_result(&bytes) {
      Ok(result) => Response::LbResult(result),
      Err(e) => Response::OpError {
        message: format!("malformed lb result: {e}"),
      },
    },
    Err(message) => Response::OpError { message },
  }
}

/// Routes a client tk execute op — same shape as `handle_lb_execute`;
/// `class` only forms the registry kind string `tk:{class}`.
#[allow(clippy::too_many_arguments)]
fn handle_tk_execute(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  pool: &WorkerPool,
  entity_datum_id: DatumId,
  class: String,
  op: seisin_protocol::TkOp,
) -> Response {
  if let Some(response) = redirect_if_foreign(self_node_id, ring, address_book, entity_datum_id) {
    return response;
  }
  let payload = seisin_protocol::encode_tk_op(&op);
  tk_result_response(pool.run_index_execute(entity_datum_id, format!("tk:{class}"), payload))
}

/// Routes a client tk query — read-only sibling of `handle_tk_execute`.
#[allow(clippy::too_many_arguments)]
fn handle_tk_query(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  pool: &WorkerPool,
  entity_datum_id: DatumId,
  class: String,
  query: seisin_protocol::TkQueryReq,
) -> Response {
  if let Some(response) = redirect_if_foreign(self_node_id, ring, address_book, entity_datum_id) {
    return response;
  }
  let payload = seisin_protocol::encode_tk_query_req(&query);
  tk_result_response(pool.run_index_query(entity_datum_id, format!("tk:{class}"), payload))
}

fn tk_result_response(result: Result<Vec<u8>, String>) -> Response {
  match result {
    Ok(bytes) => match seisin_protocol::decode_tk_result(&bytes) {
      Ok(result) => Response::TkResult(result),
      Err(e) => Response::OpError {
        message: format!("malformed tk result: {e}"),
      },
    },
    Err(message) => Response::OpError { message },
  }
}

/// Client-facing existence probe (the FK scan driver's re-check) —
/// same redirect routing as everything else.
fn handle_exists_check(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  pool: &WorkerPool,
  datum_id: DatumId,
) -> Response {
  if let Some(response) = redirect_if_foreign(self_node_id, ring, address_book, datum_id) {
    return response;
  }
  Response::Exists {
    exists: pool.run_exists_check(datum_id),
  }
}

/// Client-facing fk_pending ops (the scan driver's surface): List via
/// the read-only query path, Remove via execute. Insert never arrives
/// here — the write path delivers it as an ordinary IndexUpdate.
fn handle_fk_pending(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  pool: &WorkerPool,
  pending_datum_id: DatumId,
  op: seisin_protocol::FkPendingOp,
) -> Response {
  if let Some(response) = redirect_if_foreign(self_node_id, ring, address_book, pending_datum_id) {
    return response;
  }
  let payload = seisin_protocol::encode_fk_pending_op(&op);
  let result = match op {
    seisin_protocol::FkPendingOp::List => {
      pool.run_index_query(pending_datum_id, "fk_pending".to_string(), payload)
    }
    seisin_protocol::FkPendingOp::Remove { .. } => {
      pool.run_index_execute(pending_datum_id, "fk_pending".to_string(), payload)
    }
    seisin_protocol::FkPendingOp::Insert { .. } => {
      return Response::OpError {
        message: "fk pending inserts arrive via the write path, not this request".to_string(),
      }
    }
  };
  match result {
    Ok(bytes) => match seisin_protocol::decode_fk_entries(&bytes) {
      Ok(entries) => Response::FkPendingResult { entries },
      Err(e) => Response::OpError {
        message: format!("malformed fk pending result: {e}"),
      },
    },
    Err(message) => Response::OpError { message },
  }
}

/// Client-facing extent page (the rescan driver's enumeration).
#[allow(clippy::too_many_arguments)]
fn handle_extent_query(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  pool: &WorkerPool,
  extent_datum_id: DatumId,
  offset: u64,
  limit: u32,
) -> Response {
  if let Some(response) = redirect_if_foreign(self_node_id, ring, address_book, extent_datum_id) {
    return response;
  }
  let payload = seisin_protocol::encode_extent_page(offset, limit);
  match pool.run_index_query(extent_datum_id, "partition".to_string(), payload) {
    Ok(bytes) => match seisin_protocol::decode_extent_result(&bytes) {
      Ok((total, pks)) => Response::ExtentResult { total, pks },
      Err(e) => Response::OpError {
        message: format!("malformed extent result: {e}"),
      },
    },
    Err(message) => Response::OpError { message },
  }
}

/// Client-facing partition membership mutation (the driver's
/// invalid-set maintenance).
fn handle_partition_update(
  self_node_id: NodeId,
  ring: &Arc<RwLock<Ring>>,
  address_book: &HashMap<NodeId, String>,
  pool: &WorkerPool,
  partition_datum_id: DatumId,
  op: seisin_protocol::ExtentOp,
) -> Response {
  if let Some(response) = redirect_if_foreign(self_node_id, ring, address_book, partition_datum_id)
  {
    return response;
  }
  let payload = seisin_protocol::encode_extent_op(&op);
  match pool.run_index_execute(partition_datum_id, "partition".to_string(), payload) {
    Ok(bytes) => match seisin_protocol::decode_extent_result(&bytes) {
      Ok((total, pks)) => Response::ExtentResult { total, pks },
      Err(e) => Response::OpError {
        message: format!("malformed partition result: {e}"),
      },
    },
    Err(message) => Response::OpError { message },
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::halt::HaltState;

  fn cluster_with_storage(members: &[(NodeId, u32)]) -> ClusterState {
    ClusterState {
      compute_ring: Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)]))),
      storage_ring: Arc::new(RwLock::new(Ring::from_members(members))),
      store_addresses: Arc::new(RwLock::new(HashMap::new())),
      identity_book: Arc::new(RwLock::new(HashMap::new())),
      halt: Arc::new(HaltState::new()),
    }
  }

  #[test]
  fn is_admin_flags_only_admin_requests() {
    assert!(is_admin(&Request::GetClusterConfig));
    assert!(is_admin(&Request::Resume));
    assert!(is_admin(&Request::InstallStorageRing { members: vec![] }));
    assert!(!is_admin(&Request::Op {
      op_id: DatumId::new(),
      op_name: "x".to_string(),
      datum_ids: vec![],
      payload: vec![],
    }));
  }

  #[test]
  fn install_storage_ring_swaps_the_ring_and_books() {
    let cluster = cluster_with_storage(&[(NodeId(7), 1)]);
    let member = StorageMember {
      node_id: NodeId(9),
      weight: 1,
      store_address: "127.0.0.1:6900".to_string(),
      log_id: DatumId::from_bytes([4u8; 16]),
    };
    assert_eq!(
      handle_install_storage_ring(&cluster, vec![member]),
      Response::Ack
    );
    // The old member is gone, the new one is in.
    assert!(cluster.storage_ring.read().unwrap().contains(NodeId(9)));
    assert!(!cluster.storage_ring.read().unwrap().contains(NodeId(7)));
    assert_eq!(
      cluster.store_addresses.read().unwrap().get(&NodeId(9)),
      Some(&"127.0.0.1:6900".to_string())
    );
    assert_eq!(
      cluster.identity_book.read().unwrap().get(&NodeId(9)),
      Some(&DatumId::from_bytes([4u8; 16]))
    );
  }

  #[test]
  fn get_cluster_config_reports_members_with_addresses_and_log_ids() {
    let cluster = cluster_with_storage(&[]);
    let member = StorageMember {
      node_id: NodeId(9),
      weight: 3,
      store_address: "127.0.0.1:6900".to_string(),
      log_id: DatumId::from_bytes([4u8; 16]),
    };
    handle_install_storage_ring(&cluster, vec![member.clone()]);
    match handle_get_cluster_config(&cluster) {
      Response::ClusterConfig { members } => assert_eq!(members, vec![member]),
      other => panic!("expected ClusterConfig, got {other:?}"),
    }
  }
}
