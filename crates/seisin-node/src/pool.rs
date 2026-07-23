//! A pool of owning threads (one `WorkerHandle` per `ThreadId`),
//! interconnected so any thread can reach any other by `ThreadId` for
//! acquire/recall/release traffic (see `worker.rs`), all sharing this
//! node's backing store and op registry.

use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::mpsc;
use std::sync::{Arc, RwLock};

use seisin_core::authority::{NodeId, ThreadId};
use seisin_core::datum::DatumId;
use seisin_core::store::Store;
use seisin_ops::registry::OpRegistry;
use seisin_ring::ring::Ring;

use crate::peer_link::{PeerLink, PeerLinkRegistry};
use crate::worker::{AcquireReply, RecallReply, WorkerHandle, WorkerMessage};

pub struct WorkerPool {
  handles: Vec<WorkerHandle>,
  ring: Arc<RwLock<Ring>>,
}

impl WorkerPool {
  /// Spawns `thread_count` interconnected worker threads sharing one
  /// `store` and `ops` registry, plus the node-to-node peer-link
  /// registry every thread uses for cross-node `Acquire`/`Recall`
  /// traffic — built here, after `peers` exists but before any worker
  /// thread starts, since the registry's own incoming-request dispatch
  /// needs `peers` to route by `ThreadId`.
  #[allow(clippy::too_many_arguments)]
  pub fn spawn(
    store: Arc<dyn Store>,
    thread_count: u32,
    ops: Arc<OpRegistry>,
    ring: Arc<RwLock<Ring>>,
    self_node_id: NodeId,
    peer_link_listener: TcpListener,
    peer_link_address_book: Arc<HashMap<NodeId, String>>,
    index_kinds: Arc<crate::index_handler::IndexKindRegistry>,
  ) -> Self {
    let mut senders = Vec::with_capacity(thread_count as usize);
    let mut receivers = Vec::with_capacity(thread_count as usize);
    for _ in 0..thread_count {
      let (tx, rx) = mpsc::channel::<WorkerMessage>();
      senders.push(tx);
      receivers.push(rx);
    }
    let peers = Arc::new(senders);

    let dispatch_peers = Arc::clone(&peers);
    let on_request: Arc<
      dyn Fn(ThreadId, seisin_protocol::Request, Arc<PeerLink>, u64) + Send + Sync,
    > = Arc::new(move |target_thread, request, link, correlation_id| {
      let message = match request {
        seisin_protocol::Request::Acquire {
          op_id,
          datum_id,
          requester_node,
          requester_thread,
        } => WorkerMessage::Acquire {
          op_id,
          datum_id,
          requester_node,
          requester_thread,
          reply: AcquireReply::Remote(Arc::clone(&link), correlation_id),
        },
        seisin_protocol::Request::Recall { datum_id } => WorkerMessage::Recall {
          datum_id,
          reply: RecallReply::Remote(Arc::clone(&link), correlation_id),
        },
        seisin_protocol::Request::Release { datum_id } => {
          // Fire-and-forget, same as the local same-node case — ack
          // immediately rather than waiting on the local dispatch,
          // since the sender's callback doesn't depend on timing for
          // correctness (see try_run_if_ready).
          link.respond(correlation_id, seisin_protocol::Response::Released);
          WorkerMessage::Release { datum_id }
        }
        seisin_protocol::Request::Op { .. } => return, // client-only; never sent over a peer-link
        seisin_protocol::Request::IndexUpdate {
          target,
          op_id,
          index_kind,
          payload,
        } => WorkerMessage::IndexUpdate {
          target,
          op_id,
          index_kind,
          payload,
          reply: crate::worker::IndexUpdateReply::Remote(Arc::clone(&link), correlation_id),
        },
      };
      let _ = dispatch_peers[target_thread.0 as usize].send(message);
    });
    let peer_links = PeerLinkRegistry::start(
      peer_link_listener,
      self_node_id,
      peer_link_address_book,
      on_request,
    );

    let handles = receivers
      .into_iter()
      .enumerate()
      .map(|(idx, receiver)| {
        WorkerHandle::spawn(
          ThreadId(idx as u32),
          receiver,
          peers[idx].clone(),
          Arc::clone(&peers),
          Arc::clone(&store),
          Arc::clone(&ops),
          Arc::clone(&ring),
          self_node_id,
          Arc::clone(&peer_links),
          Arc::clone(&index_kinds),
        )
      })
      .collect();
    Self { handles, ring }
  }

  /// Picks the local thread that natively owns the most of
  /// `datum_ids` (3a's thread-assignment heuristic, unchanged), assigns
  /// the op to it, and blocks for the result. Callers are responsible
  /// for having already confirmed every id in `datum_ids` resolves to
  /// this node — see `server.rs::handle_op_request`.
  pub fn run_op(
    &self,
    op_id: DatumId,
    op_name: String,
    datum_ids: Vec<DatumId>,
    payload: Vec<u8>,
  ) -> Result<Vec<u8>, String> {
    let destination = self.pick_destination_thread(&datum_ids);
    self.handles[destination.0 as usize].run_op(op_id, op_name, datum_ids, payload)
  }

  fn pick_destination_thread(&self, datum_ids: &[DatumId]) -> ThreadId {
    let ring = self.ring.read().unwrap();
    let mut counts: HashMap<ThreadId, usize> = HashMap::new();
    for id in datum_ids {
      let (_, thread_id) = ring.native(*id);
      *counts.entry(thread_id).or_insert(0) += 1;
    }
    *counts
      .iter()
      .max_by_key(|(thread_id, count)| (**count, std::cmp::Reverse(thread_id.0)))
      .map(|(thread_id, _)| thread_id)
      .unwrap_or(&ThreadId(0))
  }

  /// Asks every worker in the pool to evict cache entries `is_native`
  /// rejects — see `WorkerHandle::evict_non_native`.
  pub fn evict_non_native(&self, is_native: Arc<dyn Fn(DatumId) -> bool + Send + Sync>) {
    for handle in &self.handles {
      handle.evict_non_native(Arc::clone(&is_native));
    }
  }

  /// Tells every worker in the pool that `node_id` is confirmed dead —
  /// see `WorkerHandle::release_locks_held_by`.
  pub fn release_locks_held_by(&self, node_id: NodeId) {
    for handle in &self.handles {
      handle.release_locks_held_by(node_id);
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use seisin_core::store::InMemoryStore;

  fn no_peers() -> (TcpListener, Arc<HashMap<NodeId, String>>) {
    (
      TcpListener::bind("127.0.0.1:0").unwrap(),
      Arc::new(HashMap::new()),
    )
  }

  fn test_pool(thread_count: u32) -> WorkerPool {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(
      NodeId(1),
      thread_count,
    )])));
    let (listener, address_book) = no_peers();
    WorkerPool::spawn(
      Arc::new(InMemoryStore::new()),
      thread_count,
      Arc::new(OpRegistry::new()),
      ring,
      NodeId(1),
      listener,
      address_book,
      Arc::new(crate::index_handler::IndexKindRegistry::new()),
    )
  }

  #[test]
  fn run_op_executes_a_registered_op_and_returns_its_result() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 2)])));
    let mut ops = OpRegistry::new();
    ops.register(
      "put_first",
      Box::new(|ctx, ids, payload| {
        ctx.put(ids[0], payload.to_vec());
        payload.to_vec()
      }),
    );
    let (listener, address_book) = no_peers();
    let pool = WorkerPool::spawn(
      Arc::new(InMemoryStore::new()),
      2,
      Arc::new(ops),
      ring,
      NodeId(1),
      listener,
      address_book,
      Arc::new(crate::index_handler::IndexKindRegistry::new()),
    );
    let id = DatumId::new();
    let result = pool.run_op(
      DatumId::new(),
      "put_first".to_string(),
      vec![id],
      b"hi".to_vec(),
    );
    assert_eq!(result, Ok(b"hi".to_vec()));
  }

  #[test]
  fn run_op_on_unknown_name_returns_an_error() {
    let pool = test_pool(1);
    assert!(pool
      .run_op(DatumId::new(), "nope".to_string(), vec![], vec![])
      .is_err());
  }

  #[test]
  fn writes_from_one_op_are_visible_to_a_later_op_regardless_of_thread() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 2)])));
    let mut ops = OpRegistry::new();
    ops.register(
      "put_first",
      Box::new(|ctx, ids, payload| {
        ctx.put(ids[0], payload.to_vec());
        vec![]
      }),
    );
    ops.register(
      "get_first",
      Box::new(|ctx, ids, _payload| ctx.get(ids[0]).unwrap_or_default()),
    );
    let (listener, address_book) = no_peers();
    let pool = WorkerPool::spawn(
      Arc::new(InMemoryStore::new()),
      2,
      Arc::new(ops),
      ring,
      NodeId(1),
      listener,
      address_book,
      Arc::new(crate::index_handler::IndexKindRegistry::new()),
    );
    let id = DatumId::new();
    pool
      .run_op(
        DatumId::new(),
        "put_first".to_string(),
        vec![id],
        b"hello".to_vec(),
      )
      .unwrap();
    let result = pool.run_op(DatumId::new(), "get_first".to_string(), vec![id], vec![]);
    assert_eq!(result, Ok(b"hello".to_vec()));
  }

  #[test]
  fn release_locks_held_by_does_not_disturb_unrelated_locks_or_break_the_pool() {
    // This test only proves the broadcast plumbing (WorkerPool ->
    // every WorkerHandle -> every NativeLock) doesn't panic and
    // doesn't disturb locks unrelated to the dead node — it can't, at
    // this single-node layer, construct a lock genuinely held by some
    // *other* node's thread (that requires real cross-node contention).
    // The actual release-a-remote-holder mechanics are proven
    // end-to-end by Task 7's integration test, which has a real second
    // node to hold the lock in the first place.
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let mut ops = OpRegistry::new();
    ops.register(
      "touch",
      Box::new(|ctx, ids, _payload| {
        ctx.put(ids[0], b"touched".to_vec());
        vec![]
      }),
    );
    let (listener, address_book) = no_peers();
    let pool = WorkerPool::spawn(
      Arc::new(InMemoryStore::new()),
      1,
      Arc::new(ops),
      Arc::clone(&ring),
      NodeId(1),
      listener,
      address_book,
      Arc::new(crate::index_handler::IndexKindRegistry::new()),
    );

    let id = DatumId::new();
    pool
      .run_op(DatumId::new(), "touch".to_string(), vec![id], vec![])
      .unwrap();

    pool.release_locks_held_by(NodeId(99));

    let result = pool.run_op(DatumId::new(), "touch".to_string(), vec![id], vec![]);
    assert_eq!(result, Ok(vec![]));
  }
}
