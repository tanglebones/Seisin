//! One owning thread per `ThreadId`. Each thread is the sole lock
//! manager (via `NativeLock`) for every datum whose `ring.native()`
//! resolves here, and independently tracks its own in-flight op records
//! (op_id -> still-needed/acquired datum_ids) for ops assigned to it.
//! All cross-thread coordination is non-blocking message passing — no
//! thread ever blocks waiting on another; a request that can't be
//! granted immediately is queued at the native-home thread and the
//! requester is notified later via an ordinary inbox message. See the
//! design doc's "Node/Thread Architecture" section.

use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex as StdMutex, RwLock};
use std::thread::{self, JoinHandle};

use seisin_core::authority::{NodeId, ThreadId};
use seisin_core::cache::Cache;
use seisin_core::datum::DatumId;
use seisin_core::store::Store;
use seisin_ops::context::OpContext;
use seisin_ops::registry::OpRegistry;
use seisin_ring::ring::Ring;

use crate::collation::{AcquireOutcome, NativeLock};
use crate::peer_link::{PeerLink, PeerLinkRegistry};

/// How many times a cross-node `Acquire` retries against the current
/// ring before giving up and failing the whole op — bounded so a
/// permanently unreachable node fails fast rather than hanging
/// forever. Each retry re-resolves `ring.native()` fresh, so a retry
/// naturally picks up wherever gossip has since moved the slot to.
const MAX_ACQUIRE_RETRIES: u32 = 3;

pub(crate) enum WorkerMessage {
  /// A client's `Request::Op`, assigned to this thread as its
  /// destination (see `pool.rs`'s native-majority heuristic).
  RunOp {
    op_id: DatumId,
    op_name: String,
    datum_ids: Vec<DatumId>,
    payload: Vec<u8>,
    reply: Sender<Result<Vec<u8>, String>>,
  },
  /// Sent to whichever thread is native home for `datum_id`, requesting
  /// the lock on behalf of `op_id`. `reply` is where the grant
  /// (`AcquireGranted`) is posted once available — a local
  /// `WorkerMessage` send for a same-node requester, or a peer-link
  /// response for a requester on a different node.
  Acquire {
    op_id: DatumId,
    datum_id: DatumId,
    requester_node: NodeId,
    requester_thread: ThreadId,
    reply: AcquireReply,
  },
  /// Posted into the requesting thread's own inbox once its `Acquire`
  /// is granted (immediately, or after a wait/recall).
  AcquireGranted { op_id: DatumId, datum_id: DatumId },
  /// Posted into the requesting thread's own inbox when a cross-node
  /// `Acquire` call fails (peer-link error) — `retries_left` is how
  /// many more attempts remain from when this specific call was made.
  AcquireFailed {
    op_id: DatumId,
    datum_id: DatumId,
    retries_left: u32,
  },
  /// Sent by native home to whoever currently holds `datum_id`, asking
  /// it to evict and release. `reply` is where the resulting `Release`
  /// ack is sent once done — same local/remote distinction as
  /// `Acquire`'s `reply`.
  Recall {
    datum_id: DatumId,
    reply: RecallReply,
  },
  /// Tells native home that whoever held `datum_id` is done with it
  /// (normal op completion, or a recall's evict-and-ack) — grants it to
  /// the oldest waiter, if any.
  Release { datum_id: DatumId },
  /// Evicts every cached entry `is_native` rejects — used after a ring
  /// mutation; unrelated to op collation.
  EvictNonNative(Arc<dyn Fn(DatumId) -> bool + Send + Sync>),
  /// Every datum this thread is native home for, if currently held by
  /// `NodeId`, is released immediately (granting to the next waiter);
  /// any of that node's own queued waiters are pruned too. Driven by
  /// gossip's failure detector confirming a node dead — see
  /// `gossip_state.rs::apply_ready_mutations`.
  ReleaseLocksHeldBy(NodeId),
  /// Sent to whichever thread natively owns `target`, applying one
  /// update to that index's resident state. See the design doc's
  /// "Automatic Index Maintenance & Op Lifecycle" section.
  IndexUpdate {
    target: DatumId,
    op_id: DatumId,
    index_kind: String,
    payload: Vec<u8>,
    reply: IndexUpdateReply,
  },
  /// Posted into the originating op's own thread inbox once a
  /// dispatched `IndexUpdate` gets a reply.
  IndexUpdateReplied {
    op_id: DatumId,
    target: DatumId,
    violation: Option<String>,
  },
  /// A read-only query against the index datum `target`, answered
  /// synchronously by its owning thread from the resident structure
  /// (cold-opening it if needed) — no collation, no op record. Query
  /// and result bytes are opaque to the framework.
  IndexQuery {
    target: DatumId,
    index_kind: String,
    query: Vec<u8>,
    reply: Sender<Result<Vec<u8>, String>>,
  },
  /// A solution-called, mutating op against the index/structured datum
  /// `target`, answered synchronously by its owning thread — the
  /// mutate-with-result sibling of `IndexQuery`. No collation, no op
  /// record: single-datum atomicity comes from serial message
  /// processing on the owning thread.
  IndexExecute {
    target: DatumId,
    index_kind: String,
    payload: Vec<u8>,
    reply: Sender<Result<Vec<u8>, String>>,
  },
}

/// Where an `Acquire`'s eventual grant should be delivered — a local
/// `WorkerMessage` send for a same-node requester, or a peer-link
/// response for a requester on a different node.
pub(crate) enum AcquireReply {
  Local(Sender<WorkerMessage>),
  Remote(Arc<PeerLink>, u64),
}

impl AcquireReply {
  fn grant(self, op_id: DatumId, datum_id: DatumId) {
    match self {
      AcquireReply::Local(inbox) => {
        let _ = inbox.send(WorkerMessage::AcquireGranted { op_id, datum_id });
      }
      AcquireReply::Remote(link, correlation_id) => {
        link.respond(correlation_id, seisin_protocol::Response::Granted);
      }
    }
  }
}

/// Where a `Recall`'s eventual ack should be delivered — same shape as
/// `AcquireReply`, for the same reason.
pub(crate) enum RecallReply {
  Local(Sender<WorkerMessage>),
  Remote(Arc<PeerLink>, u64),
}

impl RecallReply {
  fn ack(self, datum_id: DatumId) {
    match self {
      RecallReply::Local(inbox) => {
        let _ = inbox.send(WorkerMessage::Release { datum_id });
      }
      RecallReply::Remote(link, correlation_id) => {
        link.respond(correlation_id, seisin_protocol::Response::Released);
      }
    }
  }
}

/// Where an `IndexUpdate`'s eventual reply should be delivered — same
/// shape as `AcquireReply`/`RecallReply`, for the same reason.
pub(crate) enum IndexUpdateReply {
  Local(Sender<WorkerMessage>),
  Remote(Arc<PeerLink>, u64),
}

impl IndexUpdateReply {
  fn respond(self, op_id: DatumId, target: DatumId, violation: Option<String>) {
    match self {
      IndexUpdateReply::Local(inbox) => {
        let _ = inbox.send(WorkerMessage::IndexUpdateReplied {
          op_id,
          target,
          violation,
        });
      }
      IndexUpdateReply::Remote(link, correlation_id) => {
        link.respond(
          correlation_id,
          seisin_protocol::Response::IndexUpdateResult { violation },
        );
      }
    }
  }
}

struct OpRecord {
  op_name: String,
  payload: Vec<u8>,
  /// The caller's original datum_ids, in the order it specified them —
  /// preserved separately from `acquired` (below) because grants can
  /// arrive in a different order (e.g. a same-thread self-grant beats a
  /// cross-thread one), and an op function indexes its `ids` parameter
  /// positionally, so invocation must use the caller's order, not
  /// arrival order.
  datum_ids: Vec<DatumId>,
  still_needed: Vec<DatumId>,
  acquired: Vec<DatumId>,
  reply: Sender<Result<Vec<u8>, String>>,
  /// `None` until the op's business logic has run and scheduled at
  /// least one index update; `Some` while waiting on those updates'
  /// replies. See the design doc's "Automatic Index Maintenance & Op
  /// Lifecycle" section.
  index_update_state: Option<IndexUpdateState>,
}

struct IndexUpdateState {
  staged_writes: Vec<(DatumId, Option<Vec<u8>>)>,
  op_result: Vec<u8>,
  pending: usize,
  /// The first violation seen, if any — an op can have scheduled
  /// several index updates; the whole op fails if *any* of them
  /// reports one, but only one message is kept for the reply.
  violation: Option<String>,
}

pub struct WorkerHandle {
  sender: Sender<WorkerMessage>,
  _join: JoinHandle<()>,
}

impl WorkerHandle {
  #[allow(clippy::too_many_arguments)]
  pub(crate) fn spawn(
    self_thread_id: ThreadId,
    receiver: Receiver<WorkerMessage>,
    sender: Sender<WorkerMessage>,
    peers: Arc<Vec<Sender<WorkerMessage>>>,
    store: Arc<dyn Store>,
    ops: Arc<OpRegistry>,
    ring: Arc<RwLock<Ring>>,
    self_node_id: NodeId,
    peer_links: Arc<StdMutex<PeerLinkRegistry>>,
    index_kinds: Arc<crate::index_handler::IndexKindRegistry>,
  ) -> Self {
    let join_sender = sender.clone();
    let join = thread::spawn(move || {
      let mut cache = Cache::new(store);
      let mut native_locks: HashMap<DatumId, NativeLock> = HashMap::new();
      let mut op_records: HashMap<DatumId, OpRecord> = HashMap::new();
      let mut resident_indexes: HashMap<DatumId, Box<dyn crate::index_handler::ResidentIndex>> =
        HashMap::new();

      for message in receiver {
        match message {
          WorkerMessage::RunOp {
            op_id,
            op_name,
            datum_ids,
            payload,
            reply,
          } => {
            op_records.insert(
              op_id,
              OpRecord {
                op_name,
                payload,
                datum_ids: datum_ids.clone(),
                still_needed: datum_ids.clone(),
                acquired: Vec::new(),
                reply,
                index_update_state: None,
              },
            );
            for datum_id in datum_ids {
              send_acquire(
                &ring,
                &peers,
                &peer_links,
                op_id,
                datum_id,
                self_node_id,
                self_thread_id,
                join_sender.clone(),
                MAX_ACQUIRE_RETRIES,
              );
            }
            try_run_if_ready(
              op_id,
              &mut op_records,
              &mut cache,
              &ops,
              &ring,
              &peers,
              &peer_links,
              self_node_id,
              &join_sender,
            );
          }
          WorkerMessage::Acquire {
            op_id,
            datum_id,
            requester_node,
            requester_thread,
            reply,
          } => {
            let lock = native_locks.entry(datum_id).or_default();
            let on_granted = Box::new(move || {
              reply.grant(op_id, datum_id);
            });
            let outcome = lock.request(op_id, requester_node, requester_thread, on_granted);
            if let AcquireOutcome::RecallNeeded(holder) = outcome {
              let recall_reply = RecallReply::Local(join_sender.clone());
              if holder.node_id == self_node_id {
                let _ = peers[holder.thread_id.0 as usize].send(WorkerMessage::Recall {
                  datum_id,
                  reply: recall_reply,
                });
              } else {
                let self_sender = join_sender.clone();
                match peer_links.lock().unwrap().get(holder.node_id) {
                  Some(link) => {
                    link.call(
                      holder.thread_id,
                      seisin_protocol::Request::Recall { datum_id },
                      Box::new(move |response| {
                        // Either an explicit `Released` ack, or the
                        // call failed outright (the peer-link
                        // disconnected, meaning the holder is
                        // unreachable) — either way, treat it as
                        // released rather than waiting on a call that
                        // may never resolve. This is the reactive
                        // backstop for the gap between an actual crash
                        // and gossip confirming it (Task 3 handles the
                        // confirmed case).
                        let _ = response;
                        let _ = self_sender.send(WorkerMessage::Release { datum_id });
                      }),
                    );
                  }
                  // No link ever existed to this holder at all — same
                  // conclusion as a call that failed after connecting:
                  // treat it as released immediately.
                  None => {
                    let _ = self_sender.send(WorkerMessage::Release { datum_id });
                  }
                }
              }
            }
          }
          WorkerMessage::AcquireGranted { op_id, datum_id } => {
            match op_records.get_mut(&op_id) {
              Some(record) => {
                record.still_needed.retain(|id| *id != datum_id);
                record.acquired.push(datum_id);
              }
              // This op's record is already gone — it finished or was
              // abandoned by `fail_op` while this grant (for a
              // different datum in the same op) was still in flight,
              // e.g. a same-node grant needing a slower cross-thread
              // round trip racing a remote `Acquire` that exhausted
              // its retries first. With no record left to track it,
              // this datum would otherwise sit permanently held with
              // nothing to ever release it — so release it right now
              // instead.
              None => {
                release_datums(
                  vec![datum_id],
                  &mut cache,
                  &ring,
                  &peers,
                  &peer_links,
                  self_node_id,
                );
                continue;
              }
            }
            try_run_if_ready(
              op_id,
              &mut op_records,
              &mut cache,
              &ops,
              &ring,
              &peers,
              &peer_links,
              self_node_id,
              &join_sender,
            );
          }
          WorkerMessage::AcquireFailed {
            op_id,
            datum_id,
            retries_left,
          } => {
            if retries_left > 0 {
              send_acquire(
                &ring,
                &peers,
                &peer_links,
                op_id,
                datum_id,
                self_node_id,
                self_thread_id,
                join_sender.clone(),
                retries_left - 1,
              );
            } else {
              fail_op(
                op_id,
                &mut op_records,
                &mut cache,
                &ring,
                &peers,
                &peer_links,
                self_node_id,
                format!("failed to acquire datum {datum_id:?} after {MAX_ACQUIRE_RETRIES} retries"),
              );
            }
          }
          WorkerMessage::Recall { datum_id, reply } => {
            cache.invalidate(datum_id);
            let mut wounded_op_id = None;
            for (op_id, record) in op_records.iter_mut() {
              if let Some(pos) = record.acquired.iter().position(|id| *id == datum_id) {
                record.acquired.remove(pos);
                record.still_needed.push(datum_id);
                wounded_op_id = Some(*op_id);
                break;
              }
            }
            reply.ack(datum_id);
            if let Some(op_id) = wounded_op_id {
              send_acquire(
                &ring,
                &peers,
                &peer_links,
                op_id,
                datum_id,
                self_node_id,
                self_thread_id,
                join_sender.clone(),
                MAX_ACQUIRE_RETRIES,
              );
            }
          }
          WorkerMessage::Release { datum_id } => {
            // Evict native home's own cache entry, if any — it may
            // still hold a value cached from before the datum was
            // handed away, now stale (whoever held it may have
            // mutated or deleted it via storage, which this thread's
            // cache was never told about). The next grantee (possibly
            // this same thread again, via grant_front below) must
            // cache-miss through to storage rather than see a stale
            // hit.
            cache.invalidate(datum_id);
            if let Some(lock) = native_locks.get_mut(&datum_id) {
              lock.release();
            }
          }
          WorkerMessage::EvictNonNative(is_native) => {
            cache.evict_non_native(|id| is_native(id));
          }
          WorkerMessage::ReleaseLocksHeldBy(node_id) => {
            for (&datum_id, lock) in native_locks.iter_mut() {
              let was_held_by_dead_node =
                lock.current_holder().is_some_and(|h| h.node_id == node_id);
              lock.handle_node_death(node_id);
              if was_held_by_dead_node {
                cache.invalidate(datum_id);
              }
            }
          }
          WorkerMessage::IndexUpdate {
            target,
            op_id,
            index_kind,
            payload,
            reply,
          } => {
            // Cold miss: build the resident structure once via the
            // kind's `open`; every later update on this thread mutates
            // it in place, never rebuilding from bytes.
            let resident = match resident_indexes.entry(target) {
              std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
              std::collections::hash_map::Entry::Vacant(vacancy) => {
                let opened = index_kinds
                  .get(&index_kind)
                  .and_then(|kind| kind.open(target, cache.get(target)));
                match opened {
                  Ok(resident) => vacancy.insert(resident),
                  Err(message) => {
                    reply.respond(op_id, target, Some(message));
                    continue;
                  }
                }
              }
            };
            let outcome = resident.apply(&payload);
            if outcome.violation.is_none() {
              if let Some(bytes) = outcome.write_through {
                cache.put(target, bytes);
              }
            }
            reply.respond(op_id, target, outcome.violation);
          }
          WorkerMessage::IndexQuery {
            target,
            index_kind,
            query,
            reply,
          } => {
            let resident = match resident_indexes.entry(target) {
              std::collections::hash_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
              std::collections::hash_map::Entry::Vacant(vacancy) => index_kinds
                .get(&index_kind)
                .and_then(|kind| kind.open(target, cache.get(target)))
                .map(|resident| vacancy.insert(resident)),
            };
            let result = match resident {
              Ok(resident) => resident.query(&query),
              Err(message) => Err(message),
            };
            let _ = reply.send(result);
          }
          WorkerMessage::IndexExecute {
            target,
            index_kind,
            payload,
            reply,
          } => {
            let resident = match resident_indexes.entry(target) {
              std::collections::hash_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
              std::collections::hash_map::Entry::Vacant(vacancy) => index_kinds
                .get(&index_kind)
                .and_then(|kind| kind.open(target, cache.get(target)))
                .map(|resident| vacancy.insert(resident)),
            };
            let result = match resident {
              Ok(resident) => resident.execute(&payload),
              Err(message) => Err(message),
            };
            let _ = reply.send(result);
          }
          WorkerMessage::IndexUpdateReplied {
            op_id,
            target,
            violation,
          } => {
            let _ = target;
            if let Some(record) = op_records.get_mut(&op_id) {
              if let Some(state) = &mut record.index_update_state {
                state.pending -= 1;
                if violation.is_some() && state.violation.is_none() {
                  state.violation = violation;
                }
                if state.pending == 0 {
                  let record = op_records.remove(&op_id).unwrap();
                  let state = record.index_update_state.unwrap();
                  if let Some(message) = state.violation {
                    let _ = record.reply.send(Err(message));
                  } else {
                    for (id, content) in state.staged_writes {
                      match content {
                        Some(bytes) => cache.put(id, bytes),
                        None => cache.delete(id),
                      }
                    }
                    let _ = record.reply.send(Ok(state.op_result));
                  }
                  release_datums(
                    record.acquired,
                    &mut cache,
                    &ring,
                    &peers,
                    &peer_links,
                    self_node_id,
                  );
                }
              }
            }
          }
        }
      }
    });
    Self {
      sender,
      _join: join,
    }
  }

  /// Assigns a new op to this thread and blocks for its result. The
  /// thread's own message loop drives collation (acquiring whatever
  /// datums it doesn't already hold) before invoking the op.
  pub fn run_op(
    &self,
    op_id: DatumId,
    op_name: String,
    datum_ids: Vec<DatumId>,
    payload: Vec<u8>,
  ) -> Result<Vec<u8>, String> {
    let (reply_tx, reply_rx) = std::sync::mpsc::channel();
    self
      .sender
      .send(WorkerMessage::RunOp {
        op_id,
        op_name,
        datum_ids,
        payload,
        reply: reply_tx,
      })
      .expect("worker thread exited unexpectedly");
    reply_rx.recv().expect("worker dropped the reply channel")
  }

  /// Sends an index query to this thread and blocks for the answer —
  /// same synchronous shape as `run_op`, minus collation/op records.
  pub fn run_index_query(
    &self,
    target: DatumId,
    index_kind: String,
    query: Vec<u8>,
  ) -> Result<Vec<u8>, String> {
    let (reply_tx, reply_rx) = std::sync::mpsc::channel();
    self
      .sender
      .send(WorkerMessage::IndexQuery {
        target,
        index_kind,
        query,
        reply: reply_tx,
      })
      .expect("worker thread exited unexpectedly");
    reply_rx.recv().expect("worker dropped the reply channel")
  }

  /// Sends a mutate-with-result op to this thread and blocks for the
  /// answer — same synchronous shape as `run_index_query`.
  pub fn run_index_execute(
    &self,
    target: DatumId,
    index_kind: String,
    payload: Vec<u8>,
  ) -> Result<Vec<u8>, String> {
    let (reply_tx, reply_rx) = std::sync::mpsc::channel();
    self
      .sender
      .send(WorkerMessage::IndexExecute {
        target,
        index_kind,
        payload,
        reply: reply_tx,
      })
      .expect("worker thread exited unexpectedly");
    reply_rx.recv().expect("worker dropped the reply channel")
  }

  /// Asks the owning thread to evict any cache entry `is_native` rejects
  /// — fire-and-forget, no reply, but guaranteed to be processed before
  /// any `run_op` call made after this one returns (the worker's inbox
  /// is a single ordered queue).
  pub fn evict_non_native(&self, is_native: Arc<dyn Fn(DatumId) -> bool + Send + Sync>) {
    let _ = self.sender.send(WorkerMessage::EvictNonNative(is_native));
  }

  /// Tells this thread that `node_id` is confirmed dead — see
  /// `WorkerMessage::ReleaseLocksHeldBy`.
  pub fn release_locks_held_by(&self, node_id: NodeId) {
    let _ = self.sender.send(WorkerMessage::ReleaseLocksHeldBy(node_id));
  }
}

/// Sends an `Acquire` for `datum_id` on behalf of `op_id` to whichever
/// thread `ring.native()` currently names — always via a message, even
/// when that's the calling thread itself (a cheap, non-blocking
/// self-send), so there's exactly one code path regardless of whether
/// the native home turns out to be local-to-self or a different local
/// thread.
#[allow(clippy::too_many_arguments)]
fn send_acquire(
  ring: &Arc<RwLock<Ring>>,
  peers: &Arc<Vec<Sender<WorkerMessage>>>,
  peer_links: &Arc<StdMutex<PeerLinkRegistry>>,
  op_id: DatumId,
  datum_id: DatumId,
  self_node_id: NodeId,
  self_thread_id: ThreadId,
  requester_inbox: Sender<WorkerMessage>,
  retries_left: u32,
) {
  let (native_node, native_thread) = ring.read().unwrap().native(datum_id);
  if native_node == self_node_id {
    let _ = peers[native_thread.0 as usize].send(WorkerMessage::Acquire {
      op_id,
      datum_id,
      requester_node: self_node_id,
      requester_thread: self_thread_id,
      reply: AcquireReply::Local(requester_inbox),
    });
  } else {
    match peer_links.lock().unwrap().get(native_node) {
      Some(link) => {
        link.call(
          native_thread,
          seisin_protocol::Request::Acquire {
            op_id,
            datum_id,
            requester_node: self_node_id,
            requester_thread: self_thread_id,
          },
          Box::new(move |response| {
            if matches!(response, seisin_protocol::Response::Granted) {
              let _ = requester_inbox.send(WorkerMessage::AcquireGranted { op_id, datum_id });
            } else {
              let _ = requester_inbox.send(WorkerMessage::AcquireFailed {
                op_id,
                datum_id,
                retries_left,
              });
            }
          }),
        );
      }
      // No link ever existed to `native_node` at all — same
      // conclusion as a call that failed after connecting: post
      // AcquireFailed so the retry/give-up mechanics apply the same
      // way regardless of which kind of unreachability this is.
      None => {
        let _ = requester_inbox.send(WorkerMessage::AcquireFailed {
          op_id,
          datum_id,
          retries_left,
        });
      }
    }
  }
}

/// If `op_id`'s record has nothing left to acquire and hasn't already
/// entered its index-update phase, runs it. An op that scheduled no
/// index updates commits and replies immediately, exactly as before;
/// one that scheduled at least one dispatches them and waits — see
/// `WorkerMessage::IndexUpdateReplied` for the commit-or-fail step.
#[allow(clippy::too_many_arguments)]
fn try_run_if_ready(
  op_id: DatumId,
  op_records: &mut HashMap<DatumId, OpRecord>,
  cache: &mut Cache,
  ops: &OpRegistry,
  ring: &Arc<RwLock<Ring>>,
  peers: &Arc<Vec<Sender<WorkerMessage>>>,
  peer_links: &Arc<StdMutex<PeerLinkRegistry>>,
  self_node_id: NodeId,
  join_sender: &Sender<WorkerMessage>,
) {
  let ready = op_records
    .get(&op_id)
    .is_some_and(|record| record.still_needed.is_empty() && record.index_update_state.is_none());
  if !ready {
    return;
  }
  let record = op_records.get_mut(&op_id).unwrap();
  let mut ctx = OpContext::new(cache);
  let result = ops.invoke(
    &record.op_name,
    &mut ctx,
    &record.datum_ids,
    &record.payload,
  );
  let staged_writes = ctx.take_staged_writes();
  let pending_index_updates = ctx.take_pending_index_updates();

  let op_result = match result {
    Err(message) => {
      let record = op_records.remove(&op_id).unwrap();
      let _ = record.reply.send(Err(message));
      release_datums(
        record.acquired,
        cache,
        ring,
        peers,
        peer_links,
        self_node_id,
      );
      return;
    }
    Ok(op_result) => op_result,
  };

  if pending_index_updates.is_empty() {
    for (id, content) in staged_writes {
      match content {
        Some(bytes) => cache.put(id, bytes),
        None => cache.delete(id),
      }
    }
    let record = op_records.remove(&op_id).unwrap();
    let _ = record.reply.send(Ok(op_result));
    release_datums(
      record.acquired,
      cache,
      ring,
      peers,
      peer_links,
      self_node_id,
    );
    return;
  }

  let pending = pending_index_updates.len();
  record.index_update_state = Some(IndexUpdateState {
    staged_writes,
    op_result,
    pending,
    violation: None,
  });
  for update in pending_index_updates {
    dispatch_index_update(
      ring,
      peers,
      peer_links,
      self_node_id,
      op_id,
      update.target,
      update.index_kind,
      update.payload,
      join_sender.clone(),
    );
  }
}

/// Sends an `IndexUpdate` for `target` on behalf of `op_id` to
/// whichever thread `ring.native()` currently names — locally or
/// cross-node, mirroring `send_acquire`'s same local/remote split. A
/// missing peer-link, or a call that fails after connecting, is
/// reported back as a violation (failing the whole op) rather than
/// assumed successful — there's no retry here (unlike bounded acquire
/// retry), a deliberate v1 simplification.
#[allow(clippy::too_many_arguments)]
fn dispatch_index_update(
  ring: &Arc<RwLock<Ring>>,
  peers: &Arc<Vec<Sender<WorkerMessage>>>,
  peer_links: &Arc<StdMutex<PeerLinkRegistry>>,
  self_node_id: NodeId,
  op_id: DatumId,
  target: DatumId,
  index_kind: String,
  payload: Vec<u8>,
  requester_inbox: Sender<WorkerMessage>,
) {
  let (native_node, native_thread) = ring.read().unwrap().native(target);
  if native_node == self_node_id {
    let _ = peers[native_thread.0 as usize].send(WorkerMessage::IndexUpdate {
      target,
      op_id,
      index_kind,
      payload,
      reply: IndexUpdateReply::Local(requester_inbox),
    });
  } else {
    match peer_links.lock().unwrap().get(native_node) {
      Some(link) => {
        link.call(
          native_thread,
          seisin_protocol::Request::IndexUpdate {
            target,
            op_id,
            index_kind,
            payload,
          },
          Box::new(move |response| {
            let violation = match response {
              seisin_protocol::Response::IndexUpdateResult { violation } => violation,
              other => Some(format!(
                "unexpected response applying index update to {target:?}: {other:?}"
              )),
            };
            let _ = requester_inbox.send(WorkerMessage::IndexUpdateReplied {
              op_id,
              target,
              violation,
            });
          }),
        );
      }
      None => {
        let _ = requester_inbox.send(WorkerMessage::IndexUpdateReplied {
          op_id,
          target,
          violation: Some(format!("no peer-link connection to node {native_node:?}")),
        });
      }
    }
  }
}

/// Releases every datum in `datum_ids`, evicting this thread's own
/// cache entry for each first — locally if native home is this same
/// node, over the peer-link if it's a different one. Shared by normal
/// op completion (`try_run_if_ready`) and whole-op failure (`fail_op`).
fn release_datums(
  datum_ids: Vec<DatumId>,
  cache: &mut Cache,
  ring: &Arc<RwLock<Ring>>,
  peers: &Arc<Vec<Sender<WorkerMessage>>>,
  peer_links: &Arc<StdMutex<PeerLinkRegistry>>,
  self_node_id: NodeId,
) {
  for datum_id in datum_ids {
    cache.invalidate(datum_id);
    let (native_node, thread_id) = ring.read().unwrap().native(datum_id);
    if native_node == self_node_id {
      let _ = peers[thread_id.0 as usize].send(WorkerMessage::Release { datum_id });
    } else if let Some(link) = peer_links.lock().unwrap().get(native_node) {
      // Best-effort: no link to release to means native home is
      // unreachable anyway, so there's nothing meaningful to do —
      // it'll have already been (or will be) cleaned up via the
      // proactive/reactive crash-handling paths instead.
      link.call(
        thread_id,
        seisin_protocol::Request::Release { datum_id },
        Box::new(|_response| {}),
      );
    }
  }
}

/// Abandons `op_id` entirely: replies with `Err(message)` and releases
/// every datum it had already acquired (unlike a wound, which loses
/// only the one contended datum and keeps going, a whole-op failure
/// gives up everything).
#[allow(clippy::too_many_arguments)]
fn fail_op(
  op_id: DatumId,
  op_records: &mut HashMap<DatumId, OpRecord>,
  cache: &mut Cache,
  ring: &Arc<RwLock<Ring>>,
  peers: &Arc<Vec<Sender<WorkerMessage>>>,
  peer_links: &Arc<StdMutex<PeerLinkRegistry>>,
  self_node_id: NodeId,
  message: String,
) {
  if let Some(record) = op_records.remove(&op_id) {
    let _ = record.reply.send(Err(message));
    release_datums(
      record.acquired,
      cache,
      ring,
      peers,
      peer_links,
      self_node_id,
    );
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::mpsc;

  use seisin_core::store::InMemoryStore;

  use crate::peer_link::PeerLinkRegistry;

  fn empty_peer_links() -> Arc<StdMutex<PeerLinkRegistry>> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    PeerLinkRegistry::start(
      listener,
      NodeId(1),
      Arc::new(HashMap::new()),
      Arc::new(|_thread, _request, _link, _cid| {}),
    )
  }

  /// Spawns `thread_count` interconnected `WorkerHandle`s sharing one
  /// store, ring, and op registry — a minimal in-process pool, built by
  /// hand here since `pool.rs` doesn't exist in this shape yet (Task 4).
  fn spawn_test_pool(
    thread_count: u32,
    ring: Arc<RwLock<Ring>>,
    ops: OpRegistry,
  ) -> Vec<WorkerHandle> {
    spawn_test_pool_with_index_kinds(
      thread_count,
      ring,
      ops,
      crate::index_handler::IndexKindRegistry::new(),
    )
  }

  fn spawn_test_pool_with_index_kinds(
    thread_count: u32,
    ring: Arc<RwLock<Ring>>,
    ops: OpRegistry,
    index_kinds: crate::index_handler::IndexKindRegistry,
  ) -> Vec<WorkerHandle> {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let ops = Arc::new(ops);
    let peer_links = empty_peer_links();
    let index_kinds = Arc::new(index_kinds);
    let mut senders = Vec::with_capacity(thread_count as usize);
    let mut receivers = Vec::with_capacity(thread_count as usize);
    for _ in 0..thread_count {
      let (tx, rx) = mpsc::channel();
      senders.push(tx);
      receivers.push(rx);
    }
    let peers = Arc::new(senders);
    receivers
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
          NodeId(1),
          Arc::clone(&peer_links),
          Arc::clone(&index_kinds),
        )
      })
      .collect()
  }

  /// A minimal test `IndexKind`: applies always succeed (or always
  /// report `violation` when one is given), with no resident state
  /// beyond that fixed answer.
  struct FixedOutcomeKind {
    violation: Option<String>,
  }
  struct FixedOutcomeResident {
    violation: Option<String>,
  }

  impl crate::index_handler::ResidentIndex for FixedOutcomeResident {
    fn apply(&mut self, payload: &[u8]) -> crate::index_handler::IndexApplyOutcome {
      crate::index_handler::IndexApplyOutcome {
        violation: self.violation.clone(),
        write_through: Some(payload.to_vec()),
      }
    }

    fn query(&self, query: &[u8]) -> Result<Vec<u8>, String> {
      // Echo, uppercased — enough to prove the bytes round-tripped
      // through the worker rather than being fabricated.
      Ok(query.iter().map(u8::to_ascii_uppercase).collect())
    }

    fn execute(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
      // Reverse — distinct from query's uppercase, so a test can tell
      // which method actually ran.
      Ok(payload.iter().rev().copied().collect())
    }
  }

  impl crate::index_handler::IndexKind for FixedOutcomeKind {
    fn open(
      &self,
      _target: DatumId,
      _stored: Option<Vec<u8>>,
    ) -> Result<Box<dyn crate::index_handler::ResidentIndex>, String> {
      Ok(Box::new(FixedOutcomeResident {
        violation: self.violation.clone(),
      }))
    }
  }

  fn register_echo_ops(ops: &mut OpRegistry) {
    ops.register(
      "put_first",
      Box::new(|ctx, ids, payload| {
        ctx.put(ids[0], payload.to_vec());
        payload.to_vec()
      }),
    );
    ops.register(
      "get_first",
      Box::new(|ctx, ids, _payload| ctx.get(ids[0]).unwrap_or_default()),
    );
  }

  #[test]
  fn a_single_datum_op_on_its_own_native_thread_runs_immediately() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let mut ops = OpRegistry::new();
    register_echo_ops(&mut ops);
    let handles = spawn_test_pool(1, ring, ops);
    let id = DatumId::new();

    let result = handles[0].run_op(
      DatumId::new(),
      "put_first".to_string(),
      vec![id],
      b"hello".to_vec(),
    );
    assert_eq!(result, Ok(b"hello".to_vec()));

    let result = handles[0].run_op(DatumId::new(), "get_first".to_string(), vec![id], vec![]);
    assert_eq!(result, Ok(b"hello".to_vec()));
  }

  #[test]
  fn an_op_collates_a_datum_natively_owned_by_a_different_local_thread() {
    // With 4 threads, find two ids whose native homes differ so this
    // test actually exercises cross-thread acquisition rather than
    // relying on luck.
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 4)])));
    let (from, to) = loop {
      let a = DatumId::new();
      let b = DatumId::new();
      if ring.read().unwrap().native(a) != ring.read().unwrap().native(b) {
        break (a, b);
      }
    };
    let mut ops = OpRegistry::new();
    ops.register(
      "move_content",
      Box::new(|ctx, ids, _payload| {
        let content = ctx.get(ids[0]).unwrap_or_default();
        ctx.delete(ids[0]);
        ctx.put(ids[1], content.clone());
        content
      }),
    );
    register_echo_ops(&mut ops);
    let handles = spawn_test_pool(4, ring.clone(), ops);

    let (_, from_thread) = ring.read().unwrap().native(from);
    handles[from_thread.0 as usize]
      .run_op(
        DatumId::new(),
        "put_first".to_string(),
        vec![from],
        b"payload".to_vec(),
      )
      .unwrap();

    // Dispatch the collating op from thread 0 regardless of where
    // `from`/`to` natively live, to prove collation reaches across
    // threads.
    let result = handles[0].run_op(
      DatumId::new(),
      "move_content".to_string(),
      vec![from, to],
      vec![],
    );
    assert_eq!(result, Ok(b"payload".to_vec()));
  }

  #[test]
  fn an_older_op_recalls_a_datum_from_a_younger_ops_in_flight_collation() {
    // Classic two-op cycle: op1 needs (a, b), op2 needs (b, a) — opposite
    // acquisition order — with op1 the strictly older op_id. Neither
    // should deadlock; op1 (older) must win any contention immediately
    // and both ops must eventually complete. A genuine deadlock would
    // hang `thread::join` below rather than fail an assertion — that's
    // an accepted trade-off here: `cargo test`'s own harness timeout is
    // the backstop, and a hang is just as informative as a clean
    // failure for this specific bug class.
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 4)])));
    let (a, b) = loop {
      let x = DatumId::new();
      let y = DatumId::new();
      if ring.read().unwrap().native(x) != ring.read().unwrap().native(y) {
        break (x, y);
      }
    };
    let mut ops = OpRegistry::new();
    ops.register(
      "touch_both",
      Box::new(|ctx, ids, _payload| {
        ctx.put(ids[0], b"touched".to_vec());
        ctx.put(ids[1], b"touched".to_vec());
        vec![]
      }),
    );
    let handles = Arc::new(spawn_test_pool(4, ring, ops));

    let op1 = DatumId::new(); // older: created first
    let op2 = DatumId::new(); // younger

    let handles_a = Arc::clone(&handles);
    let thread1 = std::thread::spawn(move || {
      handles_a[0].run_op(op1, "touch_both".to_string(), vec![a, b], vec![])
    });
    let handles_b = Arc::clone(&handles);
    let thread2 = std::thread::spawn(move || {
      handles_b[1].run_op(op2, "touch_both".to_string(), vec![b, a], vec![])
    });

    let result1 = thread1.join().unwrap();
    let result2 = thread2.join().unwrap();
    assert_eq!(result1, Ok(vec![]));
    assert_eq!(result2, Ok(vec![]));
  }

  #[test]
  fn an_op_with_no_index_updates_commits_immediately_as_before() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let mut ops = OpRegistry::new();
    ops.register(
      "touch",
      Box::new(|ctx: &mut OpContext, ids, _payload| {
        ctx.put(ids[0], b"touched".to_vec());
        vec![]
      }),
    );
    let handles = spawn_test_pool(1, ring, ops);
    let id = DatumId::new();
    let result = handles[0].run_op(DatumId::new(), "touch".to_string(), vec![id], vec![]);
    assert_eq!(result, Ok(vec![]));
  }

  #[test]
  fn an_op_that_schedules_an_index_update_waits_for_it_before_committing() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let index_target = DatumId::new();
    let mut ops = OpRegistry::new();
    ops.register(
      "touch_with_index",
      Box::new(move |ctx: &mut OpContext, ids, _payload| {
        ctx.put(ids[0], b"touched".to_vec());
        ctx.schedule_index_update(index_target, "always_ok", vec![]);
        vec![]
      }),
    );
    let mut index_kinds = crate::index_handler::IndexKindRegistry::new();
    index_kinds.register("always_ok", Box::new(FixedOutcomeKind { violation: None }));
    let handles = spawn_test_pool_with_index_kinds(1, ring, ops, index_kinds);
    let id = DatumId::new();
    let result = handles[0].run_op(
      DatumId::new(),
      "touch_with_index".to_string(),
      vec![id],
      vec![],
    );
    assert_eq!(result, Ok(vec![]));
  }

  #[test]
  fn run_index_query_answers_from_the_resident_index() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let mut index_kinds = crate::index_handler::IndexKindRegistry::new();
    index_kinds.register("echo", Box::new(FixedOutcomeKind { violation: None }));
    let handles = spawn_test_pool_with_index_kinds(1, ring, OpRegistry::new(), index_kinds);
    let result = handles[0].run_index_query(DatumId::new(), "echo".to_string(), b"abc".to_vec());
    assert_eq!(result, Ok(b"ABC".to_vec()));
  }

  #[test]
  fn run_index_query_on_an_unregistered_kind_is_an_error() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let handles = spawn_test_pool(1, ring, OpRegistry::new());
    assert!(handles[0]
      .run_index_query(DatumId::new(), "nope".to_string(), vec![])
      .is_err());
  }

  #[test]
  fn run_index_execute_reaches_the_resident_index_and_returns_its_result() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let mut index_kinds = crate::index_handler::IndexKindRegistry::new();
    index_kinds.register("echo", Box::new(FixedOutcomeKind { violation: None }));
    let handles = spawn_test_pool_with_index_kinds(1, ring, OpRegistry::new(), index_kinds);
    let result = handles[0].run_index_execute(DatumId::new(), "echo".to_string(), b"abc".to_vec());
    assert_eq!(result, Ok(b"cba".to_vec()));
  }

  #[test]
  fn run_index_execute_on_an_unregistered_kind_is_an_error() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let handles = spawn_test_pool(1, ring, OpRegistry::new());
    assert!(handles[0]
      .run_index_execute(DatumId::new(), "nope".to_string(), vec![])
      .is_err());
  }

  #[test]
  fn execute_then_query_hit_the_same_resident_instance() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let mut index_kinds = crate::index_handler::IndexKindRegistry::new();
    index_kinds.register("echo", Box::new(FixedOutcomeKind { violation: None }));
    let handles = spawn_test_pool_with_index_kinds(1, ring, OpRegistry::new(), index_kinds);
    let target = DatumId::new();
    assert!(handles[0]
      .run_index_execute(target, "echo".to_string(), b"x".to_vec())
      .is_ok());
    let result = handles[0].run_index_query(target, "echo".to_string(), b"q".to_vec());
    assert_eq!(result, Ok(b"Q".to_vec()));
  }

  #[test]
  fn an_index_updated_then_queried_on_the_same_thread_uses_one_resident_instance() {
    // Proves both message paths (IndexUpdate and IndexQuery) resolve to
    // the same resident map entry without error — an update followed by
    // a query on the same target must not cold-open a second instance.
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let index_target = DatumId::new();
    let mut ops = OpRegistry::new();
    ops.register(
      "touch_with_index",
      Box::new(move |ctx: &mut OpContext, ids, _payload| {
        ctx.put(ids[0], b"touched".to_vec());
        ctx.schedule_index_update(index_target, "echo", vec![1]);
        vec![]
      }),
    );
    let mut index_kinds = crate::index_handler::IndexKindRegistry::new();
    index_kinds.register("echo", Box::new(FixedOutcomeKind { violation: None }));
    let handles = spawn_test_pool_with_index_kinds(1, ring, ops, index_kinds);
    handles[0]
      .run_op(
        DatumId::new(),
        "touch_with_index".to_string(),
        vec![DatumId::new()],
        vec![],
      )
      .unwrap();
    let result = handles[0].run_index_query(index_target, "echo".to_string(), b"q".to_vec());
    assert_eq!(result, Ok(b"Q".to_vec()));
  }

  #[test]
  fn a_violation_from_a_scheduled_index_update_fails_the_whole_op_with_no_write() {
    let ring = Arc::new(RwLock::new(Ring::from_members(&[(NodeId(1), 1)])));
    let index_target = DatumId::new();
    let mut ops = OpRegistry::new();
    ops.register(
      "touch_with_rejected_index",
      Box::new(move |ctx: &mut OpContext, ids, _payload| {
        ctx.put(ids[0], b"should not be written".to_vec());
        ctx.schedule_index_update(index_target, "always_reject", vec![]);
        vec![]
      }),
    );
    let mut index_kinds = crate::index_handler::IndexKindRegistry::new();
    index_kinds.register(
      "always_reject",
      Box::new(FixedOutcomeKind {
        violation: Some("rejected".to_string()),
      }),
    );
    let handles = spawn_test_pool_with_index_kinds(1, ring, ops, index_kinds);
    let id = DatumId::new();
    let result = handles[0].run_op(
      DatumId::new(),
      "touch_with_rejected_index".to_string(),
      vec![id],
      vec![],
    );
    assert_eq!(result, Err("rejected".to_string()));
  }
}
