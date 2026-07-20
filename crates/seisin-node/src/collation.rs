//! The native-home lock manager for a single datum: tracks who currently
//! holds it and, when contended, resolves priority via the client-
//! generated op_id (older always wins) — see the design doc's "Acquire &
//! Wound-Wait Mechanics" section. A pure data structure with no
//! threading/network concerns of its own; `worker.rs` drives it from
//! inbox messages, translating a grant into whatever message it needs to
//! send via the `on_granted` callback passed into `request`.

use std::collections::VecDeque;

use seisin_core::authority::{NodeId, ThreadId};
use seisin_core::datum::DatumId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Holder {
  pub node_id: NodeId,
  pub thread_id: ThreadId,
  pub op_id: DatumId,
}

struct Waiter {
  op_id: DatumId,
  node_id: NodeId,
  thread_id: ThreadId,
  on_granted: Box<dyn FnOnce() + Send>,
}

/// What the caller must do next, as a result of a `request()` call.
#[derive(Debug, PartialEq, Eq)]
pub enum AcquireOutcome {
  /// No one held it; the requester is now the holder (its `on_granted`
  /// callback has already fired).
  GrantedImmediately,
  /// The requester is older than the current holder — the caller must
  /// send a recall to `Holder`. The requester's `on_granted` callback
  /// fires later, once `release()` is called after the recall completes.
  RecallNeeded(Holder),
  /// The requester is younger than the current holder; it's queued.
  /// Its `on_granted` callback fires once granted, with no polling
  /// needed.
  Queued,
}

#[derive(Default)]
pub struct NativeLock {
  current_holder: Option<Holder>,
  waiters: VecDeque<Waiter>,
}

impl NativeLock {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn current_holder(&self) -> Option<&Holder> {
    self.current_holder.as_ref()
  }

  /// Requests the lock on behalf of `op_id`. Always enqueues the
  /// request in ascending op_id order (a younger request can arrive
  /// after an older one is already queued — e.g. a third op joining
  /// after a recall), then decides what the caller must do next.
  pub fn request(
    &mut self,
    op_id: DatumId,
    node_id: NodeId,
    thread_id: ThreadId,
    on_granted: Box<dyn FnOnce() + Send>,
  ) -> AcquireOutcome {
    let needs_recall = self
      .current_holder
      .as_ref()
      .is_some_and(|h| op_id < h.op_id);
    let insert_at = self
      .waiters
      .iter()
      .position(|w| op_id < w.op_id)
      .unwrap_or(self.waiters.len());
    self.waiters.insert(
      insert_at,
      Waiter {
        op_id,
        node_id,
        thread_id,
        on_granted,
      },
    );
    if self.current_holder.is_none() {
      self.grant_front();
      return AcquireOutcome::GrantedImmediately;
    }
    if needs_recall {
      return AcquireOutcome::RecallNeeded(self.current_holder.clone().unwrap());
    }
    AcquireOutcome::Queued
  }

  /// Releases the current holder (its op finished, or it was recalled
  /// and acknowledged) and grants the datum to the oldest waiter, if
  /// any.
  pub fn release(&mut self) {
    self.current_holder = None;
    self.grant_front();
  }

  /// `node_id` is confirmed dead: releases it as the current holder if
  /// it was one (granting to the next surviving waiter, if any), and
  /// prunes any of its own still-queued waiters first, so a dead node's
  /// pending request is never later handed a grant nobody will use.
  pub fn handle_node_death(&mut self, node_id: NodeId) {
    self.waiters.retain(|w| w.node_id != node_id);
    if self
      .current_holder
      .as_ref()
      .is_some_and(|h| h.node_id == node_id)
    {
      self.release();
    }
  }

  fn grant_front(&mut self) {
    if let Some(waiter) = self.waiters.pop_front() {
      self.current_holder = Some(Holder {
        node_id: waiter.node_id,
        thread_id: waiter.thread_id,
        op_id: waiter.op_id,
      });
      (waiter.on_granted)();
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::mpsc;

  #[test]
  fn first_request_on_an_idle_lock_is_granted_immediately() {
    let mut lock = NativeLock::new();
    let (tx, rx) = mpsc::channel();
    let op_id = DatumId::new();
    let outcome = lock.request(
      op_id,
      NodeId(1),
      ThreadId(0),
      Box::new(move || {
        let _ = tx.send(());
      }),
    );
    assert_eq!(outcome, AcquireOutcome::GrantedImmediately);
    assert!(rx.try_recv().is_ok());
    assert_eq!(
      lock.current_holder(),
      Some(&Holder {
        node_id: NodeId(1),
        thread_id: ThreadId(0),
        op_id
      })
    );
  }

  #[test]
  fn a_younger_request_against_a_held_lock_is_queued_without_firing() {
    let mut lock = NativeLock::new();
    let (tx1, _rx1) = mpsc::channel();
    let older = DatumId::new();
    lock.request(
      older,
      NodeId(1),
      ThreadId(0),
      Box::new(move || {
        let _ = tx1.send(());
      }),
    );

    let (tx2, rx2) = mpsc::channel();
    let younger = DatumId::new(); // created after `older`, so it sorts greater
    let outcome = lock.request(
      younger,
      NodeId(1),
      ThreadId(1),
      Box::new(move || {
        let _ = tx2.send(());
      }),
    );
    assert_eq!(outcome, AcquireOutcome::Queued);
    assert!(rx2.try_recv().is_err());
  }

  #[test]
  fn an_older_request_against_a_held_lock_needs_a_recall() {
    let mut lock = NativeLock::new();
    let a = DatumId::new();
    let b = DatumId::new();
    let (older, younger) = if a < b { (a, b) } else { (b, a) };

    let (tx_holder, _rx_holder) = mpsc::channel();
    lock.request(
      younger,
      NodeId(1),
      ThreadId(0),
      Box::new(move || {
        let _ = tx_holder.send(());
      }),
    );

    let (tx_requester, _rx_requester) = mpsc::channel();
    let outcome = lock.request(
      older,
      NodeId(2),
      ThreadId(3),
      Box::new(move || {
        let _ = tx_requester.send(());
      }),
    );
    match outcome {
      AcquireOutcome::RecallNeeded(holder) => assert_eq!(holder.op_id, younger),
      other => panic!("expected RecallNeeded, got {other:?}"),
    }
  }

  #[test]
  fn release_grants_to_the_oldest_queued_waiter_even_if_it_arrived_second() {
    let mut lock = NativeLock::new();
    let holder_id = DatumId::new();
    let (tx_holder, _rx_holder) = mpsc::channel();
    lock.request(
      holder_id,
      NodeId(1),
      ThreadId(0),
      Box::new(move || {
        let _ = tx_holder.send(());
      }),
    );

    let a = DatumId::new();
    let b = DatumId::new();
    let (first_waiter, second_waiter) = if a < b { (a, b) } else { (b, a) };
    let (tx1, rx1) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();
    // Insert the younger of the two waiters first, to prove granting
    // order is by op_id, not arrival order.
    lock.request(
      second_waiter,
      NodeId(1),
      ThreadId(1),
      Box::new(move || {
        let _ = tx2.send(());
      }),
    );
    lock.request(
      first_waiter,
      NodeId(1),
      ThreadId(2),
      Box::new(move || {
        let _ = tx1.send(());
      }),
    );

    lock.release();
    assert!(
      rx1.try_recv().is_ok(),
      "the oldest waiter should be granted first"
    );
    assert!(
      rx2.try_recv().is_err(),
      "the second-oldest waiter should still be waiting"
    );
    assert_eq!(
      lock.current_holder(),
      Some(&Holder {
        node_id: NodeId(1),
        thread_id: ThreadId(2),
        op_id: first_waiter
      })
    );
  }

  #[test]
  fn release_on_an_idle_lock_is_a_no_op() {
    let mut lock = NativeLock::new();
    lock.release();
    assert_eq!(lock.current_holder(), None);
  }

  #[test]
  fn handle_node_death_releases_the_current_holder_from_that_node() {
    let mut lock = NativeLock::new();
    let (tx, _rx) = mpsc::channel();
    let holder_op = DatumId::new();
    lock.request(
      holder_op,
      NodeId(1),
      ThreadId(0),
      Box::new(move || {
        let _ = tx.send(());
      }),
    );
    assert!(lock.current_holder().is_some());

    lock.handle_node_death(NodeId(1));
    assert_eq!(lock.current_holder(), None);
  }

  #[test]
  fn handle_node_death_grants_to_a_surviving_waiter() {
    let mut lock = NativeLock::new();
    let (tx_holder, _rx_holder) = mpsc::channel();
    let holder_op = DatumId::new();
    lock.request(
      holder_op,
      NodeId(1),
      ThreadId(0),
      Box::new(move || {
        let _ = tx_holder.send(());
      }),
    );

    let (tx_waiter, rx_waiter) = mpsc::channel();
    let waiter_op = DatumId::new(); // created after holder_op, sorts younger
    lock.request(
      waiter_op,
      NodeId(2),
      ThreadId(0),
      Box::new(move || {
        let _ = tx_waiter.send(());
      }),
    );
    assert!(rx_waiter.try_recv().is_err());

    lock.handle_node_death(NodeId(1));
    assert!(
      rx_waiter.try_recv().is_ok(),
      "the surviving waiter should be granted once the dead holder is released"
    );
    assert_eq!(
      lock.current_holder(),
      Some(&Holder {
        node_id: NodeId(2),
        thread_id: ThreadId(0),
        op_id: waiter_op
      })
    );
  }

  #[test]
  fn handle_node_death_prunes_that_nodes_own_queued_waiters() {
    let mut lock = NativeLock::new();
    let (tx_holder, _rx_holder) = mpsc::channel();
    let holder_op = DatumId::new();
    lock.request(
      holder_op,
      NodeId(1),
      ThreadId(0),
      Box::new(move || {
        let _ = tx_holder.send(());
      }),
    );

    // A waiter from the node that's about to die — should be pruned,
    // not granted, even though it would otherwise be next in line.
    let (tx_dead_waiter, rx_dead_waiter) = mpsc::channel();
    let dead_waiter_op = DatumId::new();
    lock.request(
      dead_waiter_op,
      NodeId(2),
      ThreadId(0),
      Box::new(move || {
        let _ = tx_dead_waiter.send(());
      }),
    );

    // A later, still-alive waiter.
    let (tx_alive_waiter, rx_alive_waiter) = mpsc::channel();
    let alive_waiter_op = DatumId::new();
    lock.request(
      alive_waiter_op,
      NodeId(3),
      ThreadId(0),
      Box::new(move || {
        let _ = tx_alive_waiter.send(());
      }),
    );

    lock.handle_node_death(NodeId(2));
    assert!(
      rx_dead_waiter.try_recv().is_err(),
      "a waiter from the dead node must never be granted"
    );
    assert!(
      rx_alive_waiter.try_recv().is_err(),
      "the current holder (node 1) is still alive, so nothing should be granted yet"
    );

    lock.handle_node_death(NodeId(1));
    assert!(
      rx_alive_waiter.try_recv().is_ok(),
      "once the holder dies too, the surviving waiter should be granted"
    );
  }
}
