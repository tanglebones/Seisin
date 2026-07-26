//! Accepts gossip TCP connections: decodes an incoming `GossipMessage`,
//! merges its piggybacked updates/mutations into `GossipState`, applies
//! any now-ready ring mutations, and replies with this node's own
//! current piggyback as an `Ack`.

use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::gossip_state::{apply_ready_mutations, ClusterState, GossipState};
use crate::pool::WorkerPool;
use seisin_core::authority::NodeId;
use seisin_gossip::wire::{decode_gossip_message, encode_gossip_message, GossipMessage};
use seisin_protocol::{read_frame, write_frame};

pub fn serve_gossip(
  listener: TcpListener,
  self_node_id: NodeId,
  gossip: Arc<GossipState>,
  cluster: Arc<ClusterState>,
  pool: Arc<WorkerPool>,
) {
  for stream in listener.incoming() {
    let stream = match stream {
      Ok(s) => s,
      Err(_) => continue,
    };
    let gossip = Arc::clone(&gossip);
    let cluster = Arc::clone(&cluster);
    let pool = Arc::clone(&pool);
    thread::spawn(move || handle_gossip_connection(stream, self_node_id, gossip, cluster, pool));
  }
}

fn handle_gossip_connection(
  mut stream: TcpStream,
  self_node_id: NodeId,
  gossip: Arc<GossipState>,
  cluster: Arc<ClusterState>,
  pool: Arc<WorkerPool>,
) {
  let payload = match read_frame(&mut stream) {
    Ok(p) => p,
    Err(_) => return,
  };
  let message = match decode_gossip_message(&payload) {
    Ok(m) => m,
    Err(_) => return,
  };
  let (updates, mutations) = match message {
    GossipMessage::Ping { updates, mutations } => (updates, mutations),
    GossipMessage::PingReq {
      updates, mutations, ..
    } => (updates, mutations),
    GossipMessage::Ack { updates, mutations } => (updates, mutations),
  };
  gossip.merge_incoming(updates, mutations);
  apply_ready_mutations(&gossip, &cluster, self_node_id, &pool);

  let (reply_updates, reply_mutations) = gossip.piggyback();
  let ack = GossipMessage::Ack {
    updates: reply_updates,
    mutations: reply_mutations,
  };
  let _ = write_frame(&mut stream, &encode_gossip_message(&ack));
}

/// The storage-role node's gossip participation: merge incoming
/// piggybacks and ack with our own — no rings, no pool, no probing
/// loop (being probed and acking is sufficient for SWIM liveness;
/// compute nodes do the probing).
pub fn serve_gossip_storage(listener: TcpListener, gossip: Arc<GossipState>) {
  for stream in listener.incoming() {
    let stream = match stream {
      Ok(s) => s,
      Err(_) => continue,
    };
    let gossip = Arc::clone(&gossip);
    thread::spawn(move || {
      let mut stream = stream;
      let payload = match read_frame(&mut stream) {
        Ok(p) => p,
        Err(_) => return,
      };
      let message = match decode_gossip_message(&payload) {
        Ok(m) => m,
        Err(_) => return,
      };
      let (updates, mutations) = match message {
        GossipMessage::Ping { updates, mutations } => (updates, mutations),
        GossipMessage::PingReq {
          updates, mutations, ..
        } => (updates, mutations),
        GossipMessage::Ack { updates, mutations } => (updates, mutations),
      };
      gossip.merge_incoming(updates, mutations);
      let (reply_updates, reply_mutations) = gossip.piggyback();
      let ack = GossipMessage::Ack {
        updates: reply_updates,
        mutations: reply_mutations,
      };
      let _ = write_frame(&mut stream, &encode_gossip_message(&ack));
    });
  }
}
