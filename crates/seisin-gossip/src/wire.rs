//! Encode/decode for the pieces that travel over the gossip socket:
//! `MemberUpdate` facts and epoch-tagged `RingMutation` records. Reuses
//! `seisin_protocol`'s length-prefixed frame reader/writer as-is — that
//! framing primitive was never tied to the client `Request`/`Response`
//! types, just to any `Read`/`Write`.

use anyhow::{bail, Context, Result};

use seisin_core::authority::NodeId;

use crate::membership::{Incarnation, MemberStatus, MemberUpdate};
use crate::sequencer::RingMutation;

const STATUS_ALIVE: u8 = 0;
const STATUS_SUSPECT: u8 = 1;
const STATUS_DEAD: u8 = 2;

pub fn encode_member_update(update: &MemberUpdate) -> Vec<u8> {
  let mut buf = Vec::new();
  buf.extend_from_slice(&update.node_id.0.to_le_bytes());
  buf.extend_from_slice(&update.incarnation.0.to_le_bytes());
  buf.push(match update.status {
    MemberStatus::Alive => STATUS_ALIVE,
    MemberStatus::Suspect => STATUS_SUSPECT,
    MemberStatus::Dead => STATUS_DEAD,
  });
  buf.extend_from_slice(&update.thread_count.to_le_bytes());
  encode_string(&mut buf, &update.client_address);
  encode_string(&mut buf, &update.gossip_address);
  buf
}

pub fn decode_member_update(buf: &[u8]) -> Result<MemberUpdate> {
  if buf.len() < 21 {
    bail!("member update payload too short: {} bytes", buf.len());
  }
  let node_id = NodeId(u64::from_le_bytes(buf[0..8].try_into().unwrap()));
  let incarnation = Incarnation(u64::from_le_bytes(buf[8..16].try_into().unwrap()));
  let status = match buf[16] {
    STATUS_ALIVE => MemberStatus::Alive,
    STATUS_SUSPECT => MemberStatus::Suspect,
    STATUS_DEAD => MemberStatus::Dead,
    tag => bail!("invalid member status tag byte: {tag}"),
  };
  let thread_count = u32::from_le_bytes(buf[17..21].try_into().unwrap());
  let mut offset = 21;
  let client_address = decode_string(buf, &mut offset).context("decoding client_address")?;
  let gossip_address = decode_string(buf, &mut offset).context("decoding gossip_address")?;
  Ok(MemberUpdate {
    node_id,
    incarnation,
    status,
    client_address,
    gossip_address,
    thread_count,
  })
}

const MUTATION_JOIN: u8 = 0;
const MUTATION_LEAVE: u8 = 1;

pub fn encode_ring_mutation_record(epoch: u64, mutation: &RingMutation) -> Vec<u8> {
  let mut buf = Vec::new();
  buf.extend_from_slice(&epoch.to_le_bytes());
  match mutation {
    RingMutation::Join {
      node_id,
      thread_count,
    } => {
      buf.push(MUTATION_JOIN);
      buf.extend_from_slice(&node_id.0.to_le_bytes());
      buf.extend_from_slice(&thread_count.to_le_bytes());
    }
    RingMutation::Leave { node_id } => {
      buf.push(MUTATION_LEAVE);
      buf.extend_from_slice(&node_id.0.to_le_bytes());
    }
  }
  buf
}

pub fn decode_ring_mutation_record(buf: &[u8]) -> Result<(u64, RingMutation)> {
  if buf.len() < 17 {
    bail!("ring mutation record too short: {} bytes", buf.len());
  }
  let epoch = u64::from_le_bytes(buf[0..8].try_into().unwrap());
  let node_id = NodeId(u64::from_le_bytes(buf[9..17].try_into().unwrap()));
  let mutation = match buf[8] {
    MUTATION_JOIN => {
      if buf.len() != 21 {
        bail!(
          "join mutation record length mismatch: expected 21 bytes, got {}",
          buf.len()
        );
      }
      let thread_count = u32::from_le_bytes(buf[17..21].try_into().unwrap());
      RingMutation::Join {
        node_id,
        thread_count,
      }
    }
    MUTATION_LEAVE => {
      if buf.len() != 17 {
        bail!(
          "leave mutation record length mismatch: expected 17 bytes, got {}",
          buf.len()
        );
      }
      RingMutation::Leave { node_id }
    }
    tag => bail!("invalid ring mutation tag byte: {tag}"),
  };
  Ok((epoch, mutation))
}

fn encode_string(buf: &mut Vec<u8>, s: &str) {
  let bytes = s.as_bytes();
  buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
  buf.extend_from_slice(bytes);
}

fn decode_string(buf: &[u8], offset: &mut usize) -> Result<String> {
  if buf.len() < *offset + 4 {
    bail!("truncated string length prefix at offset {offset}");
  }
  let len = u32::from_le_bytes(buf[*offset..*offset + 4].try_into().unwrap()) as usize;
  *offset += 4;
  if buf.len() < *offset + len {
    bail!("truncated string body at offset {offset}: expected {len} bytes");
  }
  let s =
    String::from_utf8(buf[*offset..*offset + len].to_vec()).context("string was not valid utf8")?;
  *offset += len;
  Ok(s)
}

const MSG_PING: u8 = 0;
const MSG_PING_REQ: u8 = 1;
const MSG_ACK: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GossipMessage {
  Ping {
    updates: Vec<MemberUpdate>,
    mutations: Vec<(u64, RingMutation)>,
  },
  PingReq {
    target: NodeId,
    updates: Vec<MemberUpdate>,
    mutations: Vec<(u64, RingMutation)>,
  },
  Ack {
    updates: Vec<MemberUpdate>,
    mutations: Vec<(u64, RingMutation)>,
  },
}

fn encode_list<T>(buf: &mut Vec<u8>, items: &[T], encode_one: impl Fn(&T) -> Vec<u8>) {
  buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
  for item in items {
    let encoded = encode_one(item);
    buf.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
    buf.extend_from_slice(&encoded);
  }
}

fn decode_list<T>(
  buf: &[u8],
  offset: &mut usize,
  decode_one: impl Fn(&[u8]) -> Result<T>,
) -> Result<Vec<T>> {
  if buf.len() < *offset + 4 {
    bail!("truncated list count at offset {offset}");
  }
  let count = u32::from_le_bytes(buf[*offset..*offset + 4].try_into().unwrap()) as usize;
  *offset += 4;
  let mut items = Vec::with_capacity(count);
  for _ in 0..count {
    if buf.len() < *offset + 4 {
      bail!("truncated list item length at offset {offset}");
    }
    let len = u32::from_le_bytes(buf[*offset..*offset + 4].try_into().unwrap()) as usize;
    *offset += 4;
    if buf.len() < *offset + len {
      bail!("truncated list item body at offset {offset}: expected {len} bytes");
    }
    items.push(decode_one(&buf[*offset..*offset + len])?);
    *offset += len;
  }
  Ok(items)
}

pub fn encode_gossip_message(msg: &GossipMessage) -> Vec<u8> {
  let mut buf = Vec::new();
  match msg {
    GossipMessage::Ping { updates, mutations } => {
      buf.push(MSG_PING);
      encode_list(&mut buf, updates, encode_member_update);
      encode_list(&mut buf, mutations, |(epoch, m)| {
        encode_ring_mutation_record(*epoch, m)
      });
    }
    GossipMessage::PingReq {
      target,
      updates,
      mutations,
    } => {
      buf.push(MSG_PING_REQ);
      buf.extend_from_slice(&target.0.to_le_bytes());
      encode_list(&mut buf, updates, encode_member_update);
      encode_list(&mut buf, mutations, |(epoch, m)| {
        encode_ring_mutation_record(*epoch, m)
      });
    }
    GossipMessage::Ack { updates, mutations } => {
      buf.push(MSG_ACK);
      encode_list(&mut buf, updates, encode_member_update);
      encode_list(&mut buf, mutations, |(epoch, m)| {
        encode_ring_mutation_record(*epoch, m)
      });
    }
  }
  buf
}

pub fn decode_gossip_message(buf: &[u8]) -> Result<GossipMessage> {
  if buf.is_empty() {
    bail!("empty gossip message payload");
  }
  match buf[0] {
    MSG_PING => {
      let mut offset = 1;
      let updates = decode_list(buf, &mut offset, decode_member_update)?;
      let mutations = decode_list(buf, &mut offset, decode_ring_mutation_record)?;
      Ok(GossipMessage::Ping { updates, mutations })
    }
    MSG_PING_REQ => {
      if buf.len() < 9 {
        bail!(
          "ping_req payload too short for a target node_id: {} bytes",
          buf.len()
        );
      }
      let target = NodeId(u64::from_le_bytes(buf[1..9].try_into().unwrap()));
      let mut offset = 9;
      let updates = decode_list(buf, &mut offset, decode_member_update)?;
      let mutations = decode_list(buf, &mut offset, decode_ring_mutation_record)?;
      Ok(GossipMessage::PingReq {
        target,
        updates,
        mutations,
      })
    }
    MSG_ACK => {
      let mut offset = 1;
      let updates = decode_list(buf, &mut offset, decode_member_update)?;
      let mutations = decode_list(buf, &mut offset, decode_ring_mutation_record)?;
      Ok(GossipMessage::Ack { updates, mutations })
    }
    tag => bail!("unknown gossip message tag: {tag}"),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn sample_update() -> MemberUpdate {
    MemberUpdate {
      node_id: NodeId(7),
      incarnation: Incarnation(3),
      status: MemberStatus::Suspect,
      client_address: "127.0.0.1:7878".to_string(),
      gossip_address: "127.0.0.1:8878".to_string(),
      thread_count: 4,
    }
  }

  #[test]
  fn round_trips_a_member_update() {
    let update = sample_update();
    let decoded = decode_member_update(&encode_member_update(&update)).unwrap();
    assert_eq!(decoded, update);
  }

  #[test]
  fn round_trips_every_member_status() {
    for status in [
      MemberStatus::Alive,
      MemberStatus::Suspect,
      MemberStatus::Dead,
    ] {
      let mut update = sample_update();
      update.status = status;
      let decoded = decode_member_update(&encode_member_update(&update)).unwrap();
      assert_eq!(decoded.status, status);
    }
  }

  #[test]
  fn rejects_a_member_update_with_invalid_status_byte() {
    let mut buf = encode_member_update(&sample_update());
    buf[16] = 99; // status byte, right after the 8-byte node_id + 8-byte incarnation
    assert!(decode_member_update(&buf).is_err());
  }

  #[test]
  fn round_trips_a_join_mutation_record() {
    let mutation = RingMutation::Join {
      node_id: NodeId(9),
      thread_count: 3,
    };
    let (epoch, decoded) =
      decode_ring_mutation_record(&encode_ring_mutation_record(42, &mutation)).unwrap();
    assert_eq!(epoch, 42);
    assert_eq!(decoded, mutation);
  }

  #[test]
  fn round_trips_a_leave_mutation_record() {
    let mutation = RingMutation::Leave { node_id: NodeId(9) };
    let (epoch, decoded) =
      decode_ring_mutation_record(&encode_ring_mutation_record(7, &mutation)).unwrap();
    assert_eq!(epoch, 7);
    assert_eq!(decoded, mutation);
  }

  #[test]
  fn rejects_a_mutation_record_with_invalid_tag_byte() {
    let mut buf = encode_ring_mutation_record(1, &RingMutation::Leave { node_id: NodeId(1) });
    buf[8] = 99; // tag byte, right after the 8-byte epoch
    assert!(decode_ring_mutation_record(&buf).is_err());
  }

  fn sample_mutations() -> Vec<(u64, RingMutation)> {
    vec![
      (
        1,
        RingMutation::Join {
          node_id: NodeId(1),
          thread_count: 2,
        },
      ),
      (2, RingMutation::Leave { node_id: NodeId(3) }),
    ]
  }

  #[test]
  fn round_trips_ping_with_no_piggybacked_data() {
    let msg = GossipMessage::Ping {
      updates: vec![],
      mutations: vec![],
    };
    assert_eq!(
      decode_gossip_message(&encode_gossip_message(&msg)).unwrap(),
      msg
    );
  }

  #[test]
  fn round_trips_ping_with_piggybacked_updates_and_mutations() {
    let msg = GossipMessage::Ping {
      updates: vec![sample_update()],
      mutations: sample_mutations(),
    };
    assert_eq!(
      decode_gossip_message(&encode_gossip_message(&msg)).unwrap(),
      msg
    );
  }

  #[test]
  fn round_trips_ping_req() {
    let msg = GossipMessage::PingReq {
      target: NodeId(5),
      updates: vec![sample_update(), sample_update()],
      mutations: sample_mutations(),
    };
    assert_eq!(
      decode_gossip_message(&encode_gossip_message(&msg)).unwrap(),
      msg
    );
  }

  #[test]
  fn round_trips_ack() {
    let msg = GossipMessage::Ack {
      updates: vec![],
      mutations: sample_mutations(),
    };
    assert_eq!(
      decode_gossip_message(&encode_gossip_message(&msg)).unwrap(),
      msg
    );
  }

  #[test]
  fn rejects_unknown_message_tag() {
    let mut buf = encode_gossip_message(&GossipMessage::Ack {
      updates: vec![],
      mutations: vec![],
    });
    buf[0] = 99;
    assert!(decode_gossip_message(&buf).is_err());
  }
}
