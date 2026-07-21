// #![deny(warnings)] temporarily removed — DTS Part 2 (revised) Tasks 5-6
// are a multi-task wire-up (IndexUpdateReplied/IndexUpdateReply::Local
// aren't consumed until Task 6's op-lifecycle rewrite lands) — restored
// once that's done, matching the same tolerated-transient-red precedent
// from 3b Part 1.

pub mod collation;
pub mod config;
pub mod gossip_client;
pub mod gossip_server;
pub mod gossip_state;
pub mod index_handler;
pub mod peer_link;
pub mod pool;
pub mod server;
pub mod worker;
