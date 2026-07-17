use std::net::TcpListener;
use std::sync::Arc;

use anyhow::{Context, Result};

use seisin_core::store::InMemoryStore;
use seisin_node::server::serve;
use seisin_node::worker::WorkerHandle;

fn main() -> Result<()> {
  let addr = std::env::var("SEISIN_LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:7878".to_string());
  let listener = TcpListener::bind(&addr).with_context(|| format!("failed to bind {addr}"))?;
  println!("seisin-node listening on {addr}");

  let store = Arc::new(InMemoryStore::new());
  let worker = Arc::new(WorkerHandle::spawn(store));
  serve(listener, worker);
  Ok(())
}
