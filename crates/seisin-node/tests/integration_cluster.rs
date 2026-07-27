//! Real-process, real-socket cluster tests (Sub-project 5): a
//! `ClusterHarness` generates a RON config per node, spawns real
//! `cluster_test_node` processes over localhost (storage-first), drives
//! them via `seisin-client` and the `seisin_migrate` library, and kills
//! them (SIGKILL) for crash scenarios — reaping every child on drop.
//!
//! Timeouts are turned down via config so crash detection converges in
//! ~1s. Killing a process is the crash case; the node has no
//! graceful-leave handler, so a clean exit is indistinguishable from a
//! crash to the ring (both vanish and are detected dead).

use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use seisin_core::datum::DatumId;
use seisin_protocol::{Request, Response};

const PROBE_INTERVAL_MS: u64 = 20;
const PROBE_TIMEOUT_MS: u64 = 20;
const SUSPICION_TIMEOUT_MS: u64 = 40;
// Generous vs the probe cadence: storage self-halt is not what these
// scenarios exercise, and a tight threshold would spuriously fire before
// a real-process gossip probe reaches the storage node.
const SELF_HALT_MS: u64 = 2000;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
  Compute,
  Storage,
}

struct NodeSpec {
  id: u64,
  role: Role,
  thread_count: u32,
  weight: u32,
}

impl NodeSpec {
  fn compute(id: u64, thread_count: u32) -> Self {
    Self {
      id,
      role: Role::Compute,
      thread_count,
      weight: 0,
    }
  }
  fn storage(id: u64, weight: u32) -> Self {
    Self {
      id,
      role: Role::Storage,
      thread_count: 1,
      weight,
    }
  }
}

/// A running node's addresses.
struct NodeAddrs {
  id: u64,
  role: Role,
  client: String,
  gossip: String,
  peer_link: String,
  store: String,
}

struct ClusterHarness {
  _dir: tempfile::TempDir,
  nodes: Vec<NodeAddrs>,
  children: Vec<(u64, Child)>,
}

/// Binds an ephemeral localhost port, records it, and drops the listener
/// (the standard reserve-a-port pattern used across the test suite).
fn free_port() -> u16 {
  TcpListener::bind("127.0.0.1:0")
    .unwrap()
    .local_addr()
    .unwrap()
    .port()
}

fn addr() -> String {
  format!("127.0.0.1:{}", free_port())
}

impl ClusterHarness {
  fn start(specs: &[NodeSpec]) -> Self {
    let dir = tempfile::tempdir().unwrap();
    let nodes: Vec<NodeAddrs> = specs
      .iter()
      .map(|s| NodeAddrs {
        id: s.id,
        role: s.role,
        client: addr(),
        gossip: addr(),
        peer_link: addr(),
        store: addr(),
      })
      .collect();

    // One shared members list (RON), reused in every node's config.
    let members: String = specs
      .iter()
      .zip(&nodes)
      .map(|(spec, n)| match spec.role {
        Role::Compute => format!(
          "(node_id: {}, address: \"{}\", gossip_address: \"{}\", peer_link_address: \"{}\", thread_count: {}),",
          n.id, n.client, n.gossip, n.peer_link, spec.thread_count
        ),
        Role::Storage => format!(
          "(node_id: {}, address: \"{}\", gossip_address: \"{}\", peer_link_address: \"{}\", thread_count: 1, role: Storage, store_address: Some(\"{}\"), capacity_weight: Some({})),",
          n.id, n.client, n.gossip, n.peer_link, n.store, spec.weight
        ),
      })
      .collect();

    let mut children = Vec::new();
    // Storage nodes first (the deploy order), then compute.
    let order = specs
      .iter()
      .filter(|s| s.role == Role::Storage)
      .chain(specs.iter().filter(|s| s.role == Role::Compute));
    for spec in order {
      let data_dir = dir.path().join(format!("node{}", spec.id));
      let config = format!(
        "(\n  self_node_id: {},\n  members: [{}],\n  data_dir: {:?},\n  probe_interval_millis: Some({}),\n  probe_timeout_millis: Some({}),\n  suspicion_timeout_millis: Some({}),\n  self_halt_threshold_millis: Some({}),\n)",
        spec.id,
        members,
        data_dir.to_str().unwrap(),
        PROBE_INTERVAL_MS,
        PROBE_TIMEOUT_MS,
        SUSPICION_TIMEOUT_MS,
        SELF_HALT_MS,
      );
      let config_path = dir.path().join(format!("node{}.ron", spec.id));
      std::fs::write(&config_path, config).unwrap();
      let child = Command::new(env!("CARGO_BIN_EXE_cluster_test_node"))
        .env("SEISIN_NODE_CONFIG", &config_path)
        .spawn()
        .expect("failed to spawn cluster_test_node");
      children.push((spec.id, child));
    }

    let harness = ClusterHarness {
      _dir: dir,
      nodes,
      children,
    };
    harness.await_ready();
    harness
  }

  /// Polls each node's primary listener (client for compute, store for
  /// storage) until it accepts, so a scenario never races the spawn.
  fn await_ready(&self) {
    let deadline = Instant::now() + Duration::from_secs(10);
    for n in &self.nodes {
      let target = match n.role {
        Role::Compute => &n.client,
        Role::Storage => &n.store,
      };
      loop {
        if TcpStream::connect(target).is_ok() {
          break;
        }
        assert!(
          Instant::now() < deadline,
          "node {} never came up on {target}",
          n.id
        );
        std::thread::sleep(Duration::from_millis(20));
      }
    }
  }

  fn node(&self, id: u64) -> &NodeAddrs {
    self.nodes.iter().find(|n| n.id == id).unwrap()
  }

  fn compute_ids(&self) -> Vec<u64> {
    self
      .nodes
      .iter()
      .filter(|n| n.role == Role::Compute)
      .map(|n| n.id)
      .collect()
  }

  fn compute_addr(&self, id: u64) -> String {
    self.node(id).client.clone()
  }

  #[allow(dead_code)] // used by later scenarios (Tasks 3-4)
  fn store_addr(&self, id: u64) -> String {
    self.node(id).store.clone()
  }

  /// A client op against compute node `id`, returning its `Response`.
  fn op(&self, id: u64, name: &str, datum_ids: Vec<DatumId>, payload: &[u8]) -> Response {
    seisin_client::call(
      &self.compute_addr(id),
      Request::Op {
        op_id: DatumId::new(),
        op_name: name.to_string(),
        datum_ids,
        payload: payload.to_vec(),
      },
    )
    .unwrap()
  }

  #[allow(dead_code)] // used by the crash scenarios (Task 3)
  fn kill(&mut self, id: u64) {
    if let Some((_, child)) = self.children.iter_mut().find(|(cid, _)| *cid == id) {
      let _ = child.kill();
      let _ = child.wait();
    }
  }
}

impl Drop for ClusterHarness {
  fn drop(&mut self) {
    for (_, child) in &mut self.children {
      let _ = child.kill();
      let _ = child.wait();
    }
  }
}

/// Convenience: the OpResult payload, panicking on any error response.
fn ok(resp: Response) -> Vec<u8> {
  match resp {
    Response::OpResult { payload } => payload,
    other => panic!("expected OpResult, got {other:?}"),
  }
}

#[test]
fn a_client_op_is_served_across_two_compute_nodes() {
  // Two compute nodes over real sockets; a put on one node and a get on
  // the *other* both resolve to the datum's owner via redirect.
  let h = ClusterHarness::start(&[
    NodeSpec::compute(1, 2),
    NodeSpec::compute(2, 2),
    NodeSpec::storage(10, 1),
  ]);
  // Write and read many ids through whichever node the client happens to
  // hit — every one must round-trip regardless of which node owns it.
  for i in 0..20u32 {
    let id = DatumId::new();
    let via_a = 1;
    let via_b = 2;
    assert!(ok(h.op(via_a, "put1", vec![id], format!("v{i}").as_bytes())).is_empty());
    // Read back through the other compute node — the redirect (or local
    // service) must return the same value over real sockets.
    assert_eq!(
      ok(h.op(via_b, "get1", vec![id], &[])),
      format!("v{i}").into_bytes()
    );
  }
  assert_eq!(h.compute_ids().len(), 2);
}
