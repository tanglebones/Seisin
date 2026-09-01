# Leaderboard example: HTTP API + container test cluster

## Motivation

Seisin has no runnable example of a solution built on the framework, and
no way to stand up a live cluster to test against by hand. This adds
both, using the leaderboard datum class (`seisin-types::lb`) as the
example domain, since it's a fully-implemented framework feature
(`register_lb_class`, `Request::LbExecute`/`LbQuery`, tested end-to-end
over real sockets in `crates/seisin-types/tests/integration_lb_boards.rs`)
with no gaps to fill — this work is pure composition, not new framework
functionality.

This also establishes, for the first time, the pattern a real solution
is expected to follow: Seisin's own crates (`seisin-node`, `seisin-types`,
`seisin-client`, `seisin-protocol`, `seisin-ops`) stay pure libraries.
An application composes them into its own crate, choosing which roles
(storage, compute, gateway, or any combination) each of its own binaries
plays. Nothing about HTTP, axum, or this example's routes belongs in any
`seisin-*` crate.

## Goals

- A new crate, `crates/leaderboard-http`, providing:
  - A reusable HTTP API (axum) for posting scores and reading a
    leaderboard's top N, as a library.
  - A binary that composes this HTTP API with a Seisin compute or
    storage node into one running process, driven by the same
    `SEISIN_NODE_CONFIG`-file convention every other Seisin node binary
    already uses.
- A docker-compose-based (plus an Apple `container` CLI equivalent)
  local cluster: one storage node, two compute nodes (each also serving
  HTTP), and nginx in front doing sticky (`ip_hash`) load balancing
  across the two compute nodes — a system a human can `curl` against.
- An integration test exercising the real HTTP API against a real
  in-process node (no mocking of the wire protocol or the HTTP layer).

## Non-goals

- No changes to any `seisin-*` crate. This is composition on top of
  already-shipped framework functionality.
- No auth/HTTPS — HTTPS termination is explicitly a load-balancer
  concern per the brainstorm discussion, out of scope for a local test
  harness.
- No incremental-catch-up / rack-awareness / etc. — those are the
  already-tracked Storage Tier Part C remainder items in PROGRESS.md,
  unrelated to this work.
- lb boards are node-resident indexes, not yet routed through the
  storage tier (see `CLAUDE.md`'s "Load-bearing design decisions"). The
  `storage1` node in the test topology is therefore not actually
  exercised by leaderboard traffic — it's included for topology realism
  (showing a real deployment's compute/storage split), not because lb
  needs it.

## Crate layout: `crates/leaderboard-http`

```
crates/leaderboard-http/
  Cargo.toml
  src/
    lib.rs           -- LbClassDef for "global", axum Router builder, handlers
    bin/
      leaderboard_node.rs   -- composition root: node + (if compute) HTTP
  tests/
    integration_http.rs
```

Dependencies: `seisin-core`, `seisin-node`, `seisin-types`, `seisin-ops`,
`seisin-client`, `seisin-protocol` (all path deps, all libraries), plus
`axum`, `tokio` (features: `rt-multi-thread`, `macros`), `serde`,
`serde_json`, `anyhow`. This is the workspace's first async/tokio
dependency — a deliberate, scoped exception to the rest of the
codebase's sync/thread-based style (`WorkerPool`, `seisin-client`),
justified by needing a real HTTP server rather than reinventing one;
noted here per the guideline-deviation rule rather than silently
introduced.

### `lb` class definition

One class, `"global"`, `I64` score, `LbRule::Max`, `display_len: 32`
(room for a player name). Defined once in `lib.rs` as a plain function
(`fn global_class() -> LbClassDef`) so both the binary (registers it)
and the HTTP handlers (need it to encode/decode scores) share the exact
same definition.

### Player identity

Player names are arbitrary strings from the HTTP caller — there's no
separate player-registration step. `DatumId::from_name(&PLAYER_NS,
player_name.as_bytes())` derives a stable id per name, where
`PLAYER_NS` is a fixed constant (`DatumId::from_bytes([0u8; 16])`)
local to this crate. This is safe because lb boards are self-contained
primary data (per `lb_kind.rs`'s doc comment) — nothing else needs to
resolve a player id back to a name outside of what the board itself
stores as `display`.

### HTTP API

Both routes take `{leaderboard_id}` as a path segment and an optional
`?area=` query param (default `"default"`), which together derive the
board id via `lb_board_key("global", leaderboard_id, area)`.

**`POST /leaderboards/{leaderboard_id}/scores`**

Request body:
```json
{ "player": "alice", "score": 1200 }
```
Sends `Request::LbExecute { board_id, class: "global", op:
LbExecuteOp::Update { player_id, display: player.into_bytes(), rank_key:
encode_score(...), friend_ids: vec![], top: 10, window: 0 } }` to the
node at `seed_addr` (the handler's own loopback client port when
co-located; see below).

Response body (200):
```json
{ "ok": true, "data": { "rank": 0, "total": 3, "top": [
  { "player": "bob", "score": 1500 },
  { "player": "alice", "score": 1200 }
] } }
```
`rank` is `player_rank` from `LbResult` (0-based, best = 0); `top`
entries decode `display` as UTF-8 (lossy) and `rank_key` via
`decode_rank_key`.

**`GET /leaderboards/{leaderboard_id}/top?n=10&area=default`**

Sends `Request::LbQuery` with `top: n, bottom: 0, around_player: None,
window: 0, friend_ids: vec![]`.

Response body (200):
```json
{ "ok": true, "data": { "total": 3, "top": [ { "player": "bob", "score": 1500 }, ... ] } }
```

**Error envelope**: per the backend guideline on status codes, HTTP
status describes transport outcome only. A malformed request body or
an unroutable path is a transport-level failure axum itself rejects
before the handler runs (its default 400/404) — that's genuinely
transport, not business logic. Once a handler runs, every outcome
(including "node unreachable," "wire error") returns 200 with `{ "ok":
false, "code": "...", "message": "..." }` — there is no partial/"not
found" case for these two routes (posting a score always succeeds;
querying an empty board just returns an empty `top`).

### Composition root: `src/bin/leaderboard_node.rs`

1. Read `SEISIN_NODE_CONFIG`, `NodeConfig::load(...)`.
2. Look up `self_member` (by `self_node_id`) to get its `role` and
   parse the port off its `address` — done *before* the config is
   moved, since `NodeConfig` isn't `Clone`.
3. Build an `IndexKindRegistry`, `register_lb_class(&mut registry,
   global_class(), config.data_dir.clone().into())`.
4. If `role == NodeRole::Storage`: call `seisin_node::node::run(config,
   OpRegistry::new(), registry)` directly on the main thread (it blocks
   forever) — no HTTP.
5. If `role == NodeRole::Compute`:
   - `thread::spawn(move || seisin_node::node::run(config,
     OpRegistry::new(), registry))` — node runs in the background.
   - Read `SEISIN_HTTP_ADDR` (e.g. `0.0.0.0:8080`), build
     `leaderboard_http::router(format!("127.0.0.1:{client_port}"))`,
     and serve it on a `tokio` runtime on the main thread.
   - The loopback seed address means: if this compute node isn't the
     ring owner for a given board, `seisin_client::call`'s existing
     `Redirect`-following already sends the request on to the node that
     is — no extra routing logic needed in the HTTP layer.

`SEISIN_HTTP_ADDR` is an env var (mirroring the existing
`SEISIN_NODE_CONFIG` convention) rather than a new `NodeConfig` field,
since HTTP-serving is this example app's concern, not the framework's.

## Container test cluster

```
deploy/leaderboard/
  Dockerfile
  docker-compose.yml
  nginx.conf
  configs/
    storage1.ron
    compute1.ron
    compute2.ron
  run-apple-container.sh   -- up|down, drives the same image via `container build`/`container run`
```

**`Dockerfile`**: multi-stage — `cargo build --release -p
leaderboard-http --bin leaderboard_node` in a builder stage, then a
slim runtime image (`debian:bookworm-slim` or similar) copying just the
binary. One image, used by all three node services (role comes from
the mounted config file).

**`docker-compose.yml`** services:
- `storage1` — `NodeConfig` role `Storage`; internal store/gossip
  ports only, no host port published (nothing outside the compose
  network needs to reach it directly).
- `compute1`, `compute2` — role `Compute`; `SEISIN_HTTP_ADDR=0.0.0.0:8080`
  each; client/gossip/peer-link ports internal only, HTTP port not
  published directly to the host (nginx is the entrypoint).
- `nginx` — publishes `8080:80` to the host; config below.

Each node's RON config lists the full static membership (all three
nodes, by compose service name — e.g. `compute1:7000`), matching how
`NodeConfig`/`MemberConfig` already work (statically-seeded membership,
gossip only *maintains* liveness from there, per `config.rs`'s doc
comment).

**`nginx.conf`** (sketch):
```nginx
upstream leaderboard_compute {
  ip_hash;
  server compute1:8080;
  server compute2:8080;
}
server {
  listen 80;
  location / {
    proxy_pass http://leaderboard_compute;
  }
}
```
`ip_hash` is open-source nginx's built-in sticky mechanism (the `sticky`
directive is nginx-plus-only) — every client IP consistently lands on
one compute node, good enough to emulate "sticky routing to a node" for
local testing; cross-node correctness still gets exercised because
`seisin_client::call`'s redirect-following works regardless of which
compute node nginx happens to pick.

**`run-apple-container.sh`**: since `container compose` doesn't exist
natively on Apple's `container` CLI, this script does the compose file's
job by hand — `container build` once, then `container run -d --name
... -p ...` per service (storage1, compute1, compute2, nginx, the
latter using nginx's own official image with `nginx.conf` bind-mounted)
— with `up` and `down` subcommands.

## Testing

`crates/leaderboard-http/tests/integration_http.rs`: starts one real
compute-role node in-process (same pattern as
`integration_lb_boards.rs` — bind ephemeral `TcpListener`s, spawn
`serve`), builds the real `router()` pointed at that node's address,
serves it on a real `tokio` `TcpListener` on an ephemeral port, and
drives it with real HTTP requests (`reqwest` or a raw `hyper` client as
a dev-dependency) — post a few scores, fetch top N, assert ordering and
JSON shape. No mocking of either the wire protocol or the HTTP layer.

## Open questions

None outstanding — this is composition of already-built framework
pieces plus one clearly-scoped new example crate.
