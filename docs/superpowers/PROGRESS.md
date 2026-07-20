# Seisin — Progress Tracker

Rolling status of the sub-project sequence from the design doc
(`specs/2026-07-17-distributed-datum-ownership-design.md`). Update this
file whenever a sub-project starts, finishes, or the plan changes —
commit and push immediately, since work sessions may end abruptly.

## Done

- **Sub-project 1 — Datum core & single-node protocol.** `seisin-core`,
  `seisin-protocol`, `seisin-node`. Single-node datum CRUD over the
  custom wire protocol, write-through cache, SK-as-regular-datum.
- **Sub-project 2a — Compute ring & redirect routing.** `seisin-ring`,
  `seisin-client`. Jump-consistent-hash ring, client-side `Redirect`,
  `WorkerPool`, static-config membership.
- **Sub-project 2b-i — Ring mutations & cache eviction.**
  `Ring::apply_join`/`apply_leave` (swap-with-last), `Cache::evict_non_native`.
- **Sub-project 2b-ii — SWIM membership & epoch sequencer.**
  `seisin-gossip`: `MemberTable` (SWIM merge rule), `is_sequencer`,
  `MutationLog` (epoch-ordered mutation buffering).
- **Sub-project 2b-iii-a — Gossip wire protocol.** `MemberUpdate`/
  `RingMutation` codecs, `GossipMessage` (Ping/PingReq/Ack).
- **Sub-project 2b-iii-b — Failure detector.** `ClockSource`/`Tick`
  (fake-clock testable), `FailureDetector` direct→indirect→suspect→dead
  state machine.
- **Sub-project 2b-iii-c — Gossip node wiring.** Real background probing
  loop, gossip TCP listener, `Ring` behind `RwLock`, cache-eviction
  messaging reachable cross-thread, live multi-node integration test
  proving a silently-dead node gets removed from the ring. Indirect
  probing and runtime join of brand-new nodes are explicitly deferred
  (see the plan's "deliberately out of scope" note).
- **Sub-project 3a — Op registry & single-node collation.** New crate
  `seisin-ops` (`OpContext`, `OpRegistry` with panic-safe `invoke` via
  `catch_unwind`). Wire protocol gained `Request::Op` /
  `Response::OpResult`/`OpError`. `WorkerHandle`/`WorkerPool` gained
  `evict`/`evict_single`, `run_op`. `server.rs`'s `handle_op_request`
  resolves every datum_id's native owner, rejects cross-node op requests
  (that's 3b), picks the local thread natively owning the most datums,
  evicts the rest onto it, runs the solution-defined op, then evicts
  anything left foreign afterward (simplified anti-degeneration, no
  peek-ahead). Proven end-to-end by
  `integration_op_collation.rs`: an op moving content between two datums
  natively owned by different local threads on a single 4-thread node.

- **Sub-project 3b, Part 1 — Wire unification & same-node wound-wait.**
  `Request` collapsed to a single `Op { op_id, op_name, datum_ids,
  payload }` variant — `Get`/`Put`/`Delete` retired as wire opcodes
  (they're just trivially-registered ops now, no different in kind from
  any domain op). Every op carries a client-generated `op_id` (UUIDv7,
  now `Ord`) used for wound-wait priority. New `collation::NativeLock`:
  each datum's native-home thread is the sole, permanent lock manager
  for it (current holder + an op_id-ordered wait queue), never
  delegating to whoever currently holds it — recall on an older
  request, queue on a younger one, oldest-first grants on release.
  `worker.rs` reworked so every thread tracks its own in-flight op
  records (`still_needed`/`acquired`/original `datum_ids` order) and
  drives collation via non-blocking messages (`Acquire`/
  `AcquireGranted`/`Recall`/`Release`) to itself and its local peers —
  no thread ever blocks waiting on another. `server.rs`'s dispatch
  unifies single-datum and multi-datum routing: all-native runs
  locally, all-one-other-node redirects, genuinely cross-node rejects
  (that's Part 2). Proven end-to-end by `integration_wound_wait.rs`:
  the classic two-op cycle (op1 needs `a,b`; op2 needs `b,a`, opposite
  acquisition order) resolves without deadlock over real TCP on a
  single 4-thread node.

  Found and fixed two real concurrency bugs while stress-testing this
  (both were flaky ~30% of the time before the fixes, not caught by a
  single test run): (1) an op's acquired-datums list was ordered by
  grant-arrival time instead of the caller's original order, causing
  op functions to read/write the wrong positional ids when one grant
  was a fast self-send and another a slower cross-thread round trip;
  (2) releasing a datum only updated lock bookkeeping, never evicted
  any cache entry, so a thread that had cached a value from an earlier
  direct use could keep serving that stale value after granting the
  datum away and getting it back, ignoring whatever the interim holder
  wrote or deleted via storage.

- **Sub-project 3b, Part 2a — Peer-link multiplexing & real cross-node
  acquisition.** New `peer_link.rs`: `PeerLink` (one persistent,
  multiplexed connection per node pair — envelope-framed
  `{correlation_id, kind, target_thread, body}` wrapping the existing
  `Request`/`Response` codec unchanged) and `PeerLinkRegistry` (eager
  startup-time connections, lower `NodeId` always dials higher, a
  node-id handshake preamble on connect, an unreachable peer skipped
  rather than fatal). Wire protocol gained `Request::Acquire`/`Recall`/
  `Release` and `Response::Granted`/`Released`, all node-to-node only.
  `worker.rs`'s `AcquireReply`/`RecallReply` let a grant or recall-ack
  go to either a local `WorkerMessage` send or a peer-link response,
  transparently. `server.rs` no longer rejects an op whose datums span
  more than one node — it dispatches locally and lets the destination
  thread's own `Acquire`/`Recall` machinery pull the remote ones in.
  Proven end-to-end by `integration_cross_node_collation.rs` (a
  multi-datum op collating across two real nodes) and
  `integration_cross_node_wound_wait.rs` (the classic two-op cycle,
  contended across nodes rather than just threads, resolving without
  deadlock over real peer-link traffic).

  Found and fixed a real deadlock while first running the cross-node
  wound-wait test (hung outright, not merely flaky): the release path
  only ever sent `Release` over a local channel, never checking
  whether a datum's native home was actually on a different node — a
  cross-node release silently vanished, leaving the remote wait-queue
  stuck forever. Fixed by adding `Request::Release` to the wire
  protocol, so a normal (non-recalled) completion can tell a remote
  native home it's done with a datum, the same way a recall's ack
  already could.

  Known gap, deliberately not fixed here (Part 2b's scope): peer-links
  are only established from the *static* startup member list — a node
  admitted later via gossip never gets a peer-link connection, and a
  dead peer's in-flight calls fail via disconnect but nothing
  proactively reclaims a lock it was holding or retries against a
  since-moved ring slot.

As of this entry: 8 crates, 136 tests passing, `cargo fmt --check` and
`cargo clippy --all-targets -- -D warnings` clean. All committed and
pushed to `main`.

## In progress

- **Sub-project 3b, Part 2b — Crash detection & lock release.** Not
  yet planned. Needs: proactive lock release when gossip confirms a
  node dead (extending the same ring-mutation hook that already
  triggers cache eviction), a reactive backstop when a recall's
  peer-link call errors out, bounded acquire retry against a moved
  ring slot, and failing an op back to its client with `OpError` on
  exhausted retries rather than hanging — all already designed in the
  spec's "Crash Detection & Lock Release" section.

## Not started — from the original sub-project sequence
- **Sub-project 4 — Storage tier.** Storage-role servers, capacity-
  weighted consistent hashing, storage's own gossip pool, write-through-
  before-ack wiring, fail-stop halt-on-shard-loss.
- **Sub-project 5 — Deployment & cluster tests.** Containerized
  multi-node harness, plus remaining cross-node correctness tests from
  the design doc's Testing Strategy.

## Not started — from the 2026-07-20 design additions

These are new design surface added to the doc but not yet broken into
sub-project plans:

- **Datum type system.** Typed, homogeneous datum types (Rust primitives/
  arrays/dicts with primitive keys), secondary indexes declared as part
  of a type, relational constraints (enforcement mechanism undecided —
  see the design doc's Open Questions). Four index kinds per type: pk
  (required, the datum_id itself), sk (secondary key, already mechanically
  specified), rk (stochastically ranked, mechanics TBD), tk (temporal,
  mechanics TBD) — rk/tk explicitly deferred for later detailing.
- **Framework/codegen shape.** Seisin's actual deliverable is base
  libraries a solution uses to define datum types + operations in code,
  compiling into a server executable and a paired client library. None
  of the current sub-projects have been re-examined against this framing
  yet — worth revisiting whether Sub-projects 1–2's APIs need adjustment
  once this is designed, rather than assuming they're already shaped
  right.
- **Deployment management system.** Central, only active during a
  rollout; enforces n/n-1 compatibility, requires uniform starting
  version, rolls out storage → compute → clients; datum type evolution
  is add-freely / deprecate-then-remove / alias-only (no renames). Not
  designed at all yet — see the design doc's Open Questions for what's
  still undecided even at the rules level.

## Sequencing decision (2026-07-23)

Proceeding with **Sub-project 3 (Collation & multi-datum ops)** next,
per the original sequence, rather than designing the datum type system
first. Rationale: collation operates at the `DatumId`/`AuthorityIdx`
level (which thread runs an op touching multiple datums), not on typed
content — the existing `Cache`/`Request`/`Response` model already treats
content as opaque bytes, and nothing about wound-wait/foreign-pull/
anti-degeneration needs to know about datum types, index kinds, or
relational constraints. The type system and deployment system are both
still being actively sketched (index kinds added 2026-07-23 with rk/tk
mechanics explicitly deferred) and aren't yet concrete enough to plan
against — better to let them keep accumulating design notes and revisit
once they're ready for their own brainstorm → plan cycle.
