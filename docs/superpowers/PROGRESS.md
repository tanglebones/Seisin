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

As of this entry: 7 crates, 129 tests passing, `cargo fmt --check` and
`cargo clippy --all-targets -- -D warnings` clean. All committed and
pushed to `main`.

## Not started — from the original sub-project sequence

- **Sub-project 3 — Collation & multi-datum ops.** Op-to-thread
  assignment, foreign-pull collation, wound-wait livelock avoidance, the
  anti-degeneration return-home rule.
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
