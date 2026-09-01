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

- **Sub-project 3b, Part 2b — Crash detection & lock release.** Closes
  out Sub-project 3b entirely — the whole design doc
  (`specs/2026-07-20-cross-node-collation-and-wound-wait-design.md`) is
  now implemented. Three mechanisms, all reusing Part 1/2a's existing
  infrastructure: (1) `NativeLock::handle_node_death` — proactive
  release, wired into `gossip_state.rs::apply_ready_mutations`'s
  existing `RingMutation::Leave` handling via a new
  `WorkerPool::release_locks_held_by`/`WorkerMessage::ReleaseLocksHeldBy`
  broadcast; (2) a reactive backstop — a cross-node `Recall` whose
  callback fires with anything other than an explicit ack (a failed
  call, or no peer-link connection at all) is now treated as an
  immediate release rather than waiting on an ack that may never come;
  (3) bounded acquire retry — `send_acquire` gained a `retries_left`
  parameter (`MAX_ACQUIRE_RETRIES = 3`), re-resolving `ring.native()`
  fresh on each retry so it naturally picks up wherever gossip has
  since moved the slot, and `fail_op` abandons the whole op with
  `OpError` on exhaustion, releasing everything it had already
  acquired via the newly-factored-out `release_datums`. Proven
  end-to-end by `integration_proactive_lock_release.rs` (a lock held by
  a node that goes silent releases once gossip confirms it dead) and
  `integration_crash_during_collation.rs` (a hand-scripted raw-socket
  peer that gets granted a datum, then drops the connection exactly
  when a competing older op's recall arrives; plus bounded-retry-then-
  fail against a peer that was never reachable at all).

  Found and fixed two real bugs while implementing and stress-testing
  this (neither caught by a single passing run): (1)
  `PeerLinkRegistry::get` panicked outright when no link to a peer had
  ever been established, pre-empting the bounded-retry mechanism
  before it could even run for the "never connected" case — fixed by
  making `get` return `Option<Arc<PeerLink>>`, with all three call
  sites (`Recall` dispatch, `send_acquire`, `release_datums`) treating
  a missing link the same way they already treat a call that failed
  after connecting; (2) a genuine hang, found only by running the new
  crash tests 20+ times in a loop: `fail_op` could remove an op's
  record while an *earlier* `Acquire` for a different datum in that
  same op was still in flight (e.g. a same-node grant needing a slower
  cross-thread round trip, racing a remote `Acquire` that exhausted its
  retries first) — when that late grant finally arrived, it was
  silently dropped, permanently orphaning the datum's lock with
  nothing left to ever release it. Fixed by having the `AcquireGranted`
  handler release the datum immediately whenever its op's record is
  already gone.

  Known limitations, carried forward unchanged from Part 2a (still not
  this plan's scope): peer-links still only connect from the *static*
  startup member list — a node admitted later via gossip has no
  peer-link connection to it at all.

- **Datum Type System, Part 1 — Schema declaration & field encoding.**
  New crate `seisin-types` (`field.rs`, `encoding.rs`, `schema.rs`), per
  `specs/2026-07-21-datum-type-system-design.md`. `FieldType`/
  `PrimitiveFieldType`/`FieldValue` describe and hold a datum type's
  field shapes; `value_matches_type` checks a value against a declared
  type recursively (including `Dict` key restriction to primitives).
  `encode_field_value`/`decode_field_value` are schema-driven — no
  per-value type tags on the wire, since the declared `FieldType` at
  each position (recursively, into `Array`/`Dict`) already tells the
  decoder what to expect. `DatumTypeDef` (builder API: `.field(name,
  ty)`, mirroring `OpRegistry`'s registration style rather than a
  proc-macro/codegen pipeline) plus whole-datum `encode_datum`/
  `decode_datum` validate field count and per-value type match before
  encoding, and reject trailing undecoded bytes. pk needed no new code —
  it's the existing `DatumId`. Parts 2 (sk + uniqueness), 3 (rk), 4
  (tk), 5 (relational constraints) are separate, not-yet-started plans.

- **Datum Type System, Part 2 — sk index & uniqueness constraint.**
  `DatumId::from_name` (new, `seisin-core`): deterministic UUIDv5-based
  id derivation, since sk keys must resolve to the same datum_id every
  time the same `(type, field, value)` is written, unlike `new()`'s
  time-based randomness. `seisin-types::sk_index::sk_key` derives that
  id (primitive field values only — `Array`/`Dict` rejected, no
  canonical byte representation to key on).
  `insert_sk_entry`/`remove_sk_entry` maintain an sk datum's entry list
  via the existing `seisin-core::sk` encode/decode; `insert_sk_entry`
  also performs the best-effort uniqueness check (a second distinct
  pk_id in the list), returning a `UniquenessViolation` rather than
  rejecting outright itself. `IndexDef::Sk { field, unique:
  Option<ConflictOp> }` + `DatumTypeDef.indexes`/`.index(...)` extend
  Part 1's schema. `write_typed_datum`/`delete_typed_datum` tie it
  together: read the old value if present, move the sk entry between
  old/new keys (or leave it if unchanged), surface any uniqueness
  violation via `WriteTypedResult`. `write_typed_datum_client` is the
  two-round-trip client-side orchestration the design doc's "sk Index"
  section calls for (plain read to learn the old value, then the actual
  write declaring every touched datum_id up front) — collation itself
  needed no changes. Proven end-to-end by
  `integration_typed_write_client.rs`: a second writer to an
  already-taken unique value gets the violation signal back for a
  follow-up call.

  **Explicit scope decision, not a gap**: automatically invoking the
  declared `ConflictOp` in-process was decided out of scope — there is
  no nested-op-invocation mechanism anywhere in this framework
  (`OpHandler`'s signature has no way to call another named op), and
  adding one is a real, separate framework change. A detected violation
  is surfaced as data; the client-side helper makes an ordinary
  follow-up call instead of the framework dispatching one itself.

- **Datum Type System, Part 2 (revised) — Automatic Index Maintenance &
  Op Lifecycle.** Replaces Part 2's two-round-trip sk write path with a
  three-phase op lifecycle so indexes can stay **resident** on their
  owning thread instead of being rebuilt from bytes on every op: (1)
  **execute** — the op handler's writes are staged in `OpContext`
  (`staged: HashMap<DatumId, Option<Vec<u8>>>`, read-your-own-writes)
  rather than written directly; (2) **index-update phase** — for every
  changed indexed field, the executing thread dispatches an
  `IndexUpdate` (`WorkerMessage::IndexUpdate` locally,
  `Request::IndexUpdate`/`Response::IndexUpdateResult` cross-node) to
  the index datum's owning thread, which applies it against a resident
  per-thread cache (`HashMap<DatumId, Vec<u8>>` inside `WorkerHandle`,
  loaded once on cold miss, kept live thereafter; still write-through to
  disk on every update for now — avoiding that I/O is Storage Tier's
  job, not this plan's) and checks constraints synchronously; (3)
  **commit or fail** — once every dispatched reply is in, either the
  staged writes commit and the client gets `OpResult`, or nothing is
  written and the client gets `OpError`. `IndexHandlerRegistry`
  (`seisin-node`, new) keeps this framework-level machinery type-agnostic
  — `seisin-types` registers the actual `"sk"` `IndexHandler`
  (`SkIndexOp::{Insert,Remove}`, byte-level `apply_sk_index_update`).
  `OpRecord` gained `index_update_state: Option<IndexUpdateState>`
  tracking pending replies; `try_run_if_ready` now dispatches instead of
  committing immediately whenever an op scheduled updates, and
  `WorkerMessage::IndexUpdateReplied` performs the actual commit-or-fail
  once every reply is in. `TypedOpContext` (Drop-based) gives op authors
  plain `get`/`set`/`delete` calls — its `Drop` impl diffs before/after
  `FieldValue`s per declared sk index and calls `schedule_index_update`
  automatically, so index maintenance is never hand-written per op.
  Proven end-to-end by
  `integration_automatic_index_maintenance.rs`: a second write of an
  already-taken unique value fails the whole op via the real
  cross-thread `IndexUpdate`/`IndexUpdateReplied` round trip (not a
  shortcut), stress-tested 10x with no flakiness. Retired Part 2's old
  `typed_write.rs`/`client.rs`/`integration_typed_write_client.rs`
  two-round-trip design entirely — sk's client-visible behavior
  (uniqueness rejection) is unchanged, but the mechanism underneath it
  is not. Parts 3 (rk — splay tree leaderboard), 4 (tk — bitemporal
  valid-time), and 5 (relational/FK constraint enforcement) build on
  this same IndexUpdate/IndexHandler mechanism and are next, starting
  with Part 3 (rk).

- **Index Storage Engine — counted B+Tree for rk.** While starting Part 3
  (rk), the originally-planned in-memory splay tree design was reopened:
  the index-maintenance mechanism's `IndexHandler` contract only holds
  bytes across calls, giving a real splay tree object no actual benefit
  over simpler structures — and separately, rk's index shouldn't ever be
  fully materialized in memory at all. Research
  (`research/2026-07-22-index-storage-engine-choice.md`) confirmed a
  counted (order-statistics-augmented) B+Tree is the right structure for
  this workload — LSM-trees are architecturally hostile to rank queries,
  hash/radix have no ordering, quantile sketches are complementary at
  best. New standalone crate `seisin-storage`
  (`docs/superpowers/specs/2026-07-22-index-storage-engine-design.md`):
  generic byte-keyed, disk-backed counted B+Tree with zero dependency on
  `DatumId`/ring/gossip/node concepts. Fixed-size keys/values chosen at
  tree-creation time; configurable page size (a power of 2, `>= 4096`,
  validated in a superblock) rather than hardcoded, since page-size
  auto-detection/benchmarking are deferred but configurability isn't;
  insert-only/upsert (no delete, so no free-list needed); sibling-linked
  leaf pages for bounded forward/backward scans; subtree-entry-counts on
  internal pages for O(log n) rank-based lookup backing middle-sampling.
  No WAL/fsync/crash-safety machinery — `open()` validates the superblock
  and returns `Result` (never panics) on mismatch, and `rebuild_from`
  wipes and bulk-loads from a caller-supplied iterator (the caller's job
  to re-derive entries from a full datum scan, matching this project's
  established reasoning for why index writes don't need to be fsynced
  before an op acks). Proven page-size-agnostic by running the same
  functional test logic at two distinct valid page sizes (4096, 16384).
  A real algorithmic bug (internal-node split's separator/child
  assignment was backwards in one branch) and a real test-design bug
  (`to_le_bytes()`'s byte-lexicographic order diverges from numeric order
  past 256, not a B+Tree bug) were both caught and fixed during
  execution, not left latent. Explicitly out of scope, deferred to
  separate later plans: rk's own `IndexKind` logic built on this engine,
  node-function/placement wiring (which node's disk holds a given index's
  file — a node-role model, decided during this same brainstorm, that
  reopens gossip/sequencer machinery), page-size auto-detection, and an
  operator-facing page-size benchmark tool. pk/sk/tk each get their own
  storage-engine decision later against the same research, not assumed to
  need this same engine.

- **Index-architecture review follow-ups (pre-Part 3).** Four changes
  from a direction review of the index abstraction, done before rk is
  built:
  1. **`ResidentIndex`/`IndexKind` traits replace the bytes-based
     `IndexHandlerRegistry`.** The old
     `Fn(Option<&[u8]>, &[u8]) -> (Vec<u8>, Option<String>)` contract
     fit sk but couldn't hold rk's live B+Tree file handle; the rk
     design doc's earlier answer (delete the registry, hardcode a
     per-kind string match in `worker.rs`, move sk logic into
     `seisin-node`) is superseded — the registry layering was right,
     only the handler contract was wrong. `IndexKindRegistry` now holds
     `IndexKind` trait objects whose `open` builds a per-thread
     resident `Box<dyn ResidentIndex>` on cold miss (one
     `HashMap<DatumId, Box<dyn ResidentIndex>>` in `worker.rs`, no
     parallel per-kind caches); `apply` returns
     `{violation, write_through: Option<Vec<u8>>}` so blob-persisted
     kinds (sk) write through while self-persisted kinds (rk's file)
     return `None`. sk's impl (`SkIndexKind`/`SkResidentIndex`) stays in
     `seisin-types` — it already depends on `seisin-node`, so the
     "logic must move into seisin-node" premise was wrong. Bonus fixes:
     sk entries now decode once per residency instead of per update,
     and undecodable stored sk bytes are an `open` error instead of
     silently becoming an empty index.
  2. **`TypedOpContext` silent failures fixed.** `set` swallowed encode
     errors while still updating its diff state (indexes scheduled for
     a write that never staged); `get`/`ensure_tracked` mapped
     undecodable existing bytes to `None` (corrupt data
     indistinguishable from absent — an op could overwrite real data
     and strand stale sk entries). `get`/`set`/`delete` now return
     `Result` and fail loudly before any divergence.
  3. **Version prefixes everywhere bytes cross a boundary.** Every
     encoded `Request`/`Response` (`PROTOCOL_VERSION`) and
     `GossipMessage` (`GOSSIP_PROTOCOL_VERSION`) starts with a version
     byte; every encoded datum starts with a `u16`
     `DatumTypeDef.version`. Policy documented at each constant: strict
     n → n+1 rolling deployments mean the version-n decoder is kept for
     one release after bumping; datum decoding across schema versions
     needs a version history (deployment sub-project's job) — until
     then a mismatch is a hard, explicit error, never silent
     misinterpretation. The tagless datum encoding made this
     load-bearing: without the prefix, add-a-field evolution could
     never decode pre-existing bytes at all.
  4. **Index taxonomy corrected in the design doc.** pk is identity
     (but still needs a real id→location structure — Storage Tier's
     job); sk/rk are derived+rebuildable (which is what licenses
     `seisin-storage`'s no-WAL/`rebuild_from` stance); **tk is
     decomposed field storage, not an index** — its values exist
     nowhere else, so rebuildability-based durability relaxations
     never apply to it, and its residency model is lazily-loaded
     range segments (range queries over long histories), not
     whole-history-resident. The rk design doc's framework section was
     rewritten around the trait pair (including a `query` method on
     `ResidentIndex` for the read path, and `RkIndexKind` carrying
     `data_dir` from registration so spawn signatures don't change).

- **Datum Type System, Part 3 — rk index (leaderboards).** Per
  `specs/2026-07-23-rk-index-design.md` and
  `plans/2026-07-22-datum-type-system-part3-rk-index.md`. rk rides the
  `ResidentIndex`/`IndexKind` rail end to end: `IndexDef::Rk { field }`
  (declaration-time panic if the field is undeclared or non-numeric),
  `TypedOpContext`'s drop-diffing schedules a single `RkIndexOp`
  (`old_rank_key`/`new_rank_key` options) to the one derived
  `rk:{type}.{field}` datum, and `RkIndexKind`/`RkResidentIndex`
  (`seisin-types::rk_kind`) apply it as remove-then-insert against a
  resident `seisin-storage::BPlusTree` file handle
  (`write_through: None` — self-persisted; files named
  `rk_<index-datum-id-hex>.btree` under the new `NodeConfig.data_dir`).
  Keys are 24-byte composites (order-preserving 8-byte rank key —
  sign-bit flip for I64, total_cmp bit transform for F64 — ++ pk_id as
  tiebreaker, so tied scores never collide). New engine primitive:
  `BPlusTree::remove` (presence check, then a count-decrementing
  descent; no page merge/rebalance — documented accepted limitation),
  property-tested against a model map over interleaved insert/remove.
  Read path: `ResidentIndex` gained a default-erroring `query` method;
  new client-facing `Request::RkQuery { index_datum_id, query }` /
  `Response::RkQueryResult { entries }` wire pair with standalone
  `RkQueryKind`/entry codecs in `seisin-protocol` (defined once, used
  by both `server.rs`'s routing and rk's own impl);
  `WorkerMessage::IndexQuery` + `WorkerHandle`/`WorkerPool::
  run_index_query` answer synchronously from the owning thread with no
  collation; `server.rs` redirects a non-native `RkQuery` exactly like
  `Op`. Registration is a composition-root concern
  (`register_rk_index_kind(&mut registry, data_dir)`) — the bare
  `seisin-node` binary can't do it (dependency cycle), a solution
  binary does. Proven end-to-end by `integration_rk_leaderboard.rs`
  (writes via `TypedOpContext` over the real wire, then
  TopN/BottomN/PercentileSample queries, including a score change
  moving — not duplicating — an entry), stress-run 10x, plus the
  existing wound-wait/collation suites 20x, no flakiness. Deferred, per
  the spec: conditional/ratchet updates, rank-in-write-response,
  sharding, placement wiring, page-size auto-detection.

- **lb (leaderboard) datum class.** Per
  `specs/2026-07-23-lb-datum-class-design.md` and
  `plans/2026-07-24-lb-datum-class.md`. The third class on the
  `ResidentIndex` rail: a structured ranked-set datum class —
  **primary data** with solution-called ops, not a derived index (an
  lb write is not a side effect of any datum field; the write *is* the
  op). Framework extension: `ResidentIndex::execute` (mutate-with-
  result, default-erroring) + `WorkerMessage::IndexExecute` +
  `WorkerHandle`/`WorkerPool::run_index_execute`, mirroring the query
  path — single-datum atomicity from serial message processing, no
  collation. This is the rail tk (Part 4) will reuse for corrections.
  Engine additions: `BPlusTree::rank_of_key` (counted descent, the
  mirror of `entry_at_rank`) and `scan_from_rank` (one descent + a
  sibling walk). lb itself: `LbClassDef { name, score_type,
  display_len, rule: Max|Min|Replace }` registered one-kind-per-class
  as `lb:{name}` (how `open` learns the class from just a `DatumId`);
  board identity `lb:{class}:{leaderboard_id}:{area_config_id}` with
  all board-level attributes normalized into the datum id — entries
  are exactly `rank_key(8) ++ player_id(16) -> u16-length-prefixed
  fixed-width display`; per-board resident state is the B+Tree handle
  plus a player->rank-key map rebuilt by one O(n) scan on cold open
  (derivable, never persisted). Ops: `update_lb` (declared rule
  applied on the owning thread via raw rank-key byte comparison —
  valid because the encoding is order-preserving — then one response
  bundling total, exact best-first rank, top list with covering
  display, a neighbors window via `scan_from_rank`, and friend ranks;
  no 1+N fetches anywhere), `remove_lb`, and read-only `LbQuery`
  (adds bottom lists; used by spectators and the client n+k
  oversampling/reload contract — no cursors, no push). Wire:
  `Request::LbExecute`/`LbQuery` + `Response::LbResult` with
  standalone codecs in `seisin-protocol`; `server.rs` routes both like
  `RkQuery` via a newly factored `redirect_if_foreign` shared with rk.
  Registration is composition-root-only (`register_lb_class`), same as
  rk. Proven end-to-end by `integration_lb_boards.rs`: two independent
  boards over the real wire — top ordering with covering displays,
  Max-rule rejection of a worse score, friend ranks (with absent
  friends omitted), bottom lists via query, and removal; stress-run
  10x plus the standing 20x wound-wait/collation suites, no flakiness.
  Deferred per the spec: elo (needs opponent context), tie-policy
  upgrades, bottom-in-update-response, board wipe (seasons are new
  board ids), push notification (the oversampling contract exists to
  avoid it), and variable-length display — TOAST-style inline+overflow
  storage is recorded in the spec as a named Storage Tier requirement
  (with its free-list consequence).

- **Datum Type System, Part 4 — tk (bitemporal valid-time) datum
  class.** Per `specs/2026-07-24-tk-datum-class-design.md` and
  `plans/2026-07-24-tk-datum-class.md`. The fourth resident-rail
  class: decomposed field storage — primary data (values exist
  nowhere else; no rebuild-from-scan story), riding lb's
  `execute`/`query` rail with explicit ops rather than
  `TypedOpContext` diffing, because corrections need an explicit
  `as_of` that field-diffing can't express. One counted-B+Tree file
  per (class, entity), keyed `sub_key ++ ts(lower)` — the declared
  `sub_key_width` (0 = plain per-entity) gives independent
  non-overlapping histories per *sub-part* of an entity (the driving
  example: entity = investment account, sub_key = investment id,
  value = amount held), all in one file on one owning thread, with a
  `SnapshotAt{t}` query answering "what did the whole entity hold at
  time t" in a single ordered walk — no 1+N. Rejected layouts
  documented as broken, not deferred: segment-blobs-as-datums (foreign
  ownership) and one shared per-class file (concurrent multi-owner
  writes). Residency is the open file handle only — B+Tree pages are
  the lazily-loaded range segments, O(log n) page reads per op.
  Engine addition: `BPlusTree::rank_of_floor` (counted descent;
  `Err(0)`-with-passed-subtrees steps back across the leaf boundary).
  Correction-upsert: covering-range close + inherit-old-upper insert;
  same-instant set = in-place value correction; gap-fill bounded by
  the sub-key's own successor (never leaks across sub-parts);
  `Clear` closes without a successor (gaps allowed; clear at exact
  lower removes the empty span). `as_of: Option<i64>` — `None`
  server-stamped via a new `WallClock` seam (`SystemWallClock` +
  test fakes; gossip's `ClockSource` is monotonic, wrong tool).
  `value_width`/`sub_key_width` violations and mistyped values are
  rejected loudly, never truncated. Wire:
  `Request::TkExecute`/`TkQuery` + `Response::TkResult` with
  standalone codecs, routed via the shared `redirect_if_foreign`.
  Proven end-to-end by `integration_tk_history.rs` (two accounts, two
  investments each, over the real wire: backdated correction,
  snapshot before/after a Clear, range spanning a gap, server-stamped
  write, oversized-value rejection), stress-run 10x plus the standing
  20x wound-wait/collation suites, no flakiness. Deferred per spec:
  TypedOpContext sugar, transaction-time audit, no-gaps opt-in,
  TOAST for wide values, file consolidation (Storage Tier).

- **Datum Type System, Part 5 — relational (FK) constraints & pk
  identity discipline.** Per
  `specs/2026-07-24-fk-constraints-design.md` and
  `plans/2026-07-24-fk-constraints.md`. **The Datum Type System is now
  complete (Parts 1-5).** Pk discipline: every typed datum's pk is
  `PkKind::Uuid` (version-7 enforced at `TypedOpContext`
  writes/deletes; byte-level `OpContext` stays unrestricted) or
  `PkKind::Enum(mnemonics)` — well-known names deriving their ids
  (`enum_pk_id`), derived-on-demand with no seeding, extendable only
  by schema migration. Constraints
  (`RelationalConstraintDef { field, references, resolution }`)
  reference `FkTarget::PkUuid` (runtime check),
  `FkTarget::PkEnum { mnemonics }` (**zero-dispatch static membership
  check at `set()`** — the payoff of enum pks), or
  `FkTarget::SkUnique` (runtime check against the derived sk key;
  field holds the natural-key value — a spec correction from the
  earlier Bytes-16 rule, applied to the spec). Lifecycle: new
  `ExistsCheck`/`ExistsCheckReplied` message pair mirroring
  `IndexUpdate` (wire `Request::ExistsCheck`/`Response::Exists`,
  node-to-node AND client-facing), counted in the op's pending-replies
  state; missing + `Reject` fails the op atomically ("dangling
  reference"), missing + `Track` dispatches an `IndexUpdate` inserting
  `(referencing_pk, target)` into the blob-resident `"fk_pending"`
  kind (pending grows mid-flight) and commits. The commit-or-fail tail
  was factored into `finish_op_if_settled`, shared by both reply
  handlers. The eventual scan is pure driver orchestration
  (`Request::FkPending { List | Remove }` + `ExistsCheck` probes +
  ordinary `Request::Op` ConflictOp invocation) — no framework
  threads, no nested op invocation, preserving the Part 2 decision.
  Documented approximations: write-time `SkUnique` existence is
  bytes-exist (exact at scan time); fk_pending's driver Remove mutates
  resident state without write-through (self-healing — ground truth
  is re-derivable by re-probing). One behavioral wrinkle surfaced by
  the integration test and kept as-designed: a resolution op that
  writes another dangling value is simply re-tracked — resolution ops
  must write a valid reference or delete the datum. Proven end-to-end
  by `integration_fk_constraints.rs` (enum accept/reject, hard-reject
  atomic failure, the `_e_`-style out-of-order tracked write with
  driver-observed natural resolution, and a never-resolved entry
  driver-resolved via the declared delete-flavored ConflictOp);
  stress-run 10x plus the standing 20x suites, no flakiness. Deferred
  per spec: uniqueness defense-in-depth scan, compound/prefix FKs,
  cascade policies, cross-def type registry.

- **Part 5b — delete-side FK enforcement, field checks, extent &
  rescan.** Per `specs/2026-07-25-fk-delete-side-and-validation-design.md`
  and `plans/2026-07-25-fk-delete-side-and-validation.md`. Closes the
  FK/validation story. (1) **`WriteThrough` enum** (None/Put/Delete):
  emptied sk entry lists and drained fk_pending lists now DELETE their
  stored datum instead of persisting empty blobs — Part 5's bytes-exist
  approximation retired; every exists-probe is exact. (2)
  **`Expectation`** generalizes exists checks to both polarities
  (Present-with-policy = write-time FKs; Absent-with-message =
  delete-side restrict). (3) **Delete-side guards** declared on the
  referenced type (`GuardRef { type_name, field, on_delete:
  Restrict|Track }` — the referencing type must declare an sk index on
  the FK field): Restrict schedules an Absent probe at
  `sk:{type}.{field}:<pk>` and the delete fails atomically while
  references exist; Track inserts a `(deleted_pk, sk_probe_key)`
  marker into `deleted_refs:{type}.{field}` (the fk_pending pair-set
  kind, documented dual use) and the scan driver chains the
  referencing constraint's ConflictOp one hop per pass. PkEnum is now
  documented **append-only** (mnemonic removal is not a legal
  migration), so enum-pk types need no delete side at all. (4)
  **Field checks** (`FieldCheck::Gt/Ge/Lt/Le/MinLen/MaxLen`, validated
  at declaration, enforced at set(), re-run by the rescan). (5)
  **Type extent**: the `"extent"` kind (self-persisted B+Tree of pks,
  opt-in `.track_extent()`, maintained automatically on
  create/delete), paged via `Request::ExtentQuery`/`ExtentResult`;
  single-datum-per-type funnel documented (rk's limitation class).
  (6) **Driver rescan**: `rescan_every_millis` declared per type
  (driver guidance only) and `driver::validate_type(addr, def,
  read_op, page_size) -> Vec<ValidationFinding>` — pages the extent,
  re-runs field checks/enum membership, probes every runtime FK
  target; incoming validation falls out of every type's outgoing scan
  plus the delete markers. Proven end-to-end by
  `integration_delete_side_and_rescan.rs`: a restricted delete
  rejected then succeeding after the driver-run cascade (marker → sk
  enumeration → ConflictOp → WriteThrough::Delete emptying the sk
  key), and the rescan finding exactly the check violation + two
  dangling refs seeded by a byte-level write that bypassed the typed
  layer. Stress 10x + standing 20x suites, no flakiness. Deferred per
  spec: macro DSL (three forcing functions now), extent sharding,
  framework-scheduled rescans, cross-def declaration validation.

- **Part 5c — partition index & validation scan order.** Per
  `specs/2026-07-25-partition-index-and-scan-order-design.md`. The
  extent generalizes into the **`"partition"` kind**: a named,
  pk-ordered subset of a type's datums (`partition:{type}:{name}`) —
  the extent is the trivial `all` partition (framework-maintained,
  unchanged), and the validation system's invalid-set is the
  `invalid` partition, **membership being the datum's valid/invalid
  flag** (a validation verdict must never be a content write churning
  indexes/versions). Driver-maintained partitions mutate via the new
  client-facing `Request::PartitionUpdate` (the kind gained
  `execute`); `Request::ExtentQuery` already addressed any partition
  datum by id. Driver additions: `mark_invalid`/`clear_invalid`
  (thin PartitionUpdate wrappers) and `revalidate_invalid` — the
  checker's fast path, paging only the invalid partition, clearing
  entries that pass, returning the still-failing (careful pagination:
  cleared entries shift ranks left). `scan_order(defs)` computes the
  full-sweep type order from the schema graph: most incoming runtime
  references first (fixing the most-depended-on data first can
  resolve the references pointing at it), ties by least outgoing,
  further ties by derived type id (deterministic; numeric type ids
  are future schema-registry/DSL work); PkEnum refs excluded
  everywhere — static refs never dangle. Proven by extending
  `integration_delete_side_and_rescan.rs` (mark → fast-path re-check
  keeps membership while broken → typed-write fix → re-check clears →
  empty partition; scan order putting referenced types before the
  referencer) plus unit coverage of all three tie-break levels;
  stress 10x + standing 20x suites, no flakiness. Deferred: custom
  partition orderings, predicate-declared auto-maintained partitions,
  numeric type ids.

- **Sub-project 4, Part A — Storage Tier (delta log, store wire,
  RemoteStore).** Per `specs/2026-07-25-storage-tier-part-a-design.md`
  and `plans/2026-07-25-storage-tier-part-a.md`. The durable source of
  truth over a **static capacity-weighted storage ring** (the existing
  `Ring` reused with weight units in place of thread counts; gossip/
  migration are Part B/C). Decision record honored: **storage stays
  content-agnostic** — semantic field-path patches were re-homed to a
  future compute-side typed-patch surface; the storage layer speaks
  only structure-blind byte deltas. Pieces: (1) `seisin-storage::
  delta` — prefix/suffix-trim `Delta` (diff/apply with strict bounds,
  codec; copy/insert is a drop-in later), model-tested `apply(old,
  diff(old,new)) == new` over an LCG corpus; (2) `seisin-storage::
  datum_log` — append-only Full/Delta/Tombstone log, CRC-framed
  (hand-rolled crc32), `fdatasync` before every ack (write-before-ack,
  literally), recovery scan with torn-tail truncation, **self-rebasing**
  (chain > 8 or cumulative delta bytes > half the value consolidates
  into a Full — bounding replay and pre-shrinking Part C compaction),
  `NeedFull` when a delta arrives for an unknown id; (3)
  `seisin-protocol::store_wire` — independently versioned
  (`STORE_PROTOCOL_VERSION`) Put/Patch/Get/Delete over the existing
  framing, one plain blocking TCP connection per compute worker thread
  (no multiplexing — `Store` is synchronous); (4) `Store::
  put_with_previous` (defaulted) fed by `Cache::put` with the value it
  overwrites, so `RemoteStore` ships a Patch when the delta is
  worthwhile and falls back to Put on cold caches/`NeedFull`/poor
  deltas; (5) role config (`role: Compute|Storage`, `store_address`,
  `capacity_weight`) and `main.rs` role dispatch (a storage node runs
  only the store listener over its log). Failure policy: any storage
  round-trip failure panics the compute worker naming node + datum —
  v1 fail-stop; coordinated halt is Part B. **Found during
  integration**: post-op `release_datums` invalidates the owning
  thread's cache, so blind write-only ops never have a previous value —
  the delta path lights up for read-modify-write ops, which is exactly
  the typed layer's shape (`ensure_tracked` always reads first);
  documented in the test. Proven end-to-end: write through compute →
  evict all compute caches → read from storage; **storage restart from
  the same log directory serves every acked write** (the fsync
  contract); a one-byte change to a 1 MiB datum grows the log by < 10 KB
  and reads back exactly; weighted ring spread sanity. Stress 10x +
  standing 20x suites, no flakiness. Deferred: Part B (storage gossip,
  coordinated halt, add/remove migration), Part C (compaction,
  reweighting, tk/lb B+Tree durability, CDC dedup, group commit,
  copy/insert deltas, chunk-aware wire).

- **Sub-project 4, Part B — Storage membership & coordinated fail-stop
  halt.** Per `specs/2026-07-26-storage-tier-part-b-design.md` and
  `plans/2026-07-26-storage-tier-part-b.md`. Storage nodes join the
  **one existing gossip network, role-tagged** rather than getting a
  second pool: `MemberRole::{Compute,Storage}` plus `capacity_weight`
  and `store_address` on `MemberUpdate`/`MemberRecord` and the wire
  codec (`GOSSIP_PROTOCOL_VERSION = 2`; v1 decoding deliberately
  dropped — pre-first-release, so the n±1 keep-old-decoder policy binds
  from the first deployed release, not from v1). Pieces: (1)
  `ClusterState { compute_ring, storage_ring, store_addresses, halt }`
  threaded through gossip server/loop; `apply_ready_mutations` routes
  by role — compute Join/Leave mutate the compute ring (plus eviction
  and lock release) exactly as before, a storage Join extends the
  storage ring (`weight.max(1)`) and address book live, and a
  **storage Leave engages the halt** (no ring mutation — the ring must
  keep naming the dead node's shards so nothing silently re-homes;
  no replication in v1 means shard loss is unrecoverable, so the only
  safe move is stopping the world with a reason). (2)
  `HaltState` (first reason wins) gates `serve` before dispatch: every
  client op on a halted node gets `OpError` carrying the halt reason.
  (3) `serve_gossip_storage` — storage nodes run an ack-only gossip
  responder (merge incoming, reply with piggyback; no rings, no pool),
  so compute failure detectors probe them like any member while
  storage stays out of the mutation business; `main.rs` storage branch
  runs store listener + this responder. RemoteStore's address book is
  now the shared `Arc<RwLock<...>>` so gossip-discovered storage nodes
  become routable without restart. Proven end-to-end in
  `integration_storage_halt.rs`: a real storage node (log + store +
  gossip responder) serves write-through while healthy and the halt
  stays disengaged; a compute node whose config names a
  silent-from-the-start storage member confirms it dead via the
  standard SWIM path → halt engages naming the node → client ops
  return the "cluster halted" reason → the compute ring is untouched.
  Stress 10x + standing 20x suites, no flakiness. Deferred to Part C:
  add/remove migration (a storage Join rebalances nothing yet — new
  weight only affects new placement), storage-side self-halt,
  auto-resume with log identity.

- **Sub-project 4, Part C-1 — Storage migration, reweighting, log
  identity, pause & self-halt.** Per
  `specs/2026-07-26-storage-migration-design.md` and
  `plans/2026-07-26-storage-migration.md`. One unified mechanism for
  storage node add / planned remove / capacity reweight: a client-side
  **migration driver** (`seisin-migrate`) that drains shards live,
  briefly pauses the cluster to catch the write tail, flips the storage
  ring on every compute node, and resumes. Add/remove/reweight all
  reduce to "the storage ring's member/weight set changes" → the
  **moved set** (`plan_moves`: every id whose owner differs between the
  old and proposed rings). Pieces:
  (1) **Log identity** — `DatumLog` stamps a UUIDv7 log id in its header
  at creation (`FORMAT_VERSION 2`), read back on every reopen; `Identify`
  (store wire) returns `(node_id, log_id)`; compute nodes keep an
  identity book (log id per storage member) reconciled from gossiped
  `MemberUpdate.log_id` (first-writer-wins, so an impostor's gossip can't
  overwrite a known id) and re-set wholesale by `InstallStorageRing`.
  (2) **Transfer engine** (store wire v2: `ListIds`/`Transfer`/
  `TransferStatus`/`FinishTransfer`/`Retire`) — a source snapshot-copies
  ids to a destination over the store wire while tracking a dirty set
  (any id written during the copy), re-sends the dirty tail on finish,
  and tombstones on retire. (3) **Pause** — `HaltState` grows a
  resumable, driver-owned pause alongside the permanent halt; `gate()`
  gives one answer (halt beats pause; distinct "cluster halted" vs
  "cluster paused" prefixes). (4) **Admin control plane** (client wire
  v2: `GetClusterConfig`/`Pause`/`Resume`/`ClearHalt`/
  `InstallStorageRing`) on the compute `serve` path, bypassing the op
  gate so config is readable while halted and the flip runs under the
  pause; the flip is a live swap of the shared `Arc<RwLock<Ring>>`.
  (5) **Membership behavior change** — a storage Join no longer
  auto-extends the ring (that re-homed live keys); it only records
  availability (address + log id), and the ring changes exclusively via
  a driver flip. A storage Leave halts **only if the node is still in
  the ring**, so a drained node's later Leave is ignored (planned
  removal avoids the halt for free). (6) **Storage self-halt** — the
  store server answers `Error` instead of serving if it has heard no
  gossip within the suspicion window (fresh boot counts as "just
  heard"). (7) **Driver `resume`** — verifies each ring member's
  identity via `Identify` against the identity book (`GetClusterConfig`
  readable while halted), then `ClearHalt`s every compute node; refuses
  on any node-id/log-id mismatch (impostor), leaving the halt standing.
  All six scenarios proven in `integration_storage_migration.rs` (live
  add, planned remove + no-halt-after-drain, reweight, concurrent writes
  through the dirty tail, halt+resume, impostor refusal), 10x stress, no
  flakiness. Wire bumps (all pre-first-release, old decoders dropped):
  `STORE_PROTOCOL_VERSION 2`, `PROTOCOL_VERSION 2`,
  `GOSSIP_PROTOCOL_VERSION 3`, `DatumLog FORMAT_VERSION 2`. Deferred to
  later Part C: replication (crashes still halt; recovery is log-dir
  restore + resume), log compaction, tk/lb B+Tree datum-grade
  durability, group commit, copy/insert deltas, chunk-aware wire,
  hot-value LRU.

- **Sub-project 4, Part C-2 — Per-datum-type storage replication.** Per
  `specs/2026-07-27-storage-replication-design.md` and
  `plans/2026-07-27-storage-replication.md`. Replication is a **per-type
  schema property** (`DatumTypeDef.replication_factor`, default 1;
  `.replicated(n)` builder) — app-aware, selective, the thing Seisin
  uniquely offers; whole-disk durability is deliberately out of scope
  (external block-device tooling). Pieces:
  (1) `Ring::replicas(id, n)` — up to n distinct nodes, rank 0 ==
  `native` (so n=1 is exactly today), salted re-hash for ranks 1.. with
  a `node_ids()` completeness sweep, capacity-weighted.
  (2) N stored per datum: the typed write path tags each write, the
  `DatumLog` record header carries a u16 factor (`FORMAT_VERSION 3`),
  `ListIds` returns `(id, n)` pairs — so the type-blind migration driver
  stays uniform. Store wire v3 (`STORE_PROTOCOL_VERSION 3`) carries n on
  Put/Patch.
  (3) N threads through `Store`/`Cache`/`OpContext` via `*_replicated`
  methods with N=1 back-compat wrappers, so every existing single-copy
  path is byte-for-byte unchanged.
  (4) `RemoteStore` writes to every alive, non-stale replica (≥1 to ack;
  a failing replica is marked stale), reads the primary with failover,
  and trips the coordinated whole-cluster halt **point-of-use** only on
  total shard loss (every replica gone) — for an N=1 datum, exactly its
  one node being gone, so single-copy still fail-stops. The membership-
  time storage-Leave halt is gone; `apply_ready_mutations` now maintains
  `ClusterState.storage_alive`/`storage_stale` (a confirmed-dead node is
  dropped from serving and stays stale, never auto-re-trusted, until a
  driver re-replication re-admits it).
  (5) A pool drain barrier makes the migration `Pause` a true barrier
  (in-flight ops settle before the dirty tail is captured — fixes a
  write-loss race under concurrent writes).
  (6) `seisin-migrate`: `plan_moves` over replica sets (copy to each new
  replica, source rerouted off unreachable nodes); superseded copies on
  dropped nodes deleted after the flip; new `recover` verb drops
  unreachable nodes and restores replication onto survivors. `resume` /
  `InstallStorageRing` re-admit nodes from the stale set.
  Proven in `integration_storage_replication.rs` (replicated write +
  read-one, read failover, degraded write, total-loss point-of-use halt,
  N=1 fail-stop, `recover` re-replication, stale-not-served), 10x stress
  + standing 20x wound-wait, no flakiness. Wire bumps (pre-first-release,
  old decoders dropped): `STORE_PROTOCOL_VERSION 3`, `DatumLog
  FORMAT_VERSION 3`. Deferred: incremental catch-up of a returned
  replica (v1 does a full driver resync), rack/zone-aware placement,
  read load-balancing across replicas, changing a type's N on
  already-written data.

- **Sub-project 5 — Deployment & cluster tests.** Per
  `specs/2026-07-27-deployment-cluster-tests-design.md` and
  `plans/2026-07-27-deployment-cluster-tests.md`. A real-process,
  real-socket cluster harness under `cargo test` (no Docker — spawned OS
  processes cover the real-socket path). Pieces:
  (1) **Configurable failure-detection timeouts** — `NodeConfig` gains
  optional `probe_interval`/`probe_timeout`/`suspicion_timeout`/
  `self_halt_threshold` millis, defaulting to the `failure_detector`
  production constants; the harness turns them down so crash detection
  converges in ~1s.
  (2) `main.rs`'s composition root extracted to `seisin_node::node::run(
  config, ops, index_kinds)`, shared by the bare (op-less) binary and a
  new `cluster_test_node` binary that registers byte ops (put1/get1,
  put2/get2, touch_both).
  (3) A `ClusterHarness` (tests) that generates a RON config per node,
  spawns real node processes over localhost (storage-first), polls a
  startup barrier, drives them via `seisin-client` and the
  `seisin_migrate` library, `kill`s them (SIGKILL) for crashes, and reaps
  every child on drop; cluster tests are serialized by a global lock
  (parallel process-clusters starved gossip convergence and collided
  ports).
  Six scenarios in `integration_cluster.rs`: cross-node routing/redirect,
  cross-node multi-datum ops over the real peer link, compute-kill →
  ring reclaim, storage-kill → point-of-use halt, live reweight
  migration (real `seisin_migrate::migrate`), and replication failover +
  `recover`. 10x stress clean. **Found & fixed a real bug**: `node::run`
  built the compute ring from *all* config members, so a storage node
  became a compute owner and its "owned" datums redirected to a
  non-existent client port — the compute ring is now compute-members-
  only. Deferred: the deployment *management* system (n→n+1 rollout
  orchestration), Docker/container variant, graceful-leave signal
  handling, and a static-config "available-but-not-in-ring" storage
  member (so the migration scenario reweights rather than admits a brand
  new node — the same driver path over real sockets).

- **lb storage-backed cache.** Per
  `specs/2026-09-01-lb-storage-backed-cache-design.md` and
  `plans/2026-09-01-lb-storage-backed-cache.md`. `lb` boards moved from
  a fully-resident `BPlusTree` file on the owning compute node's local
  disk into the storage tier, behind a new content-agnostic ordered-
  collection store-wire primitive (`CollectionCreate`/`Insert`/`Remove`/
  `Get`/`ScanForward`/`ScanBackward`/`Sample`/`RankOfKey`/
  `ScanFromRank`/`Count`; `STORE_PROTOCOL_VERSION` 4). Writes replicate
  as logical ops fanned out to every serving replica (not byte diffs —
  storage performs the mutation itself), reusing a new shared
  `ReplicaResolver` (extracted from `RemoteStore`) for replica
  selection/stale-marking/total-loss halt. Each board is two
  collections (`rank`, `by_player`). Compute-side, `LbCache` replaces
  the old fully-resident tree: pinned top/bottom windows (refreshed on
  invalidation) plus a bounded LRU for point/around-player/friend
  lookups, sized per board via a `register_lb_class` resolver callback
  invoked on first access (the set of actual leaderboards isn't fixed,
  so this can't be a static table). `IndexKind` gained
  `attach_collection_store`, called by `node::run` once `ClusterState`
  exists — lets a solution's `register_lb_class` call (which runs
  before `node::run`) still end up with a working storage client. Same
  client-facing wire contract throughout (`Request::LbExecute`/
  `LbQuery` unchanged) — `integration_lb_boards.rs` now runs against a
  real 2-node storage-backed cluster instead of a compute-only one, and
  a new `integration_lb_cache_eviction.rs` proves the cache actually
  stays bounded on an oversized board. 20x stress clean on the standing
  wound-wait/collation suites. Removes lb from the "tk/lb B+Tree-file
  datum-grade durability" Storage Tier Part C remainder item below (rk
  and tk are unaffected, still pending).

As of this entry: 11 crates, 516 tests passing, `cargo fmt --check` and
`cargo clippy --workspace --all-targets -- -D warnings` clean. All
committed and pushed to `main`.

## Sequencing decision (2026-07-21, revised same day)

Sub-project 3 (Collation & multi-datum ops, including all of 3b's parts)
is now fully done — the entire
`specs/2026-07-20-cross-node-collation-and-wound-wait-design.md` spec is
implemented. Initially chose **Sub-project 4 — Storage tier** next, per
the original sub-project sequence, and began brainstorming Part A
(storage role, wire protocol, capacity-weighted ring, write-through
wiring — nothing implemented, no spec written).

**Revised mid-brainstorm**: storing a datum also needs to update its
type's pk/sk/rk/tk indexes, which are themselves persisted to disk —
Storage Tier's disk format may depend on how indexes actually need to
be structured/reconstructed (indexes are expected to be derivable from
a durable journal or a scan of the datums themselves, so index writes
likely don't need to be fsynced before ack the way datum content does,
but this needs the type/index system actually designed to confirm).
Switched to designing the **datum type system** (typed datum types,
pk/sk/rk/tk index kinds, relational constraints) first, so Storage
Tier's Part A can be designed with real knowledge of what it needs to
persist rather than needing a later rework. Storage Tier Part A/B/C
resume once the type system is designed.

## Not started — from the original sub-project sequence
- **Sub-project 4 — Storage tier, Part C (remainder).** Parts A (delta
  log, store wire, RemoteStore), B (role-tagged gossip membership,
  coordinated fail-stop halt), C-1 (add/remove/reweight migration, log
  identity, pause, storage self-halt, resume), and C-2 (per-datum-type
  replication with failover + driver `recover`) are done — see "Done"
  above. Remaining Part C: incremental catch-up of a returned replica
  (v1 does a full driver resync), rack/zone-aware placement, read
  load-balancing across replicas, changing a type's N on already-written
  data, log compaction, tk B+Tree-file datum-grade durability (lb's half
  of this is done — see "lb storage-backed cache" above), group
  commit, copy/insert deltas, chunk-aware wire, hot-value LRU.
- **Sub-project 5 — Deployment & cluster tests.** Done — see "Done"
  above (spawned-process cluster harness + six real-socket scenarios).
  A Docker/container variant reusing the same scenarios remains a
  possible later addition.

## Not started — from the 2026-07-20 design additions

These are new design surface added to the doc but not yet broken into
sub-project plans:

- **Datum type system.** Fully designed in
  `specs/2026-07-21-datum-type-system-design.md` (schema, pk/sk/rk/tk,
  uniqueness/relational constraint enforcement). **Complete**: Parts 1
  (schema declaration & field encoding), 2 (sk index + uniqueness),
  3 (rk — counted-B+Tree rank index), 4 (tk — decomposed bitemporal
  valid-time field storage), and 5 (relational/FK constraints + pk
  identity discipline), plus the lb leaderboard datum class built
  between 3 and 4 — see "Done" above. The original sequence resumes
  with Sub-project 4 (Storage Tier), which now has the full picture of
  what it must persist: blob datums, self-persisted B+Tree files, the
  TOAST/free-list requirement, and the datum-grade vs index-grade
  durability split.
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

## Prior sequencing decision (2026-07-23, now fulfilled)

Chose to proceed with **Sub-project 3 (Collation & multi-datum ops)**
next, per the original sequence, rather than designing the datum type
system first — rationale: collation operates at the
`DatumId`/`AuthorityIdx` level (which thread runs an op touching
multiple datums), not on typed content, so nothing about wound-wait/
foreign-pull/anti-degeneration needed the type system designed first.
That work is now complete (see "Done" above); see the "Sequencing
decision (2026-07-21, revised same day)" section above for the
sequencing decision that replaces this one.
