# Sub-project 3b: Cross-Node Collation & Wound-Wait

Builds on Sub-project 3a (`docs/superpowers/plans/2026-07-20-op-registry-single-node-collation.md`),
which proved the op-registry/`OpContext`/thread-assignment/write-back
mechanics for the single-node, uncontended case and explicitly rejected
cross-node collation. This spec designs the piece 3a deferred: what
happens when a multi-datum op needs datums whose native homes span
multiple nodes, and what happens when two ops' collation attempts
contend for the same datum.

## Scope

3b unifies every request into one shape and one acquisition model.
`Request::Op { op_id, op_name, datum_ids, payload }` becomes the *only*
client-facing request. There is no separate `Get`/`Put`/`Delete` wire
variant — a plain read or write is just a trivially-registered op (e.g.
a solution registers `user_get(User u) { u }`), no different in kind
from an arbitrary domain op like "transfer inventory item X from user A
to user B" (which takes the two user datum_ids and an opaque payload
describing the item/quantity — the item itself is not a datum, it's
just data inside the payload). Every op — trivial or elaborate — goes
through the same collate-then-run pipeline.

Every op carries a client-generated `op_id: DatumId`-shaped UUIDv7,
which is the ordering token used to resolve contention between two
ops' collation attempts (see "Acquire & Wound-Wait Mechanics" below).

In scope:
- A universal `op_id` on every request.
- Real cross-node datum transfer: an op's needed datums, wherever their
  native homes are, get pulled onto the single thread that will run the
  op.
- Wound-wait contention handling: when two ops' collation attempts
  race for the same datum, the older op_id always wins, without
  deadlocking or livelocking, per the mechanics below.
- The node/thread architecture and server-to-server connection model
  needed to make the above genuinely non-blocking at scale (a node can
  have many ops collating concurrently, not just one).

Explicitly out of scope / deferred:
- Storage tier changes (Sub-project 4).
- Deployment/versioning (Sub-project 5 and beyond).
- The typed datum layer and SK-uniqueness-as-a-constraint mechanism —
  an SK datum participates in acquire exactly like any other datum
  today; enforcing *uniqueness* on top of that is a datum-type-system
  concern, not new plumbing 3b adds.
- Smarter-than-3a anti-degeneration (no peek-ahead at the next queued
  op) and smarter-than-"local native-majority guess" op-to-thread
  placement (redirecting a whole op to a better node up front, rather
  than pulling everything to whichever node the client happened to
  contact, is deferred).
- Existing 3a/earlier tests built around `Get`/`Put`/`Delete` need
  rewriting against equivalent registered ops as part of this plan's
  task list — noted here so it isn't a surprise mid-implementation.

## Why Get/Put/Delete Disappear as Wire Variants

Retiring them isn't just cosmetic. Once every request must acquire its
datum(s) before running (see below — this applies uniformly, no
dirty-read exception, so a read is guaranteed to never observe a value
mid-mutation by another in-flight op), a plain "get" and an arbitrary
domain op are structurally identical: caller-supplied datum_ids, an
opaque payload, framework collates then invokes. Keeping `Get`/`Put`/
`Delete` as separate wire opcodes would just be two code paths doing
the same thing. `Request::Op` is genuinely the only shape needed.

## Acquire & Wound-Wait Mechanics

Two places hold state, and neither is a separate new "coordinator"
component — both live inside the existing per-thread worker loop
(`worker.rs`), which now plays a dual role for any given datum
depending on context:

- **Native-home role** (only meaningful when `ring.native(datum_id) ==
  (self_node, self_thread)`): a single **authority pointer** — `Idle`
  or `AwayAt(node, thread)`. The native-home thread never makes
  priority decisions; it only ever answers "who currently has this."
- **Current-holder role** (whenever this thread's cache currently holds
  the datum — as its native home, or because it collated it in from
  elsewhere): tracks *which op_id it's holding the datum for*, plus a
  **wait-queue** of other requesters (`(op_id, reply-channel)` pairs)
  for that specific datum.

An `Acquire { op_id: R, datum_id }` arriving anywhere is handled as:

- **At native home, `Idle`**: grant immediately (`Granted` — no
  content; see "No Content In Transfer Messages" below), flip the
  pointer to `AwayAt(requester)`.
- **At native home, `AwayAt(X)`**: reply with a redirect naming *both*
  `X`'s node address and its `ThreadId` — unlike the existing
  client-facing `Redirect` (a bare address, since the receiving node
  re-derives the right thread via `ring.native()`), a datum that's
  away from its native home can't be re-derived that way, so the
  target thread must be named explicitly. This is a distinct response
  shape from client `Redirect` for that reason — e.g. `Response::
  AwayAt { address, thread_id }` — even though the *behavior* (caller
  reconnects/resends elsewhere) is the same idea. The requester
  re-sends the same `Acquire` directly to that `(address, thread_id)`.
  (Native home's pointer can go stale if a datum moves directly
  between two non-native holders via a recall — see "Chained
  Forwarding" below — but self-heals the next time the datum genuinely
  returns to native home.)
- **At the actual current holder, not busy**: grant immediately (covers
  the brief window right after a grant, before some op has actually
  started using it).
- **At the actual current holder, busy holding it for op_id `H`**:
  - **`R < H` (requester older): recall wins now.** Evict locally,
    reply `Granted` directly to `R` — not routed back through native
    home first (a direct hand-off between holders; see "Chained
    Forwarding"). The *wounded* op (`H`) loses only this one datum, not
    everything it holds — it keeps whatever else it's already
    acquired (its op record's `acquired` list is untouched apart from
    removing this one datum, which moves back into `still_needed`),
    and its own collation loop just re-issues `Acquire` for it again. No
    backoff needed: `H` can never be the older side of a future
    collision against `R` specifically (R already won), so an
    immediate retry can't reopen the same cycle. This is what
    guarantees forward progress overall — every live contention set
    has a strictly-oldest op_id that never gets wounded by anyone, so
    it always eventually acquires everything it needs and completes,
    at which point it releases and unblocks whoever's still waiting on
    it.
  - **`R > H` (requester younger): enqueue and wait.** `(R,
    reply-channel)` joins this datum's wait-queue. No polling — the
    reply-channel is only ever written to once, when the datum is
    actually handed over. When `H` finishes with the datum (op
    completes normally, or `H` is itself later recalled by someone
    even older), the datum goes to the *oldest* queued waiter, if any,
    before falling back to `Idle`.

This is why the classic two-op cycle (op1 needs `a,b`; op2 needs `b,a`;
each grabs one first) can't deadlock: whichever op is older always
wins any specific contention it's party to, immediately, without
waiting on the other. It's the resource-acquisition ordering that's
guaranteed livelock/deadlock-free — there is no guarantee about which
op's *business logic* runs first, and none is needed.

### No Content In Transfer Messages

`Acquire`'s grant and `Recall`'s release carry no datum content —
write-through-before-ack already guarantees anything mutated is
durable in storage before its op's response is ever acknowledged, so
releasing a datum is just "evict my cache entry, stop claiming it."
Whoever is granted next simply cache-misses through to storage on
first actual access, exactly like 3a's existing `evict_non_native`
reload path. This is a real simplification over a design that ships
bytes peer-to-peer on every hand-off.

### Chained Forwarding

A recall hands a datum directly from the old holder to the new
requester without routing back through native home — native home's
`AwayAt` pointer still names the *old* holder until the datum
eventually returns there naturally (op completes with no one waiting,
or gets recalled by someone even older who then finishes and releases
toward native home). A third request arriving at native home in the
meantime gets redirected to the stale old holder, which itself now
needs to remember briefly "I gave this to X" so it can forward that
third request onward, rather than only being able to say "I don't have
it anymore." This is the same kind of best-effort, self-healing
forwarding chain the system already leans on elsewhere (lazy crash
reclaim, redirect-based client routing) rather than requiring instant
global consistency on every hop.

## Node/Thread Architecture

No new "coordinator" component. The existing worker thread (one per
`ThreadId`, from 3a) already owns per-datum state; this extends it to
also own in-flight op records for ops assigned to it:

```
op_id -> {
  op_name, payload,
  still_needed: Vec<DatumId>,
  acquired: Vec<DatumId>,
  reply: Sender<Result<Vec<u8>, String>>,
}
```

Flow for a client's `Request::Op`:

1. The connection thread resolves a destination thread using 3a's
   existing native-majority heuristic — now just a starting guess: if
   none of the op's datums are natively local to the node the client
   happened to contact, it still assigns to some local thread (e.g.
   least-loaded), which pulls everything remotely. Smarter placement
   (redirecting the whole op to a better node before doing any local
   work) is deferred.
2. The connection thread sends the destination worker a new op record
   (with a reply-channel) and blocks on that one channel. This is the
   same pre-existing per-connection cost as today (the client is
   already waiting on this exact thread) — 3b adds no new blocking
   here.
3. The destination worker, for each not-yet-acquired datum, dispatches
   an `Acquire` via a short-lived helper thread. **Same-node,
   different-thread** targets go straight through an in-process
   `WorkerHandle`-style channel (existing Rust types, no
   serialization). **Different-node** targets go through that node
   pair's shared peer-link (see below). Either way, the helper thread
   does the (possibly blocking, possibly queued-and-later-fulfilled)
   call and posts the outcome back into the destination worker's *own*
   inbox as a message once resolved — the worker's loop itself never
   blocks.
4. Grants, redirects, later-fulfilled wait-queue notifications, and
   incoming recalls (of datums this worker currently holds for
   someone else's op) all arrive as ordinary inbox messages and update
   the relevant op record or per-datum bookkeeping.
5. Once an op record's `still_needed` is empty, run it (as in 3a),
   reply through its channel, then release every acquired datum back
   toward its native home (3a's existing anti-degeneration path,
   which now genuinely crosses node boundaries where needed).

This lets one worker thread track an arbitrary number of concurrently
in-flight ops cheaply — it's never blocked waiting on any single one of
them, the same way it's never blocked on cache I/O today.

## Server-to-Server Connection Multiplexing

Naively, if every worker thread opened its own connection to whatever
remote thread it needed, two nodes with many threads each would end up
with `O(threads^2)` connections between them. Instead: **one persistent
connection per node pair**, established lazily on first need and
reconnected on failure, shared by every local thread's outbound
`Acquire`/`Recall` traffic to that peer — giving `O(servers^2)`
connections cluster-wide, not `O(threads^2)`.

A new module (e.g. `peer_link.rs`) per remote node provides this:

- **Writer side** owns the connection's write half; drains an mpsc
  channel that any local thread can send `(correlation_id,
  encoded_request_bytes)` into, writing each as a framed message in
  arrival order.
- **Reader side** owns the read half; the link is bidirectional, so
  incoming frames are a mix of responses to *our own* outgoing calls
  and fresh incoming requests from the peer. A response is matched by
  `correlation_id` against the shared `Mutex<HashMap<correlation_id,
  Sender<Response>>>` and forwarded there. An incoming request (an
  `Acquire` or `Recall`) carries an explicit target `ThreadId` — set
  either by the sender already knowing which thread to reach (a prior
  `AwayAt` redirect), or `native()`'s own resolution for a first-hop
  request — which the reader uses to dispatch straight to that local
  worker's inbox; the reply is later shipped back tagged with the same
  `correlation_id` the request arrived with.
- **Making a call**: the short-lived helper thread (from the
  node/thread architecture above) mints a fresh `correlation_id`,
  registers its reply channel in that shared map, sends `(id, bytes)`
  to the writer, blocks on its own reply channel, then feeds the
  result back into the calling worker's inbox.

Each frame on this link is a small envelope — `{ correlation_id, body
}` — wrapping the existing `Request`/`Response` encoding unchanged.
This envelope framing is specific to peer-link connections; the
existing one-shot client-connection protocol (`read_frame`/
`write_frame` per `Request`/`Response`, no correlation id needed since
it's strictly one-at-a-time) is untouched.

If a peer-link drops, every pending correlation id on it fails with an
error — this propagates back exactly like today's "relay/forward
failed" case, which the existing lazy-reclaim philosophy already
covers (native home just reclaims and reloads from storage on next
access). No new failure-handling concept is needed.

## Testing Strategy

- **Wound-wait correctness, single process, simulated multi-node**:
  spin up 2+ in-process nodes (as 3a's integration tests already do
  for gossip/routing), register an op needing two datums whose native
  homes are on different nodes, and directly reproduce the classic
  cycle (op1 needs `a,b`; op2 needs `b,a`, opposite acquisition order)
  with controlled op_id ordering, asserting both ops eventually
  complete and neither deadlocks.
- **Recall correctness**: an older op's request for a datum currently
  held by a younger op's in-flight collation causes the younger op to
  lose only that datum (verified by checking its still-needed set
  isn't reset to everything) and successfully reacquire it once the
  older op releases.
- **Wait-queue ordering**: multiple younger requests queued for the
  same held datum are granted oldest-first when it's released.
- **Chained forwarding**: a datum recalled directly from holder A to
  holder B (without passing back through native home) is still
  reachable by a third request that hits native home first (redirect
  chain resolves correctly).
- **Peer-link multiplexing**: many concurrent `Acquire` calls from
  different local threads to the same remote node, over the one shared
  connection, each getting routed back to the correct caller.
- **Peer-link failure**: a dropped connection fails in-flight calls
  cleanly rather than hanging; a subsequent call successfully
  re-establishes the connection.
- Existing 3a tests get rewritten against equivalent registered ops in
  place of the retired `Get`/`Put`/`Delete` variants.

## Open Questions Carried Forward

- Exact heuristic for "least-loaded local thread" when an op's datums
  aren't natively local to the node a client contacted (currently just
  "pick one," e.g. thread 0 or round-robin — not yet decided which).
- Whether/when to add the deferred smarter placement (redirect a whole
  op to a better node before pulling any datums) — noted as future
  work, not needed to prove the core mechanism.
