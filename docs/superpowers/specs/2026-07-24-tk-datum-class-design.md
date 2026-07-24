# tk (Bitemporal Valid-Time) Datum Class — Design

Date: 2026-07-24

## Overview & Goals

tk stores the valid-time history of one time-varying attribute per
entity: non-overlapping, half-open `[lower, upper)` ranged values,
corrected via split-and-insert rather than in-place edit (the
`_t_` pattern from the database guideline — valid time only;
transaction-time audit is a separate, system-wide concern, per that
guideline's insistence that the two time axes stay independent).

This is Datum Type System Part 4, revised from the original tk section
of `2026-07-21-datum-type-system-design.md` in two ways settled since
that document was written:

1. **Classification.** tk is *decomposed field storage* — primary
   data, not a derived index. A tk value exists nowhere else, so it is
   not rebuildable from any scan, and every index-grade durability
   relaxation (no-fsync-before-ack, rebuild-from-scan) is off the
   table in principle. Datum-grade durability class, like lb.
2. **Write model.** tk rides the `execute`/`query` rail built for lb
   (`ResidentIndex::execute`, `WorkerPool::run_index_execute`), not
   `TypedOpContext`'s field-diffing. Corrections need an explicit
   `as_of` (backdating is the point of the correction-upsert), which
   diffing cannot express — and a standalone class avoids teaching
   `encode_datum`/`decode_datum` to strip 6NF-decomposed fields from
   datum content. `TypedOpContext` sugar over tk (auto-maintaining a
   declared field's history) is a possible later layer, not v1.

## Class Declaration, Identity, Files

```rust
pub struct TkClassDef {
  pub name: String,
  pub value_type: FieldType,  // any FieldType — values are schema-encoded
  pub value_width: u16,       // hard cap on the encoded value, bytes
}
```

Registered one-kind-per-class as registry kind `tk:{name}` via
`register_tk_class(registry, def, data_dir, clock)` — the same
pattern as lb, and for the same reason: `IndexKind::open` receives
only a `DatumId` and learns the class from the registered kind.

Entity identity: the tk datum id is derived from
`tk:{class}:{entity-id-hex}` where the entity id is the owning pk
`DatumId` (hex because the id is bytes, not text — same derivation
namespace as sk/rk/lb keys). One tk datum per (class, entity); entities
distribute across threads/nodes by ordinary ring placement, so there is
no cross-entity contention and no global bottleneck. Files:
`{data_dir}/tk_<tk-datum-id-hex>.btree`.

**`value_width` rejects, never truncates.** tk is primary data — an
encoded value longer than the class cap fails the write with an
explicit error. TOAST-style inline+overflow storage (already a named
Storage Tier requirement, see the lb design doc) lifts the cap later.

**File-per-entity count is a documented Storage Tier concern.** One
small page file per (class, entity) is correct but profligate at
millions of entities. Consolidating many entities into shared files
requires placement-aware storage — exactly Storage Tier's job. The two
obvious v1 alternatives were rejected as architecturally broken, not
merely deferred: segment-blobs-as-datums would hash segment ids to
*other* threads/nodes (the owning thread cannot write foreign datums),
and one shared per-class file would be written concurrently by every
thread/node that owns any of the class's entities.

## Entry Layout & Residency (the "thunked range" model)

Per entity, a counted B+Tree (`seisin-storage`):

- **Key (8 bytes):** the range's `lower` timestamp — i64 milliseconds
  since the Unix epoch, encoded with the existing order-preserving
  sign-flip big-endian transform (pre-1970 backdates order correctly).
- **Value (fixed `1 + 8 + 2 + value_width` bytes):**
  `upper_flag (0 = open-ended, 1 = bounded) ++ upper (8, same
  transform; zeroes when open) ++ value_len (u16 LE) ++ encoded value
  bytes ++ zero padding`. Value bytes are the schema-encoded
  `FieldValue` for the class's declared `value_type` (the existing
  tagless `encode_field_value`/`decode_field_value`).

Non-overlap is an invariant maintained by the correction-upsert (no
engine-level exclusion constraint exists or is needed — the owning
thread is the sole writer and every write goes through the upsert).

**Residency is the open file handle, nothing more.** No wholesale
materialization of a history, ever: every op is O(log n) page reads
through the tree, and the OS page cache is the v1 caching layer. This
is the lazily-loaded-range-segments model with the B+Tree's own pages
as the segments — long histories cost only the pages a query actually
touches.

## Engine Addition

One new `seisin-storage` primitive:

- `BPlusTree::rank_of_floor(&mut self, key: &[u8]) -> Result<Option<u64>>`
  — the 0-based ascending rank of the greatest key `<=` the probe
  (`None` if every key is greater). A counted descent like
  `rank_of_key`; at the leaf, `binary_search`'s `Err(i)` case steps
  back one (to `passed + i - 1`, or `None` when `passed + i == 0`).

Everything else composes from existing primitives: covering-range
lookup = floor + bounds check; `Range{from,to}` = floor rank +
`scan_from_rank` walk; `Current` = `scan_backward_bounded(1)` +
open-ended check.

## Timestamps

`as_of` is `Option<i64>` on every mutating op: `Some` for explicit
(including backdated) times, `None` for "now", which the server stamps
via an injected wall clock. gossip's `ClockSource` is
monotonic-`Instant`-based — the wrong tool for epoch millis — so
`seisin-types` gains a minimal trait:

```rust
pub trait WallClock: Send + Sync {
  fn now_millis(&self) -> i64;
}
pub struct SystemWallClock;   // SystemTime-based
// tests inject a fixed/advancing fake
```

`TkIndexKind` holds `Arc<dyn WallClock>`, supplied at registration.

## Ops (execute) & Queries (query)

`apply` is rejected, exactly like lb — tk is not on the framework-diff
rail.

**`Set { as_of: Option<i64>, value: Vec<u8> }`** (value = pre-encoded
`FieldValue` bytes; the paired client encodes via the class's
`value_type`): resolve `as_of`; validate the value decodes as
`value_type` and fits `value_width`; then the correction-upsert:

1. Floor-lookup at `as_of`.
2. If a range covers `as_of` (`lower <= as_of` and open-ended or
   `upper > as_of`):
   - `lower == as_of`: overwrite that entry's value in place (same
     instant, same bounds — a value correction, not a split).
   - else: close it (`upper = as_of`, an upsert on its own key) and
     insert the new entry `[as_of, old_upper)` — inheriting the closed
     range's old upper keeps non-overlap by construction.
3. If nothing covers `as_of` (a gap, or before the first entry): the
   new entry's upper is the *next* entry's lower (the entry at
   floor-rank + 1, or rank 0 when there is no floor), or open-ended if
   none — a gap-fill never overlaps its successor.

Returns the resulting span.

**`Clear { as_of: Option<i64> }`**: close the covering range at
`as_of` (no insertion — creates a gap, which this design allows by
default per the guideline's base case; a no-gaps opt-in invariant is
deferred). Clearing inside an existing gap is a no-op. Returns the
closed span, or nothing.

**Queries** (read-only, via the `query` method):
`AsOf(t)` — the covering span, if any; `Current` — the last span if
open-ended, else nothing; `History` — all spans ascending;
`Range { from, to }` — every span overlapping `[from, to)`, ascending.
All return `Vec<TkSpan>`:

```rust
pub struct TkSpan {
  pub lower: i64,
  pub upper: Option<i64>, // None = open-ended
  pub value: Vec<u8>,     // schema-encoded FieldValue bytes
}
```

## Wire Contract

Type-specific pair, the lb precedent exactly:
`Request::TkExecute { entity_datum_id, class, op: TkOp }`,
`Request::TkQuery { entity_datum_id, class, query: TkQueryReq }`,
`Response::TkResult { spans: Vec<TkSpan> }`, with standalone codecs
(`encode_tk_op`/`decode_tk_op`, `encode_tk_query_req`/…,
`encode_tk_result`/…) in `seisin-protocol`, used by both `server.rs`
routing (through the shared `redirect_if_foreign`) and the tk
implementation in `seisin-types`. The `class` field only forms the
registry kind string `tk:{class}`; the framework never interprets tk
semantics. Both requests are client-facing, never carried over a
peer-link.

## Durability Note

Same statement as lb, restated because it is the load-bearing
difference from sk/rk: tk files are primary data. Nothing in the
system fsyncs yet (Storage Tier is unbuilt), so practical durability
equals everything else's today — but when Storage Tier lands, tk files
belong in the datum-grade class (write-through-before-ack), and there
is deliberately no rebuild-from-scan recovery story.

## Testing Strategy

- Engine: `rank_of_floor` known-answer tests — exact hit, between
  keys, before the first key, after the last, on a multi-level tree,
  and after removes (extending the existing model-based pattern).
- tk unit tests: forward set on empty; forward set closing the open
  range; backdated correction splitting a past closed range;
  same-instant overwrite (bounds unchanged); Clear creating a gap and
  `AsOf` inside the gap returning nothing; gap-fill inheriting the
  successor's lower; set before the first entry; value-too-wide and
  wrong-type rejected loudly; `as_of: None` stamped via a fake
  `WallClock`; `Current` vs closed-final-range; `Range` spanning gaps;
  cold-reopen answering from the file; `apply` rejected; malformed
  payloads as errors not panics.
- Codec round-trips for every op/query/result shape, including empty
  span lists and `None` uppers.
- Integration (`integration_tk_history.rs`, lb's bootstrap pattern): a
  real node, two entities of one class on independent files — Set,
  backdated correction, Clear, then AsOf/Current/History/Range over
  the wire, plus the width-cap rejection surfacing as an error
  response. Stress 10x; standing 20x wound-wait/collation suites.

## Deferred, Explicitly

- **TypedOpContext sugar** — auto-maintaining a declared datum-type
  field's tk history from typed writes.
- **Transaction-time audit** — separate axis, separate system-wide
  design (unchanged from the original spec).
- **No-gaps opt-in invariant** — gaps are allowed by default.
- **TOAST overflow for wide values** — Storage Tier; `value_width`
  rejection is the v1 contract.
- **File consolidation / placement wiring** — Storage Tier.
