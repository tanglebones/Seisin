# Index Storage Engine Choice — Research Findings

Deep-research pass (101 agents, 425 tool calls, adversarially verified: 22/25
claims confirmed, 3 refuted) into which on-disk data structure should back
Seisin's index storage engine. Commissioned while designing rk's storage
(Datum Type System Part 3), scoped to that workload specifically: an
order-statistics/rank-augmented ordered index, insert/upsert-only (no
delete), no crash-safety requirement (rebuildable from a full datum scan),
single-writer-per-index, needing cheap top-N/bottom-N bounded scans from
either end plus cheap approximate rank-based sampling of the middle.

**This doc is a durable reference for storage-engine choices across pk, sk,
rk, and tk** — the conclusions below are specific to rk's workload; pk, sk,
and tk each have different access patterns (pk: point lookup by DatumId,
never scanned; sk: value-partitioned entry lists, one datum per distinct
value; tk: bitemporal versioned, per-entity, no cross-entity contention) and
should be evaluated against this same research rather than assumed to need
the same engine rk uses.

## Conclusion for rk

**A counted (order-statistics-augmented) B+Tree is the clear right choice.**
Not a close call — no viable alternative surfaced for this specific
workload (ordered, upsert-only, needs O(log n) rank/select).

- **LSM-tree — ruled out.** Its core mechanism (deferred, asynchronous
  compaction across levels) is precisely what destroys a global, queryable
  rank/position invariant. Even the state-of-the-art LSM range-query
  accelerator, REMIX (Zhong et al., USENIX FAST'21), only accelerates *scan*
  retrieval across sorted runs — it has no treatment of rank, percentile, or
  k-th-order queries at all. A genuinely rank-augmented LSM-tree does not
  appear to exist in the literature; this is an open research gap, not a
  proven or implemented approach worth building toward.
- **Hash/radix — ruled out.** No ordering to exploit for scans or rank —
  optimized purely for point lookups. (Low-confidence/unsourced inference,
  but consistent with why no literature proposes it for this need, and
  consistent with the project's own earlier reasoning for pk placement.)
- **Counted B+Tree / order-statistics tree — recommended.** Textbook-proven
  (CLRS-level result): augmenting a balanced tree with per-subtree entry
  counts enables `Select(i)` (i-th smallest/largest) and `Rank(x)` (position
  of a key) each in O(log n), at no asymptotic cost to normal
  insert/lookup. A real, disk-backed, memory-mapped precedent exists:
  **AELMDB**, an extension of LMDB (a mature, widely-used embedded B+Tree KV
  engine) carrying subtree-count augmentation to realize a
  "range-summarizable order-statistics store" — the closest known
  real-world analog to what rk needs. (Caveat: AELMDB is a 2026 arXiv paper,
  recent and unproven at scale — treat as a strong architectural precedent,
  not a battle-tested one; no independent benchmarks were found.)
- **Succinct/rank-select structures (wavelet trees, Fenwick/BIT trees) —
  supplementary building-block only, not a primary recommendation.** These
  support fast rank/select (even O(log σ) range-quantile queries) in the
  literature, but essentially all surviving verified work targets static,
  in-memory, text/alphabet-indexing use cases — not disk-backed,
  insert/upsert-friendly ordered indexes over numeric keys. Fenwick-style
  per-node prefix counts are a plausible *technique* to borrow inside a
  B+Tree's own internal-node augmentation, not a ready-made engine.
- **Approximate quantile sketches (KLL, t-digest, REQ — as in Apache
  DataSketches) — complementary, not primary.** KLL/REQ/Classic offer
  formal, distribution-independent, tunable error bounds (typically 1-2%
  relative error at 99% confidence) using small bounded memory instead of a
  full sort; t-digest has no such formal guarantee. These answer a
  *different* question (an approximate summary maintained alongside data)
  than an exact, ordered, upsertable index, and would not replace the
  counted B+Tree as rk's primary structure — but could be layered on top
  later for very cheap "roughly the median" answers on huge/fast-changing
  indexes, if that's ever needed.

## What this means for the Index Storage Engine crate

No second backend is being built speculatively — there's nothing concrete
in the literature to build against for this workload. The crate's public
surface (`insert`/`scan_forward_bounded`/`scan_backward_bounded`/
`sample_by_rank`/`rebuild_from`) is the contract a future backend would need
to satisfy if a genuinely different workload ever warranted one, but only
the counted B+Tree is implemented now.

## Open questions surfaced, not yet resolved

- Has anyone published/open-sourced a genuinely rank-augmented LSM-tree (vs.
  REMIX-style scan-only acceleration)? If not, that's a signal to avoid LSM
  for any future rank-needing index, not just rk.
- AELMDB's actual write-amplification/rebalancing cost under a
  fast-changing leaderboard workload is unmeasured in the literature found;
  worth benchmarking Seisin's own implementation once built rather than
  assuming the paper's approach is optimal for our single-writer,
  no-crash-safety-needed case.
- For a fast-changing rk index specifically: does a plain counted B+Tree
  serve adequately, or does high write volume warrant a write-buffering
  front end (an in-memory counted structure periodically flushed/merged
  into the on-disk tree)? Note this shape is effectively a mini-LSM hybrid
  — if pursued, rank queries would need to be answered correctly across the
  buffer + on-disk-tree boundary, reintroducing the exact cross-run
  rank-reconciliation problem this research found LSM-trees handle poorly.
  Not needed for rk's initial implementation; revisit only if a real
  workload demonstrates the plain counted B+Tree is insufficient.

## Sources (selected, quality-rated by the research pass)

- https://en.wikipedia.org/wiki/Order_statistic_tree (secondary)
- https://www.chiark.greenend.org.uk/~sgtatham/algorithms/cbtree.html (blog, but corroborated)
- https://arxiv.org/pdf/2603.19820 — AELMDB / RSOS paper (primary)
- https://arxiv.org/pdf/2010.12734 and https://www.usenix.org/system/files/fast21-zhong.pdf — REMIX (primary, peer-reviewed)
- https://datasketches.apache.org/docs/QuantilesStudies/KllSketchVsTDigest.html (primary, maintained)
- https://datasketches.apache.org/docs/QuantilesAll/QuantilesOverview.html (primary, maintained)
- https://www.databricks.com/blog/approximate-answers-exact-decisions-new-sketch-functions-analytics (corroborating)
- https://cp-algorithms.com/data_structures/fenwick.html (secondary — note: does not itself document a rank/order-statistic technique, only prefix-sum; the fuller claim about Fenwick trees enabling rank queries came from other sources)

Full raw research transcript (agent-by-agent) is not preserved beyond this
session's ephemeral task output; this document is the durable summary going
forward.
