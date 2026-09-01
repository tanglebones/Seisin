//! `LbCache`: the compute-side bounded cache in front of a
//! storage-backed lb board (`docs/superpowers/specs/
//! 2026-09-01-lb-storage-backed-cache-design.md`). Pinned top/bottom
//! windows plus an LRU for everything else fetched via point/around-
//! player/friend queries — middle entries are evicted first because
//! the LRU never holds a pinned entry in the first place.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use seisin_core::datum::DatumId;
use seisin_node::collection_store::CollectionStore;
use seisin_node::index_handler::{IndexApplyOutcome, ResidentIndex, WriteThrough};
use seisin_protocol::LbEntry;

pub struct LbCacheConfig {
  pub pinned_top: usize,
  pub pinned_bottom: usize,
  /// Total entries the cache may hold, pinned windows included — the
  /// LRU's own capacity is `max_cached_entries.saturating_sub(pinned_top
  /// + pinned_bottom)`.
  pub max_cached_entries: usize,
}

/// A tiny manual LRU (linear-scan eviction) — deliberately not pulling
/// in an `lru` crate dependency for what's expected to be a small
/// (tens-to-low-hundreds of entries) cache per board.
pub(crate) struct LruMiddle {
  entries: HashMap<DatumId, (LbEntry, u64)>,
  next_seq: u64,
  cap: usize,
}

impl LruMiddle {
  pub(crate) fn new(cap: usize) -> Self {
    Self {
      entries: HashMap::new(),
      next_seq: 0,
      cap,
    }
  }

  pub(crate) fn touch(&mut self, entry: LbEntry) {
    let seq = self.next_seq;
    self.next_seq += 1;
    self.entries.insert(entry.player_id, (entry, seq));
    if self.cap == 0 {
      self.entries.clear(); // a zero-capacity LRU caches nothing
    } else if self.entries.len() > self.cap {
      if let Some(evict) = self
        .entries
        .iter()
        .min_by_key(|(_, (_, seq))| *seq)
        .map(|(id, _)| *id)
      {
        self.entries.remove(&evict);
      }
    }
  }

  pub(crate) fn get(&mut self, player_id: DatumId) -> Option<LbEntry> {
    let seq = self.next_seq;
    self.next_seq += 1;
    let (entry, s) = self.entries.get_mut(&player_id)?;
    *s = seq;
    Some(entry.clone())
  }

  pub(crate) fn remove(&mut self, player_id: DatumId) {
    self.entries.remove(&player_id);
  }
}

pub(crate) struct LbCacheState {
  pub(crate) pinned_top: Option<Vec<LbEntry>>,
  pub(crate) pinned_bottom: Option<Vec<LbEntry>>,
  pub(crate) total: Option<u64>,
  pub(crate) middle: LruMiddle,
}

pub struct LbCache {
  pub(crate) def: crate::lb::LbClassDef,
  pub(crate) store: Arc<dyn CollectionStore>,
  pub(crate) rank_id: DatumId,
  pub(crate) by_player_id: DatumId,
  pub(crate) replication: u16,
  pub(crate) config: LbCacheConfig,
  /// `None` in `pinned_top`/`pinned_bottom` means "needs a storage
  /// refresh before the next read that needs it" — writes invalidate
  /// broadly rather than patching the pinned windows in place (a
  /// correct, simple v1; incremental in-place patching on write is a
  /// natural later optimization, not required for correctness). Wrapped
  /// in a `RefCell` for the same reason `LbResidentBoard::tree` was:
  /// `ResidentIndex::query` takes `&self` but cache population needs
  /// `&mut` — single-threaded by construction (one board, one owning
  /// thread).
  pub(crate) state: RefCell<LbCacheState>,
}

/// Board replication factor — fixed for now (matches
/// `cluster_test_node`'s hardcoded `REPL`); no per-board configuration
/// surface yet, per the design doc.
pub(crate) const LB_REPLICATION: u16 = 2;

impl LbCache {
  pub(crate) fn new(
    def: crate::lb::LbClassDef,
    store: Arc<dyn CollectionStore>,
    board_id: DatumId,
    replication: u16,
    config: LbCacheConfig,
  ) -> Self {
    let rank_id = board_id;
    let by_player_id = DatumId::from_name(&board_id, b"by_player");
    let value_size = 2 + def.display_len as u32;
    store.create(rank_id, 24, value_size, replication);
    store.create(by_player_id, 16, 8, replication);
    let middle_cap = config
      .max_cached_entries
      .saturating_sub(config.pinned_top + config.pinned_bottom);
    Self {
      def,
      store,
      rank_id,
      by_player_id,
      replication,
      state: RefCell::new(LbCacheState {
        pinned_top: None,
        pinned_bottom: None,
        total: None,
        middle: LruMiddle::new(middle_cap),
      }),
      config,
    }
  }
}

impl LbCache {
  fn ensure_top(&self) -> Vec<LbEntry> {
    let mut state = self.state.borrow_mut();
    if state.pinned_top.is_none() {
      let entries = self
        .store
        .scan_forward(
          self.rank_id,
          self.config.pinned_top as u32,
          self.replication,
        )
        .iter()
        .map(|(k, v)| crate::lb_kind::entry_from(k, v))
        .collect();
      state.pinned_top = Some(entries);
    }
    state.pinned_top.clone().unwrap()
  }

  fn ensure_bottom(&self) -> Vec<LbEntry> {
    let mut state = self.state.borrow_mut();
    if state.pinned_bottom.is_none() {
      let entries = self
        .store
        .scan_backward(
          self.rank_id,
          self.config.pinned_bottom as u32,
          self.replication,
        )
        .iter()
        .map(|(k, v)| crate::lb_kind::entry_from(k, v))
        .collect();
      state.pinned_bottom = Some(entries);
    }
    state.pinned_bottom.clone().unwrap()
  }

  fn ensure_total(&self) -> u64 {
    let mut state = self.state.borrow_mut();
    if state.total.is_none() {
      state.total = Some(self.store.count(self.rank_id, self.replication));
    }
    state.total.unwrap()
  }

  /// A board-write of any kind (Update/Remove) invalidates the pinned
  /// windows and the running total — the next read that needs them
  /// re-fetches from storage. Simple and correct; not the tightest
  /// possible (an update outside both windows doesn't actually need to
  /// invalidate either), left as a documented future refinement rather
  /// than complicating v1.
  fn invalidate(&self) {
    let mut state = self.state.borrow_mut();
    state.pinned_top = None;
    state.pinned_bottom = None;
    state.total = None;
  }

  fn apply_rule(&self, old_key: &[u8; 8], new_key: &[u8; 8]) -> bool {
    // Raw byte comparison is valid: rank-key encoding is order-
    // preserving, so byte order == numeric order. A byte-equal key is
    // a no-op for every rule — same score, nothing moves.
    match self.def.rule {
      crate::lb::LbRule::Max => new_key > old_key,
      crate::lb::LbRule::Min => new_key < old_key,
      crate::lb::LbRule::Replace => new_key != old_key,
    }
  }

  /// Looks up a player's current `rank_key`: storage `by_player` lookup
  /// (the LRU only holds full `LbEntry`s keyed by the `rank`
  /// collection, not this point index, so there's no cache layer to
  /// check here — the `by_player` collection is a small point index and
  /// every lookup is already O(1) storage-side).
  fn player_rank_key(&self, player_id: DatumId) -> Option<[u8; 8]> {
    self
      .store
      .get(
        self.by_player_id,
        player_id.as_bytes().to_vec(),
        self.replication,
      )
      .map(|v| v.try_into().unwrap())
  }

  /// Best-first entries window ±`half` around `player_id`'s current
  /// rank, plus that player's own best-first rank — `None` if the
  /// player isn't on the board. Ports `LbResidentBoard::assemble`'s
  /// "around" computation onto `rank_of_key`/`scan_from_rank`.
  fn around(&self, player_id: DatumId, window: u32) -> Result<Option<(u64, Vec<LbEntry>)>, String> {
    let Some(rank_key) = self.player_rank_key(player_id) else {
      return Ok(None);
    };
    let total = self.ensure_total();
    let key = crate::lb_kind::composite_key(&rank_key, player_id);
    let asc = self
      .store
      .rank_of_key(self.rank_id, key.to_vec(), self.replication)
      .ok_or_else(|| "board map/collection divergence: mapped key missing".to_string())?;
    let best_rank = total - 1 - asc;
    if window == 0 {
      return Ok(Some((best_rank, Vec::new())));
    }
    let half = (window / 2) as u64;
    let best_start = best_rank.saturating_sub(half);
    let best_end = (best_start + window as u64).min(total);
    let best_start = best_end.saturating_sub(window as u64);
    let asc_start = total - best_end;
    let mut entries: Vec<LbEntry> = self
      .store
      .scan_from_rank(
        self.rank_id,
        asc_start,
        (best_end - best_start) as u32,
        self.replication,
      )
      .iter()
      .map(|(k, v)| crate::lb_kind::entry_from(k, v))
      .collect();
    entries.reverse();
    let mut state = self.state.borrow_mut();
    for entry in &entries {
      state.middle.touch(entry.clone());
    }
    Ok(Some((best_rank, entries)))
  }

  fn friend_ranks(
    &self,
    friend_ids: &[DatumId],
  ) -> Result<Vec<seisin_protocol::LbFriendRank>, String> {
    let total = self.ensure_total();
    let mut friends = Vec::new();
    for friend_id in friend_ids {
      let cached = self.state.borrow_mut().middle.get(*friend_id);
      if let Some(cached) = cached {
        let asc = self
          .store
          .rank_of_key(
            self.rank_id,
            crate::lb_kind::composite_key(&cached.rank_key, *friend_id).to_vec(),
            self.replication,
          )
          .ok_or_else(|| "board map/collection divergence: cached key missing".to_string())?;
        friends.push(seisin_protocol::LbFriendRank {
          player_id: *friend_id,
          rank: total - 1 - asc,
          rank_key: cached.rank_key,
          display: cached.display,
        });
        continue;
      }
      let Some(rank_key) = self.player_rank_key(*friend_id) else {
        continue; // not on this board — omitted per the design doc
      };
      let key = crate::lb_kind::composite_key(&rank_key, *friend_id);
      let asc = self
        .store
        .rank_of_key(self.rank_id, key.to_vec(), self.replication)
        .ok_or_else(|| "board map/collection divergence: mapped key missing".to_string())?;
      let (k, v) = self
        .store
        .scan_from_rank(self.rank_id, asc, 1, self.replication)
        .into_iter()
        .next()
        .ok_or_else(|| "board map/collection divergence: rank scan empty".to_string())?;
      let entry = crate::lb_kind::entry_from(&k, &v);
      self.state.borrow_mut().middle.touch(entry.clone());
      friends.push(seisin_protocol::LbFriendRank {
        player_id: *friend_id,
        rank: total - 1 - asc,
        rank_key,
        display: entry.display,
      });
    }
    Ok(friends)
  }
}

impl ResidentIndex for LbCache {
  fn apply(&mut self, _payload: &[u8]) -> IndexApplyOutcome {
    IndexApplyOutcome {
      violation: Some(
        "lb boards are maintained via execute ops, not framework index updates".to_string(),
      ),
      write_through: WriteThrough::None,
    }
  }

  fn query(&self, query: &[u8]) -> Result<Vec<u8>, String> {
    let seisin_protocol::LbQueryReq {
      top,
      bottom,
      around_player,
      window,
      friend_ids,
    } = seisin_protocol::decode_lb_query_req(query).map_err(|e| e.to_string())?;
    // v1 limitation: a query for more than this board's pinned window
    // returns only what's pinned — growing the window on demand is a
    // straightforward follow-up, not done here.
    let top_entries = self.ensure_top();
    let top_entries = top_entries[..(top as usize).min(top_entries.len())].to_vec();
    let bottom_entries = self.ensure_bottom();
    let bottom_entries = bottom_entries[..(bottom as usize).min(bottom_entries.len())].to_vec();
    let total = self.ensure_total();
    let (player_rank, around_entries) = match around_player {
      Some(player_id) => match self.around(player_id, window)? {
        Some((rank, entries)) => (Some(rank), entries),
        None => (None, Vec::new()),
      },
      None => (None, Vec::new()),
    };
    let friends = self.friend_ranks(&friend_ids)?;
    let result = seisin_protocol::LbResult {
      total,
      player_rank,
      top: top_entries,
      bottom: bottom_entries,
      around: around_entries,
      friends,
    };
    Ok(seisin_protocol::encode_lb_result(&result))
  }

  fn execute(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
    match seisin_protocol::decode_lb_execute_op(payload).map_err(|e| e.to_string())? {
      seisin_protocol::LbExecuteOp::Update {
        player_id,
        display,
        rank_key,
        friend_ids,
        top,
        window,
      } => {
        let old_key = self.player_rank_key(player_id);
        let replace = match old_key {
          None => true,
          Some(old_key) => self.apply_rule(&old_key, &rank_key),
        };
        if replace {
          if let Some(old_key) = old_key {
            self.store.remove(
              self.rank_id,
              crate::lb_kind::composite_key(&old_key, player_id).to_vec(),
              self.replication,
            );
          }
          self.store.insert(
            self.rank_id,
            crate::lb_kind::composite_key(&rank_key, player_id).to_vec(),
            crate::lb_kind::encode_display(&display, self.def.display_len),
            self.replication,
          );
          self.store.insert(
            self.by_player_id,
            player_id.as_bytes().to_vec(),
            rank_key.to_vec(),
            self.replication,
          );
          self.invalidate();
          self.state.borrow_mut().middle.remove(player_id); // stale if it was cached under the old key
        }
        let query = seisin_protocol::LbQueryReq {
          top,
          bottom: 0,
          around_player: Some(player_id),
          window,
          friend_ids,
        };
        self.query(&seisin_protocol::encode_lb_query_req(&query))
      }
      seisin_protocol::LbExecuteOp::Remove { player_id } => {
        if let Some(old_key) = self.player_rank_key(player_id) {
          self.store.remove(
            self.rank_id,
            crate::lb_kind::composite_key(&old_key, player_id).to_vec(),
            self.replication,
          );
          self.store.remove(
            self.by_player_id,
            player_id.as_bytes().to_vec(),
            self.replication,
          );
          self.invalidate();
          self.state.borrow_mut().middle.remove(player_id);
        }
        let query = seisin_protocol::LbQueryReq {
          top: 0,
          bottom: 0,
          around_player: None,
          window: 0,
          friend_ids: vec![],
        };
        self.query(&seisin_protocol::encode_lb_query_req(&query))
      }
    }
  }
}
