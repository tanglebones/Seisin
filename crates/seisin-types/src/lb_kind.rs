//! The `"lb:{class}"` `IndexKind`: a storage-backed board (two
//! `CollectionStore` collections — `rank` and `by_player` — per board,
//! see `lb_cache.rs`), cached compute-side by a bounded `LbCache`. lb
//! boards are primary data (scores under max/min rules exist nowhere
//! else) — `apply` (the framework-diff rail) is rejected; all writes
//! arrive as `execute` ops.

use seisin_core::datum::DatumId;
use seisin_node::index_handler::{IndexKind, IndexKindRegistry, ResidentIndex};
use seisin_protocol::LbEntry;

use crate::lb::{lb_kind_name, LbClassDef};
use crate::lb_cache::{LbCache, LbCacheConfig, LB_REPLICATION};

pub(crate) fn composite_key(rank_key: &[u8; 8], player_id: DatumId) -> [u8; 24] {
  let mut key = [0u8; 24];
  key[0..8].copy_from_slice(rank_key);
  key[8..24].copy_from_slice(&player_id.as_bytes());
  key
}

/// Value layout: u16 LE actual display length ++ display bytes ++ zero
/// padding to the fixed width — a length prefix rather than trailing-
/// zero trimming, so displays round-trip exactly.
pub(crate) fn encode_display(display: &[u8], display_len: u16) -> Vec<u8> {
  let capped = &display[..display.len().min(display_len as usize)];
  let mut value = vec![0u8; 2 + display_len as usize];
  value[0..2].copy_from_slice(&(capped.len() as u16).to_le_bytes());
  value[2..2 + capped.len()].copy_from_slice(capped);
  value
}

pub(crate) fn decode_display(value: &[u8]) -> Vec<u8> {
  let len = u16::from_le_bytes(value[0..2].try_into().unwrap()) as usize;
  value[2..2 + len.min(value.len() - 2)].to_vec()
}

pub(crate) fn entry_from(key: &[u8], value: &[u8]) -> LbEntry {
  LbEntry {
    rank_key: key[0..8].try_into().unwrap(),
    player_id: DatumId::from_bytes(key[8..24].try_into().unwrap()),
    display: decode_display(value),
  }
}

pub struct LbIndexKind {
  def: LbClassDef,
  cache_config: Box<dyn Fn(DatumId) -> LbCacheConfig + Send + Sync>,
  collection_store:
    std::sync::OnceLock<std::sync::Arc<dyn seisin_node::collection_store::CollectionStore>>,
}

impl IndexKind for LbIndexKind {
  fn open(
    &self,
    target: DatumId,
    _stored: Option<Vec<u8>>,
  ) -> Result<Box<dyn ResidentIndex>, String> {
    let store = self
      .collection_store
      .get()
      .cloned()
      .ok_or_else(|| "lb: collection store not attached before first access".to_string())?;
    let config = (self.cache_config)(target);
    Ok(Box::new(LbCache::new(
      self.def.clone(),
      store,
      target,
      LB_REPLICATION,
      config,
    )))
  }

  fn attach_collection_store(
    &self,
    store: std::sync::Arc<dyn seisin_node::collection_store::CollectionStore>,
  ) {
    let _ = self.collection_store.set(store); // idempotent: a repeat attach is ignored, not an error
  }
}

/// Registers one leaderboard class under kind `lb:{name}` — call once
/// at the composition root per class. `cache_config` resolves each
/// specific board's cache sizing the first time this compute node opens
/// it (the set of actual boards isn't fixed, so this can't be a static
/// table) — see the design doc's "Per-board cache configuration"
/// section.
pub fn register_lb_class(
  registry: &mut IndexKindRegistry,
  def: LbClassDef,
  cache_config: impl Fn(DatumId) -> LbCacheConfig + Send + Sync + 'static,
) {
  let kind = lb_kind_name(&def.name);
  registry.register(
    kind,
    Box::new(LbIndexKind {
      def,
      cache_config: Box::new(cache_config),
      collection_store: std::sync::OnceLock::new(),
    }),
  );
}

#[cfg(test)]
mod tests {
  use std::collections::HashMap;
  use std::sync::{Arc, Mutex};

  use super::*;
  use crate::field::FieldValue;
  use crate::lb::{encode_score, LbRule, LbScoreType};
  use seisin_protocol::{
    decode_lb_result, encode_lb_execute_op, encode_lb_query_req, LbExecuteOp, LbQueryReq, LbResult,
  };

  /// An in-process ordered-collection fake: a plain sorted Vec per
  /// collection, linear scans throughout. Only for LbCache's own unit
  /// tests — the real storage-side path is exercised for real in
  /// `store_server.rs`'s collection test and the lb integration test.
  type FakeCollection = Vec<(Vec<u8>, Vec<u8>)>;

  #[derive(Default)]
  struct FakeCollectionStore {
    collections: Mutex<HashMap<DatumId, FakeCollection>>,
  }

  impl seisin_node::collection_store::CollectionStore for FakeCollectionStore {
    fn create(&self, collection_id: DatumId, _key_size: u32, _value_size: u32, _n: u16) {
      self
        .collections
        .lock()
        .unwrap()
        .entry(collection_id)
        .or_default();
    }
    fn insert(&self, collection_id: DatumId, key: Vec<u8>, value: Vec<u8>, _n: u16) {
      let mut collections = self.collections.lock().unwrap();
      let entries = collections.entry(collection_id).or_default();
      entries.retain(|(k, _)| k != &key);
      entries.push((key, value));
      entries.sort_by(|a, b| a.0.cmp(&b.0));
    }
    fn remove(&self, collection_id: DatumId, key: Vec<u8>, _n: u16) {
      if let Some(entries) = self.collections.lock().unwrap().get_mut(&collection_id) {
        entries.retain(|(k, _)| k != &key);
      }
    }
    fn get(&self, collection_id: DatumId, key: Vec<u8>, _n: u16) -> Option<Vec<u8>> {
      self
        .collections
        .lock()
        .unwrap()
        .get(&collection_id)?
        .iter()
        .find(|(k, _)| k == &key)
        .map(|(_, v)| v.clone())
    }
    fn scan_forward(&self, collection_id: DatumId, limit: u32, _n: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
      let collections = self.collections.lock().unwrap();
      let entries = collections.get(&collection_id).cloned().unwrap_or_default();
      entries.into_iter().rev().take(limit as usize).collect()
    }
    fn scan_backward(
      &self,
      collection_id: DatumId,
      limit: u32,
      _n: u16,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
      let collections = self.collections.lock().unwrap();
      let entries = collections.get(&collection_id).cloned().unwrap_or_default();
      entries.into_iter().take(limit as usize).collect()
    }
    fn sample(&self, collection_id: DatumId, k: u32, n: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
      self.scan_forward(collection_id, k, n)
    }
    fn rank_of_key(&self, collection_id: DatumId, key: Vec<u8>, _n: u16) -> Option<u64> {
      let collections = self.collections.lock().unwrap();
      collections
        .get(&collection_id)?
        .iter()
        .position(|(k, _)| k == &key)
        .map(|p| p as u64)
    }
    fn scan_from_rank(
      &self,
      collection_id: DatumId,
      rank: u64,
      limit: u32,
      _n: u16,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
      let collections = self.collections.lock().unwrap();
      let entries = collections.get(&collection_id).cloned().unwrap_or_default();
      entries
        .into_iter()
        .skip(rank as usize)
        .take(limit as usize)
        .collect()
    }
    fn count(&self, collection_id: DatumId, _n: u16) -> u64 {
      self
        .collections
        .lock()
        .unwrap()
        .get(&collection_id)
        .map(|e| e.len() as u64)
        .unwrap_or(0)
    }
  }

  fn racing(rule: LbRule) -> LbClassDef {
    LbClassDef {
      name: "racing".to_string(),
      score_type: LbScoreType::I64,
      display_len: 16,
      rule,
    }
  }

  fn generous_config(_board_id: DatumId) -> crate::lb_cache::LbCacheConfig {
    crate::lb_cache::LbCacheConfig {
      pinned_top: 20,
      pinned_bottom: 20,
      max_cached_entries: 200,
    }
  }

  /// Registers one class over a fresh `FakeCollectionStore` and returns
  /// the registry (so a test can `open` the same or a different board
  /// id against it) plus the store, kept alive so a second `open` call
  /// can share it (proving state lives in storage, not the resident
  /// object).
  fn registry_with_class(rule: LbRule) -> (IndexKindRegistry, Arc<FakeCollectionStore>) {
    let mut registry = IndexKindRegistry::new();
    register_lb_class(&mut registry, racing(rule), generous_config);
    let store = Arc::new(FakeCollectionStore::default());
    registry.attach_collection_store(
      Arc::clone(&store) as Arc<dyn seisin_node::collection_store::CollectionStore>
    );
    (registry, store)
  }

  fn open_board(rule: LbRule) -> Box<dyn ResidentIndex> {
    let (registry, _store) = registry_with_class(rule);
    registry
      .get("lb:racing")
      .unwrap()
      .open(DatumId::new(), None)
      .unwrap()
  }

  fn update(
    board: &mut dyn ResidentIndex,
    player: DatumId,
    display: &str,
    score: i64,
    friends: Vec<DatumId>,
  ) -> LbResult {
    let rank_key = encode_score(&racing(LbRule::Max), &FieldValue::I64(score)).unwrap();
    let payload = encode_lb_execute_op(&LbExecuteOp::Update {
      player_id: player,
      display: display.as_bytes().to_vec(),
      rank_key,
      friend_ids: friends,
      top: 10,
      window: 3,
    });
    decode_lb_result(&board.execute(&payload).unwrap()).unwrap()
  }

  #[test]
  fn a_fresh_update_inserts_and_reports_rank_zero_of_one() {
    let mut board = open_board(LbRule::Max);
    let alice = DatumId::new();
    let result = update(board.as_mut(), alice, "Alice", 100, vec![]);
    assert_eq!(result.total, 1);
    assert_eq!(result.player_rank, Some(0));
    assert_eq!(result.top.len(), 1);
    assert_eq!(result.top[0].player_id, alice);
    assert_eq!(result.top[0].display, b"Alice".to_vec());
  }

  #[test]
  fn max_rule_keeps_the_better_score_and_replaces_a_worse_one() {
    let mut board = open_board(LbRule::Max);
    let alice = DatumId::new();
    update(board.as_mut(), alice, "Alice", 300, vec![]);
    // A worse score changes nothing.
    let result = update(board.as_mut(), alice, "Alice", 200, vec![]);
    assert_eq!(result.total, 1);
    assert_eq!(
      crate::lb::decode_rank_key(&LbScoreType::I64, result.top[0].rank_key),
      FieldValue::I64(300)
    );
    // A better score replaces (and does not duplicate).
    let result = update(board.as_mut(), alice, "Alice", 400, vec![]);
    assert_eq!(result.total, 1);
    assert_eq!(
      crate::lb::decode_rank_key(&LbScoreType::I64, result.top[0].rank_key),
      FieldValue::I64(400)
    );
  }

  #[test]
  fn min_rule_inverts_and_replace_rule_always_wins() {
    let mut board = open_board(LbRule::Min);
    let alice = DatumId::new();
    update(board.as_mut(), alice, "Alice", 300, vec![]);
    let result = update(board.as_mut(), alice, "Alice", 200, vec![]); // better for Min
    assert_eq!(
      crate::lb::decode_rank_key(&LbScoreType::I64, result.top[0].rank_key),
      FieldValue::I64(200)
    );

    let mut board2 = open_board(LbRule::Replace);
    let bob = DatumId::new();
    update(board2.as_mut(), bob, "Bob", 300, vec![]);
    let result = update(board2.as_mut(), bob, "Bob", 100, vec![]); // worse, still wins
    assert_eq!(
      crate::lb::decode_rank_key(&LbScoreType::I64, result.top[0].rank_key),
      FieldValue::I64(100)
    );
  }

  #[test]
  fn ranks_top_order_and_friend_ranks_are_best_first() {
    let mut board = open_board(LbRule::Max);
    let (a, b, c) = (DatumId::new(), DatumId::new(), DatumId::new());
    update(board.as_mut(), a, "A", 100, vec![]);
    update(board.as_mut(), b, "B", 300, vec![]);
    let result = update(board.as_mut(), c, "C", 200, vec![a, b, DatumId::new()]);
    assert_eq!(result.total, 3);
    assert_eq!(result.player_rank, Some(1)); // c is second-best
    let top_ids: Vec<DatumId> = result.top.iter().map(|e| e.player_id).collect();
    assert_eq!(top_ids, vec![b, c, a]);
    // Friends: a at rank 2, b at rank 0; the unknown id omitted.
    assert_eq!(result.friends.len(), 2);
    let find = |id: DatumId| result.friends.iter().find(|f| f.player_id == id).unwrap();
    assert_eq!(find(a).rank, 2);
    assert_eq!(find(b).rank, 0);
    assert_eq!(find(b).display, b"B".to_vec());
  }

  #[test]
  fn around_window_centers_on_the_player_in_best_order() {
    let mut board = open_board(LbRule::Max);
    let players: Vec<DatumId> = (0..7).map(|_| DatumId::new()).collect();
    for (i, p) in players.iter().enumerate() {
      update(
        board.as_mut(),
        *p,
        &format!("P{i}"),
        (i as i64 + 1) * 10,
        vec![],
      );
    }
    // players[3] (score 40) has best-rank 3; window 3 => ranks 2,3,4.
    let result = update(board.as_mut(), players[3], "P3", 40, vec![]);
    assert_eq!(result.player_rank, Some(3));
    let around_ids: Vec<DatumId> = result.around.iter().map(|e| e.player_id).collect();
    assert_eq!(around_ids, vec![players[4], players[3], players[2]]);
  }

  #[test]
  fn remove_deletes_the_entry_and_query_reflects_it() {
    let mut board = open_board(LbRule::Max);
    let (a, b) = (DatumId::new(), DatumId::new());
    update(board.as_mut(), a, "A", 100, vec![]);
    update(board.as_mut(), b, "B", 200, vec![]);
    let payload = encode_lb_execute_op(&LbExecuteOp::Remove { player_id: a });
    let result = decode_lb_result(&board.execute(&payload).unwrap()).unwrap();
    assert_eq!(result.total, 1);

    let query = encode_lb_query_req(&LbQueryReq {
      top: 10,
      bottom: 10,
      around_player: Some(b),
      window: 1,
      friend_ids: vec![a],
    });
    let result = decode_lb_result(&board.query(&query).unwrap()).unwrap();
    assert_eq!(result.total, 1);
    assert_eq!(result.top.len(), 1);
    assert_eq!(result.bottom.len(), 1);
    assert_eq!(result.player_rank, Some(0));
    assert!(result.friends.is_empty()); // a is gone
  }

  #[test]
  fn display_is_truncated_to_the_class_width_and_round_trips_exactly() {
    let mut board = open_board(LbRule::Max);
    let alice = DatumId::new();
    let long = "AVeryLongDisplayNameIndeed"; // 26 bytes > display_len 16
    let result = update(board.as_mut(), alice, long, 100, vec![]);
    assert_eq!(result.top[0].display, long.as_bytes()[..16].to_vec());
  }

  #[test]
  fn cold_reopen_reads_current_state_back_from_storage() {
    // Two separate resident LbCache instances over the same board id
    // and the same backing store: state lives in storage now, not in
    // the resident object, so a fresh `open` sees whatever the first
    // one wrote — no local file, no rebuild step needed.
    let (registry, _store) = registry_with_class(LbRule::Max);
    let target = DatumId::new();
    let kind = registry.get("lb:racing").unwrap();
    let alice = DatumId::new();
    {
      let mut board = kind.open(target, None).unwrap();
      update(board.as_mut(), alice, "Alice", 300, vec![]);
    }
    let mut board = kind.open(target, None).unwrap();
    // A worse score under Max is still rejected — the by_player rank
    // key came from storage, not a fresh-open default.
    let result = update(board.as_mut(), alice, "Alice", 100, vec![]);
    assert_eq!(result.total, 1);
    assert_eq!(
      crate::lb::decode_rank_key(&LbScoreType::I64, result.top[0].rank_key),
      FieldValue::I64(300)
    );
  }

  #[test]
  fn apply_is_rejected_and_malformed_execute_is_an_error_not_a_panic() {
    let mut board = open_board(LbRule::Max);
    assert!(board.apply(b"anything").violation.is_some());
    assert!(board.execute(&[0xFF, 0xFF]).is_err());
  }
}
