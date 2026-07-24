//! The `"lb:{class}"` `IndexKind`: one counted-B+Tree board file per
//! (class, leaderboard, area-config) datum, resident with a
//! player->rank-key map. lb boards are primary data (scores under
//! max/min rules exist nowhere else) — `apply` (the framework-diff
//! rail) is rejected; all writes arrive as `execute` ops.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;

use seisin_core::datum::DatumId;
use seisin_node::index_handler::{IndexApplyOutcome, IndexKind, IndexKindRegistry, ResidentIndex};
use seisin_protocol::{
  decode_lb_execute_op, decode_lb_query_req, encode_lb_result, LbEntry, LbExecuteOp, LbFriendRank,
  LbQueryReq, LbResult,
};
use seisin_storage::btree::BPlusTree;

use crate::lb::{lb_kind_name, LbClassDef, LbRule};

const LB_PAGE_SIZE: u32 = 4096;

pub struct LbIndexKind {
  def: LbClassDef,
  data_dir: PathBuf,
}

impl LbIndexKind {
  pub fn new(def: LbClassDef, data_dir: PathBuf) -> Self {
    Self { def, data_dir }
  }
}

/// Files are named by the board datum's id — `IndexKind::open` only
/// receives the `DatumId`, which is already the stable, collision-free
/// derivation of `lb:{class}:{leaderboard}:{area_config}`.
fn file_name_for(target: DatumId) -> String {
  let hex: String = target
    .as_bytes()
    .iter()
    .map(|b| format!("{b:02x}"))
    .collect();
  format!("lb_{hex}.btree")
}

fn composite_key(rank_key: &[u8; 8], player_id: DatumId) -> [u8; 24] {
  let mut key = [0u8; 24];
  key[0..8].copy_from_slice(rank_key);
  key[8..24].copy_from_slice(&player_id.as_bytes());
  key
}

/// Value layout: u16 LE actual display length ++ display bytes ++ zero
/// padding to the fixed width — a length prefix rather than trailing-
/// zero trimming, so displays round-trip exactly.
fn encode_display(display: &[u8], display_len: u16) -> Vec<u8> {
  let capped = &display[..display.len().min(display_len as usize)];
  let mut value = vec![0u8; 2 + display_len as usize];
  value[0..2].copy_from_slice(&(capped.len() as u16).to_le_bytes());
  value[2..2 + capped.len()].copy_from_slice(capped);
  value
}

fn decode_display(value: &[u8]) -> Vec<u8> {
  let len = u16::from_le_bytes(value[0..2].try_into().unwrap()) as usize;
  value[2..2 + len.min(value.len() - 2)].to_vec()
}

fn entry_from(key: &[u8], value: &[u8]) -> LbEntry {
  LbEntry {
    rank_key: key[0..8].try_into().unwrap(),
    player_id: DatumId::from_bytes(key[8..24].try_into().unwrap()),
    display: decode_display(value),
  }
}

pub struct LbResidentBoard {
  def: LbClassDef,
  // RefCell for the same reason as rk: `query` takes `&self` while
  // BPlusTree page reads need `&mut`. Single-threaded by construction.
  tree: RefCell<BPlusTree>,
  by_player: HashMap<DatumId, [u8; 8]>,
}

impl LbResidentBoard {
  /// Best-first entries: the tree is ascending, so "top t" is a
  /// backward scan and rank conversions are `total - 1 - ascending`.
  fn assemble(
    &self,
    player_rank_of: Option<DatumId>,
    top: u32,
    bottom: u32,
    window: u32,
    friend_ids: &[DatumId],
  ) -> Result<LbResult, String> {
    let mut tree = self.tree.borrow_mut();
    let total = tree.len() as u64;
    let err = |e: anyhow::Error| e.to_string();

    let top_entries: Vec<LbEntry> = tree
      .scan_backward_bounded(top as usize)
      .map_err(err)?
      .iter()
      .map(|(k, v)| entry_from(k, v))
      .collect();
    let bottom_entries: Vec<LbEntry> = tree
      .scan_forward_bounded(bottom as usize)
      .map_err(err)?
      .iter()
      .map(|(k, v)| entry_from(k, v))
      .collect();

    let mut player_rank = None;
    let mut around = Vec::new();
    if let Some(player_id) = player_rank_of {
      if let Some(rank_key) = self.by_player.get(&player_id) {
        let asc = tree
          .rank_of_key(&composite_key(rank_key, player_id))
          .map_err(err)?
          .ok_or_else(|| "board map/tree divergence: mapped key missing".to_string())?;
        let best_rank = total - 1 - asc;
        player_rank = Some(best_rank);
        if window > 0 {
          // A best-order window centered on the player: compute the
          // ascending range, scan once, reverse to best-first.
          let half = (window / 2) as u64;
          let best_start = best_rank.saturating_sub(half);
          let best_end = (best_start + window as u64).min(total); // exclusive
          let best_start = best_end.saturating_sub(window as u64);
          let asc_start = total - best_end;
          let mut entries: Vec<LbEntry> = tree
            .scan_from_rank(asc_start, (best_end - best_start) as usize)
            .map_err(err)?
            .iter()
            .map(|(k, v)| entry_from(k, v))
            .collect();
          entries.reverse();
          around = entries;
        }
      }
    }

    let mut friends = Vec::new();
    for friend_id in friend_ids {
      let Some(rank_key) = self.by_player.get(friend_id) else {
        continue; // not on this board — omitted per the design doc
      };
      let key = composite_key(rank_key, *friend_id);
      let asc = tree
        .rank_of_key(&key)
        .map_err(err)?
        .ok_or_else(|| "board map/tree divergence: mapped key missing".to_string())?;
      let entry = tree
        .scan_from_rank(asc, 1)
        .map_err(err)?
        .into_iter()
        .next()
        .ok_or_else(|| "board map/tree divergence: rank scan empty".to_string())?;
      friends.push(LbFriendRank {
        player_id: *friend_id,
        rank: total - 1 - asc,
        rank_key: *rank_key,
        display: decode_display(&entry.1),
      });
    }

    Ok(LbResult {
      total,
      player_rank,
      top: top_entries,
      bottom: bottom_entries,
      around,
      friends,
    })
  }

  fn apply_rule(&self, old_key: &[u8; 8], new_key: &[u8; 8]) -> bool {
    // Raw byte comparison is valid: rank-key encoding is order-
    // preserving, so byte order == numeric order. A byte-equal key is
    // a no-op for every rule — same score, nothing moves.
    match self.def.rule {
      LbRule::Max => new_key > old_key,
      LbRule::Min => new_key < old_key,
      LbRule::Replace => new_key != old_key,
    }
  }
}

impl ResidentIndex for LbResidentBoard {
  fn apply(&mut self, _payload: &[u8]) -> IndexApplyOutcome {
    IndexApplyOutcome {
      violation: Some(
        "lb boards are maintained via execute ops, not framework index updates".to_string(),
      ),
      write_through: None,
    }
  }

  fn query(&self, query: &[u8]) -> Result<Vec<u8>, String> {
    let LbQueryReq {
      top,
      bottom,
      around_player,
      window,
      friend_ids,
    } = decode_lb_query_req(query).map_err(|e| e.to_string())?;
    let result = self.assemble(around_player, top, bottom, window, &friend_ids)?;
    Ok(encode_lb_result(&result))
  }

  fn execute(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
    match decode_lb_execute_op(payload).map_err(|e| e.to_string())? {
      LbExecuteOp::Update {
        player_id,
        display,
        rank_key,
        friend_ids,
        top,
        window,
      } => {
        let replace = match self.by_player.get(&player_id) {
          None => true,
          Some(old_key) => self.apply_rule(old_key, &rank_key),
        };
        if replace {
          let mut tree = self.tree.borrow_mut();
          if let Some(old_key) = self.by_player.get(&player_id) {
            tree
              .remove(&composite_key(old_key, player_id))
              .map_err(|e| format!("lb remove of old entry failed: {e}"))?;
          }
          tree
            .insert(
              &composite_key(&rank_key, player_id),
              &encode_display(&display, self.def.display_len),
            )
            .map_err(|e| format!("lb insert failed: {e}"))?;
          drop(tree);
          self.by_player.insert(player_id, rank_key);
        }
        let result = self.assemble(Some(player_id), top, 0, window, &friend_ids)?;
        Ok(encode_lb_result(&result))
      }
      LbExecuteOp::Remove { player_id } => {
        if let Some(old_key) = self.by_player.remove(&player_id) {
          self
            .tree
            .borrow_mut()
            .remove(&composite_key(&old_key, player_id))
            .map_err(|e| format!("lb remove failed: {e}"))?;
        }
        let result = self.assemble(None, 0, 0, 0, &[])?;
        Ok(encode_lb_result(&result))
      }
    }
  }
}

impl IndexKind for LbIndexKind {
  /// `stored` is ignored: lb persists in its own page file. The
  /// player map is rebuilt from a full scan — derivable state, never
  /// persisted.
  fn open(
    &self,
    target: DatumId,
    _stored: Option<Vec<u8>>,
  ) -> Result<Box<dyn ResidentIndex>, String> {
    let path = self.data_dir.join(file_name_for(target));
    let value_size = 2 + self.def.display_len as u32;
    let mut tree = if path.exists() {
      BPlusTree::open(&path)
    } else {
      std::fs::create_dir_all(&self.data_dir)
        .map_err(|e| format!("failed to create lb data dir {:?}: {e}", self.data_dir))?;
      BPlusTree::create(&path, 24, value_size, LB_PAGE_SIZE)
    }
    .map_err(|e| format!("failed to open lb board file {path:?}: {e}"))?;
    let len = tree.len();
    let mut by_player = HashMap::with_capacity(len);
    for (key, _) in tree
      .scan_from_rank(0, len)
      .map_err(|e| format!("failed to scan lb board {path:?}: {e}"))?
    {
      let rank_key: [u8; 8] = key[0..8].try_into().unwrap();
      let player_id = DatumId::from_bytes(key[8..24].try_into().unwrap());
      by_player.insert(player_id, rank_key);
    }
    Ok(Box::new(LbResidentBoard {
      def: self.def.clone(),
      tree: RefCell::new(tree),
      by_player,
    }))
  }
}

/// Registers one leaderboard class under kind `lb:{name}` — call once
/// at the composition root per class.
pub fn register_lb_class(registry: &mut IndexKindRegistry, def: LbClassDef, data_dir: PathBuf) {
  let kind = lb_kind_name(&def.name);
  registry.register(kind, Box::new(LbIndexKind::new(def, data_dir)));
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::field::FieldValue;
  use crate::lb::{encode_score, LbScoreType};
  use seisin_protocol::{decode_lb_result, encode_lb_execute_op, encode_lb_query_req};

  fn racing(rule: LbRule) -> LbClassDef {
    LbClassDef {
      name: "racing".to_string(),
      score_type: LbScoreType::I64,
      display_len: 16,
      rule,
    }
  }

  fn open_board(dir: &std::path::Path, rule: LbRule) -> Box<dyn ResidentIndex> {
    LbIndexKind::new(racing(rule), dir.to_path_buf())
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
    let dir = tempfile::tempdir().unwrap();
    let mut board = open_board(dir.path(), LbRule::Max);
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
    let dir = tempfile::tempdir().unwrap();
    let mut board = open_board(dir.path(), LbRule::Max);
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
    let dir = tempfile::tempdir().unwrap();
    let mut board = open_board(dir.path(), LbRule::Min);
    let alice = DatumId::new();
    update(board.as_mut(), alice, "Alice", 300, vec![]);
    let result = update(board.as_mut(), alice, "Alice", 200, vec![]); // better for Min
    assert_eq!(
      crate::lb::decode_rank_key(&LbScoreType::I64, result.top[0].rank_key),
      FieldValue::I64(200)
    );

    let dir2 = tempfile::tempdir().unwrap();
    let mut board2 = open_board(dir2.path(), LbRule::Replace);
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
    let dir = tempfile::tempdir().unwrap();
    let mut board = open_board(dir.path(), LbRule::Max);
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
    let dir = tempfile::tempdir().unwrap();
    let mut board = open_board(dir.path(), LbRule::Max);
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
    let dir = tempfile::tempdir().unwrap();
    let mut board = open_board(dir.path(), LbRule::Max);
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
    let dir = tempfile::tempdir().unwrap();
    let mut board = open_board(dir.path(), LbRule::Max);
    let alice = DatumId::new();
    let long = "AVeryLongDisplayNameIndeed"; // 26 bytes > display_len 16
    let result = update(board.as_mut(), alice, long, 100, vec![]);
    assert_eq!(result.top[0].display, long.as_bytes()[..16].to_vec());
  }

  #[test]
  fn cold_reopen_rebuilds_the_player_map_from_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let target = DatumId::new();
    let kind = LbIndexKind::new(racing(LbRule::Max), dir.path().to_path_buf());
    let alice = DatumId::new();
    {
      let mut board = kind.open(target, None).unwrap();
      update(board.as_mut(), alice, "Alice", 300, vec![]);
    }
    let mut board = kind.open(target, None).unwrap();
    // Map rebuilt: a worse score under Max is still rejected.
    let result = update(board.as_mut(), alice, "Alice", 100, vec![]);
    assert_eq!(result.total, 1);
    assert_eq!(
      crate::lb::decode_rank_key(&LbScoreType::I64, result.top[0].rank_key),
      FieldValue::I64(300)
    );
  }

  #[test]
  fn apply_is_rejected_and_malformed_execute_is_an_error_not_a_panic() {
    let dir = tempfile::tempdir().unwrap();
    let mut board = open_board(dir.path(), LbRule::Max);
    assert!(board.apply(b"anything").violation.is_some());
    assert!(board.execute(&[0xFF, 0xFF]).is_err());
  }
}
