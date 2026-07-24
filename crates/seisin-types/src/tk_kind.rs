//! The `"tk:{class}"` `IndexKind`: one counted-B+Tree history file per
//! (class, entity) datum, keyed `sub_key ++ ts(lower)` so each
//! sub-part of the entity has an independent, non-overlapping
//! valid-time history. tk is primary data (values exist nowhere else)
//! — `apply` (the framework-diff rail) is rejected; all writes arrive
//! as `execute` ops carrying an explicit or server-stamped `as_of`.
//! Residency is the open file handle only: every op is O(log n) page
//! reads — the thunked-range model with B+Tree pages as the segments.

use std::cell::RefCell;
use std::path::PathBuf;
use std::sync::Arc;

use seisin_core::datum::DatumId;
use seisin_node::index_handler::{IndexApplyOutcome, IndexKind, IndexKindRegistry, ResidentIndex};
use seisin_protocol::{
  decode_tk_op, decode_tk_query_req, encode_tk_result, TkOp, TkQueryReq, TkResult, TkSpan,
};
use seisin_storage::btree::BPlusTree;

use crate::tk::{decode_ts, encode_ts, tk_kind_name, TkClassDef, WallClock};

const TK_PAGE_SIZE: u32 = 4096;

pub struct TkIndexKind {
  def: TkClassDef,
  data_dir: PathBuf,
  clock: Arc<dyn WallClock>,
}

impl TkIndexKind {
  pub fn new(def: TkClassDef, data_dir: PathBuf, clock: Arc<dyn WallClock>) -> Self {
    Self {
      def,
      data_dir,
      clock,
    }
  }
}

/// Files are named by the tk datum's id — `IndexKind::open` only
/// receives the `DatumId`, which is already the stable derivation of
/// `tk:{class}:{entity}`.
fn file_name_for(target: DatumId) -> String {
  let hex: String = target
    .as_bytes()
    .iter()
    .map(|b| format!("{b:02x}"))
    .collect();
  format!("tk_{hex}.btree")
}

pub struct TkResidentHistory {
  def: TkClassDef,
  clock: Arc<dyn WallClock>,
  // RefCell for the same reason as rk/lb: `query` takes `&self` while
  // BPlusTree page reads need `&mut`. Single-threaded by construction.
  tree: RefCell<BPlusTree>,
}

impl TkResidentHistory {
  fn sub_w(&self) -> usize {
    self.def.sub_key_width as usize
  }

  fn composite(&self, sub_key: &[u8], t: i64) -> Vec<u8> {
    let mut key = sub_key.to_vec();
    key.extend_from_slice(&encode_ts(t));
    key
  }

  /// Record layout: upper_flag(1) ++ ts(upper) or zeroes (8) ++
  /// value_len (u16 LE) ++ value ++ zero padding to the class width.
  fn record(&self, upper: Option<i64>, value: &[u8]) -> Vec<u8> {
    let mut rec = vec![0u8; 1 + 8 + 2 + self.def.value_width as usize];
    if let Some(upper) = upper {
      rec[0] = 1;
      rec[1..9].copy_from_slice(&encode_ts(upper));
    }
    rec[9..11].copy_from_slice(&(value.len() as u16).to_le_bytes());
    rec[11..11 + value.len()].copy_from_slice(value);
    rec
  }

  fn span_from(&self, key: &[u8], rec: &[u8]) -> TkSpan {
    let sub_w = self.sub_w();
    let lower = decode_ts(key[sub_w..sub_w + 8].try_into().unwrap());
    let upper = if rec[0] == 1 {
      Some(decode_ts(rec[1..9].try_into().unwrap()))
    } else {
      None
    };
    let len = u16::from_le_bytes(rec[9..11].try_into().unwrap()) as usize;
    TkSpan {
      sub_key: key[..sub_w].to_vec(),
      lower,
      upper,
      value: rec[11..11 + len].to_vec(),
    }
  }

  fn entry_at(
    &self,
    tree: &mut BPlusTree,
    rank: u64,
  ) -> Result<Option<(Vec<u8>, Vec<u8>)>, String> {
    Ok(
      tree
        .scan_from_rank(rank, 1)
        .map_err(|e| e.to_string())?
        .into_iter()
        .next(),
    )
  }

  /// Global floor at (sub_key, t): rank + entry, regardless of which
  /// sub-key the floor lands in — callers prefix-check for
  /// in-sub-key-ness, and successor computation needs the rank either
  /// way.
  fn raw_floor(
    &self,
    tree: &mut BPlusTree,
    sub_key: &[u8],
    t: i64,
  ) -> Result<Option<(u64, Vec<u8>, Vec<u8>)>, String> {
    let probe = self.composite(sub_key, t);
    match tree.rank_of_floor(&probe).map_err(|e| e.to_string())? {
      None => Ok(None),
      Some(rank) => {
        let (k, v) = self
          .entry_at(tree, rank)?
          .ok_or_else(|| "floor rank out of range".to_string())?;
        Ok(Some((rank, k, v)))
      }
    }
  }

  /// The span covering `t` in `sub_key`, with its rank, if any.
  fn covering(
    &self,
    tree: &mut BPlusTree,
    sub_key: &[u8],
    t: i64,
  ) -> Result<Option<(u64, TkSpan)>, String> {
    match self.raw_floor(tree, sub_key, t)? {
      Some((rank, k, v)) if &k[..self.sub_w()] == sub_key => {
        let span = self.span_from(&k, &v);
        if span.upper.is_none() || span.upper.unwrap() > t {
          Ok(Some((rank, span)))
        } else {
          Ok(None)
        }
      }
      _ => Ok(None),
    }
  }

  fn check_sub_key(&self, sub_key: &[u8]) -> Result<(), String> {
    if sub_key.len() != self.sub_w() {
      return Err(format!(
        "sub_key must be exactly {} bytes for class {:?}, got {}",
        self.def.sub_key_width,
        self.def.name,
        sub_key.len()
      ));
    }
    Ok(())
  }

  fn validate_set(&self, sub_key: &[u8], value: &[u8]) -> Result<(), String> {
    self.check_sub_key(sub_key)?;
    let mut offset = 0;
    crate::encoding::decode_field_value(&self.def.value_type, value, &mut offset)
      .map_err(|e| format!("value does not decode as {:?}: {e}", self.def.value_type))?;
    if offset != value.len() {
      return Err(format!("value has {} trailing bytes", value.len() - offset));
    }
    if value.len() > self.def.value_width as usize {
      return Err(format!(
        "encoded value is {} bytes but class {:?} caps values at {} — rejected, never \
         truncated (tk is primary data)",
        value.len(),
        self.def.name,
        self.def.value_width
      ));
    }
    Ok(())
  }

  fn do_set(&self, sub_key: &[u8], as_of: i64, value: &[u8]) -> Result<Vec<TkSpan>, String> {
    let mut tree = self.tree.borrow_mut();
    let err = |e: anyhow::Error| e.to_string();
    match self.covering(&mut tree, sub_key, as_of)? {
      Some((_, span)) if span.lower == as_of => {
        // Same-instant value correction: bounds unchanged.
        tree
          .insert(
            &self.composite(sub_key, as_of),
            &self.record(span.upper, value),
          )
          .map_err(err)?;
        Ok(vec![TkSpan {
          value: value.to_vec(),
          ..span
        }])
      }
      Some((_, span)) => {
        // Close the covering range at as_of; the new range inherits
        // its old upper — non-overlap by construction.
        tree
          .insert(
            &self.composite(sub_key, span.lower),
            &self.record(Some(as_of), &span.value),
          )
          .map_err(err)?;
        tree
          .insert(
            &self.composite(sub_key, as_of),
            &self.record(span.upper, value),
          )
          .map_err(err)?;
        Ok(vec![TkSpan {
          sub_key: sub_key.to_vec(),
          lower: as_of,
          upper: span.upper,
          value: value.to_vec(),
        }])
      }
      None => {
        // Gap (or before the first entry of this sub-key): bound by
        // the sub-key's successor, if any — never leaks across
        // sub-parts.
        let successor_rank = match self.raw_floor(&mut tree, sub_key, as_of)? {
          Some((rank, _, _)) => rank + 1,
          None => 0,
        };
        let upper = match self.entry_at(&mut tree, successor_rank)? {
          Some((k, _)) if &k[..self.sub_w()] == sub_key => Some(decode_ts(
            k[self.sub_w()..self.sub_w() + 8].try_into().unwrap(),
          )),
          _ => None,
        };
        tree
          .insert(&self.composite(sub_key, as_of), &self.record(upper, value))
          .map_err(err)?;
        Ok(vec![TkSpan {
          sub_key: sub_key.to_vec(),
          lower: as_of,
          upper,
          value: value.to_vec(),
        }])
      }
    }
  }

  fn do_clear(&self, sub_key: &[u8], as_of: i64) -> Result<Vec<TkSpan>, String> {
    let mut tree = self.tree.borrow_mut();
    let err = |e: anyhow::Error| e.to_string();
    match self.covering(&mut tree, sub_key, as_of)? {
      None => Ok(vec![]), // clearing inside a gap is a no-op
      Some((_, span)) if span.lower == as_of => {
        // A [t, t) range holds no information — remove outright.
        tree
          .remove(&self.composite(sub_key, span.lower))
          .map_err(err)?;
        Ok(vec![])
      }
      Some((_, span)) => {
        tree
          .insert(
            &self.composite(sub_key, span.lower),
            &self.record(Some(as_of), &span.value),
          )
          .map_err(err)?;
        Ok(vec![TkSpan {
          upper: Some(as_of),
          ..span
        }])
      }
    }
  }

  /// First rank belonging to `sub_key` (which may hold no entries —
  /// callers' prefix checks handle that).
  fn first_rank_of(&self, tree: &mut BPlusTree, sub_key: &[u8]) -> Result<u64, String> {
    match self.raw_floor(tree, sub_key, i64::MIN)? {
      Some((rank, k, _)) if k == self.composite(sub_key, i64::MIN) => Ok(rank),
      Some((rank, _, _)) => Ok(rank + 1),
      None => Ok(0),
    }
  }

  fn collect_while(
    &self,
    tree: &mut BPlusTree,
    mut rank: u64,
    keep: impl Fn(&TkSpan) -> bool,
  ) -> Result<Vec<TkSpan>, String> {
    let mut spans = Vec::new();
    while let Some((k, v)) = self.entry_at(tree, rank)? {
      let span = self.span_from(&k, &v);
      if !keep(&span) {
        break;
      }
      spans.push(span);
      rank += 1;
    }
    Ok(spans)
  }
}

impl ResidentIndex for TkResidentHistory {
  fn apply(&mut self, _payload: &[u8]) -> IndexApplyOutcome {
    IndexApplyOutcome {
      violation: Some(
        "tk histories are maintained via execute ops, not framework index updates".to_string(),
      ),
      write_through: None,
    }
  }

  fn query(&self, query: &[u8]) -> Result<Vec<u8>, String> {
    let query = decode_tk_query_req(query).map_err(|e| e.to_string())?;
    let mut tree = self.tree.borrow_mut();
    let spans = match query {
      TkQueryReq::AsOf { sub_key, t } => {
        self.check_sub_key(&sub_key)?;
        self
          .covering(&mut tree, &sub_key, t)?
          .map(|(_, span)| vec![span])
          .unwrap_or_default()
      }
      TkQueryReq::Current { sub_key } => {
        self.check_sub_key(&sub_key)?;
        match self.raw_floor(&mut tree, &sub_key, i64::MAX)? {
          Some((_, k, v)) if k[..self.sub_w()] == sub_key[..] => {
            let span = self.span_from(&k, &v);
            if span.upper.is_none() {
              vec![span]
            } else {
              vec![]
            }
          }
          _ => vec![],
        }
      }
      TkQueryReq::History { sub_key } => {
        self.check_sub_key(&sub_key)?;
        let start = self.first_rank_of(&mut tree, &sub_key)?;
        self.collect_while(&mut tree, start, |s| s.sub_key == sub_key)?
      }
      TkQueryReq::Range { sub_key, from, to } => {
        self.check_sub_key(&sub_key)?;
        if to <= from {
          vec![]
        } else {
          let start = match self.covering(&mut tree, &sub_key, from)? {
            Some((rank, _)) => rank,
            None => match self.raw_floor(&mut tree, &sub_key, from)? {
              Some((rank, _, _)) => rank + 1,
              None => 0,
            },
          };
          self.collect_while(&mut tree, start, |s| s.sub_key == sub_key && s.lower < to)?
        }
      }
      TkQueryReq::SnapshotAt { t } => {
        let mut spans = Vec::new();
        let mut rank = 0u64;
        while let Some((k, _)) = self.entry_at(&mut tree, rank)? {
          let sub = k[..self.sub_w()].to_vec();
          if let Some((_, span)) = self.covering(&mut tree, &sub, t)? {
            spans.push(span);
          }
          // Jump past this sub-key: its greatest possible key is
          // composite(sub, i64::MAX).
          let last = tree
            .rank_of_floor(&self.composite(&sub, i64::MAX))
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "snapshot: sub-key vanished mid-walk".to_string())?;
          rank = last + 1;
        }
        spans
      }
    };
    Ok(encode_tk_result(&TkResult { spans }))
  }

  fn execute(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
    let spans = match decode_tk_op(payload).map_err(|e| e.to_string())? {
      TkOp::Set {
        sub_key,
        as_of,
        value,
      } => {
        self.validate_set(&sub_key, &value)?;
        let as_of = as_of.unwrap_or_else(|| self.clock.now_millis());
        self.do_set(&sub_key, as_of, &value)?
      }
      TkOp::Clear { sub_key, as_of } => {
        self.check_sub_key(&sub_key)?;
        let as_of = as_of.unwrap_or_else(|| self.clock.now_millis());
        self.do_clear(&sub_key, as_of)?
      }
    };
    Ok(encode_tk_result(&TkResult { spans }))
  }
}

impl IndexKind for TkIndexKind {
  /// `stored` is ignored: tk persists in its own page file.
  fn open(
    &self,
    target: DatumId,
    _stored: Option<Vec<u8>>,
  ) -> Result<Box<dyn ResidentIndex>, String> {
    let path = self.data_dir.join(file_name_for(target));
    let key_size = self.def.sub_key_width as u32 + 8;
    let value_size = 1 + 8 + 2 + self.def.value_width as u32;
    let tree = if path.exists() {
      BPlusTree::open(&path)
    } else {
      std::fs::create_dir_all(&self.data_dir)
        .map_err(|e| format!("failed to create tk data dir {:?}: {e}", self.data_dir))?;
      BPlusTree::create(&path, key_size, value_size, TK_PAGE_SIZE)
    }
    .map_err(|e| format!("failed to open tk history file {path:?}: {e}"))?;
    Ok(Box::new(TkResidentHistory {
      def: self.def.clone(),
      clock: Arc::clone(&self.clock),
      tree: RefCell::new(tree),
    }))
  }
}

/// Registers one tk class under kind `tk:{name}` — call once at the
/// composition root per class, with the wall clock that stamps
/// `as_of: None` writes.
pub fn register_tk_class(
  registry: &mut IndexKindRegistry,
  def: TkClassDef,
  data_dir: PathBuf,
  clock: Arc<dyn WallClock>,
) {
  let kind = tk_kind_name(&def.name);
  registry.register(kind, Box::new(TkIndexKind::new(def, data_dir, clock)));
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::encoding::encode_field_value;
  use crate::field::{FieldType, FieldValue};

  struct FixedClock(i64);
  impl WallClock for FixedClock {
    fn now_millis(&self) -> i64 {
      self.0
    }
  }

  fn holdings(sub_key_width: u16, value_width: u16) -> TkClassDef {
    TkClassDef {
      name: "holdings".to_string(),
      value_type: FieldType::F64,
      value_width,
      sub_key_width,
    }
  }

  fn open_history(
    dir: &std::path::Path,
    sub_key_width: u16,
    clock_millis: i64,
  ) -> Box<dyn ResidentIndex> {
    TkIndexKind::new(
      holdings(sub_key_width, 16),
      dir.to_path_buf(),
      Arc::new(FixedClock(clock_millis)),
    )
    .open(DatumId::new(), None)
    .unwrap()
  }

  fn val(x: f64) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_field_value(&FieldValue::F64(x), &mut buf);
    buf
  }

  fn set(hist: &mut dyn ResidentIndex, sub_key: &[u8], as_of: Option<i64>, x: f64) -> Vec<TkSpan> {
    let payload = seisin_protocol::encode_tk_op(&TkOp::Set {
      sub_key: sub_key.to_vec(),
      as_of,
      value: val(x),
    });
    seisin_protocol::decode_tk_result(&hist.execute(&payload).unwrap())
      .unwrap()
      .spans
  }

  fn clear(hist: &mut dyn ResidentIndex, sub_key: &[u8], as_of: Option<i64>) -> Vec<TkSpan> {
    let payload = seisin_protocol::encode_tk_op(&TkOp::Clear {
      sub_key: sub_key.to_vec(),
      as_of,
    });
    seisin_protocol::decode_tk_result(&hist.execute(&payload).unwrap())
      .unwrap()
      .spans
  }

  fn q(hist: &dyn ResidentIndex, query: TkQueryReq) -> Vec<TkSpan> {
    let payload = seisin_protocol::encode_tk_query_req(&query);
    seisin_protocol::decode_tk_result(&hist.query(&payload).unwrap())
      .unwrap()
      .spans
  }

  const NO_SUB: &[u8] = &[];

  #[test]
  fn forward_set_on_empty_creates_an_open_ended_span() {
    let dir = tempfile::tempdir().unwrap();
    let mut hist = open_history(dir.path(), 0, 0);
    let spans = set(hist.as_mut(), NO_SUB, Some(100), 1.0);
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].lower, 100);
    assert_eq!(spans[0].upper, None);
    assert_eq!(spans[0].value, val(1.0));
  }

  #[test]
  fn second_set_closes_the_open_range_and_chains_uppers() {
    let dir = tempfile::tempdir().unwrap();
    let mut hist = open_history(dir.path(), 0, 0);
    set(hist.as_mut(), NO_SUB, Some(100), 1.0);
    let spans = set(hist.as_mut(), NO_SUB, Some(200), 2.0);
    assert_eq!(spans[0].lower, 200);
    assert_eq!(spans[0].upper, None);
    let history = q(hist.as_ref(), TkQueryReq::History { sub_key: vec![] });
    assert_eq!(history.len(), 2);
    assert_eq!((history[0].lower, history[0].upper), (100, Some(200)));
    assert_eq!((history[1].lower, history[1].upper), (200, None));
  }

  #[test]
  fn backdated_correction_splits_a_past_closed_range() {
    let dir = tempfile::tempdir().unwrap();
    let mut hist = open_history(dir.path(), 0, 0);
    set(hist.as_mut(), NO_SUB, Some(100), 1.0);
    set(hist.as_mut(), NO_SUB, Some(200), 2.0);
    set(hist.as_mut(), NO_SUB, Some(300), 3.0);
    let spans = set(hist.as_mut(), NO_SUB, Some(150), 9.0); // backdated
    assert_eq!((spans[0].lower, spans[0].upper), (150, Some(200)));
    let history = q(hist.as_ref(), TkQueryReq::History { sub_key: vec![] });
    let bounds: Vec<(i64, Option<i64>)> = history.iter().map(|s| (s.lower, s.upper)).collect();
    assert_eq!(
      bounds,
      vec![
        (100, Some(150)),
        (150, Some(200)),
        (200, Some(300)),
        (300, None)
      ]
    );
    assert_eq!(history[1].value, val(9.0));
    assert_eq!(history[0].value, val(1.0)); // original value preserved up to the correction
  }

  #[test]
  fn same_instant_set_overwrites_value_keeping_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let mut hist = open_history(dir.path(), 0, 0);
    set(hist.as_mut(), NO_SUB, Some(100), 1.0);
    set(hist.as_mut(), NO_SUB, Some(200), 2.0);
    let spans = set(hist.as_mut(), NO_SUB, Some(100), 5.0);
    assert_eq!((spans[0].lower, spans[0].upper), (100, Some(200)));
    assert_eq!(spans[0].value, val(5.0));
    let history = q(hist.as_ref(), TkQueryReq::History { sub_key: vec![] });
    assert_eq!(history.len(), 2); // no new span created
  }

  #[test]
  fn clear_creates_a_gap_and_as_of_inside_it_returns_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut hist = open_history(dir.path(), 0, 0);
    set(hist.as_mut(), NO_SUB, Some(100), 1.0);
    let closed = clear(hist.as_mut(), NO_SUB, Some(200));
    assert_eq!((closed[0].lower, closed[0].upper), (100, Some(200)));
    assert!(q(
      hist.as_ref(),
      TkQueryReq::AsOf {
        sub_key: vec![],
        t: 250
      }
    )
    .is_empty());
    assert_eq!(
      q(
        hist.as_ref(),
        TkQueryReq::AsOf {
          sub_key: vec![],
          t: 150
        }
      )[0]
        .value,
      val(1.0)
    );
    assert!(q(hist.as_ref(), TkQueryReq::Current { sub_key: vec![] }).is_empty());
  }

  #[test]
  fn set_into_a_gap_inherits_the_successors_lower() {
    let dir = tempfile::tempdir().unwrap();
    let mut hist = open_history(dir.path(), 0, 0);
    set(hist.as_mut(), NO_SUB, Some(100), 1.0);
    clear(hist.as_mut(), NO_SUB, Some(200));
    set(hist.as_mut(), NO_SUB, Some(300), 3.0);
    let spans = set(hist.as_mut(), NO_SUB, Some(250), 9.0); // fills the gap
    assert_eq!((spans[0].lower, spans[0].upper), (250, Some(300)));
  }

  #[test]
  fn set_before_the_first_entry_bounds_at_first_lower() {
    let dir = tempfile::tempdir().unwrap();
    let mut hist = open_history(dir.path(), 0, 0);
    set(hist.as_mut(), NO_SUB, Some(300), 3.0);
    let spans = set(hist.as_mut(), NO_SUB, Some(100), 1.0);
    assert_eq!((spans[0].lower, spans[0].upper), (100, Some(300)));
  }

  #[test]
  fn clear_at_exact_lower_removes_the_span_entirely() {
    let dir = tempfile::tempdir().unwrap();
    let mut hist = open_history(dir.path(), 0, 0);
    set(hist.as_mut(), NO_SUB, Some(100), 1.0);
    set(hist.as_mut(), NO_SUB, Some(200), 2.0);
    let result = clear(hist.as_mut(), NO_SUB, Some(200));
    assert!(result.is_empty());
    let history = q(hist.as_ref(), TkQueryReq::History { sub_key: vec![] });
    assert_eq!(history.len(), 1);
    assert_eq!((history[0].lower, history[0].upper), (100, Some(200)));
  }

  #[test]
  fn two_sub_keys_never_interact() {
    let dir = tempfile::tempdir().unwrap();
    let mut hist = open_history(dir.path(), 16, 0);
    let a = [1u8; 16];
    let b = [2u8; 16];
    set(hist.as_mut(), &a, Some(100), 1.0);
    set(hist.as_mut(), &b, Some(500), 5.0);
    clear(hist.as_mut(), &a, Some(200));
    // A gap-fill in `a` must NOT be bounded by b's entry at 500.
    let spans = set(hist.as_mut(), &a, Some(300), 2.0);
    assert_eq!((spans[0].lower, spans[0].upper), (300, None));
    // And b at t=100 has no value.
    assert!(q(
      hist.as_ref(),
      TkQueryReq::AsOf {
        sub_key: b.to_vec(),
        t: 100
      }
    )
    .is_empty());
  }

  #[test]
  fn snapshot_at_returns_one_covering_span_per_sub_key_skipping_gaps() {
    let dir = tempfile::tempdir().unwrap();
    let mut hist = open_history(dir.path(), 16, 0);
    let (a, b, c) = ([1u8; 16], [2u8; 16], [3u8; 16]);
    set(hist.as_mut(), &a, Some(100), 1.0);
    set(hist.as_mut(), &b, Some(50), 5.0);
    clear(hist.as_mut(), &b, Some(150));
    set(hist.as_mut(), &c, Some(200), 7.0);

    let at_120 = q(hist.as_ref(), TkQueryReq::SnapshotAt { t: 120 });
    let subs_120: Vec<&[u8]> = at_120.iter().map(|s| s.sub_key.as_slice()).collect();
    assert_eq!(subs_120, vec![a.as_slice(), b.as_slice()]); // c not yet

    let at_300 = q(hist.as_ref(), TkQueryReq::SnapshotAt { t: 300 });
    let subs_300: Vec<&[u8]> = at_300.iter().map(|s| s.sub_key.as_slice()).collect();
    assert_eq!(subs_300, vec![a.as_slice(), c.as_slice()]); // b cleared
  }

  #[test]
  fn wrong_width_sub_key_and_oversized_and_mistyped_values_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut hist = open_history(dir.path(), 16, 0);
    let err = hist
      .execute(&seisin_protocol::encode_tk_op(&TkOp::Set {
        sub_key: vec![1u8; 4], // wrong width
        as_of: Some(1),
        value: val(1.0),
      }))
      .unwrap_err();
    assert!(err.contains("sub_key"), "{err}");

    // Mistyped: a String payload does not decode cleanly as F64.
    let mut string_bytes = Vec::new();
    encode_field_value(
      &FieldValue::String("hello world".to_string()),
      &mut string_bytes,
    );
    let err = hist
      .execute(&seisin_protocol::encode_tk_op(&TkOp::Set {
        sub_key: vec![1u8; 16],
        as_of: Some(1),
        value: string_bytes,
      }))
      .unwrap_err();
    assert!(!err.is_empty());

    // Oversized: class caps at 4 bytes, F64 encodes to 8 — rejected.
    let tight = TkIndexKind::new(
      holdings(0, 4),
      dir.path().to_path_buf(),
      Arc::new(FixedClock(0)),
    );
    let mut tight_hist = tight.open(DatumId::new(), None).unwrap();
    let err = tight_hist
      .execute(&seisin_protocol::encode_tk_op(&TkOp::Set {
        sub_key: vec![],
        as_of: Some(1),
        value: val(1.0),
      }))
      .unwrap_err();
    assert!(err.contains("rejected"), "{err}");
  }

  #[test]
  fn as_of_none_is_stamped_by_the_injected_clock() {
    let dir = tempfile::tempdir().unwrap();
    let mut hist = open_history(dir.path(), 0, 777);
    let spans = set(hist.as_mut(), NO_SUB, None, 1.0);
    assert_eq!(spans[0].lower, 777);
  }

  #[test]
  fn current_distinguishes_open_ended_from_closed_final_range() {
    let dir = tempfile::tempdir().unwrap();
    let mut hist = open_history(dir.path(), 0, 0);
    set(hist.as_mut(), NO_SUB, Some(100), 1.0);
    assert_eq!(
      q(hist.as_ref(), TkQueryReq::Current { sub_key: vec![] }).len(),
      1
    );
    clear(hist.as_mut(), NO_SUB, Some(200));
    assert!(q(hist.as_ref(), TkQueryReq::Current { sub_key: vec![] }).is_empty());
  }

  #[test]
  fn range_query_spans_gaps_and_clips_at_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let mut hist = open_history(dir.path(), 0, 0);
    set(hist.as_mut(), NO_SUB, Some(100), 1.0);
    set(hist.as_mut(), NO_SUB, Some(200), 2.0);
    clear(hist.as_mut(), NO_SUB, Some(300)); // gap [300, 400)
    set(hist.as_mut(), NO_SUB, Some(400), 4.0);

    let spans = q(
      hist.as_ref(),
      TkQueryReq::Range {
        sub_key: vec![],
        from: 150,
        to: 450,
      },
    );
    let lowers: Vec<i64> = spans.iter().map(|s| s.lower).collect();
    assert_eq!(lowers, vec![100, 200, 400]); // 100 overlaps from=150

    assert!(q(
      hist.as_ref(),
      TkQueryReq::Range {
        sub_key: vec![],
        from: 0,
        to: 50
      }
    )
    .is_empty());
    assert!(q(
      hist.as_ref(),
      TkQueryReq::Range {
        sub_key: vec![],
        from: 200,
        to: 200
      }
    )
    .is_empty());
  }

  #[test]
  fn cold_reopen_answers_from_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let target = DatumId::new();
    let kind = TkIndexKind::new(
      holdings(0, 16),
      dir.path().to_path_buf(),
      Arc::new(FixedClock(0)),
    );
    {
      let mut hist = kind.open(target, None).unwrap();
      set(hist.as_mut(), NO_SUB, Some(100), 1.0);
      set(hist.as_mut(), NO_SUB, Some(200), 2.0);
    }
    let hist = kind.open(target, None).unwrap();
    let history = q(hist.as_ref(), TkQueryReq::History { sub_key: vec![] });
    assert_eq!(history.len(), 2);
    assert_eq!(history[1].value, val(2.0));
  }

  #[test]
  fn apply_is_rejected_and_malformed_execute_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut hist = open_history(dir.path(), 0, 0);
    assert!(hist.apply(b"anything").violation.is_some());
    assert!(hist.execute(&[0xFF, 0xFF]).is_err());
  }
}
