//! `OpContext` is the interface a solution-defined operation function
//! uses to read/write the datums the framework has already collated
//! onto this thread. It operates at the byte level — the typed datum
//! layer from the design doc's "Datum Type System" notes is a separate,
//! not-yet-designed layer a solution's own generated code would sit on
//! top of; the framework itself stays type-agnostic.

use seisin_core::cache::Cache;
use seisin_core::datum::DatumId;

pub struct OpContext<'a> {
  cache: &'a mut Cache,
}

impl<'a> OpContext<'a> {
  pub fn new(cache: &'a mut Cache) -> Self {
    Self { cache }
  }

  pub fn get(&mut self, id: DatumId) -> Option<Vec<u8>> {
    self.cache.get(id)
  }

  pub fn put(&mut self, id: DatumId, content: Vec<u8>) {
    self.cache.put(id, content);
  }

  pub fn delete(&mut self, id: DatumId) {
    self.cache.delete(id);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::Arc;

  use seisin_core::store::InMemoryStore;

  #[test]
  fn put_then_get_round_trips_through_the_context() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let id = DatumId::new();
    ctx.put(id, b"hello".to_vec());
    assert_eq!(ctx.get(id), Some(b"hello".to_vec()));
  }

  #[test]
  fn delete_removes_the_entry() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let id = DatumId::new();
    ctx.put(id, b"hello".to_vec());
    ctx.delete(id);
    assert_eq!(ctx.get(id), None);
  }

  #[test]
  fn get_on_unknown_datum_returns_none() {
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    assert_eq!(ctx.get(DatumId::new()), None);
  }
}
