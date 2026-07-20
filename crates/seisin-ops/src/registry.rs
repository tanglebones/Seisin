//! `OpRegistry`: the table of solution-defined operations a server
//! process was built with. Registration happens once at startup;
//! lookup happens once per incoming `Request::Op`.

use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};

use seisin_core::datum::DatumId;

use crate::context::OpContext;

/// A solution-defined operation: given mutable access to the collated
/// datums (via `OpContext`), the exact datum_ids this invocation was
/// collated for, and an opaque argument payload, returns an opaque
/// result payload. Solutions choose their own payload serialization;
/// the framework never interprets it.
pub type OpHandler = Box<dyn Fn(&mut OpContext, &[DatumId], &[u8]) -> Vec<u8> + Send + Sync>;

#[derive(Default)]
pub struct OpRegistry {
  handlers: HashMap<String, OpHandler>,
}

impl OpRegistry {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn register(&mut self, name: impl Into<String>, handler: OpHandler) {
    self.handlers.insert(name.into(), handler);
  }

  /// Looks up and invokes the named op, catching a panic inside the
  /// handler so a bug in solution code can't take down the thread that
  /// owns every other datum it's currently holding. Returns `Err` with a
  /// message either if the op name is unknown or if the handler panicked.
  pub fn invoke(
    &self,
    name: &str,
    ctx: &mut OpContext,
    datum_ids: &[DatumId],
    payload: &[u8],
  ) -> Result<Vec<u8>, String> {
    let handler = self.handlers.get(name).ok_or_else(|| format!("unknown op: {name}"))?;
    catch_unwind(AssertUnwindSafe(|| handler(ctx, datum_ids, payload))).map_err(|_| format!("op '{name}' panicked"))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::Arc;

  use seisin_core::cache::Cache;
  use seisin_core::store::InMemoryStore;

  #[test]
  fn invokes_a_registered_op() {
    let mut registry = OpRegistry::new();
    registry.register("echo", Box::new(|_ctx, _ids, payload| payload.to_vec()));

    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    assert_eq!(registry.invoke("echo", &mut ctx, &[], b"hello").unwrap(), b"hello");
  }

  #[test]
  fn unknown_op_name_returns_an_error() {
    let registry = OpRegistry::new();
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    assert!(registry.invoke("nope", &mut ctx, &[], b"").is_err());
  }

  #[test]
  fn a_panicking_op_is_caught_and_reported_as_an_error() {
    let mut registry = OpRegistry::new();
    registry.register("boom", Box::new(|_ctx, _ids, _payload| panic!("solution bug")));
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    assert!(registry.invoke("boom", &mut ctx, &[], b"").is_err());
  }

  #[test]
  fn an_op_can_read_and_write_through_the_context_using_its_datum_ids() {
    let mut registry = OpRegistry::new();
    registry.register(
      "put_first",
      Box::new(|ctx, ids, payload| {
        ctx.put(ids[0], payload.to_vec());
        payload.to_vec()
      }),
    );
    let mut cache = Cache::new(Arc::new(InMemoryStore::new()));
    let mut ctx = OpContext::new(&mut cache);
    let id = DatumId::new();
    registry.invoke("put_first", &mut ctx, &[id], b"hi").unwrap();
    assert_eq!(ctx.get(id), Some(b"hi".to_vec()));
  }
}
