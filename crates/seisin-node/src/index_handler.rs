//! The table of registered "how to apply an update to index kind X"
//! callbacks — parallel to `seisin_ops::registry::OpRegistry`, but for
//! index-kind logic rather than solution ops. Registration happens once
//! at startup; lookup happens once per incoming `IndexUpdate`. This
//! stays entirely inside `seisin-node` so the framework itself remains
//! agnostic of what "sk"/"rk"/"tk" even mean — a solution (or
//! `seisin-types`) registers the handler; this registry just dispatches
//! to it by name. See the design doc's "Automatic Index Maintenance &
//! Op Lifecycle" section.

use std::collections::HashMap;

/// Applies one update to an index's resident state. `current` is the
/// index's current resident bytes (`None` on a cold miss — nothing
/// resident yet), `payload` is the opaque update to apply. Returns the
/// new resident bytes and, if the update was rejected (e.g. a
/// uniqueness violation), a message describing why — a rejected update
/// leaves the caller's resident/stored state untouched (see
/// `worker.rs`'s `IndexUpdate` handling).
pub type IndexHandler =
  Box<dyn Fn(Option<&[u8]>, &[u8]) -> (Vec<u8>, Option<String>) + Send + Sync>;

#[derive(Default)]
pub struct IndexHandlerRegistry {
  handlers: HashMap<String, IndexHandler>,
}

impl IndexHandlerRegistry {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn register(&mut self, kind: impl Into<String>, handler: IndexHandler) {
    self.handlers.insert(kind.into(), handler);
  }

  /// Looks up `kind` and applies `payload` against `current`. Returns
  /// `Err` if `kind` was never registered.
  pub fn apply(
    &self,
    kind: &str,
    current: Option<&[u8]>,
    payload: &[u8],
  ) -> Result<(Vec<u8>, Option<String>), String> {
    let handler = self
      .handlers
      .get(kind)
      .ok_or_else(|| format!("no index handler registered for kind {kind:?}"))?;
    Ok(handler(current, payload))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn apply_dispatches_to_the_registered_handler() {
    let mut registry = IndexHandlerRegistry::new();
    registry.register(
      "uppercase",
      Box::new(|_current, payload| (payload.iter().map(u8::to_ascii_uppercase).collect(), None)),
    );
    let (result, violation) = registry.apply("uppercase", None, b"hello").unwrap();
    assert_eq!(result, b"HELLO");
    assert!(violation.is_none());
  }

  #[test]
  fn apply_on_an_unregistered_kind_is_an_error() {
    let registry = IndexHandlerRegistry::new();
    assert!(registry.apply("nope", None, b"x").is_err());
  }

  #[test]
  fn apply_can_report_a_violation() {
    let mut registry = IndexHandlerRegistry::new();
    registry.register(
      "reject_everything",
      Box::new(|_current, payload| (payload.to_vec(), Some("nope".to_string()))),
    );
    let (_, violation) = registry.apply("reject_everything", None, b"x").unwrap();
    assert_eq!(violation, Some("nope".to_string()));
  }

  #[test]
  fn apply_passes_current_through_to_the_handler() {
    let mut registry = IndexHandlerRegistry::new();
    registry.register(
      "echo_current",
      Box::new(|current, _payload| (current.unwrap_or(b"cold").to_vec(), None)),
    );
    let (cold, _) = registry.apply("echo_current", None, b"").unwrap();
    assert_eq!(cold, b"cold");
    let (warm, _) = registry.apply("echo_current", Some(b"warm"), b"").unwrap();
    assert_eq!(warm, b"warm");
  }
}
