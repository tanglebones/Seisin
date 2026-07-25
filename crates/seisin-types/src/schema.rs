//! A solution's declared datum type: its name and ordered fields. See
//! the design doc's "Schema Declaration & Field Encoding" section.

use anyhow::{bail, Result};
use seisin_core::datum::DatumId;

use crate::encoding::{decode_field_value, encode_field_value};
use crate::field::{value_matches_type, FieldType, FieldValue};
use crate::sk_index::derived_id_namespace;

/// A type's pk identity discipline — one of exactly two kinds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PkKind {
  /// The default: ids must be version-7 UUIDs (what `DatumId::new`
  /// produces) — time-ordered, non-guessable.
  Uuid,
  /// A closed set of well-known mnemonics ("active", "closed", ...),
  /// each deterministically deriving its DatumId — the shared-status-
  /// set case, where entities FK by mnemonic rather than uuid.
  /// Extending the set is a schema migration (a code deploy under the
  /// n -> n+1 rollout model), never a runtime operation.
  Enum(Vec<String>),
}

/// The derived id for an Enum-pk mnemonic — derived-on-demand, no
/// seeding: the id is a valid reference target by construction.
pub fn enum_pk_id(type_name: &str, mnemonic: &str) -> DatumId {
  let name = format!("pk:{type_name}:{mnemonic}");
  DatumId::from_name(&derived_id_namespace(), name.as_bytes())
}

/// Validates `pk_id` against `def`'s declared pk discipline — called
/// by `TypedOpContext` on every typed write/delete. The byte-level
/// `OpContext` stays unrestricted (framework internals legitimately
/// use derived, non-v7 ids).
pub fn check_pk(pk_id: DatumId, def: &DatumTypeDef) -> Result<()> {
  match &def.pk {
    PkKind::Uuid => {
      // UUID version nibble: high 4 bits of byte 6.
      if pk_id.as_bytes()[6] >> 4 != 7 {
        bail!(
          "type {:?} declares Uuid pk identity: id {:?} is not a version-7 uuid",
          def.name,
          pk_id
        );
      }
    }
    PkKind::Enum(mnemonics) => {
      if !mnemonics.iter().any(|m| enum_pk_id(&def.name, m) == pk_id) {
        bail!(
          "type {:?} declares Enum pk identity: id {:?} matches none of its {} mnemonics",
          def.name,
          pk_id,
          mnemonics.len()
        );
      }
    }
  }
  Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct DatumTypeDef {
  pub name: String,
  /// The type's schema version, stamped as a prefix on every encoded
  /// datum. The encoding is deliberately tagless (schema-driven, no
  /// per-value type markers), which makes stored bytes undecodable
  /// under any *other* field layout — so bytes must carry which layout
  /// wrote them, or the planned add-freely/deprecate-then-remove schema
  /// evolution can never decode data written before a field was added.
  /// Bump this on any field-layout change. Decoding bytes stamped with
  /// an older version requires that version's field layout (a version
  /// history) — not built yet; today a mismatch is a hard, explicit
  /// decode error rather than silent misinterpretation.
  pub version: u16,
  pub pk: PkKind,
  pub fields: Vec<(String, FieldType)>,
  pub indexes: Vec<IndexDef>,
  pub constraints: Vec<RelationalConstraintDef>,
}

/// What an FK-constrained field references — always a declared
/// identity/index, never a bare scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FkTarget {
  /// References a Uuid-pk type: runtime existence check against the
  /// referenced datum. The constrained field holds the referenced
  /// DatumId as 16 `Bytes`.
  PkUuid { type_name: String },
  /// References an Enum-pk type: validity is set membership against
  /// the declared mnemonics — a schema-local, synchronous check with
  /// no runtime dispatch. The set is embedded at the declaring site
  /// (solutions define the enum once as a shared const and use it in
  /// both places); a cross-def type registry is future codegen work.
  PkEnum {
    type_name: String,
    mnemonics: Vec<String>,
  },
  /// References a *unique* sk index: runtime check against the derived
  /// sk key datum. The constrained field holds the referenced
  /// natural-key VALUE (any sk-legal primitive) — the check target is
  /// `sk_key(type_name, field, value)`.
  SkUnique { type_name: String, field: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalConstraintDef {
  pub field: String,
  pub references: FkTarget,
  /// None: a dangling reference is a hard synchronous rejection (the
  /// default). Some: the write is allowed, the dangling reference is
  /// tracked in fk_pending, and the named op is invoked by the scan
  /// driver if still missing when the scan runs — the framework never
  /// invokes it itself.
  pub resolution: Option<ConflictOp>,
}

impl DatumTypeDef {
  pub fn new(name: impl Into<String>) -> Self {
    Self {
      name: name.into(),
      version: 1,
      pk: PkKind::Uuid,
      fields: Vec::new(),
      indexes: Vec::new(),
      constraints: Vec::new(),
    }
  }

  /// Declares a relational constraint — see `RelationalConstraintDef`.
  ///
  /// # Panics
  /// Panics on a schema declaration bug (unknown field, or a field
  /// type incompatible with the reference target) — caught at process
  /// start, same policy as `index`.
  pub fn constraint(mut self, constraint: RelationalConstraintDef) -> Self {
    let declared = self
      .fields
      .iter()
      .find(|(name, _)| name == &constraint.field);
    let Some((_, field_ty)) = declared else {
      panic!(
        "constraint field {:?} on type {:?} is not a declared field",
        constraint.field, self.name
      );
    };
    match (&constraint.references, field_ty) {
      (FkTarget::PkEnum { .. }, FieldType::String) => {}
      (FkTarget::PkEnum { .. }, other) => panic!(
        "PkEnum constraint field {:?} on type {:?} must be String, found {:?}",
        constraint.field, self.name, other
      ),
      (FkTarget::PkUuid { .. }, FieldType::Bytes) => {}
      (FkTarget::PkUuid { .. }, other) => panic!(
        "PkUuid constraint field {:?} on type {:?} must be Bytes (a 16-byte DatumId), found {:?}",
        constraint.field, self.name, other
      ),
      (FkTarget::SkUnique { .. }, FieldType::Array(_) | FieldType::Dict(_, _)) => panic!(
        "SkUnique constraint field {:?} on type {:?} must be a primitive natural-key value",
        constraint.field, self.name
      ),
      (FkTarget::SkUnique { .. }, _) => {}
    }
    self.constraints.push(constraint);
    self
  }

  /// Sets the pk identity discipline — see `PkKind`.
  pub fn pk(mut self, pk: PkKind) -> Self {
    self.pk = pk;
    self
  }

  /// Sets the schema version this def describes — see the `version`
  /// field's doc for when it must be bumped.
  pub fn version(mut self, version: u16) -> Self {
    self.version = version;
    self
  }

  /// Appends a field to the type, in declaration order — that order is
  /// what `encode_datum`/`decode_datum` use, not the field name.
  pub fn field(mut self, name: impl Into<String>, ty: FieldType) -> Self {
    self.fields.push((name.into(), ty));
    self
  }

  /// Declares an index on this type — see `IndexDef`.
  ///
  /// # Panics
  /// Panics if an `Rk` index names an undeclared or non-numeric field —
  /// a solution's schema declaration bug, caught at process start (the
  /// same policy as `NodeConfig::self_address`'s documented panic).
  pub fn index(mut self, index: IndexDef) -> Self {
    if let IndexDef::Rk { field } = &index {
      let declared = self.fields.iter().find(|(name, _)| name == field);
      match declared {
        Some((_, FieldType::I64)) | Some((_, FieldType::F64)) => {}
        other => panic!(
          "rk index field {:?} on type {:?} must be a declared I64 or F64 field, found {:?}",
          field, self.name, other
        ),
      }
    }
    self.indexes.push(index);
    self
  }
}

/// Names a registered op (via `OpRegistry`, same mechanism as any domain
/// op) to call when a constraint violation is detected — see the design
/// doc's "Constraint Enforcement" section. Nothing in this crate invokes
/// it automatically; it's data a caller (the client-side typed-write
/// helper, in this plan) uses to make its own follow-up call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictOp(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexDef {
  Sk {
    field: String,
    unique: Option<ConflictOp>,
  },
  /// One global ranked structure per `type.field` (leaderboards). The
  /// field must be declared, and numeric — enforced at declaration.
  Rk { field: String },
}

/// Encodes `values` (one per field, in `def.fields`' declared order) into
/// a single byte buffer. Fails if the count doesn't match the schema or
/// any value doesn't match its field's declared type.
pub fn encode_datum(def: &DatumTypeDef, values: &[FieldValue]) -> Result<Vec<u8>> {
  if values.len() != def.fields.len() {
    bail!(
      "datum type {:?} has {} fields but {} values were given",
      def.name,
      def.fields.len(),
      values.len()
    );
  }
  let mut buf = def.version.to_le_bytes().to_vec();
  for ((field_name, field_ty), value) in def.fields.iter().zip(values) {
    if !value_matches_type(value, field_ty) {
      bail!(
        "value for field {:?} on datum type {:?} does not match its declared type {:?}",
        field_name,
        def.name,
        field_ty
      );
    }
    encode_field_value(value, &mut buf);
  }
  Ok(buf)
}

/// Decodes `bytes` into one `FieldValue` per field, in `def.fields`'
/// declared order. Fails if the bytes don't cleanly decode into exactly
/// that many fields with nothing left over.
pub fn decode_datum(def: &DatumTypeDef, bytes: &[u8]) -> Result<Vec<FieldValue>> {
  if bytes.len() < 2 {
    bail!(
      "datum bytes too short for a schema version prefix: {} bytes",
      bytes.len()
    );
  }
  let stored_version = u16::from_le_bytes(bytes[0..2].try_into().unwrap());
  if stored_version != def.version {
    bail!(
      "datum was encoded at schema version {} but type {:?} is at version {} — decoding \
       across versions needs that version's field layout (schema version history, not yet built)",
      stored_version,
      def.name,
      def.version
    );
  }
  let mut offset = 2;
  let mut values = Vec::with_capacity(def.fields.len());
  for (_, field_ty) in &def.fields {
    values.push(decode_field_value(field_ty, bytes, &mut offset)?);
  }
  if offset != bytes.len() {
    bail!(
      "datum type {:?} decoded {} of {} bytes; {} trailing bytes unaccounted for",
      def.name,
      offset,
      bytes.len(),
      bytes.len() - offset
    );
  }
  Ok(values)
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::field::FieldValue;

  fn user_type() -> DatumTypeDef {
    DatumTypeDef::new("user")
      .field("name", FieldType::String)
      .field("age", FieldType::I64)
  }

  #[test]
  fn builder_accumulates_fields_in_declared_order() {
    let def = user_type();
    assert_eq!(def.name, "user");
    assert_eq!(
      def.fields,
      vec![
        ("name".to_string(), FieldType::String),
        ("age".to_string(), FieldType::I64),
      ]
    );
  }

  #[test]
  fn round_trips_a_simple_datum() {
    let def = user_type();
    let values = vec![FieldValue::String("cliff".to_string()), FieldValue::I64(41)];
    let encoded = encode_datum(&def, &values).unwrap();
    let decoded = decode_datum(&def, &encoded).unwrap();
    assert_eq!(decoded, values);
  }

  #[test]
  fn encode_rejects_the_wrong_number_of_values() {
    let def = user_type();
    let values = vec![FieldValue::String("cliff".to_string())]; // missing "age"
    assert!(encode_datum(&def, &values).is_err());
  }

  #[test]
  fn builder_accumulates_indexes_in_declared_order() {
    let def = DatumTypeDef::new("user")
      .field("name", FieldType::String)
      .index(IndexDef::Sk {
        field: "name".to_string(),
        unique: None,
      });
    assert_eq!(
      def.indexes,
      vec![IndexDef::Sk {
        field: "name".to_string(),
        unique: None,
      }]
    );
  }

  #[test]
  fn a_unique_index_carries_its_conflict_op_name() {
    let def = DatumTypeDef::new("user")
      .field("email", FieldType::String)
      .index(IndexDef::Sk {
        field: "email".to_string(),
        unique: Some(ConflictOp("resolve_duplicate_email".to_string())),
      });
    match &def.indexes[0] {
      IndexDef::Sk {
        unique: Some(op), ..
      } => assert_eq!(op.0, "resolve_duplicate_email"),
      other => panic!("expected a unique Sk index, got {other:?}"),
    }
  }

  #[test]
  fn an_rk_index_on_a_numeric_field_is_accepted() {
    let def = DatumTypeDef::new("player")
      .field("score", FieldType::I64)
      .index(IndexDef::Rk {
        field: "score".to_string(),
      });
    assert_eq!(def.indexes.len(), 1);
  }

  #[test]
  #[should_panic(expected = "rk index field")]
  fn an_rk_index_on_a_string_field_panics_at_declaration() {
    DatumTypeDef::new("player")
      .field("name", FieldType::String)
      .index(IndexDef::Rk {
        field: "name".to_string(),
      });
  }

  #[test]
  #[should_panic(expected = "rk index field")]
  fn an_rk_index_on_an_undeclared_field_panics_at_declaration() {
    DatumTypeDef::new("player").index(IndexDef::Rk {
      field: "score".to_string(),
    });
  }

  #[test]
  fn encode_rejects_a_value_that_does_not_match_its_fields_declared_type() {
    let def = user_type();
    let values = vec![
      FieldValue::String("cliff".to_string()),
      FieldValue::String("not a number".to_string()), // "age" is declared I64
    ];
    assert!(encode_datum(&def, &values).is_err());
  }

  #[test]
  fn decode_rejects_bytes_stamped_with_a_different_schema_version() {
    let def_v1 = user_type();
    let def_v2 = user_type().version(2);
    let values = vec![FieldValue::String("cliff".to_string()), FieldValue::I64(41)];
    let encoded_v1 = encode_datum(&def_v1, &values).unwrap();
    let err = decode_datum(&def_v2, &encoded_v1).unwrap_err();
    assert!(err.to_string().contains("schema version"), "{err}");
    // Same layout, matching version: still round-trips.
    assert_eq!(decode_datum(&def_v1, &encoded_v1).unwrap(), values);
  }

  #[test]
  fn decode_rejects_bytes_too_short_for_a_version_prefix() {
    assert!(decode_datum(&user_type(), &[0x01]).is_err());
  }

  #[test]
  fn enum_pk_id_is_stable_and_distinct_per_type_and_mnemonic() {
    assert_eq!(
      enum_pk_id("status", "active"),
      enum_pk_id("status", "active")
    );
    assert_ne!(
      enum_pk_id("status", "active"),
      enum_pk_id("status", "closed")
    );
    assert_ne!(enum_pk_id("status", "active"), enum_pk_id("kind", "active"));
  }

  #[test]
  fn check_pk_enforces_v7_on_uuid_types() {
    let def = user_type(); // default PkKind::Uuid
    assert!(check_pk(seisin_core::datum::DatumId::new(), &def).is_ok());
    // A derived (v5) id is not a v7 uuid.
    let derived = enum_pk_id("status", "active");
    assert!(check_pk(derived, &def).is_err());
  }

  #[test]
  fn check_pk_enforces_membership_on_enum_types() {
    let def = DatumTypeDef::new("status")
      .pk(PkKind::Enum(vec![
        "active".to_string(),
        "closed".to_string(),
      ]))
      .field("label", FieldType::String);
    assert!(check_pk(enum_pk_id("status", "active"), &def).is_ok());
    assert!(check_pk(enum_pk_id("status", "closed"), &def).is_ok());
    assert!(check_pk(seisin_core::datum::DatumId::new(), &def).is_err());
    assert!(check_pk(enum_pk_id("status", "bogus"), &def).is_err());
    assert!(check_pk(enum_pk_id("kind", "active"), &def).is_err());
  }

  #[test]
  #[should_panic(expected = "not a declared field")]
  fn a_constraint_on_an_unknown_field_panics() {
    DatumTypeDef::new("order").constraint(RelationalConstraintDef {
      field: "nope".to_string(),
      references: FkTarget::PkUuid {
        type_name: "customer".to_string(),
      },
      resolution: None,
    });
  }

  #[test]
  #[should_panic(expected = "must be String")]
  fn a_pk_enum_constraint_on_a_non_string_field_panics() {
    DatumTypeDef::new("order")
      .field("status", FieldType::I64)
      .constraint(RelationalConstraintDef {
        field: "status".to_string(),
        references: FkTarget::PkEnum {
          type_name: "status".to_string(),
          mnemonics: vec!["active".to_string()],
        },
        resolution: None,
      });
  }

  #[test]
  #[should_panic(expected = "must be Bytes")]
  fn a_pk_uuid_constraint_on_a_non_bytes_field_panics() {
    DatumTypeDef::new("order")
      .field("customer_id", FieldType::String)
      .constraint(RelationalConstraintDef {
        field: "customer_id".to_string(),
        references: FkTarget::PkUuid {
          type_name: "customer".to_string(),
        },
        resolution: None,
      });
  }

  #[test]
  #[should_panic(expected = "primitive natural-key")]
  fn an_sk_unique_constraint_on_an_array_field_panics() {
    DatumTypeDef::new("order")
      .field("emails", FieldType::Array(Box::new(FieldType::String)))
      .constraint(RelationalConstraintDef {
        field: "emails".to_string(),
        references: FkTarget::SkUnique {
          type_name: "user".to_string(),
          field: "email".to_string(),
        },
        resolution: None,
      });
  }

  #[test]
  fn decode_rejects_bytes_with_a_trailing_garbage() {
    let def = user_type();
    let values = vec![FieldValue::String("cliff".to_string()), FieldValue::I64(41)];
    let mut encoded = encode_datum(&def, &values).unwrap();
    encoded.push(0xFF); // trailing byte no field consumes
    assert!(decode_datum(&def, &encoded).is_err());
  }
}
