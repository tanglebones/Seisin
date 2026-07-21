//! Hand-rolled binary encoding for `FieldValue`s, driven by the declared
//! `FieldType` at each position — no per-value type tags are written,
//! since the schema already tells the decoder what to expect. Matches
//! this project's existing style (`seisin-protocol`, `seisin-core::sk`),
//! not a serde-based encoding.

use anyhow::{bail, Context, Result};

use crate::field::{FieldType, FieldValue};

pub fn encode_field_value(value: &FieldValue, buf: &mut Vec<u8>) {
  match value {
    FieldValue::Bool(b) => buf.push(u8::from(*b)),
    FieldValue::I64(i) => buf.extend_from_slice(&i.to_le_bytes()),
    FieldValue::F64(f) => buf.extend_from_slice(&f.to_le_bytes()),
    FieldValue::String(s) => encode_len_prefixed(s.as_bytes(), buf),
    FieldValue::Bytes(b) => encode_len_prefixed(b, buf),
    FieldValue::Array(items) => {
      buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
      for item in items {
        encode_field_value(item, buf);
      }
    }
    FieldValue::Dict(entries) => {
      buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
      for (k, v) in entries {
        encode_field_value(k, buf);
        encode_field_value(v, buf);
      }
    }
  }
}

fn encode_len_prefixed(bytes: &[u8], buf: &mut Vec<u8>) {
  buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
  buf.extend_from_slice(bytes);
}

pub fn decode_field_value(ty: &FieldType, buf: &[u8], offset: &mut usize) -> Result<FieldValue> {
  match ty {
    FieldType::Bool => {
      let b = read_bytes(buf, offset, 1)?[0];
      Ok(FieldValue::Bool(b != 0))
    }
    FieldType::I64 => {
      let bytes: [u8; 8] = read_bytes(buf, offset, 8)?.try_into().unwrap();
      Ok(FieldValue::I64(i64::from_le_bytes(bytes)))
    }
    FieldType::F64 => {
      let bytes: [u8; 8] = read_bytes(buf, offset, 8)?.try_into().unwrap();
      Ok(FieldValue::F64(f64::from_le_bytes(bytes)))
    }
    FieldType::String => {
      let bytes = decode_len_prefixed(buf, offset)?;
      Ok(FieldValue::String(
        String::from_utf8(bytes).context("string field was not valid utf8")?,
      ))
    }
    FieldType::Bytes => Ok(FieldValue::Bytes(decode_len_prefixed(buf, offset)?)),
    FieldType::Array(inner) => {
      let count = read_u32(buf, offset)?;
      let mut items = Vec::with_capacity(count as usize);
      for _ in 0..count {
        items.push(decode_field_value(inner, buf, offset)?);
      }
      Ok(FieldValue::Array(items))
    }
    FieldType::Dict(key_ty, val_ty) => {
      let key_ty: FieldType = (*key_ty).into();
      let count = read_u32(buf, offset)?;
      let mut entries = Vec::with_capacity(count as usize);
      for _ in 0..count {
        let key = decode_field_value(&key_ty, buf, offset)?;
        let value = decode_field_value(val_ty, buf, offset)?;
        entries.push((key, value));
      }
      Ok(FieldValue::Dict(entries))
    }
  }
}

fn read_bytes<'a>(buf: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8]> {
  if *offset + len > buf.len() {
    bail!(
      "buffer truncated: need {len} bytes at offset {offset}, only {} remain",
      buf.len() - *offset
    );
  }
  let slice = &buf[*offset..*offset + len];
  *offset += len;
  Ok(slice)
}

fn read_u32(buf: &[u8], offset: &mut usize) -> Result<u32> {
  let bytes: [u8; 4] = read_bytes(buf, offset, 4)?.try_into().unwrap();
  Ok(u32::from_le_bytes(bytes))
}

fn decode_len_prefixed(buf: &[u8], offset: &mut usize) -> Result<Vec<u8>> {
  let len = read_u32(buf, offset)? as usize;
  Ok(read_bytes(buf, offset, len)?.to_vec())
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::field::{FieldType, FieldValue, PrimitiveFieldType};

  fn round_trip(ty: &FieldType, value: &FieldValue) {
    let mut buf = Vec::new();
    encode_field_value(value, &mut buf);
    let mut offset = 0;
    let decoded = decode_field_value(ty, &buf, &mut offset).unwrap();
    assert_eq!(&decoded, value);
    assert_eq!(
      offset,
      buf.len(),
      "decode must consume exactly the encoded bytes"
    );
  }

  #[test]
  fn round_trips_bool() {
    round_trip(&FieldType::Bool, &FieldValue::Bool(true));
    round_trip(&FieldType::Bool, &FieldValue::Bool(false));
  }

  #[test]
  fn round_trips_i64_including_negative() {
    round_trip(&FieldType::I64, &FieldValue::I64(-42));
    round_trip(&FieldType::I64, &FieldValue::I64(i64::MAX));
  }

  #[test]
  fn round_trips_f64() {
    round_trip(&FieldType::F64, &FieldValue::F64(3.5));
  }

  #[test]
  fn round_trips_string() {
    round_trip(&FieldType::String, &FieldValue::String("hello".to_string()));
    round_trip(&FieldType::String, &FieldValue::String(String::new()));
  }

  #[test]
  fn round_trips_bytes() {
    round_trip(&FieldType::Bytes, &FieldValue::Bytes(vec![1, 2, 3]));
  }

  #[test]
  fn round_trips_an_array_of_i64() {
    let ty = FieldType::Array(Box::new(FieldType::I64));
    let value = FieldValue::Array(vec![
      FieldValue::I64(1),
      FieldValue::I64(2),
      FieldValue::I64(3),
    ]);
    round_trip(&ty, &value);
  }

  #[test]
  fn round_trips_an_empty_array() {
    let ty = FieldType::Array(Box::new(FieldType::String));
    round_trip(&ty, &FieldValue::Array(vec![]));
  }

  #[test]
  fn round_trips_a_dict_of_string_to_i64() {
    let ty = FieldType::Dict(PrimitiveFieldType::String, Box::new(FieldType::I64));
    let value = FieldValue::Dict(vec![
      (FieldValue::String("a".to_string()), FieldValue::I64(1)),
      (FieldValue::String("b".to_string()), FieldValue::I64(2)),
    ]);
    round_trip(&ty, &value);
  }

  #[test]
  fn round_trips_a_nested_array_of_dicts() {
    let ty = FieldType::Array(Box::new(FieldType::Dict(
      PrimitiveFieldType::String,
      Box::new(FieldType::Bool),
    )));
    let value = FieldValue::Array(vec![
      FieldValue::Dict(vec![(
        FieldValue::String("k".to_string()),
        FieldValue::Bool(true),
      )]),
      FieldValue::Dict(vec![]),
    ]);
    round_trip(&ty, &value);
  }

  #[test]
  fn decode_rejects_a_truncated_string_length_prefix() {
    let buf = vec![0u8, 1]; // claims a 2-byte len prefix worth of u32 but only has 2 bytes total
    let mut offset = 0;
    assert!(decode_field_value(&FieldType::String, &buf, &mut offset).is_err());
  }
}
