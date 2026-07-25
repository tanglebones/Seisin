//! Table-based CRC-32 (IEEE 802.3 polynomial) — the delta log's record
//! checksum. Hand-rolled rather than a dependency, matching this
//! project's no-new-deps stance for small closed-form problems.

fn table() -> [u32; 256] {
  let mut table = [0u32; 256];
  let mut i = 0;
  while i < 256 {
    let mut crc = i as u32;
    let mut bit = 0;
    while bit < 8 {
      crc = if crc & 1 != 0 {
        0xEDB8_8320 ^ (crc >> 1)
      } else {
        crc >> 1
      };
      bit += 1;
    }
    table[i] = crc;
    i += 1;
  }
  table
}

pub fn crc32(bytes: &[u8]) -> u32 {
  let table = table();
  let mut crc = 0xFFFF_FFFFu32;
  for byte in bytes {
    crc = table[((crc ^ *byte as u32) & 0xFF) as usize] ^ (crc >> 8);
  }
  !crc
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn matches_the_ieee_known_answer_vector() {
    assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    assert_eq!(crc32(b""), 0);
  }
}
