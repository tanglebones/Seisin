//! Jump-consistent-hash, vendored from
//! https://github.com/tanglebones/consistent_hash (the `JumpBack`
//! variant, implementing https://arxiv.org/pdf/2403.18682) rather than
//! taken as a dependency, per that repo's own guidance for a small,
//! stable algorithm like this one. Field/type names adapted to this
//! project's style; the algorithm itself is unchanged.

/// Deterministic 64-bit RNG (SplitMix64) used by the hasher.
struct SplitMix64 {
  state: u64,
}

impl Default for SplitMix64 {
  fn default() -> Self {
    Self { state: 0 }
  }
}

impl SplitMix64 {
  fn reset_with_seed(&mut self, seed: u64) {
    self.state = seed;
  }

  fn next_long(&mut self) -> u64 {
    self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = self.state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
  }
}

/// Maps key `k` deterministically to a bucket in `[0, n)`. Growing or
/// shrinking `n` by one at the boundary remaps only ~1/n keys — see the
/// design doc's "Compute Ring Mechanics" section for how `seisin-ring`
/// applies the swap-with-last technique on top of this primitive to
/// support removing an arbitrary (not just the highest-index) bucket
/// while keeping that guarantee.
pub struct JumpBackHasher {
  rng: SplitMix64,
}

impl Default for JumpBackHasher {
  fn default() -> Self {
    Self { rng: SplitMix64::default() }
  }
}

impl JumpBackHasher {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn hash(&mut self, k: u64, n: u32) -> u32 {
    if n <= 1 {
      return 0;
    }

    self.rng.reset_with_seed(k);
    let v = self.rng.next_long();

    let n_minus_1 = n - 1;
    let mask: u32 = (!0u32) >> n_minus_1.leading_zeros();
    let u: u32 = ((v ^ (v >> 32)) as u32) & mask;

    let mut u_work = u;
    while u_work != 0 {
      let q: u32 = 1u32 << (31 - u_work.leading_zeros());
      let shift: u32 = ((u_work.count_ones() << 5) & 63) as u32;
      let b0: u32 = ((v >> shift) as u32) & (q - 1);
      let mut b: u32 = q.wrapping_add(b0);

      loop {
        if b < n {
          return b;
        }
        let w = self.rng.next_long();

        let mask2: u32 = if q == 0x8000_0000 { 0xFFFF_FFFF } else { (q << 1) - 1 };

        b = (w as u32) & mask2;
        if b < q {
          break;
        }
        if b < n {
          return b;
        }
        b = ((w >> 32) as u32) & mask2;
        if b < q {
          break;
        }
        if b < n {
          return b;
        }
      }

      u_work ^= q;
    }

    0
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn trivial_cases_match_the_upstream_crate() {
    let mut h = JumpBackHasher::new();
    assert_eq!(h.hash(0, 0), 0);
    assert_eq!(h.hash(0, 1), 0);
    assert_eq!(h.hash(1, 1), 0);
    assert_eq!(h.hash(0, 2), 0);
    assert_eq!(h.hash(1, 2), 1);
  }

  #[test]
  fn result_is_always_in_range() {
    let mut h = JumpBackHasher::new();
    for n in [2u32, 3, 4, 7, 16, 31, 32, 33, 1000] {
      for k in [0u64, 1, 2, 123456789, u64::MAX - 1, u64::MAX] {
        let r = h.hash(k, n);
        assert!(r < n, "k={k}, n={n}, r={r}");
      }
    }
  }
}
