#[inline]
pub fn siphash13(k0: u64, k1: u64, m0: u64, m1: u64) -> u64 {
    let mut v0: u64 = k0 ^ 0x736f6d6570736575;
    let mut v1: u64 = k1 ^ 0x646f72616e646f6d;
    let mut v2: u64 = k0 ^ 0x6c7967656e657261;
    let mut v3: u64 = k1 ^ 0x7465646279746573;

    v3 ^= m0;
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    v0 ^= m0;

    v3 ^= m1;
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    v0 ^= m1;

    let fin = 2u64 << 56;
    v3 ^= fin;
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    v0 ^= fin;

    v2 ^= 0xff;
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);
    sip_round(&mut v0, &mut v1, &mut v2, &mut v3);

    v0 ^ v1 ^ v2 ^ v3
}

#[inline(always)]
fn sip_round(v0: &mut u64, v1: &mut u64, v2: &mut u64, v3: &mut u64) {
    *v0 = v0.wrapping_add(*v1);
    *v1 = v1.rotate_left(13);
    *v1 ^= *v0;
    *v0 = v0.rotate_left(32);

    *v2 = v2.wrapping_add(*v3);
    *v3 = v3.rotate_left(16);
    *v3 ^= *v2;


    *v0 = v0.wrapping_add(*v3);
    *v3 = v3.rotate_left(21);
    *v3 ^= *v0;

    *v2 = v2.wrapping_add(*v1);
    *v1 = v1.rotate_left(17);
    *v1 ^= *v2;
    *v2 = v2.rotate_left(32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic() {
        let h1 = siphash13(1, 2, 100, 200);
        let h2 = siphash13(1, 2, 100, 200);
        assert_eq!(h1, h2, "same input must produce same hash");
    }

    #[test]
    fn different_inputs_different_hashes() {
        let h1 = siphash13(1, 2, 100, 200);
        let h2 = siphash13(1, 2, 100, 201);
        assert_ne!(h1, h2);
    }

    #[test]
    fn different_seeds_different_hashes() {
        let h1 = siphash13(1, 2, 100, 200);
        let h2 = siphash13(3, 4, 100, 200);
        assert_ne!(h1, h2);
    }

    #[test]
    fn non_zero() {
        let h = siphash13(0, 0, 0, 0);
        assert_ne!(h, 0);
    }

    #[test]
    fn distribution() {
        let mut or_all: u64 = 0;
        for i in 0..1000u64 {
            or_all |= siphash13(42, 43, i, i * 7);
        }
        assert_eq!(or_all, u64::MAX, "poor distribution: not all bits set");
    }
}