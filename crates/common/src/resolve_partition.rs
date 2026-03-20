use crate::siphash::siphash13;

#[inline(always)]
pub fn resolve_partition(
    account_id_hi: u64,
    account_id_lo: u64,
    seed_k0: u64,
    seed_k1: u64,
    mask: usize
) -> usize {
    (siphash13(seed_k0, seed_k1, account_id_hi, account_id_lo) as usize) & mask
}

#[cfg(test)]
mod tests {
    use super::*;

    const K0: u64 = 0x0123456789ABCDEF;
    const K1: u64 = 0xFEDCBA9876543210;

    #[test]
    fn partition_within_range() {
        // 16 партиций, mask = 15
        for i in 0..100u64 {
            let p = resolve_partition(0, i, K0, K1, 15);
            assert!(p < 16, "partition {} out of range for 16 partitions", p);
        }
    }

    #[test]
    fn deterministic() {
        let p1 = resolve_partition(0, 42, K0, K1, 15);
        let p2 = resolve_partition(0, 42, K0, K1, 15);
        assert_eq!(p1, p2, "same account must always map to same partition");
    }

    #[test]
    fn different_accounts_distribute() {
        // 16 партиций, 1000 аккаунтов — все 16 должны быть задействованы
        let mut seen = [false; 16];
        for i in 0..1000u64 {
            let p = resolve_partition(0, i, K0, K1, 15);
            seen[p] = true;
        }
        for (i, &s) in seen.iter().enumerate() {
            assert!(s, "partition {} never used with 1000 accounts", i);
        }
    }

    #[test]
    fn different_seeds_different_mapping() {
        let p1 = resolve_partition(0, 42, K0, K1, 15);
        let p2 = resolve_partition(0, 42, 99, 100, 15);
        // Разные seed'ы с высокой вероятностью дают разные партиции
        // (не гарантировано, но для конкретных значений проверяем)
        assert_ne!(p1, p2);
    }
}