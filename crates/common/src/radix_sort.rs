pub fn radix_sort_by_id_lo(
    keys: &[u64],
    indices: &mut [u16],
    temp: &mut [u16],
    count: usize,
) {
    if count <= 1 {
        return;
    }

    for i in 0..count {
        indices[i] = i as u16;
    }

    counting_sort_pass(keys, indices, temp, count, 0);
    counting_sort_pass(keys, temp, indices, count, 8);
}

pub fn radix_sort_by_full_id(
    keys_lo: &[u64],
    keys_hi: &[u64],
    indices: &mut [u16],
    temp: &mut [u16],
    count: usize,
) {
    if count <= 1 {
        return;
    }

    for i in 0..count {
        indices[i] = i as u16;
    }

    counting_sort_pass(keys_lo, indices, temp, count, 0);
    counting_sort_pass(keys_lo, temp, indices, count, 8);

    counting_sort_pass(keys_hi, indices, temp, count, 0);
    counting_sort_pass(keys_hi, temp, indices, count, 8);
}

fn counting_sort_pass(
    keys: &[u64],
    src: &[u16],
    dest: &mut [u16],
    count: usize,
    shift: u32,
) {
    let mut counts = [0u32; 256];
    for i in 0..count {
        let byte = ((keys[src[i] as usize] >> shift) & 0xff) as usize;
        counts[byte] += 1;
    }

    let mut total = 0u32;
    for c in counts.iter_mut() {
        let current = *c;
        *c = total;
        total += current;
    }

    for i in 0..count {
        let byte = ((keys[src[i] as usize] >> shift) & 0xff) as usize;
        dest[counts[byte] as usize] = src[i];
        counts[byte] += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_batch() {
        let keys: Vec<u64> = vec![];
        let mut indices = [0u16; 0];
        let mut temp = [0u16; 0];
        radix_sort_by_id_lo(&keys, &mut indices, &mut temp, 0);
    }

    #[test]
    fn single_element() {
        let keys = vec![42u64];
        let mut indices = [0u16; 1];
        let mut temp = [0u16; 1];
        radix_sort_by_id_lo(&keys, &mut indices, &mut temp, 1);
        assert_eq!(indices[0], 0);
    }

    #[test]
    fn already_sorted() {
        let keys = vec![1u64, 2, 3, 4, 5];
        let mut indices = [0u16; 5];
        let mut temp = [0u16; 5];
        radix_sort_by_id_lo(&keys, &mut indices, &mut temp, 5);

        for i in 0..5 {
            assert_eq!(keys[indices[i] as usize], (i + 1) as u64);
        }
    }

    #[test]
    fn reverse_sorted() {
        let keys = vec![5u64, 4, 3, 2, 1];
        let mut indices = [0u16; 5];
        let mut temp = [0u16; 5];
        radix_sort_by_id_lo(&keys, &mut indices, &mut temp, 5);

        for i in 0..4 {
            assert!(
                keys[indices[i] as usize] <= keys[indices[i + 1] as usize],
                "not sorted at position {}", i,
            );
        }
    }

    #[test]
    fn groups_same_keys_together() {
        let keys = vec![100u64, 200, 100, 300, 200];
        let mut indices = [0u16; 5];
        let mut temp = [0u16; 5];
        radix_sort_by_id_lo(&keys, &mut indices, &mut temp, 5);

        let sorted_keys: Vec<u64> = (0..5).map(|i| keys[indices[i] as usize]).collect();

        let mut groups: Vec<Vec<u64>> = vec![];
        let mut current_group = vec![sorted_keys[0]];
        for i in 1..5 {
            if sorted_keys[i] == sorted_keys[i - 1] {
                current_group.push(sorted_keys[i]);
            } else {
                groups.push(current_group);
                current_group = vec![sorted_keys[i]];
            }
        }
        groups.push(current_group);

        assert_eq!(groups.len(), 3);
        assert!(groups.iter().any(|g| g.len() == 2 && g[0] == 100));
        assert!(groups.iter().any(|g| g.len() == 2 && g[0] == 200));
        assert!(groups.iter().any(|g| g.len() == 1 && g[0] == 300));
    }

    #[test]
    fn stable_sort_preserves_order_within_group() {
        let keys = vec![42u64, 42, 42];
        let mut indices = [0u16; 3];
        let mut temp = [0u16; 3];
        radix_sort_by_id_lo(&keys, &mut indices, &mut temp, 3);

        assert_eq!(indices[0], 0);
        assert_eq!(indices[1], 1);
        assert_eq!(indices[2], 2);
    }

    #[test]
    fn large_keys_grouped_by_low_16_bits() {
        let keys = vec![
            0xAAAA_BBBB_0001_0002u64,
            0xCCCC_DDDD_0001_0001u64,
            0xEEEE_FFFF_0001_0002u64,
            0x1111_2222_0001_0001u64,
        ];
        let mut indices = [0u16; 4];
        let mut temp = [0u16; 4];
        radix_sort_by_id_lo(&keys, &mut indices, &mut temp, 4);

        let sorted: Vec<u64> = (0..4).map(|i| keys[indices[i] as usize] & 0xFFFF).collect();
        assert_eq!(sorted[0], 0x0001);
        assert_eq!(sorted[1], 0x0001);
        assert_eq!(sorted[2], 0x0002);
        assert_eq!(sorted[3], 0x0002);
    }

    #[test]
    fn batch_128_random_keys() {
        let keys: Vec<u64> = (0..128).map(|i| (i * 7 + 13) % 20).collect();
        let mut indices = [0u16; 128];
        let mut temp = [0u16; 128];
        radix_sort_by_id_lo(&keys, &mut indices, &mut temp, 128);

        for i in 0..127 {
            let a = keys[indices[i] as usize] & 0xFFFF;
            let b = keys[indices[i + 1] as usize] & 0xFFFF;
            assert!(a <= b, "not sorted at {}: {} > {}", i, a, b);
        }
    }

    #[test]
    fn full_sort_groups_by_hi_and_lo() {
        let keys_lo = vec![100u64, 100, 200, 100];
        let keys_hi = vec![1u64,   2,   1,   1];
        let mut indices = [0u16; 4];
        let mut temp = [0u16; 4];
        radix_sort_by_full_id(&keys_lo, &keys_hi, &mut indices, &mut temp, 4);

        let sorted: Vec<(u64, u64)> = (0..4)
            .map(|i| (keys_hi[indices[i] as usize], keys_lo[indices[i] as usize]))
            .collect();

        assert_eq!(sorted[0], (1, 100));
        assert_eq!(sorted[1], (1, 100));
        assert_eq!(sorted[2], (1, 200));
        assert_eq!(sorted[3], (2, 100));
    }

    #[test]
    fn full_sort_stable() {
        let keys_lo = vec![42u64, 42, 42];
        let keys_hi = vec![1u64, 1, 1];
        let mut indices = [0u16; 3];
        let mut temp = [0u16; 3];
        radix_sort_by_full_id(&keys_lo, &keys_hi, &mut indices, &mut temp, 3);

        assert_eq!(indices[0], 0);
        assert_eq!(indices[1], 1);
        assert_eq!(indices[2], 2);
    }
}