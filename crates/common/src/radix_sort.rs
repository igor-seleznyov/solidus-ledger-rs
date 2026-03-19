pub fn radix_sort_by_id_lo(
    keys: &[u64],
    indices: &mut [u16],
    temp: &mut [u16],
    count: usize,
) {
    if count == 0 {
        return;
    }
    if count == 1 {
        indices[0] = 0;
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
    if count == 0 {
        return;
    }
    if count == 1 {
        indices[0] = 0;
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

        // Порядок сохранён
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
        // 3 трансфера: A(2 сообщения), B(2), C(1)
        let keys = vec![100u64, 200, 100, 300, 200];
        let mut indices = [0u16; 5];
        let mut temp = [0u16; 5];
        radix_sort_by_id_lo(&keys, &mut indices, &mut temp, 5);

        // После сортировки: одинаковые ключи рядом
        let sorted_keys: Vec<u64> = (0..5).map(|i| keys[indices[i] as usize]).collect();

        // Проверяем группировку: все 100 рядом, все 200 рядом
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
        // Два сообщения с одинаковым ключом — порядок сохранён
        let keys = vec![42u64, 42, 42];
        let mut indices = [0u16; 3];
        let mut temp = [0u16; 3];
        radix_sort_by_id_lo(&keys, &mut indices, &mut temp, 3);

        // Stable: оригинальный порядок сохранён
        assert_eq!(indices[0], 0);
        assert_eq!(indices[1], 1);
        assert_eq!(indices[2], 2);
    }

    #[test]
    fn large_keys_grouped_by_low_16_bits() {
        // Ключи отличаются только в младших 16 битах
        let keys = vec![
            0xAAAA_BBBB_0001_0002u64,  // low16 = 0x0002
            0xCCCC_DDDD_0001_0001u64,  // low16 = 0x0001
            0xEEEE_FFFF_0001_0002u64,  // low16 = 0x0002
            0x1111_2222_0001_0001u64,  // low16 = 0x0001
        ];
        let mut indices = [0u16; 4];
        let mut temp = [0u16; 4];
        radix_sort_by_id_lo(&keys, &mut indices, &mut temp, 4);

        // 0x0001 группа первая, 0x0002 — вторая
        let sorted: Vec<u64> = (0..4).map(|i| keys[indices[i] as usize] & 0xFFFF).collect();
        assert_eq!(sorted[0], 0x0001);
        assert_eq!(sorted[1], 0x0001);
        assert_eq!(sorted[2], 0x0002);
        assert_eq!(sorted[3], 0x0002);
    }

    #[test]
    fn batch_128_random_keys() {
        // Реалистичный размер batch
        let keys: Vec<u64> = (0..128).map(|i| (i * 7 + 13) % 20).collect();
        let mut indices = [0u16; 128];
        let mut temp = [0u16; 128];
        radix_sort_by_id_lo(&keys, &mut indices, &mut temp, 128);

        // Проверяем что отсортировано по младшим 16 битам
        for i in 0..127 {
            let a = keys[indices[i] as usize] & 0xFFFF;
            let b = keys[indices[i + 1] as usize] & 0xFFFF;
            assert!(a <= b, "not sorted at {}: {} > {}", i, a, b);
        }
    }

    // --- Тесты 4-проходного варианта ---

    #[test]
    fn full_sort_groups_by_hi_and_lo() {
        // Два трансфера с одинаковым lo, разным hi
        let keys_lo = vec![100u64, 100, 200, 100];
        let keys_hi = vec![1u64,   2,   1,   1];
        let mut indices = [0u16; 4];
        let mut temp = [0u16; 4];
        radix_sort_by_full_id(&keys_lo, &keys_hi, &mut indices, &mut temp, 4);

        // После полной сортировки: (hi=1,lo=100), (hi=1,lo=100), (hi=1,lo=200), (hi=2,lo=100)
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
        // Три сообщения одного трансфера — порядок сохранён
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