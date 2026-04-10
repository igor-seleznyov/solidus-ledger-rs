use common::siphash::siphash13;
use ringbuf::hash_table_slot_status::{SLOT_FREE, SLOT_OCCUPIED};

pub struct InFlightMinHeap {
    heap: Vec<u64>,
    index_slots: Vec<IndexSlot>,
    index_mask: usize,
    index_count: usize,
    seed_k0: u64,
    seed_k1: u64,
    max_resize_count: u32,
    growth_factor: usize,
    load_factor_threshold_num: usize,
    load_factor_threshold_den: usize,
    resize_count: u32,
    last_removed_gsn: u64,
}

#[repr(C)]
#[derive(Copy, Clone)]
struct IndexSlot {
    gsn: u64,
    heap_pos: u32,
    psl: u8,
    status: u8,
    _pad: [u8; 2],
}

impl IndexSlot {
    fn zeroed() -> Self {
        Self { gsn: 0, heap_pos: 0, psl: 0, status: SLOT_FREE, _pad: [0; 2] }
    }
}

impl InFlightMinHeap {
    pub fn new(
        index_capacity: usize,
        seed_k0: u64,
        seed_k1: u64,
        max_resize_count: u32,
        growth_factor: usize,
    ) -> Self {
        assert!(index_capacity.is_power_of_two());
        assert!(growth_factor == 2 || growth_factor == 4, "growth_factor must be 2 or 4");

        let index_slots = vec![IndexSlot::zeroed(); index_capacity];

        Self {
            heap: Vec::with_capacity(index_capacity),
            index_slots,
            index_mask: index_capacity - 1,
            index_count: 0,
            seed_k0,
            seed_k1,
            max_resize_count,
            growth_factor,
            load_factor_threshold_num: 3,
            load_factor_threshold_den: 4,
            resize_count: 0,
            last_removed_gsn: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    pub fn min(&self) -> Option<u64> {
        self.heap.first().copied()
    }

    pub fn last_removed_gsn(&self) -> u64 {
        self.last_removed_gsn
    }

    pub fn add(&mut self, gsn: u64) -> bool {
        let capacity = self.index_slots.len();
        if self.index_count * self.load_factor_threshold_den >= capacity * self.load_factor_threshold_num {
            if !self.try_resize() {
                return false;
            }
        }

        let pos = self.heap.len();
        self.heap.push(gsn);
        let final_pos = self.sift_up(pos);
        self.index_insert(gsn, final_pos as u32);
        true
    }

    fn try_resize(&mut self) -> bool {
        if self.max_resize_count > 0 && self.resize_count >= self.max_resize_count {
            return false;
        }

        let old_capacity = self.index_slots.len();
        let new_capacity = old_capacity * self.growth_factor;

        let mut new_slots = vec![IndexSlot::zeroed(); new_capacity];
        let new_mask = new_capacity - 1;

        for i in 0..old_capacity {
            let slot = &self.index_slots[i];
            if slot.status == SLOT_OCCUPIED {
                let mut pos = siphash13(
                    self.seed_k0,
                    self.seed_k1,
                    slot.gsn,
                    0,
                ) as usize & new_mask;
                let mut psl: u8 = 1;
                let mut inserting = IndexSlot {
                    gsn: slot.gsn,
                    heap_pos: slot.heap_pos,
                    psl: 1,
                    status: SLOT_OCCUPIED,
                    _pad: [0; 2],
                };

                loop {
                    let target = &mut new_slots[pos];
                    if target.status == SLOT_FREE {
                        *target = inserting;
                        break;
                    }
                    if target.psl < psl {
                        std::mem::swap(target, &mut inserting);
                        psl = inserting.psl;
                    }
                    pos = (pos + 1) & new_mask;
                    psl += 1;
                    inserting.psl = psl;
                }
            }
        }

        self.index_slots = new_slots;
        self.index_mask = new_mask;
        self.resize_count += 1;

        println!(
            "[IFMH] resized: {} -> {} (resize #{}/{})",
            old_capacity, new_capacity,
            self.resize_count,
            if self.max_resize_count == 0 { "∞".to_string() } else { self.max_resize_count.to_string() },
        );

        true
    }

    fn sift_up(&mut self, mut pos: usize) -> usize {
        while pos > 0 {
            let parent = (pos - 1) >> 1;
            if self.heap[pos] >= self.heap[parent] {
                break;
            }
            self.heap.swap(pos, parent);
            self.index_update_pos(self.heap[pos], pos as u32);
            pos = parent;
        }
        pos
    }

    pub fn remove(&mut self, gsn: u64) {
        let pos = match self.index_lookup(gsn) {
            Some(pos) => pos as usize,
            None => return,
        };

        self.last_removed_gsn = self.last_removed_gsn.max(gsn);

        let last = self.heap.len() - 1;

        if pos == last {
            self.heap.pop();
        } else {
            let last_gsn = self.heap[last];
            self.heap.swap(pos, last);
            self.heap.pop();

            self.index_update_pos(last_gsn, pos as u32);

            if pos > 0 && self.heap[pos] < self.heap[(pos - 1) >> 1] {
                let final_pos = self.sift_up(pos);
            } else {
                self.sift_down(pos);
            }
        }

        self.index_remove(gsn);
    }

    fn sift_down(&mut self, mut pos: usize) {
        let len = self.heap.len();
        loop {
            let left = 2 * pos + 1;
            let right = 2 * pos + 2;
            let mut smallest = pos;

            if left < len && self.heap[left] < self.heap[smallest] {
                smallest = left;
            }
            if right < len && self.heap[right] < self.heap[smallest] {
                smallest = right;
            }

            if smallest == pos {
                break;
            }

            self.heap.swap(pos, smallest);
            self.index_update_pos(self.heap[pos], pos as u32);
            pos = smallest;
        }

        self.index_update_pos(self.heap[pos], pos as u32);
    }

    fn hash_gsn(&self, gsn: u64) -> usize {
        siphash13(self.seed_k0, self.seed_k1, gsn, 0) as usize & self.index_mask
    }

    fn index_insert(&mut self, gsn: u64, heap_pos: u32) {
        let mut pos = self.hash_gsn(gsn);
        let mut psl: u8 = 1;
        let mut inserting = IndexSlot {
            gsn,
            heap_pos,
            psl: 1,
            status: SLOT_OCCUPIED,
            _pad: [0; 2],
        };

        loop {
            let slot = &mut self.index_slots[pos];
            if slot.status == SLOT_FREE {
                *slot = inserting;
                self.index_count += 1;
                return;
            }
            if slot.psl < psl {
                std::mem::swap(slot, &mut inserting);
                psl = inserting.psl;
            }
            pos = (pos + 1) & self.index_mask;
            psl += 1;
            inserting.psl = psl;
        }
    }

    fn index_lookup(&self, gsn: u64) -> Option<u32> {
        let mut pos = self.hash_gsn(gsn);
        let mut psl: u8 = 1;

        loop {
            let slot = &self.index_slots[pos];
            if slot.status == SLOT_FREE {
                return None;
            }
            if slot.psl < psl {
                return None;
            }
            if slot.gsn == gsn {
                return Some(slot.heap_pos);
            }
            pos = (pos + 1) & self.index_mask;
            psl += 1;
        }
    }

    fn index_update_pos(&mut self, gsn: u64, new_heap_pos: u32) {
        let mut pos = self.hash_gsn(gsn);
        let mut psl: u8 = 1;

        loop {
            let slot = &mut self.index_slots[pos];
            if slot.status == SLOT_FREE {
                return;
            }
            if slot.psl < psl {
                return;
            }
            if slot.gsn == gsn {
                slot.heap_pos = new_heap_pos;
                return;
            }
            pos = (pos + 1) & self.index_mask;
            psl += 1;
        }
    }

    fn index_remove(&mut self, gsn: u64) {
        let mut pos = self.hash_gsn(gsn);
        let mut psl: u8 = 1;

        loop {
            let slot = &self.index_slots[pos];
            if slot.status == SLOT_FREE {
                return;
            }
            if slot.psl < psl {
                return;
            }
            if slot.gsn == gsn {
                break;
            }
            pos = (pos + 1) & self.index_mask;
            psl += 1;
        }

        loop {
            let next = (pos + 1) & self.index_mask;
            if self.index_slots[next].psl <= 1 {
                break;
            }
            self.index_slots[pos] = self.index_slots[next];
            self.index_slots[pos].psl -= 1;
            pos = next;
        }
        self.index_slots[pos].status = SLOT_FREE;
        self.index_slots[pos].psl = 0;
        self.index_count -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const K0: u64 = 0x0123456789ABCDEF;
    const K1: u64 = 0xFEDCBA9876543210;

    #[test]
    fn empty_heap() {
        let ifmh = InFlightMinHeap::new(64, K0, K1, 4, 2);
        assert!(ifmh.is_empty());
        assert_eq!(ifmh.len(), 0);
        assert_eq!(ifmh.min(), None);
        assert_eq!(ifmh.last_removed_gsn(), 0);
    }

    #[test]
    fn add_single() {
        let mut ifmh = InFlightMinHeap::new(64, K0, K1, 4, 2);
        ifmh.add(100);
        assert_eq!(ifmh.len(), 1);
        assert_eq!(ifmh.min(), Some(100));
    }

    #[test]
    fn add_ascending_min_is_first() {
        let mut ifmh = InFlightMinHeap::new(64, K0, K1, 4, 2);
        ifmh.add(100);
        ifmh.add(200);
        ifmh.add(300);
        assert_eq!(ifmh.min(), Some(100));
        assert_eq!(ifmh.len(), 3);
    }

    #[test]
    fn add_descending_min_is_smallest() {
        let mut ifmh = InFlightMinHeap::new(64, K0, K1, 4, 2);
        ifmh.add(300);
        ifmh.add(200);
        ifmh.add(100);
        assert_eq!(ifmh.min(), Some(100));
    }

    #[test]
    fn remove_min_advances() {
        let mut ifmh = InFlightMinHeap::new(64, K0, K1, 4, 2);
        ifmh.add(100);
        ifmh.add(200);
        ifmh.add(300);

        ifmh.remove(100);
        assert_eq!(ifmh.min(), Some(200));
        assert_eq!(ifmh.last_removed_gsn(), 100);
    }

    #[test]
    fn remove_middle() {
        let mut ifmh = InFlightMinHeap::new(64, K0, K1, 4, 2);
        ifmh.add(100);
        ifmh.add(200);
        ifmh.add(300);

        ifmh.remove(200);
        assert_eq!(ifmh.min(), Some(100));
        assert_eq!(ifmh.len(), 2);
    }

    #[test]
    fn remove_all_heap_empty() {
        let mut ifmh = InFlightMinHeap::new(64, K0, K1, 4, 2);
        ifmh.add(100);
        ifmh.add(200);

        ifmh.remove(100);
        ifmh.remove(200);

        assert!(ifmh.is_empty());
        assert_eq!(ifmh.min(), None);
        assert_eq!(ifmh.last_removed_gsn(), 200);
    }

    #[test]
    fn remove_out_of_order() {
        let mut ifmh = InFlightMinHeap::new(64, K0, K1, 4, 2);
        for gsn in 1..=10u64 {
            ifmh.add(gsn * 100);
        }
        assert_eq!(ifmh.len(), 10);
        assert_eq!(ifmh.min(), Some(100));

        ifmh.remove(500);
        ifmh.remove(300);
        ifmh.remove(100);

        assert_eq!(ifmh.len(), 7);
        assert_eq!(ifmh.min(), Some(200));
        assert_eq!(ifmh.last_removed_gsn(), 500);
    }

    #[test]
    fn committed_gsn_calculation() {
        let mut ifmh = InFlightMinHeap::new(64, K0, K1, 4, 2);
        ifmh.add(100);
        ifmh.add(200);
        ifmh.add(300);

        assert_eq!(ifmh.min().unwrap() - 1, 99);

        ifmh.remove(100);
        assert_eq!(ifmh.min().unwrap() - 1, 199);

        ifmh.remove(200);
        assert_eq!(ifmh.min().unwrap() - 1, 299);

        ifmh.remove(300);
        assert!(ifmh.is_empty());
        assert_eq!(ifmh.last_removed_gsn(), 300);
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let mut ifmh = InFlightMinHeap::new(64, K0, K1, 4, 2);
        ifmh.add(100);
        ifmh.remove(999);
        assert_eq!(ifmh.len(), 1);
        assert_eq!(ifmh.min(), Some(100));
    }

    #[test]
    fn large_heap() {
        let mut ifmh = InFlightMinHeap::new(1024, K0, K1, 4, 2);

        for gsn in 1..=500u64 {
            ifmh.add(gsn);
        }
        assert_eq!(ifmh.len(), 500);
        assert_eq!(ifmh.min(), Some(1));

        for gsn in (2..=500u64).step_by(2) {
            ifmh.remove(gsn);
        }
        assert_eq!(ifmh.len(), 250);
        assert_eq!(ifmh.min(), Some(1));

        ifmh.remove(1);
        assert_eq!(ifmh.min(), Some(3));
    }

    #[test]
    fn heap_property_maintained() {
        let mut ifmh = InFlightMinHeap::new(256, K0, K1, 4, 2);

        for gsn in (1..=100u64).rev() {
            ifmh.add(gsn);
        }

        for expected_min in 1..=100u64 {
            assert_eq!(ifmh.min(), Some(expected_min));
            ifmh.remove(expected_min);
        }
        assert!(ifmh.is_empty());
    }

    #[test]
    fn resize_doubles_capacity() {
        let mut heap = InFlightMinHeap::new(8, K0, K1, 4, 2);

        for i in 1..=6u64 {
            assert!(heap.add(i));
        }

        for i in 7..=12u64 {
            assert!(heap.add(i));
        }

        assert_eq!(heap.len(), 12);
        assert_eq!(heap.min(), Some(1));
    }

    #[test]
    fn resize_preserves_min_heap_order() {
        let mut heap = InFlightMinHeap::new(8, K0, K1, 4, 2);

        for i in (1..=10u64).rev() {
            assert!(heap.add(i));
        }

        assert_eq!(heap.min(), Some(1));

        for expected in 1..=10u64 {
            assert_eq!(heap.min(), Some(expected));
            heap.remove(expected);
        }

        assert!(heap.is_empty());
    }

    #[test]
    fn resize_preserves_index_lookup() {
        let mut heap = InFlightMinHeap::new(8, K0, K1, 4, 2);

        for i in 1..=20u64 {
            assert!(heap.add(i));
        }

        heap.remove(5);
        heap.remove(15);
        heap.remove(10);

        assert_eq!(heap.len(), 17);
        assert_eq!(heap.min(), Some(1));
    }

    #[test]
    fn backpressure_when_max_resize_reached() {
        let mut heap = InFlightMinHeap::new(4, K0, K1, 1, 2);

        for i in 1..=3u64 {
            assert!(heap.add(i), "Failed to add {}", i);
        }

        assert!(heap.add(4));

        assert!(heap.add(5));
        assert!(heap.add(6));

        assert!(!heap.add(7));

        assert_eq!(heap.len(), 6);
    }

    #[test]
    fn unlimited_resize() {
        let mut heap = InFlightMinHeap::new(4, K0, K1, 0, 2);

        for i in 1..=1000u64 {
            assert!(heap.add(i), "Backpressure at {}", i);
        }

        assert_eq!(heap.len(), 1000);
        assert_eq!(heap.min(), Some(1));
    }

    #[test]
    fn growth_factor_4() {
        let mut heap = InFlightMinHeap::new(4, K0, K1, 2, 4);

        for i in 1..=12u64 {
            assert!(heap.add(i));
        }

        assert_eq!(heap.len(), 12);
        assert_eq!(heap.min(), Some(1));
    }

    #[test]
    fn add_remove_after_resize() {
        let mut heap = InFlightMinHeap::new(8, K0, K1, 4, 2);

        for i in 1..=10u64 {
            heap.add(i);
        }

        for i in 1..=10u64 {
            heap.remove(i);
        }

        assert!(heap.is_empty());
        assert_eq!(heap.last_removed_gsn(), 10);

        for i in 100..=110u64 {
            assert!(heap.add(i));
        }

        assert_eq!(heap.min(), Some(100));
    }
}