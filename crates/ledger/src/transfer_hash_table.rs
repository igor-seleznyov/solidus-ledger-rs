use std::cell::UnsafeCell;
use common::mem_barrier;
use ringbuf::arena::Arena;
use common::siphash::siphash13;
use mem_barrier::release_store_u8;
use pipeline::transfer_slot::TransferSlot;
use pipeline::transfer_hash_table_entry::TransferHashTableEntry;

pub struct TransferHashTable {
    #[allow(dead_code)]
    arena: Arena,
    slots: *mut TransferSlot,
    #[allow(dead_code)]
    overflow_arena: Option<Arena>,
    overflow_base: *mut TransferHashTableEntry,
    overflow_per_slot: usize,
    capacity: usize,
    mask: usize,
    count: UnsafeCell<usize>,
    seed_k0: u64,
    seed_k1: u64,
}

unsafe impl Send for TransferHashTable {}
unsafe impl Sync for TransferHashTable {}

impl TransferHashTable {
    pub fn new(
        capacity: usize,
        seed_k0: u64,
        seed_k1: u64,
        max_entries_per_transfer: usize,
    ) -> std::io::Result<Self> {
        assert!(capacity.is_power_of_two(), "THT capacity must be a power of two");
        assert!(capacity >= 16, "THT capacity must be at least 16");
        assert!(max_entries_per_transfer >= 2, "at least 2 entries (DEBIT + CREDIT)");

        let total_size = capacity * TransferSlot::SIZE;
        let arena = Arena::new(total_size)?;
        let slots = arena.as_ptr() as *mut TransferSlot;

        let overflow_per_slot = max_entries_per_transfer.saturating_sub(8);
        let (overflow_arena, overflow_base) = if overflow_per_slot > 0 {
            let overflow_size = capacity * overflow_per_slot * TransferHashTableEntry::SIZE;
            let overflow_arena = Arena::new(overflow_size)?;
            let overflow_base = overflow_arena.as_ptr() as *mut TransferHashTableEntry;
            (Some(overflow_arena), overflow_base)
        } else {
            (None, std::ptr::null_mut())
        };

        Ok(
            Self {
                arena,
                slots,
                overflow_arena,
                overflow_base,
                overflow_per_slot,
                capacity,
                mask: capacity - 1,
                count: UnsafeCell::new(0),
                seed_k0,
                seed_k1,
            }
        )
    }

    pub fn count(&self) -> usize {
        unsafe { *self.count.get() }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn load_factory(&self) -> f64 {
        self.count() as f64 / self.capacity as f64
    }

    pub fn overflow_per_slot(&self) -> usize {
        self.overflow_per_slot
    }

    pub unsafe fn insert(
        &self,
        id_hi: u64,
        id_lo: u64,
        gsn: u64,
        connection_id: u64,
        batch_id: &[u8; 16],
        currency: &[u8; 16],
        entries_count: u8,
        transfer_datetime: u64,
        transfer_sequence_id: &[u8; 16],
    ) -> u32 {
        let hash = siphash13(self.seed_k0, self.seed_k1, id_hi, id_lo);
        let fp = (hash >> 56) as u8;
        let mut pos = (hash as usize) & self.mask;
        let mut psl: u8 = 1;

        let mut inserting = TransferSlot::zeroed();
        inserting.transfer_id_hi = id_hi;
        inserting.transfer_id_lo = id_lo;
        inserting.psl = 1;
        inserting.fingerprint = fp;
        inserting.entries_count = entries_count;
        inserting.gsn = gsn;
        inserting.connection_id = connection_id;
        inserting.batch_id = *batch_id;
        inserting.currency = *currency;
        inserting.transfer_datetime = transfer_datetime;
        inserting.transfer_sequence_id = *transfer_sequence_id;
        inserting.ready = 0;
        inserting.decision = 0;
        inserting.commit_success_count = 0;
        inserting.rollback_success_count = 0;
        inserting.confirmed_count = 0;
        inserting.failed_count = 0;

        let mut inserting_overflow = [TransferHashTableEntry::zeroed(); 8];

        let mut result_pos: u32 = u32::MAX;

        loop {
            let slot = unsafe { self.slots.add(pos) };
            let slot_psl = unsafe { (*slot).psl };

            if slot_psl == 0 {
                inserting.psl = psl;
                unsafe {
                    std::ptr::copy_nonoverlapping(&inserting, slot, 1);
                }

                if self.overflow_per_slot > 0 && inserting.entries_count > 8 {
                    unsafe {
                        let overflow_ptr = self.overflow_base.add(pos * self.overflow_per_slot);
                        std::ptr::copy_nonoverlapping(
                            inserting_overflow.as_ptr(),
                            overflow_ptr,
                            self.overflow_per_slot,
                        );
                    }
                }

                unsafe {
                    *self.count.get() += 1;
                }
                return if result_pos == u32::MAX {
                    pos as u32
                } else {
                    result_pos
                }
            }

            let slot_fingerprint = unsafe { (*slot).fingerprint };
            let slot_transfer_id_hi = unsafe { (*slot).transfer_id_hi };
            let slot_transfer_id_lo = unsafe { (*slot).transfer_id_lo };

            if slot_fingerprint == fp
                && slot_transfer_id_hi == id_hi
                && slot_transfer_id_lo == id_lo {
                return pos as u32
            }

            if slot_psl < psl {
                unsafe {
                    std::mem::swap(&mut *slot, &mut inserting);
                }

                let slot_entries_count = unsafe { (*slot).entries_count };

                if self.overflow_per_slot > 0
                    && (slot_entries_count > 8 || inserting.entries_count > 8) {
                    unsafe {
                    self.swap_overflow(pos, &mut inserting_overflow);
                        }
                }

                psl = inserting.psl;
                if result_pos == u32::MAX {
                    result_pos = pos as u32;
                }
            }

            pos = (pos + 1) & self.mask;
            psl += 1;
            inserting.psl = psl;
        }
    }

    unsafe fn swap_overflow(
        &self,
        pos: usize,
        inserting_overflow: &mut [TransferHashTableEntry; 8]
    ) {
        if self.overflow_per_slot == 0 {
            return;
        }

        let overflow_ptr = unsafe { self.overflow_base.add(pos * self.overflow_per_slot) };

        let mut temp = [TransferHashTableEntry::zeroed(); 8];
        unsafe {
            std::ptr::copy_nonoverlapping(
                overflow_ptr,
                temp.as_mut_ptr(),
                self.overflow_per_slot,
            );
        }

        unsafe {
            std::ptr::copy_nonoverlapping(
                inserting_overflow.as_ptr(),
                overflow_ptr,
                self.overflow_per_slot,
            );
        }

        inserting_overflow[..self.overflow_per_slot]
            .copy_from_slice(&temp[..self.overflow_per_slot]);
    }

    pub unsafe fn fill_entry(
        &self,
        offset: u32,
        index: usize,
        entry: &TransferHashTableEntry,
    ) {
        let slot = unsafe { self.slots.add(offset as usize) };
        unsafe {
            std::ptr::copy_nonoverlapping(entry, &mut (*slot).entries[index], 1);
        }
    }

    pub unsafe fn fill_overflow(
        &self,
        offset: u32,
        index: usize,
        entry: &TransferHashTableEntry,
    ) {
        assert!(self.overflow_per_slot > 0, "no overflow arena");
        let overflow_ptr = unsafe {
            self.overflow_base.add(
                offset as usize * self.overflow_per_slot + index,
            )
        };
        unsafe {
            std::ptr::copy_nonoverlapping(entry, overflow_ptr, 1);
        }
    }

    pub unsafe fn publish(&self, offset: u32) {
        let slot = unsafe { self.slots.add(offset as usize) };
        let ready_ptr = unsafe { &mut (*slot).ready as *mut u8 };
        unsafe {
            release_store_u8(ready_ptr, 1);
        }
    }

    #[inline(always)]
    pub unsafe fn slot_ptr(&self, offset: u32) -> *mut TransferSlot {
        unsafe {
            self.slots.add(offset as usize)
        }
    }

    #[inline(always)]
    pub unsafe fn overflow_ptr(&self, offset: u32, index: usize) -> *const TransferHashTableEntry {
        unsafe {
            self.overflow_base.add(offset as usize * self.overflow_per_slot + index)
        }
    }

    pub unsafe fn get_entry(
        &self,
        offset: u32,
        index: usize,
    ) -> &TransferHashTableEntry {
        if index < 8 {
            let slot = unsafe { self.slots.add(offset as usize) };
            unsafe {
                &(*slot).entries[index]
            }
        } else {
            unsafe {
                &*self.overflow_base.add(
                    offset as usize * self.overflow_per_slot + (index - 8),
                )
            }
        }
    }

    pub unsafe fn remove(&self, offset: u32) {
        let mut pos = offset as usize;

        loop {
            let next = (pos + 1) & self.mask;
            let next_slot = unsafe { self.slots.add(next) };
            let next_slot_psl = unsafe { (*next_slot).psl };

            if next_slot_psl <= 1 {
                break;
            }

            unsafe {
                std::ptr::copy_nonoverlapping(next_slot, self.slots.add(pos), 1);
                (*self.slots.add(pos)).psl -= 1;
            }

            let slot_entries_count = unsafe { (*self.slots.add(pos)).entries_count };

            if self.overflow_per_slot > 0 && slot_entries_count > 8 {
                let src = unsafe { self.overflow_base.add(next * self.overflow_per_slot) };
                let dest = unsafe { self.overflow_base.add(pos * self.overflow_per_slot) };
                unsafe {
                    std::ptr::copy_nonoverlapping(src, dest, self.overflow_per_slot);
                }
            }

            pos = next;
        }

        unsafe {
            std::ptr::write_bytes(self.slots.add(pos), 0, 1);
        }
        if self.overflow_per_slot > 0 {
            let overflow_ptr = unsafe { self.overflow_base.add(pos * self.overflow_per_slot) };
            unsafe {
                std::ptr::write_bytes(overflow_ptr, 0, self.overflow_per_slot);
            }
        }

        unsafe {
            *self.count.get() -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipeline::transfer_slot::*;

    const K0: u64 = 0x0123456789ABCDEF;
    const K1: u64 = 0xFEDCBA9876543210;

    fn batch_id() -> [u8; 16] { [0u8; 16] }
    fn currency() -> [u8; 16] { 
        let mut buf = [0u8; 16];
        buf[..3].copy_from_slice(b"EUR");
        buf
    }
    fn seq_id() -> [u8; 16] { [0u8; 16] }

    fn make_entry(acc_lo: u64, amount: i64, partition: u32, etype: u8) -> TransferHashTableEntry {
        TransferHashTableEntry {
            account_id_hi: 0,
            account_id_lo: acc_lo,
            amount,
            partition_id: partition,
            entry_type: etype,
            _pad: [0; 3],
        }
    }

    // --- Basic ---

    #[test]
    fn create_empty() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();
        assert_eq!(tht.count(), 0);
        assert_eq!(tht.capacity(), 64);
        assert_eq!(tht.overflow_per_slot(), 0);
    }

    #[test]
    fn create_with_overflow() {
        let tht = TransferHashTable::new(64, K0, K1, 12).unwrap();
        assert_eq!(tht.overflow_per_slot(), 4);
    }

    // --- Insert + slot_ptr ---

    #[test]
    fn insert_and_read() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();

        unsafe {
            let offset = tht.insert(0, 1, 100, 7, &batch_id(), &currency(), 2, 0, &seq_id());
            assert_eq!(tht.count(), 1);

            let slot = tht.slot_ptr(offset);
            assert_eq!((*slot).transfer_id_lo, 1);
            assert_eq!((*slot).gsn, 100);
            assert_eq!((*slot).connection_id, 7);
            assert_eq!((*slot).entries_count, 2);
            assert_eq!((*slot).ready, 0);
            assert_eq!((*slot).decision, DECISION_NONE);
        }
    }

    // --- fill_entry + publish ---

    #[test]
    fn fill_entry_and_publish() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();

        unsafe {
            let offset = tht.insert(0, 1, 100, 7, &batch_id(), &currency(), 2, 0, &seq_id());

            tht.fill_entry(offset, 0, &make_entry(10, -500, 0, 1));
            tht.fill_entry(offset, 1, &make_entry(20, 500, 1, 2));

            tht.publish(offset);

            let slot = tht.slot_ptr(offset);
            assert_eq!((*slot).ready, 1);
            assert_eq!((*slot).entries[0].amount, -500);
            assert_eq!((*slot).entries[1].amount, 500);
        }
    }

    // --- Overflow ---

    #[test]
    fn fill_overflow_entries() {
        let tht = TransferHashTable::new(64, K0, K1, 12).unwrap();

        unsafe {
            let offset = tht.insert(0, 1, 100, 7, &batch_id(), &currency(), 10, 0, &seq_id());

            for i in 0..8 {
                tht.fill_entry(offset, i, &make_entry(i as u64 + 10, 100, 0, 1));
            }
            tht.fill_overflow(offset, 0, &make_entry(18, 200, 1, 2));
            tht.fill_overflow(offset, 1, &make_entry(19, 300, 1, 2));

            tht.publish(offset);

            let slot = tht.slot_ptr(offset);
            assert_eq!((*slot).entries[7].account_id_lo, 17);

            let ovf0 = tht.overflow_ptr(offset, 0);
            assert_eq!((*ovf0).account_id_lo, 18);
            assert_eq!((*ovf0).amount, 200);

            let ovf1 = tht.overflow_ptr(offset, 1);
            assert_eq!((*ovf1).account_id_lo, 19);
        }
    }

    // --- Idempotent ---

    #[test]
    fn idempotent_insert() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();

        unsafe {
            let off1 = tht.insert(0, 1, 100, 7, &batch_id(), &currency(), 2, 0, &seq_id());
            let off2 = tht.insert(0, 1, 200, 8, &batch_id(), &currency(), 2, 0, &seq_id());

            assert_eq!(off1, off2);
            assert_eq!(tht.count(), 1);

            let slot = tht.slot_ptr(off1);
            assert_eq!((*slot).gsn, 100);
        }
    }

    // --- Remove ---

    #[test]
    fn remove_decreases_count() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();

        unsafe {
            let offset = tht.insert(0, 1, 100, 7, &batch_id(), &currency(), 2, 0, &seq_id());
            tht.remove(offset);
            assert_eq!(tht.count(), 0);

            let slot = tht.slot_ptr(offset);
            assert_eq!((*slot).psl, 0);
        }
    }

    // --- Robin Hood ---

    #[test]
    fn robin_hood_displacement() {
        let tht = TransferHashTable::new(16, K0, K1, 8).unwrap();

        unsafe {
            for i in 1..=10u64 {
                let off = tht.insert(0, i, i * 100, 0, &batch_id(), &currency(), 2, 0, &seq_id());
                let slot = tht.slot_ptr(off);
                assert_eq!((*slot).transfer_id_lo, i);
                assert_eq!((*slot).gsn, i * 100);
            }
        }
        assert_eq!(tht.count(), 10);
    }

    #[test]
    fn robin_hood_with_overflow() {
        let tht = TransferHashTable::new(16, K0, K1, 12).unwrap();

        unsafe {
            for i in 1..=8u64 {
                let off = tht.insert(0, i, i * 100, 0, &batch_id(), &currency(), 10, 0, &seq_id());
                tht.fill_overflow(off, 0, &make_entry(i * 10, i as i64 * 1000, 0, 1));
                tht.publish(off);
            }

            for i in 1..=8u64 {
                let off = tht.insert(0, i, 0, 0, &batch_id(), &currency(), 0, 0, &seq_id()); // idempotent
                let ovf = tht.overflow_ptr(off, 0);
                assert_eq!(
                    (*ovf).account_id_lo, i * 10,
                    "overflow corrupted for transfer {}",
                    i,
                );
            }
        }
    }

    #[test]
    fn psl_bounded() {
        let tht = TransferHashTable::new(256, K0, K1, 8).unwrap();

        unsafe {
            for i in 1..=192u64 {
                tht.insert(0, i, i, 0, &batch_id(), &currency(), 2, 0, &seq_id());
            }
        }

        let mut max_psl: u8 = 0;
        unsafe {
            for i in 0..256 {
                let slot = tht.slots.add(i);
                if (*slot).psl > max_psl {
                    max_psl = (*slot).psl;
                }
            }
        }

        assert!(max_psl < 20, "PSL too high: {}", max_psl);
    }

    #[test]
    fn remove_with_backward_shift() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();

        unsafe {
            let mut offsets = Vec::new();
            for i in 1..=10u64 {
                offsets.push(tht.insert(0, i, i * 100, 0, &batch_id(), &currency(), 2, 0, &seq_id()));
            }

            tht.remove(offsets[4]);
            assert_eq!(tht.count(), 9);

            for i in 1..=10u64 {
                if i == 5 { continue; }
                let off = tht.insert(0, i, 0, 0, &batch_id(), &currency(), 0, 0, &seq_id());
                let slot = tht.slot_ptr(off);
                assert_eq!((*slot).transfer_id_lo, i);
            }
            assert_eq!(tht.count(), 9);
        }
    }

    #[test]
    fn multiple_inserts_and_removes() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();

        unsafe {
            let mut offsets = Vec::new();
            for i in 1..=10u64 {
                offsets.push(tht.insert(0, i, i * 100, 0, &batch_id(), &currency(), 2, 0, &seq_id()));
            }
            assert_eq!(tht.count(), 10);

            for &off in &offsets[..5] {
                tht.remove(off);
            }
            assert_eq!(tht.count(), 5);
        }
    }

    #[test]
    fn mixed_entry_counts_with_robin_hood() {
        let tht = TransferHashTable::new(16, K0, K1, 12).unwrap();

        unsafe {
            for i in 1..=5u64 {
                let off = tht.insert(0, i, i * 100, 0, &batch_id(), &currency(), 10, 0, &seq_id());
                for j in 0..8 {
                    tht.fill_entry(off, j, &make_entry(i * 100 + j as u64, 100, 0, 1));
                }
                tht.fill_overflow(off, 0, &make_entry(i * 1000, i as i64 * 500, 0, 1));
                tht.fill_overflow(off, 1, &make_entry(i * 1000 + 1, i as i64 * 600, 1, 2));
                tht.publish(off);
            }

            for i in 6..=10u64 {
                let off = tht.insert(0, i, i * 100, 0, &batch_id(), &currency(), 2, 0, &seq_id());
                tht.fill_entry(off, 0, &make_entry(i * 100, -200, 0, 1));
                tht.fill_entry(off, 1, &make_entry(i * 100 + 1, 200, 1, 2));
                tht.publish(off);
            }

            assert_eq!(tht.count(), 10);

            for i in 1..=5u64 {
                let off = tht.insert(0, i, 0, 0, &batch_id(), &currency(), 0, 0, &seq_id());
                let slot = tht.slot_ptr(off);
                assert_eq!((*slot).entries_count, 10, "transfer {} entries_count", i);

                let ovf0 = tht.overflow_ptr(off, 0);
                assert_eq!(
                    (*ovf0).account_id_lo, i * 1000,
                    "overflow[0] corrupted for transfer {}", i,
                );
                assert_eq!((*ovf0).amount, i as i64 * 500);

                let ovf1 = tht.overflow_ptr(off, 1);
                assert_eq!(
                    (*ovf1).account_id_lo, i * 1000 + 1,
                    "overflow[1] corrupted for transfer {}", i,
                );
            }

            for i in 6..=10u64 {
                let off = tht.insert(0, i, 0, 0, &batch_id(), &currency(), 0, 0, &seq_id());
                let slot = tht.slot_ptr(off);
                assert_eq!((*slot).entries_count, 2, "transfer {} entries_count", i);
                assert_eq!((*slot).entries[0].amount, -200);
                assert_eq!((*slot).entries[1].amount, 200);
            }
        }
    }

    #[test]
    fn remove_overflow_with_backward_shift() {
        let tht = TransferHashTable::new(16, K0, K1, 12).unwrap();

        unsafe {
            let mut offsets = Vec::new();
            for i in 1..=8u64 {
                let off = tht.insert(0, i, i * 100, 0, &batch_id(), &currency(), 10, 0, &seq_id());
                tht.fill_overflow(off, 0, &make_entry(i * 10, i as i64 * 1000, 0, 1));
                tht.publish(off);
                offsets.push(off);
            }

            tht.remove(offsets[3]);
            assert_eq!(tht.count(), 7);

            for i in 1..=8u64 {
                if i == 4 { continue; }
                let off = tht.insert(0, i, 0, 0, &batch_id(), &currency(), 0, 0, &seq_id());
                let ovf = tht.overflow_ptr(off, 0);
                assert_eq!(
                    (*ovf).account_id_lo, i * 10,
                    "overflow corrupted after remove for transfer {}", i,
                );
            }
        }
    }

    #[test]
    fn exactly_8_entries_no_overflow() {
        let tht = TransferHashTable::new(64, K0, K1, 12).unwrap();

        unsafe {
            let off = tht.insert(0, 1, 100, 0, &batch_id(), &currency(), 8, 0, &seq_id());
            for i in 0..8 {
                tht.fill_entry(off, i, &make_entry(i as u64 + 10, 100, 0, 1));
            }
            tht.publish(off);

            let slot = tht.slot_ptr(off);
            assert_eq!((*slot).entries[7].account_id_lo, 17);
        }
    }

    #[test]
    fn exactly_9_entries_uses_overflow() {
        let tht = TransferHashTable::new(64, K0, K1, 12).unwrap();

        unsafe {
            let off = tht.insert(0, 1, 100, 0, &batch_id(), &currency(), 9, 0, &seq_id());
            for i in 0..8 {
                tht.fill_entry(off, i, &make_entry(i as u64 + 10, 100, 0, 1));
            }
            tht.fill_overflow(off, 0, &make_entry(18, 900, 1, 2));
            tht.publish(off);

            let slot = tht.slot_ptr(off);
            assert_eq!((*slot).entries[7].account_id_lo, 17);

            let ovf = tht.overflow_ptr(off, 0);
            assert_eq!((*ovf).account_id_lo, 18);
            assert_eq!((*ovf).amount, 900);
        }
    }

    #[test]
    fn remove_clears_overflow() {
        let tht = TransferHashTable::new(64, K0, K1, 12).unwrap();

        unsafe {
            let off = tht.insert(0, 1, 100, 0, &batch_id(), &currency(), 10, 0, &seq_id());
            tht.fill_overflow(off, 0, &make_entry(99, 999, 0, 1));
            tht.publish(off);

            tht.remove(off);

            let ovf = tht.overflow_ptr(off, 0);
            assert_eq!((*ovf).account_id_lo, 0);
            assert_eq!((*ovf).amount, 0);
        }
    }

    #[test]
    fn insert_after_remove_reuses_slot() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();

        unsafe {
            let off1 = tht.insert(0, 1, 100, 0, &batch_id(), &currency(), 2, 0, &seq_id());
            tht.publish(off1);
            tht.remove(off1);
            assert_eq!(tht.count(), 0);

            let off2 = tht.insert(0, 1, 200, 0, &batch_id(), &currency(), 2, 0, &seq_id());
            assert_eq!(tht.count(), 1);

            let slot = tht.slot_ptr(off2);
            assert_eq!((*slot).gsn, 200);
            assert_eq!((*slot).ready, 0);
        }
    }

    #[test]
    fn unpublished_slot_has_ready_zero() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();

        unsafe {
            let off = tht.insert(0, 1, 100, 0, &batch_id(), &currency(), 2, 0, &seq_id());
            tht.fill_entry(off, 0, &make_entry(10, -500, 0, 1));

            let slot = tht.slot_ptr(off);
            assert_eq!((*slot).ready, 0, "unpublished slot must have ready=0");
            assert_eq!((*slot).entries[0].amount, -500, "entries filled but not published");
        }
    }

    #[test]
    fn get_entry_inline() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();

        unsafe {
            let off = tht.insert(0, 1, 100, 0, &batch_id(), &currency(), 2, 0, &seq_id());
            tht.fill_entry(off, 0, &make_entry(10, -500, 0, 1));
            tht.fill_entry(off, 1, &make_entry(20, 500, 1, 2));

            let e0 = tht.get_entry(off, 0);
            assert_eq!(e0.account_id_lo, 10);
            assert_eq!(e0.amount, -500);

            let e1 = tht.get_entry(off, 1);
            assert_eq!(e1.account_id_lo, 20);
            assert_eq!(e1.amount, 500);
        }
    }

    #[test]
    fn get_entry_overflow() {
        let tht = TransferHashTable::new(64, K0, K1, 12).unwrap();

        unsafe {
            let off = tht.insert(0, 1, 100, 0, &batch_id(), &currency(), 10, 0, &seq_id());
            for i in 0..8 {
                tht.fill_entry(off, i, &make_entry(i as u64 + 10, 100, 0, 1));
            }
            tht.fill_overflow(off, 0, &make_entry(18, 200, 1, 2));
            tht.fill_overflow(off, 1, &make_entry(19, 300, 1, 2));

            // inline
            let e7 = tht.get_entry(off, 7);
            assert_eq!(e7.account_id_lo, 17);

            // overflow (index 8 → overflow[0], index 9 → overflow[1])
            let e8 = tht.get_entry(off, 8);
            assert_eq!(e8.account_id_lo, 18);
            assert_eq!(e8.amount, 200);

            let e9 = tht.get_entry(off, 9);
            assert_eq!(e9.account_id_lo, 19);
            assert_eq!(e9.amount, 300);
        }
    }
}