use std::cell::UnsafeCell;
use common::mem_barrier;
use ringbuf::arena::Arena;
use common::siphash::siphash13;
use mem_barrier::release_store_u8;
use pipeline::transfer_slot::TransferSlot;
use pipeline::transfer_hash_table_entry::TransferHashTableEntry;
use ringbuf::hash_table_slot_status::{SLOT_DELETED, SLOT_FREE, SLOT_OCCUPIED};

const NEIGHBOURHOOD_SIZE: usize = 32;

pub struct TransferHashTable {
    #[allow(dead_code)]
    arena: Arena,
    slots: *mut TransferSlot,
    hop_bitmaps: Vec<u32>,
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
        assert!(capacity >= NEIGHBOURHOOD_SIZE, "capacity must be >= NEIGHBOURHOOD_SIZE");

        let total_size = capacity * TransferSlot::SIZE;
        let arena = Arena::new(total_size)?;
        let slots = arena.as_ptr() as *mut TransferSlot;
        let hop_bitmaps = vec![0u32; capacity];

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
                hop_bitmaps,
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

    pub fn load_factor(&self) -> f64 {
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
        let home = (hash as usize) & self.mask;

        let bitmap = self.hop_bitmaps[home];
        let mut check_bits = bitmap;

        while check_bits != 0 {
            let bit_pos = check_bits.trailing_zeros() as usize;
            let pos = (home + bit_pos) & self.mask;
            let slot = self.slots.add(pos);

            let slot_fingerprint = unsafe { (*slot).fingerprint };
            let slot_transfer_id_hi = unsafe { (*slot).transfer_id_hi };
            let slot_transfer_id_lo = unsafe { (*slot).transfer_id_lo };
            let slot_status = unsafe { (*slot).status };

            if slot_fingerprint == fp
                && slot_transfer_id_hi == id_hi
                && slot_transfer_id_lo == id_lo
                && slot_status == SLOT_OCCUPIED {
                return pos as u32;
            }
            check_bits &= check_bits - 1;
        }

        for offset in 0..NEIGHBOURHOOD_SIZE {
            let pos = (home + offset) & self.mask;
            let slot = self.slots.add(pos);
            let slot_status = unsafe { (*slot).status };

            if slot_status == SLOT_FREE || slot_status == SLOT_DELETED {
                unsafe {
                    self.write_new_slot(
                        slot, id_hi, id_lo, fp, gsn, connection_id, batch_id,
                        currency, entries_count, transfer_datetime,
                        transfer_sequence_id,
                    );
                }

                let bitmaps_ptr = self.hop_bitmaps.as_ptr() as *mut u32;
                unsafe {
                    *bitmaps_ptr.add(home) |= 1u32 << offset;
                }

                unsafe {
                    *self.count.get() += 1;
                }
                return pos as u32
            }
        }

        unsafe {
            self.hop_and_insert(
                home, id_hi, id_lo, fp, gsn, connection_id, batch_id,
                currency, entries_count, transfer_datetime,
                transfer_sequence_id,
            )
        }
    }

    pub unsafe fn lookup(&self, id_hi: u64, id_lo: u64) -> Option<u32> {
        let hash = siphash13(self.seed_k0, self.seed_k1, id_hi, id_lo);
        let fp = (hash >> 56) as u8;
        let home = (hash as usize) & self.mask;

        let bitmap = self.hop_bitmaps[home];
        let mut check_bits = bitmap;

        while check_bits != 0 {
            let bit_pos = check_bits.trailing_zeros() as usize;
            let pos = (home + bit_pos) & self.mask;
            let slot = unsafe { self.slots.add(pos) };
            let slot_status = unsafe { (*slot).status };
            let slot_fingerprint = unsafe { (*slot).fingerprint };
            let slot_transfer_id_hi = unsafe { (*slot).transfer_id_hi };
            let slot_transfer_id_lo = unsafe { (*slot).transfer_id_lo };

            if slot_status == SLOT_OCCUPIED
                && slot_fingerprint == fp
                && slot_transfer_id_hi == id_hi
                && slot_transfer_id_lo == id_lo {
                return Some(pos as u32);
            }

            check_bits &= check_bits - 1;
        }

        None
    }

    unsafe fn hop_and_insert(
        &self,
        home: usize,
        id_hi: u64,
        id_lo: u64,
        fp: u8,
        gsn: u64,
        connection_id: u64,
        batch_id: &[u8; 16],
        currency: &[u8; 16],
        entries_count: u8,
        transfer_datetime: u64,
        transfer_sequence_id: &[u8; 16],
    ) -> u32 {
        let mut empty_pos = None;
        for dist in NEIGHBOURHOOD_SIZE..self.capacity {
            let pos = (home + dist) & self.mask;
            let slot = self.slots.add(pos);
            let slot_status = unsafe { (*slot).status };
            if slot_status == SLOT_FREE || slot_status == SLOT_DELETED {
                empty_pos = Some(pos);
                break;
            }
        }

        let mut free_pos = empty_pos.expect("Transfer Hash Table full, cannot hop");

        loop {
            let dist_to_home = if free_pos >= home {
                free_pos - home
            } else {
                free_pos + self.capacity - home
            };

            if dist_to_home < NEIGHBOURHOOD_SIZE {
                let slot = unsafe { self.slots.add(free_pos) };
                unsafe {
                    self.write_new_slot(
                        slot, id_hi, id_lo, fp, gsn, connection_id, batch_id,
                        currency, entries_count, transfer_datetime, transfer_sequence_id,
                    );
                }

                let offset = if free_pos >= home {
                    free_pos - home
                } else {
                    free_pos + self.capacity - home
                };
                let bitmaps_ptr = self.hop_bitmaps.as_ptr() as *mut u32;
                unsafe {
                    *bitmaps_ptr.add(home) |= 1u32 << offset;
                }

                unsafe {
                    *self.count.get() += 1;
                }
                return free_pos as u32;
            }

            let mut hopped = false;
            for candidate_dist in 1..NEIGHBOURHOOD_SIZE {
                let candidate_pos = if free_pos >= candidate_dist {
                    free_pos - candidate_dist
                } else {
                    free_pos + self.capacity - candidate_dist
                };

                let candidate_slot = unsafe { self.slots.add(candidate_pos) };

                let slot_ready = unsafe { (*candidate_slot).ready };
                let slot_status = unsafe { (*candidate_slot).status };
                let slot_transfer_id_hi = unsafe { (*candidate_slot).transfer_id_hi };
                let slot_transfer_id_lo = unsafe { (*candidate_slot).transfer_id_lo };

                if slot_ready == 1 {
                    continue;
                }

                if slot_status != SLOT_OCCUPIED {
                    continue;
                }

                let candidate_hash = siphash13(
                    self.seed_k0,
                    self.seed_k1,
                    slot_transfer_id_hi,
                    slot_transfer_id_lo,
                );
                let candidate_home = (candidate_hash as usize) & self.mask;

                let dist_from_candidate_home = if free_pos >= candidate_home {
                    free_pos - candidate_home
                } else {
                    free_pos + self.capacity - candidate_home
                };

                if dist_from_candidate_home < NEIGHBOURHOOD_SIZE {
                    unsafe {
                        std::ptr::copy_nonoverlapping(candidate_slot, self.slots.add(free_pos), 1);
                    }

                    let slot_entries_count = unsafe { (*candidate_slot).entries_count };

                    if self.overflow_per_slot > 0 && slot_entries_count > 8 {
                        let src = unsafe { self.overflow_base.add(candidate_pos * self.overflow_per_slot) };
                        let dst = unsafe { self.overflow_base.add(free_pos * self.overflow_per_slot) };
                        unsafe {
                            std::ptr::copy_nonoverlapping(src, dst, self.overflow_per_slot);
                        }
                    }

                    let bitmaps_ptr = self.hop_bitmaps.as_ptr() as *mut u32;

                    let old_offset = if candidate_pos >= candidate_home {
                        candidate_pos - candidate_home
                    } else {
                        candidate_pos + self.capacity - candidate_home
                    };

                    unsafe {
                        *bitmaps_ptr.add(candidate_home) &= !(1u32 << old_offset);
                        *bitmaps_ptr.add(candidate_home) |= 1u32 << dist_from_candidate_home;

                        (*candidate_slot).status = SLOT_FREE;
                    }

                    free_pos = candidate_pos;
                    hopped = true;
                    break;
                }
            }

            if !hopped {
                panic!("TransferHashTable: cannot hop, table too full or all neighbours published");
            }
        }
    }

    unsafe fn write_new_slot(
        &self,
        slot: *mut TransferSlot,
        id_hi: u64,
        id_lo: u64,
        fp: u8,
        gsn: u64,
        connection_id: u64,
        batch_id: &[u8; 16],
        currency: &[u8; 16],
        entries_count: u8,
        transfer_datetime: u64,
        transfer_sequence_id: &[u8; 16],
    ) {
        let new_slot = TransferSlot {
            transfer_id_hi: id_hi,
            transfer_id_lo: id_lo,
            psl: 0,
            fingerprint: fp,
            ready: 0,
            decision: 0,
            entries_count,
            fail_reason: 0,
            confirmed_count: 0,
            failed_count: 0,
            gsn,
            connection_id,
            batch_id: *batch_id,
            commit_success_count: 0,
            rollback_success_count: 0,
            has_metadata: 0,
            prepare_success_bitmap: 0,
            status: SLOT_OCCUPIED,
            _pad1: [0; 3],
            created_at_ns: 0,
            transfer_datetime,
            transfer_sequence_id: *transfer_sequence_id,
            currency: *currency,
            metadata_slot: 0,
            _pad2: [0; 12],
            entries: [TransferHashTableEntry::zeroed(); 8],
        };

        unsafe {
            std::ptr::copy_nonoverlapping(&new_slot, slot, 1);
        }
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

    pub unsafe fn remove(&self, offset: u32) {
        let pos = offset as usize;
        let slot = unsafe { self.slots.add(pos) };

        let hash = unsafe {
            siphash13(
                self.seed_k0,
                self.seed_k1,
                (*slot).transfer_id_hi,
                (*slot).transfer_id_lo,
            )
        };

        let home = (hash as usize) & self.mask;

        let bit_offset = if pos >= home {
            pos - home
        } else {
            pos + self.capacity - home
        };

        let bitmaps_ptr = self.hop_bitmaps.as_ptr() as *mut u32;
        unsafe {
            *bitmaps_ptr.add(home) &= !(1u32 << bit_offset);

            (*slot).status = SLOT_DELETED;
            (*slot).ready = 0;

            *self.count.get() -= 1;
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

    pub unsafe fn get_entry(&self, offset: u32, index: usize) -> &TransferHashTableEntry {
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
}

#[cfg(test)]
#[cfg(not(miri))]
mod tests {
    use super::*;
    use pipeline::transfer_slot::*;
    use ringbuf::hash_table_slot_status::SLOT_DELETED;

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

    #[test]
    fn create_empty() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();
        assert_eq!(tht.count(), 0);
        assert_eq!(tht.capacity(), 64);
    }

    #[test]
    fn insert_and_read() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();
        unsafe {
            let offset = tht.insert(0, 1, 100, 7, &batch_id(), &currency(), 2, 0, &seq_id());
            assert_eq!(tht.count(), 1);

            let slot = tht.slot_ptr(offset);
            assert_eq!((*slot).transfer_id_lo, 1);
            assert_eq!((*slot).gsn, 100);
            assert_eq!((*slot).status, SLOT_OCCUPIED);
            assert_eq!((*slot).ready, 0);
        }
    }

    #[test]
    fn idempotent_insert() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();
        unsafe {
            let off1 = tht.insert(0, 1, 100, 7, &batch_id(), &currency(), 2, 0, &seq_id());
            let off2 = tht.insert(0, 1, 200, 8, &batch_id(), &currency(), 2, 0, &seq_id());
            assert_eq!(off1, off2);
            assert_eq!(tht.count(), 1);
        }
    }

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

    #[test]
    fn remove_marks_deleted_and_clears_bitmap() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();
        unsafe {
            let offset = tht.insert(0, 1, 100, 7, &batch_id(), &currency(), 2, 0, &seq_id());
            tht.publish(offset);
            tht.remove(offset);

            assert_eq!(tht.count(), 0);
            let slot = tht.slot_ptr(offset);
            assert_eq!((*slot).status, SLOT_DELETED);
            assert_eq!((*slot).ready, 0);

            assert!(tht.lookup(0, 1).is_none());
        }
    }

    #[test]
    fn insert_reuses_deleted_slot() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();
        unsafe {
            let off1 = tht.insert(0, 1, 100, 0, &batch_id(), &currency(), 2, 0, &seq_id());
            tht.publish(off1);
            tht.remove(off1);

            let off2 = tht.insert(0, 1, 200, 0, &batch_id(), &currency(), 2, 0, &seq_id());
            assert_eq!(tht.count(), 1);
            assert_eq!(off1, off2);

            let slot = tht.slot_ptr(off2);
            assert_eq!((*slot).gsn, 200);
            assert_eq!((*slot).status, SLOT_OCCUPIED);
        }
    }

    #[test]
    fn multiple_inserts() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();
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
    fn offset_stable_after_other_inserts() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();
        unsafe {
            let off1 = tht.insert(0, 1, 100, 0, &batch_id(), &currency(), 2, 0, &seq_id());
            tht.fill_entry(off1, 0, &make_entry(10, -500, 0, 1));
            tht.fill_entry(off1, 1, &make_entry(20, 500, 1, 2));
            tht.publish(off1);

            for i in 2..=21u64 {
                tht.insert(0, i, i * 100, 0, &batch_id(), &currency(), 2, 0, &seq_id());
            }

            let slot = tht.slot_ptr(off1);
            assert_eq!((*slot).transfer_id_lo, 1);
            assert_eq!((*slot).gsn, 100);
            assert_eq!((*slot).entries[0].amount, -500);
            assert_eq!((*slot).ready, 1);
        }
    }

    #[test]
    fn lookup_finds_element() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();
        unsafe {
            let off = tht.insert(0, 42, 100, 0, &batch_id(), &currency(), 2, 0, &seq_id());
            let found = tht.lookup(0, 42);
            assert_eq!(found, Some(off));
        }
    }

    #[test]
    fn lookup_returns_none_for_missing() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();
        unsafe {
            tht.insert(0, 1, 100, 0, &batch_id(), &currency(), 2, 0, &seq_id());
            assert!(tht.lookup(0, 999).is_none());
        }
    }

    #[test]
    fn overflow_works() {
        let tht = TransferHashTable::new(64, K0, K1, 12).unwrap();
        unsafe {
            let off = tht.insert(0, 1, 100, 0, &batch_id(), &currency(), 10, 0, &seq_id());
            for i in 0..8 {
                tht.fill_entry(off, i, &make_entry(i as u64 + 10, 100, 0, 1));
            }
            tht.fill_overflow(off, 0, &make_entry(18, 200, 1, 2));
            tht.fill_overflow(off, 1, &make_entry(19, 300, 1, 2));
            tht.publish(off);

            let ovf0 = tht.overflow_ptr(off, 0);
            assert_eq!((*ovf0).account_id_lo, 18);

            let e8 = tht.get_entry(off, 8);
            assert_eq!(e8.account_id_lo, 18);
            assert_eq!(e8.amount, 200);
        }
    }

    #[test]
    fn unpublished_slot_has_ready_zero() {
        let tht = TransferHashTable::new(64, K0, K1, 8).unwrap();
        unsafe {
            let off = tht.insert(0, 1, 100, 0, &batch_id(), &currency(), 2, 0, &seq_id());
            let slot = tht.slot_ptr(off);
            assert_eq!((*slot).ready, 0);
        }
    }

    #[test]
    fn get_entry_inline_and_overflow() {
        let tht = TransferHashTable::new(64, K0, K1, 12).unwrap();
        unsafe {
            let off = tht.insert(0, 1, 100, 0, &batch_id(), &currency(), 10, 0, &seq_id());
            for i in 0..8 {
                tht.fill_entry(off, i, &make_entry(i as u64 + 10, 100, 0, 1));
            }
            tht.fill_overflow(off, 0, &make_entry(18, 200, 1, 2));
            tht.fill_overflow(off, 1, &make_entry(19, 300, 1, 2));

            assert_eq!(tht.get_entry(off, 7).account_id_lo, 17);
            assert_eq!(tht.get_entry(off, 8).account_id_lo, 18);
            assert_eq!(tht.get_entry(off, 9).account_id_lo, 19);
        }
    }
}

#[cfg(test)]
mod miri_tests {
    use std::cell::UnsafeCell;
    use common::siphash::siphash13;
    use pipeline::transfer_slot::TransferSlot;
    use pipeline::transfer_hash_table_entry::TransferHashTableEntry;
    use ringbuf::hash_table_slot_status::{SLOT_DELETED, SLOT_FREE, SLOT_OCCUPIED};

    const NEIGHBOURHOOD_SIZE: usize = 32;

    struct MiriTht {
        #[allow(dead_code)]
        storage: Vec<TransferSlot>,
        slots: *mut TransferSlot,
        hop_bitmaps: Vec<u32>,
        #[allow(dead_code)]
        overflow_storage: Vec<TransferHashTableEntry>,
        overflow_base: *mut TransferHashTableEntry,
        overflow_per_slot: usize,
        capacity: usize,
        mask: usize,
        count: UnsafeCell<usize>,
        seed_k0: u64,
        seed_k1: u64,
    }

    impl MiriTht {
        fn new(capacity: usize, seed_k0: u64, seed_k1: u64) -> Self {
            let mut storage = vec![TransferSlot::zeroed(); capacity];
            let slots = storage.as_mut_ptr();
            let hop_bitmaps = vec![0u32; capacity];
            let overflow_per_slot = 2;
            let mut overflow_storage = vec![
                TransferHashTableEntry::zeroed();
                capacity * overflow_per_slot
            ];
            let overflow_base = overflow_storage.as_mut_ptr();

            Self {
                storage,
                slots,
                hop_bitmaps,
                overflow_storage,
                overflow_base,
                overflow_per_slot,
                capacity,
                mask: capacity - 1,
                count: UnsafeCell::new(0),
                seed_k0,
                seed_k1,
            }
        }

        fn count(&self) -> usize {
            unsafe { *self.count.get() }
        }

        unsafe fn insert(&self, id_hi: u64, id_lo: u64) -> u32 {
            let hash = siphash13(self.seed_k0, self.seed_k1, id_hi, id_lo);
            let fp = (hash >> 56) as u8;
            let home = (hash as usize) & self.mask;

            let bitmap = self.hop_bitmaps[home];
            let mut check_bits = bitmap;
            while check_bits != 0 {
                let bit_pos = check_bits.trailing_zeros() as usize;
                let pos = (home + bit_pos) & self.mask;
                let slot = self.slots.add(pos);
                if (*slot).fingerprint == fp
                    && (*slot).transfer_id_hi == id_hi
                    && (*slot).transfer_id_lo == id_lo
                    && (*slot).status == SLOT_OCCUPIED
                {
                    return pos as u32;
                }
                check_bits &= check_bits - 1;
            }

            for offset in 0..NEIGHBOURHOOD_SIZE {
                let pos = (home + offset) & self.mask;
                let slot = self.slots.add(pos);
                if (*slot).status == SLOT_FREE || (*slot).status == SLOT_DELETED {
                    self.write_new_slot(slot, id_hi, id_lo, fp);

                    let bitmaps_ptr = self.hop_bitmaps.as_ptr() as *mut u32;
                    *bitmaps_ptr.add(home) |= 1u32 << offset;
                    *self.count.get() += 1;
                    return pos as u32;
                }
            }

            panic!("THT full in Miri test");
        }

        unsafe fn write_new_slot(
            &self,
            slot: *mut TransferSlot,
            id_hi: u64,
            id_lo: u64,
            fp: u8,
        ) {
            let new_slot = TransferSlot {
                transfer_id_hi: id_hi,
                transfer_id_lo: id_lo,
                fingerprint: fp,
                status: SLOT_OCCUPIED,
                entries_count: 2,
                ..TransferSlot::zeroed()
            };
            std::ptr::copy_nonoverlapping(&new_slot, slot, 1);
        }

        unsafe fn lookup(&self, id_hi: u64, id_lo: u64) -> Option<u32> {
            let hash = siphash13(self.seed_k0, self.seed_k1, id_hi, id_lo);
            let fp = (hash >> 56) as u8;
            let home = (hash as usize) & self.mask;

            let bitmap = self.hop_bitmaps[home];
            let mut check_bits = bitmap;

            while check_bits != 0 {
                let bit_pos = check_bits.trailing_zeros() as usize;
                let pos = (home + bit_pos) & self.mask;
                let slot = self.slots.add(pos);

                if (*slot).status == SLOT_OCCUPIED
                    && (*slot).fingerprint == fp
                    && (*slot).transfer_id_hi == id_hi
                    && (*slot).transfer_id_lo == id_lo
                {
                    return Some(pos as u32);
                }
                check_bits &= check_bits - 1;
            }
            None
        }

        unsafe fn remove(&self, offset: u32) {
            let pos = offset as usize;
            let slot = self.slots.add(pos);

            let hash = siphash13(
                self.seed_k0, self.seed_k1,
                (*slot).transfer_id_hi, (*slot).transfer_id_lo,
            );
            let home = (hash as usize) & self.mask;

            let bit_offset = if pos >= home {
                pos - home
            } else {
                pos + self.capacity - home
            };

            let bitmaps_ptr = self.hop_bitmaps.as_ptr() as *mut u32;
            *bitmaps_ptr.add(home) &= !(1u32 << bit_offset);
            (*slot).status = SLOT_DELETED;
            *self.count.get() -= 1;
        }

        unsafe fn fill_entry(&self, offset: u32, index: usize, entry: &TransferHashTableEntry) {
            let slot = self.slots.add(offset as usize);
            std::ptr::copy_nonoverlapping(entry, &mut (*slot).entries[index], 1);
        }

        unsafe fn get_entry(&self, offset: u32, index: usize) -> &TransferHashTableEntry {
            if index < 8 {
                let slot = self.slots.add(offset as usize);
                &(*slot).entries[index]
            } else {
                &*self.overflow_base.add(
                    offset as usize * self.overflow_per_slot + (index - 8),
                )
            }
        }
    }

    const K0: u64 = 0x0123456789ABCDEF;
    const K1: u64 = 0xFEDCBA9876543210;

    #[test]
    fn miri_tht_insert_and_lookup() {
        let table = MiriTht::new(64, K0, K1);

        unsafe {
            let offset = table.insert(100, 200);
            let found = table.lookup(100, 200);
            assert_eq!(found, Some(offset));
            assert_eq!(table.count(), 1);
        }
    }

    #[test]
    fn miri_tht_insert_duplicate() {
        let table = MiriTht::new(64, K0, K1);

        unsafe {
            let offset1 = table.insert(100, 200);
            let offset2 = table.insert(100, 200);
            assert_eq!(offset1, offset2);
            assert_eq!(table.count(), 1);
        }
    }

    #[test]
    fn miri_tht_remove() {
        let table = MiriTht::new(64, K0, K1);

        unsafe {
            let offset = table.insert(100, 200);
            assert_eq!(table.count(), 1);

            table.remove(offset);
            assert_eq!(table.count(), 0);
            assert_eq!(table.lookup(100, 200), None);
        }
    }

    #[test]
    fn miri_tht_fill_and_get_entry() {
        let table = MiriTht::new(64, K0, K1);

        unsafe {
            let offset = table.insert(100, 200);

            let mut entry = TransferHashTableEntry::zeroed();
            entry.account_id_hi = 0xAAAA;
            entry.account_id_lo = 0xBBBB;
            entry.amount = 5000;
            entry.entry_type = 1;

            table.fill_entry(offset, 0, &entry);

            let read_entry = table.get_entry(offset, 0);
            assert_eq!(read_entry.account_id_hi, 0xAAAA);
            assert_eq!(read_entry.account_id_lo, 0xBBBB);
            assert_eq!(read_entry.amount, 5000);
        }
    }

    #[test]
    fn miri_tht_multiple_inserts_and_removes() {
        let table = MiriTht::new(64, K0, K1);

        unsafe {
            let mut offsets = Vec::new();
            for i in 0..20u64 {
                offsets.push(table.insert(i, i * 100));
            }
            assert_eq!(table.count(), 20);

            for i in 0..20u64 {
                assert!(table.lookup(i, i * 100).is_some());
            }

            for i in 0..10 {
                table.remove(offsets[i]);
            }
            assert_eq!(table.count(), 10);

            for i in 0..10u64 {
                assert_eq!(table.lookup(i, i * 100), None);
            }
            for i in 10..20u64 {
                assert!(table.lookup(i, i * 100).is_some());
            }
        }
    }

    #[test]
    fn miri_tht_copy_nonoverlapping_in_write_new_slot() {
        let table = MiriTht::new(64, K0, K1);

        unsafe {
            let offset = table.insert(0xDEAD, 0xBEEF);
            let slot = table.slots.add(offset as usize);
            assert_eq!((*slot).transfer_id_hi, 0xDEAD);
            assert_eq!((*slot).transfer_id_lo, 0xBEEF);
            assert_eq!((*slot).status, SLOT_OCCUPIED);
        }
    }
}