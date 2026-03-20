use ringbuf::arena::Arena;
use common::siphash::siphash13;
use ringbuf::hash_table_slot_status::{SLOT_FREE, SLOT_OCCUPIED};
use crate::partition_version_table_slot::PartitionVersionTableSlot;
use crate::version_record::VersionRecord;

pub struct PartitionVersionTable {
    #[allow(dead_code)]
    arena: Arena,
    slots: *mut PartitionVersionTableSlot,
    #[allow(dead_code)]
    overflow_arena: Option<Arena>,
    overflow_base: *mut VersionRecord,
    overflow_free_stack: Vec<u32>,
    next_block: Vec<u32>,
    capacity: usize,
    mask: usize,
    count: usize,
    seed_k0: u64,
    seed_k1: u64,
}

impl PartitionVersionTable {
    pub fn new(capacity: usize, seed_k0: u64, seed_k1: u64) -> std::io::Result<Self> {
        assert!(capacity.is_power_of_two());
        assert!(capacity >= 16);

        let total_size = capacity * PartitionVersionTableSlot::SIZE;
        let arena = Arena::new(total_size)?;
        let slots = arena.as_ptr() as *mut PartitionVersionTableSlot;

        let overflow_block_bytes = 8 * VersionRecord::SIZE;
        let overflow_total = capacity * overflow_block_bytes;
        let overflow_arena = Arena::new(overflow_total)?;
        let overflow_base = overflow_arena.as_ptr() as *mut VersionRecord;

        let overflow_free_stack: Vec<u32> = (0..capacity as u32).rev().collect();
        let next_block = vec![0u32; capacity];

        Ok(
            Self {
                arena,
                slots,
                overflow_arena: Some(overflow_arena),
                overflow_base,
                overflow_free_stack,
                next_block,
                capacity,
                mask: capacity - 1,
                count: 0,
                seed_k0,
                seed_k1,
            }
        )
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub unsafe fn get_or_create(
        &mut self,
        id_hi: u64,
        id_lo: u64,
    ) -> *mut PartitionVersionTableSlot {
        let hash = siphash13(self.seed_k0, self.seed_k1, id_hi, id_lo);
        let mut pos = (hash as usize) & self.mask;
        let mut psl: u8 = 1;

        let mut inserting = PartitionVersionTableSlot::zeroed();
        inserting.account_id_hi = id_hi;
        inserting.account_id_lo = id_lo;
        inserting.psl = 1;
        inserting.status = SLOT_OCCUPIED;

        let mut result: *mut PartitionVersionTableSlot = std::ptr::null_mut();

        loop {
            let slot = unsafe { self.slots.add(pos) };

            let slot_psl = unsafe{ (*slot).psl };
            let slot_status = unsafe { (*slot).status };

            if slot_status == SLOT_FREE {
                inserting.psl = psl;
                unsafe {
                    std::ptr::write(slot, inserting);
                }
                self.count += 1;
                return if result.is_null() { slot } else { result };
            }

            let slot_account_id_hi = unsafe { (*slot).account_id_hi };
            let slot_account_id_lo = unsafe { (*slot).account_id_lo };

            if slot_account_id_hi == id_hi && slot_account_id_lo == id_lo {
                return slot;
            }

            if slot_psl < psl {
                unsafe {
                    std::mem::swap(&mut *slot, &mut inserting);
                }
                psl = inserting.psl;
                if result.is_null() {
                    result = slot;
                }
            }

            pos = (pos + 1) & self.mask;
            psl += 1;
            inserting.psl = psl;
        }
    }

    fn alloc_overflow_block(&mut self) -> u32 {
        self.overflow_free_stack.pop()
            .expect("Partition Version Table pool exhausted")
    }

    fn free_overflow_block(&mut self, block_index: u32) {
        self.overflow_free_stack.push(block_index);
    }

    unsafe fn get_or_alloc_overflow_chain(
        &mut self,
        slot: *mut PartitionVersionTableSlot,
        block_number: usize,
    ) -> u32 {
        if (*slot).overflow == 0 {
            let block_id = self.alloc_overflow_block();
            (*slot).overflow = block_id + 1;
        }

        let mut current = (*slot).overflow - 1;

        for _ in 0..block_number {
            let next = self.next_block[current as usize];
            if next == 0 {
                let new_block = self.alloc_overflow_block();
                self.next_block[current as usize] = new_block + 1;
                current = new_block;
            } else {
                current = next - 1;
            }
        }

        current
    }

    pub unsafe fn record_version(
        &mut self,
        id_hi: u64,
        id_lo: u64,
        gsn: u64,
        balance: i64,
    ) {
        let slot = unsafe {self.get_or_create(id_hi, id_lo) };
        let count = unsafe { (*slot).count as usize };

        if count < 8 {
            unsafe {
                (*slot).inline[count].gsn = gsn;
                (*slot).inline[count].balance = balance;
            }
        } else {
            let overflow_index = count - 8;
            let block_number = overflow_index >> 3;
            let entry_index = overflow_index & 7;

            let block_id = unsafe { self.get_or_alloc_overflow_chain(slot, block_number) };

            let ptr = unsafe { self.overflow_base.add(block_id as usize * 8 + entry_index) };

            unsafe {
                (*ptr).gsn = gsn;
                (*ptr).balance = balance;
            }
        }

        unsafe {
            (*slot).count = (count + 1) as u32;
        }
    }

    pub unsafe fn read_balance(
        &self,
        id_hi: u64,
        id_lo: u64,
        committed_gsn: u64,
    ) -> Option<i64> {
        let slot_opt = self.lookup(id_hi, id_lo);
        let slot = match slot_opt {
            Some(slot) => slot,
            None => return None,
        };

        let count = unsafe { (*slot).count as usize };
        let inline_count = if count <= 8 { count } else { 8 };

        let mut latest_committed_gsn = 0u64;
        let mut latest_committed_balance = None;

        for i in 0..inline_count {
            let record = &(*slot).inline[i];
            if record.gsn <= committed_gsn && record.gsn > latest_committed_gsn {
                latest_committed_gsn = record.gsn;
                latest_committed_balance = Some(record.balance);
            }
        }

        let slot_overflow = unsafe { (*slot).overflow };

        if count > 8 && slot_overflow != 0 {
            let mut remaining = count - 8;
            let mut current_block = slot_overflow - 1;

            while remaining > 0 {
                let entries_in_block = if remaining > 8 { 8 } else { remaining };

                for i in 0..entries_in_block {
                    let record = &*self.overflow_base.add(current_block as usize * 8 + i);
                    if record.gsn <= committed_gsn && record.gsn > latest_committed_gsn {
                        latest_committed_gsn = record.gsn;
                        latest_committed_balance = Some(record.balance);
                    }
                }

                remaining -= entries_in_block;

                if remaining > 0 {
                    let next = self.next_block[current_block as usize];
                    if next == 0 { break; }
                    current_block = next - 1;
                }
            }
        }

        latest_committed_balance
    }

    pub unsafe fn compact(
        &mut self,
        id_hi: u64,
        id_lo: u64,
        committed_gsn: u64,
    ) {
        let slot_opt = self.lookup(id_hi, id_lo);
        let slot = match slot_opt {
            Some(slot) => slot,
            None => return,
        };

        let count = unsafe { (*slot).count as usize };
        if count == 0 {
            return;
        }

        let mut versions = Vec::with_capacity(count);
        let inline_count = if count <= 8 { count } else { 8 };

        for i in 0..inline_count {
            versions.push((*slot).inline[i]);
        }

        let slot_overflow = unsafe { (*slot).overflow };

        if count > 8 && slot_overflow != 0 {
            let mut remaining = count - 8;
            let mut current_block = unsafe { (*slot).overflow - 1 };

            while remaining > 0 {
                let entries_in_block = if remaining > 8 { 8 } else { remaining };
                for i in 0..entries_in_block {
                    let record = &*self.overflow_base.add(current_block as usize * 8 + i);
                    versions.push(*record);
                }
                remaining -= entries_in_block;
                if remaining > 0 {
                    let next = self.next_block[current_block as usize];
                    if next == 0 {
                        break;
                    }
                    current_block = next - 1;
                }
            }
        }

        let mut latest_committed_gsn = 0u64;

        for version in &versions {
            if version.gsn <= committed_gsn && version.gsn > latest_committed_gsn {
                latest_committed_gsn = version.gsn;
            }
        }

        let surviving: Vec<VersionRecord> = versions.into_iter().filter(
            |version| {
                version.gsn > committed_gsn || version.gsn == latest_committed_gsn
            }
        ).collect();

        let inline_slot_overflow = unsafe { (*slot).overflow };

        if inline_slot_overflow != 0 {
            let mut current_block = unsafe { (*slot).overflow - 1 };
            loop {
                let next = self.next_block[current_block as usize];
                self.next_block[current_block as usize] = 0;
                self.free_overflow_block(current_block);
                if next == 0 {
                    break;
                }
                current_block = next - 1;
            }
            unsafe {
                (*slot).overflow = 0;
            }
        }

        unsafe {
            (*slot).count = 0;
        }
        for version in &surviving {
            self.record_version_to_slot(slot, version.gsn, version.balance);
        }
    }

    unsafe fn record_version_to_slot(
        &mut self,
        slot: *mut PartitionVersionTableSlot,
        gsn: u64,
        balance: i64,
    ) {
        let count = unsafe { (*slot).count as usize };

        if count < 8 {
            unsafe {
                (*slot).inline[count].gsn = gsn;
                (*slot).inline[count].balance = balance;
            }
        } else {
            let overflow_index = count - 8;
            let block_number = overflow_index >> 3;
            let entry_index = overflow_index & 7;
            let block_id = unsafe {  self.get_or_alloc_overflow_chain(slot, block_number) };
            let ptr = unsafe { self.overflow_base.add(block_id as usize * 8 + entry_index) };
            unsafe {
                (*ptr).gsn = gsn;
                (*ptr).balance = balance;
            }
        }

        unsafe {
            (*slot).count = (count + 1) as u32;
        }
    }

    pub unsafe fn lookup(
        &self,
        id_hi: u64,
        id_lo: u64,
    ) -> Option<*mut PartitionVersionTableSlot> {
        let hash = siphash13(self.seed_k0, self.seed_k1, id_hi, id_lo);
        let mut pos = (hash as usize) & self.mask;
        let mut psl: u8 = 1;

        loop {
            let slot = unsafe { self.slots.add(pos) };

            let slot_psl = unsafe { (*slot).psl };
            let slot_status = unsafe { (*slot).status };

            if slot_status == SLOT_FREE {
                return None;
            }
            if slot_psl < psl {
                return None;
            }

            let slot_account_id_hi = unsafe { (*slot).account_id_hi };
            let slot_account_id_lo = unsafe { (*slot).account_id_lo };

            if slot_account_id_hi == id_hi && slot_account_id_lo == id_lo {
                return Some(slot);
            }

            pos = (pos + 1) & self.mask;
            psl += 1;
        }
    }
}

unsafe impl Send for PartitionVersionTable {}

#[cfg(test)]
mod tests {
    use super::*;

    const K0: u64 = 0xBBBBCCCCDDDDEEEE;
    const K1: u64 = 0x5555666677778888;

    #[test]
    fn create_empty() {
        let pvt = PartitionVersionTable::new(64, K0, K1).unwrap();
        assert_eq!(pvt.count(), 0);
        assert_eq!(pvt.capacity(), 64);
    }

    #[test]
    fn record_and_read_single_version() {
        let mut pvt = PartitionVersionTable::new(64, K0, K1).unwrap();

        unsafe {
            pvt.record_version(0, 1, 100, 5000);

            let balance = pvt.read_balance(0, 1, 100);
            assert_eq!(balance, Some(5000));
        }
    }

    #[test]
    fn read_balance_respects_committed_gsn() {
        let mut pvt = PartitionVersionTable::new(64, K0, K1).unwrap();

        unsafe {
            pvt.record_version(0, 1, 100, 5000);
            pvt.record_version(0, 1, 200, 4500);
            pvt.record_version(0, 1, 300, 4000);

            // committed_gsn=200 → balance at gsn=200
            assert_eq!(pvt.read_balance(0, 1, 200), Some(4500));

            // committed_gsn=100 → balance at gsn=100
            assert_eq!(pvt.read_balance(0, 1, 100), Some(5000));

            // committed_gsn=300 → balance at gsn=300
            assert_eq!(pvt.read_balance(0, 1, 300), Some(4000));

            // committed_gsn=150 → balance at gsn=100 (последний <= 150)
            assert_eq!(pvt.read_balance(0, 1, 150), Some(5000));

            // committed_gsn=50 → нет версий <= 50
            assert_eq!(pvt.read_balance(0, 1, 50), None);
        }
    }

    #[test]
    fn multiple_accounts() {
        let mut pvt = PartitionVersionTable::new(64, K0, K1).unwrap();

        unsafe {
            pvt.record_version(0, 1, 100, 1000);
            pvt.record_version(0, 2, 100, 2000);
            pvt.record_version(0, 1, 200, 900);

            assert_eq!(pvt.read_balance(0, 1, 200), Some(900));
            assert_eq!(pvt.read_balance(0, 2, 200), Some(2000));
        }
    }

    #[test]
    fn inline_capacity_8_versions() {
        let mut pvt = PartitionVersionTable::new(64, K0, K1).unwrap();

        unsafe {
            for i in 1..=8u64 {
                pvt.record_version(0, 1, i * 100, (1000 - i * 50) as i64);
            }

            let slot = pvt.lookup(0, 1).unwrap();
            assert_eq!((*slot).count, 8);

            assert_eq!(pvt.read_balance(0, 1, 800), Some(600)); // 1000 - 8*50
            assert_eq!(pvt.read_balance(0, 1, 100), Some(950)); // 1000 - 1*50
        }
    }

    #[test]
    fn overflow_allocates_block() {
        let mut pvt = PartitionVersionTable::new(64, K0, K1).unwrap();

        unsafe {
            for i in 1..=10u64 {
                pvt.record_version(0, 1, i * 100, i as i64 * 1000);
            }

            let slot = pvt.lookup(0, 1).unwrap();
            assert_eq!((*slot).count, 10);
            assert!((*slot).overflow > 0, "overflow block should be allocated");

            assert_eq!(pvt.read_balance(0, 1, 1000), Some(10000));
            assert_eq!(pvt.read_balance(0, 1, 800), Some(8000));
            assert_eq!(pvt.read_balance(0, 1, 100), Some(1000));
        }
    }

    #[test]
    fn overflow_full_16_versions() {
        let mut pvt = PartitionVersionTable::new(64, K0, K1).unwrap();

        unsafe {
            for i in 1..=16u64 {
                pvt.record_version(0, 1, i * 100, i as i64 * 1000);
            }

            let slot = pvt.lookup(0, 1).unwrap();
            assert_eq!((*slot).count, 16);

            assert_eq!(pvt.read_balance(0, 1, 1600), Some(16000));
            assert_eq!(pvt.read_balance(0, 1, 100), Some(1000));
            assert_eq!(pvt.read_balance(0, 1, 900), Some(9000));
        }
    }

    #[test]
    fn overflow_chain_multiple_blocks() {
        let mut pvt = PartitionVersionTable::new(64, K0, K1).unwrap();

        unsafe {
            for i in 1..=20u64 {
                pvt.record_version(0, 1, i * 100, i as i64 * 1000);
            }

            let slot = pvt.lookup(0, 1).unwrap();
            assert_eq!((*slot).count, 20);
            assert!((*slot).overflow > 0);

            assert_eq!(pvt.read_balance(0, 1, 2000), Some(20000));
            assert_eq!(pvt.read_balance(0, 1, 100), Some(1000));
            assert_eq!(pvt.read_balance(0, 1, 900), Some(9000));  // overflow block 1
            assert_eq!(pvt.read_balance(0, 1, 1700), Some(17000)); // overflow block 2
        }
    }

    #[test]
    fn compact_removes_stale_versions() {
        let mut pvt = PartitionVersionTable::new(64, K0, K1).unwrap();

        unsafe {
            pvt.record_version(0, 1, 100, 1000);
            pvt.record_version(0, 1, 200, 900);
            pvt.record_version(0, 1, 300, 800);
            pvt.record_version(0, 1, 400, 700);

            pvt.compact(0, 1, 300);

            let slot = pvt.lookup(0, 1).unwrap();
            assert_eq!((*slot).count, 2);

            assert_eq!(pvt.read_balance(0, 1, 300), Some(800));
            assert_eq!(pvt.read_balance(0, 1, 400), Some(700));
            assert_eq!(pvt.read_balance(0, 1, 200), None);
            assert_eq!(pvt.read_balance(0, 1, 100), None);
        }
    }

    #[test]
    fn compact_frees_overflow_blocks() {
        let mut pvt = PartitionVersionTable::new(64, K0, K1).unwrap();

        let initial_free = pvt.overflow_free_stack.len();

        unsafe {
            for i in 1..=12u64 {
                pvt.record_version(0, 1, i * 100, i as i64 * 1000);
            }

            let used_blocks = initial_free - pvt.overflow_free_stack.len();
            assert_eq!(used_blocks, 1);

            pvt.compact(0, 1, 1100);

            let slot = pvt.lookup(0, 1).unwrap();
            assert_eq!((*slot).count, 2);
            assert_eq!((*slot).overflow, 0);

            assert_eq!(pvt.overflow_free_stack.len(), initial_free);
        }
    }

    #[test]
    fn compact_keeps_latest_committed() {
        let mut pvt = PartitionVersionTable::new(64, K0, K1).unwrap();

        unsafe {
            pvt.record_version(0, 1, 100, 1000);
            pvt.record_version(0, 1, 200, 900);

            pvt.compact(0, 1, 200);

            let slot = pvt.lookup(0, 1).unwrap();
            assert_eq!((*slot).count, 1); // только gsn=200

            assert_eq!(pvt.read_balance(0, 1, 200), Some(900));
            assert_eq!(pvt.read_balance(0, 1, 100), None); // удалён
        }
    }

    #[test]
    fn compact_no_committed_keeps_all() {
        let mut pvt = PartitionVersionTable::new(64, K0, K1).unwrap();

        unsafe {
            pvt.record_version(0, 1, 100, 1000);
            pvt.record_version(0, 1, 200, 900);

            pvt.compact(0, 1, 50);

            let slot = pvt.lookup(0, 1).unwrap();
            assert_eq!((*slot).count, 2);
        }
    }

    #[test]
    fn unknown_account_returns_none() {
        let pvt = PartitionVersionTable::new(64, K0, K1).unwrap();

        unsafe {
            assert_eq!(pvt.read_balance(0, 999, 100), None);
        }
    }

    #[test]
    fn robin_hood_displacement() {
        let mut pvt = PartitionVersionTable::new(16, K0, K1).unwrap();

        unsafe {
            for i in 1..=10u64 {
                pvt.record_version(0, i, i * 100, i as i64 * 1000);
            }
        }

        assert_eq!(pvt.count(), 10);

        unsafe {
            for i in 1..=10u64 {
                assert_eq!(
                    pvt.read_balance(0, i, i * 100),
                    Some(i as i64 * 1000),
                );
            }
        }
    }
}