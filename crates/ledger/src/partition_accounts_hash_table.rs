use crate::account_slot::AccountSlot;
use common::siphash::siphash13;
use ringbuf::arena::Arena;

pub struct PartitionAccountsHashTable {
    #[allow(dead_code)]
    arena: Arena,
    slots: *mut AccountSlot,
    capacity: usize,
    mask: usize,
    count: usize,
    seed_k0: u64,
    seed_k1: u64,
}

impl PartitionAccountsHashTable {
    pub fn new(capacity: usize, seed_k0: u64, seed_k1: u64) -> std::io::Result<Self> {
        assert!(capacity.is_power_of_two(), "PAHT capacity must be a power of two");
        assert!(capacity >= 16, "PAHT capacity must be at least 16");

        let total_size = capacity * AccountSlot::SIZE;
        let arena = Arena::new(total_size)?;
        let slots = arena.as_ptr() as *mut AccountSlot;

        Ok(
            Self {
                arena,
                slots,
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

    pub fn load_factor(&self) -> f64 {
        self.count as f64 / self.capacity as f64
    }

    pub unsafe fn get_or_create(&mut self, id_hi: u64, id_lo: u64) -> *mut AccountSlot {
        let hash = siphash13(self.seed_k0, self.seed_k1, id_hi, id_lo);
        let mut pos = (hash as usize) & self.mask;
        let mut psl: u8 = 1;

        let mut inserting = AccountSlot::zeroed();
        inserting.account_id_hi = id_hi;
        inserting.account_id_lo = id_lo;
        inserting.psl = 1;

        let mut result: *mut AccountSlot = std::ptr::null_mut();

        loop {
            let slot = unsafe { self.slots.add(pos) };

            let slot_psl = unsafe { (*slot).psl };

            if slot_psl == 0 {
                unsafe { std::ptr::write(slot, inserting); }
                self.count += 1;
                return if result.is_null() { slot } else { result };
            }

            let slot_id_hi = unsafe { (*slot).account_id_hi };
            let slot_id_lo = unsafe { (*slot).account_id_lo };

            if slot_id_hi == id_hi && slot_id_lo == id_lo {
                return slot;
            }

            if slot_psl < psl {
                unsafe { std::mem::swap(&mut *slot, &mut inserting) };
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

    pub unsafe fn lookup(&self, id_hi: u64, id_lo: u64) -> Option<*mut AccountSlot> {
        let hash = siphash13(self.seed_k0, self.seed_k1, id_hi, id_lo);
        let mut pos = (hash as usize) & self.mask;
        let mut psl: u8 = 1;

        loop {
            let slot = unsafe { self.slots.add(pos) };

            let slot_psl = unsafe { (*slot).psl };

            if slot_psl == 0 {
                return None;
            }

            if slot_psl < psl {
                return None;
            }

            let account_id_hi = unsafe { (*slot).account_id_hi };
            let slot_id_lo = unsafe { (*slot).account_id_lo };

            if account_id_hi == id_hi && slot_id_lo == id_lo {
                return Some(slot);
            }

            pos = (pos + 1) & self.mask;
            psl += 1;
        }
    }
}

unsafe impl Send for PartitionAccountsHashTable {}

#[cfg(test)]
mod tests {
    use super::*;

    const K0: u64 = 0x0123456789ABCDEF;
    const K1: u64 = 0xFEDCBA9876543210;

    #[test]
    fn create_empty() {
        let paht = PartitionAccountsHashTable::new(64, K0, K1).unwrap();
        assert_eq!(paht.count(), 0);
        assert_eq!(paht.capacity(), 64);
    }

    #[test]
    fn insert_and_lookup() {
        let mut paht = PartitionAccountsHashTable::new(64, K0, K1).unwrap();

        unsafe {
            let slot = paht.get_or_create(0, 1);
            (*slot).balance = 1000;
        }

        assert_eq!(paht.count(), 1);

        unsafe {
            let found = paht.lookup(0, 1).expect("should find account");
            assert_eq!((*found).balance, 1000);
            assert_eq!((*found).account_id_hi, 0);
            assert_eq!((*found).account_id_lo, 1);
        }
    }

    #[test]
    fn lookup_nonexistent_returns_none() {
        let paht = PartitionAccountsHashTable::new(64, K0, K1).unwrap();

        unsafe {
            assert!(paht.lookup(0, 999).is_none());
        }
    }

    #[test]
    fn get_or_create_returns_existing() {
        let mut paht = PartitionAccountsHashTable::new(64, K0, K1).unwrap();

        unsafe {
            let slot1 = paht.get_or_create(0, 1);
            (*slot1).balance = 500;

            let slot2 = paht.get_or_create(0, 1);
            assert_eq!((*slot2).balance, 500);
        }

        assert_eq!(paht.count(), 1);
    }

    #[test]
    fn multiple_accounts() {
        let mut paht = PartitionAccountsHashTable::new(64, K0, K1).unwrap();

        unsafe {
            for i in 1..=10u64 {
                let slot = paht.get_or_create(0, i);
                (*slot).balance = (i * 100) as i64;
            }
        }

        assert_eq!(paht.count(), 10);

        unsafe {
            for i in 1..=10u64 {
                let slot = paht.lookup(0, i).expect("should find");
                assert_eq!((*slot).balance, (i * 100) as i64);
            }
        }
    }

    #[test]
    fn robin_hood_displacement() {
        let mut paht = PartitionAccountsHashTable::new(16, K0, K1).unwrap();

        unsafe {
            for i in 1..=10u64 {
                let slot = paht.get_or_create(0, i);
                (*slot).balance = i as i64;
            }
        }

        assert_eq!(paht.count(), 10);

        unsafe {
            for i in 1..=10u64 {
                let slot = paht.lookup(0, i).expect("should find after displacement");
                assert_eq!((*slot).balance, i as i64);
            }
        }

        unsafe {
            assert!(paht.lookup(0, 999).is_none());
        }
    }

    #[test]
    fn high_load_factor() {
        let capacity = 64;
        let mut paht = PartitionAccountsHashTable::new(capacity, K0, K1).unwrap();
        let fill = (capacity as f64 * 0.75) as u64;  // 48 аккаунтов

        unsafe {
            for i in 1..=fill {
                let slot = paht.get_or_create(0, i);
                (*slot).balance = i as i64;
            }
        }

        assert_eq!(paht.count(), fill as usize);
        assert!(paht.load_factor() > 0.7);

        unsafe {
            for i in 1..=fill {
                let slot = paht.lookup(0, i).unwrap();
                assert_eq!((*slot).balance, i as i64);
            }
        }
    }

    #[test]
    fn psl_bounded() {
        let mut paht = PartitionAccountsHashTable::new(256, K0, K1).unwrap();
        let fill = 192u64;  // 75% load factor

        unsafe {
            for i in 1..=fill {
                paht.get_or_create(0, i);
            }
        }

        let mut max_psl: u8 = 0;
        unsafe {
            for i in 0..256 {
                let slot = paht.slots.add(i);
                if (*slot).psl > max_psl {
                    max_psl = (*slot).psl;
                }
            }
        }

        assert!(
            max_psl < 20,
            "Robin Hood PSL too high: {} (expected < 20 for 256 capacity)",
            max_psl
        );
    }

    #[test]
    fn staged_fields_independent() {
        let mut paht = PartitionAccountsHashTable::new(64, K0, K1).unwrap();

        unsafe {
            let slot = paht.get_or_create(0, 1);
            (*slot).balance = 1000;
            (*slot).staged_income = 200;
            (*slot).staged_outcome = 50;

            let found = paht.lookup(0, 1).unwrap();
            assert_eq!((*found).balance, 1000);
            assert_eq!((*found).staged_income, 200);
            assert_eq!((*found).staged_outcome, 50);

            // Effective balance = balance + staged_income - staged_outcome
            let effective = (*found).balance + (*found).staged_income - (*found).staged_outcome;
            assert_eq!(effective, 1150);
        }
    }
}