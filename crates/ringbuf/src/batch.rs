use std::sync::atomic::{fence, Ordering};
use crate::slot::Slot;
use crate::sequence_mem_barrier::write_sequence_volatile;

pub struct ClaimedBatch<T: Slot> {
    pub(crate) base: *mut T,
    pub(crate) mask: usize,
    pub(crate) start: u64,
    pub(crate) count: usize,
    pub(crate) capacity: u64,
}

impl<T: Slot> ClaimedBatch<T> {
    pub fn slot_mut(&mut self, i: usize) -> &mut T {
        assert!(i < self.count);
        let index = ((self.start + i as u64) as usize) & self.mask;
        unsafe { &mut *self.base.add(index) }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn publish(self) {
        for i in 0..self.count - 1 {
            let index = ((self.start + i as u64) as usize) & self.mask;
            let slot = unsafe { &mut *self.base.add(index) };
            slot.set_sequence(self.start + i as u64 + self.capacity);
        }

        let batch_end_index = self.count - 1;
        let last_index = ((self.start + batch_end_index as u64) as usize) & self.mask;
        let last_slot = unsafe { &mut *self.base.add(last_index) };
        fence(Ordering::Release);
        write_sequence_volatile(last_slot, self.start + batch_end_index as u64 + self.capacity);
    }
}

pub struct DrainBatch<T: Slot> {
    pub(crate) base: *mut T,
    pub(crate) mask: usize,
    pub(crate) consumer_seq: *mut u64,
    pub(crate) start: u64,
    pub(crate) count: usize,
    pub(crate) capacity: u64,
}

impl<T: Slot> DrainBatch<T> {
    pub fn slot(&self, i: usize) -> &T {
        assert!(i < self.count);
        let index = ((self.start + i as u64) as usize) &self.mask;
        unsafe { &*self.base.add(index) }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn release(self) {
        for i in 0..self.count {
            let index = ((self.start + i as u64) as usize) & self.mask;
            let slot = unsafe { &mut *self.base.add(index) };
            slot.set_sequence(self.start + i as u64 + self.capacity);
        }
        unsafe { *self.consumer_seq = self.start + self.count as u64; }
    }
}
