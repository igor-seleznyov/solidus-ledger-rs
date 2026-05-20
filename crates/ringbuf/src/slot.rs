use std::sync::atomic::{fence, Ordering};
use crate::sequence_mem_barrier::write_sequence_volatile;

/// # Safety
///
/// The implementing type should:
/// 1. be `#[repr(C, align(64))]`
/// 2. has field `sequence: u64` on offset 0 (the first field)
/// 3. Not contains pointers and types with Drop
///
/// undefined behavior if used in Ring Buffer.
pub unsafe trait Slot: Copy + Sized {
    fn sequence(&self) -> u64;

    fn set_sequence(&mut self, seq: u64);
}

pub struct ClaimedSlot<'a, T: Slot> {
    pub(crate) slot: &'a mut T,
    pub(crate) sequence: u64,
}

impl<'a, T: Slot> ClaimedSlot<'a, T> {
    pub fn as_mut(&mut self) -> &mut T {
        self.slot
    }

    pub fn publish(self) {
        fence(Ordering::Release);
        write_sequence_volatile(self.slot, self.sequence);
    }
}

pub struct ReadSlot<T: Slot> {
    pub(crate) slot_ptr: *const T,
    pub(crate) consumer_seq: *mut u64,
    pub(crate) next_seq: u64,
    pub(crate) release_seq: u64,
}

impl<T: Slot> ReadSlot<T> {
    pub fn as_ref(&self) -> &T {
        unsafe { &*self.slot_ptr }
    }

    pub fn release(self) {
        unsafe { *self.consumer_seq = self.next_seq; }
        unsafe {
            let seq_ptr = self.slot_ptr as *mut u64;
            *seq_ptr = self.release_seq;
        }
    }
}