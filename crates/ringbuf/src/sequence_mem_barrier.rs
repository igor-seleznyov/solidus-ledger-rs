use std::ptr;
use std::sync::atomic::{fence, Ordering};
use crate::slot::Slot;

pub fn read_sequence_acquire<T: Slot>(slot: &T) -> u64 {
    let value = unsafe {
        ptr::read_volatile(&slot.sequence() as *const u64)
    };
    fence(Ordering::Acquire);
    value
}

pub fn write_sequence_volatile<T: Slot>(slot: &mut T, seq: u64) {
    unsafe {
        let seq_ptr = slot as *mut T as *mut u64;
        ptr::write_volatile(seq_ptr, seq);
    }
}
