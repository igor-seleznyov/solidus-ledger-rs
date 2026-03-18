use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{fence, Ordering};

#[repr(C, align(64))]
struct Slot<T> {
    sequence: u64,
    value: UnsafeCell<MaybeUninit<T>>,
}

pub struct RingBuffer<T> {
    slots: Box<[Slot<T>]>,
    capacity: usize,
    mask: usize,
    _pad1: CacheLinePad,
    producer_seq: UnsafeCell<u64>,
    _pad2: CacheLinePad,
    consumer_seq: UnsafeCell<u64>,
}

#[repr(C, align(64))]
struct CacheLinePad([u8; 56]);

impl CacheLinePad {
    fn new() -> Self { Self([0; 56]) }
}

unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "capacity must be a power of two");
        let mut slots = Vec::with_capacity(capacity);
        for i in 0..capacity {
            slots.push(
                Slot {
                    sequence: i as u64,
                    value: UnsafeCell::new(MaybeUninit::uninit()),
                }
            );
        }
        Self {
            slots: slots.into_boxed_slice(),
            capacity,
            mask: capacity - 1,
            _pad1: CacheLinePad::new(),
            producer_seq: UnsafeCell::new(0),
            _pad2: CacheLinePad::new(),
            consumer_seq: UnsafeCell::new(0),
        }
    }

    pub fn push(&self, value: T) -> Result<(), T> {
        unsafe {
            let seq = *self.producer_seq.get();
            let slot = &self.slots[seq as usize & self.mask];

            let slot_seq = acquire_load_u64(&slot.sequence as *const u64);
            if slot_seq != seq {
                return Err(value);
            }

            slot.value.get().write(MaybeUninit::new(value));

            release_store_u64(
                &slot.sequence as *const u64 as *mut u64,
                seq + 1
            );
            *self.producer_seq.get() = seq + 1;
            Ok(())
        }
    }

    pub fn pop(&self) -> Option<T> {
        unsafe {
            let seq = *self.consumer_seq.get();
            let slot = &self.slots[seq as usize & self.mask];

            let slot_seq = acquire_load_u64(&slot.sequence as *const u64);
            if slot_seq != seq + 1 {
                return None;
            }

            let value = (*slot.value.get()).assume_init_read();

            release_store_u64(
                &slot.sequence as *const u64 as *mut u64,
                seq + self.capacity as u64,
            );
            *self.consumer_seq.get() = seq + 1;
            Some(value)
        }
    }

    pub fn push_batch(&self, values: &mut Vec<T>) -> usize {
        unsafe {
            let mut seq = *self.producer_seq.get();
            let mut written = 0;

            while let Some(_value) = values.last() {
                let slot = &self.slots[seq as usize & self.mask];
                let slot_seq = std::ptr::read_volatile(&slot.sequence as *const u64);
                if slot_seq != seq {
                    break;
                }
                let value = values.pop().unwrap();
                slot.value.get().write(MaybeUninit::new(value));
                std::ptr::write(
                    std::ptr::addr_of!(slot.sequence) as *mut u64,
                    seq + 1,
                );
                seq += 1;
                written += 1;
            }

            if written > 0 {
                fence(Ordering::Release);
                *self.producer_seq.get() = seq;
            }
            written
        }
    }

    pub fn pop_batch(&self, out: &mut Vec<T>, max: usize) -> usize {
        unsafe {
            let mut seq = *self.consumer_seq.get();
            let mut read = 0;

            fence(Ordering::Acquire);

            while read < max {
                let slot = &self.slots[seq as usize & self.mask];
                let slot_seq = std::ptr::read_volatile(&slot.sequence as *const u64);
                if slot_seq != seq + 1 {
                    break;
                }

                out.push((*slot.value.get()).assume_init_read());
                std::ptr::write(
                    std::ptr::addr_of!(slot.sequence) as *mut u64,
                    seq + self.capacity as u64,
                );
                seq += 1;
                read += 1;
            }

            if read > 0 {
                fence(Ordering::Release);
                *self.consumer_seq.get() = seq;
            }
            read
        }
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

#[inline(always)]
unsafe fn release_store_u64(ptr: *mut u64, val: u64) {
    fence(Ordering::Release);
    unsafe { std::ptr::write_volatile(ptr, val); }
}

#[inline(always)]
    unsafe fn acquire_load_u64(ptr: *const u64) -> u64 {
    let value = unsafe { std::ptr::read_volatile(ptr) };
    fence(Ordering::Acquire);
    value
}

