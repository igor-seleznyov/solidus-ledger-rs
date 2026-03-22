use std::sync::atomic::{AtomicU64, Ordering};
use crate::arena::Arena;
use crate::slot::Slot;
use crate::slot::ClaimedSlot;
use crate::slot::ReadSlot;
use crate::sequence_mem_barrier::read_sequence_acquire;
use crate::batch::DrainBatch;
use crate::batch::ClaimedBatch;

const HEADER_SIZE: usize = 128;

pub struct MpscRingBuffer<T: Slot> {
    #[allow(dead_code)]
    arena: Arena,
    capacity: usize,
    mask: usize,
    claim_seq: *const AtomicU64,
    consumer_seq: *mut u64,
    base: *mut T,
}

impl<T: Slot> MpscRingBuffer<T> {
    pub fn new(capacity: usize) -> std::io::Result<Self> {
        assert!(capacity.is_power_of_two(), "capacity must be a power of two");
        assert!(
            std::mem::size_of::<T>() >= 64,
            "slot size must be at least 64 bytes (cache line)"
        );
        assert!(
            std::mem::align_of::<T>() == 64,
            "slot alignment must be 64 bytes"
        );

        let slot_size = std::mem::size_of::<T>();
        let total_size = HEADER_SIZE + capacity * slot_size;
        let arena = Arena::new(total_size)?;
        let ptr = arena.as_ptr();

        let claim_seq = ptr as *const AtomicU64;
        let consumer_seq = unsafe { ptr.add(64) as *mut u64 };
        let base = unsafe { ptr.add(HEADER_SIZE) as *mut T };

        for i in 0..capacity {
            unsafe {
                let slot = &mut *base.add(i);
                slot.set_sequence(i as u64);
            }
        }

        Ok(
            Self {
                arena,
                capacity,
                mask: capacity - 1,
                claim_seq,
                consumer_seq,
                base,
            }
        )
    }

    pub fn claim(&self) -> ClaimedSlot<'_, T> {
        let claimed = unsafe { &*self.claim_seq }.fetch_add(1, Ordering::Relaxed);
        let index = (claimed as usize) & self.mask;

        let slot_ptr = unsafe { &mut *self.base.add(index) };
        let expected_seq = claimed;

        loop {
            let current = read_sequence_acquire(slot_ptr);
            if current == expected_seq {
                break;
            }
            std::hint::spin_loop();
        }

        ClaimedSlot {
            slot: slot_ptr,
            sequence: claimed + self.capacity as u64,
        }
    }

    pub fn try_read(&self) -> Option<ReadSlot<T>> {
        let consumer = unsafe { *self.consumer_seq };
        let index = (consumer as usize) & self.mask;
        let slot_ptr = unsafe { self.base.add(index) as *const T };

        let expected_seq = consumer + self.capacity as u64;
        let current = read_sequence_acquire(unsafe { &*slot_ptr });

        if current != expected_seq {
            return None;
        }

        Some(
            ReadSlot {
                slot_ptr,
                consumer_seq: self.consumer_seq,
                next_seq: consumer + 1,
                release_seq: consumer + self.capacity as u64,
            }
        )
    }

    pub fn claim_batch(&self, count: usize) -> ClaimedBatch<T> {
        assert!(count > 0 && count <= self.capacity, "batch count must be greater than 0 and less or equals to capacity {}", self.capacity);

        let start = unsafe { &*self.claim_seq }.fetch_add(count as u64, Ordering::Relaxed);

        let expected_seq = start + count as u64 - 1;
        let last_index = (expected_seq as usize) & self.mask;
        let last_slot = unsafe { &*self.base.add(last_index) };

        loop {
            let current = read_sequence_acquire(last_slot);
            if current == expected_seq {
                break;
            }
            std::hint::spin_loop();
        }

        ClaimedBatch {
            base: self.base,
            mask: self.mask,
            start,
            count,
            capacity: self.capacity as u64,
        }
    }

    pub fn drain_batch(&self, max_count: usize) -> DrainBatch<T> {
        let consumer = unsafe { *self.consumer_seq };

        let mut ready_count = 0;

        for i in 0..max_count {
            let seq = consumer + i as u64;
            let index = (seq as usize) & self.mask;
            let slot = unsafe { &*self.base.add(index) };
            let expected = seq + self.capacity as u64;

            let current = read_sequence_acquire(slot);
            if current != expected {
                break;
            }
            ready_count += 1;
        }

        DrainBatch {
            base: self.base,
            mask: self.mask,
            consumer_seq: self.consumer_seq,
            start: consumer,
            count: ready_count,
            capacity: self.capacity as u64,
        }
    }
}

unsafe impl<T: Slot> Send for MpscRingBuffer<T> {}
unsafe impl<T: Slot> Sync for MpscRingBuffer<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::slot::Slot;
    use std::thread;
    use std::sync::Arc;

    #[repr(C, align(64))]
    #[derive(Clone, Copy, Debug, PartialEq)]
    struct TestSlot {
        sequence: u64,
        value: u64,
        _padding: [u8; 48],
    }

    unsafe impl Slot for TestSlot {
        fn sequence(&self) -> u64 { self.sequence }
        fn set_sequence(&mut self, seq: u64) { self.sequence = seq; }
    }

    #[test]
    fn single_claim_publish_read_release() {
        let rb = MpscRingBuffer::<TestSlot>::new(16).unwrap();

        let mut claimed = rb.claim();
        claimed.as_mut().value = 42;
        claimed.publish();

        let read = rb.try_read().expect("should have data");
        assert_eq!(read.as_ref().value, 42);
        read.release();
    }

    #[test]
    fn empty_read_returns_none() {
        let rb = MpscRingBuffer::<TestSlot>::new(16).unwrap();
        assert!(rb.try_read().is_none());
    }

    #[test]
    fn multiple_sequential() {
        let rb = MpscRingBuffer::<TestSlot>::new(16).unwrap();

        for i in 0..10u64 {
            let mut claimed = rb.claim();
            claimed.as_mut().value = i * 100;
            claimed.publish();

            let read = rb.try_read().expect("should have data");
            assert_eq!(read.as_ref().value, i * 100);
            read.release();
        }
    }

    #[test]
    fn fill_and_drain() {
        let rb = MpscRingBuffer::<TestSlot>::new(16).unwrap();

        for i in 0..16u64 {
            let mut claimed = rb.claim();
            claimed.as_mut().value = i;
            claimed.publish();
        }

        for i in 0..16u64 {
            let read = rb.try_read().expect("should have data");
            assert_eq!(read.as_ref().value, i);
            read.release();
        }

        assert!(rb.try_read().is_none());
    }

    #[test]
    fn wrap_around() {
        let rb = MpscRingBuffer::<TestSlot>::new(4).unwrap();

        for i in 0..4u64 {
            let mut claimed = rb.claim();
            claimed.as_mut().value = i;
            claimed.publish();

            let read = rb.try_read().expect("should have data");
            assert_eq!(read.as_ref().value, i, "mismatch at iteration {}", i);
            read.release();
        }
    }

    #[test]
    fn batch_claim_and_drain() {
        let rb = MpscRingBuffer::<TestSlot>::new(16).unwrap();

        let mut batch = rb.claim_batch(4);
        for i in 0..batch.len() {
            batch.slot_mut(i).value = (i * 10) as u64;
        }
        batch.publish();

        let drain = rb.drain_batch(16);
        assert_eq!(drain.len(), 4);
        for i in 0..drain.len() {
            assert_eq!(drain.slot(i).value, (i * 10) as u64);
        }
        drain.release();
    }

    #[test]
    fn two_writers_all_values_delivered() {
        let rb = Arc::new(MpscRingBuffer::<TestSlot>::new(256).unwrap());
        let count_per_writer = 100u64;

        let rb1 = Arc::clone(&rb);
        let t1 = thread::spawn(move || {
            for i in 0..count_per_writer {
                let mut claimed = rb1.claim();
                claimed.as_mut().value = 1_000_000 + i;
                claimed.publish();
            }
        });

        let rb2 = Arc::clone(&rb);
        let t2 = thread::spawn(move || {
            for i in 0..count_per_writer {
                let mut claimed = rb2.claim();
                claimed.as_mut().value = 2_000_000 + i;
                claimed.publish();
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();

        let mut values: Vec<u64> = Vec::new();
        let total = count_per_writer * 2;
        for _ in 0..total {
            loop {
                if let Some(read) = rb.try_read() {
                    values.push(read.as_ref().value);
                    read.release();
                    break;
                }
                std::hint::spin_loop();
            }
        }

        assert!(rb.try_read().is_none());

        assert_eq!(values.len(), total as usize);

        let w1: Vec<u64> = values.iter().copied()
            .filter(|v| *v >= 1_000_000 && *v < 2_000_000)
            .collect();
        assert_eq!(w1.len(), count_per_writer as usize);

        let w2: Vec<u64> = values.iter().copied()
            .filter(|v| *v >= 2_000_000 && *v < 3_000_000)
            .collect();
        assert_eq!(w2.len(), count_per_writer as usize);

        for pair in w1.windows(2) {
            assert!(pair[0] < pair[1], "writer 1 order violated: {} >= {}", pair[0], pair[1]);
        }
        for pair in w2.windows(2) {
            assert!(pair[0] < pair[1], "writer 2 order violated: {} >= {}", pair[0], pair[1]);
        }
    }
}