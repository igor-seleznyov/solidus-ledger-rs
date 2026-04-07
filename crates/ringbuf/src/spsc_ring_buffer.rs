use crate::arena::Arena;
use crate::slot::Slot;
use crate::slot::ClaimedSlot;
use crate::batch::ClaimedBatch;
use crate::batch::DrainBatch;
use crate::slot::ReadSlot;
use crate::sequence_mem_barrier::read_sequence_acquire;

const HEADER_SIZE: usize = 128;

pub struct SpscRingBuffer<T: Slot> {
    #[allow(dead_code)]
    arena: Arena,
    capacity: usize,
    mask: usize,
    writer_seq: *mut u64,
    consumer_seq: *mut u64,
    base: *mut T,
}

impl<T: Slot> SpscRingBuffer<T> {
    pub fn new(capacity: usize) -> std::io::Result<Self> {
        assert!(capacity.is_power_of_two(), "capacity must be a power of two");
        assert!(
            size_of::<T>() >= 64,
            "slot size must be at least 64 bytes (cache line)"
        );
        assert_eq!(align_of::<T>(), 64, "slot alignment must be 64 bytes (cache line)");

        let slot_size = size_of::<T>();
        let total_size = HEADER_SIZE + capacity * slot_size;
        let arena = Arena::new(total_size)?;
        let ptr = arena.as_ptr();

        let writer_seq = ptr as *mut u64;
        let consumer_seq = unsafe { ptr.add(64) as *mut u64 };
        let base = unsafe { ptr.add(HEADER_SIZE) as *mut T };

        unsafe {
            *writer_seq = 0;
            *consumer_seq = 0;
        }

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
                writer_seq,
                consumer_seq,
                base,
            }
        )
    }

    pub fn claim(&self) -> ClaimedSlot<'_, T> {
        let seq = unsafe { *self.writer_seq };
        let index = (seq as usize) & self.mask;
        let slot_ptr = unsafe { &mut *self.base.add(index) };

        let expected_seq = seq;
        loop {
            let current = read_sequence_acquire(slot_ptr);
            if current == expected_seq {
                break;
            }
            std::hint::spin_loop();
        }

        unsafe { *self.writer_seq = seq + 1 }

        ClaimedSlot {
            slot: slot_ptr,
            sequence: seq + self.capacity as u64,
        }
    }

    pub fn try_read(&self) -> Option<ReadSlot<T>> {
        let consumer = unsafe { *self.consumer_seq };
        let index = (consumer as usize) & self.mask;
        let slot_ptr = unsafe { &*self.base.add(index) };

        let expected_seq = consumer + self.capacity as u64;
        let current = read_sequence_acquire(slot_ptr);

        if current != expected_seq {
            return None;
        }

        Some(
            ReadSlot {
                slot_ptr: slot_ptr as *const T,
                consumer_seq: self.consumer_seq,
                next_seq: consumer + 1,
                release_seq: consumer + self.capacity as u64,
            }
        )
    }

    pub fn claim_batch(&self, count: usize) -> ClaimedBatch<T> {
        assert!(count > 0 && count <= self.capacity);

        let start = unsafe { *self.writer_seq };

        let last_index = ((start + count as u64 - 1) as usize) & self.mask;
        let last_slot = unsafe { &*self.base.add(last_index) };
        let expected_seq = start + count as u64 - 1;

        loop {
            let current = read_sequence_acquire(last_slot);
            if current == expected_seq {
                break;
            }
            std::hint::spin_loop();
        }

        unsafe { *self.writer_seq = start + count as u64; }

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

        let first_index = (consumer as usize) & self.mask;
        let first_slot = unsafe { &*self.base.add(first_index) };
        let first_expected = consumer + self.capacity as u64;

        if read_sequence_acquire(first_slot) != first_expected {
            return DrainBatch {
                base: self.base,
                mask: self.mask,
                consumer_seq: self.consumer_seq,
                start: consumer,
                count: 0,
                capacity: self.capacity as u64,
            };
        }

        let mut ready_count = 1;
        if max_count > 1 {
            let check_end = max_count.min(self.capacity);
            let last_seq = consumer + check_end as u64 - 1;
            let last_index = (last_seq as usize) & self.mask;
            let last_slot = unsafe { &*self.base.add(last_index) };
            let last_expected = last_seq + self.capacity as u64;

            if read_sequence_acquire(last_slot) == last_expected {
                ready_count = check_end;
            } else {
                let mut lo: usize = 1;
                let mut hi: usize = check_end - 1;

                while lo < hi {
                    let mid = lo + ((hi - lo + 1) >> 1);
                    let mid_seq = consumer + mid as u64;
                    let mid_index = (mid_seq as usize) & self.mask;
                    let mid_slot = unsafe { &*self.base.add(mid_index) };
                    let mid_expected = mid_seq + self.capacity as u64;

                    if read_sequence_acquire(mid_slot) == mid_expected {
                        lo = mid;
                    } else {
                        hi = mid - 1;
                    }
                }

                let lo_seq = consumer + lo as u64;
                let lo_index = (lo_seq as usize) & self.mask;
                let lo_slot = unsafe { &*self.base.add(lo_index) };
                let lo_expected = lo_seq + self.capacity as u64;

                if read_sequence_acquire(lo_slot) == lo_expected {
                    ready_count = lo + 1;
                }
            }
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

unsafe impl<T: Slot> Send for SpscRingBuffer<T> {}
unsafe impl<T: Slot> Sync for SpscRingBuffer<T> {}

#[cfg(test)]
#[cfg(not(miri))]
mod tests {
    use super::*;
    use crate::slot::Slot;

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

    impl TestSlot {
        fn new(value: u64) -> Self {
            Self { sequence: 0, value, _padding: [0u8; 48] }
        }
    }

    #[test]
    fn single_claim_publish_read_release() {
        let rb = SpscRingBuffer::<TestSlot>::new(16).unwrap();

        let mut claimed = rb.claim();
        claimed.as_mut().value = 42;
        claimed.publish();

        let read = rb.try_read().expect("should have data");
        assert_eq!(read.as_ref().value, 42);
        read.release();
    }

    #[test]
    fn empty_read_returns_none() {
        let rb = SpscRingBuffer::<TestSlot>::new(16).unwrap();
        assert!(rb.try_read().is_none());
    }

    #[test]
    fn multiple_sequential() {
        let rb = SpscRingBuffer::<TestSlot>::new(16).unwrap();

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
        let rb = SpscRingBuffer::<TestSlot>::new(16).unwrap();

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
        let rb = SpscRingBuffer::<TestSlot>::new(4).unwrap();

        for i in 0..20u64 {
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
        let rb = SpscRingBuffer::<TestSlot>::new(16).unwrap();

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

        let drain = rb.drain_batch(16);
        assert!(drain.is_empty());
    }

    #[test]
    fn batch_wrap_around() {
        let rb = SpscRingBuffer::<TestSlot>::new(4).unwrap();

        for i in 0..3u64 {
            let mut claimed = rb.claim();
            claimed.as_mut().value = i;
            claimed.publish();
        }
        let drain = rb.drain_batch(4);
        assert_eq!(drain.len(), 3);
        drain.release();

        let mut batch = rb.claim_batch(4);
        for i in 0..4 {
            batch.slot_mut(i).value = (i + 100) as u64;
        }
        batch.publish();

        let drain = rb.drain_batch(4);
        assert_eq!(drain.len(), 4);
        for i in 0..4 {
            assert_eq!(drain.slot(i).value, (i + 100) as u64);
        }
        drain.release();
    }

    #[test]
    fn drain_partial_ready() {
        let rb = SpscRingBuffer::<TestSlot>::new(16).unwrap();

        for i in 0..5u64 {
            let mut claimed = rb.claim();
            claimed.as_mut().value = i;
            claimed.publish();
        }

        let drain = rb.drain_batch(16);
        assert_eq!(drain.len(), 5);
        drain.release();
    }
}

#[cfg(test)]
mod miri_tests {
    use std::cell::UnsafeCell;
    use crate::slot::Slot;
    use crate::slot::ClaimedSlot;
    use crate::slot::ReadSlot;
    use crate::batch::ClaimedBatch;
    use crate::batch::DrainBatch;
    use crate::sequence_mem_barrier::read_sequence_acquire;

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

    struct MiriSpsc {
        writer_seq: Box<UnsafeCell<u64>>,
        consumer_seq: Box<UnsafeCell<u64>>,
        slots: Vec<TestSlot>,
        capacity: usize,
        mask: usize,
    }

    impl MiriSpsc {
        fn new(capacity: usize) -> Self {
            assert!(capacity.is_power_of_two());

            let writer_seq = Box::new(UnsafeCell::new(0u64));
            let consumer_seq = Box::new(UnsafeCell::new(0u64));

            let mut slots = vec![TestSlot { sequence: 0, value: 0, _padding: [0; 48] }; capacity];
            for i in 0..capacity {
                slots[i].set_sequence(i as u64);
            }

            Self {
                writer_seq, consumer_seq, slots,
                capacity, mask: capacity - 1,
            }
        }

        fn writer_seq_ptr(&self) -> *mut u64 {
            self.writer_seq.get()
        }

        fn consumer_seq_ptr(&self) -> *mut u64 {
            self.consumer_seq.get()
        }

        fn claim(&self) -> ClaimedSlot<'_, TestSlot> {
            let seq = unsafe { *self.writer_seq_ptr() };
            let index = (seq as usize) & self.mask;
            let base = self.slots.as_ptr() as *mut TestSlot;
            let slot_ptr = unsafe { &mut *base.add(index) };

            let expected_seq = seq;
            loop {
                let current = read_sequence_acquire(slot_ptr);
                if current == expected_seq {
                    break;
                }
                std::hint::spin_loop();
            }

            unsafe { *self.writer_seq_ptr() = seq + 1; }

            ClaimedSlot {
                slot: slot_ptr,
                sequence: seq + self.capacity as u64,
            }
        }

        fn try_read(&self) -> Option<ReadSlot<TestSlot>> {
            let consumer = unsafe { *self.consumer_seq_ptr() };
            let index = (consumer as usize) & self.mask;
            let base = self.slots.as_ptr() as *mut TestSlot;
            let slot_ptr = unsafe { base.add(index) };

            let expected_seq = consumer + self.capacity as u64;
            let current = read_sequence_acquire(unsafe { &*slot_ptr });

            if current != expected_seq {
                return None;
            }

            Some(ReadSlot {
                slot_ptr: slot_ptr as *const TestSlot,
                consumer_seq: self.consumer_seq_ptr(),
                next_seq: consumer + 1,
                release_seq: consumer + self.capacity as u64,
            })
        }

        fn claim_batch(&self, count: usize) -> ClaimedBatch<TestSlot> {
            let start = unsafe { *self.writer_seq_ptr() };
            let base = self.slots.as_ptr() as *mut TestSlot;

            let last_index = ((start + count as u64 - 1) as usize) & self.mask;
            let last_slot = unsafe { &*base.add(last_index) };
            let expected_seq = start + count as u64 - 1;

            loop {
                let current = read_sequence_acquire(last_slot);
                if current == expected_seq {
                    break;
                }
                std::hint::spin_loop();
            }

            unsafe { *self.writer_seq_ptr() = start + count as u64; }

            ClaimedBatch {
                base,
                mask: self.mask,
                start,
                count,
                capacity: self.capacity as u64,
            }
        }

        fn drain_batch(&self, max_count: usize) -> DrainBatch<TestSlot> {
            let consumer = unsafe { *self.consumer_seq_ptr() };
            let base = self.slots.as_ptr() as *mut TestSlot;

            let mut ready_count = 0;
            for i in 0..max_count {
                let seq = consumer + i as u64;
                let index = (seq as usize) & self.mask;
                let slot = unsafe { &*base.add(index) };
                let expected = seq + self.capacity as u64;

                if read_sequence_acquire(slot) != expected {
                    break;
                }
                ready_count += 1;
            }

            DrainBatch {
                base,
                mask: self.mask,
                consumer_seq: self.consumer_seq_ptr(),
                start: consumer,
                count: ready_count,
                capacity: self.capacity as u64,
            }
        }
    }

    #[test]
    fn miri_spsc_single_claim_publish_read() {
        let rb = MiriSpsc::new(16);

        let mut claimed = rb.claim();
        claimed.as_mut().value = 42;
        claimed.publish();

        let read = rb.try_read().expect("should have data");
        assert_eq!(read.as_ref().value, 42);
        read.release();
    }

    #[test]
    fn miri_spsc_empty_read() {
        let rb = MiriSpsc::new(16);
        assert!(rb.try_read().is_none());
    }

    #[test]
    fn miri_spsc_multiple_sequential() {
        let rb = MiriSpsc::new(16);

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
    fn miri_spsc_fill_and_drain() {
        let rb = MiriSpsc::new(16);

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
    fn miri_spsc_wrap_around() {
        let rb = MiriSpsc::new(4);

        for i in 0..20u64 {
            let mut claimed = rb.claim();
            claimed.as_mut().value = i;
            claimed.publish();

            let read = rb.try_read().expect("should have data");
            assert_eq!(read.as_ref().value, i);
            read.release();
        }
    }

    #[test]
    fn miri_spsc_batch_claim_and_drain() {
        let rb = MiriSpsc::new(16);

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
    fn miri_spsc_batch_wrap_around() {
        let rb = MiriSpsc::new(4);

        for i in 0..3u64 {
            let mut claimed = rb.claim();
            claimed.as_mut().value = i;
            claimed.publish();
        }
        let drain = rb.drain_batch(4);
        assert_eq!(drain.len(), 3);
        drain.release();

        let mut batch = rb.claim_batch(4);
        for i in 0..4 {
            batch.slot_mut(i).value = (i + 100) as u64;
        }
        batch.publish();

        let drain = rb.drain_batch(4);
        assert_eq!(drain.len(), 4);
        for i in 0..4 {
            assert_eq!(drain.slot(i).value, (i + 100) as u64);
        }
        drain.release();
    }

    #[test]
    fn miri_spsc_release_resets_sequence() {
        let rb = MiriSpsc::new(4);

        let mut claimed = rb.claim();
        claimed.as_mut().value = 999;
        claimed.publish();

        let read = rb.try_read().expect("data");
        assert_eq!(read.as_ref().value, 999);
        read.release();

        let mut claimed2 = rb.claim();
        claimed2.as_mut().value = 888;
        claimed2.publish();

        let read2 = rb.try_read().expect("data");
        assert_eq!(read2.as_ref().value, 888);
        read2.release();
    }
}