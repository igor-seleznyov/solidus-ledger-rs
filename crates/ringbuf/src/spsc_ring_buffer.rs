use std::marker::PhantomData;
use std::slice;

use crate::arena::Arena;
use crate::sequence_mem_barrier::{load_u64_acquire, load_u64_relaxed, store_u64_release};
use crate::slot::Slot;
use crate::wait::SpinWait;

/// The ring's header: two cursors, each alone on its own cache line.
///
/// The type exists so the positions are stated once and read from the
/// declaration, rather than living as numbers inside the pointer
/// arithmetic that derives the cursors from the region's base.
///
/// The separation is not decoration: the producer writes one cursor on
/// every publication and the consumer writes the other on every release.
/// Sharing a line would make each write invalidate the other side's copy,
/// turning a local store into a coherence round trip per message.
#[repr(C)]
struct SpscRingHeader {
    /// Written by the single producer, read by the consumer.
    writer_seq: u64,
    _pad_after_writer_seq: [u8; 56],
    /// Written by the consumer, read by the producer at the gate.
    consumer_seq: u64,
    _pad_after_consumer_seq: [u8; 56],
}

const HEADER_SIZE: usize = std::mem::size_of::<SpscRingHeader>();

const _: () = assert!(std::mem::offset_of!(SpscRingHeader, writer_seq) % 64 == 0);
const _: () = assert!(std::mem::offset_of!(SpscRingHeader, consumer_seq) % 64 == 0);
const _: () = assert!(HEADER_SIZE == 128);

/// One position in a single-producer ring.
///
/// The wrapper carries cache-line alignment and nothing else. There is no
/// publication cell inside it, because a ring with exactly one producer has
/// no need of one: turns are claimed in increasing order and published in
/// that same order, so "how far has the producer got" is a single number
/// rather than a per-position mark. The multi-producer ring cannot say that
/// — several producers finish out of order, and no single number can
/// express which positions are filled — which is why it keeps a mark in
/// every slot and this one does not.
///
/// The alignment lives here rather than on the payload type so payload
/// types stay usable by both rings: the multi-producer ring gets its
/// alignment from its own container.
///
/// **For defence:** the consequence worth naming is that nothing inside a
/// slot is read by the other thread while this one writes it. That is what
/// lets a descriptor hold an ordinary exclusive borrow of a whole run of
/// slots — an exclusive borrow asserts that nobody else touches those
/// bytes, and here nobody does. In the multi-producer ring the same borrow
/// would be a lie, because the consumer polls the publication cell that
/// sits inside every slot.
#[repr(C, align(64))]
pub struct SpscSlot<T: Slot> {
    pub payload: T,
}

/// A ring with exactly one producer thread and one consumer thread.
///
/// Readiness is the difference between two cursors. The producer moves the
/// write cursor after filling slots; the consumer moves the read cursor
/// after emptying them. Neither ever inspects a slot to decide whether it
/// may proceed.
///
/// The two cursors live on separate cache lines at the head of the Arena,
/// so the producer's store to one does not evict the consumer's line.
///
/// Capacity is fixed when the ring is built and never changes. A full ring
/// therefore means the consumer is behind, and the producer waits — it
/// never grows the region and never drops a turn. Growing would mean
/// relocating memory while both threads hold live cursors into it, and it
/// would convert a load signal into an allocation, hiding exactly the
/// condition an operator needs to see.
pub struct SpscRingBuffer<T: Slot> {
    #[allow(dead_code)]
    arena: Arena,
    capacity: usize,
    mask: usize,
    writer_seq: *mut u64,
    consumer_seq: *mut u64,
    base: *mut SpscSlot<T>,
}

unsafe impl<T: Slot> Send for SpscRingBuffer<T> {}

unsafe impl<T: Slot> Sync for SpscRingBuffer<T> {}

/// A producer's hold on one claimed turn.
pub struct SpscClaimedSlot<'a, T: Slot> {
    payload: &'a mut T,
    writer_seq: *mut u64,
    next_seq: u64,

    /// Zero-sized, occupies no memory, emits no instruction. Naming a raw
    /// pointer type removes the automatically-derived ability to move the
    /// value between threads. Nothing in the memory ordering requires
    /// that, but a claim which travels can be lost, and a lost claim wedges
    /// the ring: the write cursor never advances past it and the consumer
    /// waits forever.
    _thread_bound: PhantomData<*const ()>,
}

/// A consumer's hold on one filled turn.
pub struct SpscReadSlot<'a, T: Slot> {
    payload: &'a T,
    consumer_seq: *mut u64,
    next_seq: u64,
}

/// A producer's hold on a run of claimed turns.
///
/// The run is held as one or two ordinary exclusive slices — one when it
/// lies inside the array, two when it reaches the end and continues at the
/// beginning. Every accessor below is ordinary safe code; the only
/// `unsafe` in the whole path is the single place inside the ring where
/// these slices are formed.
pub struct SpscClaimedBatch<'a, T: Slot> {
    head: &'a mut [SpscSlot<T>],
    tail: &'a mut [SpscSlot<T>],
    writer_seq: *mut u64,
    next_seq: u64,
    _thread_bound: PhantomData<*const ()>,
}

/// A consumer's hold on a run of filled turns.
pub struct SpscDrainBatch<'a, T: Slot> {
    head: &'a [SpscSlot<T>],
    tail: &'a [SpscSlot<T>],
    consumer_seq: *mut u64,
    next_seq: u64,
}

impl<T: Slot> SpscRingBuffer<T> {
    pub fn new(capacity: usize) -> std::io::Result<Self> {
        assert!(capacity.is_power_of_two(), "capacity must be a power of two");

        let total_size = HEADER_SIZE + capacity * size_of::<SpscSlot<T>>();
        let arena = Arena::new(total_size)?;
        let ptr = arena.as_ptr();

        let writer_seq = unsafe {
            ptr.add(std::mem::offset_of!(SpscRingHeader, writer_seq)) as *mut u64
        };
        let consumer_seq = unsafe {
            ptr.add(std::mem::offset_of!(SpscRingHeader, consumer_seq)) as *mut u64
        };
        let base = unsafe { ptr.add(HEADER_SIZE) as *mut SpscSlot<T> };

        debug_assert!((writer_seq as usize).is_multiple_of(align_of::<u64>()));
        debug_assert!((consumer_seq as usize).is_multiple_of(align_of::<u64>()));
        debug_assert!((base as usize).is_multiple_of(align_of::<SpscSlot<T>>()));

        unsafe {
            *writer_seq = 0;
            *consumer_seq = 0;
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

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Reads the producer's own cursor.
    ///
    /// A plain relaxed load is right here: the producer is the only writer
    /// of this cell, so it is reading back its own last value and needs no
    /// ordering against anyone.
    #[inline(always)]
    fn writer_position(&self) -> u64 {
        unsafe { load_u64_relaxed(self.writer_seq) }
    }

    /// Reads the consumer's cursor from the producer's side.
    ///
    /// The acquire ordering is what makes it safe to re-enter a slot the
    /// consumer has released: observing the advanced cursor guarantees the
    /// consumer's reads of those payload bytes are complete.
    #[inline(always)]
    fn consumer_position_acquire(&self) -> u64 {
        unsafe { load_u64_acquire(self.consumer_seq) }
    }

    /// Waits until `count` turns starting at `start` all fit inside the
    /// ring, then returns.
    ///
    /// The wait has no bound and cannot fail, because the only producer of
    /// this ring is an internal stage holding an item the system already
    /// owns — a record the recovery path will expect to find. Declining to
    /// place it would lose it. The waiting is itself how back-pressure
    /// travels: a producer parked here is a thread that has stopped
    /// draining the ring feeding it, and the same stall reaches the ingress
    /// one stage at a time, where it does turn into an answer to the
    /// client.
    ///
    /// The escalation matters even so. A consumer that is merely part-way
    /// through a batch will free room within a few hundred nanoseconds and
    /// the pause instructions cover it without ever leaving the core; a
    /// consumer that has genuinely stalled would otherwise hold this core
    /// at full rate indefinitely, which is the case the yielding steps
    /// exist for.
    #[inline(always)]
    fn await_room(&self, start: u64, count: usize) {
        let capacity = self.capacity as u64;
        let mut spin_wait = SpinWait::new();

        while start + count as u64 > self.consumer_position_acquire() + capacity {
            spin_wait.wait();
        }
    }

    /// Splits a run of turns into the at most two contiguous regions it
    /// occupies. A run that reaches the end of the array continues at the
    /// beginning, and those are two separate pieces of memory.
    #[inline(always)]
    fn split_run(&self, start: u64, count: usize) -> (usize, usize, usize) {
        let start_index = (start as usize) & self.mask;
        let head_length = count.min(self.capacity - start_index);
        (start_index, head_length, count - head_length)
    }

    pub fn claim(&self) -> SpscClaimedSlot<'_, T> {
        let turn = self.writer_position();
        self.await_room(turn, 1);

        let index = (turn as usize) & self.mask;
        let payload = unsafe {
            &mut (*self.base.add(index)).payload
        };

        SpscClaimedSlot {
            payload,
            writer_seq: self.writer_seq,
            next_seq: turn + 1,
            _thread_bound: PhantomData,
        }
    }

    pub fn claim_batch(&self, count: usize) -> SpscClaimedBatch<'_, T> {
        assert!(count > 0 && count <= self.capacity);

        let start = self.writer_position();
        self.await_room(start, count);

        let (start_index, head_length, tail_length) = self.split_run(start, count);
        let (head, tail) = unsafe {
            (
                slice::from_raw_parts_mut(self.base.add(start_index), head_length),
                slice::from_raw_parts_mut(self.base, tail_length),
            )
        };

        SpscClaimedBatch {
            head,
            tail,
            writer_seq: self.writer_seq,
            next_seq: start + count as u64,
            _thread_bound: PhantomData,
        }
    }

    /// Reads the producer's cursor from the consumer's side.
    ///
    /// The acquire ordering is the other half of the pair: observing an
    /// advanced write cursor guarantees every payload byte the producer
    /// wrote before moving it is visible here.
    #[inline(always)]
    fn writer_position_acquire(&self) -> u64 {
        unsafe { load_u64_acquire(self.writer_seq) }
    }

    #[inline(always)]
    fn consumer_position(&self) -> u64 {
        unsafe { load_u64_relaxed(self.consumer_seq) }
    }

    pub fn try_read(&self) -> Option<SpscReadSlot<'_, T>> {
        let turn = self.consumer_position();
        if turn == self.writer_position_acquire() {
            return None;
        }

        let index = (turn as usize) & self.mask;
        let payload = unsafe {
            &(*self.base.add(index)).payload
        };

        Some(
            SpscReadSlot {
                payload,
                consumer_seq: self.consumer_seq,
                next_seq: turn + 1,
            }
        )
    }

    pub fn drain_batch(&self, max_count: usize) -> SpscDrainBatch<'_, T> {
        let start = self.consumer_position();
        let available = (self.writer_position_acquire() - start) as usize;
        let count = available.min(max_count);

        let (start_index, head_length, tail_length) = self.split_run(start, count);
        let (head, tail) = unsafe {
            (
                slice::from_raw_parts(self.base.add(start_index), head_length),
                slice::from_raw_parts(self.base, tail_length),
            )
        };

        SpscDrainBatch {
            head,
            tail,
            consumer_seq: self.consumer_seq,
            next_seq: start + count as u64,
        }
    }
}

impl<T: Slot> SpscClaimedSlot<'_, T> {
    pub fn as_mut(&mut self) -> &mut T {
        self.payload
    }

    /// Hands the turn to the consumer by advancing the write cursor.
    ///
    /// The release ordering is what makes the payload visible: a consumer
    /// that observes this cursor value is guaranteed to see every byte
    /// written before the store.
    pub fn publish(self) {
        unsafe {
            store_u64_release(self.writer_seq, self.next_seq);
        }
    }
}

impl<T: Slot> SpscReadSlot<'_, T> {
    pub fn as_ref(&self) -> &T {
        self.payload
    }

    /// Returns the turn to the producer by advancing the read cursor.
    ///
    /// The release ordering keeps this consumer's reads of the payload
    /// ahead of the store, so a producer that observes the advanced cursor
    /// and re-enters the slot cannot overwrite bytes still being read.
    pub fn release(self) {
        unsafe {
            store_u64_release(self.consumer_seq, self.next_seq);
        }
    }
}

impl<T: Slot> SpscClaimedBatch<'_, T> {
    pub fn len(&self) -> usize {
        self.head.len() + self.tail.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn slot_mut(&mut self, index: usize) -> &mut T {
        let head_length = self.head.len();
        if index < head_length {
            &mut self.head[index].payload
        } else {
            &mut self.tail[index - head_length].payload
        }
    }

    /// Hands the whole run to the consumer with a single cursor store.
    ///
    /// One store for the entire batch, whatever its size — there are no
    /// per-slot marks to write, so nothing here scales with the number of
    /// turns.
    pub fn publish(self) {
        unsafe {
            store_u64_release(self.writer_seq, self.next_seq);
        }
    }
}

impl<T: Slot> SpscDrainBatch<'_, T> {
    pub fn len(&self) -> usize {
        self.head.len() + self.tail.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn slot(&self, index: usize) -> &T {
        let head_length = self.head.len();
        if index < head_length {
            &self.head[index].payload
        } else {
            &self.tail[index - head_length].payload
        }
    }

    /// Returns the whole run to the producer with a single cursor store.
    pub fn release(self) {
        unsafe {
            store_u64_release(self.consumer_seq, self.next_seq);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// How many fruitless reads a consumer tolerates before it declares the
    /// producer gone. Generous enough that a merely slow producer never
    /// trips it, small enough that a broken one fails in seconds.
    #[cfg(not(miri))]
    const IDLE_READ_CEILING: u64 = 50_000_000;

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct TestPayload {
        value: u64,
    }

    impl Slot for TestPayload {}

    fn ring(capacity: usize) -> SpscRingBuffer<TestPayload> {
        SpscRingBuffer::<TestPayload>::new(capacity).expect("ring")
    }

    #[test]
    fn single_claim_publish_read_release() {
        let ring = ring(16);

        let mut claimed = ring.claim();
        claimed.as_mut().value = 42;
        claimed.publish();

        let read = ring.try_read().expect("published turn must be readable");
        assert_eq!(read.as_ref().value, 42);
        read.release();

        assert!(ring.try_read().is_none(), "the ring is empty after release");
    }

    #[test]
    fn empty_ring_reads_nothing() {
        let ring = ring(16);
        assert!(ring.try_read().is_none());
        assert!(ring.drain_batch(8).is_empty());
    }

    #[test]
    fn batch_round_trip_preserves_order() {
        let ring = ring(16);

        let mut claimed = ring.claim_batch(4);
        for index in 0..claimed.len() {
            claimed.slot_mut(index).value = 100 + index as u64;
        }
        claimed.publish();

        let drained = ring.drain_batch(8);
        assert_eq!(drained.len(), 4);
        for index in 0..drained.len() {
            assert_eq!(drained.slot(index).value, 100 + index as u64);
        }
        drained.release();
    }

    #[test]
    fn drain_takes_no_more_than_asked() {
        let ring = ring(16);

        let mut claimed = ring.claim_batch(6);
        for index in 0..claimed.len() {
            claimed.slot_mut(index).value = index as u64;
        }
        claimed.publish();

        let drained = ring.drain_batch(2);
        assert_eq!(drained.len(), 2);
        drained.release();

        let rest = ring.drain_batch(16);
        assert_eq!(rest.len(), 4, "the untaken turns stay available");
        assert_eq!(rest.slot(0).value, 2);
        rest.release();
    }

    #[test]
    fn batch_wraps_around_the_end_of_the_array() {
        let ring = ring(8);

        for value in 0..6u64 {
            let mut claimed = ring.claim();
            claimed.as_mut().value = value;
            claimed.publish();
            let read = ring.try_read().expect("turn must be readable");
            assert_eq!(read.as_ref().value, value);
            read.release();
        }

        let mut claimed = ring.claim_batch(4);
        assert_eq!(claimed.len(), 4);
        for index in 0..claimed.len() {
            claimed.slot_mut(index).value = 200 + index as u64;
        }
        claimed.publish();

        let drained = ring.drain_batch(8);
        assert_eq!(drained.len(), 4);
        for index in 0..drained.len() {
            assert_eq!(
                drained.slot(index).value,
                200 + index as u64,
                "a run split across the end of the array reads back in order",
            );
        }
        drained.release();
    }

    #[test]
    fn wrap_around_single_slots_preserve_values() {
        let ring = ring(4);

        for value in 0..12u64 {
            let mut claimed = ring.claim();
            claimed.as_mut().value = value;
            claimed.publish();

            let read = ring.try_read().expect("turn must be readable");
            assert_eq!(read.as_ref().value, value);
            read.release();
        }
    }

    /// Producer and consumer on separate threads over the real ring.
    ///
    /// The capacity equals the item count so neither thread ever waits on
    /// the other for room — a producer spinning on a full ring costs hours
    /// under the interpreter this test is meant to run in.
    #[test]
    fn miri_spsc_threaded_single_slots() {
        use std::thread;

        const ITEM_COUNT: u64 = 8;

        let ring = ring(ITEM_COUNT as usize);

        thread::scope(|scope| {
            scope.spawn(|| {
                for value in 0..ITEM_COUNT {
                    let mut claimed = ring.claim();
                    claimed.as_mut().value = value;
                    claimed.publish();
                }
            });

            let received = scope.spawn(|| {
                let mut received: Vec<u64> = Vec::with_capacity(ITEM_COUNT as usize);
                while (received.len() as u64) < ITEM_COUNT {
                    match ring.try_read() {
                        Some(read) => {
                            received.push(read.as_ref().value);
                            read.release();
                        }
                        None => std::hint::spin_loop(),
                    }
                }
                received
            });

            let received = received.join().expect("consumer thread panicked");
            let expected: Vec<u64> = (0..ITEM_COUNT).collect();
            assert_eq!(received, expected, "a single-producer ring preserves order");
        });
    }

    /// The same across the batch path, and with a partial drain — the
    /// consumer asks for fewer turns than the producer published, which is
    /// the shape that has to hold without any per-slot mark to consult.
    #[test]
    fn miri_spsc_threaded_partial_batch_drain() {
        use std::thread;

        const ITEM_COUNT: u64 = 8;

        let ring = ring(ITEM_COUNT as usize);

        thread::scope(|scope| {
            scope.spawn(|| {
                let mut claimed = ring.claim_batch(ITEM_COUNT as usize);
                for index in 0..claimed.len() {
                    claimed.slot_mut(index).value = 500 + index as u64;
                }
                claimed.publish();
            });

            let received = scope.spawn(|| {
                let mut received: Vec<u64> = Vec::with_capacity(ITEM_COUNT as usize);
                while (received.len() as u64) < ITEM_COUNT {
                    let drained = ring.drain_batch(1);
                    if drained.is_empty() {
                        std::hint::spin_loop();
                        continue;
                    }
                    for index in 0..drained.len() {
                        received.push(drained.slot(index).value);
                    }
                    drained.release();
                }
                received
            });

            let received = received.join().expect("consumer thread panicked");
            let expected: Vec<u64> = (0..ITEM_COUNT).map(|index| 500 + index).collect();
            assert_eq!(received, expected);
        });
    }

    /// The producer is forced to wait, and only the consumer's release lets
    /// it finish.
    ///
    /// Every other threaded test in this file is deliberately sized so that
    /// nobody ever waits, because a producer spinning on a full ring costs
    /// hours under the interpreter. This one is the opposite case on
    /// purpose: four turns of room and sixty-four values to push through
    /// them, so the producer meets a full ring again and again and the run
    /// completes only if each wait ends when the consumer gives a turn
    /// back. Without it the waiting path — the whole point of the gate —
    /// has no coverage at all, so the test is excluded from the interpreter
    /// rather than shrunk until the waiting disappears.
    #[test]
    #[cfg(not(miri))]
    fn producer_waits_for_room_and_resumes() {
        use std::thread;

        const CAPACITY: usize = 4;
        const ITEM_COUNT: u64 = 64;

        let ring = ring(CAPACITY);

        thread::scope(|scope| {
            scope.spawn(|| {
                for value in 0..ITEM_COUNT {
                    let mut claimed = ring.claim();
                    claimed.as_mut().value = value;
                    claimed.publish();
                }
            });

            let received = scope.spawn(|| {
                let mut received: Vec<u64> = Vec::with_capacity(ITEM_COUNT as usize);
                let mut empty_reads = 0u64;
                while (received.len() as u64) < ITEM_COUNT {
                    match ring.try_read() {
                        Some(read) => {
                            received.push(read.as_ref().value);
                            read.release();
                            empty_reads = 0;
                        }
                        None => {
                            empty_reads += 1;
                            assert!(
                                empty_reads < IDLE_READ_CEILING,
                                "the producer stopped delivering after {} values",
                                received.len(),
                            );
                            std::hint::spin_loop();
                        }
                    }
                }
                received
            });

            let received = received.join().expect("consumer thread panicked");
            let expected: Vec<u64> = (0..ITEM_COUNT).collect();
            assert_eq!(received, expected, "no turn is lost or reordered by the wait");
        });
    }

    /// The same for the batch path, where a run can also straddle the end of
    /// the array while the producer is waiting for its far end to free up.
    #[test]
    #[cfg(not(miri))]
    fn batch_producer_waits_for_room_and_resumes() {
        use std::thread;

        const CAPACITY: usize = 4;
        const BATCH: usize = 3;
        const BATCH_COUNT: u64 = 20;

        let ring = ring(CAPACITY);

        thread::scope(|scope| {
            scope.spawn(|| {
                for batch_index in 0..BATCH_COUNT {
                    let mut claimed = ring.claim_batch(BATCH);
                    for index in 0..claimed.len() {
                        claimed.slot_mut(index).value = batch_index * BATCH as u64 + index as u64;
                    }
                    claimed.publish();
                }
            });

            let received = scope.spawn(|| {
                let total = BATCH_COUNT * BATCH as u64;
                let mut received: Vec<u64> = Vec::with_capacity(total as usize);
                let mut empty_drains = 0u64;
                while (received.len() as u64) < total {
                    let drained = ring.drain_batch(CAPACITY);
                    if drained.is_empty() {
                        empty_drains += 1;
                        assert!(
                            empty_drains < IDLE_READ_CEILING,
                            "the producer stopped delivering after {} values",
                            received.len(),
                        );
                        std::hint::spin_loop();
                        continue;
                    }
                    empty_drains = 0;
                    for index in 0..drained.len() {
                        received.push(drained.slot(index).value);
                    }
                    drained.release();
                }
                received
            });

            let received = received.join().expect("consumer thread panicked");
            let expected: Vec<u64> = (0..BATCH_COUNT * BATCH as u64).collect();
            assert_eq!(received, expected, "a run split by the wait still reads back in order");
        });
    }
}
