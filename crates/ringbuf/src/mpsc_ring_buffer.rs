use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::arena::Arena;
use crate::wait::{BoundedSpinWait, SpinWait};
use crate::slot::{load_sequence_acquire, ClaimedSlot, RbSlot, ReadSlot, Slot};
use crate::batch::{ClaimedBatch, DrainBatch};
use crate::sequence_mem_barrier::{load_u64_acquire, load_u64_relaxed};

/// The ring's header: three counters, each alone on its own cache line.
///
/// The type exists so the positions are stated once and read from the
/// declaration. They used to be written as numbers at the three places
/// that derive pointers from the region's base, which meant the layout
/// lived in the arithmetic rather than anywhere a reader could see it.
///
/// The separation is not decoration. Producers increment the ticket
/// counter constantly; the consumer writes its cursor just as often. Two
/// counters on one line would make every producer's increment invalidate
/// the line the consumer is writing and the other way round, turning what
/// should be a local write into a coherence round trip on every message.
#[repr(C)]
struct MpscRingHeader {
    /// Handed out by `fetch_add`; every producer writes it.
    claim_seq: AtomicU64,
    _pad_after_claim_seq: [u8; 56],
    /// Written by the consumer alone, read by every producer at the gate.
    consumer_seq: u64,
    _pad_after_consumer_seq: [u8; 56],
    /// The most recent consumer position a producer has observed, so the
    /// gate can usually answer without touching the consumer's own line.
    cached_consumer_seq: AtomicU64,
    _pad_after_cached_consumer_seq: [u8; 56],
}

const HEADER_SIZE: usize = std::mem::size_of::<MpscRingHeader>();

const _: () = assert!(std::mem::offset_of!(MpscRingHeader, claim_seq) % 64 == 0);
const _: () = assert!(std::mem::offset_of!(MpscRingHeader, consumer_seq) % 64 == 0);
const _: () = assert!(std::mem::offset_of!(MpscRingHeader, cached_consumer_seq) % 64 == 0);
const _: () = assert!(HEADER_SIZE == 192);

/// A ring shared by several producer threads and one consumer thread.
///
/// Several producers finish writing out of order — the one that took the
/// later turn may publish first — so no single counter can say how far the
/// filled region reaches. That is why every slot carries its own
/// publication cell and the consumer has to look at them. The
/// single-producer ring next to this one needs none of that: with one
/// writer the publication order is the claim order, so its readiness
/// question is the difference between two counters.
pub struct MpscRingBuffer<T: Slot> {
    #[allow(dead_code)]
    arena: Arena,
    capacity: usize,
    mask: usize,

    /// Hands out turns to producers. This one is genuinely atomic: several
    /// threads increment it concurrently, and the increment is what decides
    /// which of them owns which turn.
    claim_seq: *const AtomicU64,

    consumer_seq: *mut u64,

    /// The most recent consumer position any producer has observed.
    ///
    /// The admission gate needs to know how far the consumer has got, and
    /// reading its cursor directly is expensive for a reason that has
    /// nothing to do with contention between producers: that line is
    /// written by the consumer continuously, so every read of it by a
    /// producer is a coherence miss. This copy is written only when a
    /// producer actually had to look, so on a ring that is not full it is
    /// read-shared by every producer and never written at all.
    ///
    /// It lives on its own cache line, away from the ticket counter that
    /// every producer increments.
    cached_consumer_seq: *const AtomicU64,

    /// Array of slot containers rather than bare payloads: the ring owns
    /// the publication cell, and the payload type cannot see it.
    base: *mut RbSlot<T>,
}

unsafe impl<T: Slot> Send for MpscRingBuffer<T> {}

unsafe impl<T: Slot> Sync for MpscRingBuffer<T> {}

impl<T: Slot> MpscRingBuffer<T> {
    pub fn new(capacity: usize) -> std::io::Result<Self> {
        assert!(capacity.is_power_of_two(), "capacity must be a power of two");
        #[cfg(not(feature = "loom"))]
        const { RbSlot::<T>::assert_layout(); }

        let slot_size = std::mem::size_of::<RbSlot<T>>();
        let total_size = HEADER_SIZE + capacity * slot_size;
        let arena = Arena::new(total_size)?;
        let ptr = arena.as_ptr();

        let claim_seq = unsafe {
            ptr.add(std::mem::offset_of!(MpscRingHeader, claim_seq)) as *const AtomicU64
        };
        let consumer_seq = unsafe {
            ptr.add(std::mem::offset_of!(MpscRingHeader, consumer_seq)) as *mut u64
        };
        let cached_consumer_seq = unsafe {
            ptr.add(std::mem::offset_of!(MpscRingHeader, cached_consumer_seq)) as *const AtomicU64
        };
        let base = unsafe { ptr.add(HEADER_SIZE) as *mut RbSlot<T> };

        debug_assert!((claim_seq as usize).is_multiple_of(align_of::<AtomicU64>()));
        debug_assert!((consumer_seq as usize).is_multiple_of(align_of::<AtomicU64>()));
        debug_assert!((cached_consumer_seq as usize).is_multiple_of(align_of::<AtomicU64>()));
        debug_assert!((base as usize).is_multiple_of(align_of::<RbSlot<T>>()));

        for i in 0..capacity {
            unsafe {
                RbSlot::init_sequence(base.add(i), i as u64);
            }
        }

        Ok(
            Self {
                arena,
                capacity,
                mask: capacity - 1,
                claim_seq,
                consumer_seq,
                cached_consumer_seq,
                base,
            }
        )
    }


    /// The ticket counter, for tests that watch arbitration between
    /// producers directly.
    #[cfg(test)]
    pub(crate) fn claim_seq(&self) -> &AtomicU64 {
        unsafe { &*self.claim_seq }
    }

    /// Waits until the slot of `last_turn` is free.
    ///
    /// The cheap path consults the shared copy of the consumer position and
    /// returns without touching the consumer's own cursor at all. That is
    /// the common case: while the ring is not close to full, no producer
    /// needs to know exactly where the consumer is, only that it is far
    /// enough ahead.
    ///
    /// **For defence:** the copy is not merely a performance trick, and the
    /// orderings on it are load-bearing. The gate is what gives a producer
    /// the right to overwrite payload bytes the consumer read on the
    /// previous lap, and that right arrives as an ordering edge from the
    /// consumer's release when it advanced its cursor. Taking the cheap
    /// path skips that read, so the edge has to arrive another way: the
    /// producer that last refreshed the copy did acquire the consumer's
    /// cursor, and it published the copy with a release store; a producer
    /// that acquires the copy therefore inherits everything the refreshing
    /// producer had, the consumer's release included. Weaken either the
    /// load or the store to relaxed and the chain breaks silently — the
    /// code still runs, and the payload bytes stop being ordered.
    ///
    /// A stale copy costs a needless re-read and nothing else. Producers
    /// overwrite it without coordinating, so a slower one can move it
    /// backwards; that only sends the next producer down the expensive
    /// path, which is always correct.
    ///
    /// The wait has no bound and cannot fail: a producer that reaches it is
    /// holding a turn it has already taken off the counter, and there is no
    /// way to give a turn back. The bounded form takes its turns the other
    /// way round and lives in `try_claim_batch`.
    ///
    /// The escalation still matters. A consumer part-way through a batch
    /// frees room within a few hundred nanoseconds, and the pause
    /// instructions cover that without leaving the core; a consumer that has
    /// stalled outright would otherwise keep this core at full rate for the
    /// rest of the run, which is what the yielding steps are for.
    #[inline(always)]
    fn await_room(&self, last_turn: u64) {
        let capacity = self.capacity as u64;
        let cached = unsafe { &*self.cached_consumer_seq };

        if last_turn < cached.load(Ordering::Acquire) + capacity {
            return;
        }

        let mut spin_wait = SpinWait::new();

        loop {
            let consumer = unsafe { load_u64_acquire(self.consumer_seq) };
            if last_turn < consumer + capacity {
                cached.store(consumer, Ordering::Release);
                return;
            }
            spin_wait.wait();
        }
    }

    pub fn claim(&self) -> ClaimedSlot<'_, T> {
        let claimed = unsafe { &*self.claim_seq }.fetch_add(1, Ordering::Relaxed);
        let index = (claimed as usize) & self.mask;

        let slot_ptr = unsafe { self.base.add(index) };

        self.await_room(claimed);

        let payload = unsafe { RbSlot::payload_mut(slot_ptr) };
        let sequence_cell = unsafe { RbSlot::sequence_ref(slot_ptr) };

        ClaimedSlot::new(payload, sequence_cell, claimed + self.capacity as u64)
    }

    pub fn try_read(&self) -> Option<ReadSlot<'_, T>> {
        let consumer = unsafe { load_u64_relaxed(self.consumer_seq) };
        let index = (consumer as usize) & self.mask;
        let slot_ptr = unsafe { self.base.add(index) };
        let sequence_cell = unsafe { RbSlot::sequence_ref(slot_ptr) };

        let expected_seq = consumer + self.capacity as u64;
        if load_sequence_acquire(sequence_cell) != expected_seq {
            return None;
        }

        let payload = unsafe { RbSlot::payload_ref(slot_ptr) };

        Some(
            ReadSlot::new(
                payload,
                self.consumer_seq,
                consumer + 1
            )
        )
    }

    pub fn claim_batch(&self, count: usize) -> ClaimedBatch<'_, T> {
        assert!(count > 0 && count <= self.capacity, "batch count must be greater than 0 and less or equals to capacity {}", self.capacity);

        let start = unsafe { &*self.claim_seq }.fetch_add(count as u64, Ordering::Relaxed);

        self.await_room(start + count as u64 - 1);

        ClaimedBatch {
            base: self.base,
            mask: self.mask,
            start,
            count,
            capacity: self.capacity as u64,
            _ring: PhantomData,
        }
    }

    /// Claims a run of turns, giving up if the ring has not made room
    /// within `max_yields` yielding steps.
    ///
    /// This is the ingress form, and the only claim in the project that may
    /// fail. It exists because the caller — a Worker holding a batch it has
    /// decoded but not yet acknowledged — is the one producer in the system
    /// whose item does not exist inside the system yet, and which therefore
    /// still has somewhere to put a refusal: a client is on the other end
    /// of an open connection waiting for an answer. Every other producer
    /// holds something already accounted for and must wait however long it
    /// takes.
    ///
    /// **For defence:** the reason this cannot be the blocking claim with a
    /// timer bolted on is the counter. `claim_batch` takes its turns with a
    /// single unconditional increment and only then waits for the slots to
    /// free, which is wait-free and exactly right when giving up is not an
    /// option — but a producer that took turns and then abandoned them
    /// would leave a permanent hole in the sequence, and the consumer would
    /// wait on the first abandoned turn forever. There is no way to hand a
    /// turn back once the increment has published it. So the order has to
    /// be inverted: establish that the room exists, then take the turns
    /// with a compare-and-exchange that fails harmlessly if another
    /// producer moved first. The price is that this path is lock-free
    /// rather than wait-free — a producer can lose the exchange repeatedly
    /// under contention — and that price is affordable precisely here,
    /// because the allowance bounds it and the outcome of losing for too
    /// long is the rejection this method exists to deliver.
    ///
    /// One exchange covers a whole batch, so producers contend per batch
    /// rather than per transfer.
    pub fn try_claim_batch(&self, count: usize, max_yields: u32) -> Option<ClaimedBatch<'_, T>> {
        assert!(count > 0 && count <= self.capacity, "batch count must be greater than 0 and less or equals to capacity {}", self.capacity);

        let capacity = self.capacity as u64;
        let claim_seq = unsafe { &*self.claim_seq };
        let cached = unsafe { &*self.cached_consumer_seq };

        let mut wait = BoundedSpinWait::new(max_yields);
        let mut start = claim_seq.load(Ordering::Relaxed);

        loop {
            let last_turn = start + count as u64 - 1;

            if last_turn >= cached.load(Ordering::Acquire) + capacity {
                let consumer = unsafe { load_u64_acquire(self.consumer_seq) };

                if last_turn >= consumer + capacity {
                    if !wait.wait() {
                        return None;
                    }
                    start = claim_seq.load(Ordering::Relaxed);
                    continue;
                }

                cached.store(consumer, Ordering::Release);
            }

            match claim_seq.compare_exchange_weak(
                start,
                start + count as u64,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => start = current,
            }
        }

        Some(
            ClaimedBatch {
                base: self.base,
                mask: self.mask,
                start,
                count,
                capacity: self.capacity as u64,
                _ring: PhantomData,
            }
        )
    }

    pub fn drain_batch(&self, max_count: usize) -> DrainBatch<'_, T> {
        let consumer = unsafe { load_u64_relaxed(self.consumer_seq) };

        let mut ready_count = 0;

        for i in 0..max_count {
            let seq = consumer + i as u64;
            let index = (seq as usize) & self.mask;
            let sequence_cell = unsafe { RbSlot::sequence_ref(self.base.add(index)) };
            let expected = seq + self.capacity as u64;

            if load_sequence_acquire(sequence_cell) != expected {
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
            _ring: PhantomData,
        }
    }
}

#[cfg(test)]
#[cfg(not(miri))]
#[cfg(not(feature = "loom"))]
mod tests {
    use super::*;
    use crate::slot::Slot;
    use std::thread;
    use std::sync::Arc;

    /// How many fruitless reads a consumer tolerates before it declares the
    /// producers gone. Generous enough that merely slow producers never
    /// trip it, small enough that broken ones fail in seconds.
    const IDLE_READ_CEILING: u64 = 50_000_000;

    /// 56 bytes of payload; `RbSlot<TestPayload>` is 64 — the same shape
    /// the former `TestSlot` had, with the publication cell now owned by
    /// the container instead of the payload type.
    #[repr(C)]
    #[derive(Clone, Copy, Debug, PartialEq)]
    struct TestPayload {
        value: u64,
        _padding: [u8; 48],
    }

    impl Slot for TestPayload {}

    #[test]
    fn single_claim_publish_read_release() {
        let rb = MpscRingBuffer::<TestPayload>::new(16).unwrap();

        let mut claimed = rb.claim();
        claimed.as_mut().value = 42;
        claimed.publish();

        let read = rb.try_read().expect("should have data");
        assert_eq!(read.as_ref().value, 42);
        read.release();
    }

    #[test]
    fn empty_read_returns_none() {
        let rb = MpscRingBuffer::<TestPayload>::new(16).unwrap();
        assert!(rb.try_read().is_none());
    }

    #[test]
    fn multiple_sequential() {
        let rb = MpscRingBuffer::<TestPayload>::new(16).unwrap();

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
        let rb = MpscRingBuffer::<TestPayload>::new(16).unwrap();

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
        let rb = MpscRingBuffer::<TestPayload>::new(4).unwrap();

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
        let rb = MpscRingBuffer::<TestPayload>::new(16).unwrap();

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
        let rb = Arc::new(MpscRingBuffer::<TestPayload>::new(256).unwrap());
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

    /// Two producers against a ring far too small for what they send, so
    /// both are pushed onto the waiting path repeatedly.
    ///
    /// The other threaded tests here are sized so nobody waits. This one
    /// exists to exercise the gate itself: every value must still arrive,
    /// and each producer's own values must still arrive in the order it
    /// sent them, however many times its wait was interrupted.
    #[test]
    fn producers_wait_for_room_and_resume() {
        const CAPACITY: usize = 4;
        const PER_WRITER: u64 = 64;

        let rb = Arc::new(MpscRingBuffer::<TestPayload>::new(CAPACITY).unwrap());

        let writers: Vec<_> = (0..2u64)
            .map(|writer_id| {
                let rb = Arc::clone(&rb);
                thread::spawn(move || {
                    for index in 0..PER_WRITER {
                        let mut claimed = rb.claim();
                        claimed.as_mut().value = writer_id * 1_000 + index;
                        claimed.publish();
                    }
                })
            })
            .collect();

        let total = (PER_WRITER * 2) as usize;
        let mut received: Vec<u64> = Vec::with_capacity(total);
        let mut empty_reads = 0u64;
        while received.len() < total {
            match rb.try_read() {
                Some(read) => {
                    received.push(read.as_ref().value);
                    read.release();
                    empty_reads = 0;
                }
                None => {
                    empty_reads += 1;
                    assert!(
                        empty_reads < IDLE_READ_CEILING,
                        "the producers stopped delivering after {} values",
                        received.len(),
                    );
                    std::hint::spin_loop();
                }
            }
        }

        for writer in writers {
            writer.join().expect("writer thread panicked");
        }

        for writer_id in 0..2u64 {
            let mine: Vec<u64> = received
                .iter()
                .copied()
                .filter(|value| value / 1_000 == writer_id)
                .collect();
            let expected: Vec<u64> = (0..PER_WRITER).map(|index| writer_id * 1_000 + index).collect();
            assert_eq!(mine, expected, "writer {writer_id} lost or reordered a value across a wait");
        }
    }
}

/// A Miri-friendly MPSC ring buffer: a Vec in place of the Arena.
/// The same layout: [claim_seq(AtomicU64) | _pad(56) | consumer_seq(u64) | _pad(56) | slots...]
///
/// Miri cannot run threads, so what is checked here is single-threaded
/// pointer correctness; the cross-thread guarantees are covered by the
/// Loom models instead.
///
/// cargo +nightly miri test -p ringbuf -- miri_mpsc
#[cfg(test)]
#[cfg(not(feature = "loom"))]
mod miri_tests {
    use super::*;

    /// 56 bytes of payload; `RbSlot<TestPayload>` is 64 — the same shape
    /// the former `TestSlot` had.
    #[repr(C)]
    #[derive(Clone, Copy, Debug, PartialEq)]
    struct TestPayload {
        value: u64,
        _padding: [u8; 48],
    }

    impl Slot for TestPayload {}


    #[test]
    fn miri_mpsc_single_claim_publish_read() {
        let rb = MpscRingBuffer::<TestPayload>::new(16).unwrap();

        let mut claimed = rb.claim();
        claimed.as_mut().value = 42;
        claimed.publish();

        let read = rb.try_read().expect("should have data");
        assert_eq!(read.as_ref().value, 42);
        read.release();
    }

    #[test]
    fn miri_mpsc_empty_read() {
        let rb = MpscRingBuffer::<TestPayload>::new(16).unwrap();
        assert!(rb.try_read().is_none());
    }

    #[test]
    fn miri_mpsc_multiple_sequential() {
        let rb = MpscRingBuffer::<TestPayload>::new(16).unwrap();

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
    fn miri_mpsc_fill_and_drain() {
        let rb = MpscRingBuffer::<TestPayload>::new(16).unwrap();

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
    fn miri_mpsc_wrap_around() {
        let rb = MpscRingBuffer::<TestPayload>::new(4).unwrap();

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
    fn miri_mpsc_batch_claim_and_drain() {
        let rb = MpscRingBuffer::<TestPayload>::new(16).unwrap();

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
    fn miri_mpsc_batch_wrap_around() {
        let rb = MpscRingBuffer::<TestPayload>::new(4).unwrap();

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
    fn miri_mpsc_simulated_two_writers() {
        let rb = MpscRingBuffer::<TestPayload>::new(16).unwrap();

        let mut c1 = rb.claim();
        c1.as_mut().value = 1_000_000;
        c1.publish();

        let mut c2 = rb.claim();
        c2.as_mut().value = 2_000_000;
        c2.publish();

        let mut c3 = rb.claim();
        c3.as_mut().value = 1_000_001;
        c3.publish();

        let r1 = rb.try_read().expect("data 1");
        assert_eq!(r1.as_ref().value, 1_000_000);
        r1.release();

        let r2 = rb.try_read().expect("data 2");
        assert_eq!(r2.as_ref().value, 2_000_000);
        r2.release();

        let r3 = rb.try_read().expect("data 3");
        assert_eq!(r3.as_ref().value, 1_000_001);
        r3.release();

        assert!(rb.try_read().is_none());
    }

    #[test]
    fn miri_mpsc_atomic_fetch_add_correctness() {
        let rb = MpscRingBuffer::<TestPayload>::new(8).unwrap();

        for i in 0..8u64 {
            let mut claimed = rb.claim();
            claimed.as_mut().value = i;
            claimed.publish();
        }

        let claim_val = rb.claim_seq().load(Ordering::Relaxed);
        assert_eq!(claim_val, 8);

        for i in 0..8u64 {
            let read = rb.try_read().expect("should have data");
            assert_eq!(read.as_ref().value, i);
            read.release();
        }

        for i in 0..4u64 {
            let mut claimed = rb.claim();
            claimed.as_mut().value = i + 100;
            claimed.publish();
        }

        assert_eq!(rb.claim_seq().load(Ordering::Relaxed), 12);
    }

    /// The cross-thread half of obligation O3 on the multi-producer side:
    /// two producers each holding a `&mut` payload projection while the
    /// consumer concurrently atomic-loads the sequence cells.
    ///
    /// **For defence:** together with `miri_spsc_threaded` this is the test
    /// that CAUGHT the retag-vs-atomic-load data race. On the previous
    /// shape a producer's handle held `&mut T` over the WHOLE slot, so its
    /// retag claimed the eight bytes at offset 0 that the consumer polls —
    /// Miri reported "Data race detected ... retag write". With `RbSlot<P>`
    /// the producer's reference covers only the payload extent, so the
    /// accesses are disjoint by construction. This variant additionally
    /// exercises the `fetch_add(Relaxed)` claim under genuine contention,
    /// which the single-threaded `miri_mpsc_simulated_two_writers` cannot
    /// reach.
    ///
    /// Capacity equals the TOTAL item count (two producers x four values),
    /// so no producer ever spin-waits on a full ring — a blocked producer
    /// is catastrophic under the Miri interpreter, where every spin
    /// iteration is faithfully interpreted.
    #[test]
    fn miri_mpsc_threaded() {
        use std::sync::Arc;
        use std::thread;

        const VALUES_PER_PRODUCER: usize = 4;
        const CAPACITY: usize = 8;

        let first_producer_values: [u64; VALUES_PER_PRODUCER] =
            [1_000_000, 1_000_001, 1_000_002, 1_000_003];
        let second_producer_values: [u64; VALUES_PER_PRODUCER] =
            [2_000_000, 2_000_001, 2_000_002, 2_000_003];

        let ring = Arc::new(MpscRingBuffer::<TestPayload>::new(CAPACITY).unwrap());

        let first_ring = Arc::clone(&ring);
        let first_producer = thread::spawn(move || {
            for value in first_producer_values {
                let mut claimed = first_ring.claim();
                claimed.as_mut().value = value;
                claimed.publish();
            }
        });

        let second_ring = Arc::clone(&ring);
        let second_producer = thread::spawn(move || {
            for value in second_producer_values {
                let mut claimed = second_ring.claim();
                claimed.as_mut().value = value;
                claimed.publish();
            }
        });

        let consumer_ring = Arc::clone(&ring);
        let consumer = thread::spawn(move || {
            let mut received: Vec<u64> = Vec::with_capacity(CAPACITY);
            while received.len() < CAPACITY {
                match consumer_ring.try_read() {
                    Some(read) => {
                        received.push(read.as_ref().value);
                        read.release();
                    }
                    None => std::hint::spin_loop(),
                }
            }
            received
        });

        first_producer.join().expect("first producer thread panicked");
        second_producer.join().expect("second producer thread panicked");
        let received = consumer.join().expect("consumer thread panicked");

        for value in first_producer_values {
            assert!(received.contains(&value), "value {value} from the first producer was lost");
        }
        for value in second_producer_values {
            assert!(received.contains(&value), "value {value} from the second producer was lost");
        }

        let received_from_first: Vec<u64> =
            received.iter().copied().filter(|value| first_producer_values.contains(value)).collect();
        assert_eq!(received_from_first, first_producer_values.to_vec(),
            "per-producer FIFO order violated for the first producer");

        let received_from_second: Vec<u64> =
            received.iter().copied().filter(|value| second_producer_values.contains(value)).collect();
        assert_eq!(received_from_second, second_producer_values.to_vec(),
            "per-producer FIFO order violated for the second producer");
    }

    /// A consumer that takes a PARTIAL batch gates on an intermediate
    /// sequence cell and never reaches the batch's last one. Batch
    /// publication therefore has to emit its release barrier ahead of
    /// EVERY cell store, not just ahead of the last: a cell written before
    /// the barrier grants a reader no right to the payload.
    ///
    /// `drain_batch(1)` is the shortest expression of that path — it takes
    /// exactly one turn and returns. If the ordering regressed, the payload
    /// read below would race with the producer's write to the same bytes
    /// and the race detector would report it.
    #[test]
    fn miri_batch_partial_drain_gates_on_intermediate_cell() {
        use std::sync::Arc;
        use std::thread;

        const BATCH: usize = 2;

        let ring = Arc::new(MpscRingBuffer::<TestPayload>::new(4).unwrap());

        let producer_ring = Arc::clone(&ring);
        let producer = thread::spawn(move || {
            let mut claimed = producer_ring.claim_batch(BATCH);
            for index in 0..BATCH {
                claimed.slot_mut(index).value = 100 + index as u64;
            }
            claimed.publish();
        });

        let consumer_ring = Arc::clone(&ring);
        let consumer = thread::spawn(move || {
            loop {
                let batch = consumer_ring.drain_batch(1);
                if batch.is_empty() {
                    std::hint::spin_loop();
                    continue;
                }
                let observed = batch.slot(0).value;
                batch.release();
                return observed;
            }
        });

        let observed = consumer.join().expect("consumer thread panicked");
        producer.join().expect("producer thread panicked");

        assert_eq!(observed, 100, "the first slot of the batch must read back");
    }

    #[test]
    fn bounded_claim_succeeds_while_the_ring_has_room() {
        let rb = MpscRingBuffer::<TestPayload>::new(8).unwrap();

        let mut batch = rb
            .try_claim_batch(4, 64)
            .expect("a ring with room must admit the batch");
        for i in 0..batch.len() {
            batch.slot_mut(i).value = i as u64;
        }
        batch.publish();

        let drained = rb.drain_batch(8);
        assert_eq!(drained.len(), 4);
        for i in 0..drained.len() {
            assert_eq!(drained.slot(i).value, i as u64);
        }
        drained.release();
    }

    #[test]
    fn bounded_claim_gives_up_on_a_full_ring() {
        let rb = MpscRingBuffer::<TestPayload>::new(4).unwrap();

        let mut batch = rb
            .try_claim_batch(4, 64)
            .expect("the first batch fills the ring");
        for i in 0..batch.len() {
            batch.slot_mut(i).value = i as u64;
        }
        batch.publish();

        assert!(
            rb.try_claim_batch(1, 1).is_none(),
            "a full ring with nothing drained must refuse rather than block",
        );
    }

    #[test]
    fn bounded_claim_leaves_no_hole_when_it_gives_up() {
        let rb = MpscRingBuffer::<TestPayload>::new(4).unwrap();

        let mut batch = rb
            .try_claim_batch(4, 64)
            .expect("the first batch fills the ring");
        for i in 0..batch.len() {
            batch.slot_mut(i).value = 100 + i as u64;
        }
        batch.publish();

        assert!(rb.try_claim_batch(2, 1).is_none());

        let drained = rb.drain_batch(4);
        assert_eq!(drained.len(), 4, "a refused claim must not swallow turns");
        for i in 0..drained.len() {
            assert_eq!(drained.slot(i).value, 100 + i as u64);
        }
        drained.release();

        let mut batch = rb
            .try_claim_batch(2, 64)
            .expect("released turns must be claimable again");
        for i in 0..batch.len() {
            batch.slot_mut(i).value = 200 + i as u64;
        }
        batch.publish();

        let drained = rb.drain_batch(4);
        assert_eq!(drained.len(), 2);
        assert_eq!(drained.slot(0).value, 200);
        assert_eq!(drained.slot(1).value, 201);
        drained.release();
    }
}
