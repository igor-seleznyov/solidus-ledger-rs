use std::marker::PhantomData;
use crate::slot::{RbSlot, Slot};
use crate::sequence_mem_barrier::store_u64_relaxed;

#[cfg(not(feature = "loom"))]
use std::sync::atomic::{fence, Ordering};
#[cfg(feature = "loom")]
use loom::sync::atomic::{fence, Ordering};

/// A producer's exclusive hold on a run of claimed turns.
///
/// The single-turn descriptors hold real borrows of the individual slot
/// fields; this one deliberately holds a base pointer instead, and the
/// difference is a consequence of the layout rather than unconverted work.
/// A descriptor covering one turn needs one borrow per field and has
/// somewhere to keep it. A descriptor covering K turns would need K of
/// them, and there is nowhere to put K borrows inside a fixed-size value
/// without allocating, which the hot path does not do. Nor can one borrow
/// stand in for them: payload bytes are separated by a whole slot each,
/// with a publication cell between every pair, so no contiguous run of
/// payloads exists to borrow as a single slice.
pub struct ClaimedBatch<'a, P: Slot> {
    pub(crate) base: *mut RbSlot<P>,
    pub(crate) mask: usize,
    pub(crate) start: u64,
    pub(crate) count: usize,
    pub(crate) capacity: u64,

    /// Ties the batch to the ring it came from without borrowing any slot.
    /// A batch outliving its ring would index freed memory, and this marker
    /// makes that a compile error. It is deliberately not a borrow of the
    /// slots: an exclusive borrow spanning whole slots would also cover the
    /// publication cells, which a consumer polls concurrently and
    /// legitimately.
    pub(crate) _ring: PhantomData<&'a ()>,
}

/// A consumer's hold on a run of published turns.
///
/// It carries a base pointer for the same reason its producer-side sibling
/// does: a run of K turns cannot be expressed as a fixed number of borrows,
/// and the payloads are not contiguous.
pub struct DrainBatch<'a, T: Slot> {
    pub(crate) base: *mut RbSlot<T>,
    pub(crate) mask: usize,
    pub(crate) consumer_seq: *mut u64,
    pub(crate) start: u64,
    pub(crate) count: usize,

    /// Ties the batch to the ring it came from without borrowing any slot,
    /// so a batch cannot outlive the memory it indexes.
    pub(crate) _ring: PhantomData<&'a ()>,
}

impl<'a, P: Slot> ClaimedBatch<'a, P> {
    pub fn slot_mut(&mut self, i: usize) -> &mut P {
        assert!(i < self.count);
        let index = ((self.start + i as u64) as usize) & self.mask;
        unsafe {
            RbSlot::payload_mut(self.base.add(index))
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    /// Publishes every turn of the batch.
    ///
    /// The release barrier is emitted before the FIRST cell store, not
    /// before the last, and the placement is load-bearing. A release
    /// barrier grants a reader the right to see everything written before
    /// it only when that reader observes a value stored after the barrier;
    /// a cell stored ahead of the barrier grants nothing at all. A consumer
    /// taking a partial batch stops at whichever turn happens to be ready
    /// and never looks at this batch's final cell, so every cell has to be
    /// a valid entry point on its own. Publishing the earlier cells ahead
    /// of the barrier would leave such a consumer reading payload bytes
    /// with nothing ordering them against the writes that produced them —
    /// a race that hardware ordering stores among themselves happens to
    /// hide, and weaker hardware does not.
    ///
    /// One barrier still covers the whole batch, so the cost stays one
    /// barrier per batch however many turns it carries.
    pub fn publish(self) {
        fence(Ordering::Release);

        for i in 0..self.count {
            let index = ((self.start + i as u64) as usize) & self.mask;
            let sequence_cell = unsafe { RbSlot::sequence_ref(self.base.add(index)) };
            sequence_cell.store(self.start + i as u64 + self.capacity, Ordering::Relaxed);
        }
    }
}

impl<'a, P: Slot> DrainBatch<'a, P> {
    pub fn slot(&self, i: usize) -> &P {
        assert!(i < self.count);
        let index = ((self.start + i as u64) as usize) & self.mask;
        unsafe {
            RbSlot::payload_ref(self.base.add(index))
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Hands the whole drained run back by advancing the consumer cursor
    /// once, writing nothing into any slot.
    ///
    /// The release barrier orders this consumer's payload reads ahead of
    /// the cursor advance, so a producer that observes the advanced cursor
    /// and enters those turns cannot overwrite bytes this consumer is still
    /// reading.
    ///
    /// **For defence:** an earlier form of this protocol had the consumer
    /// write one cell per message here, and configured batch sizes are 64
    /// and 128. Counted in cache lines over the batch path, each of those
    /// stores is a read-for-ownership transaction that pulls the line away
    /// from the producer's core, and the value stored was the one
    /// publication had already left there. Writing nothing removes that
    /// traffic outright; a producer now learns which turns are free from
    /// the cursor alone.
    pub fn release(self) {
        fence(Ordering::Release);
        unsafe {
            store_u64_relaxed(self.consumer_seq, self.start + self.count as u64);
        }
    }
}
