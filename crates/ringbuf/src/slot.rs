use std::marker::PhantomData;
use std::mem::offset_of;
use crate::sequence_mem_barrier::store_u64_relaxed;

#[cfg(not(feature = "loom"))]
use std::sync::atomic::{fence, AtomicU64, Ordering};

#[cfg(feature = "loom")]
use loom::sync::atomic::{fence, AtomicU64, Ordering};

/// Payload marker for Ring Buffer slots.
///
/// **For defence:** the predecessor was an `unsafe trait` carrying three
/// human-enforced promises (repr(C, align(64)), `sequence: u64` at offset
/// 0, no Drop). `RbSlot`'s construction now enforces the layout promises
/// by the compiler (const asserts in the rings' `new()`), and the no-Drop
/// promise follows from the `Copy` bound — no human promise remains, so
/// the trait is safe. `sequence()` / `set_sequence` are gone: the
/// publication cell is owned by ringbuf, not by payload types.
pub trait Slot: Copy + Sized {}

/// Ring Buffer slot container: the ring owns the synchronisation cell,
/// the payload type cannot see it.
///
/// **For defence:** a `&mut` over the WHOLE former slot type asserted
/// exclusivity over the `sequence` cell at offset 0 that other threads
/// legitimately poll atomically — a Miri-confirmed data race (latent in
/// the shipped volatile code; the atomics migration made it visible).
/// With `RbSlot`, `sequence` is not a member of `P`, so no expressible
/// safe reference and no whole-payload assignment can cover the
/// publication cell — the race is closed by construction for all safe and
/// cross-crate access. Within `ringbuf` the projection discipline still
/// governs: the `pub(crate)` fields keep a whole-slot `&mut *base.add(i)`
/// or a non-atomic store to `sequence` in-crate-expressible, so the "by
/// construction" guarantee is exact only against safe / cross-crate code.
/// The fields cannot be made private — cross-module `offset_of!`
/// const-asserts and the in-crate Miri/Loom harnesses must construct
/// `RbSlot` directly. Layout: `sequence` at offset 0, `payload` at offset
/// 8, both const-asserted. Offset 8 holds only while every payload type
/// aligns to 8 or less; a payload wanting 16 or 32 would push its own
/// start past 8 and silently break every reader that computes the offset
/// once, so that limit is asserted at construction rather than trusted.
/// `align(64)` preserves the former slot guarantee, and byte sizes are
/// unchanged (64/64/64/192/192).
#[repr(C, align(64))]
pub struct RbSlot<P: Slot> {
    pub(crate) sequence: AtomicU64,
    pub(crate) payload: P,
}

impl<P: Slot> RbSlot<P> {
    /// Payload offset for external layout tests (fields are `pub(crate)`;
    /// an `RbSlot` is not constructible from outside the crate).
    pub const PAYLOAD_OFFSET: usize = offset_of!(RbSlot<P>, payload);

    /// Layout covenant asserted at construction (const-eval, no runtime
    /// cost). Single source for the `RbSlot` layout contract, called from
    /// each ring's `new()` under `cfg(not(loom))`; Phase-3 `TcpRingBuffer`
    /// inherits the same guard without copying the block.
    pub(crate) const fn assert_layout() {
        assert!(offset_of!(RbSlot<P>, sequence) == 0);
        assert!(Self::PAYLOAD_OFFSET == 8);
        assert!(std::mem::align_of::<RbSlot<P>>() == 64);
        assert!(std::mem::align_of::<P>() <= 8);
    }

    /// Projection onto ONLY the sequence cell: `&AtomicU64` is a shared
    /// reference to an atomic — legal concurrently. A `&RbSlot` over the
    /// whole slot is never formed: it would alias the payload a producer
    /// may hold `&mut` to.
    ///
    /// # Safety
    /// `slot_ptr` must be valid (masked to < capacity -> Arena slot
    /// region) for the returned reference's lifetime.
    #[inline(always)]
    pub(crate) unsafe fn sequence_ref<'a>(slot_ptr: *const Self) -> &'a AtomicU64 {
        unsafe {
            &(*slot_ptr).sequence
        }
    }

    /// The one place where the whole design rests on a single property of
    /// the aliasing model: a `&mut` taken through a field projection covers
    /// exactly the payload bytes and asserts nothing over the publication
    /// cell that shares the same slot. If that were untrue, every producer
    /// would be claiming exclusive access to eight bytes a consumer polls
    /// concurrently. The Miri race test that runs both borrow models
    /// exercises exactly this and is what keeps the property honest.
    ///
    /// # Safety
    /// `slot_ptr` must be valid, and the caller must own the payload
    /// exclusively: a producer between claim and publish, or
    /// single-threaded initialisation before the ring is shared.
    #[inline(always)]
    pub(crate) unsafe fn payload_mut<'a>(slot_ptr: *mut Self) -> &'a mut P {
        unsafe {
            &mut (*slot_ptr).payload
        }
    }

    /// # Safety
    /// `slot_ptr` must be valid; the slot must be published and not yet
    /// reused (a consumer between observing the sequence and release).
    #[inline(always)]
    pub(crate) unsafe fn payload_ref<'a>(slot_ptr: *const Self) -> &'a P {
        unsafe {
            &(*slot_ptr).payload
        }
    }

    /// Initialises the cell BEFORE the ring is shared (happens-before via
    /// construction + spawn); a raw write, no reference over the slot.
    ///
    /// # Safety
    /// `slot_ptr` must be valid; single-threaded, before the ring is
    /// handed to any other thread.
    pub(crate) unsafe fn init_sequence(slot_ptr: *mut Self, initial_sequence: u64) {
        unsafe {
            (&raw mut (*slot_ptr).sequence).write(AtomicU64::new(initial_sequence));
        }
    }
}

/// Acquire-load of a typed sequence cell.
///
/// **For defence:** a safe function, because the cell arrives as a typed
/// `&AtomicU64` and no raw pointer crosses the boundary. The spelling is a
/// `Relaxed` load followed by a standalone acquire fence, paired with the
/// writer's release fence, rather than an acquire load. On x86-64 the two
/// lower to the same single `mov` plus a compiler-only barrier — the
/// hardware already orders load-load, so no fence instruction is emitted
/// and the choice costs nothing on this target. The ordering a reader
/// depends on comes from pairing with the writer's fence, not from any
/// per-field annotation: once a matching turn number is visible, every
/// payload byte written before that fence is visible too.
#[inline(always)]
pub(crate) fn load_sequence_acquire(sequence_cell: &AtomicU64) -> u64 {
    let value = sequence_cell.load(Ordering::Relaxed);
    fence(Ordering::Acquire);
    value
}

/// A producer's exclusive hold on one claimed slot.
///
/// The `'a` lifetime comes from the two real borrows below, not from a
/// zero-sized stand-in, so the ring is genuinely borrowed for as long as a
/// claim is live and cannot be dropped or moved underneath it. That closes
/// a use-after-free which is otherwise expressible in safe code:
///
/// ```compile_fail,E0505
/// use ringbuf::spsc_ring_buffer::SpscRingBuffer;
/// use ringbuf::slot::Slot;
/// #[repr(C)]
/// #[derive(Clone, Copy)]
/// struct DocPayload { value: u64 }
/// impl Slot for DocPayload {}
/// let ring = SpscRingBuffer::<DocPayload>::new(8).unwrap();
/// let mut claimed = ring.claim();
/// drop(ring);                 // ERROR: ring is borrowed by `claimed`
/// claimed.as_mut().value = 1;
/// ```
///
/// A claim also stays on the thread that took it. Nothing in the memory
/// ordering requires that — publication emits its release barrier on
/// whichever thread runs it, and the consumer's acquire pairs with that
/// barrier wherever it came from — but a claim that travels can be lost,
/// and a lost claim wedges the ring: the turn is never published, so the
/// consumer waits on it forever while producers keep taking later turns
/// until they hit the admission gate. Making the type refuse to cross a
/// thread boundary turns that mistake into a compile error at the moment
/// it is written, which matters because the code moves values into
/// spawned closures routinely and a claim captured by accident looks
/// exactly like any other capture:
///
/// ```compile_fail,E0277
/// use ringbuf::spsc_ring_buffer::SpscRingBuffer;
/// use ringbuf::slot::Slot;
/// #[repr(C)]
/// #[derive(Clone, Copy)]
/// struct DocPayload { value: u64 }
/// impl Slot for DocPayload {}
/// let ring = SpscRingBuffer::<DocPayload>::new(8).unwrap();
/// let claimed = ring.claim();
/// std::thread::scope(|scope| {
///     scope.spawn(move || {       // ERROR: the claim cannot be sent
///         let mut claimed = claimed;
///         claimed.as_mut().value = 1;
///         claimed.publish();
///     });
/// });
/// ```
pub struct ClaimedSlot<'a, P: Slot> {
    /// Exclusive borrow of the payload bytes ALONE. It does not cover the
    /// publication cell, so a consumer polling that cell concurrently is
    /// not aliased by this reference — which is the whole point: an
    /// exclusive borrow of the whole slot would assert exclusivity over
    /// eight bytes another thread legitimately reads, and two halves of a
    /// 64-byte slot could then be observed torn.
    payload: &'a mut P,

    /// Shared borrow of the publication cell ALONE. Shared is sufficient
    /// because the cell is an atomic and publication stores through it;
    /// shared is also necessary, since the consumer holds its own shared
    /// borrow of the same eight bytes at the same time.
    sequence_cell: &'a AtomicU64,

    /// The value publication writes into the cell — the turn number this
    /// claim occupies, advanced by one full lap of the ring so that a
    /// consumer can tell "written this lap" from "written last lap".
    turn: u64,

    /// Zero-sized, occupies no memory, emits no instruction. It exists for
    /// one reason: a raw pointer cannot be sent between threads, and a
    /// struct loses that ability as soon as one of its fields lacks it, so
    /// naming a raw-pointer type here is what keeps a claim on the thread
    /// that took it. Both real fields above are plain references, which
    /// carry the ability freely, so without this field the compiler would
    /// silently allow a claim to be moved into another thread. The reason
    /// that must not happen is on the struct's own documentation.
    _thread_bound: PhantomData<*const ()>,
}

impl<'a, P: Slot> ClaimedSlot<'a, P> {
    /// Both borrows are created once, by the ring, from the Arena slot
    /// region; the descriptor itself performs no pointer arithmetic and
    /// contains no unsafe operation.
    pub(crate) fn new(payload: &'a mut P, sequence_cell: &'a AtomicU64, turn: u64) -> Self {
        Self {
            payload,
            sequence_cell,
            turn,
            _thread_bound: PhantomData
        }
    }

    pub fn as_mut(&mut self) -> &mut P {
        &mut *self.payload
    }

    /// Publishes the slot: the release fence orders every payload write
    /// made through `as_mut` before the cell store becomes visible, so a
    /// consumer that reads this turn number with a matching acquire fence
    /// sees a complete payload. Consuming `self` is what makes the write
    /// window closed — the payload borrow ends here and cannot outlive
    /// publication.
    pub fn publish(self) {
        fence(Ordering::Release);
        self.sequence_cell.store(self.turn, Ordering::Relaxed);
    }
}

pub struct ReadSlot<'a, P: Slot> {
    /// Shared borrow of the payload bytes ALONE, valid because the slot is
    /// published and the producer may not re-enter it until this
    /// descriptor advances the consumer cursor.
    payload: &'a P,

    /// The consumer cursor lives in the Arena as a plain counter rather
    /// than an atomic, so it is reached through a raw pointer. The
    /// consumer is its only writer; a producer reads it to decide whether
    /// its turn may enter a slot.
    consumer_seq: *mut u64,

    /// The value `release` stores into that cursor: this consumer's turn
    /// plus one. It is fixed when the slot is handed over rather than
    /// recomputed at release time, so the two halves of a read — taking the
    /// slot and giving it back — cannot disagree about which turn was
    /// consumed.
    next_seq: u64,
}

impl<'a, P: Slot> ReadSlot<'a, P> {
    pub(crate) fn new(payload: &'a P, consumer_seq: *mut u64, next_seq: u64) -> Self {
        Self {
            payload,
            consumer_seq,
            next_seq
        }
    }

    pub fn as_ref(&self) -> &P {
        self.payload
    }

    /// Hands the slot back to the producer by advancing the consumer
    /// cursor. The per-slot sequence cell is deliberately NOT written.
    ///
    /// **For defence:** the predecessor stored `consumer + capacity` into
    /// the cell here — precisely the value `publish` had already put
    /// there, since `publish` writes `claim_seq + capacity` and on this
    /// turn `claim_seq == consumer`. The store was therefore identical in
    /// value and told no one anything, while the shared number made
    /// "published, unread" and "free for the next turn" indistinguishable
    /// and let a producer overtake an unread slot. Dropping the store
    /// makes the cell single-writer — written only by `publish`, read
    /// only by `try_read` — removing the race at its source.
    ///
    /// With the cell out of the protocol the entire cross-thread edge
    /// rests on `consumer_seq`: the `fence(Release)` below orders this
    /// consumer's payload READS before the advanced cursor becomes
    /// visible, and the producer reads that cursor with an acquire load.
    pub fn release(self) {
        fence(Ordering::Release);
        unsafe {
            store_u64_relaxed(self.consumer_seq, self.next_seq)
        }
    }
}
