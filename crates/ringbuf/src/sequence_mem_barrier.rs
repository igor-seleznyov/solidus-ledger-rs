use std::sync::atomic::{AtomicU64, Ordering};

/// Relaxed store of an Arena `u64` counter cell.
///
/// **For defence:** the slot-sequence helpers of the previous revision
/// dissolved — the typed `RbSlot.sequence: AtomicU64` field replaced
/// them. Only the two Arena COUNTER cells (`writer_seq` / `consumer_seq`
/// at Arena offsets 0/64) remain behind `from_ptr`, because they have no
/// typed home. `Relaxed` on a single-thread-private counter is a
/// uniformity choice, not synchronisation — a `Relaxed` `u64` access is a
/// plain `mov`. This is an `unsafe fn` because a public safe function
/// dereferencing a caller-supplied raw pointer would push unverifiable
/// validity onto the caller (`clippy::not_unsafe_ptr_arg_deref`); the
/// contract is declared here.
///
/// # Safety
/// `cell` must be a valid, 8-byte-aligned `u64` counter cell in the Arena
/// (page-aligned offset 0/64) with no concurrent non-atomic access.
#[inline(always)]
pub unsafe fn store_u64_relaxed(cell: *mut u64, value: u64) {
    debug_assert!((cell as usize).is_multiple_of(align_of::<AtomicU64>()));
    unsafe {
        AtomicU64::from_ptr(cell).store(value, Ordering::Relaxed);
    }
}

/// Release store of an Arena `u64` counter cell — the publishing half of
/// a cursor hand-over.
///
/// **For defence:** a single-producer ring publishes by moving one cursor
/// rather than by marking each slot, so there is exactly one store per
/// hand-over and nothing to amortise a standalone barrier over. That is
/// why this is a release STORE and not a release FENCE followed by a
/// relaxed store: on x86-64 both lower to the same plain `mov`, and on
/// aarch64 the release store is a single `stlr` while the fence form emits
/// a `dmb ish` in addition to the store. The batched form earns its fence
/// by covering many stores at once; this one would pay the barrier for a
/// single store and gain nothing.
///
/// # Safety
/// `cell` must be a valid, 8-byte-aligned `u64` counter cell in the Arena
/// with no concurrent non-atomic access.
#[inline(always)]
pub unsafe fn store_u64_release(cell: *mut u64, value: u64) {
    debug_assert!((cell as usize).is_multiple_of(align_of::<AtomicU64>()));
    unsafe {
        AtomicU64::from_ptr(cell).store(value, Ordering::Release);
    }
}

/// Relaxed load of an Arena `u64` counter cell.
///
/// # Safety
/// `cell` must be a valid, 8-byte-aligned `u64` counter cell in the Arena
/// with no concurrent non-atomic access.
#[inline(always)]
pub unsafe fn load_u64_relaxed(cell: *const u64) -> u64 {
    debug_assert!((cell as usize).is_multiple_of(align_of::<AtomicU64>()));
    unsafe {
        AtomicU64::from_ptr(cell as *mut u64).load(Ordering::Relaxed)
    }
}

/// Acquire load of an Arena `u64` counter cell — the producer's half of
/// the cross-thread edge.
///
/// **For defence:** a producer no longer asks the per-slot cell whether a
/// slot is free; it asks the consumer's cursor, so the happens-before
/// edge lives on this counter. This load is what the consumer's
/// `fence(Release)` in `release` synchronises with: observing an advanced
/// cursor guarantees that consumer's payload reads are complete before
/// this producer's writes to the slot begin.
///
/// `Acquire` on the load rather than `Relaxed` plus a separate
/// `fence(Acquire)`, on measured codegen for both shapes: on x86-64 the
/// two are instruction-identical (both a plain `mov`, no barrier
/// instruction at all), so the decision is settled on aarch64. There an
/// acquire load emits `ldar` inside the wait loop and nothing on exit,
/// while relaxed-plus-fence emits `ldr` in the loop and a `dmb ishld`
/// full barrier on exit. On the uncontended path — the one that runs a
/// million times a second, because the rings are sized so a producer does
/// not block — this shape emits no standalone barrier instruction and the
/// alternative emits one.
///
/// # Safety
/// `cell` must be a valid, 8-byte-aligned `u64` counter cell in the Arena
/// with no concurrent non-atomic access.
#[inline(always)]
pub unsafe fn load_u64_acquire(cell: *const u64) -> u64 {
    debug_assert!((cell as usize).is_multiple_of(align_of::<AtomicU64>()));
    unsafe {
        AtomicU64::from_ptr(cell as *mut u64).load(Ordering::Acquire)
    }
}
