#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::{_mm_crc32_u64, _mm_crc32_u8};

const CASTAGNOLI_POLYNOMIAL_REVERSED: u32 = 0x82F63B78;
const CRC32C_TABLE: [u32; 256] = build_crc32c_table();

const fn build_crc32c_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut byte_value = 0usize;
    while byte_value < 256 {
        let mut crc = byte_value as u32;
        let mut bit_position = 0usize;
        while bit_position < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ CASTAGNOLI_POLYNOMIAL_REVERSED;
            } else {
                crc >>= 1;
            }
            bit_position += 1;
        }
        table[byte_value] = crc;
        byte_value += 1;
    }
    table
}

/// Switches to the hardware (SSE4.2) CRC-32C path when `true`. Otherwise the
/// software path runs.
///
/// Publish-before-spawn pattern: `init()` writes this from the main thread
/// before `std::thread::spawn` happens; std documents that `thread::spawn`
/// synchronises-with the spawned thread's first instruction, so the write
/// is observable on every worker without an atomic. Worker threads only
/// read; no concurrent mutation exists by construction.
///
/// `static mut` is the appropriate primitive per `docs/invariants.md`
/// hot-path discipline: «no atomic — single writer; the value is stable
/// for the connection's lifetime». An `AtomicBool::load(Relaxed)` would
/// compile to the same `mov` on x86_64, but `Atomic*` semantically signals
/// «expect concurrent mutation», which is misleading here — the value is
/// monotonic (`false` once, then optionally `true`, never back).
///
/// Default `false` (software path). Forgetting `init()` is a performance
/// bug, never a correctness bug — both paths produce identical CRC-32C
/// values.
static mut USE_HARDWARE_CRC32C: bool = false;

/// Selects the fastest available CRC-32C implementation for this CPU.
///
/// Call **once** from `main()` before any `std::thread::spawn`. The spawn
/// happens-before barrier propagates the write to every worker thread.
/// Idempotent within the constraint «main thread only, before spawn»;
/// concurrent calls or calls after worker threads exist would race.
///
/// No-op on non-x86_64 targets (software path is the default and only
/// path).
#[inline]
pub fn init() {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("sse4.2") {
            // SAFETY: by contract, `init()` runs on the main thread before
            // any `thread::spawn`. No concurrent writer exists; no reader
            // exists yet on any other thread. The spawn happens-before
            // barrier will propagate this write to all subsequently spawned
            // threads.
            unsafe { USE_HARDWARE_CRC32C = true };
        }
    }
}

/// Computes CRC-32C (Castagnoli) over `len` bytes starting at `data`.
///
/// Dispatches to the hardware (SSE4.2) or software path based on the flag
/// set by `init()`. No atomic; no fence; no transmute. The flag read is a
/// plain `mov` from a static address, the branch is perfectly predicted
/// after the first call, and the call to `crc32c_hw` / `crc32c_sw` is
/// direct (LLVM can inline either body fully into the caller).
///
/// **For defence:** every cheaper dispatch design — fn-pointer stored in
/// `AtomicUsize` recovered via `transmute<usize, fn>`; `AtomicPtr<()>`
/// round-trip — either fails const-eval or is rejected by Miri's
/// strict-provenance model (the recovered fn-pointer has no provenance
/// tag, calling it is UB). A monotonic `static mut bool` with one writer
/// in `init()` before `thread::spawn` is the cheapest dispatch that
/// satisfies the Rust memory model, Miri, and `docs/invariants.md`'s
/// «no atomic for single writer» rule simultaneously.
///
/// # Safety
/// `data..data+len` must be a valid readable byte range for the caller's
/// duration. `len == 0` is safe; `data` may be dangling when `len == 0`.
#[inline]
pub unsafe fn crc32c(data: *const u8, len: usize) -> u32 {
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: `USE_HARDWARE_CRC32C` is written only by `init()` from the
        // main thread before `thread::spawn`. All callers on worker threads
        // observe the final value via the spawn happens-before barrier. No
        // concurrent mutation; a `Copy` `bool` read is a plain load, no
        // reference is formed (the `static_mut_refs` lint does not apply).
        if unsafe { USE_HARDWARE_CRC32C } {
            // SAFETY: the flag is `true` only after `init()` confirmed
            // SSE4.2 via `is_x86_feature_detected!`. Caller guarantees
            // `data..data+len` valid.
            return unsafe { crc32c_hw(data, len) };
        }
    }
    // SAFETY: caller guarantees `data..data+len` valid; `crc32c_sw` has no
    // platform requirements.
    unsafe { crc32c_sw(data, len) }
}


/// Table-based Castagnoli CRC-32C.
///
/// Reversed polynomial `0x82F63B78`. Pure Rust internals; the `unsafe fn`
/// signature is required only for the raw-pointer API contract, not for any
/// unsafe operation inside beyond the single pointer dereference.
/// Available on every target, including Miri and ARM.
///
/// # Safety
/// `data..data+len` must be a valid readable byte range.
unsafe fn crc32c_sw(data: *const u8, len: usize) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    let mut byte_index = 0usize;
    while byte_index < len {
        // SAFETY: `byte_index < len`; caller guarantees `data..data+len` valid.
        let byte = unsafe { *data.add(byte_index) };
        let table_index = ((crc ^ (byte as u32)) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32C_TABLE[table_index];
        byte_index += 1;
    }
    !crc
}

/// Hardware CRC-32C via SSE4.2 `CRC32` instruction.
///
/// Compiled only for `x86_64`. `#[target_feature]` scopes the SSE4.2
/// permission to this function only — neighbouring functions are unaffected,
/// unlike a translation-unit-wide `-msse4.2` flag.
///
/// # Safety
/// (a) `data..data+len` must be a valid readable byte range.
/// (b) The CPU must support SSE4.2. `init()` verifies this with
///     `is_x86_feature_detected!` before storing this function's address.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn crc32c_hw(data: *const u8, len: usize) -> u32 {
    let mut crc: u64 = 0xFFFF_FFFF;
    let mut cursor = data;
    // SAFETY: `data` is valid per contract; `data.add(len & !7) <= data.add(len)`.
    let end_of_aligned_chunks = unsafe { data.add(len & !7) };
    let end_of_buffer = unsafe { data.add(len) };

    while cursor < end_of_aligned_chunks {
        // SAFETY: `cursor < end_of_aligned_chunks <= data.add(len)`; the 8-byte
        // read is within the valid range. `read_unaligned` handles non-8-aligned data.
        crc = unsafe {
            _mm_crc32_u64(crc, std::ptr::read_unaligned(cursor as *const u64))
        };
        // SAFETY: `cursor < end_of_aligned_chunks`; advance by 8 stays in range.
        cursor = unsafe { cursor.add(8) };
    }

    while cursor < end_of_buffer {
        // SAFETY: `cursor < end_of_buffer = data.add(len)`; single byte is in range.
        crc = unsafe { _mm_crc32_u8(crc as u32, *cursor) as u64 };
        // SAFETY: `cursor < end_of_buffer`; advance by 1 stays in range.
        cursor = unsafe { cursor.add(1) };
    }

    !(crc as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_empty() {
        unsafe {
            let data: [u8; 0] = [];
            let crc = crc32c(data.as_ptr(), 0);
            assert_eq!(crc, 0x0000_0000);
        }
    }

    #[test]
    fn crc32c_deterministic() {
        unsafe {
            let data = [1u8, 2, 3, 4, 5, 6, 7, 8];
            let crc_first_call = crc32c(data.as_ptr(), data.len());
            let crc_second_call = crc32c(data.as_ptr(), data.len());
            assert_eq!(crc_first_call, crc_second_call);
        }
    }

    #[test]
    fn crc32c_different_data_different_crc() {
        unsafe {
            let data_all_ones = [1u8; 16];
            let data_all_twos = [2u8; 16];
            let crc_ones = crc32c(data_all_ones.as_ptr(), data_all_ones.len());
            let crc_twos = crc32c(data_all_twos.as_ptr(), data_all_twos.len());
            assert_ne!(crc_ones, crc_twos);
        }
    }

    #[test]
    fn crc32c_detects_single_bit_flip() {
        unsafe {
            let mut data = [0u8; 64];
            data[32] = 0xFF;
            let crc_original = crc32c(data.as_ptr(), data.len());

            data[32] = 0xFE;
            let crc_flipped = crc32c(data.as_ptr(), data.len());

            assert_ne!(crc_original, crc_flipped);
        }
    }

    #[test]
    fn crc32c_known_value() {
        unsafe {
            let data = b"123456789";
            let crc = crc32c(data.as_ptr(), data.len());
            assert_eq!(crc, 0xE306_9283);
        }
    }

    #[test]
    fn crc32c_128_bytes() {
        unsafe {
            let data = [0xABu8; 128];
            let crc_first = crc32c(data.as_ptr(), data.len());
            let crc_second = crc32c(data.as_ptr(), data.len());
            assert_ne!(crc_first, 0);
            assert_eq!(crc_first, crc_second);
        }
    }

    #[test]
    fn crc32c_software_path_known_value() {
        unsafe {
            let data = b"123456789";
            let crc = crc32c_sw(data.as_ptr(), data.len());
            assert_eq!(crc, 0xE306_9283);
        }
    }

    #[test]
    #[cfg(target_arch = "x86_64")]
    fn crc32c_software_and_hardware_paths_agree() {
        if !is_x86_feature_detected!("sse4.2") {
            return;
        }

        for input in [b"" as &[u8], b"A", b"12345678", &[0u8; 64], &[0xABu8; 128], b"hello, world!"] {
            unsafe {
                let crc_sw = crc32c_sw(input.as_ptr(), input.len());
                // SAFETY: `is_x86_feature_detected!("sse4.2")` returned true;
                // `input` is a valid readable slice.
                let crc_hw = crc32c_hw(input.as_ptr(), input.len());
                assert_eq!(
                    crc_sw,
                    crc_hw,
                    "sw/hw CRC32C mismatch for input of len {}",
                    input.len()
                );
            }
        }
    }

    #[test]
    fn crc32c_init_is_idempotent() {
        init();
        let crc_after_first_init = unsafe {
            let data = b"idempotency check";
            crc32c(data.as_ptr(), data.len())
        };

        init();
        let crc_after_second_init = unsafe {
            let data = b"idempotency check";
            crc32c(data.as_ptr(), data.len())
        };

        assert_eq!(crc_after_first_init, crc_after_second_init);
    }

    #[test]
    fn crc32c_dispatcher_correct_after_init() {
        init();
        unsafe {
            let data = b"123456789";
            let crc = crc32c(data.as_ptr(), data.len());
            assert_eq!(crc, 0xE306_9283);
        }
    }

    #[test]
    #[cfg(miri)]
    fn crc32c_software_path_correct_under_miri() {
        unsafe {
            let data = b"123456789";
            let crc = crc32c_sw(data.as_ptr(), data.len());
            assert_eq!(crc, 0xE306_9283);
        }
    }
}