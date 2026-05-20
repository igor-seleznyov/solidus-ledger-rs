use std::mem;
use common::crc32c::crc32c;

// 'LDSTPSTR' little-endian LeDgerSTorage PoSTing Record
pub const POSTING_RECORD_MAGIC: u64 = 0x5254_5350_5453_444C;

/// Posting written to the LS file. 128 B, 2 cache lines.
///
/// # Layout invariant (I-001)
///
/// `checksum` MUST remain the last field at offset 124 = SIZE - 4.
/// `compute_checksum` reads bytes `[0..124)` as a single contiguous slice,
/// no mutation of `self` required. Safe to call on PROT_READ mmap.
///
/// Future fields MUST go before `checksum` and eat into the reserved budget.
/// When the reserved budget runs out, bump `LS_FORMAT_VERSION` in `LsFileHeader`
/// and add version dispatch in the scanner.
#[repr(C, align(64))]
#[derive(Copy, Clone)]
pub struct PostingRecord {
    // ═══ Cache line 0: identity + amounts ═══
    pub magic: u64,
    pub transfer_id_hi: u64,
    pub transfer_id_lo: u64,
    pub account_id_hi: u64,
    pub account_id_lo: u64,
    pub gsn: u64,
    pub amount: i64,
    pub ordinal: u64,

    // ═══ Cache line 1: timestamps + metadata + checksum ═══
    pub prev_posting_record_offset: u64,
    pub timestamp_ns: u64,
    pub transfer_sequence_id: [u8; 16],
    pub currency: [u8; 16],

    pub partition_seq: u64,
    pub entry_type: u8,
    pub sign: i8,
    pub transfer_posting_records_count: u8,
    pub _pad: [u8; 1],
    pub checksum: u32,
}

impl PostingRecord {
    pub const SIZE: usize = std::mem::size_of::<PostingRecord>();

    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }
    
    pub fn new(
        transfer_id_hi: u64,
        transfer_id_lo: u64,
        gsn: u64,
    ) -> Self {
        Self {
            magic: POSTING_RECORD_MAGIC,
            transfer_id_hi,
            transfer_id_lo,
            gsn,
            account_id_hi: 0,
            account_id_lo: 0,
            amount: 0,
            ordinal: 0,
            prev_posting_record_offset: 0,
            timestamp_ns: 0,
            transfer_sequence_id: [0u8; 16],
            currency: [0u8; 16],
            partition_seq: 0,
            entry_type: 0,
            sign: 0,
            transfer_posting_records_count: 0,
            _pad: [0u8; 1],
            checksum: 0,
        }
    }

    pub fn compute_checksum(&self) -> u32 {
        // SAFETY: `self` is a valid `PostingRecord` of exactly `SIZE` bytes.
        // Reading `[0..SIZE - 4)` excludes only the trailing `checksum: u32`
        // field. The struct is `Copy`, so no drop glue runs concurrently.
        // No aliasing violation: we hold `&self`, not `&mut self`.
        const PAYLOAD: usize = PostingRecord::SIZE - std::mem::size_of::<u32>();
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                PAYLOAD,
            )
        };
        unsafe {
            crc32c(bytes.as_ptr(), bytes.len())
        }
    }

    pub fn fill_checksum(&mut self) {
        self.checksum = self.compute_checksum();
    }

    pub fn verify_checksum(&self) -> bool {
        self.checksum == self.compute_checksum()
    }

    pub fn set_magic(&mut self) {
        self.magic = POSTING_RECORD_MAGIC;
    }

    pub fn verify_magic(&self) -> bool {
        self.magic == POSTING_RECORD_MAGIC
    }
}

/// Compile-time assertion for unexpected field add after checksum
const _: () = assert!(
    std::mem::offset_of!(PostingRecord, checksum) == PostingRecord::SIZE - std::mem::size_of::<u32>()
);

#[cfg(test)]
#[cfg(not(miri))]
mod tests {
    use super::*;

    #[test]
    fn size_is_128_bytes() {
        assert_eq!(std::mem::size_of::<PostingRecord>(), 128);
        assert_eq!(PostingRecord::SIZE, 128);
    }

    #[test]
    fn alignment_is_64() {
        assert_eq!(std::mem::align_of::<PostingRecord>(), 64);
    }

    #[test]
    fn identity_fields_in_first_cache_line() {
        assert!(std::mem::offset_of!(PostingRecord, transfer_id_hi) < 64);
        assert!(std::mem::offset_of!(PostingRecord, account_id_hi) < 64);
        assert!(std::mem::offset_of!(PostingRecord, gsn) < 64);
        assert!(std::mem::offset_of!(PostingRecord, ordinal) < 64);
        assert!(std::mem::offset_of!(PostingRecord, amount) < 64);
    }

    #[test]
    fn checksum_at_end() {
        assert_eq!(
              std::mem::offset_of!(PostingRecord, checksum),
              PostingRecord::SIZE - std::mem::size_of::<u32>(),
          );
    }

    #[test]
    fn field_offsets() {
        assert_eq!(std::mem::offset_of!(PostingRecord, magic), 0);
        assert_eq!(std::mem::offset_of!(PostingRecord, transfer_id_hi), 8);
        assert_eq!(std::mem::offset_of!(PostingRecord, transfer_id_lo), 16);
        assert_eq!(std::mem::offset_of!(PostingRecord, account_id_hi), 24);
        assert_eq!(std::mem::offset_of!(PostingRecord, account_id_lo), 32);
        assert_eq!(std::mem::offset_of!(PostingRecord, gsn), 40);
        assert_eq!(std::mem::offset_of!(PostingRecord, amount), 48);
        assert_eq!(std::mem::offset_of!(PostingRecord, ordinal), 56);
        assert_eq!(std::mem::offset_of!(PostingRecord, prev_posting_record_offset), 64);
        assert_eq!(std::mem::offset_of!(PostingRecord, timestamp_ns), 72);
        assert_eq!(std::mem::offset_of!(PostingRecord, transfer_sequence_id), 80);
        assert_eq!(std::mem::offset_of!(PostingRecord, currency), 96);
        assert_eq!(std::mem::offset_of!(PostingRecord, partition_seq), 112);
        assert_eq!(std::mem::offset_of!(PostingRecord, entry_type), 120);
        assert_eq!(std::mem::offset_of!(PostingRecord, sign), 121);
        assert_eq!(std::mem::offset_of!(PostingRecord, transfer_posting_records_count), 122);
        assert_eq!(std::mem::offset_of!(PostingRecord, _pad), 123);
        assert_eq!(std::mem::offset_of!(PostingRecord, checksum), 124);
    }

    #[test]
    fn magic_word() {
        let mut record = PostingRecord::zeroed();
        assert!(!record.verify_magic());

        record.set_magic();
        assert!(record.verify_magic());
        assert_eq!(record.magic, POSTING_RECORD_MAGIC);
    }

    #[test]
    fn fill_then_verify_succeeds() {
        let mut record = PostingRecord::zeroed();
        record.transfer_id_hi = 1;
        record.transfer_id_lo = 2;
        record.gsn = 100;
        record.amount = 500;

        record.fill_checksum();

        assert_ne!(record.checksum, 0);
        assert!(record.verify_checksum());
    }

    #[test]
    fn corrupted_payload_fails_verify() {
        let mut record = PostingRecord::zeroed();
        record.gsn = 42;
        record.amount = 1000;
        record.fill_checksum();
        assert!(record.verify_checksum());

        record.amount = 999;
        assert!(!record.verify_checksum());
    }

    #[test]
    fn compute_checksum_is_pure() {
        let mut record = PostingRecord::zeroed();
        record.gsn = 7;
        record.amount = 42;

        let first = record.compute_checksum();
        let second = record.compute_checksum();

        assert_eq!(first, second);
        assert_eq!(record.checksum, 0);
    }

    #[test]
    fn zeroed_then_filled_verifies() {
        let mut record = PostingRecord::zeroed();
        record.fill_checksum();
        assert!(record.verify_checksum());
    }

    #[test]
    fn refill_after_modification_verifies() {
        let mut record = PostingRecord::zeroed();
        record.gsn = 1;
        record.fill_checksum();
        let first_checksum = record.checksum;

        record.gsn = 2;
        record.fill_checksum();

        assert_ne!(record.checksum, first_checksum);
        assert!(record.verify_checksum());
    }
}
