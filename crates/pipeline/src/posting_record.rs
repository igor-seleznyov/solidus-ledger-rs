use common::crc32c::crc32c;

#[repr(C, align(64))]
#[derive(Copy, Clone)]
pub struct PostingRecord {
    // ═══ Cache line 0: identity + amounts ═══
    pub transfer_id_hi: u64,
    pub transfer_id_lo: u64,
    pub account_id_hi: u64,
    pub account_id_lo: u64,
    pub gsn: u64,
    pub ordinal: u64,
    pub amount: i64,
    pub prev_posting_record_offset: u64,

    // ═══ Cache line 1: timestamps + metadata + checksum ═══
    pub timestamp_ns: u64,
    pub transfer_sequence_id: [u8; 16],
    pub currency: [u8; 16],
    pub entry_type: u8,
    pub sign: i8,
    pub transfer_posting_records_count: u8,
    pub _pad: [u8; 17],
    pub checksum: u32,
}

impl PostingRecord {
    pub const SIZE: usize = 128;

    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }

    pub unsafe fn compute_checksum(&mut self) {
        let ptr = self as *const PostingRecord as *const u8;
        self.checksum = unsafe { crc32c(ptr, 124) };
    }

    pub unsafe fn verify_checksum(&self) -> bool {
        let ptr = self as *const PostingRecord as *const u8;
        let computed = unsafe { crc32c(ptr, 124) };
        computed == self.checksum
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_128_bytes() {
        assert_eq!(std::mem::size_of::<PostingRecord>(), 128);
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
        assert_eq!(std::mem::offset_of!(PostingRecord, checksum), 124);
    }

    #[test]
    fn field_offsets() {
        assert_eq!(std::mem::offset_of!(PostingRecord, transfer_id_hi), 0);
        assert_eq!(std::mem::offset_of!(PostingRecord, transfer_id_lo), 8);
        assert_eq!(std::mem::offset_of!(PostingRecord, account_id_hi), 16);
        assert_eq!(std::mem::offset_of!(PostingRecord, account_id_lo), 24);
        assert_eq!(std::mem::offset_of!(PostingRecord, gsn), 32);
        assert_eq!(std::mem::offset_of!(PostingRecord, ordinal), 40);
        assert_eq!(std::mem::offset_of!(PostingRecord, amount), 48);
        assert_eq!(std::mem::offset_of!(PostingRecord, prev_posting_record_offset), 56);
        assert_eq!(std::mem::offset_of!(PostingRecord, timestamp_ns), 64);
        assert_eq!(std::mem::offset_of!(PostingRecord, transfer_sequence_id), 72);
        assert_eq!(std::mem::offset_of!(PostingRecord, currency), 88);
        assert_eq!(std::mem::offset_of!(PostingRecord, entry_type), 104);
        assert_eq!(std::mem::offset_of!(PostingRecord, sign), 105);
        assert_eq!(std::mem::offset_of!(PostingRecord, transfer_posting_records_count), 106);
        assert_eq!(std::mem::offset_of!(PostingRecord, checksum), 124);
    }

    #[test]
    fn compute_and_verify_checksum() {
        let mut record = PostingRecord::zeroed();
        record.transfer_id_hi = 1;
        record.transfer_id_lo = 2;
        record.gsn = 100;
        record.amount = 500;

        unsafe {
            record.compute_checksum();
            assert_ne!(record.checksum, 0);
            assert!(record.verify_checksum());
        }
    }

    #[test]
    fn corrupted_data_fails_checksum() {
        let mut record = PostingRecord::zeroed();
        record.gsn = 42;
        record.amount = 1000;

        unsafe {
            record.compute_checksum();
            assert!(record.verify_checksum());

            record.amount = 999;
            assert!(!record.verify_checksum());
        }
    }

    #[test]
    fn zeroed_is_valid() {
        let mut record = PostingRecord::zeroed();
        unsafe {
            record.compute_checksum();
            assert!(record.verify_checksum());
        }
    }
}