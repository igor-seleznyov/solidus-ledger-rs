use common::crc32c::crc32c;

pub const POSTING_RECORD_MAGIC: u64 = 0x5254_5350_5453_444C;

#[repr(C, align(64))]
#[derive(Copy, Clone)]
pub struct PostingRecord {
    pub magic: u64,
    pub transfer_id_hi: u64,
    pub transfer_id_lo: u64,
    pub account_id_hi: u64,
    pub account_id_lo: u64,
    pub gsn: u64,
    pub amount: i64,
    pub ordinal: u64,

    pub prev_posting_record_offset: u64,
    pub timestamp_ns: u64,
    pub transfer_sequence_id: [u8; 16],
    pub currency: [u8; 16],
    pub entry_type: u8,
    pub sign: i8,
    pub transfer_posting_records_count: u8,
    pub _pad: [u8; 9],
    pub checksum: u32,
}

impl PostingRecord {
    pub const SIZE: usize = 128;

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
            entry_type: 0,
            sign: 0,
            transfer_posting_records_count: 0,
            _pad: [0u8; 9],
            checksum: 0,
        }
    }

    pub unsafe fn compute_checksum(&mut self) {
        self.checksum = 0;
        let ptr = self as *const PostingRecord as *const u8;
        self.checksum = unsafe { crc32c(ptr, Self::SIZE) };
    }

    pub fn set_magic(&mut self) {
        self.magic = POSTING_RECORD_MAGIC;
    }

    pub fn verify_magic(&self) -> bool {
        self.magic == POSTING_RECORD_MAGIC
    }

    pub unsafe fn verify_checksum(&mut self) -> bool {
        let ptr = self as *const PostingRecord as *const u8;
        let checksum_value = self.checksum;
        self.checksum = 0;
        let computed = unsafe { crc32c(ptr, Self::SIZE) };
        self.checksum = checksum_value;
        computed == self.checksum
    }
}

#[cfg(test)]
#[cfg(not(miri))]
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
        assert_eq!(std::mem::offset_of!(PostingRecord, entry_type), 112);
        assert_eq!(std::mem::offset_of!(PostingRecord, sign), 113);
        assert_eq!(std::mem::offset_of!(PostingRecord, transfer_posting_records_count), 114);
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