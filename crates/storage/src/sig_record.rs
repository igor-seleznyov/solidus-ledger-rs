pub const SIG_RECORD_MAGIC: u64 = 0x524E_4753_5453_444C;

#[repr(C, align(64))]
#[derive(Copy, Clone)]
pub struct SigRecord {
    pub magic: u64,
    pub transfer_id_hi: u64,
    pub transfer_id_lo: u64,
    pub gsn: u64,
    pub ls_offset: u64,
    pub timestamp_ns: u64,
    pub postings_count: u8,
    pub algorithm: u8,
    pub key_version: u16,
    pub checksum: u32,
    pub batch_seq: u64,

    pub prev_tx_hash: [u8; 32],
    pub postings_hash: [u8; 32],

    pub signature: [u8; 64],
}

impl SigRecord {
    pub const SIZE: usize = 192;

    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }

    pub unsafe fn compute_checksum(&mut self) {
        self.checksum = 0;
        let ptr = self as *const SigRecord as *const u8;
        self.checksum = common::crc32c::crc32c(ptr, Self::SIZE);
    }

    pub unsafe fn verify_checksum(&self) -> bool {
        let saved = self.checksum;
        let self_mut = self as *const SigRecord as *mut SigRecord;
        unsafe {
            (*self_mut).checksum = 0;
        }
        let ptr = self as *const SigRecord as *const u8;
        let computed = common::crc32c::crc32c(ptr, Self::SIZE);
        unsafe {
            (*self_mut).checksum = saved;
        }
        computed == saved
    }

    pub fn set_magic(&mut self) {
        self.magic = SIG_RECORD_MAGIC;
    }

    pub fn verify_magic(&self) -> bool {
        self.magic == SIG_RECORD_MAGIC
    }

    pub unsafe fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const SigRecord as *const u8,
                Self::SIZE,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_192_bytes() {
        assert_eq!(size_of::<SigRecord>(), 192);
    }

    #[test]
    fn alignment_is_64() {
        assert_eq!(align_of::<SigRecord>(), 64);
    }

    #[test]
    fn field_offsets() {
        assert_eq!(std::mem::offset_of!(SigRecord, magic), 0);
        assert_eq!(std::mem::offset_of!(SigRecord, transfer_id_hi), 8);
        assert_eq!(std::mem::offset_of!(SigRecord, transfer_id_lo), 16);
        assert_eq!(std::mem::offset_of!(SigRecord, gsn), 24);
        assert_eq!(std::mem::offset_of!(SigRecord, ls_offset), 32);
        assert_eq!(std::mem::offset_of!(SigRecord, timestamp_ns), 40);
        assert_eq!(std::mem::offset_of!(SigRecord, postings_count), 48);
        assert_eq!(std::mem::offset_of!(SigRecord, algorithm), 49);
        assert_eq!(std::mem::offset_of!(SigRecord, key_version), 50);
        assert_eq!(std::mem::offset_of!(SigRecord, checksum), 52);
        assert_eq!(std::mem::offset_of!(SigRecord, batch_seq), 56);
        assert_eq!(std::mem::offset_of!(SigRecord, prev_tx_hash), 64);
        assert_eq!(std::mem::offset_of!(SigRecord, postings_hash), 96);
        assert_eq!(std::mem::offset_of!(SigRecord, signature), 128);
    }
    
    #[test]
    fn checksum_compute_and_verify() {
        let mut record = SigRecord::zeroed();
        record.transfer_id_hi = 1;
        record.gsn = 100;

        unsafe {
            record.compute_checksum();
            assert_ne!(record.checksum, 0);
            assert!(record.verify_checksum());
        }
    }

    #[test]
    fn corrupted_sig_record_fails_checksum() {
        let mut record = SigRecord::zeroed();
        record.gsn = 42;

        unsafe {
            record.compute_checksum();
            record.gsn = 43;
            assert!(!record.verify_checksum());
        }
    }
}