use common::crc32c::crc32c;

pub const SIG_RECORD_MAGIC: u64 = 0x524E_4753_5453_444C;

/// Ed25519 signature record, one per flushed batch. 256 B (4 cache lines).
///
/// # Layout invariant (I-001)
///
/// `checksum` MUST remain the last field at offset 252 = SIZE - 4.
/// Future fields MUST go before `checksum` and eat into `_pad2`
/// (60 B available, covering the I-023 SigRecord reservation).
/// When `_pad2` is exhausted, bump `LS_SIGN_FORMAT_VERSION` in
/// `LsSignFileHeader` and add version dispatch in the scanner.
#[repr(C, align(64))]
#[derive(Copy, Clone)]
pub struct SigRecord {
    pub magic: u64,
    pub transfer_id_hi: u64,
    pub transfer_id_lo: u64,
    pub gsn: u64,
    pub ls_offset: u64,
    pub timestamp_ns: u64,
    pub batch_seq: u64,
    pub postings_count: u8,
    pub algorithm: u8,
    pub key_version: u16,
    pub _pad1: [u8; 4],

    pub prev_tx_hash: [u8; 32],
    pub postings_hash: [u8; 32],
    pub signature: [u8; 64],

    pub _pad2: [u8; 60],
    pub checksum: u32,
}
const _: () = assert!(
    std::mem::size_of::<SigRecord>()
        == size_of::<u64>() * 7
                + size_of::<u8>() * 2
                + size_of::<u16>()
                + size_of::<[u8; 4]>()
                + size_of::<[u8; 32]>() * 2
                + size_of::<[u8; 64]>()
                + size_of::<[u8; 60]>()
                + size_of::<u32>(),
    "SigRecord is larger than its fields: the compiler inserted alignment \
     padding. Declare it as an explicit field so the layout is stated, and \
     so the checksum stays the record's final bytes",
);


impl SigRecord {
    pub const SIZE: usize = 256;

    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }

    /// Compute CRC32C over bytes `[0..SIZE - 4)`, excluding `checksum`.
    /// Safe to call on PROT_READ mmap (no mutation of `self`).
    ///
    /// # Safety (internal)
    ///
    /// The single `unsafe` block builds a `[0..SIZE - 4)` byte view over
    /// `self`. Valid because `self` is a live `&SigRecord` of exactly
    /// `SIZE` bytes; the view excludes the trailing `checksum`; no
    /// mutation occurs, so it is sound on read-only memory.
    pub fn compute_checksum(&self) -> u32 {
        const PAYLOAD: usize = SigRecord::SIZE - std::mem::size_of::<u32>();
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
        self.magic = SIG_RECORD_MAGIC;
    }

    pub fn verify_magic(&self) -> bool {
        self.magic == SIG_RECORD_MAGIC
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const SigRecord as *const u8,
                Self::SIZE,
            )
        }
    }
}

const _: () = assert!(
    std::mem::offset_of!(SigRecord, checksum) == SigRecord::SIZE - std::mem::size_of::<u32>()
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_256_bytes() {
        assert_eq!(size_of::<SigRecord>(), 256);
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
        assert_eq!(std::mem::offset_of!(SigRecord, batch_seq), 48);
        assert_eq!(std::mem::offset_of!(SigRecord, postings_count), 56);
        assert_eq!(std::mem::offset_of!(SigRecord, algorithm), 57);
        assert_eq!(std::mem::offset_of!(SigRecord, key_version), 58);
        assert_eq!(std::mem::offset_of!(SigRecord, _pad1), 60);
        assert_eq!(std::mem::offset_of!(SigRecord, prev_tx_hash), 64);
        assert_eq!(std::mem::offset_of!(SigRecord, postings_hash), 96);
        assert_eq!(std::mem::offset_of!(SigRecord, signature), 128);
        assert_eq!(std::mem::offset_of!(SigRecord, _pad2), 192);
        assert_eq!(std::mem::offset_of!(SigRecord, checksum), 252);
    }

    #[test]
    fn checksum_is_last_field() {
        assert_eq!(
            std::mem::offset_of!(SigRecord, checksum),
            SigRecord::SIZE - 4,
        );
    }

    #[test]
    fn checksum_compute_then_verify() {
        let mut record = SigRecord::zeroed();
        record.transfer_id_hi = 1;
        record.gsn = 100;
        record.fill_checksum();
        assert_ne!(record.checksum, 0);
        assert!(record.verify_checksum());
    }

    #[test]
    fn corrupted_sig_record_fails_checksum() {
        let mut record = SigRecord::zeroed();
        record.gsn = 42;
        record.fill_checksum();
        record.gsn = 43;
        assert!(!record.verify_checksum());
    }

    #[test]
    fn verify_accepts_shared_reference() {
        let mut record = SigRecord::zeroed();
        record.fill_checksum();
        let shared: &SigRecord = &record;
        assert!(shared.verify_checksum());
    }
}
