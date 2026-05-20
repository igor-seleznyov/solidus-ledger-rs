use common::crc32c::crc32c;
use crate::consts::FILE_PAGE_SIZE;
use std::time::{SystemTime, UNIX_EPOCH};
use pipeline::posting_record::PostingRecord;

pub const LS_FILE_MAGIC: u64 = 0x4752_5453_5247_444C;
pub const LS_FORMAT_VERSION: u16 = 1;
pub const LS_FILE_TYPE_LS: u8 = 0;

#[repr(C)]
pub struct LsFileHeader {
    pub magic: u64,
    pub format_version: u16,
    pub file_type: u8,
    pub signing_enabled: u8,
    pub partition_count: u16,
    pub metadata_enabled: u8,
    pub _pad1: u8,
    pub created_at_ns: u64,
    pub record_size: u32,
    pub data_offset: u32,
    pub max_file_size: u64,
    pub file_seq: u64,
    pub rules_count: u16,
    pub _pad2: [u8; 2],
    pub rules_checksum: u32,
    pub public_key_hash: u32,
    /// Phase 3 Merkle root over the LS file's PostingRecords, computed at
    /// rotation by Index Builder per ADR-029 (RFC 6962-style SHA-256 tree).
    /// All-zero = pre-Phase-3 (or rotation-not-yet-completed) sentinel;
    /// canonical non-zero value set only by the Phase 3 Merkle activation
    /// step. Anchored in three places: this LS file header, the Manifest
    /// entry for this file, and the Meta Chain `RotationRecord`.
    ///
    /// Named-typed-field naming form per I-023 §"Discipline rules"
    /// §"Named-typed-field naming form (clarification 2026-05-10)".
    ///
    /// **For defence:** declared as `[u8; 32]` at offset 60 rather than
    /// keeping `_reserved: [u8; 64]` because ADR-029 is accepted (design),
    /// the field type and size are exact, and declaring it now eliminates a
    /// rename-on-activation edit while preserving SIZE, `checksum` offset,
    /// and CRC formula unchanged at Phase 3 activation.
    pub merkle_root: [u8; 32],
    /// Genuine Phase 3 future-budget (no field designed yet). When this
    /// reservation runs out, format-version bump applies per I-001's
    /// tail-extension rule.
    pub _pad3: [u8; 32],
    /// CRC32C over bytes `[0..SIZE - 4)`. MUST remain the last field (I-001).
    pub checksum: u32,
}

impl LsFileHeader {
    pub const SIZE: usize = 128;
    pub const DATA_OFFSET: u32 = 4096;

    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }

    pub fn new(
        signing_enabled: bool,
        partition_count: u16,
        max_file_size: u64,
        file_seq: u64,
        public_key_hash: u32,
        metadata_enabled: bool,
    ) -> Self {
        let mut header = Self::zeroed();
        header.magic = LS_FILE_MAGIC;
        header.format_version = LS_FORMAT_VERSION;
        header.file_type = LS_FILE_TYPE_LS;
        header.signing_enabled = if signing_enabled { 1 } else { 0 };
        header.partition_count = partition_count;
        header.created_at_ns = Self::now_nanos();
        header.record_size = pipeline::posting_record::PostingRecord::SIZE as u32;
        header.data_offset = Self::DATA_OFFSET;
        header.max_file_size = max_file_size;
        header.file_seq = file_seq;
        header.rules_count = 0;
        header.rules_checksum = 0;
        header.public_key_hash = public_key_hash;
        header.metadata_enabled = if metadata_enabled { 1 } else { 0 };

        header.fill_checksum();

        header
    }

    pub fn fill_checksum(&mut self) {
        self.checksum = self.compute_checksum();
    }

    pub fn verify_checksum(&self) -> bool {
        self.checksum == self.compute_checksum()
    }

    pub fn compute_checksum(&self) -> u32 {
        // SAFETY: `self` is a valid `LsFileHeader` of exactly `SIZE` bytes.
        // Bytes `[0..SIZE-4)` exclude only the trailing `checksum: u32`.
        const PAYLOAD: usize = LsFileHeader::SIZE - std::mem::size_of::<u32>();
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                PAYLOAD,
            )
        };
        unsafe { crc32c(bytes.as_ptr(), bytes.len()) }
    }

    pub fn to_page(&self) -> [u8; FILE_PAGE_SIZE] {
        let mut page = [0u8; FILE_PAGE_SIZE];
        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const LsFileHeader as *const u8,
                Self::SIZE,
            )
        };
        page[..Self::SIZE].copy_from_slice(header_bytes);
        page
    }

    pub unsafe fn from_bytes(bytes: &[u8]) -> &LsFileHeader {
        assert!(bytes.len() >= Self::SIZE);
        unsafe { &*(bytes.as_ptr() as *const LsFileHeader) }
    }

    fn now_nanos() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }
}

/// Compile-time assertion for check no any fields added after checksum
const _: () = assert!(
    std::mem::offset_of!(LsFileHeader, checksum) == LsFileHeader::SIZE - std::mem::size_of::<u32>()
);


#[cfg(test)]
#[cfg(not(miri))]
mod tests {
    use super::*;

    #[test]
    fn size_is_128_bytes() {
        assert_eq!(std::mem::size_of::<LsFileHeader>(), LsFileHeader::SIZE);
        assert_eq!(LsFileHeader::SIZE, 128);
    }

    #[test]
    fn field_offsets() {
        assert_eq!(std::mem::offset_of!(LsFileHeader, magic), 0);
        assert_eq!(std::mem::offset_of!(LsFileHeader, format_version), 8);
        assert_eq!(std::mem::offset_of!(LsFileHeader, file_type), 10);
        assert_eq!(std::mem::offset_of!(LsFileHeader, signing_enabled), 11);
        assert_eq!(std::mem::offset_of!(LsFileHeader, partition_count), 12);
        assert_eq!(std::mem::offset_of!(LsFileHeader, metadata_enabled), 14);
        assert_eq!(std::mem::offset_of!(LsFileHeader, _pad1), 15);
        assert_eq!(std::mem::offset_of!(LsFileHeader, created_at_ns), 16);
        assert_eq!(std::mem::offset_of!(LsFileHeader, record_size), 24);
        assert_eq!(std::mem::offset_of!(LsFileHeader, data_offset), 28);
        assert_eq!(std::mem::offset_of!(LsFileHeader, max_file_size), 32);
        assert_eq!(std::mem::offset_of!(LsFileHeader, file_seq), 40);
        assert_eq!(std::mem::offset_of!(LsFileHeader, rules_count), 48);
        assert_eq!(std::mem::offset_of!(LsFileHeader, _pad2), 50);
        assert_eq!(std::mem::offset_of!(LsFileHeader, rules_checksum), 52);
        assert_eq!(std::mem::offset_of!(LsFileHeader, public_key_hash), 56);
        assert_eq!(std::mem::offset_of!(LsFileHeader, merkle_root), 60);
        assert_eq!(std::mem::offset_of!(LsFileHeader, _pad3), 92);
        assert_eq!(std::mem::offset_of!(LsFileHeader, checksum), 124);
    }

    #[test]
    fn checksum_at_end() {
        assert_eq!(
            std::mem::offset_of!(LsFileHeader, checksum),
            LsFileHeader::SIZE - std::mem::size_of::<u32>()
        );
    }

    #[test]
    fn compute_and_verify_checksum() {
        let mut header = LsFileHeader::new(true, 4, 1 << 30, 42, 0xDEAD_BEEF, false);
        assert!(header.verify_checksum());
        assert_ne!(header.checksum, 0);
    }

    #[test]
    fn corrupted_data_fails_checksum() {
        let mut header = LsFileHeader::new(true, 4, 1 << 30, 42, 0xDEAD_BEEF, false);
        assert!(header.verify_checksum());

        header.file_seq = 999;
        assert!(!header.verify_checksum());
    }

    #[test]
    fn zeroed_then_filled_is_valid() {
        let mut header = LsFileHeader::zeroed();
        header.fill_checksum();
        assert!(header.verify_checksum());
    }
}
