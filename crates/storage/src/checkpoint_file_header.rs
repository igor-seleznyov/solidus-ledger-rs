use common::crc32c::crc32c;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::checkpoint_record::CheckpointRecord;

pub const CHECKPOINT_FILE_MAGIC: u64 = 0x5450_4B43_5453_444C;

#[repr(C)]
pub struct CheckpointFileHeader {
    pub magic: u64,
    pub format_version: u16,
    pub _pad: [u8; 2],
    pub record_size: u32,
    pub created_at_ns: u64,
    pub linked_ls_file_seq: u64,
    pub data_offset: u32,
    pub checksum: u32,
}
const _: () = assert!(
    std::mem::size_of::<CheckpointFileHeader>()
        == size_of::<u64>() * 3
                + size_of::<u16>()
                + size_of::<[u8; 2]>()
                + size_of::<u32>() * 3,
    "CheckpointFileHeader is larger than its fields: the compiler inserted alignment \
     padding. Declare it as an explicit field so the layout is stated, and \
     so the checksum stays the record's final bytes",
);


impl CheckpointFileHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();
    pub const DATA_OFFSET: u32 = Self::SIZE as u32;

    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }

    pub fn new(linked_ls_file_seq: u64) -> Self {
        let created_at_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let mut header = Self {
            magic: CHECKPOINT_FILE_MAGIC,
            format_version: 1,
            _pad: [0; 2],
            record_size: CheckpointRecord::SIZE as u32,
            created_at_ns,
            linked_ls_file_seq,
            data_offset: Self::DATA_OFFSET,
            checksum: 0,
        };

        header.fill_checksum();
        header
    }

    /// CRC over `[0..SIZE - 4)`, excluding the trailing `checksum` field by range.
    pub fn compute_checksum(&self) -> u32 {
        const PAYLOAD: usize = CheckpointFileHeader::SIZE - std::mem::size_of::<u32>();
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

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const CheckpointFileHeader as *const u8,
                Self::SIZE,
            )
        }
    }

    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self as *mut CheckpointFileHeader as *mut u8,
                Self::SIZE,
            )
        }
    }
}

const _: () = assert!(
    std::mem::offset_of!(CheckpointFileHeader, checksum) == CheckpointFileHeader::SIZE - std::mem::size_of::<u32>()
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_size() {
        assert_eq!(CheckpointFileHeader::SIZE, 40);
        assert_eq!(std::mem::size_of::<CheckpointFileHeader>(), 40);
    }

    #[test]
    fn layout_offsets() {
        assert_eq!(std::mem::offset_of!(CheckpointFileHeader, magic), 0);
        assert_eq!(std::mem::offset_of!(CheckpointFileHeader, format_version), 8);
        assert_eq!(std::mem::offset_of!(CheckpointFileHeader, record_size), 12);
        assert_eq!(std::mem::offset_of!(CheckpointFileHeader, created_at_ns), 16);
        assert_eq!(std::mem::offset_of!(CheckpointFileHeader, linked_ls_file_seq), 24);
        assert_eq!(std::mem::offset_of!(CheckpointFileHeader, data_offset), 32);
        assert_eq!(std::mem::offset_of!(CheckpointFileHeader, checksum), 36);
    }

    #[test]
    fn new_computes_checksum() {
        let header = CheckpointFileHeader::new(0);
        assert_ne!(header.checksum, 0);
        assert!(header.verify_checksum());
    }

    #[test]
    fn verify_checksum_detects_corruption() {
        let mut header = CheckpointFileHeader::new(0);
        header.linked_ls_file_seq = 999;
        assert!(!header.verify_checksum());
    }

    #[test]
    fn magic_correct() {
        let header = CheckpointFileHeader::new(0);
        assert_eq!(header.magic, CHECKPOINT_FILE_MAGIC);
    }

    #[test]
    fn data_offset_correct() {
        let header = CheckpointFileHeader::new(5);
        assert_eq!(header.data_offset, 40);
        assert_eq!(header.record_size, 32);
        assert_eq!(header.linked_ls_file_seq, 5);
    }

    #[test]
    fn as_bytes_roundtrip() {
        let header = CheckpointFileHeader::new(42);
        let bytes = header.as_bytes();
        assert_eq!(bytes.len(), CheckpointFileHeader::SIZE);

        let mut restored = CheckpointFileHeader::zeroed();
        restored.as_bytes_mut().copy_from_slice(&bytes[..CheckpointFileHeader::SIZE]);
        assert_eq!(restored.magic, CHECKPOINT_FILE_MAGIC);
        assert_eq!(restored.linked_ls_file_seq, 42);
        assert!(restored.verify_checksum());
    }

    #[test]
    fn format_version_is_one() {
        let header = CheckpointFileHeader::new(0);
        assert_eq!(header.format_version, 1);
    }

    #[test]
    fn canonical_crc_survives_serialize_then_read_back_into_aligned_self() {
        let header = CheckpointFileHeader::new(7);

        let bytes: Vec<u8> = header.as_bytes().to_vec();
        assert_eq!(bytes.len(), CheckpointFileHeader::SIZE);

        assert!(bytes.len() >= CheckpointFileHeader::SIZE);
        let mut restored = CheckpointFileHeader::zeroed();
        restored.as_bytes_mut().copy_from_slice(&bytes[..CheckpointFileHeader::SIZE]);

        assert_eq!(restored.magic, CHECKPOINT_FILE_MAGIC);
        assert_eq!(restored.linked_ls_file_seq, 7);
        assert_eq!(restored.checksum, header.checksum);
        assert!(restored.verify_checksum());
    }

    #[test]
    fn canonical_crc_rejects_single_byte_corruption_after_read_back() {
        let header = CheckpointFileHeader::new(7);
        let mut bytes: Vec<u8> = header.as_bytes().to_vec();

        bytes[24] ^= 0xFF;

        assert!(bytes.len() >= CheckpointFileHeader::SIZE);
        let mut restored = CheckpointFileHeader::zeroed();
        restored.as_bytes_mut().copy_from_slice(&bytes[..CheckpointFileHeader::SIZE]);

        assert!(!restored.verify_checksum());
    }
}