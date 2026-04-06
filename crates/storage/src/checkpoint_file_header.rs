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

        header.checksum = unsafe {
            crc32c(
                &header as *const CheckpointFileHeader as *const u8,
                Self::SIZE,
            )
        };

        header
    }

    pub unsafe fn verify_checksum(&self) -> bool {
        let saved = self.checksum;
        let self_mut = self as *const CheckpointFileHeader as *mut CheckpointFileHeader;
        unsafe {
            (*self_mut).checksum = 0;
        }
        let computed = unsafe {
            crc32c(
                self as *const CheckpointFileHeader as *const u8,
                Self::SIZE,
            )
        };
        unsafe {
            (*self_mut).checksum = saved;
        }
        computed == saved
    }

    pub unsafe fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const CheckpointFileHeader as *const u8,
                Self::SIZE,
            )
        }
    }

    pub unsafe fn from_bytes(bytes: &[u8]) -> &CheckpointFileHeader {
        assert!(bytes.len() >= Self::SIZE);
        unsafe {
            &*(bytes.as_ptr() as *const CheckpointFileHeader)
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
        assert!(unsafe { header.verify_checksum() });
    }

    #[test]
    fn verify_checksum_detects_corruption() {
        let mut header = CheckpointFileHeader::new(0);
        header.linked_ls_file_seq = 999;
        assert!(!unsafe { header.verify_checksum() });
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
        let bytes = unsafe { header.as_bytes() };
        assert_eq!(bytes.len(), CheckpointFileHeader::SIZE);

        let restored = unsafe { CheckpointFileHeader::from_bytes(bytes) };
        assert_eq!(restored.magic, CHECKPOINT_FILE_MAGIC);
        assert_eq!(restored.linked_ls_file_seq, 42);
        assert!(unsafe { restored.verify_checksum() });
    }

    #[test]
    fn format_version_is_one() {
        let header = CheckpointFileHeader::new(0);
        assert_eq!(header.format_version, 1);
    }
}