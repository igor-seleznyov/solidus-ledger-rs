use common::crc32c::crc32c;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::manifest_entry::ManifestEntry;

pub const MANIFEST_HEADER_MAGIC: u64 = 0x5446_4E4D_5453_444C;
pub const MANIFEST_FORMAT_VERSION: u16 = 1;

#[repr(C, align(64))]
pub struct ManifestHeader {
    pub magic: u64,
    pub format_version: u16,
    pub _pad1: [u8; 2],
    pub entries_count: u32,
    pub current_entry_index: u32,
    pub shard_id: u16,
    pub _pad2: [u8; 2],
    pub created_at_ns: u64,
    pub last_updated_at_ns: u64,
    pub _reserved: [u8; 20],
    pub checksum: u32,
}

impl ManifestHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    pub fn new(shard_id: u16) -> Self {
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let mut header = Self {
            magic: MANIFEST_HEADER_MAGIC,
            format_version: MANIFEST_FORMAT_VERSION,
            _pad1: [0; 2],
            entries_count: 0,
            current_entry_index: 0,
            shard_id,
            _pad2: [0; 2],
            created_at_ns: now_ns,
            last_updated_at_ns: now_ns,
            _reserved: [0; 20],
            checksum: 0,
        };

        unsafe { header.compute_checksum(); }
        header
    }

    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }

    pub unsafe fn compute_checksum(&mut self) {
        self.checksum = 0;
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const ManifestHeader as *const u8,
                Self::SIZE,
            )
        };
        self.checksum = unsafe {
            crc32c(bytes.as_ptr(), bytes.len())
        };
    }

    pub unsafe fn verify_checksum(&self) -> bool {
        let saved = self.checksum;
        let self_mut = self as *const ManifestHeader as *mut ManifestHeader;
        unsafe {
            (*self_mut).checksum = 0;
        }
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const ManifestHeader as *const u8,
                Self::SIZE,
            )
        };
        let computed = unsafe {
            crc32c(bytes.as_ptr(), bytes.len())
        };
        unsafe {
            (*self_mut).checksum = saved;
            computed == saved
        }
    }

    pub unsafe fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const ManifestHeader as *const u8,
                Self::SIZE,
            )
        }
    }

    pub unsafe fn from_bytes(bytes: &[u8]) -> &ManifestHeader {
        assert!(bytes.len() >= Self::SIZE, "Buffer too small for ManifestHeader");
        unsafe {
            &*(bytes.as_ptr() as *const ManifestHeader)
        }
    }

    pub unsafe fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self as *mut ManifestHeader as *mut u8,
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
        assert_eq!(ManifestHeader::SIZE, 64);
        assert_eq!(std::mem::size_of::<ManifestHeader>(), 64);
        assert_eq!(std::mem::align_of::<ManifestHeader>(), 64);
    }

    #[test]
    fn layout_offsets() {
        assert_eq!(std::mem::offset_of!(ManifestHeader, magic), 0);
        assert_eq!(std::mem::offset_of!(ManifestHeader, format_version), 8);
        assert_eq!(std::mem::offset_of!(ManifestHeader, entries_count), 12);
        assert_eq!(std::mem::offset_of!(ManifestHeader, current_entry_index), 16);
        assert_eq!(std::mem::offset_of!(ManifestHeader, shard_id), 20);
        assert_eq!(std::mem::offset_of!(ManifestHeader, created_at_ns), 24);
        assert_eq!(std::mem::offset_of!(ManifestHeader, last_updated_at_ns), 32);
        assert_eq!(std::mem::offset_of!(ManifestHeader, checksum), 60);
    }

    #[test]
    fn new_computes_checksum() {
        let header = ManifestHeader::new(0);
        assert_ne!(header.checksum, 0);
        assert!(unsafe { header.verify_checksum() });
    }

    #[test]
    fn magic_correct() {
        let header = ManifestHeader::new(0);
        assert_eq!(header.magic, MANIFEST_HEADER_MAGIC);
    }

    #[test]
    fn shard_id_stored() {
        let header = ManifestHeader::new(5);
        assert_eq!(header.shard_id, 5);
    }

    #[test]
    fn verify_detects_corruption() {
        let mut header = ManifestHeader::new(0);
        header.entries_count = 999;
        assert!(!unsafe { header.verify_checksum() });
    }

    #[test]
    fn as_bytes_roundtrip() {
        let header = ManifestHeader::new(3);
        let bytes = unsafe { header.as_bytes() };
        assert_eq!(bytes.len(), ManifestHeader::SIZE);

        let restored = unsafe { ManifestHeader::from_bytes(bytes) };
        assert_eq!(restored.magic, MANIFEST_HEADER_MAGIC);
        assert_eq!(restored.shard_id, 3);
        assert!(unsafe { restored.verify_checksum() });
    }
}