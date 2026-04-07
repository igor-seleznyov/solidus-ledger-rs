use common::crc32c::crc32c;

pub const INDEX_FORMAT_VERSION: u16 = 1;

pub const INDEX_MAGIC_ACCOUNTS: u64 = 0x4158_4449_5453_444C;
pub const INDEX_MAGIC_ORDINAL: u64 = 0x4F58_4449_5453_444C;
pub const INDEX_MAGIC_TIMESTAMP: u64 = 0x5458_4449_5453_444C;

#[repr(C)]
pub struct IndexFileHeader {
    pub magic: u64,
    pub format_version: u16,
    pub file_type: u8,
    pub _pad1: u8,
    pub entries_count: u32,
    pub linked_ls_file_seq: u64,
    pub data_offset: u32,
    pub _pad2: u32,
    pub created_at_ns: u64,
    pub _reserved: [u8; 20],
    pub checksum: u32,
}

impl IndexFileHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    pub fn new(magic: u64, file_type: u8, entries_count: u32, linked_ls_file_seq: u64) -> Self {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let mut header = Self {
            magic,
            format_version: INDEX_FORMAT_VERSION,
            file_type,
            _pad1: 0,
            entries_count,
            linked_ls_file_seq,
            data_offset: Self::SIZE as u32,
            _pad2: 0,
            created_at_ns: now_ns,
            _reserved: [0; 20],
            checksum: 0,
        };

        unsafe { header.compute_checksum(); }
        header
    }

    pub unsafe fn compute_checksum(&mut self) {
        self.checksum = 0;
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                Self::SIZE
            )
        };
        self.checksum = unsafe {
            crc32c(bytes.as_ptr(), bytes.len())
        };
    }

    pub unsafe fn verify_checksum(&self) -> bool {
        let saved = self.checksum;
        let self_mut = self as *const Self as *mut Self;
        unsafe {
            (*self_mut).checksum = 0;
        }
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                Self::SIZE,
            )
        };
        let computed = unsafe {
            crc32c(bytes.as_ptr(), bytes.len())
        };
        unsafe {
            (*self_mut).checksum = saved;
        }
        computed == saved
    }

    pub unsafe fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
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
        assert_eq!(IndexFileHeader::SIZE, 64);
        assert_eq!(std::mem::size_of::<IndexFileHeader>(), 64);
    }

    #[test]
    fn layout_offsets() {
        assert_eq!(std::mem::offset_of!(IndexFileHeader, magic), 0);
        assert_eq!(std::mem::offset_of!(IndexFileHeader, format_version), 8);
        assert_eq!(std::mem::offset_of!(IndexFileHeader, file_type), 10);
        assert_eq!(std::mem::offset_of!(IndexFileHeader, entries_count), 12);
        assert_eq!(std::mem::offset_of!(IndexFileHeader, linked_ls_file_seq), 16);
        assert_eq!(std::mem::offset_of!(IndexFileHeader, data_offset), 24);
        assert_eq!(std::mem::offset_of!(IndexFileHeader, created_at_ns), 32);
        assert_eq!(std::mem::offset_of!(IndexFileHeader, checksum), 60);
    }

    #[test]
    fn new_computes_checksum() {
        let header = IndexFileHeader::new(INDEX_MAGIC_ACCOUNTS, 1, 100, 0);
        assert_ne!(header.checksum, 0);
        assert!(unsafe { header.verify_checksum() });
    }

    #[test]
    fn verify_detects_corruption() {
        let mut header = IndexFileHeader::new(INDEX_MAGIC_ACCOUNTS, 1, 100, 0);
        header.entries_count = 999;
        assert!(!unsafe { header.verify_checksum() });
    }
}