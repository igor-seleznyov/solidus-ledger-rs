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
    pub _pad3: [u8; 20],
    pub checksum: u32,
}
const _: () = assert!(
    std::mem::size_of::<IndexFileHeader>()
        == size_of::<u64>() * 3
                + size_of::<u16>()
                + size_of::<u8>() * 2
                + size_of::<u32>() * 4
                + size_of::<[u8; 20]>(),
    "IndexFileHeader is larger than its fields: the compiler inserted alignment \
     padding. Declare it as an explicit field so the layout is stated, and \
     so the checksum stays the record's final bytes",
);


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
            _pad3: [0; 20],
            checksum: 0,
        };

        header.fill_checksum();
        header
    }

    /// Compute CRC32C over bytes `[0..SIZE - 4)`, excluding `checksum`.
    /// Safe to call on PROT_READ mmap (no mutation of `self`).
    ///
    /// # Safety (internal)
    ///
    /// The single `unsafe` block builds a `[0..SIZE - 4)` byte view over
    /// `self`. Valid because `self` is a live `&IndexFileHeader` of
    /// exactly `SIZE` bytes; the view excludes the trailing `checksum`;
    /// no mutation occurs, so it is sound on read-only memory.
    pub fn compute_checksum(&self) -> u32 {
        const PAYLOAD: usize = IndexFileHeader::SIZE - std::mem::size_of::<u32>();
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                PAYLOAD
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
                self as *const Self as *const u8,
                Self::SIZE,
            )
        }
    }
}

const _: () = assert!(
    std::mem::offset_of!(IndexFileHeader, checksum) == IndexFileHeader::SIZE - std::mem::size_of::<u32>()
);

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
        assert_eq!(std::mem::offset_of!(IndexFileHeader, _pad3), 40);
        assert_eq!(std::mem::offset_of!(IndexFileHeader, checksum), 60);
    }

    #[test]
    fn new_computes_checksum() {
        let header = IndexFileHeader::new(INDEX_MAGIC_ACCOUNTS, 1, 100, 0);
        assert_ne!(header.checksum, 0);
        assert!(header.verify_checksum());
    }

    #[test]
    fn verify_detects_corruption() {
        let mut header = IndexFileHeader::new(INDEX_MAGIC_ACCOUNTS, 1, 100, 0);
        header.entries_count = 999;
        assert!(!header.verify_checksum());
    }
}