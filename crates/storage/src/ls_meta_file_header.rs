use common::crc32c::crc32c;
use crate::consts::FILE_PAGE_SIZE;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::ls_file_header::LS_FILE_MAGIC;

pub const LS_META_FILE_MAGIC: u64 = 0x4154_454D_5453_444C;
pub const LS_META_FORMAT_VERSION: u16 = 1;
pub const LS_META_FILE_TYPE: u8 = 1;

/// Header of the `.ls_meta` metadata file. 128 B.
///
/// # Layout invariant (I-001)
///
/// `checksum` MUST remain the last field at offset 124 = SIZE - 4.
/// Future fields MUST go before `checksum` and eat into `_reserved`.
/// When `_reserved` is exhausted, bump `LS_META_FORMAT_VERSION` and
/// add version dispatch in the scanner.
#[repr(C)]
pub struct LsMetaFileHeader {
    pub magic: u64,
    pub format_version: u16,
    pub file_type: u8,
    pub _pad1: [u8; 5],
    pub created_at_ns: u64,
    pub record_size: u32,
    pub data_offset: u32,
    pub max_file_size: u64,
    pub linked_ls_file_seq: u64,
    pub _pad2: [u8; 76],
    pub checksum: u32,
}
const _: () = assert!(
    std::mem::size_of::<LsMetaFileHeader>()
        == size_of::<u64>() * 4
                + size_of::<u16>()
                + size_of::<u8>()
                + size_of::<[u8; 5]>()
                + size_of::<u32>() * 3
                + size_of::<[u8; 76]>(),
    "LsMetaFileHeader is larger than its fields: the compiler inserted alignment \
     padding. Declare it as an explicit field so the layout is stated, and \
     so the checksum stays the record's final bytes",
);


impl LsMetaFileHeader {
    pub const SIZE: usize = 128;
    pub const DATA_OFFSET: u32 = 4096;

    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }

    pub fn new(
        record_size: u32,
        max_file_size: u64,
        linked_ls_file_seq: u64,
    ) -> Self {
        let mut header = Self::zeroed();
        header.magic = LS_FILE_MAGIC;
        header.format_version = LS_META_FORMAT_VERSION;
        header.file_type = LS_META_FILE_TYPE;
        header.created_at_ns = Self::now_nanos();
        header.record_size = record_size;
        header.data_offset = Self::DATA_OFFSET;
        header.max_file_size = max_file_size;
        header.linked_ls_file_seq = linked_ls_file_seq;

        header.fill_checksum();

        header
    }

    /// Compute CRC32C over bytes `[0..SIZE - 4)`, excluding `checksum`.
    /// Safe to call on PROT_READ mmap (no mutation of `self`).
    ///
    /// # Safety (internal)
    ///
    /// The single `unsafe` block builds a `[0..SIZE - 4)` byte view over
    /// `self`. Valid because `self` is a live `&LsMetaFileHeader` of
    /// exactly `SIZE` bytes; the view excludes the trailing `checksum`;
    /// no mutation occurs, so it is sound on read-only memory.
    pub fn compute_checksum(&self) -> u32 {
        const PAYLOAD: usize = LsMetaFileHeader::SIZE - std::mem::size_of::<u32>();
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                PAYLOAD,
            )
        };
        unsafe { crc32c(bytes.as_ptr(), bytes.len()) }
    }

    pub fn fill_checksum(&mut self) {
        self.checksum = self.compute_checksum();
    }

    pub fn verify_checksum(&self) -> bool {
        self.checksum == self.compute_checksum()
    }

    pub fn to_page(&self) -> [u8; FILE_PAGE_SIZE] {
        let mut page = [0u8; FILE_PAGE_SIZE];
        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const LsMetaFileHeader as *const u8,
                Self::SIZE,
            )
        };
        page[..Self::SIZE].copy_from_slice(header_bytes);
        page
    }

    fn now_nanos() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }
}

const _: () = assert!(
    std::mem::offset_of!(LsMetaFileHeader, checksum) == LsMetaFileHeader::SIZE - std::mem::size_of::<u32>()
);
