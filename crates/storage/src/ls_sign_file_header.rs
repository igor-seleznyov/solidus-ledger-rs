use crate::consts::FILE_PAGE_SIZE;
use common::crc32c::crc32c;
use std::time::{SystemTime, UNIX_EPOCH};

pub const LS_SIGN_FILE_MAGIC: u64 = 0x4E47_4953_5453_444C;
pub const LS_SIGN_FORMAT_VERSION: u16 = 1;
pub const LS_SIGN_FILE_TYPE: u8 = 3;

/// Header of the `.ls_sign` signature file. 128 B.
///
/// # Layout invariant (I-001)
///
/// `checksum` MUST remain the last field at offset 124 = SIZE - 4.
/// Future fields MUST go before `checksum` and eat into `_reserved`.
/// When `_reserved` is exhausted, bump `LS_SIGN_FORMAT_VERSION` and
/// add version dispatch in the scanner.
#[repr(C)]
pub struct LsSignFileHeader {
    pub magic: u64,
    pub format_version: u16,
    pub file_type: u8,
    pub algorithm: u8,
    pub key_version: u16,
    pub _pad1: [u8; 2],
    pub created_at_ns: u64,
    pub linked_ls_file_seq: u64,
    pub public_key: [u8; 32],
    pub genesis_hash: [u8; 32],
    pub _pad2: [u8; 28],
    pub checksum: u32,
}
const _: () = assert!(
    std::mem::size_of::<LsSignFileHeader>()
        == size_of::<u64>() * 3
                + size_of::<u16>() * 2
                + size_of::<u8>() * 2
                + size_of::<[u8; 2]>()
                + size_of::<[u8; 32]>() * 2
                + size_of::<[u8; 28]>()
                + size_of::<u32>(),
    "LsSignFileHeader is larger than its fields: the compiler inserted alignment \
     padding. Declare it as an explicit field so the layout is stated, and \
     so the checksum stays the record's final bytes",
);


impl LsSignFileHeader {
    pub const SIZE: usize = 128;
    pub const DATA_OFFSET: u32 = 4096;

    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }

    pub fn new(
        algorithm: u8,
        key_version: u16,
        linked_ls_file_seq: u64,
        public_key: [u8; 32],
        genesis_hash: [u8; 32],
    ) -> Self {
        let mut header = Self::zeroed();
        header.magic = LS_SIGN_FILE_MAGIC;
        header.format_version = LS_SIGN_FORMAT_VERSION;
        header.file_type = LS_SIGN_FILE_TYPE;
        header.algorithm = algorithm;
        header.key_version = key_version;
        header.created_at_ns = Self::now_nanos();
        header.linked_ls_file_seq = linked_ls_file_seq;
        header.public_key = public_key;
        header.genesis_hash = genesis_hash;

        header.fill_checksum();

        header
    }

    /// Compute CRC32C over bytes `[0..SIZE - 4)`, excluding `checksum`.
    /// Safe to call on PROT_READ mmap (no mutation of `self`).
    ///
    /// # Safety (internal)
    ///
    /// The single `unsafe` block builds a `[0..SIZE - 4)` byte view over
    /// `self`. Valid because `self` is a live `&LsSignFileHeader` of
    /// exactly `SIZE` bytes; the view excludes the trailing `checksum`;
    /// no mutation occurs, so it is sound on read-only memory.
    pub fn compute_checksum(&self) -> u32 {
        const PAYLOAD: usize = LsSignFileHeader::SIZE - std::mem::size_of::<u32>();
        let bytes =
            unsafe { std::slice::from_raw_parts(self as *const Self as *const u8, PAYLOAD) };
        unsafe { crc32c(bytes.as_ptr(), bytes.len()) }
    }

    pub fn fill_checksum(&mut self) {
        self.checksum = self.compute_checksum();
    }

    /// Return `true` if `self.checksum` matches `compute_checksum()`.
    pub fn verify_checksum(&self) -> bool {
        self.checksum == self.compute_checksum()
    }

    pub fn to_page(&self) -> [u8; FILE_PAGE_SIZE] {
        let mut page = [0u8; FILE_PAGE_SIZE];
        let header_bytes = unsafe {
            std::slice::from_raw_parts(self as *const LsSignFileHeader as *const u8, Self::SIZE)
        };
        page[..Self::SIZE].copy_from_slice(header_bytes);
        page
    }

    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self as *mut LsSignFileHeader as *mut u8, Self::SIZE)
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self as *const LsSignFileHeader as *const u8, Self::SIZE)
        }
    }

    fn now_nanos() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }
}

const _: () = assert!(
    std::mem::offset_of!(LsSignFileHeader, checksum)
        == LsSignFileHeader::SIZE - std::mem::size_of::<u32>()
);
