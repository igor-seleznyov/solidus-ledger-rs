use common::crc32c::crc32c;
use crate::consts::FILE_PAGE_SIZE;
use std::time::{SystemTime, UNIX_EPOCH};

pub const LS_SIGN_FILE_MAGIC: u64 = 0x4E47_4953_5453_444C;
pub const LS_SIGN_FORMAT_VERSION: u16 = 1;
pub const LS_SIGN_FILE_TYPE: u8 = 3;

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
    pub _pad2: [u8; 24],
    pub checksum: u32,
    pub _pad3: [u8; 4],
}

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

        unsafe { header.compute_checksum(); }

        header
    }

    pub unsafe fn compute_checksum(&mut self) {
        self.checksum = 0;
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const LsSignFileHeader as *const u8,
                Self::SIZE
            )
        };
        self.checksum = unsafe {
            crc32c(bytes.as_ptr(), bytes.len())
        }
    }

    pub unsafe fn verify_checksum(&self) -> bool {
        let saved = self.checksum;
        let self_mut = self as *const LsSignFileHeader as *mut LsSignFileHeader;
        unsafe {
            (*self_mut).checksum = 0
        };
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const LsSignFileHeader as *const u8,
                Self::SIZE
            )
        };
        let computed = unsafe {
            crc32c(bytes.as_ptr(), bytes.len())
        };
        unsafe {
            (*self_mut).checksum = saved
        };
        computed == saved
    }

    pub fn to_page(&self) -> [u8; FILE_PAGE_SIZE] {
        let mut page = [0u8; FILE_PAGE_SIZE];
        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const LsSignFileHeader as *const u8,
                Self::SIZE
            )
        };
        page[..Self::SIZE].copy_from_slice(header_bytes);
        page
    }

    pub unsafe fn from_bytes(bytes: &[u8]) -> &LsSignFileHeader {
        assert!(bytes.len() >= Self::SIZE);
        unsafe {
            &*(bytes.as_ptr() as *const LsSignFileHeader)
        }
    }

    fn now_nanos() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }
}