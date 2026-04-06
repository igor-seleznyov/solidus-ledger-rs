use common::crc32c::crc32c;
use crate::consts::LS_FILE_PAGE_SIZE;
use std::time::{SystemTime, UNIX_EPOCH};

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
    pub _pad3: [u8; 64],
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

        unsafe { header.compute_checksum(); }

        header
    }

    pub unsafe fn compute_checksum(&mut self) {
        self.checksum = 0;
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const LsFileHeader as *const u8,
                Self::SIZE,
            )
        };

        self.checksum = unsafe { crc32c(bytes.as_ptr(), bytes.len()) };
    }

    pub unsafe fn verify_checksum(&self) -> bool {
        let saved = self.checksum;
        let self_mut = self as *const LsFileHeader as *mut LsFileHeader;
        unsafe {
            (*self_mut).checksum = 0;
        }
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const LsFileHeader as *const u8,
                Self::SIZE,
            )
        };
        let computed = unsafe { crc32c(bytes.as_ptr(), bytes.len()) };
        unsafe {
            (*self_mut).checksum = saved;
        }
        computed == saved
    }

    pub fn to_page(&self) -> [u8; LS_FILE_PAGE_SIZE] {
        let mut page = [0u8; LS_FILE_PAGE_SIZE];
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