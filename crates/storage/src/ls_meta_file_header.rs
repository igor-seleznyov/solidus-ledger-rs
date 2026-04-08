use common::crc32c::crc32c;
use crate::consts::FILE_PAGE_SIZE;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::ls_file_header::LS_FILE_MAGIC;

pub const LS_META_FILE_MAGIC: u64 = 0x4154_454D_5453_444C;
pub const LS_META_FORMAT_VERSION: u16 = 1;
pub const LS_META_FILE_TYPE: u8 = 1;

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
    pub _pad2: [u8; 72],
    pub checksum: u32,
    pub pad3: [u8; 4],
}

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

        unsafe { header.compute_checksum(); }

        header
    }

    pub unsafe fn compute_checksum(&mut self) {
        self.checksum = 0;
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const LsMetaFileHeader as *const u8,
                Self::SIZE,
            )
        };
        self.checksum = unsafe {
            crc32c(bytes.as_ptr(), bytes.len())
        };
    }

    pub unsafe fn verify_checksum(&self) -> bool {
        let saved = self.checksum;
        let self_mut = self as *const LsMetaFileHeader as *mut LsMetaFileHeader;
        unsafe {
            (*self_mut).checksum = 0;
        }
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const LsMetaFileHeader as *const u8,
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