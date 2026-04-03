use common::crc32c;
use common::crc32c::crc32c;

pub const MANIFEST_ENTRY_MAGIC: u64 = 0x4E45_464D_5453_444C;

pub const MANIFEST_STATUS_CURRENT: u8 = 0;
pub const MANIFEST_STATUS_ROTATED: u8 = 1;
pub const MANIFEST_STATUS_ARCHIVED: u8 = 2;

#[repr(C, align(64))]
pub struct ManifestEntry {
    pub file_seq: u64,
    pub status: u8,
    pub signing_enabled: u8,
    pub metadata_enabled: u8,
    pub _pad1: u8,
    pub record_size: u32,
    pub rules_checksum: u32,
    pub _ipad2: [u8; 4],
    pub gsn_min: u64,
    pub gsn_max: u64,
    pub timestamp_min_ns: u64,
    pub timestamp_max_ns: u64,
    pub checksum: u32,
    pub _pad3: [u8; 4],

    pub filename: [u8; 64],
}

impl ManifestEntry {
    pub const SIZE: usize = std::mem::size_of::<ManifestEntry>();

    pub fn zeroed() -> Self {
        unsafe {
            std::mem::zeroed()
        }
    }

    pub fn set_filename(&mut self, name: &str) {
        assert!(
            name.len() <= 64,
            "Filename too long: {} bytes, max 64",
            name.len()
        );
        self.filename = [0u8; 64];
        self.filename[..name.len()].copy_from_slice(name.as_bytes());
    }

    pub fn filename_str(&self) -> &str {
        let len = self.filename.iter().position(|&b| b == 0).unwrap_or(64);
        std::str::from_utf8(&self.filename[..len])
            .expect("Manifest entry filename is not valid UTF-8")
    }
    
    pub unsafe fn compute_checksum(&mut self) {
        self.checksum = 0;
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const ManifestEntry as *const u8,
                Self::SIZE,
            )
        };
        self.checksum = unsafe { crc32c::crc32c(bytes.as_ptr(), bytes.len()) };
    }
    
    pub unsafe fn verify_checksum(&self) -> bool {
        let saved = self.checksum;
        let self_mut = self as *const ManifestEntry as *mut ManifestEntry;
        unsafe {
            (*self_mut).checksum = 0
        }
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const ManifestEntry as *const u8,
                Self::SIZE,
            )
        };
        let computed = unsafe { crc32c::crc32c(bytes.as_ptr(), bytes.len()) };
        unsafe {
            (*self_mut).checksum = saved
        }
        computed == saved
    }
    
    pub unsafe fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const ManifestEntry as *const u8,
                Self::SIZE,
            )
        }
    }
}