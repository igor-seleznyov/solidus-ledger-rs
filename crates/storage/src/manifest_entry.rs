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
    pub _pad2: [u8; 4],
    pub gsn_min: u64,
    pub gsn_max: u64,
    pub timestamp_min_ns: u64,
    pub timestamp_max_ns: u64,
    pub filename: [u8; 64],
    pub _pad3: [u8; 4],
    pub checksum: u32,
}
const _: () = assert!(
    std::mem::size_of::<ManifestEntry>()
        == size_of::<u64>() * 5
                + size_of::<u8>() * 4
                + size_of::<u32>() * 3
                + size_of::<[u8; 4]>() * 2
                + size_of::<[u8; 64]>(),
    "ManifestEntry is larger than its fields: the compiler inserted alignment \
     padding. Declare it as an explicit field so the layout is stated, and \
     so the checksum stays the record's final bytes",
);


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

    /// Compute CRC32C over bytes `[0..SIZE - 4)`, excluding `checksum`.
    /// Safe to call on PROT_READ mmap (no mutation of `self`).
    ///
    /// # Safety (internal)
    ///
    /// The single `unsafe` block builds a `[0..SIZE - 4)` byte view over
    /// `self`. Valid because `self` is a live `&ManifestEntry` of exactly
    /// `SIZE` bytes; the view excludes the trailing `checksum`; no
    /// mutation occurs, so it is sound on read-only memory.
    pub fn compute_checksum(&self) -> u32 {
        const PAYLOAD: usize = ManifestEntry::SIZE - std::mem::size_of::<u32>();
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                PAYLOAD,
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
                self as *const ManifestEntry as *const u8,
                Self::SIZE,
            )
        }
    }

    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self as *mut ManifestEntry as *mut u8,
                Self::SIZE,
            )
        }
    }
}

const _: () = assert!(
    std::mem::offset_of!(ManifestEntry, checksum) == ManifestEntry::SIZE - std::mem::size_of::<u32>()
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_size() {
        assert_eq!(ManifestEntry::SIZE, 128);
        assert_eq!(std::mem::size_of::<ManifestEntry>(), 128);
        assert_eq!(std::mem::align_of::<ManifestEntry>(), 64);
    }

    #[test]
    fn layout_offsets() {
        assert_eq!(std::mem::offset_of!(ManifestEntry, file_seq), 0);
        assert_eq!(std::mem::offset_of!(ManifestEntry, status), 8);
        assert_eq!(std::mem::offset_of!(ManifestEntry, signing_enabled), 9);
        assert_eq!(std::mem::offset_of!(ManifestEntry, metadata_enabled), 10);
        assert_eq!(std::mem::offset_of!(ManifestEntry, record_size), 12);
        assert_eq!(std::mem::offset_of!(ManifestEntry, rules_checksum), 16);
        assert_eq!(std::mem::offset_of!(ManifestEntry, _pad2), 20);
        assert_eq!(std::mem::offset_of!(ManifestEntry, gsn_min), 24);
        assert_eq!(std::mem::offset_of!(ManifestEntry, gsn_max), 32);
        assert_eq!(std::mem::offset_of!(ManifestEntry, timestamp_min_ns), 40);
        assert_eq!(std::mem::offset_of!(ManifestEntry, timestamp_max_ns), 48);

        assert_eq!(std::mem::offset_of!(ManifestEntry, filename), 56);
        assert_eq!(std::mem::offset_of!(ManifestEntry, _pad3), 120);
        assert_eq!(std::mem::offset_of!(ManifestEntry, checksum), 124);
    }

    #[test]
    fn filename_set_and_get() {
        let mut entry = ManifestEntry::zeroed();
        entry.set_filename("ls_20260403-120000-000-0-0.ls");
        assert_eq!(entry.filename_str(), "ls_20260403-120000-000-0-0.ls");
    }

    #[test]
    fn checksum_roundtrip() {
        let mut entry = ManifestEntry::zeroed();
        entry.file_seq = 5;
        entry.set_filename("test.ls");
        entry.fill_checksum();

        assert_ne!(entry.checksum, 0);
        assert!(entry.verify_checksum());
    }

    #[test]
    fn checksum_detects_corruption() {
        let mut entry = ManifestEntry::zeroed();
        entry.file_seq = 5;
        entry.fill_checksum();

        entry.file_seq = 999;
        assert!(!entry.verify_checksum());
    }

    #[test]
    #[should_panic(expected = "Filename too long")]
    fn filename_too_long() {
        let mut entry = ManifestEntry::zeroed();
        entry.set_filename(&"a".repeat(65));
    }
}