use common::crc32c::crc32c;
use std::time::{SystemTime, UNIX_EPOCH};

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
    pub _pad3: [u8; 20],
    pub checksum: u32,
}
const _: () = assert!(
    std::mem::size_of::<ManifestHeader>()
        == size_of::<u64>() * 3
                + size_of::<u16>() * 2
                + size_of::<[u8; 2]>() * 2
                + size_of::<u32>() * 3
                + size_of::<[u8; 20]>(),
    "ManifestHeader is larger than its fields: the compiler inserted alignment \
     padding. Declare it as an explicit field so the layout is stated, and \
     so the checksum stays the record's final bytes",
);


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
            _pad3: [0; 20],
            checksum: 0,
        };

        header.fill_checksum();
        header
    }

    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }

    /// Compute CRC32C over bytes `[0..SIZE - 4)`, excluding `checksum`.
    /// Safe to call on PROT_READ mmap (no mutation of `self`).
    ///
    /// # Safety (internal)
    ///
    /// The single `unsafe` block builds a `[0..SIZE - 4)` byte view over
    /// `self`. Valid because `self` is a live `&ManifestHeader` of exactly
    /// `SIZE` bytes; the view excludes the trailing `checksum`; no
    /// mutation occurs, so it is sound on read-only memory.
    pub fn compute_checksum(&self) -> u32 {
        const PAYLOAD: usize = ManifestHeader::SIZE - std::mem::size_of::<u32>();
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
                self as *const ManifestHeader as *const u8,
                Self::SIZE,
            )
        }
    }

    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self as *mut ManifestHeader as *mut u8,
                Self::SIZE,
            )
        }
    }
}

const _: () = assert!(
    std::mem::offset_of!(ManifestHeader, checksum) == ManifestHeader::SIZE - std::mem::size_of::<u32>()
);

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
        assert_eq!(std::mem::offset_of!(ManifestHeader, _pad3), 40);
        assert_eq!(std::mem::offset_of!(ManifestHeader, checksum), 60);
    }

    #[test]
    fn new_computes_checksum() {
        let header = ManifestHeader::new(0);
        assert_ne!(header.checksum, 0);
        assert!(header.verify_checksum());
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
        assert!(!header.verify_checksum());
    }

    #[test]
    fn as_bytes_roundtrip() {
        let header = ManifestHeader::new(3);
        let bytes = header.as_bytes();
        assert_eq!(bytes.len(), ManifestHeader::SIZE);

        let mut restored = ManifestHeader::zeroed();
        restored.as_bytes_mut().copy_from_slice(&bytes[..ManifestHeader::SIZE]);
        assert_eq!(restored.magic, MANIFEST_HEADER_MAGIC);
        assert_eq!(restored.shard_id, 3);
        assert!(restored.verify_checksum());
    }
}