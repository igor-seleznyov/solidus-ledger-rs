use common::crc32c::crc32c;
use std::time::{SystemTime, UNIX_EPOCH};

#[repr(C)]
pub struct CheckpointRecord {
    pub first_posting_offset: u64,
    pub posting_count: u64,
    pub batch_seq: u32,
    pub checksum: u32,
    pub timestamp_ns: u64,
}

impl CheckpointRecord {
    pub const SIZE: usize = std::mem::size_of::<CheckpointRecord>();
    
    pub fn new(
        first_posting_offset: u64,
        posting_count: u64,
        batch_seq: u32,
    ) -> Self {
        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        
        let mut record = Self {
            first_posting_offset,
            posting_count,
            batch_seq,
            checksum: 0,
            timestamp_ns,
        };
        
        record.checksum = unsafe {
            crc32c(
                &record as *const CheckpointRecord as *const u8,
                Self::SIZE,
            )
        };
        
        record
    }
    
    pub unsafe fn verify_checksum(&self) -> bool {
        let saved = self.checksum;
        let self_mut = self as *const CheckpointRecord as *mut CheckpointRecord;
        unsafe {
            (*self_mut).checksum = 0;
        }
        let computed = unsafe {
            crc32c(
                self as *const CheckpointRecord as *const u8,
                Self::SIZE,
            )
        };
        unsafe {
            (*self_mut).checksum = saved;
        }
        computed == saved
    }
    
    pub unsafe fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const CheckpointRecord as *const u8,
                Self::SIZE,
            )
        }
    }
    
    pub unsafe fn from_bytes(bytes: &[u8]) -> &CheckpointRecord {
        assert!(bytes.len() >= Self::SIZE);
        unsafe {
            &*(bytes.as_ptr() as *const CheckpointRecord)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_size() {
        assert_eq!(CheckpointRecord::SIZE, 32);
        assert_eq!(std::mem::size_of::<CheckpointRecord>(), 32);
    }

    #[test]
    fn layout_offsets() {
        assert_eq!(std::mem::offset_of!(CheckpointRecord, first_posting_offset), 0);
        assert_eq!(std::mem::offset_of!(CheckpointRecord, posting_count), 8);
        assert_eq!(std::mem::offset_of!(CheckpointRecord, batch_seq), 16);
        assert_eq!(std::mem::offset_of!(CheckpointRecord, checksum), 20);
        assert_eq!(std::mem::offset_of!(CheckpointRecord, timestamp_ns), 24);
    }

    #[test]
    fn new_computes_checksum() {
        let record = CheckpointRecord::new(4096, 10, 0);
        assert_ne!(record.checksum, 0);
        assert!(unsafe { record.verify_checksum() });
    }

    #[test]
    fn verify_checksum_detects_corruption() {
        let mut record = CheckpointRecord::new(4096, 10, 0);
        record.posting_count = 999;
        assert!(!unsafe { record.verify_checksum() });
    }

    #[test]
    fn fields_stored_correctly() {
        let record = CheckpointRecord::new(8192, 42, 7);
        assert_eq!(record.first_posting_offset, 8192);
        assert_eq!(record.posting_count, 42);
        assert_eq!(record.batch_seq, 7);
        assert_ne!(record.timestamp_ns, 0);
    }

    #[test]
    fn as_bytes_roundtrip() {
        let record = CheckpointRecord::new(4096, 5, 1);
        let bytes = unsafe { record.as_bytes() };
        assert_eq!(bytes.len(), CheckpointRecord::SIZE);

        let restored = unsafe { CheckpointRecord::from_bytes(bytes) };
        assert_eq!(restored.first_posting_offset, 4096);
        assert_eq!(restored.posting_count, 5);
        assert_eq!(restored.batch_seq, 1);
        assert!(unsafe { restored.verify_checksum() });
    }

    #[test]
    fn different_inputs_different_checksums() {
        let r1 = CheckpointRecord::new(4096, 10, 0);
        let r2 = CheckpointRecord::new(8192, 10, 0);
        assert_ne!(r1.checksum, r2.checksum);
    }
}