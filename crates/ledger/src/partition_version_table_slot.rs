use ringbuf::hash_table_slot_status::SLOT_FREE;
use crate::version_record::VersionRecord;

pub const INLINE_VERSIONS: usize = 8;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct PartitionVersionTableSlot {
    pub account_id_hi: u64,
    pub account_id_lo: u64,
    pub psl: u8,
    pub status: u8,
    pub _pad: [u8; 2],
    pub count: u32,
    pub inline: [VersionRecord; INLINE_VERSIONS],
    pub overflow: u32,
    pub _pad2: [u8; 4],
}

impl PartitionVersionTableSlot {
    pub const SIZE: usize = 160;

    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }

    pub fn is_empty(&self) -> bool {
        self.status == SLOT_FREE
    }
}

#[cfg(test)]
#[cfg(not(miri))]
mod tests {
    use super::*;

    #[test]
    fn size_is_160_bytes() {
        assert_eq!(
            std::mem::size_of::<PartitionVersionTableSlot>(), 160,
        );
    }

    #[test]
    fn field_offsets() {
        assert_eq!(std::mem::offset_of!(PartitionVersionTableSlot, account_id_hi), 0);
        assert_eq!(std::mem::offset_of!(PartitionVersionTableSlot, account_id_lo), 8);
        assert_eq!(std::mem::offset_of!(PartitionVersionTableSlot, psl), 16);
        assert_eq!(std::mem::offset_of!(PartitionVersionTableSlot, count), 20);
        assert_eq!(std::mem::offset_of!(PartitionVersionTableSlot, inline), 24);
        assert_eq!(std::mem::offset_of!(PartitionVersionTableSlot, overflow), 152);
    }

    #[test]
    fn zeroed_is_empty() {
        let slot = PartitionVersionTableSlot::zeroed();
        assert!(slot.is_empty());
        assert_eq!(slot.count, 0);
    }
}