#[repr(C)]
#[derive(Copy, Clone)]
pub struct TransferHashTableEntry {
    pub account_id_hi: u64,
    pub account_id_lo: u64,
    pub amount: i64,
    pub partition_id: u32,
    pub entry_type: u8,
    pub _pad: [u8; 3],
}

impl TransferHashTableEntry {
    pub const SIZE: usize = 32;

    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_32_bytes() {
        assert_eq!(std::mem::size_of::<TransferHashTableEntry>(), 32);
    }

    #[test]
    fn field_offsets() {
        assert_eq!(std::mem::offset_of!(TransferHashTableEntry, account_id_hi), 0);
        assert_eq!(std::mem::offset_of!(TransferHashTableEntry, account_id_lo), 8);
        assert_eq!(std::mem::offset_of!(TransferHashTableEntry, amount), 16);
        assert_eq!(std::mem::offset_of!(TransferHashTableEntry, partition_id), 24);
        assert_eq!(std::mem::offset_of!(TransferHashTableEntry, entry_type), 28);
    }
}