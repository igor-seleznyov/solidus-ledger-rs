#[repr(C)]
pub struct AccountIndexRecord {
    pub account_id_hi: u64,
    pub account_id_lo: u64,
    pub ordinal_file_offset: u64,
    pub timestamp_file_offset: u64,
    pub records_count: u32,
    pub _pad: u32,
}

impl AccountIndexRecord {
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_size() {
        assert_eq!(AccountIndexRecord::SIZE, 40);
        assert_eq!(std::mem::size_of::<AccountIndexRecord>(), 40);
    }

    #[test]
    fn layout_offsets() {
        assert_eq!(std::mem::offset_of!(AccountIndexRecord, account_id_hi), 0);
        assert_eq!(std::mem::offset_of!(AccountIndexRecord, account_id_lo), 8);
        assert_eq!(std::mem::offset_of!(AccountIndexRecord, ordinal_file_offset), 16);
        assert_eq!(std::mem::offset_of!(AccountIndexRecord, timestamp_file_offset), 24);
        assert_eq!(std::mem::offset_of!(AccountIndexRecord, records_count), 32);
    }
}