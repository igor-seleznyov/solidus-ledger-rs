pub struct TimestampIndexEntry {
    pub timestamp_ns: u64,
    pub ls_offset: u64,
}

impl TimestampIndexEntry {
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_size() {
        assert_eq!(TimestampIndexEntry::SIZE, 16);
        assert_eq!(std::mem::size_of::<TimestampIndexEntry>(), 16);
    }

    #[test]
    fn layout_offsets() {
        assert_eq!(std::mem::offset_of!(TimestampIndexEntry, timestamp_ns), 0);
        assert_eq!(std::mem::offset_of!(TimestampIndexEntry, ls_offset), 8);
    }
}