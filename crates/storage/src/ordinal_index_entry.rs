#[repr(C)]
pub struct OrdinalIndexEntry {
    pub ordinal: u64,
    pub ls_offset: u64,
}

impl OrdinalIndexEntry {
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_size() {
        assert_eq!(OrdinalIndexEntry::SIZE, 16);
        assert_eq!(std::mem::size_of::<OrdinalIndexEntry>(), 16);
    }

    #[test]
    fn layout_offsets() {
        assert_eq!(std::mem::offset_of!(OrdinalIndexEntry, ordinal), 0);
        assert_eq!(std::mem::offset_of!(OrdinalIndexEntry, ls_offset), 8);
    }
}