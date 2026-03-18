
#[repr(C)]
#[derive(Copy, Clone)]
pub struct VersionRecord {
    pub gsn: u64,
    pub balance: i64,
}

impl VersionRecord {
    pub const SIZE: usize = 16;
    
    pub fn zeroed() -> Self {
        Self { gsn: 0, balance: 0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_16_bytes() {
        assert_eq!(std::mem::size_of::<VersionRecord>(), 16);
    }

    #[test]
    fn field_offsets() {
        assert_eq!(std::mem::offset_of!(VersionRecord, gsn), 0);
        assert_eq!(std::mem::offset_of!(VersionRecord, balance), 8);
    }
}