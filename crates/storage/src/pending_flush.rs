#[repr(C)]
pub struct PendingFlush {
    pub transfer_id_hi: u64,
    pub transfer_id_lo: u64,
    pub transfer_hash_table_offset: u32,
    pub _pad: u32,
    pub gsn: u64,
}

impl PendingFlush {
    pub const SIZE: usize = size_of::<PendingFlush>();
}