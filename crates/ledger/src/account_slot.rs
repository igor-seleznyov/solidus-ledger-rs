use ringbuf::hash_table_slot_status::SLOT_FREE;

#[repr(C, align(64))]
#[derive(Clone, Copy)]
pub struct AccountSlot {
    // ═══ Cache line 0: hot data (each PREPARE) ═══
    pub account_id_hi: u64,
    pub account_id_lo: u64,
    pub psl: u8,
    pub status: u8,
    pub _pad1: [u8; 6],
    pub balance: i64,
    pub staged_income: i64,
    pub staged_outcome: i64,
    pub last_gsn: u64,
    pub initial_balance: i64,

    // ═══ Cache line 1: lifecycle (more rarely) ═══
    pub ordinal: u64,
    pub total_operations: u64,
    pub first_operation_time_ns: u64,
    pub last_operation_time_ns: u64,
    pub max_interval_ns: u64,
    pub ops_since_snapshot: u32,
    pub _pad2: u32,
    pub last_snapshot_ts_ms: u64,
    pub _reserved1: [u8; 8],

    // ═══ Cache line 2: persistence + reserve ═══
    pub ls_filename_hash: u64,
    pub ls_offset: u64,
    pub _reserved2: [u8; 48],
}

impl AccountSlot {
    pub const SIZE: usize = 192;

    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }

    pub fn is_empty(&self) -> bool {
        self.status == SLOT_FREE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_192() {
        assert_eq!(
            std::mem::size_of::<AccountSlot>(), 192,
            "AccountSlot must be exactly 192 bytes (3 cache lines)"
        );
    }

    #[test]
    fn alignment_is_64() {
        assert_eq!(
            std::mem::align_of::<AccountSlot>(), 64,
            "AccountSlot must be cache-line aligned"
        );
    }

    #[test]
    fn hot_fields_in_first_cache_line() {
        assert!(std::mem::offset_of!(AccountSlot, account_id_hi) < 64);
        assert!(std::mem::offset_of!(AccountSlot, account_id_lo) < 64);
        assert!(std::mem::offset_of!(AccountSlot, psl) < 64);
        assert!(std::mem::offset_of!(AccountSlot, balance) < 64);
        assert!(std::mem::offset_of!(AccountSlot, staged_income) < 64);
        assert!(std::mem::offset_of!(AccountSlot, staged_outcome) < 64);
    }

    #[test]
    fn zeroed_is_empty() {
        let slot = AccountSlot::zeroed();
        assert!(slot.is_empty());
        assert_eq!(slot.balance, 0);
        assert_eq!(slot.psl, 0);
    }

    #[test]
    fn offsets_match_layout() {
        assert_eq!(std::mem::offset_of!(AccountSlot, account_id_hi), 0);
        assert_eq!(std::mem::offset_of!(AccountSlot, account_id_lo), 8);
        assert_eq!(std::mem::offset_of!(AccountSlot, psl), 16);
        assert_eq!(std::mem::offset_of!(AccountSlot, balance), 24);
        assert_eq!(std::mem::offset_of!(AccountSlot, staged_income), 32);
        assert_eq!(std::mem::offset_of!(AccountSlot, staged_outcome), 40);
        assert_eq!(std::mem::offset_of!(AccountSlot, last_gsn), 48);
        assert_eq!(std::mem::offset_of!(AccountSlot, initial_balance), 56);
        assert_eq!(std::mem::offset_of!(AccountSlot, ordinal), 64);
        assert_eq!(std::mem::offset_of!(AccountSlot, ls_filename_hash), 128);
        assert_eq!(std::mem::offset_of!(AccountSlot, ls_offset), 136);
    }
}