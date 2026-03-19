use crate::transfer_hash_table_entry::TransferHashTableEntry;

pub const INLINE_ENTRIES: usize = 8;

pub const DECISION_NONE: u8 = 0;
pub const DECISION_COMMIT: u8 = 1;
pub const DECISION_ROLLBACK: u8 = 2;
pub const DECISION_PENDING_FLUSH: u8 = 3;

#[repr(C, align(64))]
#[derive(Copy, Clone)]
pub struct TransferSlot {
    pub transfer_id_hi: u64,
    pub transfer_id_lo: u64,
    pub psl: u8,
    pub fingerprint: u8,
    pub ready: u8,
    pub decision: u8,
    pub entries_count: u8,
    pub fail_reason: u8,
    pub confirmed_count: u8,
    pub failed_count: u8,
    pub gsn: u64,
    pub connection_id: u64,
    pub batch_id: [u8; 16],
    pub commit_success_count: u8,
    pub rollback_success_count: u8,
    pub has_metadata: u8,
    pub _pad1: [u8; 5],

    pub created_at_ns: u64,
    pub transfer_datetime: u64,
    pub transfer_sequence_id: [u8; 16],
    pub currency: [u8; 16],
    pub metadata_slot: u32,
    pub _pad2: [u8; 12],

    pub entries: [TransferHashTableEntry; INLINE_ENTRIES],
}

impl TransferSlot {
    pub const SIZE: usize = 384;

    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }

    pub fn is_empty(&self) -> bool {
        self.psl == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_384_bytes() {
        assert_eq!(
            std::mem::size_of::<TransferSlot>(), 384,
            "TransferSlot must be exactly 384 bytes (6 cache lines)"
        );
    }

    #[test]
    fn alignment_is_64() {
        assert_eq!(std::mem::align_of::<TransferSlot>(), 64);
    }

    #[test]
    fn probe_fields_in_first_cache_line() {
        assert!(std::mem::offset_of!(TransferSlot, transfer_id_hi) < 64);
        assert!(std::mem::offset_of!(TransferSlot, transfer_id_lo) < 64);
        assert!(std::mem::offset_of!(TransferSlot, psl) < 64);
        assert!(std::mem::offset_of!(TransferSlot, fingerprint) < 64);
        assert!(std::mem::offset_of!(TransferSlot, ready) < 64);
        assert!(std::mem::offset_of!(TransferSlot, gsn) < 64);
        assert!(std::mem::offset_of!(TransferSlot, confirmed_count) < 64);
        assert!(std::mem::offset_of!(TransferSlot, failed_count) < 64);
    }

    #[test]
    fn entries_start_at_128() {
        assert_eq!(std::mem::offset_of!(TransferSlot, entries), 128);
    }

    #[test]
    fn field_offsets() {
        assert_eq!(std::mem::offset_of!(TransferSlot, transfer_id_hi), 0);
        assert_eq!(std::mem::offset_of!(TransferSlot, transfer_id_lo), 8);
        assert_eq!(std::mem::offset_of!(TransferSlot, psl), 16);
        assert_eq!(std::mem::offset_of!(TransferSlot, fingerprint), 17);
        assert_eq!(std::mem::offset_of!(TransferSlot, ready), 18);
        assert_eq!(std::mem::offset_of!(TransferSlot, decision), 19);
        assert_eq!(std::mem::offset_of!(TransferSlot, entries_count), 20);
        assert_eq!(std::mem::offset_of!(TransferSlot, fail_reason), 21);
        assert_eq!(std::mem::offset_of!(TransferSlot, confirmed_count), 22);
        assert_eq!(std::mem::offset_of!(TransferSlot, failed_count), 23);
        assert_eq!(std::mem::offset_of!(TransferSlot, gsn), 24);
        assert_eq!(std::mem::offset_of!(TransferSlot, connection_id), 32);
        assert_eq!(std::mem::offset_of!(TransferSlot, batch_id), 40);
        assert_eq!(std::mem::offset_of!(TransferSlot, commit_success_count), 56);
        assert_eq!(std::mem::offset_of!(TransferSlot, rollback_success_count), 57);
        assert_eq!(std::mem::offset_of!(TransferSlot, created_at_ns), 64);
        assert_eq!(std::mem::offset_of!(TransferSlot, transfer_datetime), 72);
        assert_eq!(std::mem::offset_of!(TransferSlot, transfer_sequence_id), 80);
        assert_eq!(std::mem::offset_of!(TransferSlot, metadata_slot), 112);
        assert_eq!(std::mem::offset_of!(TransferSlot, entries), 128);
    }

    #[test]
    fn zeroed_is_empty() {
        let slot = TransferSlot::zeroed();
        assert!(slot.is_empty());
        assert_eq!(slot.ready, 0);
        assert_eq!(slot.decision, DECISION_NONE);
        assert_eq!(slot.confirmed_count, 0);
        assert_eq!(slot.failed_count, 0);
    }

    #[test]
    fn decision_constants() {
        assert_eq!(DECISION_NONE, 0);
        assert_eq!(DECISION_COMMIT, 1);
        assert_eq!(DECISION_ROLLBACK, 2);
        assert_eq!(DECISION_PENDING_FLUSH, 3);
    }
}