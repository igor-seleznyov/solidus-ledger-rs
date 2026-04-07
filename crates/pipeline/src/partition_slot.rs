use ringbuf::slot::Slot;

pub const ENTRY_TYPE_DEBIT: u8 = 1;
pub const ENTRY_TYPE_CREDIT: u8 = 2;

pub const MSG_TYPE_PREPARE: u8 = 1;
pub const MSG_TYPE_COMMIT: u8 = 2;
pub const MSG_TYPE_ROLLBACK: u8 = 3;

#[repr(C, align(64))]
#[derive(Copy, Clone)]
pub struct PartitionSlot {
    pub sequence: u64,
    pub gsn: u64,
    pub transfer_id: [u8; 16],
    pub account_id: [u8; 16],
    pub amount: i64,
    pub entry_type: u8,
    pub msg_type: u8,
    pub shard_id: u8,
    pub entry_index: u8,
    pub transfer_hash_table_offset: u32,
}

unsafe impl Slot for PartitionSlot {
    fn sequence(&self) -> u64 {
        self.sequence
    }

    fn set_sequence(&mut self, seq: u64) {
        self.sequence = seq;
    }
}

impl PartitionSlot {
    pub fn zeroed() -> Self {
        Self {
            sequence: 0,
            gsn: 0,
            transfer_id: [0u8; 16],
            account_id: [0u8; 16],
            amount: 0,
            entry_type: 0,
            msg_type: 0,
            shard_id: 0,
            entry_index: 0,
            transfer_hash_table_offset: 0,
        }
    }
}

#[cfg(test)]
#[cfg(not(miri))]
mod tests {
    use super::*;

    #[test]
    fn size_is_64_bytes() {
        assert_eq!(
            std::mem::size_of::<PartitionSlot>(), 64,
            "PartitionSlot must be exactly 64 bytes (1 cache line)"
        );
    }

    #[test]
    fn alignment_is_64() {
        assert_eq!(
            std::mem::align_of::<PartitionSlot>(), 64,
            "PartitionSlot must be cache-line aligned"
        );
    }

    #[test]
    fn sequence_at_offset_zero() {
        assert_eq!(
            std::mem::offset_of!(PartitionSlot, sequence), 0,
            "sequence must be at offset 0 (Slot trait contract)"
        );
    }

    #[test]
    fn field_offsets() {
        assert_eq!(std::mem::offset_of!(PartitionSlot, gsn), 8);
        assert_eq!(std::mem::offset_of!(PartitionSlot, transfer_id), 16);
        assert_eq!(std::mem::offset_of!(PartitionSlot, account_id), 32);
        assert_eq!(std::mem::offset_of!(PartitionSlot, amount), 48);
        assert_eq!(std::mem::offset_of!(PartitionSlot, entry_type), 56);
        assert_eq!(std::mem::offset_of!(PartitionSlot, msg_type), 57);
        assert_eq!(std::mem::offset_of!(PartitionSlot, shard_id), 58);
        assert_eq!(std::mem::offset_of!(PartitionSlot, entry_index), 59);
        assert_eq!(std::mem::offset_of!(PartitionSlot, transfer_hash_table_offset), 60);
    }

    #[test]
    fn slot_trait_set_and_get_sequence() {
        let mut slot = PartitionSlot::zeroed();
        slot.set_sequence(42);
        assert_eq!(slot.sequence(), 42);
    }

    #[test]
    fn zeroed_all_fields_zero() {
        let slot = PartitionSlot::zeroed();
        assert_eq!(slot.sequence, 0);
        assert_eq!(slot.gsn, 0);
        assert_eq!(slot.transfer_id, [0u8; 16]);
        assert_eq!(slot.account_id, [0u8; 16]);
        assert_eq!(slot.amount, 0);
        assert_eq!(slot.entry_type, 0);
        assert_eq!(slot.msg_type, 0);
        assert_eq!(slot.shard_id, 0);
        assert_eq!(slot.entry_index, 0);
        assert_eq!(slot.transfer_hash_table_offset, 0);
    }

    #[test]
    fn entry_type_constants() {
        assert_eq!(ENTRY_TYPE_DEBIT, 1);
        assert_eq!(ENTRY_TYPE_CREDIT, 2);
    }

    #[test]
    fn msg_type_constants() {
        assert_eq!(MSG_TYPE_PREPARE, 1);
        assert_eq!(MSG_TYPE_COMMIT, 2);
        assert_eq!(MSG_TYPE_ROLLBACK, 3);
    }

    #[test]
    fn can_create_mpsc_ring_buffer() {
        let _rb = ringbuf::mpsc_ring_buffer::MpscRingBuffer::<PartitionSlot>::new(64)
            .expect("should create MPSC RB for PartitionSlot");
    }
}