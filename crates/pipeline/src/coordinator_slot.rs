use ringbuf::slot::Slot;

pub const COORD_PREPARE_SUCCESS: u8 = 0;
pub const COORD_PREPARE_FAIL: u8 = 1;
pub const COORD_COMMIT_SUCCESS: u8 = 2;
pub const COORD_ROLLBACK_SUCCESS: u8 = 3;

#[repr(C, align(64))]
#[derive(Copy, Clone)]
pub struct CoordinatorSlot {
    pub sequence: u64,
    pub msg_type: u8,
    pub shard_id: u8,
    pub reason: u8,
    pub _pad1: u8,
    pub partition_id: u32,
    pub transfer_hash_table_offset: u32,
    pub _pad2: [u8; 4],
    pub transfer_id_hi: u64,
    pub transfer_id_lo: u64,
    pub gsn: u64,
    pub _pad3: [u8; 16],
}

unsafe impl Slot for CoordinatorSlot {
    fn sequence(&self) -> u64 {
        self.sequence
    }

    fn set_sequence(&mut self, seq: u64) {
        self.sequence = seq;
    }
}

impl CoordinatorSlot {
    pub fn zeroed() -> Self {
        Self {
            sequence: 0,
            msg_type: 0,
            shard_id: 0,
            reason: 0,
            _pad1: 0,
            partition_id: 0,
            transfer_hash_table_offset: 0,
            _pad2: [0u8; 4],
            transfer_id_hi: 0,
            transfer_id_lo: 0,
            gsn: 0,
            _pad3: [0u8; 16],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_64_bytes() {
        assert_eq!(
            std::mem::size_of::<CoordinatorSlot>(), 64,
            "CoordinatorSlot must be exactly 64 bytes (1 cache line)"
        );
    }

    #[test]
    fn alignment_is_64() {
        assert_eq!(std::mem::align_of::<CoordinatorSlot>(), 64);
    }

    #[test]
    fn sequence_at_offset_zero() {
        assert_eq!(
            std::mem::offset_of!(CoordinatorSlot, sequence), 0,
            "sequence must be at offset 0 (Slot trait contract)"
        );
    }

    #[test]
    fn field_offsets() {
        assert_eq!(std::mem::offset_of!(CoordinatorSlot, sequence), 0);
        assert_eq!(std::mem::offset_of!(CoordinatorSlot, msg_type), 8);
        assert_eq!(std::mem::offset_of!(CoordinatorSlot, shard_id), 9);
        assert_eq!(std::mem::offset_of!(CoordinatorSlot, reason), 10);
        assert_eq!(std::mem::offset_of!(CoordinatorSlot, partition_id), 12);
        assert_eq!(std::mem::offset_of!(CoordinatorSlot, transfer_hash_table_offset), 16);
        assert_eq!(std::mem::offset_of!(CoordinatorSlot, transfer_id_hi), 24);
        assert_eq!(std::mem::offset_of!(CoordinatorSlot, transfer_id_lo), 32);
        assert_eq!(std::mem::offset_of!(CoordinatorSlot, gsn), 40);
    }

    #[test]
    fn slot_trait_set_and_get_sequence() {
        let mut slot = CoordinatorSlot::zeroed();
        slot.set_sequence(42);
        assert_eq!(slot.sequence(), 42);
    }

    #[test]
    fn zeroed_all_fields_zero() {
        let slot = CoordinatorSlot::zeroed();
        assert_eq!(slot.sequence, 0);
        assert_eq!(slot.msg_type, 0);
        assert_eq!(slot.partition_id, 0);
        assert_eq!(slot.shard_id, 0);
        assert_eq!(slot.reason, 0);
        assert_eq!(slot.transfer_hash_table_offset, 0);
        assert_eq!(slot.transfer_id_hi, 0);
        assert_eq!(slot.transfer_id_lo, 0);
        assert_eq!(slot.gsn, 0);
    }

    #[test]
    fn msg_type_constants() {
        assert_eq!(COORD_PREPARE_SUCCESS, 0);
        assert_eq!(COORD_PREPARE_FAIL, 1);
        assert_eq!(COORD_COMMIT_SUCCESS, 2);
        assert_eq!(COORD_ROLLBACK_SUCCESS, 3);
    }

    #[test]
    fn can_create_mpsc_ring_buffer() {
        let _rb = ringbuf::mpsc_ring_buffer::MpscRingBuffer::<CoordinatorSlot>::new(64)
            .expect("should create MPSC RB for CoordinatorSlot");
    }
}