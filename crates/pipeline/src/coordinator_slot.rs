use ringbuf::slot::Slot;

pub const COORD_PREPARE_SUCCESS: u8 = 0;
pub const COORD_PREPARE_FAIL: u8 = 1;
pub const COORD_COMMIT_SUCCESS: u8 = 2;
pub const COORD_ROLLBACK_SUCCESS: u8 = 3;

#[repr(C, align(8))]
#[derive(Copy, Clone)]
pub struct CoordinatorSlot {
    pub msg_type: u8,
    pub shard_id: u8,
    pub reason: u8,
    pub entry_index: u8,
    pub partition_id: u32,
    pub transfer_hash_table_offset: u32,
    pub _pad2: [u8; 4],
    pub transfer_id_hi: u64,
    pub transfer_id_lo: u64,
    pub gsn: u64,
    pub _pad3: [u8; 16],
}

impl Slot for CoordinatorSlot {}

impl CoordinatorSlot {
    pub fn zeroed() -> Self {
        Self {
            msg_type: 0,
            shard_id: 0,
            reason: 0,
            entry_index: 0,
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
#[cfg(not(miri))]
mod tests {
    use super::*;

    #[test]
    fn size_is_56_bytes() {
        assert_eq!(
            std::mem::size_of::<CoordinatorSlot>(), 56,
            "the payload fills one cache line less the ring's own cell"
        );
    }

    /// The payload aligns to eight, not to a cache line.
    ///
    /// The cache-line alignment moved to the ring's container along with
    /// the publication cell. A payload demanding 64 here would push its own
    /// start past the container's fixed payload offset of 8 and silently
    /// break every reader that computes that offset once, which is why the
    /// container asserts the limit rather than trusting it.
    #[test]
    fn alignment_is_8() {
        assert_eq!(std::mem::align_of::<CoordinatorSlot>(), 8);
    }

    /// What the old `align(64)` on this type was really protecting: one
    /// message still occupies exactly one cache line inside the ring,
    /// starting on a cache-line boundary.
    #[test]
    fn composes_into_one_cache_line_inside_the_ring() {
        type Slot = ringbuf::slot::RbSlot<CoordinatorSlot>;
        assert_eq!(std::mem::size_of::<Slot>(), 64);
        assert_eq!(std::mem::align_of::<Slot>(), 64);
        assert_eq!(Slot::PAYLOAD_OFFSET, 8);
    }


    #[test]
    fn field_offsets() {
        assert_eq!(std::mem::offset_of!(CoordinatorSlot, msg_type), 0);
        assert_eq!(std::mem::offset_of!(CoordinatorSlot, shard_id), 1);
        assert_eq!(std::mem::offset_of!(CoordinatorSlot, reason), 2);
        assert_eq!(std::mem::offset_of!(CoordinatorSlot, entry_index), 3);
        assert_eq!(std::mem::offset_of!(CoordinatorSlot, partition_id), 4);
        assert_eq!(std::mem::offset_of!(CoordinatorSlot, transfer_hash_table_offset), 8);
        assert_eq!(std::mem::offset_of!(CoordinatorSlot, transfer_id_hi), 16);
        assert_eq!(std::mem::offset_of!(CoordinatorSlot, transfer_id_lo), 24);
        assert_eq!(std::mem::offset_of!(CoordinatorSlot, gsn), 32);
    }


    #[test]
    fn zeroed_all_fields_zero() {
        let slot = CoordinatorSlot::zeroed();
        assert_eq!(slot.msg_type, 0);
        assert_eq!(slot.partition_id, 0);
        assert_eq!(slot.shard_id, 0);
        assert_eq!(slot.reason, 0);
        assert_eq!(slot.entry_index, 0);
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