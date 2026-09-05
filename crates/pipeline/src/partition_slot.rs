use ringbuf::slot::Slot;

pub const ENTRY_TYPE_DEBIT: u8 = 1;
pub const ENTRY_TYPE_CREDIT: u8 = 2;

pub const MSG_TYPE_PREPARE: u8 = 1;
pub const MSG_TYPE_COMMIT: u8 = 2;
pub const MSG_TYPE_ROLLBACK: u8 = 3;

#[repr(C, align(8))]
#[derive(Copy, Clone)]
pub struct PartitionSlot {
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

impl Slot for PartitionSlot {}

impl PartitionSlot {
    pub fn zeroed() -> Self {
        Self {
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
    fn size_is_56_bytes() {
        assert_eq!(
            std::mem::size_of::<PartitionSlot>(), 56,
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
        assert_eq!(std::mem::align_of::<PartitionSlot>(), 8);
    }

    /// What the old `align(64)` on this type was really protecting: one
    /// message still occupies exactly one cache line inside the ring,
    /// starting on a cache-line boundary.
    #[test]
    fn composes_into_one_cache_line_inside_the_ring() {
        type Slot = ringbuf::slot::RbSlot<PartitionSlot>;
        assert_eq!(std::mem::size_of::<Slot>(), 64);
        assert_eq!(std::mem::align_of::<Slot>(), 64);
        assert_eq!(Slot::PAYLOAD_OFFSET, 8);
    }


    #[test]
    fn field_offsets() {
        assert_eq!(std::mem::offset_of!(PartitionSlot, gsn), 0);
        assert_eq!(std::mem::offset_of!(PartitionSlot, transfer_id), 8);
        assert_eq!(std::mem::offset_of!(PartitionSlot, account_id), 24);
        assert_eq!(std::mem::offset_of!(PartitionSlot, amount), 40);
        assert_eq!(std::mem::offset_of!(PartitionSlot, entry_type), 48);
        assert_eq!(std::mem::offset_of!(PartitionSlot, msg_type), 49);
        assert_eq!(std::mem::offset_of!(PartitionSlot, shard_id), 50);
        assert_eq!(std::mem::offset_of!(PartitionSlot, entry_index), 51);
        assert_eq!(std::mem::offset_of!(PartitionSlot, transfer_hash_table_offset), 52);
    }


    #[test]
    fn zeroed_all_fields_zero() {
        let slot = PartitionSlot::zeroed();
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