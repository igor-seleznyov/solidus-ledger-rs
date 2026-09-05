use ringbuf::slot::Slot;

#[repr(C, align(8))]
#[derive(Clone, Copy, Debug)]
pub struct IncomingSlot {
    pub batch_id: [u8; 16],
    pub connection_id: u64,
    pub transfer_id: [u8; 16],
    pub idempotency_key: [u8; 16],
    pub debit_account_id: [u8; 16],
    pub credit_account_id: [u8; 16],
    pub amount: [u8; 8],
    pub currency: [u8; 16],
    pub transfer_sequence_id: [u8; 16],
    pub transfer_datetime: [u8; 8],
    pub _padding: [u8; 48],
}

impl Slot for IncomingSlot {}

impl IncomingSlot {
    pub fn zeroed() -> Self {
        Self {
            batch_id: [0u8; 16],
            connection_id: 0,
            transfer_id: [0u8; 16],
            idempotency_key: [0u8; 16],
            debit_account_id: [0u8; 16],
            credit_account_id: [0u8; 16],
            amount: [0u8; 8],
            currency: [0u8; 16],
            transfer_sequence_id: [0u8; 16],
            transfer_datetime: [0u8; 8],
            _padding: [0u8; 48],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_184_bytes() {
        assert_eq!(
            std::mem::size_of::<IncomingSlot>(), 184,
            "the payload fills three cache lines less the ring's own cell"
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
        assert_eq!(
            std::mem::align_of::<IncomingSlot>(), 8
        );
    }

    /// What the old `align(64)` on this type was really protecting: one
    /// message still occupies exactly three cache lines inside the ring,
    /// starting on a cache-line boundary.
    #[test]
    fn composes_into_three_cache_lines_inside_the_ring() {
        type Slot = ringbuf::slot::RbSlot<IncomingSlot>;
        assert_eq!(std::mem::size_of::<Slot>(), 192);
        assert_eq!(std::mem::align_of::<Slot>(), 64);
        assert_eq!(Slot::PAYLOAD_OFFSET, 8);
    }


    #[test]
    fn zeroed_all_fields_zero() {
        let slot = IncomingSlot::zeroed();
        assert_eq!(slot.batch_id, [0u8; 16]);
        assert_eq!(slot.connection_id, 0);
        assert_eq!(slot.transfer_id, [0u8; 16]);
        assert_eq!(slot.idempotency_key, [0u8; 16]);
        assert_eq!(slot.debit_account_id, [0u8; 16]);
        assert_eq!(slot.credit_account_id, [0u8; 16]);
        assert_eq!(slot.amount, [0u8; 8]);
        assert_eq!(slot.currency, [0u8; 16]);
        assert_eq!(slot.transfer_sequence_id, [0u8; 16]);
        assert_eq!(slot.transfer_datetime, [0u8; 8]);
    }



    #[test]
    fn transfer_fields_start_at_offset_24() {
        assert_eq!(std::mem::offset_of!(IncomingSlot, transfer_id), 24);
    }

    #[test]
    fn transfer_block_is_112_bytes() {
        let start = std::mem::offset_of!(IncomingSlot, transfer_id);
        let end = std::mem::offset_of!(IncomingSlot, _padding);
        assert_eq!(end - start, 112, "transfer block must match TRANSFER_BASE_SIZE");
    }
}