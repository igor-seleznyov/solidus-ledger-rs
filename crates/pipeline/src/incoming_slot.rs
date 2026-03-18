use ringbuf::slot::Slot;

#[repr(C, align(64))]
#[derive(Clone, Copy, Debug)]
pub struct IncomingSlot {
    pub sequence: u64,
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
    pub _padding: [u8; 40],
}

unsafe impl Slot for IncomingSlot {
    fn sequence(&self) -> u64 {
        self.sequence
    }

    fn set_sequence(&mut self, seq: u64) {
        self.sequence = seq;
    }
}

impl IncomingSlot {
    pub fn zeroed() -> Self {
        Self {
            sequence: 0,
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
            _padding: [0u8; 40],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_192_bytes() {
        assert_eq!(
            std::mem::size_of::<IncomingSlot>(), 192,
            "IncomingSlot must be exactly 192 bytes (3 cache lines)"
        );
    }

    #[test]
    fn alignment_is_64() {
        assert_eq!(
            std::mem::align_of::<IncomingSlot>(), 64,
            "IncomingSlot must be cache-line aligned (64 bytes)"
        );
    }

    #[test]
    fn sequence_at_offset_zero() {
        assert_eq!(
            std::mem::offset_of!(IncomingSlot, sequence), 0,
            "sequence must be at offset 0 (Slot trait contract)"
        );
    }

    #[test]
    fn zeroed_all_fields_zero() {
        let slot = IncomingSlot::zeroed();
        assert_eq!(slot.sequence, 0);
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
    fn slot_trait_set_and_get_sequence() {
        let mut slot = IncomingSlot::zeroed();
        slot.set_sequence(42);
        assert_eq!(slot.sequence(), 42);
        assert_eq!(slot.sequence, 42);
    }

    #[test]
    fn slot_trait_sequence_does_not_affect_other_fields() {
        let mut slot = IncomingSlot::zeroed();
        slot.amount = [0, 0, 0, 0, 0, 0, 0x03, 0xE8];
        slot.connection_id = 7;
        slot.set_sequence(99);
        slot.amount = [0, 0, 0, 0, 0, 0, 0x03, 0xE8];
        assert_eq!(slot.connection_id, 7);
    }

    #[test]
    fn transfer_fields_start_at_offset_32() {
        assert_eq!(std::mem::offset_of!(IncomingSlot, transfer_id), 32);
    }

    #[test]
    fn transfer_block_is_112_bytes() {
        let start = std::mem::offset_of!(IncomingSlot, transfer_id);
        let end = std::mem::offset_of!(IncomingSlot, _padding);
        assert_eq!(end - start, 112, "transfer block must match TRANSFER_BASE_SIZE");
    }
}