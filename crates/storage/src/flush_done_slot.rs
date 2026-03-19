use ringbuf::slot::Slot;

#[repr(C, align(64))]
#[derive(Copy, Clone)]
pub struct FlushDoneSlot {
    pub sequence: u64,
    pub transfer_id_hi: u64,
    pub transfer_id_lo: u64,
    pub transfer_hash_table_offset: u32,
    pub _pad: [u8; 36],
}

unsafe impl Slot for FlushDoneSlot {
    fn sequence(&self) -> u64 {
        self.sequence
    }

    fn set_sequence(&mut self, seq: u64) {
        self.sequence = seq;
    }
}

impl FlushDoneSlot {
    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_64_bytes() {
        assert_eq!(std::mem::size_of::<FlushDoneSlot>(), 64);
    }

    #[test]
    fn alignment_is_64() {
        assert_eq!(std::mem::align_of::<FlushDoneSlot>(), 64);
    }

    #[test]
    fn sequence_at_offset_zero() {
        assert_eq!(std::mem::offset_of!(FlushDoneSlot, sequence), 0);
    }

    #[test]
    fn field_offsets() {
        assert_eq!(std::mem::offset_of!(FlushDoneSlot, transfer_id_hi), 8);
        assert_eq!(std::mem::offset_of!(FlushDoneSlot, transfer_id_lo), 16);
        assert_eq!(std::mem::offset_of!(FlushDoneSlot, transfer_hash_table_offset), 24);
    }

    #[test]
    fn slot_trait_works() {
        let mut slot = FlushDoneSlot::zeroed();
        slot.set_sequence(42);
        assert_eq!(slot.sequence(), 42);
    }

    #[test]
    fn can_create_spsc_ring_buffer() {
        let _rb = ringbuf::mpsc_ring_buffer::MpscRingBuffer::<FlushDoneSlot>::new(64)
            .expect("should create RB for FlushDoneSlot");
    }
}