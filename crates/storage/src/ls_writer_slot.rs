use ringbuf::slot::Slot;
use pipeline::posting_record::PostingRecord;

pub const LS_MSG_ADD_TO_HEAP: u8 = 1;
pub const LS_MSG_REMOVE_FROM_HEAP: u8 = 2;
pub const LS_MSG_POSTING: u8 = 3;
pub const LS_MSG_FLUSH_MARKER: u8 = 4;

#[repr(C, align(64))]
#[derive(Copy, Clone)]
pub struct LsWriterSlot {
    pub sequence: u64,
    pub msg_type: u8,
    pub _pad1: [u8; 3],
    pub transfer_hash_table_offset: u32,
    pub gsn: u64,
    pub transfer_id_hi: u64,
    pub transfer_id_lo: u64,
    pub _pad2: [u8; 24],
    
    pub posting: PostingRecord,
}

unsafe impl Slot for LsWriterSlot {
    fn sequence(&self) -> u64 {
        self.sequence
    }
    
    fn set_sequence(&mut self, seq: u64) {
        self.sequence = seq;
    }
}

impl LsWriterSlot {
    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_192_bytes() {
        assert_eq!(std::mem::size_of::<LsWriterSlot>(), 192);
    }

    #[test]
    fn alignment_is_64() {
        assert_eq!(std::mem::align_of::<LsWriterSlot>(), 64);
    }

    #[test]
    fn sequence_at_offset_zero() {
        assert_eq!(std::mem::offset_of!(LsWriterSlot, sequence), 0);
    }

    #[test]
    fn posting_at_offset_64() {
        assert_eq!(std::mem::offset_of!(LsWriterSlot, posting), 64);
    }

    #[test]
    fn msg_type_constants() {
        assert_eq!(LS_MSG_ADD_TO_HEAP, 1);
        assert_eq!(LS_MSG_REMOVE_FROM_HEAP, 2);
        assert_eq!(LS_MSG_POSTING, 3);
        assert_eq!(LS_MSG_FLUSH_MARKER, 4);
    }

    #[test]
    fn slot_trait_works() {
        let mut slot = LsWriterSlot::zeroed();
        slot.set_sequence(42);
        assert_eq!(slot.sequence(), 42);
    }

    #[test]
    fn can_create_mpsc_ring_buffer() {
        let _rb = ringbuf::mpsc_ring_buffer::MpscRingBuffer::<LsWriterSlot>::new(64)
            .expect("should create MPSC RB for LsWriterSlot");
    }
}