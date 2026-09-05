use ringbuf::slot::Slot;
use pipeline::posting_record::PostingRecord;

pub const LS_MSG_ADD_TO_HEAP: u8 = 1;
pub const LS_MSG_REMOVE_FROM_HEAP: u8 = 2;
pub const LS_MSG_POSTING: u8 = 3;
pub const LS_MSG_FLUSH_MARKER: u8 = 4;

#[repr(C, align(8))]
#[derive(Copy, Clone)]
pub struct LsWriterSlot {
    pub msg_type: u8,
    pub _pad1: [u8; 3],
    pub transfer_hash_table_offset: u32,
    pub gsn: u64,
    pub transfer_id_hi: u64,
    pub transfer_id_lo: u64,
    pub _pad2: [u8; 24],
    
    pub posting: PostingRecord,
}

impl Slot for LsWriterSlot {}

impl LsWriterSlot {
    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_184_bytes() {
        assert_eq!(
            std::mem::size_of::<LsWriterSlot>(), 184,
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
        assert_eq!(std::mem::align_of::<LsWriterSlot>(), 8);
    }

    /// What the old `align(64)` on this type was really protecting: one
    /// message still occupies exactly three cache lines inside the ring,
    /// and the posting record inside it still starts on the second line.
    #[test]
    fn composes_into_three_cache_lines_inside_the_ring() {
        type Slot = ringbuf::slot::RbSlot<LsWriterSlot>;
        assert_eq!(std::mem::size_of::<Slot>(), 192);
        assert_eq!(std::mem::align_of::<Slot>(), 64);
        assert_eq!(Slot::PAYLOAD_OFFSET, 8);
        assert_eq!(
            Slot::PAYLOAD_OFFSET + std::mem::offset_of!(LsWriterSlot, posting), 64,
            "the posting record must still begin on a cache-line boundary",
        );
    }


    #[test]
    fn posting_at_offset_56() {
        assert_eq!(std::mem::offset_of!(LsWriterSlot, posting), 56);
    }

    #[test]
    fn msg_type_constants() {
        assert_eq!(LS_MSG_ADD_TO_HEAP, 1);
        assert_eq!(LS_MSG_REMOVE_FROM_HEAP, 2);
        assert_eq!(LS_MSG_POSTING, 3);
        assert_eq!(LS_MSG_FLUSH_MARKER, 4);
    }


    #[test]
    fn can_create_mpsc_ring_buffer() {
        let _rb = ringbuf::mpsc_ring_buffer::MpscRingBuffer::<LsWriterSlot>::new(64)
            .expect("should create MPSC RB for LsWriterSlot");
    }
}