use ringbuf::slot::Slot;

#[repr(C, align(8))]
#[derive(Copy, Clone)]
pub struct FlushDoneSlot {
    pub transfer_id_hi: u64,
    pub transfer_id_lo: u64,
    pub transfer_hash_table_offset: u32,
    pub _pad: [u8; 36],
}

impl Slot for FlushDoneSlot {}

impl FlushDoneSlot {
    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_56_bytes() {
        assert_eq!(
            std::mem::size_of::<FlushDoneSlot>(), 
            56,
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
        assert_eq!(std::mem::align_of::<FlushDoneSlot>(), 8);
    }

    /// What the old `align(64)` on this type was really protecting: one
    /// message still occupies exactly one cache line inside the ring,
    /// starting on a cache-line boundary.
    #[test]
    fn composes_into_one_cache_line_inside_the_ring() {
        type Slot = ringbuf::slot::RbSlot<FlushDoneSlot>;
        assert_eq!(std::mem::size_of::<Slot>(), 64);
        assert_eq!(std::mem::align_of::<Slot>(), 64);
        assert_eq!(Slot::PAYLOAD_OFFSET, 8);
    }


    #[test]
    fn field_offsets() {
        assert_eq!(std::mem::offset_of!(FlushDoneSlot, transfer_id_hi), 0);
        assert_eq!(std::mem::offset_of!(FlushDoneSlot, transfer_id_lo), 8);
        assert_eq!(std::mem::offset_of!(FlushDoneSlot, transfer_hash_table_offset), 16);
    }


    #[test]
    fn can_create_spsc_ring_buffer() {
        let _rb = ringbuf::mpsc_ring_buffer::MpscRingBuffer::<FlushDoneSlot>::new(64)
            .expect("should create RB for FlushDoneSlot");
    }
}