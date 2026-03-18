use std::sync::Arc;
use ringbuf::mpsc_ring_buffer::MpscRingBuffer;
use crate::incoming_slot::IncomingSlot;
use crate::partition_slot::PartitionSlot;

pub trait PipelineHandler {
    fn handle(
        &mut self,
        slot: &IncomingSlot,
        gsn: u64,
        partition_rb: &[Arc<MpscRingBuffer<PartitionSlot>>],
    );
}