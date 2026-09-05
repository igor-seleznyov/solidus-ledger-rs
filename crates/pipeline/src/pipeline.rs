use ringbuf::mpsc_ring_buffer::MpscRingBuffer;
use crate::incoming_slot::IncomingSlot;
use crate::partition_slot::PartitionSlot;
use crate::sequencer::Sequencer;
use crate::pipeline_handler::PipelineHandler;

pub struct Pipeline<'scope, H: PipelineHandler> {
    id: usize,
    incoming: &'scope MpscRingBuffer<IncomingSlot>,
    sequencer: Sequencer,
    batch_size: usize,
    partition_rb: &'scope [MpscRingBuffer<PartitionSlot>],
    handler: H,
}

impl <'scope, H: PipelineHandler> Pipeline<'scope, H> {
    pub fn new(
        id: usize,
        incoming: &'scope MpscRingBuffer<IncomingSlot>,
        batch_size: usize,
        partition_rb: &'scope [MpscRingBuffer<PartitionSlot>],
        handler: H,
    ) -> Self {
        Self {
            id,
            incoming,
            sequencer: Sequencer::new(),
            batch_size,
            partition_rb,
            handler,
        }
    }

    pub fn run(&mut self) {
        println!("[pipeline {}] started", self.id);

        loop {
            let batch = self.incoming.drain_batch(self.batch_size);

            if batch.is_empty() {
                std::hint::spin_loop();
                continue;
            }

            for i in 0..batch.len() {
                let slot = batch.slot(i);
                let gsn = self.sequencer.next();

                self.handler.handle(slot, gsn, self.partition_rb);
            }

            batch.release();
        }
    }
}