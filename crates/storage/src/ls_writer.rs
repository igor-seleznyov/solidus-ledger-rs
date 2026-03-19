use std::sync::Arc;
use std::time::Instant;
use ringbuf::mpsc_ring_buffer::MpscRingBuffer;
use pipeline::posting_record::PostingRecord;
use pipeline::in_flight_min_heap::InFlightMinHeap;
use crate::ls_writer_slot::*;
use crate::flush_done_slot::FlushDoneSlot;
use crate::flush_backend::FlushBackend;
use common::mem_barrier::release_store_u64;

const CLOCK_CHECK_REPEATS_COUNT_INTERVAL: u32 = 100_000;

struct PendingFlush {
    transfer_id_hi: u64,
    transfer_id_lo: u64,
    transfer_hash_table_offset: u32,
    gsn: u64,
}

pub struct LsWriter<T: FlushBackend> {
    id: usize,
    ls_writer_rb: Arc<MpscRingBuffer<LsWriterSlot>>,
    flush_done_rb: Arc<MpscRingBuffer<FlushDoneSlot>>,
    global_committed_gsn: *mut u64,
    backend: T,
    in_flight_min_heap: InFlightMinHeap,
    batch_size: usize,

    buffer: Vec<u8>,
    write_offset: u64,

    flush_timeout_ms: u64,
    flush_max_buffer_size: usize,
    pending_flush: Vec<PendingFlush>,

    idle_count: u32,
    last_flush: Instant,

    flush_in_flight: bool,
    flush_pending_records: Vec<PendingFlush>,
    flush_buffer_snapshot_len: usize,
}

impl<T: FlushBackend> LsWriter<T> {
    pub fn new(
        id: usize,
        ls_writer_rb: Arc<MpscRingBuffer<LsWriterSlot>>,
        flush_done_rb: Arc<MpscRingBuffer<FlushDoneSlot>>,
        global_committed_gsn: *mut u64,
        mut backend: T,
        ls_file_path: &str,
        in_flight_min_heap_capacity: usize,
        in_flight_min_heap_seed_k0: u64,
        in_flight_min_heap_seed_k1: u64,
        batch_size: usize,
        flush_timeout_ms: u64,
        flush_max_buffer_posting_records: usize,
    ) -> std::io::Result<LsWriter<T>> {
        backend.open_ls_file(ls_file_path)?;

        let flush_max_buffer_bytes = flush_max_buffer_posting_records * PostingRecord::SIZE;

        Ok(
            Self {
                id,
                ls_writer_rb,
                flush_done_rb,
                global_committed_gsn,
                backend,
                in_flight_min_heap: InFlightMinHeap::new(
                    in_flight_min_heap_capacity,
                    in_flight_min_heap_seed_k0,
                    in_flight_min_heap_seed_k1,
                ),
                batch_size,
                buffer: Vec::with_capacity(flush_max_buffer_bytes * 2),
                write_offset: 0, //TODO: read file size on recovery
                flush_timeout_ms,
                flush_max_buffer_size: flush_max_buffer_bytes,
                pending_flush: Vec::with_capacity(256),
                idle_count: 0,
                last_flush: Instant::now(),
                flush_in_flight: false,
                flush_pending_records: Vec::with_capacity(256),
                flush_buffer_snapshot_len: 0,
            }
        )
    }

    pub fn run(&mut self) {
        println!("[ls-writer {}] started", self.id);

        loop {
            let batch = self.ls_writer_rb.drain_batch(self.batch_size);

            if batch.is_empty() {
                self.poll_and_handle_completions();

                if !self.buffer.is_empty() && !self.flush_in_flight {
                    self.idle_count += 1;
                    if self.idle_count >= CLOCK_CHECK_REPEATS_COUNT_INTERVAL {
                        if self.last_flush.elapsed().as_millis() >= self.flush_timeout_ms as u128 {
                            self.submit_flush();
                        }
                        self.idle_count = 0;
                    }
                }
                std::hint::spin_loop();
                continue;
            }

            self.idle_count = 0;

            for i in 0..batch.len() {
                let slot = batch.slot(i);
                self.process_message(slot);
            }

            batch.release();

            self.poll_and_handle_completions();

            if self.buffer.len() >= self.flush_max_buffer_size && !self.flush_in_flight {
                self.submit_flush();
            }
        }
    }

    pub fn submit_flush(&mut self) {
        if self.buffer.is_empty() || self.flush_in_flight {
            return;
        }

        self.backend
            .submit_write_and_sync(&self.buffer, self.write_offset)
            .expect("Failed to LS submit flush");

        self.flush_in_flight = true;
        self.flush_buffer_snapshot_len = self.buffer.len();

        self.flush_pending_records = std::mem::take(&mut self.pending_flush);

        self.last_flush = Instant::now();
        self.idle_count = 0;
    }

    pub fn poll_and_handle_completions(&mut self) {
        if !self.flush_in_flight {
            return;
        }

        let completion = match self.backend.poll_completion() {
            Some(completion) => completion,
            None => return,
        };

        assert!(completion.success, "LS flush failed");

        self.write_offset += self.flush_buffer_snapshot_len as u64;

        self.buffer.drain(..self.flush_buffer_snapshot_len);

        for pending in self.flush_pending_records.drain(..) {
            self.in_flight_min_heap.remove(pending.gsn);

            let mut claimed = self.flush_done_rb.claim();
            let slot = claimed.as_mut();
            slot.transfer_id_hi = pending.transfer_id_hi;
            slot.transfer_id_lo = pending.transfer_id_lo;
            slot.transfer_hash_table_offset = pending.transfer_hash_table_offset;
            claimed.publish();
        }

        self.maybe_advance_committed_gsn();

        self.flush_in_flight = false;
    }

    pub fn process_message(&mut self, slot: &LsWriterSlot) {
        match slot.msg_type {
            LS_MSG_ADD_TO_HEAP => {
                self.in_flight_min_heap.add(slot.gsn);
            }
            LS_MSG_REMOVE_FROM_HEAP => {
                self.in_flight_min_heap.remove(slot.gsn);
                self.maybe_advance_committed_gsn();
            }
            LS_MSG_POSTING => {
                let posting_bytes = unsafe {
                    std::slice::from_raw_parts(
                        &slot.posting as *const PostingRecord as *const u8,
                        PostingRecord::SIZE,
                    )
                };
                self.buffer.extend_from_slice(posting_bytes);
            }
            LS_MSG_FLUSH_MARKER => {
                self.pending_flush.push(
                    PendingFlush {
                        transfer_id_hi: slot.transfer_id_hi,
                        transfer_id_lo: slot.transfer_id_lo,
                        transfer_hash_table_offset: slot.transfer_hash_table_offset,
                        gsn: slot.gsn,
                    }
                );
            }
            _ => {}
        }
    }

    fn maybe_advance_committed_gsn(&self) {
        let new_committed = match self.in_flight_min_heap.min() {
            Some(min_in_flight) => min_in_flight - 1,
            None => self.in_flight_min_heap.last_removed_gsn(),
        };

        unsafe {
            release_store_u64(self.global_committed_gsn, new_committed);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use super::*;
    use crate::flush_backend::{FlushBackend, FlushCompletion};
    use pipeline::posting_record::PostingRecord;
    use ringbuf::mpsc_ring_buffer::MpscRingBuffer;
    use crate::flush_done_slot::FlushDoneSlot;
    use crate::ls_writer::LsWriter;
    use crate::ls_writer_slot::{LsWriterSlot, LS_MSG_ADD_TO_HEAP, LS_MSG_FLUSH_MARKER, LS_MSG_POSTING, LS_MSG_REMOVE_FROM_HEAP};

    const K0: u64 = 0x0123456789ABCDEF;
    const K1: u64 = 0xFEDCBA9876543210;

    // --- Mock FlushBackend ---

    struct MockFlushBackend {
        written: Vec<(Vec<u8>, u64)>,
        pending: Option<FlushCompletion>,
        open_called: bool,
    }

    impl MockFlushBackend {
        fn new() -> Self {
            Self {
                written: Vec::new(),
                pending: None,
                open_called: false,
            }
        }
    }

    impl FlushBackend for MockFlushBackend {
        fn open_ls_file(&mut self, _path: &str) -> std::io::Result<()> {
            self.open_called = true;
            Ok(())
        }

        fn submit_write_and_sync(&mut self, data: &[u8], offset: u64) -> std::io::Result<()> {
            let len = data.len();
            self.written.push((data.to_vec(), offset));
            self.pending = Some(FlushCompletion {
                bytes_written: len,
                success: true,
            });
            Ok(())
        }

        fn poll_completion(&mut self) -> Option<FlushCompletion> {
            self.pending.take()
        }

        fn close(&mut self) {}
    }


    static mut TEST_COMMITTED_GSN: u64 = 0;

    fn make_writer() -> LsWriter<MockFlushBackend> {
        let ls_writer_rb = Arc::new(
            MpscRingBuffer::<LsWriterSlot>::new(64).unwrap()
        );
        let flush_done_rb = Arc::new(
            MpscRingBuffer::<FlushDoneSlot>::new(64).unwrap()
        );

        unsafe { TEST_COMMITTED_GSN = 0; }
        let committed_gsn_ptr = unsafe { &raw mut TEST_COMMITTED_GSN };

        LsWriter::new(
            0,
            ls_writer_rb,
            flush_done_rb,
            committed_gsn_ptr,
            MockFlushBackend::new(),
            "test.ls",
            64,
            K0, K1,
            64,
            2,
            512,
        ).unwrap()
    }

    fn make_writer_with_flush_done_rb() -> (
        LsWriter<MockFlushBackend>,
        Arc<MpscRingBuffer<FlushDoneSlot>>,
    ) {
        let ls_writer_rb = Arc::new(
            MpscRingBuffer::<LsWriterSlot>::new(64).unwrap()
        );
        let flush_done_rb = Arc::new(
            MpscRingBuffer::<FlushDoneSlot>::new(64).unwrap()
        );
        let flush_done_rb_clone = Arc::clone(&flush_done_rb);

        unsafe { TEST_COMMITTED_GSN = 0; }
        let committed_gsn_ptr = unsafe { &raw mut TEST_COMMITTED_GSN };

        let writer = LsWriter::new(
            0,
            ls_writer_rb,
            flush_done_rb,
            committed_gsn_ptr,
            MockFlushBackend::new(),
            "test.ls",
            64, K0, K1,
            64, 2, 512,
        ).unwrap();

        (writer, flush_done_rb_clone)
    }

    fn make_add_to_heap_slot(gsn: u64) -> LsWriterSlot {
        let mut slot = LsWriterSlot::zeroed();
        slot.msg_type = LS_MSG_ADD_TO_HEAP;
        slot.gsn = gsn;
        slot
    }

    fn make_remove_from_heap_slot(gsn: u64) -> LsWriterSlot {
        let mut slot = LsWriterSlot::zeroed();
        slot.msg_type = LS_MSG_REMOVE_FROM_HEAP;
        slot.gsn = gsn;
        slot
    }

    fn make_posting_slot(gsn: u64, amount: i64) -> LsWriterSlot {
        let mut slot = LsWriterSlot::zeroed();
        slot.msg_type = LS_MSG_POSTING;
        slot.gsn = gsn;
        slot.posting.gsn = gsn;
        slot.posting.amount = amount;
        slot.posting.transfer_id_hi = 0;
        slot.posting.transfer_id_lo = gsn; // для простоты transfer_id_lo = gsn
        slot
    }

    fn make_flush_marker_slot(gsn: u64, transfer_id_lo: u64, tht_offset: u32) -> LsWriterSlot {
        let mut slot = LsWriterSlot::zeroed();
        slot.msg_type = LS_MSG_FLUSH_MARKER;
        slot.gsn = gsn;
        slot.transfer_id_hi = 0;
        slot.transfer_id_lo = transfer_id_lo;
        slot.transfer_hash_table_offset = tht_offset;
        slot
    }

    // --- Tests

    #[test]
    fn new_opens_file() {
        let writer = make_writer();
        assert!(writer.backend.open_called);
    }

    #[test]
    fn new_initializes_empty_state() {
        let writer = make_writer();
        assert_eq!(writer.buffer.len(), 0);
        assert_eq!(writer.write_offset, 0);
        assert!(!writer.flush_in_flight);
        assert_eq!(writer.flush_buffer_snapshot_len, 0);
        assert_eq!(writer.pending_flush.len(), 0);
        assert_eq!(writer.flush_pending_records.len(), 0);
    }

    #[test]
    fn process_add_to_heap_adds_to_ifmh() {
        let mut writer = make_writer();

        writer.process_message(&make_add_to_heap_slot(100));
        writer.process_message(&make_add_to_heap_slot(200));

        assert_eq!(writer.in_flight_min_heap.len(), 2);
        assert_eq!(writer.in_flight_min_heap.min(), Some(100));
    }

    #[test]
    fn process_remove_from_heap_removes_from_ifmh() {
        let mut writer = make_writer();

        writer.process_message(&make_add_to_heap_slot(100));
        writer.process_message(&make_add_to_heap_slot(200));
        writer.process_message(&make_remove_from_heap_slot(100));

        assert_eq!(writer.in_flight_min_heap.len(), 1);
        assert_eq!(writer.in_flight_min_heap.min(), Some(200));
    }

    #[test]
    fn process_posting_appends_to_buffer() {
        let mut writer = make_writer();

        writer.process_message(&make_posting_slot(100, 500));

        assert_eq!(writer.buffer.len(), PostingRecord::SIZE); // 128 байт
    }

    #[test]
    fn process_multiple_postings_accumulates_buffer() {
        let mut writer = make_writer();

        writer.process_message(&make_posting_slot(100, 500));
        writer.process_message(&make_posting_slot(200, 300));
        writer.process_message(&make_posting_slot(300, 100));

        assert_eq!(writer.buffer.len(), PostingRecord::SIZE * 3); // 384 байт
    }

    #[test]
    fn process_flush_marker_adds_to_pending() {
        let mut writer = make_writer();

        writer.process_message(&make_flush_marker_slot(100, 1, 42));

        assert_eq!(writer.pending_flush.len(), 1);
    }

    #[test]
    fn submit_flush_sends_buffer_to_backend() {
        let mut writer = make_writer();

        writer.process_message(&make_posting_slot(100, 500));
        writer.process_message(&make_flush_marker_slot(100, 1, 42));

        writer.submit_flush();

        assert_eq!(writer.backend.written.len(), 1);
        assert_eq!(writer.backend.written[0].0.len(), PostingRecord::SIZE);
        assert_eq!(writer.backend.written[0].1, 0); // offset = 0 (первая запись)
    }

    #[test]
    fn submit_flush_sets_in_flight() {
        let mut writer = make_writer();

        writer.process_message(&make_posting_slot(100, 500));
        writer.submit_flush();

        assert!(writer.flush_in_flight);
        assert_eq!(writer.flush_buffer_snapshot_len, PostingRecord::SIZE);
    }

    #[test]
    fn submit_flush_moves_pending_to_flush_pending_records() {
        let mut writer = make_writer();

        writer.process_message(&make_flush_marker_slot(100, 1, 42));
        writer.process_message(&make_flush_marker_slot(200, 2, 43));
        writer.process_message(&make_posting_slot(100, 500));

        writer.submit_flush();

        assert_eq!(writer.pending_flush.len(), 0);
        assert_eq!(writer.flush_pending_records.len(), 2);
    }

    #[test]
    fn submit_flush_ignores_empty_buffer() {
        let mut writer = make_writer();

        writer.submit_flush();

        assert!(!writer.flush_in_flight);
        assert_eq!(writer.backend.written.len(), 0);
    }

    #[test]
    fn submit_flush_ignores_if_already_in_flight() {
        let mut writer = make_writer();

        writer.process_message(&make_posting_slot(100, 500));
        writer.submit_flush();

        writer.process_message(&make_posting_slot(200, 300));
        writer.submit_flush();

        assert_eq!(writer.backend.written.len(), 1);
    }

    #[test]
    fn poll_completions_clears_buffer_and_advances_offset() {
        let mut writer = make_writer();

        writer.process_message(&make_posting_slot(100, 500));
        writer.submit_flush();

        assert_eq!(writer.write_offset, 0);
        assert_eq!(writer.buffer.len(), PostingRecord::SIZE);

        writer.poll_and_handle_completions();

        assert_eq!(writer.write_offset, PostingRecord::SIZE as u64);
        assert_eq!(writer.buffer.len(), 0);
        assert!(!writer.flush_in_flight);
    }

    #[test]
    fn poll_completions_removes_gsn_from_ifmh() {
        let mut writer = make_writer();

        writer.process_message(&make_add_to_heap_slot(100));
        writer.process_message(&make_add_to_heap_slot(200));
        writer.process_message(&make_posting_slot(100, 500));
        writer.process_message(&make_flush_marker_slot(100, 1, 42));

        writer.submit_flush();
        writer.poll_and_handle_completions();

        assert_eq!(writer.in_flight_min_heap.len(), 1);
        assert_eq!(writer.in_flight_min_heap.min(), Some(200));
    }

    #[test]
    fn poll_completions_sends_flush_done() {
        let (mut writer, flush_done_rb) = make_writer_with_flush_done_rb();

        writer.process_message(&make_add_to_heap_slot(100));
        writer.process_message(&make_posting_slot(100, 500));
        writer.process_message(&make_flush_marker_slot(100, 1, 42));

        writer.submit_flush();
        writer.poll_and_handle_completions();

        let batch = flush_done_rb.drain_batch(64);
        assert_eq!(batch.len(), 1);

        let done_slot = batch.slot(0);
        assert_eq!(done_slot.transfer_id_hi, 0);
        assert_eq!(done_slot.transfer_id_lo, 1);
        assert_eq!(done_slot.transfer_hash_table_offset, 42);

        batch.release();
    }

    #[test]
    fn poll_completions_advances_committed_gsn() {
        let mut writer = make_writer();

        writer.process_message(&make_add_to_heap_slot(100));
        writer.process_message(&make_add_to_heap_slot(200));
        writer.process_message(&make_posting_slot(100, 500));
        writer.process_message(&make_flush_marker_slot(100, 1, 42));

        writer.submit_flush();
        writer.poll_and_handle_completions();

        let committed = unsafe { *writer.global_committed_gsn };
        assert_eq!(committed, 199);
    }

    #[test]
    fn committed_gsn_equals_last_removed_when_ifmh_empty() {
        let mut writer = make_writer();

        writer.process_message(&make_add_to_heap_slot(100));
        writer.process_message(&make_posting_slot(100, 500));
        writer.process_message(&make_flush_marker_slot(100, 1, 42));

        writer.submit_flush();
        writer.poll_and_handle_completions();

        assert!(writer.in_flight_min_heap.is_empty());
        let committed = unsafe { *writer.global_committed_gsn };
        assert_eq!(committed, 100);
    }

    #[test]
    fn group_commit_multiple_postings_one_flush() {
        let mut writer = make_writer();

        writer.process_message(&make_add_to_heap_slot(100));
        writer.process_message(&make_add_to_heap_slot(200));
        writer.process_message(&make_add_to_heap_slot(300));

        writer.process_message(&make_posting_slot(100, 500));
        writer.process_message(&make_posting_slot(200, 300));
        writer.process_message(&make_posting_slot(300, 100));

        writer.process_message(&make_flush_marker_slot(100, 1, 42));
        writer.process_message(&make_flush_marker_slot(200, 2, 43));
        writer.process_message(&make_flush_marker_slot(300, 3, 44));

        writer.submit_flush();

        assert_eq!(writer.backend.written.len(), 1);
        assert_eq!(writer.backend.written[0].0.len(), PostingRecord::SIZE * 3);

        writer.poll_and_handle_completions();

        assert!(writer.in_flight_min_heap.is_empty());
        assert_eq!(writer.buffer.len(), 0);
        assert_eq!(writer.write_offset, (PostingRecord::SIZE * 3) as u64);
    }

    #[test]
    fn sequential_flushes_advance_offset() {
        let mut writer = make_writer();

        writer.process_message(&make_add_to_heap_slot(100));
        writer.process_message(&make_posting_slot(100, 500));
        writer.process_message(&make_flush_marker_slot(100, 1, 42));

        writer.submit_flush();
        writer.poll_and_handle_completions();

        assert_eq!(writer.write_offset, PostingRecord::SIZE as u64);

        writer.process_message(&make_add_to_heap_slot(200));
        writer.process_message(&make_posting_slot(200, 300));
        writer.process_message(&make_flush_marker_slot(200, 2, 43));

        writer.submit_flush();

        assert_eq!(writer.backend.written.len(), 2);
        assert_eq!(writer.backend.written[1].1, PostingRecord::SIZE as u64);

        writer.poll_and_handle_completions();

        assert_eq!(writer.write_offset, (PostingRecord::SIZE * 2) as u64);
    }

    #[test]
    fn poll_noop_when_no_flush_in_flight() {
        let mut writer = make_writer();

        writer.poll_and_handle_completions();

        assert_eq!(writer.write_offset, 0);
        assert!(!writer.flush_in_flight);
    }

    #[test]
    fn new_postings_accumulate_during_in_flight() {
        let mut writer = make_writer();

        writer.process_message(&make_posting_slot(100, 500));
        writer.submit_flush();

        assert!(writer.flush_in_flight);

        writer.process_message(&make_posting_slot(200, 300));
        writer.process_message(&make_posting_slot(300, 100));

        assert_eq!(writer.buffer.len(), PostingRecord::SIZE * 3);

        writer.poll_and_handle_completions();

        assert_eq!(writer.buffer.len(), PostingRecord::SIZE * 2);
        assert!(!writer.flush_in_flight);
    }

    #[test]
    fn remove_from_heap_advances_committed_gsn() {
        let mut writer = make_writer();

        writer.process_message(&make_add_to_heap_slot(100));
        writer.process_message(&make_add_to_heap_slot(200));

        writer.process_message(&make_remove_from_heap_slot(100));

        let committed = unsafe { *writer.global_committed_gsn };
        assert_eq!(committed, 199);
    }
}
