use std::sync::Arc;
use std::time::Instant;
use ringbuf::mpsc_ring_buffer::MpscRingBuffer;
use pipeline::posting_record::PostingRecord;
use pipeline::in_flight_min_heap::InFlightMinHeap;
use crate::ls_writer_slot::*;
use crate::flush_done_slot::FlushDoneSlot;
use crate::flush_backend::FlushBackend;
use common::mem_barrier::release_store_u64;
use crate::sig_record::SigRecord;
use crate::signing_state::SigningState;

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
    ls_file_path: String,
    max_ls_file_size: usize,
    in_flight_min_heap: InFlightMinHeap,
    batch_size: usize,

    buffer_arena: ringbuf::arena::Arena,
    buffer_ptr: *mut u8,
    buffer_len: usize,
    buffer_capacity: usize,
    write_offset: u64,

    flush_timeout_ms: u64,
    flush_max_buffer_size: usize,
    pending_flush: Vec<PendingFlush>,

    idle_count: u32,
    last_flush: Instant,

    flush_in_flight: bool,
    flush_pending_records: Vec<PendingFlush>,
    flush_buffer_snapshot_len: usize,
    signing_state: Option<SigningState>,
    sign_fd: i32,
    sign_buffer: Vec<u8>,
    sign_write_offset: u64,
}

impl<T: FlushBackend> LsWriter<T> {
    pub fn new(
        id: usize,
        ls_writer_rb: Arc<MpscRingBuffer<LsWriterSlot>>,
        flush_done_rb: Arc<MpscRingBuffer<FlushDoneSlot>>,
        global_committed_gsn: *mut u64,
        backend: T,
        ls_file_path: String,
        max_ls_file_size: usize,
        in_flight_min_heap_capacity: usize,
        in_flight_min_heap_seed_k0: u64,
        in_flight_min_heap_seed_k1: u64,
        batch_size: usize,
        flush_timeout_ms: u64,
        flush_max_buffer_posting_records: usize,
        signing_state: Option<SigningState>,
    ) -> Self {
        let flush_max_buffer_bytes = flush_max_buffer_posting_records * PostingRecord::SIZE;
        let buffer_capacity = (flush_max_buffer_bytes * 2 + 4095) & !4095;
        let buffer_arena = ringbuf::arena::Arena::new(buffer_capacity)
            .expect("Failed to create LS Writer buffer Arena");
        let buffer_ptr = buffer_arena.as_ptr();

        Self {
            id,
            ls_writer_rb,
            flush_done_rb,
            global_committed_gsn,
            backend,
            ls_file_path,
            max_ls_file_size,
            in_flight_min_heap: InFlightMinHeap::new(
                in_flight_min_heap_capacity,
                in_flight_min_heap_seed_k0,
                in_flight_min_heap_seed_k1,
            ),
            batch_size,
            buffer_arena,
            buffer_ptr,
            buffer_len: 0,
            buffer_capacity,
            write_offset: 0,
            flush_timeout_ms,
            flush_max_buffer_size: flush_max_buffer_bytes,
            pending_flush: Vec::with_capacity(256),
            idle_count: 0,
            last_flush: Instant::now(),
            flush_in_flight: false,
            flush_pending_records: Vec::with_capacity(256),
            flush_buffer_snapshot_len: 0,
            signing_state,
            sign_fd: -1,
            sign_buffer: Vec::with_capacity(256 * SigRecord::SIZE),
            sign_write_offset: 0,
        }
    }

    pub fn run(&mut self) {
        self.initialize();

        println!("[ls-writer {}] started", self.id);

        loop {
            let batch = self.ls_writer_rb.drain_batch(self.batch_size);

            if batch.is_empty() {
                self.poll_and_handle_completions();

                if self.buffer_len != 0 && !self.flush_in_flight {
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

            if self.buffer_len >= self.flush_max_buffer_size && !self.flush_in_flight {
                self.submit_flush();
            }
        }
    }

    pub fn submit_flush(&mut self) {
        if self.buffer_len == 0 || self.flush_in_flight {
            return;
        }

        if let Some(ref mut signing) = self.signing_state {
            Self::sign_batch_inner(
                signing,
                self.buffer_ptr,
                self.buffer_len,
                self.write_offset,
                &mut self.sign_buffer,
            );
        }

        self.flush_sign_buffer();

        let padded_len = (self.buffer_len + 4095) & !4095;

        let data = unsafe {
            std::slice::from_raw_parts(self.buffer_ptr, padded_len)
        };

        self.backend
            .submit_write_and_sync(data, self.write_offset)
            .expect("Failed to LS submit flush");

        self.flush_in_flight = true;
        self.flush_buffer_snapshot_len = self.buffer_len;

        self.flush_pending_records = std::mem::take(&mut self.pending_flush);

        self.last_flush = Instant::now();
        self.idle_count = 0;
    }

    fn sign_batch_inner(
        signing: &mut SigningState,
        buffer_ptr: *mut u8,
        buffer_len: usize,
        write_offset: u64,
        sign_buffer: &mut Vec<u8>,
    ) {
        let record_count = buffer_len / PostingRecord::SIZE;
        if record_count == 0 {
            return;
        }

        let records = unsafe {
            std::slice::from_raw_parts(
                buffer_ptr as *const PostingRecord,
                record_count,
            )
        };

        for i in 0..record_count {
            let record = &records[i];
            let ls_offset = write_offset + (i * PostingRecord::SIZE) as u64;

            let sig_record = signing.sign_posting(
                record.transfer_id_hi,
                record.transfer_id_lo,
                record.gsn,
                ls_offset,
                record,
            );

            let sig_bytes = unsafe {
                std::slice::from_raw_parts(
                    &sig_record as *const SigRecord as *const u8,
                    SigRecord::SIZE,
                )
            };
            sign_buffer.extend_from_slice(sig_bytes);
        }
    }

    fn sign_batch(&mut self, signing: &mut SigningState) {
        let record_count = self.buffer_len / PostingRecord::SIZE;
        if record_count == 0 {
            return;
        }

        let records = unsafe {
            std::slice::from_raw_parts(
                self.buffer_ptr as *const PostingRecord,
                record_count,
            )
        };

        for i in 0..record_count {
            let record = &records[i];
            let ls_offset = self.write_offset + (i * PostingRecord::SIZE) as u64;

            let sig_record = signing.sign_posting(
                record.transfer_id_hi,
                record.transfer_id_lo,
                record.gsn,
                ls_offset,
                record,
            );

            let sig_bytes = unsafe {
                std::slice::from_raw_parts(
                    &sig_record as *const SigRecord as *const u8,
                    SigRecord::SIZE,
                )
            };
            self.sign_buffer.extend_from_slice(sig_bytes);
        }
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

        let padded_len = (self.flush_buffer_snapshot_len + 4095) & !4095;
        self.write_offset += padded_len as u64;

        let remaining = self.buffer_len - self.flush_buffer_snapshot_len;
        if remaining > 0 {
            unsafe {
                std::ptr::copy(
                    self.buffer_ptr.add(self.flush_buffer_snapshot_len),
                    self.buffer_ptr,
                    remaining,
                );
            }
        }
        self.buffer_len = remaining;

        if remaining < self.buffer_capacity {
            unsafe {
                std::ptr::write_bytes(
                    self.buffer_ptr.add(remaining),
                    0,
                    self.flush_buffer_snapshot_len,
                );
            }
        }

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
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        posting_bytes.as_ptr(),
                        self.buffer_ptr.add(self.buffer_len),
                        PostingRecord::SIZE,
                    );
                    self.buffer_len += PostingRecord::SIZE;
                }
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

    fn initialize(&mut self) {
        self.backend
            .open_ls_file(&self.ls_file_path)
            .expect("Failed to open LS file");

        self.backend
            .fallocate(self.max_ls_file_size)
            .expect("Failed to fallocate LS file");

        if self.signing_state.is_none() {
            let sign_path = format!("{}.sign", self.ls_file_path);
            let c_path = std::ffi::CString::new(sign_path.as_str()).unwrap();
            let fd = unsafe {
                libc::open(
                    c_path.as_ptr(),
                    libc::O_CREAT | libc::O_WRONLY,
                    0o644,
                )
            };
            assert!(fd >= 0, "Failed to open ls_sign file");
            self.sign_fd = fd;
        }

        println!(
            "[ls-writer {}] initialized: file={}, max_size={}MB",
            self.id, self.ls_file_path, self.max_ls_file_size / (1024 * 1024),
        );
    }

    fn flush_sign_buffer(&mut self) {
        if self.sign_buffer.is_empty() || self.sign_fd < 0 {
            return;
        }

        let written = unsafe {
            libc::pwrite(
                self.sign_fd,
                self.sign_buffer.as_ptr() as *const libc::c_void,
                self.sign_buffer.len(),
                self.sign_write_offset as libc::off_t,
            )
        };

        assert!(written > 0, "ls_sign pwrite failed");

        let sync_result = unsafe { libc::fdatasync(self.sign_fd) };
        assert_eq!(sync_result, 0, "ls_sign fdatasync failed");

        self.sign_write_offset += written as u64;
        self.sign_buffer.clear();
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
    use ed25519_dalek::SigningKey;
    use super::*;
    use crate::flush_backend::{FlushBackend, FlushCompletion};
    use pipeline::posting_record::PostingRecord;
    use ringbuf::mpsc_ring_buffer::MpscRingBuffer;
    use crate::flush_done_slot::FlushDoneSlot;
    use crate::ls_writer::LsWriter;
    use crate::ls_writer_slot::{LsWriterSlot, LS_MSG_ADD_TO_HEAP, LS_MSG_FLUSH_MARKER, LS_MSG_POSTING, LS_MSG_REMOVE_FROM_HEAP};

    const K0: u64 = 0x0123456789ABCDEF;
    const K1: u64 = 0xFEDCBA9876543210;


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

        fn fallocate(&mut self, size: usize) -> std::io::Result<()> {
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

        let mut writer = LsWriter::new(
            0,
            ls_writer_rb,
            flush_done_rb,
            committed_gsn_ptr,
            MockFlushBackend::new(),
            "test.ls".to_string(),
            0,
            64,
            K0,
            K1,
            64,
            2,
            512,
            None,
        );
        writer.initialize();
        writer
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

        let mut writer = LsWriter::new(
            0,
            ls_writer_rb,
            flush_done_rb,
            committed_gsn_ptr,
            MockFlushBackend::new(),
            "test.ls".to_string(),
            0,
            64,
            K0,
            K1,
            64, 2, 512,
            None,
        );
        writer.initialize();

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
        slot.posting.transfer_id_lo = gsn;
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

    fn make_writer_with_signing(signing: SigningState) -> LsWriter<MockFlushBackend> {
        let ls_writer_rb = Arc::new(
            MpscRingBuffer::<LsWriterSlot>::new(64).unwrap()
        );
        let flush_done_rb = Arc::new(
            MpscRingBuffer::<FlushDoneSlot>::new(64).unwrap()
        );

        unsafe { TEST_COMMITTED_GSN = 0; }
        let committed_gsn_ptr = unsafe { &raw mut TEST_COMMITTED_GSN };

        let key = SigningKey::from_bytes(&[0x42u8; 32]);
        let genesis = [0u8; 32];

        let mut writer = LsWriter::new(
            0,
            ls_writer_rb,
            flush_done_rb,
            committed_gsn_ptr,
            MockFlushBackend::new(),
            "test.ls".to_string(),
            0,
            64, K0, K1,
            64, 2, 512,
            Some(signing),
        );
        writer.initialize();
        writer
    }


    #[test]
    fn sign_batch_creates_sig_records() {
        let key = SigningKey::from_bytes(&[0x42u8; 32]);
        let genesis = [0u8; 32];
        let signing = SigningState::new(key, genesis);

        let mut writer = make_writer_with_signing(signing);
        writer.initialize();

        let mut slot1 = make_posting_slot(100, 500);
        slot1.posting.transfer_id_hi = 0;
        slot1.posting.transfer_id_lo = 1;

        let mut slot2 = make_posting_slot(100, -500);
        slot2.posting.transfer_id_hi = 0;
        slot2.posting.transfer_id_lo = 1;

        writer.process_message(&slot1);
        writer.process_message(&slot2);

        assert!(writer.sign_buffer.is_empty());

        writer.submit_flush();

        assert_ne!(writer.signing_state.as_ref().unwrap().last_tx_hash(), &genesis);
    }

    #[test]
    fn initialize_opens_file() {
        let writer = make_writer();
        assert!(writer.backend.open_called);
    }

    #[test]
    fn new_initializes_empty_state() {
        let writer = make_writer();
        assert_eq!(writer.buffer_len, 0);
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

        assert_eq!(writer.buffer_len, PostingRecord::SIZE);
    }

    #[test]
    fn process_multiple_postings_accumulates_buffer() {
        let mut writer = make_writer();

        writer.process_message(&make_posting_slot(100, 500));
        writer.process_message(&make_posting_slot(200, 300));
        writer.process_message(&make_posting_slot(300, 100));

        assert_eq!(writer.buffer_len, PostingRecord::SIZE * 3);
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
        assert_eq!(writer.backend.written[0].0.len() % 4096, 0);
        assert!(writer.backend.written[0].0.len() >= PostingRecord::SIZE);
        assert_eq!(writer.backend.written[0].1, 0);
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
        assert_eq!(writer.buffer_len, PostingRecord::SIZE);

        writer.poll_and_handle_completions();

        assert_eq!(writer.write_offset, 4096);
        assert_eq!(writer.buffer_len, 0);
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
        assert_eq!(writer.backend.written[0].0.len() % 4096, 0);
        assert!(writer.backend.written[0].0.len() >= PostingRecord::SIZE);

        writer.poll_and_handle_completions();

        assert!(writer.in_flight_min_heap.is_empty());
        assert_eq!(writer.buffer_len, 0);
        assert_eq!(writer.write_offset, 4096);
    }

    #[test]
    fn sequential_flushes_advance_offset() {
        let mut writer = make_writer();

        writer.process_message(&make_add_to_heap_slot(100));
        writer.process_message(&make_posting_slot(100, 500));
        writer.process_message(&make_flush_marker_slot(100, 1, 42));

        writer.submit_flush();
        writer.poll_and_handle_completions();

        assert_eq!(writer.write_offset, 4096);

        writer.process_message(&make_add_to_heap_slot(200));
        writer.process_message(&make_posting_slot(200, 300));
        writer.process_message(&make_flush_marker_slot(200, 2, 43));

        writer.submit_flush();

        assert_eq!(writer.backend.written.len(), 2);
        assert_eq!(writer.backend.written[0].0.len() % 4096, 0);
        assert!(writer.backend.written[0].0.len() >= PostingRecord::SIZE);

        writer.poll_and_handle_completions();

        assert_eq!(writer.write_offset, 8192);
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

        assert_eq!(writer.buffer_len, PostingRecord::SIZE * 3);

        writer.poll_and_handle_completions();

        assert_eq!(writer.buffer_len, PostingRecord::SIZE * 2);
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
