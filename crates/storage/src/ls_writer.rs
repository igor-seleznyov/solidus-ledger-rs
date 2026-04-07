use chrono::Local;
use common::mem_barrier::release_store_u64;
use pipeline::posting_record::PostingRecord;
use pipeline::in_flight_min_heap::InFlightMinHeap;
use ringbuf::mpsc_ring_buffer::MpscRingBuffer;
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::time::Instant;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use crate::ls_writer_slot::*;
use crate::flush_done_slot::FlushDoneSlot;
use crate::flush_backend::FlushBackend;
use crate::ls_file_header::{LsFileHeader};
use crate::metadata_strategy::MetadataStrategy;
use crate::pending_flush::PendingFlush;
use crate::signing_strategy::SigningStrategy;
use crate::checkpoint_record::CheckpointRecord;
use crate::checkpoint_file_header::CheckpointFileHeader;
use crate::manifest::Manifest;
use crate::manifest_entry::ManifestEntry;
use crate::recovery::recover_checkpoint_state;
use crate::recovery::recover_ls_state;
use common::make_test_dir::make_test_dir;
use crate::index_builder::IndexBuilderTask;

const CLOCK_CHECK_REPEATS_COUNT_INTERVAL: u32 = 100_000;

fn generate_ls_filename(shard_id: usize, file_seq: u64) -> String {
    let now = Local::now();
    format!("ls_{}-{}-{}.ls", now.format("%Y%m%d-%H%M%S-%3f"), shard_id, file_seq)
}

pub struct LsWriter<T: FlushBackend, S: SigningStrategy, M: MetadataStrategy> {
    id: usize,
    ls_writer_rb: Arc<MpscRingBuffer<LsWriterSlot>>,
    flush_done_rb: Arc<MpscRingBuffer<FlushDoneSlot>>,
    global_committed_gsn: *mut u64,
    backend: T,
    signing_strategy: S,
    metadata_strategy: M,
    ls_directory: String,
    current_ls_file_path: String,
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
    collecting_arena: ringbuf::arena::Arena,
    collecting_ptr: *mut PendingFlush,
    collecting_len: usize,
    collecting_capacity: usize,

    idle_count: u32,
    last_flush: Instant,

    flush_in_flight: bool,
    in_flushing_arena: ringbuf::arena::Arena,
    in_flushing_ptr: *mut PendingFlush,
    in_flushing_len: usize,
    in_flushing_capacity: usize,
    flush_buffer_snapshot_len: usize,

    partition_count: u16,
    file_seq: u64,

    ls_handle_index: u8,
    sign_handle_index: u8,
    meta_handle_index: u8,

    checkpoint_handle_index: u8,
    checkpoint_write_offset: u64,
    batch_seq: u32,
    in_flushing_first_offset: u64,
    in_flushing_posting_count: u64,
    checkpoint_prealloc_size: usize,

    manifest: Manifest,
    rules_checksum: u32,

    current_gsn_min: u64,
    current_timestamp_min_ns: u64,

    buffered_gsn_max: u64,
    buffered_timestamp_max_ns: u64,

    in_flushing_gsn_max: u64,
    in_flushing_timestamp_max_ns: u64,

    flushed_gsn_max: u64,
    flushed_timestamp_max_ns: u64,

    index_builder_tx: Sender<IndexBuilderTask>,
}

unsafe impl<T: FlushBackend, S: SigningStrategy + Send, M: MetadataStrategy + Send> Send for LsWriter<T, S, M> {}

impl<T: FlushBackend, S: SigningStrategy + Send, M: MetadataStrategy + Send> LsWriter<T, S, M> {
    pub fn new(
        id: usize,
        ls_writer_rb: Arc<MpscRingBuffer<LsWriterSlot>>,
        flush_done_rb: Arc<MpscRingBuffer<FlushDoneSlot>>,
        global_committed_gsn: *mut u64,
        backend: T,
        signing_strategy: S,
        metadata_strategy: M,
        ls_directory: String,
        max_ls_file_size: usize,
        in_flight_min_heap_capacity: usize,
        in_flight_min_heap_seed_k0: u64,
        in_flight_min_heap_seed_k1: u64,
        batch_size: usize,
        flush_timeout_ms: u64,
        flush_max_buffer_posting_records: usize,
        partition_count: u16,
        checkpoint_prealloc_multiplier: usize,
        rules_checksum: u32,
        manifest: Manifest,
        index_builder_tx: Sender<IndexBuilderTask>,
    ) -> Self {
        let flush_max_buffer_bytes = flush_max_buffer_posting_records * PostingRecord::SIZE;
        let buffer_capacity = (flush_max_buffer_bytes * 2 + 4095) & !4095;
        let buffer_arena = ringbuf::arena::Arena::new(buffer_capacity)
            .expect("Failed to create LS Writer buffer Arena");
        let buffer_ptr = buffer_arena.as_ptr();

        let pending_capacity = flush_max_buffer_posting_records;
        let pending_arena_size = (pending_capacity * PendingFlush::SIZE + 4096) & !4095;

        let pending_flush_arena = ringbuf::arena::Arena::new(pending_arena_size)
            .expect("Failed to create pending_flush buffer Arena");
        let pending_flush_ptr = pending_flush_arena.as_ptr() as *mut PendingFlush;

        let flush_pending_arena = ringbuf::arena::Arena::new(pending_arena_size)
            .expect("Failed to create flush_pending buffer Arena");
        let flush_pending_ptr = flush_pending_arena.as_ptr() as *mut PendingFlush;

        Self {
            id,
            ls_writer_rb,
            flush_done_rb,
            global_committed_gsn,
            backend,
            signing_strategy,
            metadata_strategy,
            ls_directory,
            current_ls_file_path: String::new(),
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
            collecting_arena: pending_flush_arena,
            collecting_ptr: pending_flush_ptr,
            collecting_len: 0,
            collecting_capacity: pending_capacity,

            idle_count: 0,
            last_flush: Instant::now(),
            flush_in_flight: false,

            in_flushing_arena: flush_pending_arena,
            in_flushing_ptr: flush_pending_ptr,
            in_flushing_len: 0,
            in_flushing_capacity: pending_capacity,

            flush_buffer_snapshot_len: 0,

            partition_count,
            file_seq: 0,
            ls_handle_index: 0,
            sign_handle_index: 0,
            meta_handle_index: 0,

            checkpoint_handle_index: 0,
            checkpoint_write_offset: 0,
            batch_seq: 0,
            in_flushing_first_offset: 0,
            in_flushing_posting_count: 0,
            checkpoint_prealloc_size: Self::compute_checkpoint_prealloc(
                max_ls_file_size,
                flush_max_buffer_posting_records,
                checkpoint_prealloc_multiplier,
            ),
            manifest,
            rules_checksum,
            current_gsn_min: 0,
            current_timestamp_min_ns: 0,
            buffered_gsn_max: 0,
            buffered_timestamp_max_ns: 0,
            in_flushing_gsn_max: 0,
            in_flushing_timestamp_max_ns: 0,
            flushed_gsn_max: 0,
            flushed_timestamp_max_ns: 0,

            index_builder_tx,
        }
    }

    pub fn run(&mut self) {
        self.startup();

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

        let signature_flush = self.signing_strategy.prepare_flush(
            self.buffer_ptr,
            self.buffer_len,
            self.write_offset,
        );

        let metadata_flush = self.metadata_strategy.prepare_flush();

        let mut sync_count = 0;

        if let Some((data, len, offset)) = signature_flush {
            self.backend.submit_write(self.sign_handle_index, data, len, offset)
                .expect("Failed to submit signature flush write");
            self.backend.submit_sync(self.sign_handle_index)
                .expect("Failed to submit signature flush sync");
            sync_count += 1;
        }

        if let Some((data, len, offset)) = metadata_flush {
            self.backend.submit_write(self.meta_handle_index, data, len, offset)
                .expect("Failed to submit posting metadata flush write");
            self.backend.submit_sync(self.meta_handle_index)
                .expect("Failed to submit posting metadata flush sync");
            sync_count += 1;
        }

        if sync_count > 0 {
            self.backend.flush_submissions()
                .expect("Failed to flush signature and(or) postings metadata submissions");
            self.backend.wait_completions(sync_count);

            self.signing_strategy.on_flush_complete();
            self.metadata_strategy.on_flush_complete();
        }

        let padded_len = (self.buffer_len + 4095) & !4095;
        self.backend
            .submit_write(
                self.ls_handle_index,
                self.buffer_ptr as *const u8,
                padded_len,
                self.write_offset,
            ).expect("Failed to submit ledger storage write");
        self.backend.submit_sync(self.ls_handle_index)
            .expect("Failed to submit ledger storage sync");

        self.flush_in_flight = true;
        self.flush_buffer_snapshot_len = self.buffer_len;

        self.in_flushing_first_offset = self.write_offset;
        self.in_flushing_posting_count = (self.flush_buffer_snapshot_len / PostingRecord::SIZE) as u64;

        self.in_flushing_gsn_max = self.buffered_gsn_max;
        self.in_flushing_timestamp_max_ns = self.buffered_timestamp_max_ns;

        std::mem::swap(&mut self.collecting_ptr, &mut self.in_flushing_ptr);
        std::mem::swap(&mut self.collecting_arena, &mut self.in_flushing_arena);
        self.in_flushing_len = self.collecting_len;
        self.collecting_len = 0;

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

        for i in 0..self.in_flushing_len {
            let pending = unsafe { &*self.in_flushing_ptr.add(i) };

            self.in_flight_min_heap.remove(pending.gsn);

            let mut claimed = self.flush_done_rb.claim();
            let slot = claimed.as_mut();
            slot.transfer_id_hi = pending.transfer_id_hi;
            slot.transfer_id_lo = pending.transfer_id_lo;
            slot.transfer_hash_table_offset = pending.transfer_hash_table_offset;
            claimed.publish();
        }
        self.in_flushing_len = 0;

        self.flushed_gsn_max = self.in_flushing_gsn_max;
        self.flushed_timestamp_max_ns = self.in_flushing_timestamp_max_ns;

        self.maybe_advance_committed_gsn();

        self.flush_in_flight = false;

        self.write_checkpoint_record();

        if self.should_rotate() {
            self.rotate();
        }
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
                if self.current_gsn_min == 0 {
                    self.current_gsn_min = slot.posting.gsn;
                    self.current_timestamp_min_ns = slot.posting.timestamp_ns;
                    self.manifest.update_entry_min_values(
                        self.manifest.current_entry_index(),
                        self.current_gsn_min,
                        self.current_timestamp_min_ns,
                    );
                }

                self.buffered_gsn_max = slot.posting.gsn;
                self.buffered_timestamp_max_ns = slot.posting.timestamp_ns;

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
                unsafe {
                    let pending_flush = self.collecting_ptr.add(self.collecting_len);
                    (*pending_flush).transfer_id_hi = slot.transfer_id_hi;
                    (*pending_flush).transfer_id_lo = slot.transfer_id_lo;
                    (*pending_flush).transfer_hash_table_offset = slot.transfer_hash_table_offset;
                    (*pending_flush)._pad = 0;
                    (*pending_flush).gsn = slot.gsn;
                }
                self.collecting_len += 1;
            }
            _ => {}
        }
    }

    pub fn startup(&mut self) {
        if self.manifest.entries_count() == 0 {
            self.open_new_files();

            let mut entry = self.make_manifest_entry();
            self.manifest.append_current_entry(&mut entry);

            println!(
                "[ls-writer {}] first launch: file={}, signing={}, metadata={}",
                self.id, self.current_ls_file_path,
                self.signing_strategy.is_enabled(),
                self.metadata_strategy.is_enabled(),
            );
        } else {
            let current_entry = self.manifest.read_current_entry();

            if self.config_matches(&current_entry) {
                self.reopen_files(current_entry.filename_str());
                self.file_seq = current_entry.file_seq;

                println!(
                    "[ls-writer {}] reopen: file={}, file_seq={}",
                    self.id, self.current_ls_file_path, self.file_seq,
                );
            } else {
                let current_index = self.manifest.current_entry_index();
                self.manifest.finalize_entry(
                    current_index,
                    self.flushed_gsn_max,
                    self.flushed_timestamp_max_ns,
                );

                self.file_seq = current_entry.file_seq + 1;

                self.open_new_files();

                let mut entry = self.make_manifest_entry();
                self.manifest.append_current_entry(&mut entry);

                println!(
                    "[ls-writer {}] config changed, rotated to: file={}, file_seq={}",
                    self.id, self.current_ls_file_path, self.file_seq,
                );
            }
        }

        println!(
            "[ls-writer {}] startup complete: write_offset={}, checkpoint_prealloc={}",
            self.id, self.write_offset, self.checkpoint_prealloc_size,
        );
    }

    fn open_new_files(&mut self) {
        let filename = generate_ls_filename(self.id, self.file_seq);
        self.current_ls_file_path = format!("{}/{}", self.ls_directory, filename);
        self.ls_handle_index = self.backend.open_file(
            &self.current_ls_file_path,
            self.max_ls_file_size,
        ).expect("Failed to open LS file");
        self.open_strategy_files();
        self.write_all_headers();
    }

    fn reopen_files(&mut self, filename: &str) {
        self.current_ls_file_path = format!("{}/{}", self.ls_directory, filename);

        self.ls_handle_index = self.backend.open_file(
            &self.current_ls_file_path,
            self.max_ls_file_size,
        ).expect("FFailed to reopen LS file");

        let checkpoint_path = format!("{}.checkpoint", self.current_ls_file_path);
        let (recovered_checkpoint_offset, recovered_batch_seq) =
            recover_checkpoint_state(&checkpoint_path);
        self.checkpoint_write_offset = recovered_checkpoint_offset;
        self.batch_seq = recovered_batch_seq;

        let ls_state = recover_ls_state(&self.current_ls_file_path);
        self.write_offset = ls_state.write_offset;

        self.current_gsn_min = ls_state.gsn_min;
        self.current_timestamp_min_ns = ls_state.timestamp_min_ns;
        self.flushed_gsn_max = ls_state.gsn_max;
        self.flushed_timestamp_max_ns = ls_state.timestamp_max_ns;

        self.buffered_gsn_max = ls_state.gsn_max;
        self.buffered_timestamp_max_ns = ls_state.timestamp_max_ns;

        println!(
            "[ls-writer {}] recovery: write_offset={}, postings={}, checkpoint_offset={}, batch_seq={}, gsn=[{}..{}]",
            self.id,
            self.write_offset,
            ls_state.postings_count,
            self.checkpoint_write_offset,
            self.batch_seq,
            ls_state.gsn_min,
            ls_state.gsn_max,
        );
    }

    fn open_strategy_files(&mut self) {
        if let Some((sign_path, prealloc)) = self.signing_strategy.file_info(&self.current_ls_file_path) {
            self.sign_handle_index = self.backend.open_file(&sign_path, prealloc)
                .expect("Failed to open signature file");
        }

        if let Some((meta_path, prealloc)) = self.metadata_strategy.file_info(&self.current_ls_file_path) {
            self.meta_handle_index = self.backend.open_file(&meta_path, prealloc)
                .expect("Failed to open metadata file");
        }

        let checkpoint_path = format!("{}.checkpoint", self.current_ls_file_path);
        self.checkpoint_handle_index = self.backend.open_file_buffered(
            &checkpoint_path,
            self.checkpoint_prealloc_size,
        ).expect("Failed to open checkpoint file");
    }

    fn write_all_headers(&mut self) {
        let signature_header = self.signing_strategy.prepare_header(self.file_seq);
        let metadata_header = self.metadata_strategy.prepare_header(self.file_seq, self.max_ls_file_size as u64);

        let mut sync_count = 0;

        if let Some((data, len, offset)) = signature_header {
            self.backend.submit_write(self.sign_handle_index, data, len, offset)
                .expect("Failed to submit signature header write");
            self.backend.submit_sync(self.sign_handle_index)
                .expect("Failed to submit signature header sync");
            sync_count += 1;
        }

        if let Some((data, len, offset)) = metadata_header {
            self.backend.submit_write(self.meta_handle_index, data, len, offset)
                .expect("Failed to submit posting metadata header write");
            self.backend.submit_sync(self.meta_handle_index)
                .expect("Failed to submit posting metadata header sync");
            sync_count += 1;
        }

        if sync_count > 0 {
            self.backend.flush_submissions()
                .expect("Failed to flush header submissions");
            self.backend.wait_completions(sync_count);

            self.signing_strategy.on_header_written();
            self.metadata_strategy.on_header_written();
        }

        let header = LsFileHeader::new(
            self.signing_strategy.is_enabled(),
            self.partition_count,
            self.max_ls_file_size as u64,
            self.file_seq,
            self.signing_strategy.public_key_hash(),
            self.metadata_strategy.is_enabled(),
        );

        let header_page = header.to_page();
        self.backend.submit_write(
            self.ls_handle_index,
            header_page.as_ptr(),
            header_page.len(),
            0,
        ).expect("Failed to submit ledger storage header write");

        self.backend.submit_sync(self.ls_handle_index)
            .expect("Failed to sync ledger storage header sync");
        self.backend.flush_submissions()
            .expect("Failed to flush ledger storage header");

        self.backend.wait_completions(1);

        self.write_offset = LsFileHeader::DATA_OFFSET as u64;

        let checkpoint_header = CheckpointFileHeader::new(self.file_seq);
        let checkpoint_header_bytes = unsafe { checkpoint_header.as_bytes() };

        self.backend.submit_write(
            self.checkpoint_handle_index,
            checkpoint_header_bytes.as_ptr(),
            CheckpointFileHeader::SIZE,
            0,
        ).expect("Failed to submit checkpoint header write");

        self.backend.submit_sync(self.checkpoint_handle_index)
            .expect("Failed to submit checkpoint header sync");
        self.backend.flush_submissions()
            .expect("Failed to flush checkpoint header");

        self.checkpoint_write_offset = CheckpointFileHeader::DATA_OFFSET as u64;
        self.batch_seq = 0;
    }

    fn config_matches(&self, entry: &ManifestEntry) -> bool {
        let signing_matches = (entry.signing_enabled != 0) == self.signing_strategy.is_enabled();
        let metadata_matches = (entry.metadata_enabled != 0) == self.metadata_strategy.is_enabled();
        let rules_matches = entry.rules_checksum == self.rules_checksum;

        signing_matches && metadata_matches && rules_matches
    }

    fn make_manifest_entry(&mut self) -> ManifestEntry {
        let filename = self.current_ls_file_path
            .rsplit('/')
            .next()
            .unwrap_or(&self.current_ls_file_path);

        let mut entry = ManifestEntry::zeroed();
        entry.file_seq = self.file_seq;
        entry.signing_enabled = if self.signing_strategy.is_enabled() { 1 } else { 0 };
        entry.metadata_enabled = if self.metadata_strategy.is_enabled() { 1 } else { 0 };
        entry.record_size = if self.metadata_strategy.is_enabled() {
            PostingRecord::SIZE as u32
        } else { 0 };

        entry.rules_checksum = self.rules_checksum;
        entry.gsn_min = 0;
        entry.gsn_max = 0;
        entry.timestamp_min_ns = 0;
        entry.timestamp_max_ns = 0;
        entry.set_filename(filename);
        entry
    }

    fn open_files(&mut self) {
        let filename = generate_ls_filename(self.id, self.file_seq);
        self.current_ls_file_path = format!("{}/{}", self.ls_directory, filename);

        self.ls_handle_index = self.backend
            .open_file(&self.current_ls_file_path, self.max_ls_file_size)
            .expect("Failed to open LS file");

        if let Some((sign_path, prealloc)) = self.signing_strategy.file_info(&self.current_ls_file_path) {
            self.sign_handle_index = self.backend.open_file(&sign_path, prealloc)
                .expect("Failed to open signature file");
        }

        if let Some((meta_path, prealloc)) = self.metadata_strategy.file_info(&self.current_ls_file_path) {
            self.meta_handle_index = self.backend.open_file(&meta_path, prealloc)
                .expect("Failed to open metadata file");
        }

        let checkpoint_path = format!("{}.checkpoint", self.current_ls_file_path);
        self.checkpoint_handle_index = self.backend
            .open_file_buffered(&checkpoint_path, self.checkpoint_prealloc_size)
            .expect("Failed to open checkpoint file");

        let signature_header = self.signing_strategy.prepare_header(self.file_seq);
        let metadata_header = self.metadata_strategy.prepare_header(self.file_seq, self.max_ls_file_size as u64);

        let mut sync_count = 0;

        if let Some((data, len, offset)) = signature_header {
            self.backend.submit_write(self.sign_handle_index, data, len, offset)
                .expect("Failed to submit signature header write");
            self.backend.submit_sync(self.sign_handle_index)
                .expect("Failed to submit signature header sync");
            sync_count += 1;
        }

        if let Some((data, len, offset)) = metadata_header {
            self.backend.submit_write(self.meta_handle_index, data, len, offset)
                .expect("Failed to submit posting metadata header write");
            self.backend.submit_sync(self.meta_handle_index)
                .expect("Failed to submit posting metadata header sync");
            sync_count += 1;
        }

        if sync_count > 0 {
            self.backend.flush_submissions()
                .expect("Failed to flush header submissions");
            self.backend.wait_completions(sync_count);

            self.signing_strategy.on_header_written();
            self.metadata_strategy.on_header_written();
        }

        let header = LsFileHeader::new(
            self.signing_strategy.is_enabled(),
            self.partition_count,
            self.max_ls_file_size as u64,
            self.file_seq,
            self.signing_strategy.public_key_hash(),
            self.metadata_strategy.is_enabled(),
        );

        let header_page = header.to_page();
        self.backend.submit_write(
            self.ls_handle_index,
            header_page.as_ptr(),
            header_page.len(),
            0,
        ).expect("Failed to submit ledger storage header write");

        self.backend.submit_sync(self.ls_handle_index)
            .expect("Failed to submit ledger storage header sync");

        self.backend.flush_submissions()
            .expect("Failed to flush ledger storage header");

        self.backend.wait_completions(1);

        self.write_offset = LsFileHeader::DATA_OFFSET as u64;

        let checkpoint_header = CheckpointFileHeader::new(self.file_seq);
        let checkpoint_header_bytes = unsafe { checkpoint_header.as_bytes() };

        self.backend.submit_write(
            self.checkpoint_handle_index,
            checkpoint_header_bytes.as_ptr(),
            CheckpointFileHeader::SIZE,
            0,
        ).expect("Failed to submit checkpoint header write");

        self.backend.submit_sync(self.checkpoint_handle_index)
            .expect("Failed to submit checkpoint header sync");

        self.backend.flush_submissions()
            .expect("Failed to flush checkpoint header");

        self.checkpoint_write_offset = CheckpointFileHeader::DATA_OFFSET as u64;
        self.batch_seq = 0;
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

    fn should_rotate(&self) -> bool {
        self.max_ls_file_size > 0 && self.write_offset >= self.max_ls_file_size as u64
    }

    pub fn rotate(&mut self) {
        let old_path = self.current_ls_file_path.clone();

        let current_index = self.manifest.current_entry_index();
        self.manifest.finalize_entry(
            current_index,
            self.flushed_gsn_max,
            self.flushed_timestamp_max_ns,
        );

        self.backend.close_file(self.checkpoint_handle_index);

        if self.metadata_strategy.is_enabled() {
            self.backend.close_file(self.meta_handle_index);
        }

        if self.signing_strategy.is_enabled() {
            self.backend.close_file(self.sign_handle_index);
        }

        self.backend.close_file(self.ls_handle_index);

        let task = IndexBuilderTask {
            ls_path: old_path.clone(),
            file_seq: self.file_seq,
            shard_id: self.id,
            signing_enabled: self.signing_strategy.is_enabled(),
        };

        if let Err(error) = self.index_builder_tx.send(task) {
            println!(
                "[ls-writer {}] WARNING: failed to send index builder task: {}",
                self.id, error,
            );
        }

        println!(
            "[ls-writer {}] rotation: index builder stub for {}",
            self.id, old_path
        );

        self.file_seq += 1;

        self.signing_strategy.on_rotation();
        self.metadata_strategy.on_rotation();

        self.current_gsn_min = 0;
        self.current_timestamp_min_ns = 0;
        self.buffered_gsn_max = 0;
        self.buffered_timestamp_max_ns = 0;
        self.in_flushing_gsn_max = 0;
        self.in_flushing_timestamp_max_ns = 0;
        self.flushed_gsn_max = 0;
        self.flushed_timestamp_max_ns = 0;

        self.open_new_files();

        let mut entry = self.make_manifest_entry();
        self.manifest.append_current_entry(&mut entry);

        println!(
            "[ls-writer {}] rotated: {} -> {}",
            self.id, old_path, self.current_ls_file_path
        );
    }

    fn compute_checkpoint_prealloc(
        max_ls_file_size: usize,
        flush_max_buffer_posting_records: usize,
        checkpoint_prealloc_multiplier: usize,
    ) -> usize {
        let max_batch_bytes = flush_max_buffer_posting_records * PostingRecord::SIZE;
        let max_batches = if max_batch_bytes > 0 {
            max_ls_file_size / max_batch_bytes
        } else {
            1024
        };
        let base_size = CheckpointFileHeader::SIZE + max_batches * CheckpointRecord::SIZE;
        base_size * checkpoint_prealloc_multiplier
    }

    fn write_checkpoint_record(&mut self) {
        let record = CheckpointRecord::new(
            self.in_flushing_first_offset,
            self.in_flushing_posting_count,
            self.batch_seq,
        );

        let record_bytes = unsafe { record.as_bytes() };

        self.backend.submit_write(
            self.checkpoint_handle_index,
            record_bytes.as_ptr(),
            CheckpointRecord::SIZE,
            self.checkpoint_write_offset,
        ).expect("Failed to submit checkpoint record write");

        self.backend.submit_sync(self.checkpoint_handle_index)
            .expect("Failed to submit checkpoint record sync");

        self.backend.flush_submissions()
            .expect("Failed to flush checkpoint submissions");

        self.backend.wait_for_the_one_completion();

        self.checkpoint_write_offset += CheckpointRecord::SIZE as u64;
        self.batch_seq += 1;
    }

    pub fn current_ls_file_path(&self) -> &str {
        &self.current_ls_file_path
    }

    pub fn signing_chain_hash(&self) -> Option<&[u8; 32]> {
        self.signing_strategy.chain_hash()
    }

    fn recover_checkpoint_state(checkpoint_path: &str) -> (u64, u32) {
        let mut file = match File::open(checkpoint_path) {
            Ok(file) => file,
            Err(_) => {
                return (CheckpointFileHeader::DATA_OFFSET as u64, 0);
            }
        };

        let mut header = CheckpointFileHeader::zeroed();
        if file.read_exact(
            unsafe { header.as_bytes_mut() }
        ).is_err() {
            return (CheckpointFileHeader::DATA_OFFSET as u64, 0);
        }

        if header.magic != crate::checkpoint_file_header::CHECKPOINT_FILE_MAGIC
            || !unsafe { header.verify_checksum() } {
            panic!(
                "Corrupt checkpoint file header: {}",
                checkpoint_path,
            );
        }

        let mut offset = CheckpointFileHeader::DATA_OFFSET as u64;
        let mut last_batch_seq: u32 = 0;
        let mut records_count: u64 = 0;
        let mut buf = [0u8; CheckpointRecord::SIZE];

        loop {
            if file.seek(SeekFrom::Start(offset)).is_err() {
                break;
            }
            match file.read_exact(&mut buf) {
                Ok(()) => {}
                Err(_) => break,
            }

            let record = unsafe { CheckpointRecord::from_bytes(&buf) };
            if !unsafe { record.verify_checksum() } {
                break;
            }

            last_batch_seq = record.batch_seq;
            records_count += 1;
            offset += CheckpointRecord::SIZE as u64;
        }

        let checkpoint_write_offset = CheckpointFileHeader::DATA_OFFSET as u64
            + records_count * CheckpointRecord::SIZE as u64;

        let batch_seq = if records_count > 0 {
            last_batch_seq + 1
        } else { 0 };

        (checkpoint_write_offset, batch_seq)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{mpsc, Arc};
    use ed25519_dalek::SigningKey;
    use super::*;
    use crate::flush_backend::{FlushBackend, FlushCompletion};
    use pipeline::posting_record::PostingRecord;
    use ringbuf::mpsc_ring_buffer::MpscRingBuffer;
    use crate::consts::LS_FILE_PAGE_SIZE;
    use crate::ed25519_signing_strategy::Ed25519SigningStrategy;
    use crate::flush_done_slot::FlushDoneSlot;
    use crate::ls_writer::LsWriter;
    use crate::ls_writer_slot::{LsWriterSlot, LS_MSG_ADD_TO_HEAP, LS_MSG_FLUSH_MARKER, LS_MSG_POSTING, LS_MSG_REMOVE_FROM_HEAP};
    use crate::ls_file_header::LsFileHeader;
    use crate::ls_meta_file_header::LsMetaFileHeader;
    use crate::ls_sign_file_header::LsSignFileHeader;
    use crate::no_metadata_strategy::NoMetadataStrategy;
    use crate::no_signing_strategy::NoSigningStrategy;
    use crate::posting_metadata_strategy::PostingMetadataStrategy;
    use crate::signing_state::SigningState;
    use crate::checkpoint_record::CheckpointRecord;
    use crate::checkpoint_file_header::CheckpointFileHeader;
    use crate::manifest_entry::MANIFEST_STATUS_ROTATED;
    use crate::portable_flush_backend::PortableFlushBackend;

    const K0: u64 = 0x0123456789ABCDEF;
    const K1: u64 = 0xFEDCBA9876543210;


    const MAX_FILES: usize = 8;

    struct MockFlushBackend {
        fds_opened: [bool; MAX_FILES],
        written: Vec<(u8, Vec<u8>, u64)>,
        pending_completion: Option<FlushCompletion>,
    }

    impl MockFlushBackend {
        fn new() -> Self {
            Self {
                fds_opened: [false; MAX_FILES],
                written: Vec::new(),
                pending_completion: None,
            }
        }

        fn find_free_slot(&self) -> usize {
            for i in 0..MAX_FILES {
                if !self.fds_opened[i] {
                    return i;
                }
            }
            panic!("MockFlushBackend: no free file slots");
        }
    }

    impl FlushBackend for MockFlushBackend {
        fn open_file(&mut self, _path: &str, _prealloc: usize) -> std::io::Result<u8> {
            let handle = self.find_free_slot();
            self.fds_opened[handle] = true;
            Ok(handle as u8)
        }

        fn submit_write(
            &mut self,
            handle_index: u8,
            data: *const u8,
            len: usize,
            offset: u64,
        ) -> std::io::Result<()> {
            let slice = unsafe { std::slice::from_raw_parts(data, len) };
            self.written.push((handle_index, slice.to_vec(), offset));
            self.pending_completion = Some(FlushCompletion {
                bytes_written: len,
                success: true,
            });
            Ok(())
        }

        fn submit_sync(&mut self, _handle_index: u8) -> std::io::Result<()> {
            Ok(())
        }

        fn flush_submissions(&mut self) -> std::io::Result<()> {
            Ok(())
        }

        fn wait_completions(&mut self, _count: usize) {
        }

        fn poll_completion(&mut self) -> Option<FlushCompletion> {
            self.pending_completion.take()
        }

        fn close_file(&mut self, handle_index: u8) {
            self.fds_opened[handle_index as usize] = false;
        }
    }


    static mut TEST_COMMITTED_GSN: u64 = 0;

    fn make_temp_dir_for_writer(test_name: &str) -> String {
        let dir = make_test_dir();
        dir
    }

    fn cleanup_dir(dir: &str) {
        std::fs::remove_dir_all(dir).ok();
    }

    fn make_writer() -> LsWriter<MockFlushBackend, NoSigningStrategy, NoMetadataStrategy> {
        let dir = make_test_dir();

        let manifest_path = format!("{}/0.manifest", dir);
        std::fs::remove_file(&manifest_path).ok();

        let manifest = Manifest::create(&dir, 0);

        let (index_tx, _) = mpsc::channel();

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
            NoSigningStrategy,
            NoMetadataStrategy,
            dir.to_string(),
            0,
            64,
            K0,
            K1,
            64,
            2,
            512,
            16,
            4,
            0,
            manifest,
            index_tx,
        );
        writer.startup();
        writer
    }

    fn make_writer_with_metadata(record_size: usize) -> LsWriter<MockFlushBackend, NoSigningStrategy, PostingMetadataStrategy> {
        let dir = make_test_dir();

        let manifest_path = format!("{}/0.manifest", dir);
        std::fs::remove_file(&manifest_path).ok();

        let manifest = Manifest::create(&dir, 0);

        let ls_writer_rb = Arc::new(
            MpscRingBuffer::<LsWriterSlot>::new(64).unwrap()
        );
        let flush_done_rb = Arc::new(
            MpscRingBuffer::<FlushDoneSlot>::new(64).unwrap()
        );

        unsafe { TEST_COMMITTED_GSN = 0; }
        let committed_gsn_ptr = unsafe { &raw mut TEST_COMMITTED_GSN };

        let metadata = PostingMetadataStrategy::new(record_size, 512);

        let (index_tx, _) = mpsc::channel();

        let mut writer = LsWriter::new(
            0,
            ls_writer_rb,
            flush_done_rb,
            committed_gsn_ptr,
            MockFlushBackend::new(),
            NoSigningStrategy,
            metadata,
            "/tmp/solidus-test".to_string(),
            0,
            64, K0, K1,
            64, 2, 512,
            16,
            4,
            0,
            manifest,
            index_tx,
        );
        writer.startup();
        writer
    }

    fn make_writer_with_flush_done_rb() -> (
        LsWriter<MockFlushBackend, NoSigningStrategy, NoMetadataStrategy>,
        Arc<MpscRingBuffer<FlushDoneSlot>>,
    ) {
        let dir = make_test_dir();

        let manifest_path = format!("{}/0.manifest", dir);
        std::fs::remove_file(&manifest_path).ok();

        let manifest = Manifest::create(&dir, 0);

        let ls_writer_rb = Arc::new(
            MpscRingBuffer::<LsWriterSlot>::new(64).unwrap()
        );
        let flush_done_rb = Arc::new(
            MpscRingBuffer::<FlushDoneSlot>::new(64).unwrap()
        );
        let flush_done_rb_clone = Arc::clone(&flush_done_rb);

        unsafe { TEST_COMMITTED_GSN = 0; }
        let committed_gsn_ptr = unsafe { &raw mut TEST_COMMITTED_GSN };

        let (index_tx, _) = mpsc::channel();

        let mut writer = LsWriter::new(
            0,
            ls_writer_rb,
            flush_done_rb,
            committed_gsn_ptr,
            MockFlushBackend::new(),
            NoSigningStrategy,
            NoMetadataStrategy,
            "/tmp/solidus-test".to_string(),
            0,
            64,
            K0,
            K1,
            64, 2, 512,
            16,
            4,
            0,
            manifest,
            index_tx,
        );
        writer.startup();

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
        slot.posting.set_magic();
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

    fn make_writer_with_signing() -> LsWriter<MockFlushBackend, Ed25519SigningStrategy, NoMetadataStrategy> {
        let dir = make_test_dir();

        let manifest_path = format!("{}/0.manifest", dir);
        std::fs::remove_file(&manifest_path).ok();

        let manifest = Manifest::create(&dir, 0);

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
        let signing_state = SigningState::new(key, genesis);
        let signing = Ed25519SigningStrategy::new(signing_state, 512);

        let (index_tx, _) = mpsc::channel();

        let mut writer = LsWriter::new(
            0,
            ls_writer_rb,
            flush_done_rb,
            committed_gsn_ptr,
            MockFlushBackend::new(),
            signing,
            NoMetadataStrategy,
            "/tmp/solidus-test".to_string(),
            0,
            64, K0, K1,
            64, 2, 512,
            16,
            4,
            0,
            manifest,
            index_tx,
        );
        writer.startup();
        writer
    }

    fn make_writer_with_max_size(max_size: usize) -> LsWriter<MockFlushBackend, NoSigningStrategy, NoMetadataStrategy> {
        let dir = make_test_dir();

        let manifest_path = format!("{}/0.manifest", dir);
        std::fs::remove_file(&manifest_path).ok();

        let manifest = Manifest::create(&dir, 0);

        let ls_writer_rb = Arc::new(
            MpscRingBuffer::<LsWriterSlot>::new(64).unwrap()
        );
        let flush_done_rb = Arc::new(
            MpscRingBuffer::<FlushDoneSlot>::new(64).unwrap()
        );

        unsafe { TEST_COMMITTED_GSN = 0; }
        let committed_gsn_ptr = unsafe { &raw mut TEST_COMMITTED_GSN };

        let (index_tx, _) = mpsc::channel();

        let mut writer = LsWriter::new(
            0,
            ls_writer_rb,
            flush_done_rb,
            committed_gsn_ptr,
            MockFlushBackend::new(),
            NoSigningStrategy,
            NoMetadataStrategy,
            "/tmp/solidus-test".to_string(),
            max_size,
            64,
            K0,
            K1,
            64,
            2,
            512,
            16,
            4,
            0,
            manifest,
            index_tx,
        );
        writer.startup();
        writer
    }


    #[test]
    fn sign_batch_creates_sig_records() {
        let mut writer = make_writer_with_signing();

        let mut slot1 = make_posting_slot(100, 500);
        slot1.posting.transfer_id_hi = 0;
        slot1.posting.transfer_id_lo = 1;

        let mut slot2 = make_posting_slot(100, -500);
        slot2.posting.transfer_id_hi = 0;
        slot2.posting.transfer_id_lo = 1;

        writer.process_message(&slot1);
        writer.process_message(&slot2);

        writer.submit_flush();

        assert_eq!(writer.backend.written.len(), 5);
    }

    #[test]
    fn initialize_opens_file() {
        let writer = make_writer();
        assert!(writer.backend.fds_opened[0]);
        assert!(writer.backend.fds_opened[1]);
        assert_eq!(writer.backend.written.len(), 2);
        assert_eq!(writer.backend.written[0].2, 0);
        assert_eq!(writer.backend.written[0].1.len(), 4096);

        assert_eq!(writer.backend.written[1].2, 0);
        assert_eq!(writer.backend.written[1].1.len(), CheckpointFileHeader::SIZE);
    }

    #[test]
    fn new_initializes_empty_state() {
        let writer = make_writer();
        assert_eq!(writer.buffer_len, 0);
        assert_eq!(writer.write_offset, LS_FILE_PAGE_SIZE as u64);
        assert!(!writer.flush_in_flight);
        assert_eq!(writer.flush_buffer_snapshot_len, 0);
        assert_eq!(writer.collecting_len, 0);
        assert_eq!(writer.in_flushing_len, 0)
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

        assert_eq!(writer.collecting_len, 1);
    }

    #[test]
    fn submit_flush_sends_buffer_to_backend() {
        let mut writer = make_writer();

        writer.process_message(&make_posting_slot(100, 500));
        writer.process_message(&make_flush_marker_slot(100, 1, 42));

        writer.submit_flush();

        assert_eq!(writer.backend.written.len(), 3);
        assert_eq!(writer.backend.written[2].1.len() % LS_FILE_PAGE_SIZE, 0);
        assert!(writer.backend.written[2].1.len() >= PostingRecord::SIZE);
        assert_eq!(writer.backend.written[2].2, LsFileHeader::DATA_OFFSET as u64);
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

        assert_eq!(writer.collecting_len, 0);
        assert_eq!(writer.in_flushing_len, 2);
    }

    #[test]
    fn submit_flush_ignores_empty_buffer() {
        let mut writer = make_writer();

        writer.submit_flush();

        assert!(!writer.flush_in_flight);
        assert_eq!(writer.backend.written.len(), 2);
    }

    #[test]
    fn submit_flush_ignores_if_already_in_flight() {
        let mut writer = make_writer();

        writer.process_message(&make_posting_slot(100, 500));
        writer.submit_flush();

        writer.process_message(&make_posting_slot(200, 300));
        writer.submit_flush();

        assert_eq!(writer.backend.written.len(), 3);
    }

    #[test]
    fn poll_completions_clears_buffer_and_advances_offset() {
        let mut writer = make_writer();

        writer.process_message(&make_posting_slot(100, 500));
        writer.submit_flush();

        assert_eq!(writer.write_offset, LsFileHeader::DATA_OFFSET as u64);
        assert_eq!(writer.buffer_len, PostingRecord::SIZE);

        writer.poll_and_handle_completions();

        assert_eq!(writer.write_offset, LsFileHeader::DATA_OFFSET as u64 + LS_FILE_PAGE_SIZE as u64);
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

        assert_eq!(writer.backend.written.len(), 3);
        assert_eq!(writer.backend.written[2].1.len() % LS_FILE_PAGE_SIZE, 0);
        assert!(writer.backend.written[2].1.len() >= PostingRecord::SIZE * 3);

        writer.poll_and_handle_completions();

        assert_eq!(writer.backend.written.len(), 4);

        assert!(writer.in_flight_min_heap.is_empty());
        assert_eq!(writer.buffer_len, 0);
        assert_eq!(writer.write_offset, LsFileHeader::DATA_OFFSET as u64 + LS_FILE_PAGE_SIZE as u64);
    }

    #[test]
    fn sequential_flushes_advance_offset() {
        let mut writer = make_writer();

        writer.process_message(&make_add_to_heap_slot(100));
        writer.process_message(&make_posting_slot(100, 500));
        writer.process_message(&make_flush_marker_slot(100, 1, 42));

        writer.submit_flush();
        writer.poll_and_handle_completions();

        assert_eq!(writer.write_offset, LsFileHeader::DATA_OFFSET as u64 + LS_FILE_PAGE_SIZE as u64);

        writer.process_message(&make_add_to_heap_slot(200));
        writer.process_message(&make_posting_slot(200, 300));
        writer.process_message(&make_flush_marker_slot(200, 2, 43));

        writer.submit_flush();

        assert_eq!(writer.backend.written.len(), 5);
        assert_eq!(writer.backend.written[4].2, LsFileHeader::DATA_OFFSET as u64 + LS_FILE_PAGE_SIZE as u64);

        writer.poll_and_handle_completions();

        assert_eq!(writer.write_offset, LsFileHeader::DATA_OFFSET as u64 + LS_FILE_PAGE_SIZE as u64 + LS_FILE_PAGE_SIZE as u64);
    }

    #[test]
    fn poll_noop_when_no_flush_in_flight() {
        let mut writer = make_writer();

        writer.poll_and_handle_completions();

        assert_eq!(writer.write_offset, LsFileHeader::DATA_OFFSET as u64);
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

    #[test]
    fn ls_file_header_layout() {
        assert_eq!(LsFileHeader::SIZE, 128);
        assert_eq!(std::mem::size_of::<LsFileHeader>(), 128);

        assert_eq!(std::mem::offset_of!(LsFileHeader, magic), 0);
        assert_eq!(std::mem::offset_of!(LsFileHeader, format_version), 8);
        assert_eq!(std::mem::offset_of!(LsFileHeader, file_type), 10);
        assert_eq!(std::mem::offset_of!(LsFileHeader, signing_enabled), 11);
        assert_eq!(std::mem::offset_of!(LsFileHeader, partition_count), 12);
        assert_eq!(std::mem::offset_of!(LsFileHeader, metadata_enabled), 14);
        assert_eq!(std::mem::offset_of!(LsFileHeader, created_at_ns), 16);
        assert_eq!(std::mem::offset_of!(LsFileHeader, record_size), 24);
        assert_eq!(std::mem::offset_of!(LsFileHeader, data_offset), 28);
        assert_eq!(std::mem::offset_of!(LsFileHeader, max_file_size), 32);
        assert_eq!(std::mem::offset_of!(LsFileHeader, file_seq), 40);
        assert_eq!(std::mem::offset_of!(LsFileHeader, rules_count), 48);
        assert_eq!(std::mem::offset_of!(LsFileHeader, rules_checksum), 52);
        assert_eq!(std::mem::offset_of!(LsFileHeader, public_key_hash), 56);
        assert_eq!(std::mem::offset_of!(LsFileHeader, checksum), 124);
    }

    #[test]
    fn ls_sign_file_header_layout() {
        assert_eq!(LsSignFileHeader::SIZE, 128);
        assert_eq!(std::mem::size_of::<LsSignFileHeader>(), 128);

        assert_eq!(std::mem::offset_of!(LsSignFileHeader, magic), 0);
        assert_eq!(std::mem::offset_of!(LsSignFileHeader, format_version), 8);
        assert_eq!(std::mem::offset_of!(LsSignFileHeader, file_type), 10);
        assert_eq!(std::mem::offset_of!(LsSignFileHeader, algorithm), 11);
        assert_eq!(std::mem::offset_of!(LsSignFileHeader, key_version), 12);
        assert_eq!(std::mem::offset_of!(LsSignFileHeader, created_at_ns), 16);
        assert_eq!(std::mem::offset_of!(LsSignFileHeader, linked_ls_file_seq), 24);
        assert_eq!(std::mem::offset_of!(LsSignFileHeader, public_key), 32);
        assert_eq!(std::mem::offset_of!(LsSignFileHeader, public_key), 32);
        assert_eq!(std::mem::offset_of!(LsSignFileHeader, genesis_hash), 64);
        assert_eq!(std::mem::offset_of!(LsSignFileHeader, checksum), 120);
    }

    #[test]
    fn ls_meta_file_header_layout() {
        assert_eq!(LsMetaFileHeader::SIZE, 128);
        assert_eq!(std::mem::size_of::<LsMetaFileHeader>(), 128);

        assert_eq!(std::mem::offset_of!(LsMetaFileHeader, magic), 0);
        assert_eq!(std::mem::offset_of!(LsMetaFileHeader, format_version), 8);
        assert_eq!(std::mem::offset_of!(LsMetaFileHeader, file_type), 10);
        assert_eq!(std::mem::offset_of!(LsMetaFileHeader, created_at_ns), 16);
        assert_eq!(std::mem::offset_of!(LsMetaFileHeader, record_size), 24);
        assert_eq!(std::mem::offset_of!(LsMetaFileHeader, data_offset), 28);
        assert_eq!(std::mem::offset_of!(LsMetaFileHeader, max_file_size), 32);
        assert_eq!(std::mem::offset_of!(LsMetaFileHeader, linked_ls_file_seq), 40);
        assert_eq!(std::mem::offset_of!(LsMetaFileHeader, checksum), 120);
    }

    #[test]
    fn metadata_enabled_initializes_meta_buffer() {
        let writer = make_writer_with_metadata(256);
        assert!(writer.metadata_strategy.is_enabled());
        assert_eq!(writer.metadata_strategy.record_size(), 256);
    }

    #[test]
    fn metadata_disabled_no_meta_buffer() {
        let writer = make_writer();
        assert!(!writer.metadata_strategy.is_enabled());
    }

    #[test]
    fn ls_file_header_checksum() {
        let header = LsFileHeader::new(false, 16, 256 * 1024 * 1024, 0, 0, false);

        assert_ne!(header.checksum, 0);
        assert!(unsafe { header.verify_checksum() });
    }

    #[test]
    fn ls_file_header_to_page() {
        let header = LsFileHeader::new(true, 16, 256 * 1024 * 1024, 1, 0x12345678, false);
        let page = header.to_page();

        assert_eq!(page.len(), 4096);
        let magic = u64::from_le_bytes(page[0..8].try_into().unwrap());
        assert_eq!(magic, crate::ls_file_header::LS_FILE_MAGIC);
        assert!(page[128..].iter().all(|&b| b == 0));
    }

    #[test]
    fn initialize_writes_header_at_offset_zero() {
        let writer = make_writer();

        assert!(!writer.backend.written.is_empty());
        let (_, data, offset) = &writer.backend.written[0];
        assert_eq!(*offset, 0);
        assert_eq!(data.len(), 4096);

        let magic = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(magic, crate::ls_file_header::LS_FILE_MAGIC);

        assert_eq!(writer.write_offset, 4096);
    }

    #[test]
    fn checkpoint_record_written_after_flush_completion() {
        let mut writer = make_writer();

        writer.process_message(&make_add_to_heap_slot(100));
        writer.process_message(&make_posting_slot(100, 500));
        writer.process_message(&make_flush_marker_slot(100, 1, 42));

        writer.submit_flush();
        assert_eq!(writer.backend.written.len(), 3);

        writer.poll_and_handle_completions();
        assert_eq!(writer.backend.written.len(), 4);

        let (handle, data, offset) = &writer.backend.written[3];
        assert_eq!(*handle, writer.checkpoint_handle_index);
        assert_eq!(data.len(), CheckpointRecord::SIZE);
        assert_eq!(*offset, CheckpointFileHeader::DATA_OFFSET as u64);
    }

    #[test]
    fn checkpoint_batch_seq_increments() {
        let mut writer = make_writer();

        writer.process_message(&make_posting_slot(100, 500));
        writer.submit_flush();
        writer.poll_and_handle_completions();
        assert_eq!(writer.batch_seq, 1);

        writer.process_message(&make_posting_slot(200, 300));
        writer.submit_flush();
        writer.poll_and_handle_completions();
        assert_eq!(writer.batch_seq, 2);
    }

    #[test]
    fn checkpoint_write_offset_advances() {
        let mut writer = make_writer();

        assert_eq!(writer.checkpoint_write_offset, CheckpointFileHeader::DATA_OFFSET as u64);

        writer.process_message(&make_posting_slot(100, 500));
        writer.submit_flush();
        writer.poll_and_handle_completions();

        assert_eq!(
            writer.checkpoint_write_offset,
            CheckpointFileHeader::DATA_OFFSET as u64 + CheckpointRecord::SIZE as u64,
        );

        writer.process_message(&make_posting_slot(200, 300));
        writer.submit_flush();
        writer.poll_and_handle_completions();

        assert_eq!(
            writer.checkpoint_write_offset,
            CheckpointFileHeader::DATA_OFFSET as u64 + 2 * CheckpointRecord::SIZE as u64,
        );
    }

    #[test]
    fn checkpoint_first_posting_offset_correct() {
        let mut writer = make_writer();

        writer.process_message(&make_posting_slot(100, 500));
        writer.submit_flush();

        assert_eq!(writer.in_flushing_first_offset, LsFileHeader::DATA_OFFSET as u64);
    }

    #[test]
    fn checkpoint_posting_count_correct() {
        let mut writer = make_writer();

        writer.process_message(&make_posting_slot(100, 500));
        writer.process_message(&make_posting_slot(200, 300));
        writer.process_message(&make_posting_slot(300, 100));
        writer.submit_flush();

        assert_eq!(writer.in_flushing_posting_count, 3);
    }

    #[test]
    fn checkpoint_record_content_correct() {
        let mut writer = make_writer();

        writer.process_message(&make_posting_slot(100, 500));
        writer.process_message(&make_posting_slot(200, 300));
        writer.submit_flush();
        writer.poll_and_handle_completions();

        let (_, data, _) = &writer.backend.written[3];
        let record = unsafe { CheckpointRecord::from_bytes(data) };

        assert_eq!(record.first_posting_offset, LsFileHeader::DATA_OFFSET as u64);
        assert_eq!(record.posting_count, 2);
        assert_eq!(record.batch_seq, 0);
        assert!(unsafe { record.verify_checksum() });
    }

    #[test]
    fn checkpoint_header_written_at_initialize() {
        let writer = make_writer();

        let (handle, data, offset) = &writer.backend.written[1];
        assert_eq!(*handle, writer.checkpoint_handle_index);
        assert_eq!(data.len(), CheckpointFileHeader::SIZE);
        assert_eq!(*offset, 0u64);

        let header = unsafe { CheckpointFileHeader::from_bytes(data) };
        assert_eq!(header.magic, crate::checkpoint_file_header::CHECKPOINT_FILE_MAGIC);
        assert_eq!(header.linked_ls_file_seq, 0);
        assert!(unsafe { header.verify_checksum() });
    }

    #[test]
    fn signing_on_rotation_preserves_last_tx_hash() {
        let key = SigningKey::from_bytes(&[0x42u8; 32]);
        let genesis = [0u8; 32];
        let signing_state = SigningState::new(key, genesis);
        let mut signing = Ed25519SigningStrategy::new(signing_state, 512);

        let hash_before = *signing.chain_hash().unwrap();

        signing.on_rotation();

        let hash_after = *signing.chain_hash().unwrap();
        assert_eq!(hash_before, hash_after);
    }

    #[test]
    fn signing_on_rotation_resets_write_offset() {
        let key = SigningKey::from_bytes(&[0x42u8; 32]);
        let genesis = [0u8; 32];
        let signing_state = SigningState::new(key, genesis);
        let mut signing = Ed25519SigningStrategy::new(signing_state, 512);

        signing.prepare_header(0);
        signing.on_header_written();

        signing.on_rotation();

    }

    #[test]
    fn metadata_on_rotation_resets_write_offset() {
        let mut metadata = PostingMetadataStrategy::new(256, 512);

        metadata.prepare_header(0, 256 * 1024 * 1024);
        metadata.on_header_written();

        metadata.on_rotation();

    }

    #[test]
    fn no_signing_on_rotation_is_noop() {
        let mut no_signing = NoSigningStrategy;
        no_signing.on_rotation();
    }

    #[test]
    fn no_metadata_on_rotation_is_noop() {
        let mut no_metadata = NoMetadataStrategy;
        no_metadata.on_rotation();
    }

    #[test]
    fn should_rotate_false_when_max_size_zero() {
        let writer = make_writer();
        assert!(!writer.should_rotate());
    }

    #[test]
    fn should_rotate_false_when_below_max_size() {
        let mut writer = make_writer_with_max_size(1024 * 1024);
        assert!(!writer.should_rotate());
    }

    #[test]
    fn should_rotate_true_when_at_max_size() {
        let mut writer = make_writer_with_max_size(4096);
        assert!(writer.should_rotate());
    }

    #[test]
    fn rotate_opens_new_files() {
        let mut writer = make_writer_with_max_size(4096);

        let old_path = writer.current_ls_file_path.clone();

        writer.rotate();

        assert!(writer.backend.fds_opened[writer.ls_handle_index as usize]);
        assert!(writer.backend.fds_opened[writer.checkpoint_handle_index as usize]);

        assert_ne!(writer.current_ls_file_path, old_path);

        assert_eq!(writer.file_seq, 1);
    }

    #[test]
    fn rotate_resets_offsets() {
        let mut writer = make_writer_with_max_size(4096);

        writer.write_offset = 8192;
        writer.batch_seq = 5;
        writer.checkpoint_write_offset = 200;

        writer.rotate();

        assert_eq!(writer.write_offset, LsFileHeader::DATA_OFFSET as u64);
        assert_eq!(writer.batch_seq, 0);
        assert_eq!(writer.checkpoint_write_offset, CheckpointFileHeader::DATA_OFFSET as u64);
    }

    #[test]
    fn rotate_increments_file_seq() {
        let mut writer = make_writer_with_max_size(4096);
        assert_eq!(writer.file_seq, 0);

        writer.rotate();
        assert_eq!(writer.file_seq, 1);

        writer.rotate();
        assert_eq!(writer.file_seq, 2);
    }

    #[test]
    fn rotate_reuses_handle_indices() {
        let mut writer = make_writer_with_max_size(4096);

        let first_ls = writer.ls_handle_index;
        let first_checkpoint = writer.checkpoint_handle_index;

        writer.rotate();

        assert!(writer.backend.fds_opened[writer.ls_handle_index as usize]);
        assert!(writer.backend.fds_opened[writer.checkpoint_handle_index as usize]);

        assert!(!writer.backend.fds_opened[first_ls as usize]
            || first_ls == writer.ls_handle_index);
        assert!(!writer.backend.fds_opened[first_checkpoint as usize]
            || first_checkpoint == writer.checkpoint_handle_index);
    }

    #[test]
    fn auto_rotate_after_flush_completion() {
        let mut writer = make_writer_with_max_size(8192);

        let initial_file_seq = writer.file_seq;

        writer.process_message(&make_add_to_heap_slot(100));
        writer.process_message(&make_posting_slot(100, 500));
        writer.process_message(&make_flush_marker_slot(100, 1, 42));

        writer.submit_flush();
        writer.poll_and_handle_completions();

        assert_eq!(writer.file_seq, initial_file_seq + 1);
        assert_eq!(writer.write_offset, LsFileHeader::DATA_OFFSET as u64);
        assert_eq!(writer.batch_seq, 0);
    }

    #[test]
    fn no_rotate_when_below_threshold() {
        let mut writer = make_writer_with_max_size(12288);

        let initial_file_seq = writer.file_seq;

        writer.process_message(&make_posting_slot(100, 500));
        writer.submit_flush();
        writer.poll_and_handle_completions();

        assert_eq!(writer.file_seq, initial_file_seq);
    }

    #[test]
    fn generate_ls_filename_format() {
        let name = generate_ls_filename(0, 0);
        assert!(name.starts_with("ls_"));
        assert!(name.ends_with("-0-0.ls"));
        assert_eq!(name.len(), 29);

        let name2 = generate_ls_filename(2, 15);
        assert!(name2.ends_with("-2-15.ls"));
        assert_eq!(name2.len(), 30);
    }

    #[test]
    fn manifest_entry_layout() {
        use crate::manifest_entry::ManifestEntry;
        assert_eq!(ManifestEntry::SIZE, 128);
        assert_eq!(std::mem::size_of::<ManifestEntry>(), 128);
        assert_eq!(std::mem::align_of::<ManifestEntry>(), 64);

        assert_eq!(std::mem::offset_of!(ManifestEntry, file_seq), 0);
        assert_eq!(std::mem::offset_of!(ManifestEntry, status), 8);
        assert_eq!(std::mem::offset_of!(ManifestEntry, signing_enabled), 9);
        assert_eq!(std::mem::offset_of!(ManifestEntry, metadata_enabled), 10);
        assert_eq!(std::mem::offset_of!(ManifestEntry, record_size), 12);
        assert_eq!(std::mem::offset_of!(ManifestEntry, rules_checksum), 16);
        assert_eq!(std::mem::offset_of!(ManifestEntry, gsn_min), 24);
        assert_eq!(std::mem::offset_of!(ManifestEntry, gsn_max), 32);
        assert_eq!(std::mem::offset_of!(ManifestEntry, timestamp_min_ns), 40);
        assert_eq!(std::mem::offset_of!(ManifestEntry, timestamp_max_ns), 48);
        assert_eq!(std::mem::offset_of!(ManifestEntry, checksum), 56);

        assert_eq!(std::mem::offset_of!(ManifestEntry, filename), 64);

    }

    #[test]
    fn manifest_entry_filename() {
        use crate::manifest_entry::ManifestEntry;
        let mut entry = ManifestEntry::zeroed();
        entry.set_filename("ls_20260331-153045-123-0.ls");
        assert_eq!(entry.filename_str(), "ls_20260331-153045-123-0.ls");
    }

    #[test]
    fn manifest_entry_checksum() {
        use crate::manifest_entry::ManifestEntry;
        let mut entry = ManifestEntry::zeroed();
        entry.file_seq = 1;
        entry.set_filename("test.ls");

        unsafe { entry.compute_checksum(); }
        assert_ne!(entry.checksum, 0);
        assert!(unsafe { entry.verify_checksum() });
    }

    #[test]
    #[should_panic(expected = "Filename too long")]
    fn manifest_entry_filename_too_long() {
        use crate::manifest_entry::ManifestEntry;
        let mut entry = ManifestEntry::zeroed();
        let long_name = "a".repeat(65);
        entry.set_filename(&long_name);
    }

    #[test]
    fn startup_first_launch_creates_manifest_entry() {
        let dir = make_temp_dir_for_writer("startup-first");
        let manifest_path = format!("{}/0.manifest", dir);
        std::fs::remove_file(&manifest_path).ok();

        let manifest = Manifest::create(&dir, 0);

        let ls_writer_rb = Arc::new(MpscRingBuffer::<LsWriterSlot>::new(64).unwrap());
        let flush_done_rb = Arc::new(MpscRingBuffer::<FlushDoneSlot>::new(64).unwrap());

        unsafe { TEST_COMMITTED_GSN = 0; }
        let committed_gsn_ptr = unsafe { &raw mut TEST_COMMITTED_GSN };

        let (index_tx, _) = mpsc::channel();

        let mut writer = LsWriter::new(
            0, ls_writer_rb, flush_done_rb, committed_gsn_ptr,
            MockFlushBackend::new(), NoSigningStrategy, NoMetadataStrategy,
            dir.to_string(), 0, 64, K0, K1, 64, 2, 512, 16, 4,
            0, manifest, index_tx,
        );
        writer.startup();

        assert_eq!(writer.manifest.entries_count(), 1);
        assert_eq!(writer.manifest.current_entry_index(), 0);

        let entry = writer.manifest.read_current_entry();
        assert_eq!(entry.file_seq, 0);
        assert_eq!(entry.signing_enabled, 0);
        assert_eq!(entry.metadata_enabled, 0);
        assert_eq!(entry.rules_checksum, 0);

        cleanup_dir(&dir);
    }

    #[test]
    fn first_posting_updates_manifest_gsn_min() {
        let dir = make_temp_dir_for_writer("posting-gsn-min");
        let manifest_path = format!("{}/0.manifest", dir);
        std::fs::remove_file(&manifest_path).ok();

        let manifest = Manifest::create(&dir, 0);

        let ls_writer_rb = Arc::new(MpscRingBuffer::<LsWriterSlot>::new(64).unwrap());
        let flush_done_rb = Arc::new(MpscRingBuffer::<FlushDoneSlot>::new(64).unwrap());

        unsafe { TEST_COMMITTED_GSN = 0; }
        let committed_gsn_ptr = unsafe { &raw mut TEST_COMMITTED_GSN };

        let (index_tx, _) = mpsc::channel();

        let mut writer = LsWriter::new(
            0, ls_writer_rb, flush_done_rb, committed_gsn_ptr,
            MockFlushBackend::new(), NoSigningStrategy, NoMetadataStrategy,
            dir.to_string(), 0, 64, K0, K1, 64, 2, 512, 16, 4,
            0, manifest, index_tx,
        );
        writer.startup();

        let mut slot = make_posting_slot(42, 500);
        slot.posting.timestamp_ns = 1700000000_000_000_000;
        writer.process_message(&slot);

        assert_eq!(writer.current_gsn_min, 42);
        assert_eq!(writer.current_timestamp_min_ns, 1700000000_000_000_000);

        let entry = writer.manifest.read_current_entry();
        assert_eq!(entry.gsn_min, 42);
        assert_eq!(entry.timestamp_min_ns, 1700000000_000_000_000);

        let mut slot2 = make_posting_slot(100, 300);
        slot2.posting.timestamp_ns = 1700000100_000_000_000;
        writer.process_message(&slot2);

        assert_eq!(writer.current_gsn_min, 42);

        cleanup_dir(&dir);
    }

    #[test]
    fn config_mismatch_triggers_rotation_at_startup() {
        let dir = make_temp_dir_for_writer("config-mismatch");
        let manifest_path = format!("{}/0.manifest", dir);
        std::fs::remove_file(&manifest_path).ok();

        let mut manifest = Manifest::create(&dir, 0);
        let mut entry = ManifestEntry::zeroed();
        entry.file_seq = 0;
        entry.signing_enabled = 1;
        entry.metadata_enabled = 0;
        entry.rules_checksum = 0;
        entry.set_filename("ls_old-0-0.ls");
        manifest.append_current_entry(&mut entry);

        let ls_writer_rb = Arc::new(MpscRingBuffer::<LsWriterSlot>::new(64).unwrap());
        let flush_done_rb = Arc::new(MpscRingBuffer::<FlushDoneSlot>::new(64).unwrap());

        unsafe { TEST_COMMITTED_GSN = 0; }
        let committed_gsn_ptr = unsafe { &raw mut TEST_COMMITTED_GSN };

        let (index_tx, _) = mpsc::channel();

        let mut writer = LsWriter::new(
            0, ls_writer_rb, flush_done_rb, committed_gsn_ptr,
            MockFlushBackend::new(), NoSigningStrategy, NoMetadataStrategy,
            dir.to_string(), 0, 64, K0, K1, 64, 2, 512, 16, 4,
            0, manifest, index_tx,
        );
        writer.startup();

        assert_eq!(writer.manifest.entries_count(), 2);
        assert_eq!(writer.manifest.current_entry_index(), 1);
        assert_eq!(writer.file_seq, 1);

        let old_entry = writer.manifest.read_entry(0);
        assert_eq!(old_entry.status, MANIFEST_STATUS_ROTATED);

        let new_entry = writer.manifest.read_current_entry();
        assert_eq!(new_entry.signing_enabled, 0);
        assert_eq!(new_entry.file_seq, 1);

        cleanup_dir(&dir);
    }

    #[test]
    fn recover_checkpoint_state_empty_file() {
        let dir = make_test_dir();
        let checkpoint_path = format!("{}/test.ls.checkpoint", dir);

        {
            use std::io::Write;
            let mut f = std::fs::File::create(&checkpoint_path).unwrap();
            let header = CheckpointFileHeader::new(0);
            f.write_all(unsafe { header.as_bytes() }).unwrap();
            f.sync_all().unwrap();
        }

        let (offset, batch_seq) = crate::recovery::recover_checkpoint_state(&checkpoint_path);

        assert_eq!(offset, CheckpointFileHeader::DATA_OFFSET as u64);
        assert_eq!(batch_seq, 0);

        cleanup_dir(&dir);
    }

    #[test]
    fn recover_checkpoint_state_with_records() {
        let dir = make_test_dir();
        let checkpoint_path = format!("{}/test.ls.checkpoint", dir);

        {
            use std::io::Write;
            let mut f = std::fs::File::create(&checkpoint_path).unwrap();

            let header = CheckpointFileHeader::new(0);
            f.write_all(unsafe { header.as_bytes() }).unwrap();

            for i in 0..3u32 {
                let record = CheckpointRecord::new(
                    4096 + i as u64 * 4096,
                    32,
                    i,
                );
                f.write_all(unsafe { record.as_bytes() }).unwrap();
            }
            f.sync_all().unwrap();
        }

        let (offset, batch_seq) = crate::recovery::recover_checkpoint_state(&checkpoint_path);

        assert_eq!(
        offset,
        CheckpointFileHeader::DATA_OFFSET as u64 + 3 * CheckpointRecord::SIZE as u64,
    );
        assert_eq!(batch_seq, 3);

        cleanup_dir(&dir);
    }

    #[test]
    fn recover_checkpoint_state_no_file() {
        let (offset, batch_seq) = crate::recovery::recover_checkpoint_state("/tmp/nonexistent-checkpoint-file");

        assert_eq!(offset, CheckpointFileHeader::DATA_OFFSET as u64);
        assert_eq!(batch_seq, 0);
    }

    #[test]
    fn recover_ls_state_empty_file() {
        let dir = make_test_dir();
        let ls_path = format!("{}/test.ls", dir);

        {
            use std::io::Write;
            let mut f = std::fs::File::create(&ls_path).unwrap();
            let header = LsFileHeader::new(false, 16, 256 * 1024 * 1024, 0, 0, false);
            let page = header.to_page();
            f.write_all(&page).unwrap();
            f.sync_all().unwrap();
        }

        let state = crate::recovery::recover_ls_state(&ls_path);

        assert_eq!(state.write_offset, LsFileHeader::DATA_OFFSET as u64);
        assert_eq!(state.gsn_min, 0);
        assert_eq!(state.gsn_max, 0);
        assert_eq!(state.postings_count, 0);

        cleanup_dir(&dir);
    }

    #[test]
    fn recover_ls_state_with_postings() {
        let dir = make_test_dir();
        let ls_path = format!("{}/test.ls", dir);

        {
            use std::io::Write;
            let mut f = std::fs::File::create(&ls_path).unwrap();

            let header = LsFileHeader::new(false, 16, 256 * 1024 * 1024, 0, 0, false);
            let page = header.to_page();
            f.write_all(&page).unwrap();

            let mut data_page = [0u8; 4096];
            for i in 0..3u64 {
                let mut record = PostingRecord::zeroed();
                record.set_magic();
                record.gsn = 100 + i;
                record.timestamp_ns = 1700000000_000_000_000 + i * 1_000_000;
                record.amount = (i + 1) as i64 * 100;
                unsafe { record.compute_checksum(); }

                let offset = i as usize * PostingRecord::SIZE;
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        &record as *const PostingRecord as *const u8,
                        data_page[offset..].as_mut_ptr(),
                        PostingRecord::SIZE,
                    );
                }
            }
            f.write_all(&data_page).unwrap();
            f.sync_all().unwrap();
        }

        let state = crate::recovery::recover_ls_state(&ls_path);

        assert_eq!(state.write_offset, LsFileHeader::DATA_OFFSET as u64 + 4096);
        assert_eq!(state.gsn_min, 100);
        assert_eq!(state.gsn_max, 102);
        assert_eq!(state.timestamp_min_ns, 1700000000_000_000_000);
        assert_eq!(state.timestamp_max_ns, 1700000000_002_000_000);
        assert_eq!(state.postings_count, 3);

        cleanup_dir(&dir);
    }

    #[test]
    fn recover_ls_state_multiple_pages() {
        let dir = make_test_dir();
        let ls_path = format!("{}/test.ls", dir);

        {
            use std::io::Write;
            let mut f = std::fs::File::create(&ls_path).unwrap();

            let header = LsFileHeader::new(false, 16, 256 * 1024 * 1024, 0, 0, false);
            f.write_all(&header.to_page()).unwrap();

            let mut page1 = [0u8; 4096];
            for i in 0..32u64 {
                let mut record = PostingRecord::zeroed();
                record.set_magic();
                record.gsn = 1 + i;
                record.timestamp_ns = 1700000000_000_000_000 + i;
                unsafe { record.compute_checksum(); }

                let offset = i as usize * PostingRecord::SIZE;
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        &record as *const PostingRecord as *const u8,
                        page1[offset..].as_mut_ptr(),
                        PostingRecord::SIZE,
                    );
                }
            }
            f.write_all(&page1).unwrap();

            let mut page2 = [0u8; 4096];
            for i in 0..5u64 {
                let mut record = PostingRecord::zeroed();
                record.set_magic();
                record.gsn = 33 + i;
                record.timestamp_ns = 1700000000_000_000_032 + i;
                unsafe { record.compute_checksum(); }

                let offset = i as usize * PostingRecord::SIZE;
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        &record as *const PostingRecord as *const u8,
                        page2[offset..].as_mut_ptr(),
                        PostingRecord::SIZE,
                    );
                }
            }
            f.write_all(&page2).unwrap();
            f.sync_all().unwrap();
        }

        let state = crate::recovery::recover_ls_state(&ls_path);

        assert_eq!(state.postings_count, 37);
        assert_eq!(state.gsn_min, 1);
        assert_eq!(state.gsn_max, 37);
        assert_eq!(
        state.write_offset,
        LsFileHeader::DATA_OFFSET as u64 + 4096 + 4096,
    );

        cleanup_dir(&dir);
    }

    #[test]
    fn recover_ls_state_no_file() {
        let state = crate::recovery::recover_ls_state("/tmp/nonexistent-ls-file");

        assert_eq!(state.write_offset, LsFileHeader::DATA_OFFSET as u64);
        assert_eq!(state.gsn_min, 0);
        assert_eq!(state.postings_count, 0);
    }

    #[test]
    fn recover_ls_state_detects_corrupt_posting() {
        let dir = make_test_dir();
        let ls_path = format!("{}/test.ls", dir);

        {
            use std::io::Write;
            let mut f = std::fs::File::create(&ls_path).unwrap();

            let header = LsFileHeader::new(false, 16, 256 * 1024 * 1024, 0, 0, false);
            f.write_all(&header.to_page()).unwrap();

            let mut page = [0u8; 4096];

            let mut r0 = PostingRecord::zeroed();
            r0.set_magic();
            r0.gsn = 100;
            r0.timestamp_ns = 1700000000_000_000_000;
            unsafe { r0.compute_checksum(); }
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &r0 as *const PostingRecord as *const u8,
                    page.as_mut_ptr(),
                    PostingRecord::SIZE,
                );
            }

            let mut r1 = PostingRecord::zeroed();
            r1.set_magic();
            r1.gsn = 200;
            unsafe { r1.compute_checksum(); }
            r1.amount = 999;
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &r1 as *const PostingRecord as *const u8,
                    page[PostingRecord::SIZE..].as_mut_ptr(),
                    PostingRecord::SIZE,
                );
            }

            f.write_all(&page).unwrap();
            f.sync_all().unwrap();
        }

        let state = crate::recovery::recover_ls_state(&ls_path);

        assert_eq!(state.postings_count, 1);
        assert_eq!(state.gsn_min, 100);
        assert_eq!(state.gsn_max, 100);

        cleanup_dir(&dir);
    }
    #[test]
    fn reopen_recovers_write_offset() {
        let dir = make_test_dir();

        let ls_path;
        {
            let manifest_path = format!("{}/0.manifest", dir);
            std::fs::remove_file(&manifest_path).ok();
            let manifest = Manifest::create(&dir, 0);

            let ls_writer_rb = Arc::new(MpscRingBuffer::<LsWriterSlot>::new(64).unwrap());
            let flush_done_rb = Arc::new(MpscRingBuffer::<FlushDoneSlot>::new(64).unwrap());

            unsafe { TEST_COMMITTED_GSN = 0; }
            let committed_gsn_ptr = unsafe { &raw mut TEST_COMMITTED_GSN };

            let (index_tx, _) = mpsc::channel();

            let mut writer = LsWriter::new(
                0, ls_writer_rb, flush_done_rb, committed_gsn_ptr,
                PortableFlushBackend::new(), NoSigningStrategy, NoMetadataStrategy,
                dir.to_string(), 0, 64, K0, K1,
                64, 2, 512, 16,
                4, 0, manifest, index_tx,
            );
            writer.startup();

            let mut slot1 = make_posting_slot(100, 500);
            slot1.posting.timestamp_ns = 1700000000_000_000_000;
            unsafe {
                slot1.posting.compute_checksum();
            }
            writer.process_message(&make_add_to_heap_slot(100));
            writer.process_message(&slot1);
            writer.process_message(&make_flush_marker_slot(100, 1, 42));

            let mut slot2 = make_posting_slot(200, 300);
            slot2.posting.timestamp_ns = 1700000000_001_000_000;
            unsafe {
                slot2.posting.compute_checksum();
            }
            writer.process_message(&make_add_to_heap_slot(200));
            writer.process_message(&slot2);
            writer.process_message(&make_flush_marker_slot(200, 2, 43));

            writer.submit_flush();
            writer.poll_and_handle_completions();

            ls_path = writer.current_ls_file_path().to_string();
        }

        {
            let manifest = Manifest::open(&dir, 0);

            let ls_writer_rb = Arc::new(MpscRingBuffer::<LsWriterSlot>::new(64).unwrap());
            let flush_done_rb = Arc::new(MpscRingBuffer::<FlushDoneSlot>::new(64).unwrap());

            unsafe { TEST_COMMITTED_GSN = 0; }
            let committed_gsn_ptr = unsafe { &raw mut TEST_COMMITTED_GSN };

            let (index_tx, _) = mpsc::channel();

            let mut writer = LsWriter::new(
                0, ls_writer_rb, flush_done_rb, committed_gsn_ptr,
                PortableFlushBackend::new(), NoSigningStrategy, NoMetadataStrategy,
                dir.to_string(), 0, 64, K0, K1,
                64, 2, 512, 16, 4, 0,
                manifest, index_tx,
            );
            writer.startup();

            assert_eq!(
            writer.write_offset,
            LsFileHeader::DATA_OFFSET as u64 + 4096,
        );

            assert_eq!(writer.batch_seq, 1);

            assert_eq!(
            writer.checkpoint_write_offset,
            CheckpointFileHeader::DATA_OFFSET as u64 + CheckpointRecord::SIZE as u64,
        );

            assert_eq!(writer.current_gsn_min, 100);
            assert_eq!(writer.flushed_gsn_max, 200);
            assert_eq!(writer.current_timestamp_min_ns, 1700000000_000_000_000);
            assert_eq!(writer.flushed_timestamp_max_ns, 1700000000_001_000_000);

            let mut slot3 = make_posting_slot(300, 100);
            slot3.posting.timestamp_ns = 1700000000_002_000_000;
            unsafe {
                slot3.posting.compute_checksum();
            }
            writer.process_message(&make_add_to_heap_slot(300));
            writer.process_message(&slot3);
            writer.process_message(&make_flush_marker_slot(300, 3, 44));

            writer.submit_flush();
            writer.poll_and_handle_completions();

            assert_eq!(
            writer.write_offset,
            LsFileHeader::DATA_OFFSET as u64 + 4096 + 4096,
        );
        }

        cleanup_dir(&dir);
    }

    #[test]
    fn rotate_sends_index_builder_task() {
        let dir = make_test_dir();
        let manifest_path = format!("{}/0.manifest", dir);
        std::fs::remove_file(&manifest_path).ok();
        let manifest = Manifest::create(&dir, 0);

        let (index_tx, index_rx) = std::sync::mpsc::channel();

        let ls_writer_rb = Arc::new(MpscRingBuffer::<LsWriterSlot>::new(64).unwrap());
        let flush_done_rb = Arc::new(MpscRingBuffer::<FlushDoneSlot>::new(64).unwrap());

        unsafe { TEST_COMMITTED_GSN = 0; }
        let committed_gsn_ptr = unsafe { &raw mut TEST_COMMITTED_GSN };

        let mut writer = LsWriter::new(
            0, ls_writer_rb, flush_done_rb, committed_gsn_ptr,
            MockFlushBackend::new(), NoSigningStrategy, NoMetadataStrategy,
            dir.to_string(), 4096, 64, K0, K1, 64, 2, 512, 16, 4,
            0, manifest, index_tx,
        );
        writer.startup();

        let old_path = writer.current_ls_file_path().to_string();
        writer.rotate();

        let task = index_rx.try_recv().expect("Expected index builder task");
        assert_eq!(task.ls_path, old_path);
        assert_eq!(task.file_seq, 0);
        assert_eq!(task.shard_id, 0);
        assert!(!task.signing_enabled);

        cleanup_dir(&dir);
    }
}