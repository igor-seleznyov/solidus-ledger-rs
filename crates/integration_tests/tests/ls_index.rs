use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use ringbuf::mpsc_ring_buffer::MpscRingBuffer;
use storage::ls_writer::LsWriter;
use storage::ls_writer_slot::*;
use storage::flush_done_slot::FlushDoneSlot;
use storage::portable_flush_backend::PortableFlushBackend;
use storage::no_signing_strategy::NoSigningStrategy;
use storage::no_metadata_strategy::NoMetadataStrategy;
use storage::manifest::Manifest;
use storage::index_builder::{IndexBuilder, IndexBuilderTask, IndexBufferEntry};
use storage::index_reader::{lookup_account, query_timestamp_range, query_ordinal_range};
use storage::index_file_header::IndexFileHeader;
use storage::ordinal_index_entry::OrdinalIndexEntry;
use storage::timestamp_index_entry::TimestampIndexEntry;
use std::collections::HashMap;
use storage::index_builder::AccountMeta;
use storage::index_writer::write_index_files;


const K0: u64 = 0x0123456789ABCDEF;
const K1: u64 = 0xFEDCBA9876543210;

static mut TEST_COMMITTED_GSN: u64 = 0;

fn make_temp_dir(test_name: &str) -> String {
    let dir = format!(
        "/tmp/solidus-it-index-{}-{}",
        test_name,
        std::process::id()
    );
    std::fs::create_dir_all(&dir).expect("Failed to create temp dir");
    dir
}

fn cleanup(dir: &str) {
    std::fs::remove_dir_all(dir).ok();
}

fn make_writer(
    dir: &str,
    max_ls_file_size: usize,
    index_tx: mpsc::Sender<IndexBuilderTask>,
) -> LsWriter<PortableFlushBackend, NoSigningStrategy, NoMetadataStrategy> {
    let ls_writer_rb = Arc::new(
        MpscRingBuffer::<LsWriterSlot>::new(64).unwrap()
    );
    let flush_done_rb = Arc::new(
        MpscRingBuffer::<FlushDoneSlot>::new(64).unwrap()
    );

    unsafe { TEST_COMMITTED_GSN = 0; }
    let committed_gsn_ptr = unsafe { &raw mut TEST_COMMITTED_GSN };

    let manifest_path = format!("{}/0.manifest", dir);
    std::fs::remove_file(&manifest_path).ok();
    let manifest = Manifest::create(dir, 0);

    let mut writer = LsWriter::new(
        0,
        ls_writer_rb,
        flush_done_rb,
        committed_gsn_ptr,
        PortableFlushBackend::new(),
        NoSigningStrategy,
        NoMetadataStrategy,
        dir.to_string(),
        max_ls_file_size,
        64, K0, K1,
        64, 2, 512,
        16,
        4,
        0,
        manifest,
        false,
        index_tx,
    );
    writer.startup();
    writer
}

fn make_posting_slot(
    gsn: u64,
    amount: i64,
    account_hi: u64,
    account_lo: u64,
    ordinal: u64,
    timestamp_ns: u64,
) -> LsWriterSlot {
    let mut slot = LsWriterSlot::zeroed();
    slot.msg_type = LS_MSG_POSTING;
    slot.gsn = gsn;
    slot.posting.set_magic();
    slot.posting.gsn = gsn;
    slot.posting.amount = amount;
    slot.posting.transfer_id_hi = 0;
    slot.posting.transfer_id_lo = gsn;
    slot.posting.account_id_hi = account_hi;
    slot.posting.account_id_lo = account_lo;
    slot.posting.ordinal = ordinal;
    slot.posting.timestamp_ns = timestamp_ns;
    unsafe { slot.posting.compute_checksum(); }
    slot
}

fn make_add_to_heap_slot(gsn: u64) -> LsWriterSlot {
    let mut slot = LsWriterSlot::zeroed();
    slot.msg_type = LS_MSG_ADD_TO_HEAP;
    slot.gsn = gsn;
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

fn write_flush_rotate_build(
    writer: &mut LsWriter<PortableFlushBackend, NoSigningStrategy, NoMetadataStrategy>,
    index_rx: &mpsc::Receiver<IndexBuilderTask>,
    postings: &Vec<LsWriterSlot>,
) -> String {
    for slot in postings {
        if slot.msg_type == LS_MSG_POSTING {
            writer.process_message(&make_add_to_heap_slot(slot.gsn));
        }
        writer.process_message(slot);
        if slot.msg_type == LS_MSG_POSTING {
            writer.process_message(&make_flush_marker_slot(slot.gsn, slot.gsn, 0));
        }
    }

    writer.submit_flush();
    writer.poll_and_handle_completions();

    let ls_path = writer.current_ls_file_path().to_string();

    writer.rotate();

    let task = index_rx.recv().expect("Expected index builder task");

    let builder_rx_dummy = mpsc::channel::<IndexBuilderTask>();
    let builder = IndexBuilder::new(0, builder_rx_dummy.1, None);
    {
        if !task.entries.is_empty() {
            let total_count = task.entries.len();

            let mut counts: HashMap<(u64, u64), u32> = HashMap::new();
            for entry in &task.entries {
                *counts.entry((entry.account_id_hi, entry.account_id_lo)).or_insert(0) += 1;
            }

            let (idx_records, sorted_keys, mut accounts) = {
                let mut sorted_keys: Vec<(u64, u64)> = counts.keys().copied().collect();
                sorted_keys.sort_unstable();

                let mut account_map: HashMap<(u64, u64), AccountMeta> = HashMap::with_capacity(counts.len());
                let mut idx_records = Vec::with_capacity(sorted_keys.len());

                let mut buffer_offset: usize = 0;
                let mut ordinal_file_offset: u64 = IndexFileHeader::SIZE as u64;
                let mut timestamp_file_offset: u64 = IndexFileHeader::SIZE as u64;

                for &(hi, lo) in &sorted_keys {
                    let count = counts[&(hi, lo)];
                    account_map.insert((hi, lo), AccountMeta {
                        start: buffer_offset,
                        count,
                        cursor: 0,
                    });
                    idx_records.push(storage::account_index_record::AccountIndexRecord {
                        account_id_hi: hi,
                        account_id_lo: lo,
                        ordinal_file_offset,
                        timestamp_file_offset,
                        records_count: count,
                        _pad: 0,
                    });
                    buffer_offset += count as usize;
                    ordinal_file_offset += count as u64 * OrdinalIndexEntry::SIZE as u64;
                    timestamp_file_offset += count as u64 * TimestampIndexEntry::SIZE as u64;
                }

                (idx_records, sorted_keys, account_map)
            };

            let mut buffer: Vec<IndexBufferEntry> = Vec::with_capacity(total_count);
            buffer.resize(total_count, IndexBufferEntry {
                account_id_hi: 0, account_id_lo: 0,
                ordinal: 0, timestamp_ns: 0, ls_offset: 0,
            });

            for entry in &task.entries {
                let key = (entry.account_id_hi, entry.account_id_lo);
                let meta = accounts.get_mut(&key).unwrap();
                let index = meta.start + meta.cursor as usize;
                buffer[index] = *entry;
                meta.cursor += 1;
            }

            write_index_files(
                &task.ls_path,
                task.file_seq,
                &idx_records,
                &sorted_keys,
                &mut buffer,
                &accounts,
            );
        }
    }

    ls_path
}


#[test]
fn rotation_creates_index_files() {
    let dir = make_temp_dir("creates-files");
    let (index_tx, index_rx) = mpsc::channel();
    let mut writer = make_writer(&dir, 1024 * 1024, index_tx);

    let postings = vec![
        make_posting_slot(100, 500, 0, 1, 0, 1_000_000_000),
    ];

    let ls_path = write_flush_rotate_build(&mut writer, &index_rx, &postings);

    assert!(std::path::Path::new(&format!("{}.posting-accounts", ls_path)).exists());
    assert!(std::path::Path::new(&format!("{}.ordinal", ls_path)).exists());
    assert!(std::path::Path::new(&format!("{}.timestamp", ls_path)).exists());

    cleanup(&dir);
}

#[test]
fn lookup_account_after_rotation() {
    let dir = make_temp_dir("lookup-account");
    let (index_tx, index_rx) = mpsc::channel();
    let mut writer = make_writer(&dir, 1024 * 1024, index_tx);

    let postings = vec![
        make_posting_slot(100, 500, 0, 1, 0, 1_000_000_000),
        make_posting_slot(101, 300, 0, 2, 0, 1_000_001_000),
        make_posting_slot(102, 200, 0, 3, 0, 1_000_002_000),
    ];

    let ls_path = write_flush_rotate_build(&mut writer, &index_rx, &postings);

    let idx_path = format!("{}.posting-accounts", ls_path);

    let result = lookup_account(&idx_path, 0, 1);
    assert!(result.is_some());
    assert_eq!(result.unwrap().records_count, 1);

    let result = lookup_account(&idx_path, 0, 2);
    assert!(result.is_some());
    assert_eq!(result.unwrap().records_count, 1);

    let result = lookup_account(&idx_path, 0, 3);
    assert!(result.is_some());
    assert_eq!(result.unwrap().records_count, 1);

    assert!(lookup_account(&idx_path, 0, 99).is_none());

    cleanup(&dir);
}

#[test]
fn lookup_100_accounts() {
    let dir = make_temp_dir("100-accounts");
    let (index_tx, index_rx) = mpsc::channel();
    let mut writer = make_writer(&dir, 1024 * 1024, index_tx);

    let mut postings = Vec::new();
    for i in 0..100u64 {
        postings.push(make_posting_slot(
            100 + i,
            (i + 1) as i64 * 100,
            0,
            i + 1,
            0,
            1_000_000_000 + i * 1000,
        ));
    }

    let ls_path = write_flush_rotate_build(&mut writer, &index_rx, &postings);
    let idx_path = format!("{}.posting-accounts", ls_path);

    for i in 0..100u64 {
        let result = lookup_account(&idx_path, 0, i + 1);
        assert!(result.is_some(), "Account {} not found", i + 1);
        assert_eq!(result.unwrap().records_count, 1);
    }

    assert!(lookup_account(&idx_path, 0, 0).is_none());
    assert!(lookup_account(&idx_path, 0, 101).is_none());

    cleanup(&dir);
}

#[test]
fn timestamp_range_query_after_rotation() {
    let dir = make_temp_dir("ts-range");
    let (index_tx, index_rx) = mpsc::channel();
    let mut writer = make_writer(&dir, 1024 * 1024, index_tx);

    let postings = vec![
        make_posting_slot(100, 100, 0, 1, 0, 1000),
        make_posting_slot(101, 200, 0, 1, 1, 2000),
        make_posting_slot(102, 300, 0, 1, 2, 3000),
        make_posting_slot(103, 400, 0, 1, 3, 4000),
        make_posting_slot(104, 500, 0, 1, 4, 5000),
    ];

    let ls_path = write_flush_rotate_build(&mut writer, &index_rx, &postings);

    let idx_path = format!("{}.posting-accounts", ls_path);
    let ts_path = format!("{}.timestamp", ls_path);

    let result = lookup_account(&idx_path, 0, 1).unwrap();
    assert_eq!(result.records_count, 5);

    let offsets = query_timestamp_range(
        &ts_path, result.timestamp_file_offset, result.records_count, 1000, 5000,
    );
    assert_eq!(offsets.len(), 5);

    let offsets = query_timestamp_range(
        &ts_path, result.timestamp_file_offset, result.records_count, 2000, 4000,
    );
    assert_eq!(offsets.len(), 3);

    let offsets = query_timestamp_range(
        &ts_path, result.timestamp_file_offset, result.records_count, 6000, 9000,
    );
    assert!(offsets.is_empty());

    cleanup(&dir);
}

#[test]
fn ordinal_range_query_after_rotation() {
    let dir = make_temp_dir("ord-range");
    let (index_tx, index_rx) = mpsc::channel();
    let mut writer = make_writer(&dir, 1024 * 1024, index_tx);

    let postings = vec![
        make_posting_slot(100, 100, 0, 1, 0, 1000),
        make_posting_slot(101, 200, 0, 1, 1, 2000),
        make_posting_slot(102, 300, 0, 1, 2, 3000),
        make_posting_slot(103, 400, 0, 1, 3, 4000),
        make_posting_slot(104, 500, 0, 1, 4, 5000),
    ];

    let ls_path = write_flush_rotate_build(&mut writer, &index_rx, &postings);

    let idx_path = format!("{}.posting-accounts", ls_path);
    let ord_path = format!("{}.ordinal", ls_path);

    let result = lookup_account(&idx_path, 0, 1).unwrap();

    let offsets = query_ordinal_range(
        &ord_path, result.ordinal_file_offset, result.records_count, 1, 3,
    );
    assert_eq!(offsets.len(), 3);

    let offsets = query_ordinal_range(
        &ord_path, result.ordinal_file_offset, result.records_count, 0, 4,
    );
    assert_eq!(offsets.len(), 5);

    cleanup(&dir);
}

#[test]
fn multiple_accounts_with_many_postings() {
    let dir = make_temp_dir("multi-many");
    let (index_tx, index_rx) = mpsc::channel();
    let mut writer = make_writer(&dir, 1024 * 1024, index_tx);

    let mut postings = Vec::new();
    let mut gsn = 100u64;

    for i in 0..100u64 {
        postings.push(make_posting_slot(gsn, i as i64, 0, 1, i, 1_000_000 + i * 1000));
        gsn += 1;
    }
    for i in 0..50u64 {
        postings.push(make_posting_slot(gsn, i as i64, 0, 2, i, 2_000_000 + i * 1000));
        gsn += 1;
    }
    for i in 0..30u64 {
        postings.push(make_posting_slot(gsn, i as i64, 0, 3, i, 3_000_000 + i * 1000));
        gsn += 1;
    }

    let ls_path = write_flush_rotate_build(&mut writer, &index_rx, &postings);

    let idx_path = format!("{}.posting-accounts", ls_path);
    let ts_path = format!("{}.timestamp", ls_path);
    let ord_path = format!("{}.ordinal", ls_path);

    let r1 = lookup_account(&idx_path, 0, 1).unwrap();
    assert_eq!(r1.records_count, 100);

    let r2 = lookup_account(&idx_path, 0, 2).unwrap();
    assert_eq!(r2.records_count, 50);

    let r3 = lookup_account(&idx_path, 0, 3).unwrap();
    assert_eq!(r3.records_count, 30);

    let offsets = query_timestamp_range(
        &ts_path, r1.timestamp_file_offset, r1.records_count,
        1_000_000, 1_009_000,
    );
    assert_eq!(offsets.len(), 10);

    let offsets = query_ordinal_range(
        &ord_path, r2.ordinal_file_offset, r2.records_count,
        20, 39,
    );
    assert_eq!(offsets.len(), 20);

    let offsets = query_ordinal_range(
        &ord_path, r3.ordinal_file_offset, r3.records_count,
        0, 29,
    );
    assert_eq!(offsets.len(), 30);

    cleanup(&dir);
}

#[test]
fn account_not_found_after_rotation() {
    let dir = make_temp_dir("not-found");
    let (index_tx, index_rx) = mpsc::channel();
    let mut writer = make_writer(&dir, 1024 * 1024, index_tx);

    let postings = vec![
        make_posting_slot(100, 500, 0, 1, 0, 1000),
    ];

    let ls_path = write_flush_rotate_build(&mut writer, &index_rx, &postings);
    let idx_path = format!("{}.posting-accounts", ls_path);

    assert!(lookup_account(&idx_path, 0, 99).is_none());
    assert!(lookup_account(&idx_path, 1, 1).is_none());

    cleanup(&dir);
}

#[test]
fn index_builder_thread_builds_files() {
    let dir = make_temp_dir("real-thread");
    let (index_tx, index_rx) = mpsc::channel();
    let mut writer = make_writer(&dir, 1024 * 1024, index_tx);

    let builder_handle = thread::spawn(move || {
        let builder = IndexBuilder::new(0, index_rx, None);
        builder.run();
    });

    let postings = vec![
        make_posting_slot(100, 500, 0, 1, 0, 1_000_000_000),
        make_posting_slot(101, 300, 0, 2, 0, 1_000_001_000),
    ];

    for slot in &postings {
        writer.process_message(&make_add_to_heap_slot(slot.gsn));
        writer.process_message(slot);
        writer.process_message(&make_flush_marker_slot(slot.gsn, slot.gsn, 0));
    }

    writer.submit_flush();
    writer.poll_and_handle_completions();

    let ls_path = writer.current_ls_file_path().to_string();
    writer.rotate();

    drop(writer);

    builder_handle.join().expect("Index Builder thread panicked");

    assert!(
        std::path::Path::new(&format!("{}.posting-accounts", ls_path)).exists(),
        ".posting-accounts not found"
    );
    assert!(
        std::path::Path::new(&format!("{}.ordinal", ls_path)).exists(),
        ".ordinal not found"
    );
    assert!(
        std::path::Path::new(&format!("{}.timestamp", ls_path)).exists(),
        ".timestamp not found"
    );

    let idx_path = format!("{}.posting-accounts", ls_path);
    let r1 = lookup_account(&idx_path, 0, 1);
    assert!(r1.is_some(), "Account 1 not found after thread build");

    let r2 = lookup_account(&idx_path, 0, 2);
    assert!(r2.is_some(), "Account 2 not found after thread build");

    cleanup(&dir);
}