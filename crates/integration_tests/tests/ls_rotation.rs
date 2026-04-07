use std::sync::{mpsc, Arc};
use ed25519_dalek::SigningKey;
use common::make_test_dir::make_test_dir;
use ringbuf::mpsc_ring_buffer::MpscRingBuffer;
use storage::ls_writer::LsWriter;
use storage::ls_writer_slot::*;
use storage::flush_done_slot::FlushDoneSlot;
use storage::portable_flush_backend::PortableFlushBackend;
use storage::no_signing_strategy::NoSigningStrategy;
use storage::no_metadata_strategy::NoMetadataStrategy;
use storage::ed25519_signing_strategy::Ed25519SigningStrategy;
use storage::posting_metadata_strategy::PostingMetadataStrategy;
use storage::signing_state::SigningState;
use storage::ls_file_header::LsFileHeader;
use storage::ls_sign_file_header::LsSignFileHeader;
use storage::ls_meta_file_header::LsMetaFileHeader;
use storage::checkpoint_file_header::CheckpointFileHeader;
use storage::manifest::Manifest;

const K0: u64 = 0x0123456789ABCDEF;
const K1: u64 = 0xFEDCBA9876543210;

static mut TEST_COMMITTED_GSN: u64 = 0;


fn make_temp_dir(test_name: &str) -> String {
    let dir = make_test_dir();
    dir
}

fn cleanup_temp_dir(dir: &str) {
    std::fs::remove_dir_all(dir).ok();
}

fn make_writer(
    dir: &str,
    max_ls_file_size: usize,
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

    let (index_tx, _) = mpsc::channel();

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
        index_tx,
    );
    writer.startup();
    writer
}

fn make_writer_with_signing(
    dir: &str,
    max_ls_file_size: usize,
) -> LsWriter<PortableFlushBackend, Ed25519SigningStrategy, NoMetadataStrategy> {
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

    let manifest_path = format!("{}/0.manifest", dir);
    std::fs::remove_file(&manifest_path).ok();
    let manifest = Manifest::create(dir, 0);

    let (index_tx, _) = mpsc::channel();

    let mut writer = LsWriter::new(
        0,
        ls_writer_rb,
        flush_done_rb,
        committed_gsn_ptr,
        PortableFlushBackend::new(),
        signing,
        NoMetadataStrategy,
        dir.to_string(),
        max_ls_file_size,
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

fn make_writer_with_metadata(
    dir: &str,
    max_ls_file_size: usize,
    record_size: usize,
) -> LsWriter<PortableFlushBackend, NoSigningStrategy, PostingMetadataStrategy> {
    let ls_writer_rb = Arc::new(
        MpscRingBuffer::<LsWriterSlot>::new(64).unwrap()
    );
    let flush_done_rb = Arc::new(
        MpscRingBuffer::<FlushDoneSlot>::new(64).unwrap()
    );

    unsafe { TEST_COMMITTED_GSN = 0; }
    let committed_gsn_ptr = unsafe { &raw mut TEST_COMMITTED_GSN };

    let metadata = PostingMetadataStrategy::new(record_size, 512);

    let manifest_path = format!("{}/0.manifest", dir);
    std::fs::remove_file(&manifest_path).ok();
    let manifest = Manifest::create(dir, 0);

    let (index_tx, _) = mpsc::channel();

    let mut writer = LsWriter::new(
        0,
        ls_writer_rb,
        flush_done_rb,
        committed_gsn_ptr,
        PortableFlushBackend::new(),
        NoSigningStrategy,
        metadata,
        dir.to_string(),
        max_ls_file_size,
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

fn make_add_to_heap_slot(gsn: u64) -> LsWriterSlot {
    let mut slot = LsWriterSlot::zeroed();
    slot.msg_type = LS_MSG_ADD_TO_HEAP;
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


#[test]
fn rotation_creates_new_files_on_disk() {
    let dir = make_temp_dir("new-files");
    let mut writer = make_writer(&dir, 4096);

    let first_path = writer.current_ls_file_path().to_string();
    assert!(std::path::Path::new(&first_path).exists());

    let first_checkpoint = format!("{}.checkpoint", first_path);
    assert!(std::path::Path::new(&first_checkpoint).exists());

    writer.rotate();

    let second_path = writer.current_ls_file_path().to_string();
    assert!(std::path::Path::new(&second_path).exists());

    let second_checkpoint = format!("{}.checkpoint", second_path);
    assert!(std::path::Path::new(&second_checkpoint).exists());

    assert!(std::path::Path::new(&first_path).exists());
    assert!(std::path::Path::new(&second_path).exists());
    assert_ne!(first_path, second_path);

    cleanup_temp_dir(&dir);
}

#[test]
fn rotation_ls_header_correct() {
    let dir = make_temp_dir("ls-header");
    let mut writer = make_writer(&dir, 4096);

    writer.rotate();

    let ls_bytes = std::fs::read(writer.current_ls_file_path())
        .expect("Failed to read new LS file");

    assert!(ls_bytes.len() >= LsFileHeader::SIZE);

    let header = unsafe { LsFileHeader::from_bytes(&ls_bytes) };
    assert_eq!(header.magic, storage::ls_file_header::LS_FILE_MAGIC);
    assert_eq!(header.file_seq, 1);
    assert_eq!(header.signing_enabled, 0);
    assert_eq!(header.metadata_enabled, 0);
    assert!(unsafe { header.verify_checksum() });

    cleanup_temp_dir(&dir);
}

#[test]
fn rotation_checkpoint_header_linked_file_seq() {
    let dir = make_temp_dir("checkpoint-seq");
    let mut writer = make_writer(&dir, 4096);

    let first_checkpoint_path = format!("{}.checkpoint", writer.current_ls_file_path());
    let bytes0 = std::fs::read(&first_checkpoint_path)
        .expect("Failed to read first checkpoint");
    let header0 = unsafe { CheckpointFileHeader::from_bytes(&bytes0) };
    assert_eq!(header0.linked_ls_file_seq, 0);
    assert!(unsafe { header0.verify_checksum() });

    writer.rotate();

    let second_checkpoint_path = format!("{}.checkpoint", writer.current_ls_file_path());
    let bytes1 = std::fs::read(&second_checkpoint_path)
        .expect("Failed to read second checkpoint");
    let header1 = unsafe { CheckpointFileHeader::from_bytes(&bytes1) };
    assert_eq!(header1.linked_ls_file_seq, 1);
    assert!(unsafe { header1.verify_checksum() });

    cleanup_temp_dir(&dir);
}

#[test]
fn rotation_data_written_to_new_file() {
    let dir = make_temp_dir("data-new-file");
    let mut writer = make_writer(&dir, 1024 * 1024);

    writer.process_message(&make_add_to_heap_slot(100));
    writer.process_message(&make_posting_slot(100, 500));
    writer.process_message(&make_flush_marker_slot(100, 1, 42));
    writer.submit_flush();
    writer.poll_and_handle_completions();

    let first_path = writer.current_ls_file_path().to_string();
    let first_file_size = std::fs::metadata(&first_path)
        .expect("Failed to stat first LS file").len();
    assert!(first_file_size >= 8192);

    writer.rotate();

    let second_path = writer.current_ls_file_path().to_string();

    writer.process_message(&make_add_to_heap_slot(200));
    writer.process_message(&make_posting_slot(200, 300));
    writer.process_message(&make_flush_marker_slot(200, 2, 43));
    writer.submit_flush();
    writer.poll_and_handle_completions();

    let second_file_size = std::fs::metadata(&second_path)
        .expect("Failed to stat second LS file").len();
    assert!(second_file_size >= 8192);

    let first_file_size_after = std::fs::metadata(&first_path)
        .expect("Failed to re-stat first LS file").len();
    assert_eq!(first_file_size, first_file_size_after);

    cleanup_temp_dir(&dir);
}

#[test]
fn auto_rotation_on_max_file_size() {
    let dir = make_temp_dir("auto-rotate");
    let mut writer = make_writer(&dir, 8192);

    let first_path = writer.current_ls_file_path().to_string();

    writer.process_message(&make_add_to_heap_slot(100));
    writer.process_message(&make_posting_slot(100, 500));
    writer.process_message(&make_flush_marker_slot(100, 1, 42));
    writer.submit_flush();
    writer.poll_and_handle_completions();

    assert_ne!(writer.current_ls_file_path(), first_path);

    assert!(std::path::Path::new(&first_path).exists());
    assert!(std::path::Path::new(writer.current_ls_file_path()).exists());

    let new_ls_bytes = std::fs::read(writer.current_ls_file_path())
        .expect("Failed to read new LS file");
    let new_header = unsafe { LsFileHeader::from_bytes(&new_ls_bytes) };
    assert_eq!(new_header.file_seq, 1);

    cleanup_temp_dir(&dir);
}

#[test]
fn rotation_sign_files_created() {
    let dir = make_temp_dir("sign-files");
    let mut writer = make_writer_with_signing(&dir, 4096);

    let first_ls = writer.current_ls_file_path().to_string();
    let first_sign = format!("{}.sign", first_ls);
    assert!(std::path::Path::new(&first_sign).exists());

    writer.rotate();

    let second_ls = writer.current_ls_file_path().to_string();
    let second_sign = format!("{}.sign", second_ls);
    assert!(std::path::Path::new(&second_sign).exists());

    assert!(std::path::Path::new(&first_sign).exists());
    assert!(std::path::Path::new(&second_sign).exists());

    cleanup_temp_dir(&dir);
}

#[test]
fn rotation_sign_cross_file_chain() {
    let dir = make_temp_dir("sign-chain");
    let mut writer = make_writer_with_signing(&dir, 1024 * 1024);

    writer.process_message(&make_add_to_heap_slot(100));
    writer.process_message(&make_posting_slot(100, 500));
    writer.process_message(&make_flush_marker_slot(100, 1, 42));
    writer.submit_flush();
    writer.poll_and_handle_completions();

    let chain_hash_before_rotation = *writer.signing_chain_hash()
        .expect("signing enabled — chain_hash must be Some");

    writer.rotate();

    let new_sign_path = format!("{}.sign", writer.current_ls_file_path());
    let sign_bytes = std::fs::read(&new_sign_path)
        .expect("Failed to read new sign file");

    let sign_header = unsafe { LsSignFileHeader::from_bytes(&sign_bytes) };
    assert_eq!(sign_header.magic, storage::ls_sign_file_header::LS_SIGN_FILE_MAGIC);
    assert_eq!(sign_header.linked_ls_file_seq, 1);
    assert!(unsafe { sign_header.verify_checksum() });

    assert_eq!(sign_header.genesis_hash, chain_hash_before_rotation);

    cleanup_temp_dir(&dir);
}

#[test]
fn rotation_meta_files_created() {
    let dir = make_temp_dir("meta-files");
    let mut writer = make_writer_with_metadata(&dir, 4096, 256);

    let first_ls = writer.current_ls_file_path().to_string();
    let first_meta = format!("{}.posting-metadata", first_ls);
    assert!(std::path::Path::new(&first_meta).exists());

    writer.rotate();

    let second_ls = writer.current_ls_file_path().to_string();
    let second_meta = format!("{}.posting-metadata", second_ls);
    assert!(std::path::Path::new(&second_meta).exists());

    assert!(std::path::Path::new(&first_meta).exists());
    assert!(std::path::Path::new(&second_meta).exists());

    cleanup_temp_dir(&dir);
}

#[test]
fn rotation_meta_header_linked_file_seq() {
    let dir = make_temp_dir("meta-seq");
    let mut writer = make_writer_with_metadata(&dir, 4096, 256);

    writer.rotate();

    let meta_path = format!("{}.posting-metadata", writer.current_ls_file_path());
    let meta_bytes = std::fs::read(&meta_path)
        .expect("Failed to read new meta file");

    assert!(meta_bytes.len() >= LsMetaFileHeader::SIZE);

    let linked_seq = u64::from_le_bytes(
        meta_bytes[40..48].try_into().unwrap()
    );
    assert_eq!(linked_seq, 1);

    cleanup_temp_dir(&dir);
}

#[test]
fn multiple_rotations_handle_reuse() {
    let dir = make_temp_dir("multi-rotate");
    let mut writer = make_writer(&dir, 4096);

    for _ in 0..4 {
        let path = writer.current_ls_file_path().to_string();
        assert!(std::path::Path::new(&path).exists());
        writer.rotate();
    }

    assert!(std::path::Path::new(writer.current_ls_file_path()).exists());

    let ls_bytes = std::fs::read(writer.current_ls_file_path())
        .expect("Failed to read LS file");
    let header = unsafe { LsFileHeader::from_bytes(&ls_bytes) };
    assert_eq!(header.file_seq, 4);

    cleanup_temp_dir(&dir);
}

#[test]
fn multiple_rotations_with_signing_handle_reuse() {
    let dir = make_temp_dir("multi-sign-rotate");
    let mut writer = make_writer_with_signing(&dir, 4096);

    for _ in 0..3 {
        writer.rotate();
    }

    assert!(std::path::Path::new(writer.current_ls_file_path()).exists());

    let sign_path = format!("{}.sign", writer.current_ls_file_path());
    assert!(std::path::Path::new(&sign_path).exists());

    let ls_bytes = std::fs::read(writer.current_ls_file_path())
        .expect("Failed to read LS file");
    let header = unsafe { LsFileHeader::from_bytes(&ls_bytes) };
    assert_eq!(header.file_seq, 3);

    cleanup_temp_dir(&dir);
}