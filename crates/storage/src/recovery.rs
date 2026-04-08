use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use crate::checkpoint_record::CheckpointRecord;
use crate::checkpoint_file_header::{CheckpointFileHeader, CHECKPOINT_FILE_MAGIC};
use crate::ls_file_header::LsFileHeader;
use pipeline::posting_record::{PostingRecord, POSTING_RECORD_MAGIC};
use crate::consts::FILE_PAGE_SIZE;
use crate::posting_scan_visitor::scan_ls_postings;
use crate::recovery_visitor::RecoveryVisitor;

pub struct RecoveredLsState {
    pub write_offset: u64,
    pub gsn_min: u64,
    pub gsn_max: u64,
    pub timestamp_min_ns: u64,
    pub timestamp_max_ns: u64,
    pub postings_count: u64,
}

pub fn recover_checkpoint_state(checkpoint_path: &str) -> (u64, u32) {
    let mut file = match File::open(&checkpoint_path) {
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

        let record = unsafe {
            CheckpointRecord::from_bytes(&buf)
        };
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

pub fn recover_ls_state(ls_path: &str) -> RecoveredLsState {
    let mut visitor = RecoveryVisitor::new();
    let write_offset = scan_ls_postings(ls_path, &mut visitor);
    
    RecoveredLsState {
        write_offset,
        gsn_min: visitor.gsn_min,
        gsn_max: visitor.gsn_max,
        timestamp_min_ns: visitor.timestamp_min_ns,
        timestamp_max_ns: visitor.timestamp_max_ns,
        postings_count: visitor.postings_count,
    }
}