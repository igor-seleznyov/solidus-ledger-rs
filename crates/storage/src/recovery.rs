use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use crate::checkpoint_record::CheckpointRecord;
use crate::checkpoint_file_header::{CheckpointFileHeader, CHECKPOINT_FILE_MAGIC};
use crate::ls_file_header::LsFileHeader;
use pipeline::posting_record::{PostingRecord, POSTING_RECORD_MAGIC};
use crate::consts::LS_FILE_PAGE_SIZE;

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
    let mut file = match File::open(ls_path) {
        Ok(file) => file,
        Err(_) => {
            return RecoveredLsState {
                write_offset: LsFileHeader::DATA_OFFSET as u64,
                gsn_min: 0,
                gsn_max: 0,
                timestamp_min_ns: 0,
                timestamp_max_ns: 0,
                postings_count: 0,
            };
        }
    };

    let page_size: usize = LS_FILE_PAGE_SIZE;
    let mut page_buf = [0u8; LS_FILE_PAGE_SIZE];
    let mut offset = LsFileHeader::DATA_OFFSET as u64;
    let mut write_offset = offset;

    let mut gsn_min: u64 = 0;
    let mut gsn_max: u64 = 0;
    let mut timestamp_min_ns: u64 = 0;
    let mut timestamp_max_ns: u64 = 0;
    let mut postings_count: u64 = 0;

    loop {
        if file.seek(SeekFrom::Start(offset)).is_err() {
            break;
        }

        let bytes_read = match file.read(&mut page_buf) {
            Ok(read_size) => read_size,
            Err(_) => break
        };

        if bytes_read < PostingRecord::SIZE {
            break;
        }

        let mut valid_in_page = 0u64;

        let max_record = bytes_read / PostingRecord::SIZE;
        for i in 0..max_record {
            let record_start = i * PostingRecord::SIZE;
            let record_end = record_start + PostingRecord::SIZE;
            if record_end > bytes_read {
                break;
            }

            let magic = u64::from_le_bytes(
                page_buf[record_start..record_start + 8]
                    .try_into()
                    .unwrap()
            );
            if magic != pipeline::posting_record::POSTING_RECORD_MAGIC {
                break;
            }

            let mut record = PostingRecord::zeroed();
            unsafe {
                std::ptr::copy_nonoverlapping(
                    page_buf[record_start..].as_ptr(),
                    &mut record as *mut PostingRecord as *mut u8,
                    PostingRecord::SIZE,
                );
            }

            if !unsafe { record.verify_checksum() } {
                break;
            }

            if gsn_min == 0 {
                gsn_min = record.gsn;
                timestamp_min_ns = record.timestamp_ns;
            }
            gsn_max = record.gsn;
            timestamp_max_ns = record.timestamp_ns;
            valid_in_page += 1;
            postings_count += 1;
        }

        if valid_in_page == 0 {
            break;
        }

        let data_end = offset + valid_in_page * PostingRecord::SIZE as u64;
        write_offset = (data_end + page_size as u64 - 1) & !(page_size as u64 - 1);
        offset = write_offset;
    }

    RecoveredLsState {
        write_offset,
        gsn_min,
        gsn_max,
        timestamp_min_ns,
        timestamp_max_ns,
        postings_count,
    }
}