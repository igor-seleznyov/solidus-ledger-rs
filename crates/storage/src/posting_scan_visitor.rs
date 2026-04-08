use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use pipeline::posting_record::{PostingRecord, POSTING_RECORD_MAGIC};
use crate::consts::FILE_PAGE_SIZE;
use crate::ls_file_header::LsFileHeader;

pub trait PostingScanVisitor {
    fn on_posting(&mut self, ls_offset: u64, record: &PostingRecord);
}

pub fn scan_ls_postings(ls_path: &str, visitor: &mut impl PostingScanVisitor) -> u64 {
    let mut file = match File::open(ls_path) {
        Ok(file) => file,
        Err(_) => {
            return LsFileHeader::DATA_OFFSET as u64;
        }
    };

    let page_size: usize = FILE_PAGE_SIZE;
    let mut page_buf = [0u8; FILE_PAGE_SIZE];
    let mut offset = LsFileHeader::DATA_OFFSET as u64;
    let mut write_offset = offset;

    loop {
        if file.seek(SeekFrom::Start(offset)).is_err() {
            break;
        }

        let bytes_read = match file.read(&mut page_buf) {
            Ok(0) => break,
            Ok(read_size) => read_size,
            Err(_) => break
        };

        if bytes_read < PostingRecord::SIZE {
            break;
        }

        let mut valid_in_page = 0u64;

        let max_records = bytes_read / PostingRecord::SIZE;
        for i in 0..max_records {
            let record_start = i * PostingRecord::SIZE;

            let magic = u64::from_le_bytes(
                page_buf[record_start..record_start + 8]
                    .try_into()
                    .unwrap()
            );
            if magic != POSTING_RECORD_MAGIC {
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

            let posting_offset = offset + record_start as u64;
            visitor.on_posting(posting_offset, &record);
            valid_in_page += 1;
        }

        if valid_in_page == 0 {
            break;
        }

        let data_end = offset + valid_in_page * PostingRecord::SIZE as u64;
        write_offset = (data_end + page_size as u64 - 1) & !(page_size as u64 - 1);
        offset = write_offset;
    }

    write_offset
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipeline::posting_record::PostingRecord;
    use crate::ls_file_header::LsFileHeader;

    fn make_test_dir(name: &str) -> String {
        let dir = format!(
            "/tmp/solidus-test-recovery-{}-{}",
            name,
            std::process::id()
        );
        std::fs::create_dir_all(&dir).expect("Failed to create temp dir");
        dir
    }

    fn cleanup(dir: &str) {
        std::fs::remove_dir_all(dir).ok();
    }

    struct CountingVisitor {
        count: u64,
        offsets: Vec<u64>,
    }

    impl CountingVisitor {
        fn new() -> Self {
            Self { count: 0, offsets: Vec::new() }
        }
    }

    impl PostingScanVisitor for CountingVisitor {
        fn on_posting(&mut self, ls_offset: u64, _record: &PostingRecord) {
            self.count += 1;
            self.offsets.push(ls_offset);
        }
    }

    #[test]
    fn scan_ls_postings_empty_file() {
        let dir = make_test_dir("scan-empty");
        let ls_path = format!("{}/test.ls", dir);

        {
            use std::io::Write;
            let mut f = std::fs::File::create(&ls_path).unwrap();
            let header = LsFileHeader::new(false, 16, 256 * 1024 * 1024, 0, 0, false);
            f.write_all(&header.to_page()).unwrap();
            f.sync_all().unwrap();
        }

        let mut visitor = CountingVisitor::new();
        let write_offset = scan_ls_postings(&ls_path, &mut visitor);

        assert_eq!(write_offset, LsFileHeader::DATA_OFFSET as u64);
        assert_eq!(visitor.count, 0);

        cleanup(&dir);
    }

    #[test]
    fn scan_ls_postings_returns_correct_offsets() {
        let dir = make_test_dir("scan-offsets");
        let ls_path = format!("{}/test.ls", dir);

        {
            use std::io::Write;
            let mut f = std::fs::File::create(&ls_path).unwrap();
            let header = LsFileHeader::new(false, 16, 256 * 1024 * 1024, 0, 0, false);
            f.write_all(&header.to_page()).unwrap();

            let mut data_page = [0u8; 4096];
            for i in 0..3u64 {
                let mut record = PostingRecord::zeroed();
                record.set_magic();
                record.gsn = 100 + i;
                record.account_id_hi = 0;
                record.account_id_lo = i + 1;
                record.timestamp_ns = 1700000000_000_000_000 + i;
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

        let mut visitor = CountingVisitor::new();
        let write_offset = scan_ls_postings(&ls_path, &mut visitor);

        assert_eq!(visitor.count, 3);
        assert_eq!(write_offset, LsFileHeader::DATA_OFFSET as u64 + 4096);

        assert_eq!(visitor.offsets[0], LsFileHeader::DATA_OFFSET as u64);
        assert_eq!(visitor.offsets[1], LsFileHeader::DATA_OFFSET as u64 + 128);
        assert_eq!(visitor.offsets[2], LsFileHeader::DATA_OFFSET as u64 + 256);

        cleanup(&dir);
    }

    #[test]
    fn scan_ls_postings_no_file() {
        let mut visitor = CountingVisitor::new();
        let write_offset = scan_ls_postings("/tmp/nonexistent-ls", &mut visitor);

        assert_eq!(write_offset, LsFileHeader::DATA_OFFSET as u64);
        assert_eq!(visitor.count, 0);
    }
}