use crate::account_index_record::AccountIndexRecord;
use crate::consts::FILE_PAGE_SIZE;
use crate::index_file_header::{IndexFileHeader, INDEX_MAGIC_ACCOUNTS};
use crate::mmap_reader::MmapReader;

const RECORDS_PER_PAGE: usize = FILE_PAGE_SIZE / AccountIndexRecord::SIZE;

pub struct AccountLookupResult {
    pub ordinal_file_offset: u64,
    pub timestamp_file_offset: u64,
    pub records_count: u32,
}

pub fn lookup_account(
    posting_accounts_path: &str,
    account_id_hi: u64,
    account_id_lo: u64,
) -> Option<AccountLookupResult> {
    let mmap = MmapReader::open(posting_accounts_path).ok()?;

    if mmap.size() < IndexFileHeader::SIZE {
        return None;
    }

    let header_bytes = mmap.slice(0, IndexFileHeader::SIZE);
    let magic = u64::from_le_bytes(header_bytes[0..8].try_into().unwrap());
    if magic != INDEX_MAGIC_ACCOUNTS {
        return None;
    }

    let data_offset = IndexFileHeader::SIZE;
    let data_size = mmap.size() - data_offset;

    if data_size == 0 {
        return None;
    }

    let total_records = data_size / AccountIndexRecord::SIZE;
    if total_records == 0 {
        return None;
    }

    let records_ptr = unsafe {
        mmap.as_ptr().add(data_offset) as *const AccountIndexRecord
    };

    let total_pages = (total_records + RECORDS_PER_PAGE - 1) / RECORDS_PER_PAGE;

    if total_pages == 0 {
        return None;
    }

    let mut lo_page: usize = 0;
    let mut hi_page: usize = total_pages - 1;

    while lo_page <= hi_page {
        if hi_page - lo_page < 2 {
            let first_record = lo_page * RECORDS_PER_PAGE;
            let last_record = std::cmp::min(
                (hi_page + 1) * RECORDS_PER_PAGE,
                total_records,
            );
            return binary_search_records(
                records_ptr,
                first_record,
                last_record,
                account_id_hi,
                account_id_lo,
            );
        }

        let mid_page = lo_page + ((hi_page - lo_page) >> 1);
        let page_first_idx = mid_page * RECORDS_PER_PAGE;
        let page_last_idx = std::cmp::min(
            (mid_page + 1) * RECORDS_PER_PAGE,
            total_records,
        ) - 1;

        let first = unsafe { &*records_ptr.add(page_first_idx) };
        let last = unsafe { &*records_ptr.add(page_last_idx) };

        let cmp_first = compare_account_id(
            account_id_hi,
            account_id_lo,
            first.account_id_hi,
            first.account_id_lo,
        );
        let cmp_last = compare_account_id(
            account_id_hi,
            account_id_lo,
            last.account_id_hi,
            last.account_id_lo,
        );

        if cmp_first == std::cmp::Ordering::Less {
            if mid_page == 0 {
                return None;
            }
            hi_page = mid_page - 1;
        } else if cmp_last == std::cmp::Ordering::Greater {
            lo_page = mid_page + 1;
        } else {
            return binary_search_records(
                records_ptr,
                page_first_idx,
                page_last_idx + 1,
                account_id_hi,
                account_id_lo,
            );
        }
    }

    None
}

fn binary_search_records(
    records_ptr: *const AccountIndexRecord,
    lo: usize,
    hi: usize,
    account_id_hi: u64,
    account_id_lo: u64,
) -> Option<AccountLookupResult> {
    if lo >= hi {
        return None;
    }

    let mut left = lo;
    let mut right = hi;

    while left < right {
        let mid = left + (right - left) / 2;

        let record = unsafe { &*records_ptr.add(mid) };

        match compare_account_id(
            record.account_id_hi,
            record.account_id_lo,
            account_id_hi,
            account_id_lo,
        ) {
            std::cmp::Ordering::Equal => {
                return Some(
                    AccountLookupResult {
                        ordinal_file_offset: record.ordinal_file_offset,
                        timestamp_file_offset: record.timestamp_file_offset,
                        records_count: record.records_count,
                    }
                );
            }
            std::cmp::Ordering::Less => {
                left = mid + 1;
            }
            std::cmp::Ordering::Greater => {
                right = mid;
            }
        }
    }

    None
}

#[inline]
fn compare_account_id(
    left_hi: u64,
    left_lo: u64,
    right_hi: u64,
    right_lo: u64,
) -> std::cmp::Ordering {
    left_hi.cmp(&right_hi).then(left_lo.cmp(&right_lo))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account_index_record::AccountIndexRecord;
    use crate::index_file_header::IndexFileHeader;
    use std::io::Write;

    fn make_test_dir(name: &str) -> String {
        let dir = format!(
            "/tmp/solidus-test-idxreader-{}-{}",
            name,
            std::process::id(),
        );
        std::fs::create_dir_all(&dir).expect("Failed to create temp dir");
        dir
    }

    fn cleanup(dir: &str) {
        std::fs::remove_dir_all(dir).ok();
    }

    fn write_posting_accounts_file(
        path: &str,
        accounts: &Vec<(u64, u64, u64, u64, u32)>,
    ) {
        let mut f = std::fs::File::create(path).unwrap();

        let header = IndexFileHeader::new(
            INDEX_MAGIC_ACCOUNTS,
            1,
            accounts.len() as u32,
            0,
        );
        f.write_all(unsafe { header.as_bytes() }).unwrap();

        for &(hi, lo, ord_off, ts_off, count) in accounts {
            let record = AccountIndexRecord {
                account_id_hi: hi,
                account_id_lo: lo,
                ordinal_file_offset: ord_off,
                timestamp_file_offset: ts_off,
                records_count: count,
                _pad: 0,
            };
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    &record as *const AccountIndexRecord as *const u8,
                    AccountIndexRecord::SIZE,
                )
            };
            f.write_all(bytes).unwrap();
        }

        f.sync_all().unwrap();
    }

    #[test]
    fn lookup_single_account() {
        let dir = make_test_dir("single");
        let path = format!("{}/test.ls.posting-accounts", dir);

        write_posting_accounts_file(&path, &vec![
            (0, 1, 64, 64, 5),
        ]);

        let result = lookup_account(&path, 0, 1);
        assert!(result.is_some());

        let r = result.unwrap();
        assert_eq!(r.ordinal_file_offset, 64);
        assert_eq!(r.timestamp_file_offset, 64);
        assert_eq!(r.records_count, 5);

        cleanup(&dir);
    }

    #[test]
    fn lookup_not_found() {
        let dir = make_test_dir("not-found");
        let path = format!("{}/test.ls.posting-accounts", dir);

        write_posting_accounts_file(&path, &vec![
            (0, 1, 64, 64, 5),
            (0, 3, 144, 144, 3),
        ]);

        let result = lookup_account(&path, 0, 2);
        assert!(result.is_none());

        cleanup(&dir);
    }

    #[test]
    fn lookup_first_account() {
        let dir = make_test_dir("first");
        let path = format!("{}/test.ls.posting-accounts", dir);

        write_posting_accounts_file(&path, &vec![
            (0, 1, 64, 64, 2),
            (0, 2, 96, 96, 3),
            (0, 3, 144, 144, 1),
        ]);

        let result = lookup_account(&path, 0, 1).unwrap();
        assert_eq!(result.records_count, 2);

        cleanup(&dir);
    }

    #[test]
    fn lookup_last_account() {
        let dir = make_test_dir("last");
        let path = format!("{}/test.ls.posting-accounts", dir);

        write_posting_accounts_file(&path, &vec![
            (0, 1, 64, 64, 2),
            (0, 2, 96, 96, 3),
            (0, 3, 144, 144, 1),
        ]);

        let result = lookup_account(&path, 0, 3).unwrap();
        assert_eq!(result.records_count, 1);

        cleanup(&dir);
    }

    #[test]
    fn lookup_middle_account() {
        let dir = make_test_dir("middle");
        let path = format!("{}/test.ls.posting-accounts", dir);

        write_posting_accounts_file(&path, &vec![
            (0, 1, 64, 64, 2),
            (0, 5, 96, 96, 3),
            (0, 10, 144, 144, 1),
            (0, 20, 160, 160, 7),
            (0, 50, 272, 272, 4),
        ]);

        let result = lookup_account(&path, 0, 10).unwrap();
        assert_eq!(result.records_count, 1);
        assert_eq!(result.ordinal_file_offset, 144);

        cleanup(&dir);
    }

    #[test]
    fn lookup_with_hi_component() {
        let dir = make_test_dir("hi-component");
        let path = format!("{}/test.ls.posting-accounts", dir);

        write_posting_accounts_file(&path, &vec![
            (0, 1, 64, 64, 1),
            (0, 2, 80, 80, 1),
            (1, 1, 96, 96, 1),
            (1, 2, 112, 112, 1),
        ]);

        let result = lookup_account(&path, 1, 1).unwrap();
        assert_eq!(result.ordinal_file_offset, 96);

        let result = lookup_account(&path, 0, 2).unwrap();
        assert_eq!(result.ordinal_file_offset, 80);

        assert!(lookup_account(&path, 1, 3).is_none());

        cleanup(&dir);
    }

    #[test]
    fn lookup_many_accounts() {
        let dir = make_test_dir("many");
        let path = format!("{}/test.ls.posting-accounts", dir);

        let accounts: Vec<(u64, u64, u64, u64, u32)> = (0..1000u64)
            .map(|i| (0, i + 1, 64 + i * 16, 64 + i * 16, (i % 10 + 1) as u32))
            .collect();

        write_posting_accounts_file(&path, &accounts);

        for i in (0..1000).step_by(100) {
            let account_lo = i as u64 + 1;
            let result = lookup_account(&path, 0, account_lo);
            assert!(result.is_some(), "Account {} not found", account_lo);
            assert_eq!(result.unwrap().ordinal_file_offset, 64 + i as u64 * 16);
        }

        assert!(lookup_account(&path, 0, 1001).is_none());
        assert!(lookup_account(&path, 0, 0).is_none());

        cleanup(&dir);
    }

    #[test]
    fn lookup_nonexistent_file() {
        let result = lookup_account("/tmp/nonexistent-posting-accounts", 0, 1);
        assert!(result.is_none());
    }

    #[test]
    fn lookup_empty_index() {
        let dir = make_test_dir("empty-idx");
        let path = format!("{}/test.ls.posting-accounts", dir);

        write_posting_accounts_file(&path, &vec![]);

        let result = lookup_account(&path, 0, 1);
        assert!(result.is_none());

        cleanup(&dir);
    }
}