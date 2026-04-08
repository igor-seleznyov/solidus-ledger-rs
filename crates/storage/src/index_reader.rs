use crate::account_index_record::AccountIndexRecord;
use crate::consts::FILE_PAGE_SIZE;
use crate::index_file_header::{IndexFileHeader, INDEX_MAGIC_ACCOUNTS};
use crate::mmap_reader::MmapReader;
use crate::ordinal_index_entry::OrdinalIndexEntry;
use crate::timestamp_index_entry::TimestampIndexEntry;

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

pub fn query_timestamp_range(
    timestamp_path: &str,
    timestamp_file_offset: u64,
    records_count: u32,
    from_ns: u64,
    to_ns: u64,
) -> Vec<u64> {
    if records_count == 0 || from_ns > to_ns {
        return Vec::new();
    }

    let mmap = match MmapReader::open(timestamp_path) {
        Ok(mmap) => mmap,
        Err(_) => return Vec::new(),
    };

    let offset = timestamp_file_offset as usize;
    let count = records_count as usize;
    let section_size = count * TimestampIndexEntry::SIZE;

    if offset + section_size > mmap.size() {
        return Vec::new();
    }

    let entries_ptr = unsafe {
        mmap.as_ptr().add(offset) as *const TimestampIndexEntry
    };

    let lower = lower_bound_u64(
        entries_ptr,
        count,
        from_ns,
        |entry| {
            unsafe { (*entry).timestamp_ns }
        }
    );

    let upper = upper_bound_u64(
        entries_ptr,
        count,
        to_ns,
        |entry| {
            unsafe { (*entry).timestamp_ns }
        }
    );

    if lower >= upper {
        return Vec::new();
    }

    let mut result = Vec::with_capacity(upper - lower);
    for i in lower..upper {
        let entry = unsafe { &*entries_ptr.add(i) };
        result.push(entry.ls_offset);
    }

    result
}

pub fn query_ordinal_range(
    ordinal_path: &str,
    ordinal_file_offset: u64,
    records_count: u32,
    from_ordinal: u64,
    to_ordinal: u64,
) -> Vec<u64> {
    if records_count == 0 || from_ordinal > to_ordinal {
        return Vec::new();
    }

    let mmap = match MmapReader::open(ordinal_path) {
        Ok(mmap) => mmap,
        Err(_) => return Vec::new(),
    };

    let offset = ordinal_file_offset as usize;
    let count = records_count as usize;
    let section_size = count * OrdinalIndexEntry::SIZE;

    if offset + section_size > mmap.size() {
        return Vec::new();
    }

    let entries_ptr = unsafe {
        mmap.as_ptr().add(offset) as *const OrdinalIndexEntry
    };

    let lower = lower_bound_u64(
        entries_ptr,
        count,
        from_ordinal,
        |entry| {
            unsafe { (*entry).ordinal }
        }
    );

    let upper = upper_bound_u64(
        entries_ptr,
        count,
        to_ordinal,
        |entry| {
            unsafe { (*entry).ordinal }
        }
    );

    if lower >= upper {
        return Vec::new();
    }

    let mut result = Vec::with_capacity(upper - lower);
    for i in lower..upper {
        let entry = unsafe {
            &*entries_ptr.add(i)
        };
        result.push(entry.ls_offset);
    }

    result
}

#[inline]
fn lower_bound_u64<T>(
    ptr: *const T,
    count: usize,
    target: u64,
    key_fn: impl Fn(*const T) -> u64,
) -> usize {
    let mut lo = 0usize;
    let mut hi = count;

    while lo < hi {
        let mid = lo + ((hi - lo) >> 1);
        let value = key_fn(
            unsafe { ptr.add(mid) }
        );
        if value < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    lo
}

#[inline]
fn upper_bound_u64<T>(
    ptr: *const T,
    count: usize,
    target: u64,
    key_fn: impl Fn(*const T) -> u64,
) -> usize {
    let mut lo = 0usize;
    let mut hi = count;

    while lo < hi {
        let mid = lo + ((hi - lo) >> 1);
        let value = key_fn(
            unsafe { ptr.add(mid) }
        );
        if value <= target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    lo
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

    use crate::ordinal_index_entry::OrdinalIndexEntry;
    use crate::timestamp_index_entry::TimestampIndexEntry;
    use crate::index_file_header::{INDEX_MAGIC_ORDINAL, INDEX_MAGIC_TIMESTAMP};

    fn write_timestamp_file(path: &str, entries: &Vec<(u64, u64)>) {
        let mut f = std::fs::File::create(path).unwrap();

        let header = IndexFileHeader::new(
            INDEX_MAGIC_TIMESTAMP,
            3,
            entries.len() as u32,
            0,
        );
        f.write_all(unsafe { header.as_bytes() }).unwrap();

        for &(timestamp_ns, ls_offset) in entries {
            let entry = TimestampIndexEntry { timestamp_ns, ls_offset };
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    &entry as *const TimestampIndexEntry as *const u8,
                    TimestampIndexEntry::SIZE,
                )
            };
            f.write_all(bytes).unwrap();
        }

        f.sync_all().unwrap();
    }

    fn write_ordinal_file(path: &str, entries: &Vec<(u64, u64)>) {
        let mut f = std::fs::File::create(path).unwrap();

        let header = IndexFileHeader::new(
            INDEX_MAGIC_ORDINAL,
            2,
            entries.len() as u32,
            0,
        );
        f.write_all(unsafe { header.as_bytes() }).unwrap();

        for &(ordinal, ls_offset) in entries {
            let entry = OrdinalIndexEntry { ordinal, ls_offset };
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    &entry as *const OrdinalIndexEntry as *const u8,
                    OrdinalIndexEntry::SIZE,
                )
            };
            f.write_all(bytes).unwrap();
        }

        f.sync_all().unwrap();
    }

    #[test]
    fn timestamp_range_full() {
        let dir = make_test_dir("ts-full");
        let path = format!("{}/test.ls.timestamp", dir);

        write_timestamp_file(&path, &vec![
            (1000, 4096),
            (2000, 4224),
            (3000, 4352),
            (4000, 4480),
            (5000, 4608),
        ]);

        let offsets = query_timestamp_range(
            &path,
            IndexFileHeader::SIZE as u64,
            5,
            1000,
            5000,
        );

        assert_eq!(offsets.len(), 5);
        assert_eq!(offsets, vec![4096, 4224, 4352, 4480, 4608]);

        cleanup(&dir);
    }

    #[test]
    fn timestamp_range_partial() {
        let dir = make_test_dir("ts-partial");
        let path = format!("{}/test.ls.timestamp", dir);

        write_timestamp_file(&path, &vec![
            (1000, 4096),
            (2000, 4224),
            (3000, 4352),
            (4000, 4480),
            (5000, 4608),
        ]);

        let offsets = query_timestamp_range(
            &path,
            IndexFileHeader::SIZE as u64,
            5,
            2000,
            4000,
        );

        assert_eq!(offsets.len(), 3);
        assert_eq!(offsets, vec![4224, 4352, 4480]);

        cleanup(&dir);
    }

    #[test]
    fn timestamp_range_single_match() {
        let dir = make_test_dir("ts-single");
        let path = format!("{}/test.ls.timestamp", dir);

        write_timestamp_file(&path, &vec![
            (1000, 4096),
            (2000, 4224),
            (3000, 4352),
        ]);

        let offsets = query_timestamp_range(
            &path,
            IndexFileHeader::SIZE as u64,
            3,
            2000,
            2000,
        );

        assert_eq!(offsets.len(), 1);
        assert_eq!(offsets[0], 4224);

        cleanup(&dir);
    }

    #[test]
    fn timestamp_range_no_match() {
        let dir = make_test_dir("ts-no-match");
        let path = format!("{}/test.ls.timestamp", dir);

        write_timestamp_file(&path, &vec![
            (1000, 4096),
            (3000, 4224),
            (5000, 4352),
        ]);

        let offsets = query_timestamp_range(
            &path,
            IndexFileHeader::SIZE as u64,
            3,
            1500,
            2500,
        );

        assert!(offsets.is_empty());

        cleanup(&dir);
    }

    #[test]
    fn timestamp_range_before_all() {
        let dir = make_test_dir("ts-before");
        let path = format!("{}/test.ls.timestamp", dir);

        write_timestamp_file(&path, &vec![
            (1000, 4096),
            (2000, 4224),
        ]);

        let offsets = query_timestamp_range(
            &path,
            IndexFileHeader::SIZE as u64,
            2,
            100,
            500,
        );

        assert!(offsets.is_empty());

        cleanup(&dir);
    }

    #[test]
    fn timestamp_range_after_all() {
        let dir = make_test_dir("ts-after");
        let path = format!("{}/test.ls.timestamp", dir);

        write_timestamp_file(&path, &vec![
            (1000, 4096),
            (2000, 4224),
        ]);

        let offsets = query_timestamp_range(
            &path,
            IndexFileHeader::SIZE as u64,
            2,
            5000,
            9000,
        );

        assert!(offsets.is_empty());

        cleanup(&dir);
    }

    #[test]
    fn ordinal_range_full() {
        let dir = make_test_dir("ord-full");
        let path = format!("{}/test.ls.ordinal", dir);

        write_ordinal_file(&path, &vec![
            (0, 4096),
            (1, 4224),
            (2, 4352),
            (3, 4480),
        ]);

        let offsets = query_ordinal_range(
            &path,
            IndexFileHeader::SIZE as u64,
            4,
            0,
            3,
        );

        assert_eq!(offsets.len(), 4);
        assert_eq!(offsets, vec![4096, 4224, 4352, 4480]);

        cleanup(&dir);
    }

    #[test]
    fn ordinal_range_partial() {
        let dir = make_test_dir("ord-partial");
        let path = format!("{}/test.ls.ordinal", dir);

        write_ordinal_file(&path, &vec![
            (0, 4096),
            (1, 4224),
            (2, 4352),
            (3, 4480),
            (4, 4608),
        ]);

        let offsets = query_ordinal_range(
            &path,
            IndexFileHeader::SIZE as u64,
            5,
            1,
            3,
        );

        assert_eq!(offsets.len(), 3);
        assert_eq!(offsets, vec![4224, 4352, 4480]);

        cleanup(&dir);
    }

    #[test]
    fn ordinal_range_empty_count() {
        let offsets = query_ordinal_range(
            "/tmp/nonexistent",
            64,
            0,
            0,
            10,
        );
        assert!(offsets.is_empty());
    }

    #[test]
    fn ordinal_range_invalid_range() {
        let offsets = query_ordinal_range(
            "/tmp/nonexistent",
            64,
            5,
            10,
            5,
        );
        assert!(offsets.is_empty());
    }

    #[test]
    fn timestamp_range_nonexistent_file() {
        let offsets = query_timestamp_range(
            "/tmp/nonexistent-timestamp",
            64,
            5,
            1000,
            5000,
        );
        assert!(offsets.is_empty());
    }

    #[test]
    fn ordinal_range_with_offset_in_file() {
        let dir = make_test_dir("ord-offset");
        let path = format!("{}/test.ls.ordinal", dir);

        write_ordinal_file(&path, &vec![
            (0, 4096),
            (1, 4224),
            (2, 4352),
            (0, 4480),
            (1, 4608),
        ]);

        let account_2_offset = IndexFileHeader::SIZE as u64
            + 3 * OrdinalIndexEntry::SIZE as u64;

        let offsets = query_ordinal_range(
            &path,
            account_2_offset,
            2,
            0,
            1,
        );

        assert_eq!(offsets.len(), 2);
        assert_eq!(offsets, vec![4480, 4608]);

        cleanup(&dir);
    }
}