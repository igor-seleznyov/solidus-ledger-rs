use std::fs::File;
use std::io::Write;
use std::collections::HashMap;
use crate::account_index_record::AccountIndexRecord;
use crate::ordinal_index_entry::OrdinalIndexEntry;
use crate::timestamp_index_entry::TimestampIndexEntry;
use crate::index_file_header::{
    IndexFileHeader, INDEX_MAGIC_ACCOUNTS, INDEX_MAGIC_ORDINAL, INDEX_MAGIC_TIMESTAMP,
};
use crate::index_builder::{IndexBufferEntry, AccountMeta};

pub fn write_index_files(
    ls_path: &str,
    file_seq: u64,
    idx_records: &Vec<AccountIndexRecord>,
    sorted_keys: &Vec<(u64, u64)>,
    buffer: &mut Vec<IndexBufferEntry>,
    accounts: &HashMap<(u64, u64), AccountMeta>,
) {
    write_posting_accounts_file(ls_path, file_seq, idx_records);
    write_ordinal_file(ls_path, file_seq, sorted_keys, buffer, accounts);
    write_timestamp_file(ls_path, file_seq, sorted_keys, buffer, accounts);
}

fn write_posting_accounts_file(
    ls_path: &str,
    file_seq: u64,
    idx_records: &Vec<AccountIndexRecord>,
) {
    let path = format!("{}.posting-accounts", ls_path);
    let mut file = File::create(&path)
        .expect(
            &format!("Failed to create posting-accounts index file: {}", path)
        );

    let header = IndexFileHeader::new(
        INDEX_MAGIC_ACCOUNTS,
        1,
        idx_records.len() as u32,
        file_seq,
    );
    file.write_all(
        unsafe { header.as_bytes() }
    ).expect("Failed to write posting-accounts header");

    if !idx_records.is_empty() {
        let bytes = unsafe {
            std::slice::from_raw_parts(
                idx_records.as_ptr() as *const u8,
                idx_records.len() * AccountIndexRecord::SIZE,
            )
        };
        file.write_all(bytes)
            .expect("Failed to write posting-accounts records");
    }

    file.sync_all().expect("Failed to fsync posting-accounts index file");
}

fn write_ordinal_file(
    ls_path: &str,
    file_seq: u64,
    sorted_keys: &Vec<(u64, u64)>,
    buffer: &mut Vec<IndexBufferEntry>,
    accounts: &HashMap<(u64, u64), AccountMeta>,
) {
    let path = format!("{}.ordinal", ls_path);
    let mut file = File::create(&path)
        .expect(
            &format!("Failed to create ordinal index file: {}", path)
        );

    let total_entries: u32 = accounts.values().map(|meta| meta.count).sum();

    let header = IndexFileHeader::new(
        INDEX_MAGIC_ORDINAL,
        2,
        total_entries,
        file_seq,
    );
    file.write_all(
        unsafe { header.as_bytes() }
    ).expect("Failed to write ordinal header");

    for &(hi, lo) in sorted_keys {
        let meta = &accounts[&(hi, lo)];
        let start = meta.start;
        let end = start + meta.count as usize;
        let sub = &mut buffer[start..end];

        sub.sort_unstable_by_key(|entry| entry.ordinal);

        let entries: Vec<OrdinalIndexEntry> = sub.iter().map(
            |entry| {
                OrdinalIndexEntry {
                    ordinal: entry.ordinal,
                    ls_offset: entry.ls_offset,
                }
            }
        ).collect();

        let bytes = unsafe {
            std::slice::from_raw_parts(
                entries.as_ptr() as *const u8,
                entries.len() * OrdinalIndexEntry::SIZE,
            )
        };
        file.write_all(bytes).expect("Failed to write ordinal entries");
    }

    file.sync_all().expect("Failed to fsync ordinal index file");
}

fn write_timestamp_file(
    ls_path: &str,
    file_seq: u64,
    sorted_keys: &Vec<(u64, u64)>,
    buffer: &mut Vec<IndexBufferEntry>,
    accounts: &HashMap<(u64, u64), AccountMeta>,
) {
    let path = format!("{}.timestamp", ls_path);
    let mut file = File::create(&path)
        .expect(
            &format!("Failed to create timestamp index file: {}", path)
        );

    let total_entries: u32 = accounts.values().map(|meta| meta.count).sum();

    let header = IndexFileHeader::new(
        INDEX_MAGIC_TIMESTAMP,
        3,
        total_entries,
        file_seq,
    );
    file.write_all(
        unsafe { header.as_bytes() }
    ).expect("Failed to write timestamp header");

    for &(hi, lo) in sorted_keys {
        let meta = &accounts[&(hi, lo)];
        let start = meta.start;
        let end = start + meta.count as usize;
        let sub = &mut buffer[start..end];

        sub.sort_unstable_by_key(|entry| entry.timestamp_ns);

        let entries: Vec<TimestampIndexEntry> = sub.iter().map(
            |entry| {
                TimestampIndexEntry {
                    timestamp_ns: entry.timestamp_ns,
                    ls_offset: entry.ls_offset,
                }
            }
        ).collect();

        let bytes = unsafe {
            std::slice::from_raw_parts(
                entries.as_ptr() as *const u8,
                entries.len() * TimestampIndexEntry::SIZE,
            )
        };
        file.write_all(bytes)
            .expect("Failed to write timestamp entries");
    }

    file.sync_all().expect("Failed to fsync timestamp index file");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_builder::{IndexBufferEntry, AccountMeta};
    use crate::index_file_header::{
        INDEX_MAGIC_ACCOUNTS, INDEX_MAGIC_ORDINAL, INDEX_MAGIC_TIMESTAMP,
    };
    use std::collections::HashMap;
    use std::io::Read;

    fn make_test_dir(name: &str) -> String {
        let dir = format!(
            "/tmp/solidus-test-idxwriter-{}-{}",
            name,
            std::process::id(),
        );
        std::fs::create_dir_all(&dir).expect("Failed to create temp dir");
        dir
    }

    fn cleanup(dir: &str) {
        std::fs::remove_dir_all(dir).ok();
    }

    fn make_test_data() -> (
        Vec<AccountIndexRecord>,
        Vec<(u64, u64)>,
        Vec<IndexBufferEntry>,
        HashMap<(u64, u64), AccountMeta>,
    ) {

        let sorted_keys = vec![(0u64, 1u64), (0, 2), (0, 3)];

        let mut accounts = HashMap::new();
        accounts.insert((0, 1), AccountMeta { start: 0, count: 2, cursor: 2 });
        accounts.insert((0, 2), AccountMeta { start: 2, count: 1, cursor: 1 });
        accounts.insert((0, 3), AccountMeta { start: 3, count: 2, cursor: 2 });

        let buffer = vec![
            IndexBufferEntry { ordinal: 1, timestamp_ns: 2000, ls_offset: 4224 },
            IndexBufferEntry { ordinal: 0, timestamp_ns: 1000, ls_offset: 4096 },
            IndexBufferEntry { ordinal: 0, timestamp_ns: 1500, ls_offset: 4352 },
            IndexBufferEntry { ordinal: 1, timestamp_ns: 3000, ls_offset: 4608 },
            IndexBufferEntry { ordinal: 0, timestamp_ns: 500, ls_offset: 4480 },
        ];

        let header_size = IndexFileHeader::SIZE as u64;
        let idx_records = vec![
            AccountIndexRecord {
                account_id_hi: 0,
                account_id_lo: 1,
                ordinal_file_offset: header_size,
                timestamp_file_offset: header_size,
                records_count: 2,
                _pad: 0,
            },
            AccountIndexRecord {
                account_id_hi: 0,
                account_id_lo: 2,
                ordinal_file_offset: header_size + 2 * OrdinalIndexEntry::SIZE as u64,
                timestamp_file_offset: header_size + 2 * TimestampIndexEntry::SIZE as u64,
                records_count: 1,
                _pad: 0,
            },
            AccountIndexRecord {
                account_id_hi: 0,
                account_id_lo: 3,
                ordinal_file_offset: header_size + 3 * OrdinalIndexEntry::SIZE as u64,
                timestamp_file_offset: header_size + 3 * TimestampIndexEntry::SIZE as u64,
                records_count: 2,
                _pad: 0,
            },
        ];

        (idx_records, sorted_keys, buffer, accounts)
    }

    #[test]
    fn write_creates_three_files() {
        let dir = make_test_dir("creates-files");
        let ls_path = format!("{}/test.ls", dir);

        let (idx_records, sorted_keys, mut buffer, accounts) = make_test_data();
        write_index_files(&ls_path, 0, &idx_records, &sorted_keys, &mut buffer, &accounts);

        assert!(std::path::Path::new(&format!("{}.posting-accounts", ls_path)).exists());
        assert!(std::path::Path::new(&format!("{}.ordinal", ls_path)).exists());
        assert!(std::path::Path::new(&format!("{}.timestamp", ls_path)).exists());

        cleanup(&dir);
    }

    #[test]
    fn posting_accounts_file_has_correct_header() {
        let dir = make_test_dir("accounts-header");
        let ls_path = format!("{}/test.ls", dir);

        let (idx_records, sorted_keys, mut buffer, accounts) = make_test_data();
        write_index_files(&ls_path, 7, &idx_records, &sorted_keys, &mut buffer, &accounts);

        let path = format!("{}.posting-accounts", ls_path);
        let data = std::fs::read(&path).unwrap();

        assert!(data.len() >= IndexFileHeader::SIZE);

        let magic = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(magic, INDEX_MAGIC_ACCOUNTS);

        let entries_count = u32::from_le_bytes(data[12..16].try_into().unwrap());
        assert_eq!(entries_count, 3);

        let linked_seq = u64::from_le_bytes(data[16..24].try_into().unwrap());
        assert_eq!(linked_seq, 7);

        cleanup(&dir);
    }

    #[test]
    fn posting_accounts_file_has_correct_size() {
        let dir = make_test_dir("accounts-size");
        let ls_path = format!("{}/test.ls", dir);

        let (idx_records, sorted_keys, mut buffer, accounts) = make_test_data();
        write_index_files(&ls_path, 0, &idx_records, &sorted_keys, &mut buffer, &accounts);

        let path = format!("{}.posting-accounts", ls_path);
        let data = std::fs::read(&path).unwrap();

        assert_eq!(data.len(), IndexFileHeader::SIZE + 3 * AccountIndexRecord::SIZE);

        cleanup(&dir);
    }

    #[test]
    fn posting_accounts_records_sorted_by_account_id() {
        let dir = make_test_dir("accounts-sorted");
        let ls_path = format!("{}/test.ls", dir);

        let (idx_records, sorted_keys, mut buffer, accounts) = make_test_data();
        write_index_files(&ls_path, 0, &idx_records, &sorted_keys, &mut buffer, &accounts);

        let path = format!("{}.posting-accounts", ls_path);
        let data = std::fs::read(&path).unwrap();

        let offset = IndexFileHeader::SIZE;
        let account_id_lo = u64::from_le_bytes(
            data[offset + 8..offset + 16].try_into().unwrap()
        );
        assert_eq!(account_id_lo, 1);

        let offset2 = offset + AccountIndexRecord::SIZE;
        let account_id_lo_2 = u64::from_le_bytes(
            data[offset2 + 8..offset2 + 16].try_into().unwrap()
        );
        assert_eq!(account_id_lo_2, 2);

        let offset3 = offset2 + AccountIndexRecord::SIZE;
        let account_id_lo_3 = u64::from_le_bytes(
            data[offset3 + 8..offset3 + 16].try_into().unwrap()
        );
        assert_eq!(account_id_lo_3, 3);

        cleanup(&dir);
    }

    #[test]
    fn ordinal_file_has_correct_header() {
        let dir = make_test_dir("ordinal-header");
        let ls_path = format!("{}/test.ls", dir);

        let (idx_records, sorted_keys, mut buffer, accounts) = make_test_data();
        write_index_files(&ls_path, 0, &idx_records, &sorted_keys, &mut buffer, &accounts);

        let path = format!("{}.ordinal", ls_path);
        let data = std::fs::read(&path).unwrap();

        let magic = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(magic, INDEX_MAGIC_ORDINAL);

        let entries_count = u32::from_le_bytes(data[12..16].try_into().unwrap());
        assert_eq!(entries_count, 5);

        cleanup(&dir);
    }

    #[test]
    fn ordinal_file_has_correct_size() {
        let dir = make_test_dir("ordinal-size");
        let ls_path = format!("{}/test.ls", dir);

        let (idx_records, sorted_keys, mut buffer, accounts) = make_test_data();
        write_index_files(&ls_path, 0, &idx_records, &sorted_keys, &mut buffer, &accounts);

        let path = format!("{}.ordinal", ls_path);
        let data = std::fs::read(&path).unwrap();

        assert_eq!(data.len(), IndexFileHeader::SIZE + 5 * OrdinalIndexEntry::SIZE);

        cleanup(&dir);
    }

    #[test]
    fn ordinal_entries_sorted_within_account() {
        let dir = make_test_dir("ordinal-sorted");
        let ls_path = format!("{}/test.ls", dir);

        let (idx_records, sorted_keys, mut buffer, accounts) = make_test_data();
        write_index_files(&ls_path, 0, &idx_records, &sorted_keys, &mut buffer, &accounts);

        let path = format!("{}.ordinal", ls_path);
        let data = std::fs::read(&path).unwrap();

        let offset = IndexFileHeader::SIZE;

        let ordinal_0 = u64::from_le_bytes(
            data[offset..offset + 8].try_into().unwrap()
        );
        assert_eq!(ordinal_0, 0);

        let ordinal_1 = u64::from_le_bytes(
            data[offset + 16..offset + 24].try_into().unwrap()
        );
        assert_eq!(ordinal_1, 1);

        cleanup(&dir);
    }

    #[test]
    fn timestamp_entries_sorted_within_account() {
        let dir = make_test_dir("timestamp-sorted");
        let ls_path = format!("{}/test.ls", dir);

        let (idx_records, sorted_keys, mut buffer, accounts) = make_test_data();
        write_index_files(&ls_path, 0, &idx_records, &sorted_keys, &mut buffer, &accounts);

        let path = format!("{}.timestamp", ls_path);
        let data = std::fs::read(&path).unwrap();

        let offset = IndexFileHeader::SIZE + 3 * TimestampIndexEntry::SIZE;

        let ts_0 = u64::from_le_bytes(
            data[offset..offset + 8].try_into().unwrap()
        );
        assert_eq!(ts_0, 500);

        let ts_1 = u64::from_le_bytes(
            data[offset + 16..offset + 24].try_into().unwrap()
        );
        assert_eq!(ts_1, 3000);

        cleanup(&dir);
    }

    #[test]
    fn timestamp_file_has_correct_header() {
        let dir = make_test_dir("timestamp-header");
        let ls_path = format!("{}/test.ls", dir);

        let (idx_records, sorted_keys, mut buffer, accounts) = make_test_data();
        write_index_files(&ls_path, 0, &idx_records, &sorted_keys, &mut buffer, &accounts);

        let path = format!("{}.timestamp", ls_path);
        let data = std::fs::read(&path).unwrap();

        let magic = u64::from_le_bytes(data[0..8].try_into().unwrap());
        assert_eq!(magic, INDEX_MAGIC_TIMESTAMP);

        let entries_count = u32::from_le_bytes(data[12..16].try_into().unwrap());
        assert_eq!(entries_count, 5);

        cleanup(&dir);
    }

    #[test]
    fn ordinal_ls_offsets_match_sorted_order() {
        let dir = make_test_dir("ordinal-offsets");
        let ls_path = format!("{}/test.ls", dir);

        let (idx_records, sorted_keys, mut buffer, accounts) = make_test_data();
        write_index_files(&ls_path, 0, &idx_records, &sorted_keys, &mut buffer, &accounts);

        let path = format!("{}.ordinal", ls_path);
        let data = std::fs::read(&path).unwrap();

        let offset = IndexFileHeader::SIZE;

        let ls_off_0 = u64::from_le_bytes(
            data[offset + 8..offset + 16].try_into().unwrap()
        );
        assert_eq!(ls_off_0, 4096);

        let ls_off_1 = u64::from_le_bytes(
            data[offset + 24..offset + 32].try_into().unwrap()
        );
        assert_eq!(ls_off_1, 4224);

        cleanup(&dir);
    }
}