use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use crate::account_index_record::AccountIndexRecord;
use crate::index_file_header::IndexFileHeader;
use crate::ordinal_index_entry::OrdinalIndexEntry;
use crate::timestamp_index_entry::TimestampIndexEntry;

pub struct IndexBuilderTask {
    pub ls_path: String,
    pub file_seq: u64,
    pub shard_id: usize,
    pub signing_enabled: bool,
    pub metadata_enabled: bool,
    pub entries: Vec<IndexBufferEntry>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct IndexBufferEntry {
    pub account_id_hi: u64,
    pub account_id_lo: u64,
    pub ordinal: u64,
    pub timestamp_ns: u64,
    pub ls_offset: u64,
}

pub struct AccountMeta {
    pub start: usize,
    pub count: u32,
    pub cursor: u32,
}

pub struct IndexBuilder {
    id: usize,
    rx: Receiver<IndexBuilderTask>,
}

impl IndexBuilder {
    pub fn new(
        id: usize,
        rx: Receiver<IndexBuilderTask>,
    ) -> Self {
        Self { id, rx }
    }

    pub fn run(&self) {
        println!("[index-builder {}] started", self.id);

        loop {
            let task = match self.rx.recv() {
                Ok(task) => task,
                Err(_) => {
                    println!("[index-builder {}] channel closed, exiting", self.id);
                    break;
                }
            };

            self.build_indices(&task);
        }
    }

    fn compute_offsets(
        counts: HashMap<(u64, u64), u32>,
        idx_header_size: u64,
        ordinal_header_size: u64,
        timestamp_header_size: u64,
    ) -> (Vec<AccountIndexRecord>, Vec<(u64, u64)>, HashMap<(u64, u64), AccountMeta>) {
        let mut sorted_keys: Vec<(u64, u64)> = counts.keys().copied().collect();
        sorted_keys.sort_unstable();

        let mut accounts: HashMap<(u64, u64), AccountMeta> = HashMap::with_capacity(counts.len());
        let mut idx_records: Vec<AccountIndexRecord> = Vec::with_capacity(sorted_keys.len());

        let mut buffer_offset: usize = 0;
        let mut ordinal_file_offset: u64 = ordinal_header_size;
        let mut timestamp_file_offset: u64 = timestamp_header_size;

        for &(hi, lo) in &sorted_keys {
            let count = counts[&(hi, lo)];

            accounts.insert(
                (hi, lo),
                AccountMeta {
                    start: buffer_offset,
                    count,
                    cursor: 0,
                }
            );

            idx_records.push(
                AccountIndexRecord {
                    account_id_hi: hi,
                    account_id_lo: lo,
                    ordinal_file_offset,
                    timestamp_file_offset,
                    records_count: count,
                    _pad: 0,
                }
            );

            buffer_offset += count as usize;
            ordinal_file_offset += count as u64 * OrdinalIndexEntry::SIZE as u64;
            timestamp_file_offset += count as u64 * TimestampIndexEntry::SIZE as u64;
        }

        (idx_records, sorted_keys, accounts)
    }

    fn build_indices(&self, task: &IndexBuilderTask) {
        if let Err(error) = crate::file_protection::protect_rotated_files(
            &task.ls_path,
            task.signing_enabled,
            task.metadata_enabled,
            true
        ) {
            eprintln!(
                "[index-builder {}] FATAL: file protection failed for {}: {}. Initiating shutdown.",
                self.id, task.ls_path, error,
            );
            std::process::exit(1);
        }

        println!(
            "[index-builder {}] building indices for {} (file_seq={})",
            self.id, task.ls_path, task.file_seq,
        );

        if task.entries.is_empty() {
            println!(
                "[index-builder {}] no postings in {} — skipping index build",
                self.id, task.ls_path,
            );
            return;
        }

        let total_count = task.entries.len();

        let mut counts: HashMap<(u64, u64), u32> = HashMap::new();
        for entry in &task.entries {
            *counts.entry(
                (entry.account_id_hi, entry.account_id_lo),
            ).or_insert(0) += 1;
        }
        let accounts_count = counts.len();

        let (idx_records, sorted_keys, mut accounts) = Self::compute_offsets(
            counts,
            IndexFileHeader::SIZE as u64,
            IndexFileHeader::SIZE as u64,
            IndexFileHeader::SIZE as u64,
        );

        let mut buffer: Vec<IndexBufferEntry> = Vec::with_capacity(total_count);
        buffer.resize(
            total_count,
            IndexBufferEntry {
                account_id_hi: 0,
                account_id_lo: 0,
                ordinal: 0,
                timestamp_ns: 0,
                ls_offset: 0,
            }
        );

        for entry in &task.entries {
            let key = (entry.account_id_hi, entry.account_id_lo);
            let meta = accounts.get_mut(&key)
                .expect("Account not found - counting missed it");
            let index = meta.start + meta.cursor as usize;
            buffer[index] = *entry;
            meta.cursor += 1;
        }

        println!(
            "[index-builder {}] {} postings, {} accounts for {} (file_seq={})",
            self.id, total_count, accounts_count, task.ls_path, task.file_seq,
        );

        crate::index_writer::write_index_files(
            &task.ls_path,
            task.file_seq,
            &idx_records,
            &sorted_keys,
            &mut buffer,
            &accounts,
        );

        println!(
            "[index-builder {}] indices built for {} (file_seq={})",
            self.id, task.ls_path, task.file_seq,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_offsets_prefix_sum() {
        let mut counts = HashMap::new();
        counts.insert((0u64, 1u64), 3u32);
        counts.insert((0, 2), 2);
        counts.insert((1, 1), 5);

        let header_size = 64u64;
        let (idx_records, sorted_keys, accounts) = IndexBuilder::compute_offsets(
            counts, header_size, header_size, header_size,
        );

        assert_eq!(idx_records.len(), 3);
        assert_eq!(sorted_keys, vec![(0, 1), (0, 2), (1, 1)]);

        assert_eq!(idx_records[0].account_id_lo, 1);
        assert_eq!(idx_records[0].records_count, 3);
        assert_eq!(idx_records[0].ordinal_file_offset, header_size);

        assert_eq!(idx_records[1].account_id_lo, 2);
        assert_eq!(idx_records[1].ordinal_file_offset, header_size + 3 * 16);

        assert_eq!(idx_records[2].account_id_hi, 1);
        assert_eq!(idx_records[2].account_id_lo, 1);
        assert_eq!(idx_records[2].records_count, 5);
        assert_eq!(idx_records[2].ordinal_file_offset, header_size + 5 * 16);

        assert_eq!(accounts[&(0, 1)].start, 0);
        assert_eq!(accounts[&(0, 2)].start, 3);
        assert_eq!(accounts[&(1, 1)].start, 5);
    }

    #[test]
    fn compute_offsets_empty() {
        let counts = HashMap::new();
        let (idx_records, sorted_keys, accounts) = IndexBuilder::compute_offsets(
            counts, 64, 64, 64,
        );
        assert!(idx_records.is_empty());
        assert!(sorted_keys.is_empty());
        assert!(accounts.is_empty());
    }

    #[test]
    fn index_buffer_entry_size() {
        assert_eq!(std::mem::size_of::<IndexBufferEntry>(), 40);
    }

    #[test]
    fn build_indices_counts_and_places_from_entries() {
        let entries = vec![
            IndexBufferEntry {
                account_id_hi: 0, account_id_lo: 2,
                ordinal: 0, timestamp_ns: 1000, ls_offset: 4096,
            },
            IndexBufferEntry {
                account_id_hi: 0, account_id_lo: 1,
                ordinal: 0, timestamp_ns: 1001, ls_offset: 4224,
            },
            IndexBufferEntry {
                account_id_hi: 0, account_id_lo: 1,
                ordinal: 1, timestamp_ns: 1002, ls_offset: 4352,
            },
        ];

        let mut counts: HashMap<(u64, u64), u32> = HashMap::new();
        for entry in &entries {
            *counts.entry((entry.account_id_hi, entry.account_id_lo)).or_insert(0) += 1;
        }
        assert_eq!(counts[&(0, 1)], 2);
        assert_eq!(counts[&(0, 2)], 1);

        let (idx_records, _sorted_keys, mut accounts) = IndexBuilder::compute_offsets(
            counts, 64, 64, 64,
        );
        assert_eq!(idx_records.len(), 2);

        let total = entries.len();
        let mut buffer: Vec<IndexBufferEntry> = Vec::with_capacity(total);
        buffer.resize(total, IndexBufferEntry {
            account_id_hi: 0, account_id_lo: 0,
            ordinal: 0, timestamp_ns: 0, ls_offset: 0,
        });

        for entry in &entries {
            let key = (entry.account_id_hi, entry.account_id_lo);
            let meta = accounts.get_mut(&key).unwrap();
            let index = meta.start + meta.cursor as usize;
            buffer[index] = *entry;
            meta.cursor += 1;
        }

        assert_eq!(buffer[0].account_id_lo, 1);
        assert_eq!(buffer[0].ls_offset, 4224);
        assert_eq!(buffer[1].account_id_lo, 1);
        assert_eq!(buffer[1].ls_offset, 4352);

        assert_eq!(buffer[2].account_id_lo, 2);
        assert_eq!(buffer[2].ls_offset, 4096);
    }
}