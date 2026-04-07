use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use crate::account_index_record::AccountIndexRecord;
use crate::counting_cursor::CountingVisitor;
use crate::index_file_header::IndexFileHeader;
use crate::ordinal_index_entry::OrdinalIndexEntry;
use crate::placing_visitor::PlacingVisitor;
use crate::timestamp_index_entry::TimestampIndexEntry;

pub struct IndexBuilderTask {
    pub ls_path: String,
    pub file_seq: u64,
    pub shard_id: usize,
    pub signing_enabled: bool,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct IndexBufferEntry {
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
    ) -> (Vec<AccountIndexRecord>, HashMap<(u64, u64), AccountMeta>) {
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

        (idx_records, accounts)
    }

    fn build_indices(&self, task: &IndexBuilderTask) {
        println!(
            "[index-builder {}] building indices for {} (file_seq={})",
            self.id, task.ls_path, task.file_seq,
        );

        let mut counting = CountingVisitor::new();
        let _write_offset = crate::posting_scan_visitor::scan_ls_postings(
            &task.ls_path,
            &mut counting,
        );

        let total_count = counting.total_count as usize;
        let accounts_count = counting.accounts.len();

        if total_count == 0 {
            println!(
                "[index-builder {}] no postings in {} — skipping index build",
                self.id, task.ls_path,
            );
            return;
        }

        let (_idx_records, mut accounts) = Self::compute_offsets(
            counting.accounts,
            IndexFileHeader::SIZE as u64,
            IndexFileHeader::SIZE as u64,
            IndexFileHeader::SIZE as u64,
        );

        let mut buffer: Vec<IndexBufferEntry> = Vec::with_capacity(total_count);
        buffer.resize(
            total_count,
            IndexBufferEntry {
                ordinal: 0,
                timestamp_ns: 0,
                ls_offset: 0,
            }
        );

        let mut placing = PlacingVisitor::new(&mut buffer, &mut accounts);
        crate::posting_scan_visitor::scan_ls_postings(&task.ls_path, &mut placing);

        println!(
            "[index-builder {}] scanned {} postings, {} accounts for {} (file_seq={})",
            self.id, total_count, accounts_count, task.ls_path, task.file_seq,
        );


        println!(
            "[index-builder {}] indices built for {} (file_seq={})",
            self.id, task.ls_path, task.file_seq,
        );
    }
}

#[cfg(test)]
mod tests {
    use pipeline::posting_record::PostingRecord;
    use crate::posting_scan_visitor::PostingScanVisitor;
    use super::*;

    #[test]
    fn counting_visitor_counts_per_account() {
        let mut visitor = CountingVisitor::new();

        let mut r1 = PostingRecord::zeroed();
        r1.account_id_hi = 0;
        r1.account_id_lo = 1;
        visitor.on_posting(4096, &r1);

        let mut r2 = PostingRecord::zeroed();
        r2.account_id_hi = 0;
        r2.account_id_lo = 2;
        visitor.on_posting(4224, &r2);

        let mut r3 = PostingRecord::zeroed();
        r3.account_id_hi = 0;
        r3.account_id_lo = 1;
        visitor.on_posting(4352, &r3);

        assert_eq!(visitor.total_count, 3);
        assert_eq!(visitor.accounts.len(), 2);
        assert_eq!(visitor.accounts[&(0, 1)], 2);
        assert_eq!(visitor.accounts[&(0, 2)], 1);
    }

    #[test]
    fn compute_offsets_prefix_sum() {
        let mut counts = HashMap::new();
        counts.insert((0u64, 1u64), 3u32);
        counts.insert((0, 2), 2);
        counts.insert((1, 1), 5);

        let header_size = 64u64;
        let (idx_records, accounts) = IndexBuilder::compute_offsets(
            counts, header_size, header_size, header_size,
        );

        assert_eq!(idx_records.len(), 3);
        assert_eq!(idx_records[0].account_id_hi, 0);
        assert_eq!(idx_records[0].account_id_lo, 1);
        assert_eq!(idx_records[0].records_count, 3);
        assert_eq!(idx_records[0].ordinal_file_offset, header_size);
        assert_eq!(idx_records[0].timestamp_file_offset, header_size);

        assert_eq!(idx_records[1].account_id_lo, 2);
        assert_eq!(idx_records[1].records_count, 2);
        assert_eq!(idx_records[1].ordinal_file_offset, header_size + 3 * 16);
        assert_eq!(idx_records[1].timestamp_file_offset, header_size + 3 * 16);

        assert_eq!(idx_records[2].account_id_hi, 1);
        assert_eq!(idx_records[2].account_id_lo, 1);
        assert_eq!(idx_records[2].records_count, 5);
        assert_eq!(idx_records[2].ordinal_file_offset, header_size + 5 * 16);

        assert_eq!(accounts[&(0, 1)].start, 0);
        assert_eq!(accounts[&(0, 2)].start, 3);
        assert_eq!(accounts[&(1, 1)].start, 5);
    }

    #[test]
    fn placing_visitor_fills_buffer() {
        let mut counts = HashMap::new();
        counts.insert((0u64, 1u64), 2u32);
        counts.insert((0, 2), 1);

        let (_idx_records, mut accounts) = IndexBuilder::compute_offsets(counts, 64, 64, 64);

        let total = 3;
        let mut buffer: Vec<IndexBufferEntry> = Vec::with_capacity(total);
        buffer.resize(total, IndexBufferEntry { ordinal: 0, timestamp_ns: 0, ls_offset: 0 });

        let mut r1 = PostingRecord::zeroed();
        r1.account_id_hi = 0;
        r1.account_id_lo = 2;
        r1.ordinal = 0;
        r1.timestamp_ns = 1000;

        let mut r2 = PostingRecord::zeroed();
        r2.account_id_hi = 0;
        r2.account_id_lo = 1;
        r2.ordinal = 0;
        r2.timestamp_ns = 1001;

        let mut r3 = PostingRecord::zeroed();
        r3.account_id_hi = 0;
        r3.account_id_lo = 1;
        r3.ordinal = 1;
        r3.timestamp_ns = 1002;

        {
            let mut placing = PlacingVisitor::new(&mut buffer, &mut accounts);
            placing.on_posting(4096, &r1);
            placing.on_posting(4224, &r2);
            placing.on_posting(4352, &r3);
        }

        assert_eq!(buffer[0].ordinal, 0);
        assert_eq!(buffer[0].ls_offset, 4224);
        assert_eq!(buffer[1].ordinal, 1);
        assert_eq!(buffer[1].ls_offset, 4352);

        assert_eq!(buffer[2].ordinal, 0);
        assert_eq!(buffer[2].ls_offset, 4096);
    }

    #[test]
    fn compute_offsets_empty() {
        let counts = HashMap::new();
        let (idx_records, accounts) = IndexBuilder::compute_offsets(counts, 64, 64, 64);
        assert!(idx_records.is_empty());
        assert!(accounts.is_empty());
    }

    #[test]
    fn placing_visitor_all_cursors_match_counts() {
        let mut counts = HashMap::new();
        counts.insert((0u64, 1u64), 2u32);
        counts.insert((0, 2), 3);

        let (_idx_records, mut accounts) = IndexBuilder::compute_offsets(counts, 64, 64, 64);

        let total = 5;
        let mut buffer: Vec<IndexBufferEntry> = Vec::with_capacity(total);
        buffer.resize(total, IndexBufferEntry { ordinal: 0, timestamp_ns: 0, ls_offset: 0 });

        {
            let mut placing = PlacingVisitor::new(&mut buffer, &mut accounts);

            for i in 0..2u64 {
                let mut r = PostingRecord::zeroed();
                r.account_id_hi = 0;
                r.account_id_lo = 1;
                r.ordinal = i;
                placing.on_posting(4096 + i * 128, &r);
            }

            for i in 0..3u64 {
                let mut r = PostingRecord::zeroed();
                r.account_id_hi = 0;
                r.account_id_lo = 2;
                r.ordinal = i;
                placing.on_posting(8192 + i * 128, &r);
            }
        }

        assert_eq!(accounts[&(0, 1)].cursor, 2);
        assert_eq!(accounts[&(0, 2)].cursor, 3);
    }
}