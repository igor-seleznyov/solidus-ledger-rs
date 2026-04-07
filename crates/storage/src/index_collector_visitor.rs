use crate::posting_scan_visitor::PostingScanVisitor;
use pipeline::posting_record::PostingRecord;
use crate::posting_index_entry::PostingIndexEntry;

pub struct IndexCollectorVisitor {
    pub entries: Vec<PostingIndexEntry>,
}

impl IndexCollectorVisitor {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
    
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }
}

impl PostingScanVisitor for IndexCollectorVisitor {
    fn on_posting(&mut self, ls_offset: u64, record: &PostingRecord) {
        self.entries.push(
            PostingIndexEntry {
                account_id_hi: record.account_id_hi,
                account_id_lo: record.account_id_lo,
                gsn: record.gsn,
                ordinal: record.ordinal,
                timestamp_ns: record.timestamp_ns,
                ls_offset
            }
        );
    }
}