use pipeline::posting_record::PostingRecord;
use crate::posting_scan_visitor::PostingScanVisitor;

pub struct RecoveryVisitor {
    pub gsn_min: u64,
    pub gsn_max: u64,
    pub timestamp_min_ns: u64,
    pub timestamp_max_ns: u64,
    pub postings_count: u64,
}

impl RecoveryVisitor {
    pub fn new() -> Self {
        Self {
            gsn_min: 0,
            gsn_max: 0,
            timestamp_min_ns: 0,
            timestamp_max_ns: 0,
            postings_count: 0,
        }
    }
}

impl PostingScanVisitor for RecoveryVisitor {
    fn on_posting(&mut self, _ls_offset: u64, record: &PostingRecord) {
        if self.gsn_min == 0 {
            self.gsn_min = record.gsn;
            self.timestamp_min_ns = record.timestamp_ns;
        }
        self.gsn_max = record.gsn;
        self.timestamp_max_ns = record.timestamp_ns;
        self.postings_count += 1;
    }
}