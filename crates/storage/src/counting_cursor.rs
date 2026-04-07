use std::collections::HashMap;
use pipeline::posting_record::PostingRecord;
use crate::posting_scan_visitor::PostingScanVisitor;

pub struct CountingVisitor {
    pub accounts: HashMap<(u64, u64), u32>,
    pub total_count: u64,
}

impl CountingVisitor {
    pub fn new() -> Self {
        Self {
            accounts: HashMap::new(),
            total_count: 0,
        }
    }
}

impl PostingScanVisitor for CountingVisitor {
    fn on_posting(&mut self, _ls_offset: u64, record: &PostingRecord) {
        let key = (record.account_id_hi, record.account_id_lo);
        *self.accounts.entry(key).or_insert(0) += 1;
        self.total_count += 1;
    }
}