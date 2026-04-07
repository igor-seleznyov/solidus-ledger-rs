use std::collections::HashMap;
use pipeline::posting_record::PostingRecord;
use crate::index_builder::{AccountMeta, IndexBufferEntry};
use crate::posting_scan_visitor::PostingScanVisitor;

pub struct PlacingVisitor<'a> {
    pub buffer: &'a mut Vec<IndexBufferEntry>,
    pub accounts: &'a mut HashMap<(u64, u64), AccountMeta>,
}

impl<'a> PlacingVisitor<'a> {
    pub fn new(
        buffer: &'a mut Vec<IndexBufferEntry>,
        accounts: &'a mut HashMap<(u64, u64), AccountMeta>,
    ) -> Self {
        Self { buffer, accounts }
    }
}

impl<'a> PostingScanVisitor for PlacingVisitor<'a> {
    fn on_posting(&mut self, ls_offset: u64, record: &PostingRecord) {
        let key = (record.account_id_hi, record.account_id_lo);
        let meta = self.accounts.get_mut(&key)
            .expect("Account not found in pass 2 - pass 1 missed for it");
        
        let index = meta.start + meta.cursor as usize;
        self.buffer[index] = IndexBufferEntry {
            ordinal: record.ordinal,
            timestamp_ns: record.timestamp_ns,
            ls_offset,
        };
        meta.cursor += 1;
    }
}