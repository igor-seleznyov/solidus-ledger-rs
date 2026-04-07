pub struct PostingIndexEntry {
    pub account_id_hi: u64,
    pub account_id_lo: u64,
    pub gsn: u64,
    pub ordinal: u64,
    pub timestamp_ns: u64,
    pub ls_offset: u64,
}