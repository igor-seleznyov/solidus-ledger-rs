pub trait MetadataStrategy {
    fn file_info(&self, file_path: &str) -> Option<(String, usize)>;

    fn prepare_header(&mut self, file_seq: u64, max_file_size: u64) -> Option<(*const u8, usize, u64)>;

    fn on_header_written(&mut self);

    fn prepare_flush(&mut self) -> Option<(*const u8, usize, u64)>;

    fn on_flush_complete(&mut self);

    fn is_enabled(&self) -> bool;
}
