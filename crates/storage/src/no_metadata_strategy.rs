use crate::metadata_strategy::MetadataStrategy;

pub struct NoMetadataStrategy;

impl MetadataStrategy for NoMetadataStrategy {
    fn file_info(&self, file_path: &str) -> Option<(String, usize)> { None }

    fn prepare_header(&mut self, file_seq: u64, max_file_size: u64) -> Option<(*const u8, usize, u64)> { None }

    fn on_header_written(&mut self) {}

    fn prepare_flush(&mut self) -> Option<(*const u8, usize, u64)> { None }

    fn on_flush_complete(&mut self) {}

    fn is_enabled(&self) -> bool {
        false
    }
}