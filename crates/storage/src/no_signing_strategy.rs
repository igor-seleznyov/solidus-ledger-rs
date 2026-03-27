use crate::signing_strategy::SigningStrategy;

pub struct NoSigningStrategy;

impl SigningStrategy for NoSigningStrategy {

    fn file_info(&self, _file_path: &str) -> Option<(String, usize)> { None }
    fn prepare_header(&mut self, _file_seq: u64) -> Option<(*const u8, usize, u64)> { None }
    fn on_header_written(&mut self) {}
    fn prepare_flush(
        &mut self,
        _buffer_ptr: *mut u8,
        _buffer_len: usize,
        _write_offset: u64,
    ) -> Option<(*const u8, usize, u64)> { None }
    fn on_flush_complete(&mut self) {}
    fn is_enabled(&self) -> bool { false }
    fn public_key_hash(&self) -> u32 { 0 }
}