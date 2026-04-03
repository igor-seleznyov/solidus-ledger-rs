pub trait SigningStrategy {
    fn file_info(&self, file_path: &str) -> Option<(String, usize)>;

    fn prepare_header(&mut self, file_seq: u64) -> Option<(*const u8, usize, u64)>;

    fn on_header_written(&mut self);

    fn prepare_flush(
        &mut self,
        buffer_ptr: *mut u8,
        buffer_len: usize,
        write_offset: u64,
    ) -> Option<(*const u8, usize, u64)>;

    fn on_flush_complete(&mut self);
    
    fn on_rotation(&mut self) {}
    
    fn is_enabled(&self) -> bool;

    fn public_key_hash(&self) -> u32;

    fn chain_hash(&self) -> Option<&[u8; 32]> { None }
}
