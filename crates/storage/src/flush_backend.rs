pub struct FlushCompletion {
    pub bytes_written: usize,
    pub success: bool,
}

pub trait FlushBackend {
    fn open_ls_file(&mut self, path: &str) -> std::io::Result<()>;
    
    fn fallocate(&mut self, size: usize) -> std::io::Result<()>;

    fn submit_write_and_sync(&mut self, data: &[u8], offset: u64) -> std::io::Result<()>;

    fn poll_completion(&mut self) -> Option<FlushCompletion>;
    
    fn close(&mut self);
}
