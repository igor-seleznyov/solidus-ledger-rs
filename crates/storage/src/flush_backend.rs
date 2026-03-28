pub struct FlushCompletion {
    pub bytes_written: usize,
    pub success: bool,
}

pub trait FlushBackend {
    fn open_file(
        &mut self,
        path: &str,
        prealloc_size: usize,
    ) -> std::io::Result<u8>;

    fn open_file_buffered(
        &mut self,
        path: &str,
        prealloc_size: usize,
    ) -> std::io::Result<u8> {
        self.open_file(path, prealloc_size)
    }

    fn submit_write(
        &mut self,
        handle_index: u8,
        data: *const u8,
        len: usize,
        offset: u64,
    ) -> std::io::Result<()>;

    fn submit_sync(&mut self, handle_index: u8) -> std::io::Result<()>;

    fn flush_submissions(&mut self) -> std::io::Result<()>;

    fn wait_completions(&mut self, count: usize);
    
    #[inline]
    fn wait_for_the_one_completion(&mut self) {
        self.wait_completions(1);
    }

    fn poll_completion(&mut self) -> Option<FlushCompletion>;

    fn close_file(&mut self, handle_index: u8);
}
