use std::ffi::CString;
use crate::flush_backend::{FlushBackend, FlushCompletion};

const MAX_FILES: usize = 8;

pub struct PortableFlushBackend {
    fds: [i32; MAX_FILES],
    fds_count: usize,
    pending_completion: Option<FlushCompletion>,
}

impl PortableFlushBackend {
    pub fn new() -> Self {
        Self {
            fds: [-1; MAX_FILES],
            fds_count: 0,
            pending_completion: None,
        }
    }
}

impl FlushBackend for PortableFlushBackend {
    fn open_file(&mut self, path: &str, _prealloc_size: usize) -> std::io::Result<u8> {
        assert!(self.fds_count < MAX_FILES, "Too many open files");
        let c_path = CString::new(path).map_err(
            |_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput, "Invalid path"
                )
            }
        )?;

        let fd = unsafe {
            libc::open(
                c_path.as_ptr(),
                libc::O_CREAT | libc::O_WRONLY,
                0o644
            )
        };

        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        let handle_index = self.fds_count as u8;
        self.fds[self.fds_count] = fd;
        self.fds_count += 1;
        Ok(handle_index)
    }

    fn submit_write(
        &mut self,
        handle_index: u8,
        data: *const u8,
        len: usize,
        offset: u64,
    ) -> std::io::Result<()> {
        let fd = self.fds[handle_index as usize];
        assert!(fd >= 0, "File not opened: handle={:?}", handle_index);

        let slice = unsafe { std::slice::from_raw_parts(data, len) };

        let written = unsafe {
            libc::pwrite(
                fd,
                slice.as_ptr() as *const libc::c_void,
                slice.len(),
                offset as libc::off_t,
            )
        };

        if written < 0 {
            return Err(std::io::Error::last_os_error());
        }

        self.pending_completion = Some(
            FlushCompletion {
                bytes_written: written as usize,
                success: true,
            }
        );

        Ok(())
    }

    fn submit_sync(&mut self, handle_index: u8) -> std::io::Result<()> {
        let fd = self.fds[handle_index as usize];
        assert!(fd >= 0, "File not opened: handle={:?}", handle_index);

        let result = unsafe { libc::fdatasync(fd) };
        if result != 0 {
            if let Some(ref mut completion) = self.pending_completion {
                completion.success = false;
            }
        }
        Ok(())
    }

    fn flush_submissions(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn wait_completions(&mut self, _count: usize) {
    }

    fn poll_completion(&mut self) -> Option<FlushCompletion> {
        self.pending_completion.take()
    }

    fn close_file(&mut self, handle_index: u8) {
        let idx = handle_index as usize;
        if idx < MAX_FILES && self.fds[idx] >= 0 {
            unsafe {
                libc::close(self.fds[idx]);
            }
            self.fds[idx] = -1;
        }
    }
}

impl Drop for PortableFlushBackend {
    fn drop(&mut self) {
        for i in 0..MAX_FILES {
            self.close_file(i as u8);
        }
    }
}