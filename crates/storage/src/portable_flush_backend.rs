use std::ffi::CString;
use crate::flush_backend::{FlushBackend, FlushCompletion};

pub struct PortableFlushBackend {
    fd: i32,
    pending_completion: Option<FlushCompletion>,
}

impl PortableFlushBackend {
    pub fn new() -> Self {
        Self {
            fd: -1,
            pending_completion: None,
        }
    }
}

impl FlushBackend for PortableFlushBackend {
    fn open_ls_file(&mut self, path: &str) -> std::io::Result<()> {
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

        self.fd = fd;
        Ok(())
    }

    fn submit_write_and_sync(&mut self, data: &[u8], offset: u64) -> std::io::Result<()> {
        assert!(self.fd >= 0, "LS file not opened");

        let written = unsafe {
            libc::pwrite(
                self.fd,
                data.as_ptr() as *const libc::c_void,
                data.len(),
                offset as libc::off_t,
            )
        };

        if written < 0 {
            self.pending_completion = Some(
                FlushCompletion {
                    bytes_written: 0,
                    success: false,
                }
            );
            return Err(std::io::Error::last_os_error());
        }

        let sync_result = unsafe { libc::fdatasync(self.fd) };

        self.pending_completion = Some(
            FlushCompletion {
                bytes_written: written as usize,
                success: sync_result == 0,
            }
        );

        Ok(())
    }

    fn poll_completion(&mut self) -> Option<FlushCompletion> {
        self.pending_completion.take()
    }
    
    fn close(&mut self) {
        if self.fd >= 0 {
            unsafe { libc::close(self.fd) };
            self.fd = -1;
        }
    }
}

impl Drop for PortableFlushBackend {
    fn drop(&mut self) {
        self.close();
    }
}