use std::ffi::CString;
use std::os::fd::RawFd;
use io_uring::{ IoUring, opcode, types, squeue };
use crate::flush_backend::{ FlushBackend, FlushCompletion };

const FSYNC_USER_DATA: u64 = 0xF1_05_D0_0E;
const PWRITE_USER_DATA: u64 = 0x70_57_17_0E;
const RING_ENTRIES: u32 = 32;

pub struct IoUringFlushBackend {
    ring: IoUring,
    fd: RawFd,
    pending_bytes: usize,
    fsync_submitted: bool,
}

impl IoUringFlushBackend {
    pub fn new() -> std::io::Result<Self> {
        let ring = IoUring::builder()
            .setup_sqpoll(2000)
            .build(RING_ENTRIES)
            .or_else(
                |_| {
                    println!("[io_uring] SQPOLL not available, falling back to normal mode");
                    IoUring::builder().build(RING_ENTRIES)
                }
            )?;

        Ok(
            Self {
                ring,
                fd: -1,
                pending_bytes: 0,
                fsync_submitted: false,
            }
        )
    }
}

impl FlushBackend for IoUringFlushBackend {
    fn open_ls_file(&mut self, path: &str) -> std::io::Result<()> {
        let c_path = CString::new(path).map_err(
            |_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid path")
            }
        )?;

        let fd = unsafe {
            libc::open(
                c_path.as_ptr(),
                libc::O_CREAT | libc::O_WRONLY,
                0o644,
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
        assert!(!self.fsync_submitted, "Previous flush not completed");

        self.pending_bytes = data.len();

        let pwrite_entry = opcode::Write::new(
            types::Fd(self.fd),
            data.as_ptr(),
            data.len() as u32,
        ).offset(offset)
            .build()
            .user_data(PWRITE_USER_DATA)
            .flags(squeue::Flags::IO_LINK);

        let fsync_entry = opcode::Fsync::new(
            types::Fd(self.fd),
        ).build().user_data(FSYNC_USER_DATA);

        unsafe {
            self.ring
                .submission()
                .push(&pwrite_entry)
                .map_err(
                    |_| std::io::Error::new(
                        std::io::ErrorKind::Other, "SQ full: pwrite",
                    )
                )?;

            self.ring
                .submission()
                .push(&fsync_entry)
                .map_err(
                    |_| std::io::Error::new(
                        std::io::ErrorKind::Other, "SQ full: fsync",
                    )
                )?;

            self.ring.submit()?;

            self.fsync_submitted = true;

            Ok(())
        }
    }

    fn poll_completion(&mut self) -> Option<FlushCompletion> {
        if !self.fsync_submitted {
            return None;
        }

        let mut pwrite_done = false;
        let mut fsync_done = false;
        let mut success = true;
        let mut bytes_written = 0usize;

        while let Some(cqe) = self.ring.completion().next() {
            match cqe.user_data() {
                PWRITE_USER_DATA => {
                    pwrite_done = true;
                    if cqe.result() < 0 {
                        success = false;
                    } else {
                        bytes_written = cqe.result() as usize;
                    }
                }
                FSYNC_USER_DATA => {
                    fsync_done = true;
                    if cqe.result() < 0 {
                        success = false;
                    }
                }
                _ => {}
            }
        }

        if fsync_done {
            self.fsync_submitted = false;
            Some(
                FlushCompletion {
                    bytes_written: if bytes_written > 0 {
                        bytes_written
                    } else {
                        self.pending_bytes
                    },
                    success,
                }
            )
        } else {
            None
        }
    }

    fn close(&mut self) {
        if self.fd >= 0 {
            unsafe {
                libc::close(self.fd);
            }
            self.fd = -1;
        }
    }
}

impl Drop for IoUringFlushBackend {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
#[cfg(target_os = "linux")]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Read;

    fn temp_path(name: &str) -> String {
        format!("/tmp/solidus-test-iouring-{}.ls", name)
    }

    #[test]
    fn open_creates_file() {
        let path = temp_path("open");
        let mut backend = IoUringFlushBackend::new().unwrap();
        backend.open_ls_file(&path).unwrap();
        assert!(fs::metadata(&path).is_ok());
        backend.close();
        fs::remove_file(&path).ok();
    }

    #[test]
    fn write_and_sync_persists_data() {
        let path = temp_path("write");
        let mut backend = IoUringFlushBackend::new().unwrap();
        backend.open_ls_file(&path).unwrap();

        let data = vec![0xABu8; 128];
        backend.submit_write_and_sync(&data, 0).unwrap();

        loop {
            if let Some(completion) = backend.poll_completion() {
                assert!(completion.success);
                assert_eq!(completion.bytes_written, 128);
                break;
            }
            std::hint::spin_loop();
        }

        backend.close();

        let mut file = fs::File::open(&path).unwrap();
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).unwrap();
        assert_eq!(contents.len(), 128);
        assert_eq!(contents[0], 0xAB);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn sequential_writes_at_correct_offsets() {
        let path = temp_path("sequential");
        let mut backend = IoUringFlushBackend::new().unwrap();
        backend.open_ls_file(&path).unwrap();

        let data1 = vec![0x01u8; 64];
        backend.submit_write_and_sync(&data1, 0).unwrap();
        loop {
            if let Some(c) = backend.poll_completion() {
                assert!(c.success);
                break;
            }
            std::hint::spin_loop();
        }

        let data2 = vec![0x02u8; 64];
        backend.submit_write_and_sync(&data2, 64).unwrap();
        loop {
            if let Some(c) = backend.poll_completion() {
                assert!(c.success);
                break;
            }
            std::hint::spin_loop();
        }

        backend.close();

        let contents = fs::read(&path).unwrap();
        assert_eq!(contents.len(), 128);
        assert_eq!(contents[0], 0x01);
        assert_eq!(contents[64], 0x02);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn poll_returns_none_before_submit() {
        let backend = IoUringFlushBackend::new().unwrap();
        let mut backend = backend;
        assert!(backend.poll_completion().is_none());
    }

    #[test]
    fn large_write() {
        let path = temp_path("large");
        let mut backend = IoUringFlushBackend::new().unwrap();
        backend.open_ls_file(&path).unwrap();

        let data = vec![0xCDu8; 65536];
        backend.submit_write_and_sync(&data, 0).unwrap();

        loop {
            if let Some(c) = backend.poll_completion() {
                assert!(c.success);
                assert_eq!(c.bytes_written, 65536);
                break;
            }
            std::hint::spin_loop();
        }

        backend.close();

        let contents = fs::read(&path).unwrap();
        assert_eq!(contents.len(), 65536);

        fs::remove_file(&path).ok();
    }
}