use std::ffi::CString;
use std::os::fd::RawFd;
use io_uring::{ IoUring, opcode, types, squeue };
use crate::flush_backend::{ FlushBackend, FlushCompletion };

const FSYNC_USER_DATA: u64 = 0xF1_05_D0_0E;
const PWRITE_USER_DATA: u64 = 0x70_57_17_0E;
const RING_ENTRIES: u32 = 32;
const PAGE_SIZE: usize = 4096;

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

    fn fallocate(&self, size: usize) -> std::io::Result<()> {
        let result = unsafe {
            libc::fallocate(self.fd, 0, 0, size as libc::off_t)
        };
        if result != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
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
                libc::O_CREAT | libc::O_WRONLY | libc::O_DIRECT,
                0o644,
            )
        };

        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        self.fd = fd;
        Ok(())
    }
    
    fn fallocate(&mut self, size: usize) -> std::io::Result<()> {
        assert!(self.fd >= 0, "LS file is not opened");
        if size == 0 {
            return Ok(());
        }
        let result = unsafe {
            libc::fallocate(self.fd, 0, 0, size as libc::off_t)
        };
        if result != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    fn submit_write_and_sync(&mut self, data: &[u8], offset: u64) -> std::io::Result<()> {
        assert!(self.fd >= 0, "LS file not opened");
        assert!(!self.fsync_submitted, "Previous flush not completed");
        assert_eq!(data.as_ptr() as usize % PAGE_SIZE, 0, "Buffer must be page-aligned for O_DIRECT");
        assert_eq!(data.len() % PAGE_SIZE, 0, "Write size must be multiple of PAGE_SIZE for O_DIRECT");
        assert_eq!(offset % PAGE_SIZE as u64, 0, "Offset must be multiple of PAGE_SIZE for O_DIRECT");

        self.pending_bytes = data.len();

        let pwrite_entry = opcode::Write::new(
            types::Fd(self.fd),
            data.as_ptr(),
            data.len() as u32,
        )
            .offset(offset)
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
        format!("/tmp/solidus-test-odirect-{}.ls", name)
    }

    #[test]
    fn write_aligned_data() {
        let path = temp_path("aligned");
        let mut backend = IoUringFlushBackend::new().unwrap();
        backend.open_ls_file(&path).unwrap();
        backend.fallocate(1024 * 1024).unwrap();

        let arena = ringbuf::arena::Arena::new(4096).unwrap();
        let ptr = arena.as_ptr();
        unsafe {
            std::ptr::write_bytes(ptr, 0xAB, 128);
        }

        let data = unsafe { std::slice::from_raw_parts(ptr, 4096) };
        backend.submit_write_and_sync(data, 0).unwrap();

        loop {
            if let Some(c) = backend.poll_completion() {
                assert!(c.success);
                assert_eq!(c.bytes_written, 4096);
                break;
            }
            std::hint::spin_loop();
        }

        backend.close();

        let contents = fs::read(&path).unwrap();
        assert!(contents.len() >= 4096);
        assert_eq!(contents[0], 0xAB);
        assert_eq!(contents[127], 0xAB);
        assert_eq!(contents[128], 0x00);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn fallocate_preallocates_space() {
        let path = temp_path("fallocate");
        let mut backend = IoUringFlushBackend::new().unwrap();
        backend.open_ls_file(&path).unwrap();
        backend.fallocate(256 * 1024 * 1024).unwrap();

        let metadata = fs::metadata(&path).unwrap();
        backend.close();

        fs::remove_file(&path).ok();
    }

    #[test]
    fn sequential_aligned_writes() {
        let path = temp_path("seq-aligned");
        let mut backend = IoUringFlushBackend::new().unwrap();
        backend.open_ls_file(&path).unwrap();
        backend.fallocate(1024 * 1024).unwrap();

        let arena = ringbuf::arena::Arena::new(8192).unwrap();
        let ptr = arena.as_ptr();

        unsafe { std::ptr::write_bytes(ptr, 0x01, 4096); }
        let data1 = unsafe { std::slice::from_raw_parts(ptr, 4096) };
        backend.submit_write_and_sync(data1, 0).unwrap();
        loop {
            if let Some(c) = backend.poll_completion() {
                assert!(c.success);
                break;
            }
            std::hint::spin_loop();
        }

        unsafe { std::ptr::write_bytes(ptr.add(4096), 0x02, 4096); }
        let data2 = unsafe { std::slice::from_raw_parts(ptr.add(4096), 4096) };
        backend.submit_write_and_sync(data2, 4096).unwrap();
        loop {
            if let Some(c) = backend.poll_completion() {
                assert!(c.success);
                break;
            }
            std::hint::spin_loop();
        }

        backend.close();

        let contents = fs::read(&path).unwrap();
        assert!(contents.len() >= 8192);
        assert_eq!(contents[0], 0x01);
        assert_eq!(contents[4096], 0x02);

        fs::remove_file(&path).ok();
    }
}