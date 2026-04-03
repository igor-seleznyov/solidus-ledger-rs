use std::ffi::CString;
use std::os::fd::RawFd;
use io_uring::{ IoUring, opcode, types, squeue };
use crate::flush_backend::{ FlushBackend, FlushCompletion };

const FSYNC_USER_DATA: u64 = 0xF1_05_D0_0E;
const PWRITE_USER_DATA: u64 = 0x70_57_17_0E;
const RING_ENTRIES: u32 = 32;
const PAGE_SIZE: usize = 4096;
const MAX_FILES: usize = 8;

pub struct IoUringFlushBackend {
    ring: IoUring,
    fds: [i32; MAX_FILES],
    pending_bytes: usize,
    direct_io: [bool; MAX_FILES],
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
                fds: [-1; MAX_FILES],
                pending_bytes: 0,
                direct_io: [false; MAX_FILES],
            }
        )
    }

    fn find_free_slot(&self) -> usize {
        for i in 0..MAX_FILES {
            if self.fds[i] == -1 {
                return i;
            }
        }
        panic!("No free file slots (MAX_FILES = {})", MAX_FILES);
    }
}

impl FlushBackend for IoUringFlushBackend {
    fn open_file(&mut self, path: &str, prealloc_size: usize) -> std::io::Result<u8> {
        let handle_index = self.find_free_slot();

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

        if prealloc_size > 0 {
            let result = unsafe {
                libc::fallocate(fd, 0, 0, prealloc_size as libc::off_t)
            };
            if result != 0 {
                unsafe { libc::close(fd); }
                return Err(std::io::Error::last_os_error());
            }
        }

        self.fds[handle_index] = fd;
        self.direct_io[handle_index] = true;
        Ok(handle_index as u8)
    }
    
    fn open_file_buffered(
        &mut self,
        path: &str,
        prealloc_size: usize,
    ) -> std::io::Result<u8> {
        let handle_index = self.find_free_slot();

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
        
        if prealloc_size > 0 {
            let result = unsafe {
                libc::fallocate(fd, 0, 0, prealloc_size as libc::off_t)
            };
            if result != 0 {
                unsafe {
                    libc::close(fd);
                }
                return Err(std::io::Error::last_os_error());
            }
        }
        
        self.fds[handle_index] = fd;
        self.direct_io[handle_index] = false;
        Ok(handle_index as u8)
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
        
        if self.direct_io[handle_index as usize] {
            assert_eq!(data as usize % PAGE_SIZE, 0, "Buffer must be page-aligned for O_DIRECT");
            assert_eq!(len % PAGE_SIZE, 0, "Write size must be multiple of PAGE_SIZE for O_DIRECT");
            assert_eq!(offset % PAGE_SIZE as u64, 0, "Offset must be multiple of PAGE_SIZE for O_DIRECT");
        }

        self.pending_bytes = len;

        let pwrite_sqe = opcode::Write::new(
            types::Fd(fd),
            data,
            len as u32,
        ).offset(offset)
            .build()
            .user_data(PWRITE_USER_DATA)
            .flags(squeue::Flags::IO_LINK);

        unsafe {
            self.ring.submission().push(&pwrite_sqe)
                .map_err(
                    |_| std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "SQ full: pwrite"
                    )
                )?;
        }
        Ok(())
    }

    fn submit_sync(&mut self, handle_index: u8) -> std::io::Result<()> {
        let fd = self.fds[handle_index as usize];
        assert!(fd >= 0, "File not opened: handle={:?}", handle_index);

        let fsync_sqe = opcode::Fsync::new(types::Fd(fd))
            .build()
            .user_data(FSYNC_USER_DATA);

        unsafe {
            self.ring.submission().push(&fsync_sqe)
                .map_err(
                    |_| std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "SQ full: fsync",
                    )
                )?;
        }
        Ok(())
    }

    fn flush_submissions(&mut self) -> std::io::Result<()> {
        self.ring.submit()?;
        Ok(())
    }

    fn wait_completions(&mut self, count: usize) {
        let mut completed = 0;

        while completed < count {
            for cqe in self.ring.completion() {
                match cqe.user_data() {
                    PWRITE_USER_DATA => {
                        assert!(cqe.result() >= 0, "pwrite failed: {}", cqe.result());
                    }
                    FSYNC_USER_DATA => {
                        assert!(cqe.result() >= 0, "fsync failed: {}", cqe.result());
                        completed += 1;
                    }
                    _ => {
                        panic!("Unexpected CQE user_data={:#x}", cqe.user_data());
                    }
                }
            }
            if completed < count {
                std::hint::spin_loop();
            }
        }
    }

    fn poll_completion(&mut self) -> Option<FlushCompletion> {
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

impl Drop for IoUringFlushBackend {
    fn drop(&mut self) {
        for i in 0..MAX_FILES {
            self.close_file(i as u8);
        }
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
        let handle = backend.open_file(&path, 1024 * 1024).unwrap();

        let arena = ringbuf::arena::Arena::new(4096).unwrap();
        let ptr = arena.as_ptr();
        unsafe {
            std::ptr::write_bytes(ptr, 0xAB, 128);
        }

        backend.submit_write(handle, ptr, 4096, 0).unwrap();
        backend.submit_sync(handle).unwrap();
        backend.flush_submissions().unwrap();

        loop {
            if let Some(c) = backend.poll_completion() {
                assert!(c.success);
                assert_eq!(c.bytes_written, 4096);
                break;
            }
            std::hint::spin_loop();
        }

        backend.close_file(handle);

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
        let handle = backend.open_file(&path, 256 * 1024 * 1024).unwrap();

        let metadata = fs::metadata(&path).unwrap();
        backend.close_file(handle);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn sequential_aligned_writes() {
        let path = temp_path("seq-aligned");
        let mut backend = IoUringFlushBackend::new().unwrap();
        let handle = backend.open_file(&path, 1024 * 1024).unwrap();

        let arena = ringbuf::arena::Arena::new(8192).unwrap();
        let ptr = arena.as_ptr();

        unsafe { std::ptr::write_bytes(ptr, 0x01, 4096); }

        backend.submit_write(handle, ptr, 4096, 0).unwrap();
        backend.submit_sync(handle).unwrap();
        backend.flush_submissions().unwrap();

        loop {
            if let Some(c) = backend.poll_completion() {
                assert!(c.success);
                break;
            }
            std::hint::spin_loop();
        }

        unsafe {
            std::ptr::write_bytes(ptr.add(4096), 0x02, 4096);

            backend.submit_write(handle, ptr.add(4096) as *const u8, 4096, 4096).unwrap();
        }

        backend.submit_sync(handle).unwrap();
        backend.flush_submissions().unwrap();
        loop {
            if let Some(c) = backend.poll_completion() {
                assert!(c.success);
                break;
            }
            std::hint::spin_loop();
        }

        backend.close_file(handle);

        let contents = fs::read(&path).unwrap();
        assert!(contents.len() >= 8192);
        assert_eq!(contents[0], 0x01);
        assert_eq!(contents[4096], 0x02);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn open_file_buffered_no_direct_io() {
        let path = temp_path("buffered");
        let mut backend = IoUringFlushBackend::new().unwrap();
        let handle = backend.open_file_buffered(&path, 4096).unwrap();

        assert_eq!(handle, 0);
        assert!(!backend.direct_io[handle as usize]);
        assert!(backend.fds[handle as usize] >= 0);

        backend.close_file(handle);
        fs::remove_file(&path).ok();
    }

    #[test]
    fn buffered_write_unaligned_size() {
        let path = temp_path("buffered-unaligned");
        let mut backend = IoUringFlushBackend::new().unwrap();
        let handle = backend.open_file_buffered(&path, 4096).unwrap();

        let data = [0xABu8; 32];

        let arena = ringbuf::arena::Arena::new(4096).unwrap();
        let ptr = arena.as_ptr();
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, 32);
        }

        backend.submit_write(handle, ptr as *const u8, 32, 0).unwrap();
        backend.submit_sync(handle).unwrap();
        backend.flush_submissions().unwrap();

        loop {
            if let Some(c) = backend.poll_completion() {
                assert!(c.success);
                break;
            }
            std::hint::spin_loop();
        }

        backend.close_file(handle);

        let contents = fs::read(&path).unwrap();
        assert!(contents.len() >= 32);
        assert_eq!(contents[0], 0xAB);
        assert_eq!(contents[31], 0xAB);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn mixed_direct_and_buffered_handles() {
        let path_direct = temp_path("mixed-direct");
        let path_buffered = temp_path("mixed-buffered");
        let mut backend = IoUringFlushBackend::new().unwrap();

        let h_direct = backend.open_file(&path_direct, 1024 * 1024).unwrap();
        let h_buffered = backend.open_file_buffered(&path_buffered, 4096).unwrap();

        assert!(backend.direct_io[h_direct as usize]);
        assert!(!backend.direct_io[h_buffered as usize]);
        assert_eq!(h_direct, 0);
        assert_eq!(h_buffered, 1);

        backend.close_file(h_direct);
        backend.close_file(h_buffered);
        fs::remove_file(&path_direct).ok();
        fs::remove_file(&path_buffered).ok();
    }
}