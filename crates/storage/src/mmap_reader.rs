use std::io;
use std::ffi::CString;

pub struct MmapReader {
    ptr: *const u8,
    size: usize,
}

impl MmapReader {
    pub fn open(path: &str) -> io::Result<Self> {
        let c_path = CString::new(path).map_err(
            |_| {
                io::Error::new(io::ErrorKind::InvalidInput, "Invalid path")
            }
        )?;

        let fd = unsafe {
            libc::open(c_path.as_ptr(), libc::O_RDONLY)
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        let size = unsafe {
            let mut stat: libc::stat = std::mem::zeroed();
            if libc::fstat(fd, &mut stat) != 0 {
                libc::close(fd);
                return Err(io::Error::last_os_error());
            }
            stat.st_size as usize
        };

        if size == 0 {
            unsafe { libc::close(fd); }
            return Err(
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Cannon mmap empty file",
                )
            );
        }

        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                size,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                fd,
                0,
            )
        };

        unsafe { libc::close(fd); }

        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        unsafe {
            libc::madvise(ptr, size, libc::MADV_SEQUENTIAL);
        }

        Ok(
            Self {
                ptr: ptr as *const u8,
                size,
            }
        )
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self.ptr, self.size)
        }
    }

    pub fn slice(&self, offset: usize, len: usize) -> &[u8] {
        assert!(
            offset + len <= self.size,
            "MmapReader::slice out of bounds: offset={}, len={}, size={}",
            offset,
            len,
            self.size,
        );
        unsafe {
            std::slice::from_raw_parts(self.ptr.add(offset), len)
        }
    }
}

unsafe impl Send for MmapReader {}

impl Drop for MmapReader {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.size);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn make_test_dir(name: &str) -> String {
        let dir = format!(
            "/tmp/solidus-test-mmap-{}-{}",
            name,
            std::process::id(),
        );
        std::fs::create_dir_all(&dir).expect("Failed to create temp dir");
        dir
    }

    fn cleanup(dir: &str) {
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn open_and_read() {
        let dir = make_test_dir("open-read");
        let path = format!("{}/test.bin", dir);

        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(b"hello world").unwrap();
            f.sync_all().unwrap();
        }

        let reader = MmapReader::open(&path).unwrap();
        assert_eq!(reader.size(), 11);
        assert_eq!(reader.as_slice(), b"hello world");

        cleanup(&dir);
    }

    #[test]
    fn slice_range() {
        let dir = make_test_dir("slice-range");
        let path = format!("{}/test.bin", dir);

        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(b"0123456789ABCDEF").unwrap();
            f.sync_all().unwrap();
        }

        let reader = MmapReader::open(&path).unwrap();
        assert_eq!(reader.slice(0, 4), b"0123");
        assert_eq!(reader.slice(10, 6), b"ABCDEF");
        assert_eq!(reader.slice(5, 5), b"56789");

        cleanup(&dir);
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn slice_out_of_bounds() {
        let dir = make_test_dir("slice-oob");
        let path = format!("{}/test.bin", dir);

        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(b"short").unwrap();
            f.sync_all().unwrap();
        }

        let reader = MmapReader::open(&path).unwrap();
        reader.slice(3, 10);
    }

    #[test]
    fn open_nonexistent_file() {
        let result = MmapReader::open("/tmp/nonexistent-mmap-test-file");
        assert!(result.is_err());
    }

    #[test]
    fn open_empty_file() {
        let dir = make_test_dir("empty");
        let path = format!("{}/empty.bin", dir);

        {
            std::fs::File::create(&path).unwrap();
        }

        let result = MmapReader::open(&path);
        assert!(result.is_err());

        cleanup(&dir);
    }

    #[test]
    fn read_binary_data() {
        let dir = make_test_dir("binary");
        let path = format!("{}/test.bin", dir);

        {
            let mut f = std::fs::File::create(&path).unwrap();
            let mut page = [0u8; 4096];
            for i in 0..4096 {
                page[i] = (i % 256) as u8;
            }
            f.write_all(&page).unwrap();
            f.sync_all().unwrap();
        }

        let reader = MmapReader::open(&path).unwrap();
        assert_eq!(reader.size(), 4096);

        let data = reader.as_slice();
        assert_eq!(data[0], 0);
        assert_eq!(data[255], 255);
        assert_eq!(data[256], 0);
        assert_eq!(data[4095], 255);

        cleanup(&dir);
    }

    #[test]
    fn read_u64_from_mmap() {
        let dir = make_test_dir("u64");
        let path = format!("{}/test.bin", dir);

        {
            let mut f = std::fs::File::create(&path).unwrap();
            let value: u64 = 0x0123456789ABCDEF;
            f.write_all(&value.to_le_bytes()).unwrap();
            let value2: u64 = 42;
            f.write_all(&value2.to_le_bytes()).unwrap();
            f.sync_all().unwrap();
        }

        let reader = MmapReader::open(&path).unwrap();
        assert_eq!(reader.size(), 16);

        let bytes = reader.slice(0, 8);
        let val = u64::from_le_bytes(bytes.try_into().unwrap());
        assert_eq!(val, 0x0123456789ABCDEF);

        let bytes2 = reader.slice(8, 8);
        let val2 = u64::from_le_bytes(bytes2.try_into().unwrap());
        assert_eq!(val2, 42);

        cleanup(&dir);
    }

    #[test]
    fn drop_releases_mapping() {
        let dir = make_test_dir("drop");
        let path = format!("{}/test.bin", dir);

        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(&[0u8; 4096]).unwrap();
            f.sync_all().unwrap();
        }

        let reader = MmapReader::open(&path).unwrap();
        assert_eq!(reader.size(), 4096);
        drop(reader);

        let reader2 = MmapReader::open(&path).unwrap();
        assert_eq!(reader2.size(), 4096);

        cleanup(&dir);
    }
}