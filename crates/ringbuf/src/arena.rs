use std::io;
use std::ptr;

pub struct Arena {
    ptr: *mut u8,
    size: usize,
}

impl Arena {
    pub fn new (size: usize) -> io::Result<Self> {
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        let ptr = ptr as *mut u8;

        let lock_result = unsafe { libc::mlock(ptr as *mut libc::c_void, size) };

        if lock_result != 0 {
            unsafe { libc::munmap(ptr as *mut libc::c_void, size); }
            return Err(io::Error::last_os_error());
        }

        Ok(Self { ptr, size })
    }

    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

unsafe impl Send for Arena {}

impl Drop for Arena {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.size);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_size() {
        let arena = Arena::new(4096).expect("mmap failed");
        assert_eq!(arena.size(), 4096);
    }

    #[test]
    fn pointer_is_not_null() {
        let arena = Arena::new(4096).expect("mmap failed");
        assert!(!arena.as_ptr().is_null());
    }

    #[test]
    fn write_and_read_back() {
        let arena = Arena::new(4096).expect("mmap failed");
        let ptr = arena.as_ptr();
        unsafe {
            ptr.write(0xAB);
            ptr.add(1).write(0xCD);
            assert_eq!(ptr.read(), 0xAB);
            assert_eq!(ptr.add(1).read(), 0xCD);
        }
    }

    #[test]
    fn drop_does_not_panic() {
        let arena = Arena::new(4096).expect("mmap failed");
        drop(arena);
    }

    #[test]
    fn zero_initialized() {
        let arena = Arena::new(4096).expect("mmap failed");
        let ptr = arena.as_ptr();
        for i in 0..4096 {
            let byte = unsafe { ptr.add(i).read() };
            assert_eq!(byte, 0, "byte at offset {} is not zero", i);
        }
    }
}