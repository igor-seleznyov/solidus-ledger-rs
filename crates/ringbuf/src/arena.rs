use std::io;
use std::ptr;

#[cfg(any(miri, test))]
use common::consts::CPU_CACHE_LINE_SIZE;

#[cfg(miri)]
use std::alloc::{alloc_zeroed, Layout, dealloc};

/// A fixed region of memory handed out at startup and never grown.
///
/// The base is aligned to a cache line, and everything placed in an Arena
/// depends on that: a ring keeps its two counters on separate lines at
/// fixed offsets from the base, and its slots are cache-line-aligned
/// containers indexed from it. A real mapping is page-aligned and
/// satisfies this with room to spare; the interpreter backing has to ask
/// for the alignment explicitly.
pub struct Arena {
    ptr: *mut u8,
    size: usize,
}

impl Arena {
    /// Backing used when the code runs inside the undefined-behaviour
    /// interpreter, which cannot perform the mapping syscalls.
    ///
    /// **For defence:** without this the interpreter can only check a
    /// hand-written copy of each ring rather than the shipped one, and a
    /// copy silently stops matching the original — the project has already
    /// paid that cost once, in a hash table whose interpreter tests
    /// validated an older shape than the code. Aligned zeroed memory from
    /// the ordinary allocator is indistinguishable from a mapping for
    /// every property the interpreter checks: it verifies aliasing and
    /// ordering, neither of which depends on where the pages came from.
    /// Production is untouched — the attribute is set only while the
    /// interpreter runs.
    #[cfg(miri)]
    pub fn new(size: usize) -> io::Result<Self> {
        let layout = Layout::from_size_align(size, CPU_CACHE_LINE_SIZE)
            .map_err(|_| io::Error::other("Invalid arena layout"))?;
        let ptr = unsafe {
            alloc_zeroed(layout)
        };

        if ptr.is_null() {
            return Err(io::Error::other("arena allocation failed"));
        }

        Ok(Self { ptr, size })
    }

    #[cfg(not(miri))]
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
    #[cfg(miri)]
    fn drop(&mut self) {
        unsafe {
            let layout = Layout::from_size_align_unchecked(self.size, CPU_CACHE_LINE_SIZE);
            dealloc(self.ptr, layout);
        }
    }

    #[cfg(not(miri))]
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
        let arena = Arena::new(4096).expect("arena allocation failed");
        assert_eq!(arena.size(), 4096);
    }

    #[test]
    fn pointer_is_not_null() {
        let arena = Arena::new(4096).expect("arena allocation failed");
        assert!(!arena.as_ptr().is_null());
    }

    #[test]
    fn write_and_read_back() {
        let arena = Arena::new(4096).expect("arena allocation failed");
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
        let arena = Arena::new(4096).expect("arena allocation failed");
        drop(arena);
    }

    /// The region's base is aligned to a cache line.
    ///
    /// Everything placed in an Arena assumes it: the ring's two counters
    /// sit on separate lines at fixed offsets, and its slots are
    /// cache-line-aligned containers indexed from the base. A real mapping
    /// satisfies this with room to spare, being page-aligned; the
    /// interpreter backing has to ask for it explicitly, and this is what
    /// checks that it did.
    #[test]
    fn base_is_cache_line_aligned() {
        let arena = Arena::new(4096).expect("arena allocation failed");
        assert!(
            (arena.as_ptr() as usize).is_multiple_of(CPU_CACHE_LINE_SIZE),
            "arena base must be cache-line aligned",
        );
    }

    #[test]
    fn zero_initialized() {
        let arena = Arena::new(4096).expect("arena allocation failed");
        let ptr = arena.as_ptr();
        for i in 0..4096 {
            let byte = unsafe { ptr.add(i).read() };
            assert_eq!(byte, 0, "byte at offset {} is not zero", i);
        }
    }
}