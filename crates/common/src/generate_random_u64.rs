pub fn generate_random_u64() -> u64 {
    let mut buf = [0u8; 8];
    let ret = unsafe {
        libc::getrandom(
            buf.as_mut_ptr() as *mut libc::c_void,
            8,
            0,
        )
    };
    assert_eq!(ret, 8, "getrandom syscall failed");
    u64::from_ne_bytes(buf)
}