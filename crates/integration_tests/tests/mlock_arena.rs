use ringbuf::arena::Arena;

#[test]
fn mlock_512mb_allocate_and_release() {
    let size = 512 * 1024 * 1024;

    let arena = Arena::new(size)
        .expect("Failed to mmap+mlock 512MB — check ulimit -l");

    assert_eq!(arena.size(), size);
    assert!(!arena.as_ptr().is_null());

    let ptr = arena.as_ptr();
    unsafe {
        ptr.write(0xAA);
        ptr.add(size - 1).write(0xBB);

        assert_eq!(ptr.read(), 0xAA);
        assert_eq!(ptr.add(size - 1).read(), 0xBB);
    }

    let status = std::fs::read_to_string("/proc/self/status")
        .expect("Failed to read /proc/self/status");

    let vmlck_line = status.lines()
        .find(|line| line.starts_with("VmLck:"))
        .expect("VmLck not found in /proc/self/status");

    let vmlck_kb: u64 = vmlck_line
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();

    assert!(
        vmlck_kb >= 512 * 1024,
        "VmLck too low: {} kB, expected >= {} kB",
        vmlck_kb, 512 * 1024,
    );

    drop(arena);

    let status_after = std::fs::read_to_string("/proc/self/status").unwrap();
    let vmlck_after: u64 = status_after.lines()
        .find(|line| line.starts_with("VmLck:"))
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();

    assert!(
        vmlck_after < vmlck_kb,
        "VmLck not released after drop: before={} kB, after={} kB",
        vmlck_kb, vmlck_after,
    );
}