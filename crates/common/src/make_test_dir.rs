use std::sync::atomic::{AtomicU64, Ordering};

static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn make_test_dir() -> String {
    let id = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let process_id = std::process::id();
    let dir = format!("/tmp/solidus-ledger-test-{}-{}", process_id, id);
    std::fs::create_dir_all(&dir)
        .expect(
            &format!(
                "Failed to create test dir with process id = {} and counter value = {}",
                process_id,
                id
            )
        );
    dir
}