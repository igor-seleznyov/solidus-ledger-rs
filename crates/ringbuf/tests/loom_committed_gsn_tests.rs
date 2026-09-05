#[cfg(feature = "loom")]
#[cfg(test)]
mod loom_committed_gsn_tests {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicU64, Ordering};

    /// LS Writer: writes the data — a stand-in for fdatasync — then
    /// release_store(committed_gsn). The DM does acquire_load(committed_gsn);
    /// at or above target_gsn the data is on disk.
    ///
    /// Loom looks for an interleaving where the DM sees committed_gsn = 100
    /// while disk_written is still false.
    #[test]
    fn loom_committed_gsn_release_acquire() {
        loom::model(|| {
            let committed_gsn = Arc::new(AtomicU64::new(0));
            let disk_written = Arc::new(AtomicU64::new(0));

            let gsn_w = committed_gsn.clone();
            let disk_w = disk_written.clone();

            let writer = loom::thread::spawn(move || {
                disk_w.store(1, Ordering::Relaxed);

                gsn_w.store(100, Ordering::Release);
            });

            let reader = loom::thread::spawn(move || {
                let gsn = committed_gsn.load(Ordering::Acquire);
                if gsn >= 100 {
                    assert_eq!(disk_written.load(Ordering::Relaxed), 1);
                }
            });

            writer.join().unwrap();
            reader.join().unwrap();
        });
    }

    /// Two flushes in sequence.
    /// LS Writer: flush1 (gsn=50) → flush2 (gsn=100).
    /// DM: seeing 100 means both the first and the second flush are on disk.
    #[test]
    fn loom_committed_gsn_sequential_flushes() {
        loom::model(|| {
            let committed_gsn = Arc::new(AtomicU64::new(0));
            let flush1_done = Arc::new(AtomicU64::new(0));
            let flush2_done = Arc::new(AtomicU64::new(0));

            let gsn_w = committed_gsn.clone();
            let f1_w = flush1_done.clone();
            let f2_w = flush2_done.clone();

            let writer = loom::thread::spawn(move || {
                f1_w.store(1, Ordering::Relaxed);
                gsn_w.store(50, Ordering::Release);

                f2_w.store(1, Ordering::Relaxed);
                gsn_w.store(100, Ordering::Release);
            });

            let reader = loom::thread::spawn(move || {
                let gsn = committed_gsn.load(Ordering::Acquire);
                if gsn >= 100 {
                    assert_eq!(flush1_done.load(Ordering::Relaxed), 1);
                    assert_eq!(flush2_done.load(Ordering::Relaxed), 1);
                } else if gsn >= 50 {
                    assert_eq!(flush1_done.load(Ordering::Relaxed), 1);
                }
            });

            writer.join().unwrap();
            reader.join().unwrap();
        });
    }
}