#[cfg(feature = "loom")]
#[cfg(test)]
mod loom_extended_flush_tests {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicU64, AtomicU8, Ordering};

    #[test]
    fn loom_extended_flush_to_client_response() {
        loom::model(|| {
            let disk_written = Arc::new(AtomicU64::new(0));
            let committed_gsn = Arc::new(AtomicU64::new(0));
            let response_seq = Arc::new(AtomicU64::new(0));
            let response_status = Arc::new(AtomicU8::new(0));

            let disk_w = disk_written.clone();
            let gsn_w = committed_gsn.clone();

            let ls_writer = loom::thread::spawn(move || {
                disk_w.store(1, Ordering::Relaxed);
                gsn_w.store(100, Ordering::Release);
            });

            let gsn_dm = committed_gsn.clone();
            let resp_seq_dm = response_seq.clone();
            let resp_status_dm = response_status.clone();

            let dm = loom::thread::spawn(move || {
                let gsn = gsn_dm.load(Ordering::Acquire);
                if gsn >= 100 {
                    resp_status_dm.store(1, Ordering::Relaxed);
                    resp_seq_dm.store(1, Ordering::Release);
                }
            });

            let io = loom::thread::spawn(move || {
                let seq = response_seq.load(Ordering::Acquire);
                if seq == 1 {
                    let status = response_status.load(Ordering::Relaxed);
                    assert_eq!(status, 1, "response должен быть COMMITTED");

                    let disk = disk_written.load(Ordering::Relaxed);
                    assert_eq!(disk, 1, "данные должны быть на диске");
                }
            });

            ls_writer.join().unwrap();
            dm.join().unwrap();
            io.join().unwrap();
        });
    }

    #[test]
    fn loom_extended_two_flushes_transitivity() {
        loom::model(|| {
            let flush1_done = Arc::new(AtomicU64::new(0));
            let flush2_done = Arc::new(AtomicU64::new(0));
            let committed_gsn = Arc::new(AtomicU64::new(0));
            let response_seq = Arc::new(AtomicU64::new(0));

            let f1 = flush1_done.clone();
            let f2 = flush2_done.clone();
            let gsn_w = committed_gsn.clone();

            let ls_writer = loom::thread::spawn(move || {
                f1.store(1, Ordering::Relaxed);
                gsn_w.store(100, Ordering::Release);

                f2.store(1, Ordering::Relaxed);
                gsn_w.store(200, Ordering::Release);
            });

            let gsn_dm = committed_gsn.clone();
            let resp_dm = response_seq.clone();

            let dm = loom::thread::spawn(move || {
                let gsn = gsn_dm.load(Ordering::Acquire);
                if gsn >= 200 {
                    resp_dm.store(1, Ordering::Release);
                }
            });

            let io = loom::thread::spawn(move || {
                let seq = response_seq.load(Ordering::Acquire);
                if seq == 1 {
                    assert_eq!(flush1_done.load(Ordering::Relaxed), 1);
                    assert_eq!(flush2_done.load(Ordering::Relaxed), 1);
                }
            });

            ls_writer.join().unwrap();
            dm.join().unwrap();
            io.join().unwrap();
        });
    }
}