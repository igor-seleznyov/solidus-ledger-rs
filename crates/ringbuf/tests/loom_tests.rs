#[cfg(loom)]
#[cfg(test)]
mod loom_tests {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicU8, AtomicU64, Ordering};

    #[test]
    fn loom_tht_ready_barrier() {
        loom::model(|| {
            let ready = Arc::new(AtomicU8::new(0));
            let gsn = Arc::new(AtomicU64::new(0));

            let ready_w = ready.clone();
            let gsn_w = gsn.clone();

            let writer = loom::thread::spawn(move || {
                gsn_w.store(42, Ordering::Relaxed);
                ready_w.store(1, Ordering::Release);
            });

            let reader = loom::thread::spawn(move || {
                if ready.load(Ordering::Acquire) == 1 {
                    assert_eq!(gsn.load(Ordering::Relaxed), 42);
                }
            });

            writer.join().unwrap();
            reader.join().unwrap();
        });
    }

    #[test]
    fn loom_tht_ready_barrier_multiple_fields() {
        loom::model(|| {
            let ready = Arc::new(AtomicU8::new(0));
            let transfer_id_hi = Arc::new(AtomicU64::new(0));
            let transfer_id_lo = Arc::new(AtomicU64::new(0));
            let gsn = Arc::new(AtomicU64::new(0));

            let ready_w = ready.clone();
            let id_hi_w = transfer_id_hi.clone();
            let id_lo_w = transfer_id_lo.clone();
            let gsn_w = gsn.clone();

            let writer = loom::thread::spawn(move || {
                id_hi_w.store(0xAAAA, Ordering::Relaxed);
                id_lo_w.store(0xBBBB, Ordering::Relaxed);
                gsn_w.store(100, Ordering::Relaxed);
                ready_w.store(1, Ordering::Release);
            });

            let reader = loom::thread::spawn(move || {
                if ready.load(Ordering::Acquire) == 1 {
                    assert_eq!(transfer_id_hi.load(Ordering::Relaxed), 0xAAAA);
                    assert_eq!(transfer_id_lo.load(Ordering::Relaxed), 0xBBBB);
                    assert_eq!(gsn.load(Ordering::Relaxed), 100);
                }
            });

            writer.join().unwrap();
            reader.join().unwrap();
        });
    }
}