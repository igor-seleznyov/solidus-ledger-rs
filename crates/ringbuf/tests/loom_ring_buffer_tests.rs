#[cfg(feature = "loom")]
#[cfg(test)]
mod loom_ring_buffer_tests {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicU64, Ordering, fence};

    #[test]
    fn loom_rb_sequence_barrier() {
        loom::model(|| {
            let sequence = Arc::new(AtomicU64::new(0));
            let value = Arc::new(AtomicU64::new(0));

            let seq_w = sequence.clone();
            let val_w = value.clone();

            let writer = loom::thread::spawn(move || {
                val_w.store(42, Ordering::Relaxed);
                fence(Ordering::Release);
                seq_w.store(1, Ordering::Relaxed);
            });

            let reader = loom::thread::spawn(move || {
                let seq = sequence.load(Ordering::Relaxed);
                if seq == 1 {
                    fence(Ordering::Acquire);
                    assert_eq!(value.load(Ordering::Relaxed), 42);
                }
            });

            writer.join().unwrap();
            reader.join().unwrap();
        });
    }

    #[test]
    fn loom_mpsc_two_writers() {
        loom::model(|| {
            let claim_seq = Arc::new(AtomicU64::new(0));
            let slot0_seq = Arc::new(AtomicU64::new(0));
            let slot0_val = Arc::new(AtomicU64::new(0));
            let slot1_seq = Arc::new(AtomicU64::new(0));
            let slot1_val = Arc::new(AtomicU64::new(0));

            let cs1 = claim_seq.clone();
            let s0s = slot0_seq.clone();
            let s0v = slot0_val.clone();
            let s1s = slot1_seq.clone();
            let s1v = slot1_val.clone();

            let w1 = loom::thread::spawn(move || {
                let claimed = cs1.fetch_add(1, Ordering::Relaxed);
                if claimed == 0 {
                    s0v.store(1000, Ordering::Relaxed);
                    fence(Ordering::Release);
                    s0s.store(1, Ordering::Relaxed);
                } else {
                    s1v.store(1000, Ordering::Relaxed);
                    fence(Ordering::Release);
                    s1s.store(1, Ordering::Relaxed);
                }
            });

            let cs2 = claim_seq.clone();
            let s0s2 = slot0_seq.clone();
            let s0v2 = slot0_val.clone();
            let s1s2 = slot1_seq.clone();
            let s1v2 = slot1_val.clone();

            let w2 = loom::thread::spawn(move || {
                let claimed = cs2.fetch_add(1, Ordering::Relaxed);
                if claimed == 0 {
                    s0v2.store(2000, Ordering::Relaxed);
                    fence(Ordering::Release);
                    s0s2.store(1, Ordering::Relaxed);
                } else {
                    s1v2.store(2000, Ordering::Relaxed);
                    fence(Ordering::Release);
                    s1s2.store(1, Ordering::Relaxed);
                }
            });

            w1.join().unwrap();
            w2.join().unwrap();

            let s0 = slot0_seq.load(Ordering::Relaxed);
            let s1 = slot1_seq.load(Ordering::Relaxed);

            if s0 == 1 {
                fence(Ordering::Acquire);
                let v = slot0_val.load(Ordering::Relaxed);
                assert!(v == 1000 || v == 2000);
            }
            if s1 == 1 {
                fence(Ordering::Acquire);
                let v = slot1_val.load(Ordering::Relaxed);
                assert!(v == 1000 || v == 2000);
            }
        });
    }
}