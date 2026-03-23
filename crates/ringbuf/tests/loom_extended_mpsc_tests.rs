#[cfg(feature = "loom")]
#[cfg(test)]
mod loom_extended_mpsc_tests {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicU64, Ordering, fence};

    #[test]
    fn loom_extended_mpsc_two_writers_one_reader() {
        loom::model(|| {
            let claim_seq = Arc::new(AtomicU64::new(0));

            let slot0_seq = Arc::new(AtomicU64::new(0));
            let slot0_val = Arc::new(AtomicU64::new(0));
            let slot1_seq = Arc::new(AtomicU64::new(0));
            let slot1_val = Arc::new(AtomicU64::new(0));

            let cs1 = claim_seq.clone();
            let s0s1 = slot0_seq.clone(); let s0v1 = slot0_val.clone();
            let s1s1 = slot1_seq.clone(); let s1v1 = slot1_val.clone();

            let w1 = loom::thread::spawn(move || {
                let claimed = cs1.fetch_add(1, Ordering::Relaxed);
                if claimed == 0 {
                    s0v1.store(1000, Ordering::Relaxed);
                    s0s1.store(2, Ordering::Release);
                } else {
                    s1v1.store(1000, Ordering::Relaxed);
                    s1s1.store(3, Ordering::Release);
                }
            });

            let cs2 = claim_seq.clone();
            let s0s2 = slot0_seq.clone(); let s0v2 = slot0_val.clone();
            let s1s2 = slot1_seq.clone(); let s1v2 = slot1_val.clone();

            let w2 = loom::thread::spawn(move || {
                let claimed = cs2.fetch_add(1, Ordering::Relaxed);
                if claimed == 0 {
                    s0v2.store(2000, Ordering::Relaxed);
                    s0s2.store(2, Ordering::Release);
                } else {
                    s1v2.store(2000, Ordering::Relaxed);
                    s1s2.store(3, Ordering::Release);
                }
            });

            w1.join().unwrap();
            w2.join().unwrap();

            let s0_seq = slot0_seq.load(Ordering::Acquire);
            if s0_seq == 2 {
                let v = slot0_val.load(Ordering::Relaxed);
                assert!(v == 1000 || v == 2000, "slot0 value must be from writer 1 or 2");
            }

            let s1_seq = slot1_seq.load(Ordering::Acquire);
            if s1_seq == 3 {
                let v = slot1_val.load(Ordering::Relaxed);
                assert!(v == 1000 || v == 2000, "slot1 value must be from writer 1 or 2");
            }

            if s0_seq == 2 && s1_seq == 3 {
                let v0 = slot0_val.load(Ordering::Relaxed);
                let v1 = slot1_val.load(Ordering::Relaxed);
                assert!(v0 == 1000 || v0 == 2000, "slot0 corrupted: {v0}");
                assert!(v1 == 1000 || v1 == 2000, "slot1 corrupted: {v1}");
                assert_ne!(v0, v1, "оба слота от одного writer'а — race на claim");
            }
        });
    }
}