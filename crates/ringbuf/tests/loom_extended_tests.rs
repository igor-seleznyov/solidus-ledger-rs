#[cfg(feature = "loom")]
#[cfg(test)]
mod loom_extended_tests {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicU8, AtomicU64, Ordering, fence};

    #[test]
    fn loom_extended_three_threads_pipeline_actor_dm() {
        loom::model(|| {
            let tht_ready = Arc::new(AtomicU8::new(0));
            let tht_gsn = Arc::new(AtomicU64::new(0));
            let tht_transfer_id_hi = Arc::new(AtomicU64::new(0));
            let tht_transfer_id_lo = Arc::new(AtomicU64::new(0));
            let tht_entries_count = Arc::new(AtomicU8::new(0));

            let coord_sequence = Arc::new(AtomicU64::new(0));
            let coord_result = Arc::new(AtomicU8::new(0));

            let tht_ready_p = tht_ready.clone();
            let tht_gsn_p = tht_gsn.clone();
            let tht_id_hi_p = tht_transfer_id_hi.clone();
            let tht_id_lo_p = tht_transfer_id_lo.clone();
            let tht_ec_p = tht_entries_count.clone();

            let pipeline = loom::thread::spawn(move || {
                tht_gsn_p.store(42, Ordering::Relaxed);
                tht_id_hi_p.store(0xAAAA, Ordering::Relaxed);
                tht_id_lo_p.store(0xBBBB, Ordering::Relaxed);
                tht_ec_p.store(2, Ordering::Relaxed);
                tht_ready_p.store(1, Ordering::Release);
            });

            let coord_seq_a = coord_sequence.clone();
            let coord_res_a = coord_result.clone();

            let actor = loom::thread::spawn(move || {
                coord_res_a.store(1, Ordering::Relaxed);
                coord_seq_a.store(100, Ordering::Release);
            });

            let dm = loom::thread::spawn(move || {
                let seq = coord_sequence.load(Ordering::Acquire);
                if seq == 100 {
                    let result = coord_result.load(Ordering::Relaxed);
                    assert_eq!(result, 1, "COMMIT_OK должен быть виден после acquire(sequence)");

                    let ready = tht_ready.load(Ordering::Acquire);
                    if ready == 1 {
                        let gsn = tht_gsn.load(Ordering::Relaxed);
                        let id_hi = tht_transfer_id_hi.load(Ordering::Relaxed);
                        let id_lo = tht_transfer_id_lo.load(Ordering::Relaxed);
                        let ec = tht_entries_count.load(Ordering::Relaxed);

                        assert_eq!(gsn, 42, "GSN должен быть виден после acquire(ready)");
                        assert_eq!(id_hi, 0xAAAA);
                        assert_eq!(id_lo, 0xBBBB);
                        assert_eq!(ec, 2);
                    }
                }
            });

            pipeline.join().unwrap();
            actor.join().unwrap();
            dm.join().unwrap();
        });
    }

    #[test]
    fn loom_extended_dm_reads_tht_before_coord() {
        loom::model(|| {
            let tht_ready = Arc::new(AtomicU8::new(0));
            let tht_gsn = Arc::new(AtomicU64::new(0));

            let coord_sequence = Arc::new(AtomicU64::new(0));
            let coord_result = Arc::new(AtomicU8::new(0));

            let tht_ready_p = tht_ready.clone();
            let tht_gsn_p = tht_gsn.clone();

            let pipeline = loom::thread::spawn(move || {
                tht_gsn_p.store(42, Ordering::Relaxed);
                tht_ready_p.store(1, Ordering::Release);
            });

            let coord_seq_a = coord_sequence.clone();
            let coord_res_a = coord_result.clone();

            let actor = loom::thread::spawn(move || {
                coord_res_a.store(1, Ordering::Relaxed);
                coord_seq_a.store(100, Ordering::Release);
            });

            let dm = loom::thread::spawn(move || {
                let ready = tht_ready.load(Ordering::Acquire);
                if ready == 1 {
                    assert_eq!(tht_gsn.load(Ordering::Relaxed), 42);

                    let seq = coord_sequence.load(Ordering::Acquire);
                    if seq == 100 {
                        assert_eq!(coord_result.load(Ordering::Relaxed), 1);
                    }
                }
            });

            pipeline.join().unwrap();
            actor.join().unwrap();
            dm.join().unwrap();
        });
    }

    #[test]
    fn loom_extended_full_chain_pipeline_actor_dm() {
        loom::model(|| {
            let tht_ready = Arc::new(AtomicU8::new(0));
            let tht_gsn = Arc::new(AtomicU64::new(0));
            let tht_entries_count = Arc::new(AtomicU8::new(0));

            let partition_seq = Arc::new(AtomicU64::new(0));
            let partition_amount = Arc::new(AtomicU64::new(0));

            let coord_seq = Arc::new(AtomicU64::new(0));
            let coord_msg_type = Arc::new(AtomicU8::new(0));

            let tht_ready_p = tht_ready.clone();
            let tht_gsn_p = tht_gsn.clone();
            let tht_ec_p = tht_entries_count.clone();
            let part_seq_p = partition_seq.clone();
            let part_amt_p = partition_amount.clone();

            let pipeline = loom::thread::spawn(move || {
                tht_gsn_p.store(42, Ordering::Relaxed);
                tht_ec_p.store(2, Ordering::Relaxed);
                tht_ready_p.store(1, Ordering::Release);

                part_amt_p.store(1000, Ordering::Relaxed);
                part_seq_p.store(1, Ordering::Release);
            });

            let part_seq_a = partition_seq.clone();
            let part_amt_a = partition_amount.clone();
            let coord_seq_a = coord_seq.clone();
            let coord_mt_a = coord_msg_type.clone();

            let actor = loom::thread::spawn(move || {
                let seq = part_seq_a.load(Ordering::Acquire);
                if seq == 1 {
                    let amount = part_amt_a.load(Ordering::Relaxed);
                    assert_eq!(amount, 1000);

                    coord_mt_a.store(1, Ordering::Relaxed);
                    coord_seq_a.store(1, Ordering::Release);
                }
            });

            let dm = loom::thread::spawn(move || {
                let cseq = coord_seq.load(Ordering::Acquire);
                if cseq == 1 {
                    assert_eq!(coord_msg_type.load(Ordering::Relaxed), 1);

                    let ready = tht_ready.load(Ordering::Acquire);
                    if ready == 1 {
                        assert_eq!(tht_gsn.load(Ordering::Relaxed), 42);
                        assert_eq!(tht_entries_count.load(Ordering::Relaxed), 2);
                    }
                }
            });

            pipeline.join().unwrap();
            actor.join().unwrap();
            dm.join().unwrap();
        });
    }
}