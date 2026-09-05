//! Model of the bounded claim's arbitration between producers.
//!
//! The bounded claim is the one place in the project where a producer may
//! walk away without taking the turns it asked for, and that is exactly what
//! makes it worth checking exhaustively: a producer that gave up after
//! moving the counter would leave a hole no consumer can ever get past.
//!
//! The model counts retries, exactly as the shipped code does. What matters
//! for correctness is that giving up happens *before* the exchange, never
//! after it.

#[cfg(loom)]
#[cfg(test)]
mod loom_bounded_claim {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicU64, Ordering};

    /// Establishes that the run fits, then takes it with a compare-and-
    /// exchange. Returns the first turn of the run, or `None` once the
    /// allowance is used up.
    fn try_claim(
        claim_seq: &AtomicU64,
        consumer_seq: &AtomicU64,
        capacity: u64,
        count: u64,
        mut attempts_left: u32,
    ) -> Option<u64> {
        let mut start = claim_seq.load(Ordering::Relaxed);

        loop {
            let last_turn = start + count - 1;

            if last_turn >= consumer_seq.load(Ordering::Acquire) + capacity {
                if attempts_left == 0 {
                    return None;
                }
                attempts_left -= 1;
                loom::thread::yield_now();
                start = claim_seq.load(Ordering::Relaxed);
                continue;
            }

            match claim_seq.compare_exchange(
                start,
                start + count,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(start),
                Err(current) => start = current,
            }
        }
    }

    /// Two producers, room for both: each must come away with a turn of its
    /// own, and the counter must land exactly on the total taken.
    #[test]
    fn two_producers_never_share_a_turn() {
        loom::model(|| {
            const CAPACITY: u64 = 2;

            let claim_seq = Arc::new(AtomicU64::new(0));
            let consumer_seq = Arc::new(AtomicU64::new(0));

            let first = {
                let claim_seq = claim_seq.clone();
                let consumer_seq = consumer_seq.clone();
                loom::thread::spawn(move || {
                    try_claim(&claim_seq, &consumer_seq, CAPACITY, 1, 4)
                })
            };

            let second = {
                let claim_seq = claim_seq.clone();
                let consumer_seq = consumer_seq.clone();
                loom::thread::spawn(move || {
                    try_claim(&claim_seq, &consumer_seq, CAPACITY, 1, 4)
                })
            };

            let first = first.join().expect("producer panicked");
            let second = second.join().expect("producer panicked");

            let first = first.expect("the ring has room for both producers");
            let second = second.expect("the ring has room for both producers");

            assert_ne!(first, second, "two producers took the same turn");
            assert!(first < CAPACITY && second < CAPACITY);
            assert_eq!(
                claim_seq.load(Ordering::Relaxed),
                2,
                "the counter must account for exactly the turns handed out",
            );
        });
    }

    /// Room for one, two producers asking: exactly one is served, and the
    /// one that walks away must not have moved the counter — a refused
    /// claim that consumed a turn would wedge the consumer on a slot nobody
    /// will ever fill.
    #[test]
    fn a_refused_claim_leaves_no_hole() {
        loom::model(|| {
            const CAPACITY: u64 = 1;

            let claim_seq = Arc::new(AtomicU64::new(0));
            let consumer_seq = Arc::new(AtomicU64::new(0));

            let first = {
                let claim_seq = claim_seq.clone();
                let consumer_seq = consumer_seq.clone();
                loom::thread::spawn(move || {
                    try_claim(&claim_seq, &consumer_seq, CAPACITY, 1, 2)
                })
            };

            let second = {
                let claim_seq = claim_seq.clone();
                let consumer_seq = consumer_seq.clone();
                loom::thread::spawn(move || {
                    try_claim(&claim_seq, &consumer_seq, CAPACITY, 1, 2)
                })
            };

            let first = first.join().expect("producer panicked");
            let second = second.join().expect("producer panicked");

            let served = [first, second].into_iter().flatten().count() as u64;
            assert_eq!(served, 1, "a ring with one free turn must serve one producer");
            assert_eq!(
                claim_seq.load(Ordering::Relaxed),
                served,
                "the refused producer must not have advanced the counter",
            );
        });
    }

    /// The producer's right to enter a slot comes from the consumer's
    /// release of the previous lap. A claim admitted after the consumer
    /// advanced must see everything the consumer did before advancing.
    #[test]
    fn admission_carries_the_consumer_edge() {
        loom::model(|| {
            const CAPACITY: u64 = 1;

            let claim_seq = Arc::new(AtomicU64::new(1));
            let consumer_seq = Arc::new(AtomicU64::new(0));
            let payload = Arc::new(AtomicU64::new(0));

            let consumer = {
                let consumer_seq = consumer_seq.clone();
                let payload = payload.clone();
                loom::thread::spawn(move || {
                    payload.store(7, Ordering::Relaxed);
                    consumer_seq.store(1, Ordering::Release);
                })
            };

            let producer = {
                let claim_seq = claim_seq.clone();
                let consumer_seq = consumer_seq.clone();
                let payload = payload.clone();
                loom::thread::spawn(move || {
                    if try_claim(&claim_seq, &consumer_seq, CAPACITY, 1, 3).is_some() {
                        assert_eq!(
                            payload.load(Ordering::Relaxed),
                            7,
                            "an admitted claim must see the consumer's last reads",
                        );
                    }
                })
            };

            consumer.join().expect("consumer panicked");
            producer.join().expect("producer panicked");
        });
    }
}
