use std::thread;

/// How many pausing steps a waiter takes before it starts yielding.
///
/// Step `n` issues `1 << n` pause instructions in a row without looking at
/// anything in between, so the last step decides how long the waiter can be
/// blind and how long it holds a core it is not using. The count follows
/// from where those two costs cross: keep doubling while one more group of
/// pauses is still cheaper than the yield it stands in for, and stop at the
/// step where it is not.
///
/// Measured on the development machine (AMD Ryzen 7 260): one pause costs
/// 13.2 ns and one `yield_now` costs 566 ns, so a yield buys about 43
/// pauses. Step 5 issues 32 of them — 423 ns, still the cheaper move; step
/// 6 would issue 64 — 845 ns, already dearer than simply handing the core
/// over. Steps 0 through 5 is six of them, and the whole pausing phase then
/// lasts 63 pauses, a little over 800 ns, before the first yield.
///
/// **For defence:** the earlier value of 10 was picked without measuring
/// and was wrong in a way worth naming — its last step alone spun for
/// 6.8 µs, roughly twelve times the cost of the yield it was avoiding,
/// while never once checking whether the room it was waiting for had
/// already appeared.
const SPIN_STEPS: u32 = 6;

/// What a single wait step actually did.
///
/// A waiter that may give up counts only the yielding steps, and this is
/// what tells the two apart. The pausing steps are fixed by construction —
/// there are always `SPIN_STEPS` of them and they always run — so they
/// carry no information about whether the consumer is stuck; the yields do,
/// because each one means the scheduler ran someone else and the ring was
/// still full when this thread came back.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WaitStep {
    /// The step issued pause instructions and kept the core.
    Spun,
    /// The step handed the core to the scheduler.
    Yielded,
}

/// A wait that gets more expensive the longer it lasts.
///
/// The shape is fixed by what the two kinds of step cost. A pause
/// instruction is nearly free and keeps the waiting thread's cache lines
/// warm, so it wins outright when the wait turns out to be short — which is
/// the common case, because the consumer that must move is usually already
/// running. A yield costs a trip through the scheduler and gives up the
/// core, which is a loss on a short wait and the only correct behaviour on
/// a long one, where spinning buys nothing and denies the core to whoever
/// could actually make progress.
///
/// Since the waiter cannot know in advance which case it is in, it starts
/// cheap and grows: doubling the pause count each step reaches the
/// expensive kind of step after a number of steps proportional to the
/// logarithm of the wait, while keeping the total time spent pausing within
/// a small factor of the shortest wait that would have needed it.
pub struct SpinWait {
    step: u32,
}

/// A wait that gives up after a fixed number of yields.
///
/// **For defence:** the bound is a count and not a duration, and that is a
/// deliberate choice against the obvious one. A clock-based bound would
/// read `clock_gettime` through the vDSO on a path inside the hottest
/// component in the system, and — worse — it would make the outcome depend
/// on how the machine's clock and scheduler happened to line up, so the
/// same batch could be admitted on one run and refused on the next with
/// nothing about the batch having changed. A ledger that answers
/// differently on identical input has given up a property it cannot buy
/// back. A count costs one register decrement, gives the same answer every
/// time, and states the thing that actually matters more directly: the
/// consumer did not free a slot across this many scheduler round-trips.
pub struct BoundedSpinWait {
    spin_wait: SpinWait,
    yields_left: u32,
}

impl Default for SpinWait {
    fn default() -> Self {
        Self::new()
    }
}

impl SpinWait {
    pub const fn new() -> Self {
        Self { step: 0 }
    }

    /// Takes one wait step and reports what it did.
    #[inline]
    pub fn wait(&mut self) -> WaitStep {
        if self.step < SPIN_STEPS {
            for _ in 0..(1u32 << self.step) {
                std::hint::spin_loop();
            }
            self.step += 1;
            return WaitStep::Spun;
        }

        thread::yield_now();
        WaitStep::Yielded
    }
}

impl BoundedSpinWait {
    /// Starts a wait that gives up after `max_yields` yielding steps.
    pub const fn new(max_yields: u32) -> Self {
        Self {
            spin_wait: SpinWait::new(),
            yields_left: max_yields,
        }
    }

    /// Takes one wait step and reports whether waiting further is still
    /// permitted. `false` means the allowance is used up and the caller
    /// must give up.
    #[inline]
    pub fn wait(&mut self) -> bool {
        if self.spin_wait.wait() == WaitStep::Spun {
            return true;
        }

        self.yields_left = self.yields_left.saturating_sub(1);
        self.yields_left > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn waiter_pauses_before_it_yields() {
        let mut spin_wait = SpinWait::new();
        for _ in 0..SPIN_STEPS {
            assert_eq!(spin_wait.wait(), WaitStep::Spun);
        }
        assert_eq!(spin_wait.wait(), WaitStep::Yielded);
    }

    #[test]
    fn waiter_keeps_yielding_once_it_has_started() {
        let mut spin_wait = SpinWait::new();
        for _ in 0..SPIN_STEPS {
            spin_wait.wait();
        }
        for _ in 0..4 {
            assert_eq!(spin_wait.wait(), WaitStep::Yielded);
        }
    }

    #[test]
    fn bounded_wait_permits_every_pausing_step() {
        let mut wait = BoundedSpinWait::new(1);
        for _ in 0..SPIN_STEPS {
            assert!(wait.wait(), "a pausing step must never spend the allowance");
        }
    }

    #[test]
    fn bounded_wait_gives_up_once_the_yields_are_used_up() {
        let mut wait = BoundedSpinWait::new(1);
        for _ in 0..SPIN_STEPS {
            wait.wait();
        }
        assert!(!wait.wait(), "the last permitted yield must stop the wait");
    }

    #[test]
    fn bounded_wait_spends_one_yield_per_yielding_step() {
        const MAX_YIELDS: u32 = 4;

        let mut wait = BoundedSpinWait::new(MAX_YIELDS);
        for _ in 0..SPIN_STEPS {
            wait.wait();
        }
        for _ in 0..MAX_YIELDS - 1 {
            assert!(wait.wait());
        }
        assert!(!wait.wait());
    }

    #[test]
    fn bounded_wait_gives_the_same_answer_every_run() {
        for _ in 0..64 {
            let mut wait = BoundedSpinWait::new(2);
            let mut steps = 0;
            while wait.wait() {
                steps += 1;
            }
            assert_eq!(
                steps,
                SPIN_STEPS + 1,
                "a counted bound must not depend on how the run was scheduled",
            );
        }
    }
}
