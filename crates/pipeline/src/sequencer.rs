pub struct Sequencer {
    next_gsn: u64,
}

impl Sequencer {
    pub fn new() -> Self {
        Self { next_gsn: 1 }
    }

    pub fn next(&mut self) -> u64 {
        let gsn = self.next_gsn;
        self.next_gsn += 1;
        gsn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_at_one() {
        let mut seq = Sequencer::new();
        assert_eq!(seq.next(), 1);
    }

    #[test]
    fn monotonically_increasing() {
        let mut seq = Sequencer::new();
        let a = seq.next();
        let b = seq.next();
        let c = seq.next();
        assert_eq!(a, 1);
        assert_eq!(b, 2);
        assert_eq!(c, 3);
    }

    #[test]
    fn thousand_values_no_gaps() {
        let mut seq = Sequencer::new();
        for expected in 1..=1000 {
            assert_eq!(seq.next(), expected);
        }
    }
}