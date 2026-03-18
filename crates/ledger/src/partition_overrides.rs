use std::collections::HashMap;

pub struct PartitionAssignmentsOverrides {
    overrides: HashMap<[u8; 16], usize>,
}

impl PartitionAssignmentsOverrides {
    pub fn empty() -> Self {
        Self {
            overrides: HashMap::new(),
        }
    }
    
    pub fn from_map(overrides: HashMap<[u8; 16], usize>) -> Self {
        Self { overrides }
    }
    
    pub fn get(&self, account_id: &[u8; 16]) -> Option<usize> {
        self.overrides.get(account_id).copied()
    }
    
    pub fn len(&self) -> usize {
        self.overrides.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_returns_none() {
        let overrides = PartitionAssignmentsOverrides::empty();
        let id = [1u8; 16];
        assert!(overrides.get(&id).is_none());
        assert_eq!(overrides.len(), 0);
    }

    #[test]
    fn from_map_lookup_hit() {
        let mut map = HashMap::new();
        map.insert([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], 5usize);
        map.insert([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2], 10usize);

        let overrides = PartitionAssignmentsOverrides::from_map(map);
        assert_eq!(overrides.len(), 2);
        assert_eq!(
            overrides.get(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
            Some(5),
        );
        assert_eq!(
            overrides.get(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]),
            Some(10),
        );
    }

    #[test]
    fn lookup_miss_returns_none() {
        let mut map = HashMap::new();
        map.insert([0u8; 16], 0usize);

        let overrides = PartitionAssignmentsOverrides::from_map(map);
        let unknown = [1u8; 16];
        assert!(overrides.get(&unknown).is_none());
    }

    #[test]
    fn from_empty_map() {
        let overrides = PartitionAssignmentsOverrides::from_map(HashMap::new());
        assert_eq!(overrides.len(), 0);
        assert!(overrides.get(&[0u8; 16]).is_none());
    }
    
    
}
