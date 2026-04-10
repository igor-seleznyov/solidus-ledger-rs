use std::collections::HashMap;
use std::time::Instant;
use crate::signature_verifier::{verify_ls_signatures, SignatureVerifyResult};

pub struct SignatureVerificationCache {
    entries: HashMap<String, CacheEntry>,
}

struct CacheEntry {
    verified_at: Instant,
    result: SignatureVerifyResult,
}

impl SignatureVerificationCache {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn verify_or_cached(&mut self, ls_sign_path: &str) -> &SignatureVerifyResult {
        if !self.entries.contains_key(ls_sign_path) {
            let result = verify_ls_signatures(ls_sign_path);
            self.entries.insert(
                ls_sign_path.to_string(),
                CacheEntry {
                    verified_at: Instant::now(),
                    result,
                }
            );
        }
        &self.entries[ls_sign_path].result
    }

    pub fn invalidate(&mut self, ls_sign_path: &str) {
        self.entries.remove(ls_sign_path);
    }

    pub fn invalidate_all(&mut self) {
        self.entries.clear();
    }

    pub fn last_verified(&self, ls_sign_path: &str) -> Option<Instant> {
        self.entries.get(ls_sign_path).map(|entry| entry.verified_at)
    }
}