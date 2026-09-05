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
            entries: HashMap::new()
        }
    }

    /// Look up `ls_sign_path` in the cache; if absent, verify and insert.
    ///
    /// Uses `HashMap::entry` for a single hash probe rather than the
    /// `contains_key` + index double-probe pattern. `verify_ls_signatures`
    /// is called lazily only on cache miss (identical semantics to the
    /// prior `contains_key` guard).
    pub fn verify_or_cached(&mut self, ls_sign_path: &str) -> SignatureVerifyResult {
        self.entries.entry(ls_sign_path.to_string())
            .or_insert_with(
                || CacheEntry {
                    verified_at: Instant::now(),
                    result: verify_ls_signatures(ls_sign_path),
                }
            ).result.clone()
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