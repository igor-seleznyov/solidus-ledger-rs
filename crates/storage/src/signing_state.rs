use ed25519_dalek::{SigningKey, Signer, VerifyingKey, Signature, Verifier};
use sha2::{ Sha256, Digest };
use sha2::digest::DynDigest;
use pipeline::posting_record::PostingRecord;
use crate::sig_record::SigRecord;

#[repr(C)]
struct SignedContent {
    prev_tx_hash: [u8; 32],
    gsn: u64,
    transfer_id_hi: u64,
    transfer_id_lo: u64,
    postings_hash: [u8; 32],
}

pub struct SigningState {
    key: SigningKey,
    last_tx_hash: [u8; 32],
}

impl SigningState {
    pub const ALGORITHM_ED25519: u8 = 0;

    pub fn new(key: SigningKey, genesis_hash: [u8; 32]) -> Self {
        Self {
            key,
            last_tx_hash: genesis_hash,
        }
    }

    pub fn restore(key: SigningKey, last_tx_hash: [u8; 32]) -> Self {
        Self {
            key,
            last_tx_hash,
        }
    }

    pub fn last_tx_hash(&self) -> &[u8; 32] {
        &self.last_tx_hash
    }

    pub fn public_key_bytes(&self) -> [u8; 32] {
        self.key.verifying_key().to_bytes()
    }

    pub fn sign_posting(
        &mut self,
        transfer_id_hi: u64,
        transfer_id_lo: u64,
        gsn: u64,
        ls_offset: u64,
        posting: &PostingRecord,
    ) -> SigRecord {
        let prev_hash = self.last_tx_hash;

        let posting_bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(
                posting as *const PostingRecord as *const u8,
                PostingRecord::SIZE,
            )
        };

        let mut hash_hasher = Sha256::new();
        Digest::update(&mut hash_hasher, posting_bytes);
        let postings_hash: [u8; 32] = hash_hasher.finalize().into();

        let content = SignedContent {
            prev_tx_hash: prev_hash,
            gsn,
            transfer_id_hi,
            transfer_id_lo,
            postings_hash,
        };

        let content_bytes = unsafe {
            std::slice::from_raw_parts(
                &content as *const SignedContent as *const u8,
                std::mem::size_of::<SignedContent>(),
            )
        };

        let signature = self.key.sign(content_bytes);
        let signature_bytes = signature.to_bytes();

        let mut chain_hasher = Sha256::new();
        Digest::update(&mut chain_hasher, content_bytes);
        Digest::update(&mut chain_hasher, &signature_bytes);
        self.last_tx_hash = chain_hasher.finalize().into();

        let mut record = SigRecord::zeroed();
        record.transfer_id_hi = transfer_id_hi;
        record.transfer_id_lo = transfer_id_lo;
        record.gsn = gsn;
        record.ls_offset = ls_offset;
        record.postings_count = 1;
        record.algorithm = SigningState::ALGORITHM_ED25519;
        record.prev_tx_hash = prev_hash;
        record.postings_hash = postings_hash;
        record.signature = signature_bytes;

        record.set_magic();

        unsafe { record.compute_checksum(); }

        record
    }

    pub fn sign_transfer(
        &mut self,
        transfer_id_hi: u64,
        transfer_id_lo: u64,
        gsn: u64,
        ls_offset: u64,
        postings: &[PostingRecord],
    ) -> SigRecord {
        let prev_hash = self.last_tx_hash;
        let postings_hash = self.compute_postings_hash(postings);

        let content = SignedContent {
            prev_tx_hash: prev_hash,
            gsn,
            transfer_id_hi,
            transfer_id_lo,
            postings_hash,
        };

        let content_bytes = unsafe {
            std::slice::from_raw_parts(
                &content as *const SignedContent as *const u8,
                std::mem::size_of::<SignedContent>(),
            )
        };

        let signature = self.key.sign(content_bytes);
        let signature_bytes = signature.to_bytes();

        let mut hasher = Sha256::new();
        Digest::update(&mut hasher, content_bytes);
        Digest::update(&mut hasher, &signature_bytes);
        self.last_tx_hash = hasher.finalize().into();

        let mut record = SigRecord::zeroed();
        record.transfer_id_hi = transfer_id_hi;
        record.transfer_id_lo = transfer_id_lo;
        record.gsn = gsn;
        record.ls_offset = ls_offset;
        record.postings_count = postings.len() as u8;

        record.prev_tx_hash = prev_hash;

        record.postings_hash = postings_hash;
        record.signature = signature_bytes;

        unsafe { record.compute_checksum(); }

        record
    }

    fn compute_postings_hash(&self, postings: &[PostingRecord]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        for posting in postings {
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    posting as *const PostingRecord as *const u8,
                    PostingRecord::SIZE
                )
            };
            Digest::update(&mut hasher, bytes);
        }
        hasher.finalize().into()
    }
}

pub fn verify_sig_record(
    record: &SigRecord,
    prev_tx_hash_full: &[u8; 32],
    public_key: &VerifyingKey,
) -> bool {
    let content = SignedContent {
        prev_tx_hash: *prev_tx_hash_full,
        gsn: record.gsn,
        transfer_id_hi: record.transfer_id_hi,
        transfer_id_lo: record.transfer_id_lo,
        postings_hash: record.postings_hash,
    };

    let content_bytes = unsafe {
        std::slice::from_raw_parts(
            &content as *const SignedContent as *const u8,
            std::mem::size_of::<SignedContent>(),
        )
    };

    let signature = Signature::from_bytes(&record.signature);

    public_key.verify(content_bytes, &signature).is_ok()
}


#[cfg(test)]
mod tests {
    use super::*;

    fn generate_signing_key() -> SigningKey {
        let secret = [0x42u8; 32];
        SigningKey::from(&secret)
    }

    fn generate_another_signing_key() -> SigningKey {
        let secret = [0x74u8; 32];
        SigningKey::from(&secret)
    }

    fn make_signing_state() -> SigningState {
        let key = generate_signing_key();
        let genesis = [0xABu8; 32];
        SigningState::new(key, genesis)
    }

    fn make_posting(gsn: u64, amount: i64) -> PostingRecord {
        let mut p = PostingRecord::zeroed();
        p.gsn = gsn;
        p.amount = amount;
        p.transfer_id_hi = 0;
        p.transfer_id_lo = gsn;
        p
    }

    #[test]
    fn sign_produces_valid_sig_record() {
        let mut state = make_signing_state();
        let postings = vec![make_posting(100, 500), make_posting(100, -500)];

        let record = state.sign_transfer(0, 1, 100, 0, &postings);

        assert_eq!(record.transfer_id_lo, 1);
        assert_eq!(record.gsn, 100);
        assert_eq!(record.postings_count, 2);
        assert_ne!(record.signature, [0u8; 64]);
        assert_ne!(record.postings_hash, [0u8; 32]);

        unsafe {
            assert!(record.verify_checksum());
        }
    }

    #[test]
    fn chain_advances_prev_tx_hash() {
        let mut state = make_signing_state();
        let postings = vec![make_posting(100, 500)];

        let hash_before = *state.last_tx_hash();
        let record1 = state.sign_transfer(0, 1, 100, 0, &postings);
        let hash_after_1 = *state.last_tx_hash();

        assert_ne!(hash_before, hash_after_1);

        let record2 = state.sign_transfer(0, 2, 200, 128, &postings);
        let hash_after_2 = *state.last_tx_hash();

        assert_ne!(hash_after_1, hash_after_2);

        assert_eq!(record2.prev_tx_hash, hash_after_1);
    }

    #[test]
    fn verify_signature() {
        let key = generate_signing_key();
        let genesis = [0xCDu8; 32];
        let mut state = SigningState::new(key.clone(), genesis);
        let public_key = key.verifying_key();

        let postings = vec![make_posting(100, 500), make_posting(100, -500)];
        let record = state.sign_transfer(0, 1, 100, 0, &postings);

        assert!(verify_sig_record(&record, &genesis, &public_key));
    }

    #[test]
    fn verify_chain_of_two() {
        let key = generate_signing_key();
        let genesis = [0u8; 32];
        let mut state = SigningState::new(key.clone(), genesis);
        let public_key = key.verifying_key();

        let postings = vec![make_posting(100, 500)];

        let record1 = state.sign_transfer(0, 1, 100, 0, &postings);
        assert!(verify_sig_record(&record1, &genesis, &public_key));

        let mut content1 = [0u8; 88];
        content1[0..32].copy_from_slice(&genesis);
        content1[32..40].copy_from_slice(&100u64.to_le_bytes());
        content1[40..48].copy_from_slice(&0u64.to_le_bytes());
        content1[48..56].copy_from_slice(&1u64.to_le_bytes());
        content1[56..88].copy_from_slice(&record1.postings_hash);

        let mut hasher = Sha256::new();
        Digest::update(&mut hasher, content1);
        Digest::update(&mut hasher, &record1.signature);
        let hash_after_1: [u8; 32] = hasher.finalize().into();

        let record2 = state.sign_transfer(0, 2, 200, 128, &postings);
        assert!(verify_sig_record(&record2, &hash_after_1, &public_key));
    }

    #[test]
    fn tampered_sig_fails_verification() {
        let key = generate_signing_key();
        let genesis = [0u8; 32];
        let mut state = SigningState::new(key.clone(), genesis);
        let public_key = key.verifying_key();

        let postings = vec![make_posting(100, 500)];
        let mut record = state.sign_transfer(0, 1, 100, 0, &postings);

        record.gsn = 999;
        assert!(!verify_sig_record(&record, &genesis, &public_key));
    }

    #[test]
    fn wrong_prev_hash_fails_verification() {
        let key = generate_signing_key();
        let genesis = [0u8; 32];
        let mut state = SigningState::new(key.clone(), genesis);
        let public_key = key.verifying_key();

        let postings = vec![make_posting(100, 500)];
        let record = state.sign_transfer(0, 1, 100, 0, &postings);

        let wrong_hash = [0xFFu8; 32];
        assert!(!verify_sig_record(&record, &wrong_hash, &public_key));
    }

    #[test]
    fn different_keys_fail_verification() {
        let key1 = generate_signing_key();
        let key2 = generate_another_signing_key();
        let genesis = [0u8; 32];
        let mut state = SigningState::new(key1, genesis);

        let postings = vec![make_posting(100, 500)];
        let record = state.sign_transfer(0, 1, 100, 0, &postings);

        let wrong_public = key2.verifying_key();
        assert!(!verify_sig_record(&record, &genesis, &wrong_public));
    }

    #[test]
    fn restore_continues_chain() {
        let key = generate_signing_key();
        let genesis = [0u8; 32];
        let mut state = SigningState::new(key.clone(), genesis);

        let postings = vec![make_posting(100, 500)];
        state.sign_transfer(0, 1, 100, 0, &postings);

        let saved_hash = *state.last_tx_hash();

        let mut restored = SigningState::restore(key, saved_hash);
        let record = restored.sign_transfer(0, 2, 200, 128, &postings);

        assert_eq!(record.prev_tx_hash, saved_hash);
    }
}