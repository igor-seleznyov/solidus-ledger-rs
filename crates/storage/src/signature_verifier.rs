use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use ed25519_dalek::VerifyingKey;
use crate::sig_record::{SigRecord, SIG_RECORD_MAGIC};
use crate::ls_sign_file_header::{LsSignFileHeader, LS_SIGN_FILE_MAGIC};
use crate::signing_state::verify_sig_record;
use sha2::{Sha256, Digest};
use sha2::digest::Update;

#[derive(Debug, PartialEq)]
pub enum SignatureVerifyResult {
    Ok { records_count: u64 },
    SignFileNotFound,
    InvalidHeader(String),
    InvalidSignature { record_index: u64, gsn: u64, reason: String },
    ChainBroken { record_index: u64, gsn: u64 },
    ChecksumMismatch { record_index: u64 },
    ReadError(String),
}

pub fn verify_ls_signatures(ls_sign_path: &str) -> SignatureVerifyResult {
    let mut file = match File::open(ls_sign_path) {
        Ok(file) => file,
        Err(_) => return SignatureVerifyResult::SignFileNotFound,
    };

    let mut header = LsSignFileHeader::zeroed();
    if file.read_exact(
        unsafe { header.as_bytes_mut() }
    ).is_err() {
        return SignatureVerifyResult::ReadError("Failed to read signature file header".to_owned());
    }

    if header.magic != LS_SIGN_FILE_MAGIC {
        return SignatureVerifyResult::InvalidHeader("Invalid magic in header".to_string())
    };

    if !unsafe { header.verify_checksum() } {
        return SignatureVerifyResult::InvalidHeader("Header checksum mismatch".to_string())
    };

    let public_key = match VerifyingKey::from_bytes(&header.public_key) {
        Ok(key) => key,
        Err(error) => {
            return SignatureVerifyResult::InvalidHeader(
                format!("Invalid public key in header: {}", error)
            );
        }
    };

    let mut prev_tx_hash: [u8; 32] = header.genesis_hash;

    let mut offset = LsSignFileHeader::DATA_OFFSET as u64;

    let mut offset = LsSignFileHeader::DATA_OFFSET as u64;
    let mut record_index: u64 = 0;
    let mut buf = [0u8; SigRecord::SIZE];

    loop {
        if file.seek(SeekFrom::Start(offset)).is_err() {
            break;
        }

        match file.read_exact(&mut buf) {
            Ok(()) => {}
            Err(_) => break
        }

        let magic = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        if magic != SIG_RECORD_MAGIC {
            break;
        }

        let mut record = SigRecord::zeroed();
        unsafe {
            std::ptr::copy_nonoverlapping(
                buf.as_ptr(),
                &mut record as *mut SigRecord as *mut u8,
                SigRecord::SIZE,
            );
        }

        if !unsafe { record.verify_checksum() } {
            return SignatureVerifyResult::ChecksumMismatch { record_index };
        }

        if record.prev_tx_hash != prev_tx_hash {
            return SignatureVerifyResult::ChainBroken {
                record_index,
                gsn: record.gsn,
            };
        }

        if !verify_sig_record(&record, &prev_tx_hash, &public_key) {
            return SignatureVerifyResult::InvalidSignature {
                record_index,
                gsn: record.gsn,
                reason: "Ed25519 verification failed".to_string(),
            };
        }

        prev_tx_hash = compute_tx_hash(&record);

        record_index += 1;
        offset += SigRecord::SIZE as u64;
    }

    SignatureVerifyResult::Ok { records_count: record_index }
}

fn compute_tx_hash(record: &SigRecord) -> [u8; 32] {
    let mut hasher = Sha256::new();

    Digest::update(&mut hasher, &record.prev_tx_hash);
    Digest::update(&mut hasher, &record.gsn.to_le_bytes());
    Digest::update(&mut hasher, &record.transfer_id_hi.to_le_bytes());
    Digest::update(&mut hasher, &record.transfer_id_lo.to_le_bytes());
    Digest::update(&mut hasher, &record.postings_hash);

    Digest::update(&mut hasher, &record.signature);

    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signing_state::SigningState;
    use crate::sig_record::SigRecord;
    use crate::ls_sign_file_header::LsSignFileHeader;
    use ed25519_dalek::SigningKey;
    use std::io::Write;
    use pipeline::posting_record::PostingRecord;
    use crate::signature_verification_cache::SignatureVerificationCache;

    fn make_test_dir(name: &str) -> String {
        let dir = format!(
            "/tmp/solidus-test-sigverify-{}-{}",
            name,
            std::process::id(),
        );
        std::fs::create_dir_all(&dir).expect("Failed to create temp dir");
        dir
    }

    fn cleanup(dir: &str) {
        std::fs::remove_dir_all(dir).ok();
    }

    fn make_signing_key() -> SigningKey {
        SigningKey::from_bytes(&[0x42u8; 32])
    }

    fn write_sign_file(
        path: &str,
        key: &SigningKey,
        genesis: [u8; 32],
        postings: &Vec<(u64, u64, u64)>,
    ) {
        let mut file = std::fs::File::create(path).unwrap();

        let verifying_key = key.verifying_key();
        let mut pub_key_bytes = [0u8; 32];
        pub_key_bytes.copy_from_slice(verifying_key.as_bytes());

        let header = LsSignFileHeader::new(
            1,
            0,
            0,
            pub_key_bytes,
            genesis,
        );

        let mut page = vec![0u8; 4096];
        unsafe {
            std::ptr::copy_nonoverlapping(
                header.as_bytes().as_ptr(),
                page.as_mut_ptr(),
                LsSignFileHeader::SIZE,
            );
        }
        file.write_all(&page).unwrap();

        let mut state = SigningState::new(key.clone(), genesis);

        for &(gsn, transfer_id_hi, transfer_id_lo) in postings {
            let mut posting = PostingRecord::new(
                transfer_id_hi,
                transfer_id_lo,
                gsn,
            );
            unsafe { posting.compute_checksum(); }

            let record = state.sign_posting(transfer_id_hi, transfer_id_lo, gsn, 0, &posting);
            let bytes = unsafe { record.as_bytes() };
            file.write_all(bytes).unwrap();
        }

        file.sync_all().unwrap();
    }

    #[test]
    fn verify_valid_signatures() {
        let dir = make_test_dir("valid");
        let path = format!("{}/test.ls.sign", dir);

        let key = make_signing_key();
        let genesis = [0u8; 32];

        write_sign_file(&path, &key, genesis, &vec![
            (100, 0, 1),
            (101, 0, 2),
        ]);

        let result = verify_ls_signatures(&path);
        match result {
            SignatureVerifyResult::Ok { records_count } => {
                assert_eq!(records_count, 2);
            }
            other => panic!("Expected Ok, got {:?}", other),
        }

        cleanup(&dir);
    }

    #[test]
    fn verify_empty_sign_file() {
        let dir = make_test_dir("empty");
        let path = format!("{}/test.ls.sign", dir);

        let key = make_signing_key();
        let genesis = [0u8; 32];

        write_sign_file(&path, &key, genesis, &vec![]);

        let result = verify_ls_signatures(&path);
        match result {
            SignatureVerifyResult::Ok { records_count } => {
                assert_eq!(records_count, 0);
            }
            other => panic!("Expected Ok, got {:?}", other),
        }

        cleanup(&dir);
    }

    #[test]
    fn verify_nonexistent_file() {
        let result = verify_ls_signatures("/tmp/nonexistent-sign-file");
        assert_eq!(result, SignatureVerifyResult::SignFileNotFound);
    }

    #[test]
    fn verify_corrupted_signature() {
        let dir = make_test_dir("corrupt-sig");
        let path = format!("{}/test.ls.sign", dir);

        let key = make_signing_key();
        let genesis = [0u8; 32];

        write_sign_file(&path, &key, genesis, &vec![
            (100, 0, 1),
        ]);

        {
            use std::io::{Seek, SeekFrom};
            let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            let sig_offset = 4096 + 128;
            f.seek(SeekFrom::Start(sig_offset)).unwrap();
            f.write_all(&[0xFF; 8]).unwrap();
            f.sync_all().unwrap();
        }

        let result = verify_ls_signatures(&path);
        match result {
            SignatureVerifyResult::ChecksumMismatch { record_index } => {
                assert_eq!(record_index, 0);
            }
            SignatureVerifyResult::InvalidSignature { record_index, .. } => {
                assert_eq!(record_index, 0);
            }
            other => panic!("Expected ChecksumMismatch or InvalidSignature, got {:?}", other),
        }

        cleanup(&dir);
    }

    #[test]
    fn cache_avoids_repeated_verification() {
        let dir = make_test_dir("cache");
        let path = format!("{}/test.ls.sign", dir);

        let key = make_signing_key();
        let genesis = [0u8; 32];

        write_sign_file(&path, &key, genesis, &vec![
            (100, 0, 1),
        ]);

        let mut cache = SignatureVerificationCache::new();

        let result = cache.verify_or_cached(&path);
        assert!(matches!(result, SignatureVerifyResult::Ok { records_count: 1 }));
        assert!(cache.last_verified(&path).is_some());

        let result2 = cache.verify_or_cached(&path);
        assert!(matches!(result2, SignatureVerifyResult::Ok { records_count: 1 }));

        cache.invalidate(&path);
        assert!(cache.last_verified(&path).is_none());

        let result3 = cache.verify_or_cached(&path);
        assert!(matches!(result3, SignatureVerifyResult::Ok { records_count: 1 }));

        cleanup(&dir);
    }

    #[test]
    fn verify_chain_of_three_records() {
        let dir = make_test_dir("chain-3");
        let path = format!("{}/test.ls.sign", dir);

        let key = make_signing_key();
        let genesis = [0xABu8; 32];

        write_sign_file(&path, &key, genesis, &vec![
            (1, 0, 1),
            (2, 0, 2),
            (3, 0, 3),
        ]);

        let result = verify_ls_signatures(&path);
        match result {
            SignatureVerifyResult::Ok { records_count } => {
                assert_eq!(records_count, 3);
            }
            other => panic!("Expected Ok with 3 records, got {:?}", other),
        }

        cleanup(&dir);
    }
}