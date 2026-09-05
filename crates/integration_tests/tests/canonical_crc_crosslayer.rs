//! Cross-layer canonical-CRC contract (step 8-10-6d-consolidated Part 3).
//!
//! Drives the real write -> bytes -> read -> verify path for `repr(C)` structs
//! migrated to the canonical CRC32C triple. No side is mocked: the producer
//! is the struct's own `as_bytes()`, the consumer reads the buffer back into
//! an aligned `Self` (Preference B per I-043) + `verify_checksum()`. A drift
//! between the writer-side CRC input range `[0..SIZE-4)` and the reader-side
//! verification surfaces here.

use storage::ls_sign_file_header::LsSignFileHeader;
use storage::manifest_header::ManifestHeader;
use storage::manifest_entry::ManifestEntry;

/// I-001 cross-layer: an `LsSignFileHeader` written with the new CRC formula
/// reads back and verifies. Reader-side verify MUST be `true` on a valid
/// round-trip — against the live inverted `!=` it would be `false`.
#[test]
fn test_ls_sign_header_crosslayer_roundtrip_verifies() {
    let header = LsSignFileHeader::new(1, 1, 7, [0x11u8; 32], [0x22u8; 32]);

    let bytes: Vec<u8> = header.as_bytes().to_vec();
    assert_eq!(bytes.len(), LsSignFileHeader::SIZE);

    let mut restored = LsSignFileHeader::zeroed();
    restored.as_bytes_mut().copy_from_slice(&bytes[..LsSignFileHeader::SIZE]);
    assert_eq!(restored.magic, storage::ls_sign_file_header::LS_SIGN_FILE_MAGIC);
    assert_eq!(restored.linked_ls_file_seq, 7);
    assert!(restored.verify_checksum());
}

/// I-001 cross-layer: a single corrupted byte in the serialised buffer is
/// detected by the reader-side `verify_checksum`.
#[test]
fn test_ls_sign_header_crosslayer_corruption_detected() {
    let header = LsSignFileHeader::new(1, 1, 7, [0x11u8; 32], [0x22u8; 32]);
    let mut bytes: Vec<u8> = header.as_bytes().to_vec();
    bytes[0] ^= 0xFF;

    let mut restored = LsSignFileHeader::zeroed();
    restored.as_bytes_mut().copy_from_slice(&bytes[..LsSignFileHeader::SIZE]);
    assert!(!restored.verify_checksum());
}

/// I-001 cross-layer: `ManifestHeader` write -> bytes -> read -> verify.
#[test]
fn test_manifest_header_crosslayer_roundtrip_verifies() {
    let header = ManifestHeader::new(3);
    let bytes: Vec<u8> = header.as_bytes().to_vec();
    assert_eq!(bytes.len(), ManifestHeader::SIZE);

    let mut restored = ManifestHeader::zeroed();
    restored.as_bytes_mut().copy_from_slice(&bytes[..ManifestHeader::SIZE]);
    assert_eq!(restored.magic, storage::manifest_header::MANIFEST_HEADER_MAGIC);
    assert_eq!(restored.shard_id, 3);
    assert!(restored.verify_checksum());
}

/// I-001 cross-layer: `ManifestEntry` write -> bytes -> read -> verify, with
/// `filename` now placed BEFORE `checksum` (Part 3.6 layout surgery). Confirms
/// the reader covers the same `[0..SIZE-4)` payload after the field move.
#[test]
fn test_manifest_entry_crosslayer_roundtrip_verifies() {
    let mut entry = ManifestEntry::zeroed();
    entry.file_seq = 5;
    entry.set_filename("ls_20260403-120000-000-0-0.ls");
    entry.fill_checksum();

    let bytes: Vec<u8> = entry.as_bytes().to_vec();
    assert_eq!(bytes.len(), ManifestEntry::SIZE);

    let mut restored = ManifestEntry::zeroed();
    restored.as_bytes_mut().copy_from_slice(&bytes[..ManifestEntry::SIZE]);
    assert_eq!(restored.filename_str(), "ls_20260403-120000-000-0-0.ls");
    assert_eq!(restored.file_seq, 5);
    assert!(restored.verify_checksum());
}
