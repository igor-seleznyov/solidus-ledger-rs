use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::collections::HashSet;
use common::crc32c::crc32c;
use crate::time_utils::{now_ns, format_utc_timestamp};


/// File-header magic: "SLTSPGLr" — Solidus Ledger Tampering Segment Page Global Log record.
pub const TAMPERING_LOG_MAGIC: u64 = 0x534C545350474C72;
pub const TAMPERING_LOG_VERSION: u32 = 2;

/// Per-entry magic: "TLGE" — Tampering Log Global Entry.
pub const TAMPERING_ENTRY_MAGIC: u32 = 0x544C4745;

/// Bad-entry tolerance per I-041 §9: skip up to 16 consecutive bad-CRC entries
/// before stopping the scan. Absorbs a single bad sector without losing the
/// rest of the log.
const MAX_CONSECUTIVE_BAD_ENTRIES: usize = 16;

/// All-zero `instance_id` sentinel — disables the H2 planted-file check on
/// either side of the comparison. Distinguished from `_reserved` zero-padding:
/// this is a *semantic* zero (check disabled), not a structural zero.
///
/// See ADR-017 §Amendment 2026-05-09.
pub const ZERO_INSTANCE_ID: [u8; 16] = [0u8; 16];


/// Recovery-path error type for `TamperingLog`.
///
/// Separates an ordinary I/O failure (`Io`) from a deliberate security
/// halt (`SecurityHalt`). The caller — ultimately `src/main.rs` —
/// reacts to each differently: an I/O error may be transient and is
/// reported as a generic startup failure; a `SecurityHalt` is a
/// fail-stop that demands operator investigation before the ledger may
/// start, and gets a security-framed FATAL log line.
///
/// `SecurityHalt` is raised by `load_compromised_set` /
/// `scan_segment_into` when the CURRENT (live, never-rotated)
/// `tampering.log` segment fails the variant-C identity or
/// header-integrity check. See ADR-017 §Amendment 2026-05-18,
/// I-041 §11, I-042 §9, ADR-018 §Failure-mode handling per phase (E9).
#[derive(Debug)]
pub enum TamperingLogError {
    /// An ordinary filesystem / I/O failure on the recovery cold path.
    Io(std::io::Error),
    /// The current TamperingLog segment failed a variant-C security
    /// check; recovery must halt. `reason` is a security-framed,
    /// human-readable message suitable for a FATAL log line.
    SecurityHalt { reason: String },
}

impl std::fmt::Display for TamperingLogError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "tampering-log I/O error: {error}"),
            Self::SecurityHalt { reason } => {
                write!(formatter, "tampering-log security halt: {reason}")
            }
        }
    }
}

impl std::error::Error for TamperingLogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::SecurityHalt { .. } => None,
        }
    }
}

impl From<std::io::Error> for TamperingLogError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Taxonomy of tampering events per ADR-017 §Decision.
///
/// Kinds 1..127 are ledger-scope. Kinds 128..255 are reserved for
/// Phase 3 / external Guard use (Meta Chain attestation events).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TamperingKind {
    Signature           = 1,
    Crc                 = 2,
    Deleted             = 3,
    ImmutableRemoved    = 4,
    Replaced            = 5,
    Moved               = 6,
    /// Operator cleared compromise. `operator_id` is populated.
    Restored            = 7,
    /// Forensic CLI bypass invocation. `operator_id` is populated.
    ForensicAccess      = 8,
    /// Recovery-time rejection of a TamperingLog archive segment whose
    /// header `instance_id` did not match the server's configured
    /// value. Written by FileWatcher startup replay; `operator_id` is
    /// zero. See ADR-017 Amendment 2026-05-10c.
    SegmentRejected     = 9,
}

/// Canonical conversion from on-disk discriminant byte. Returns `Err(())`
/// for unrecognised values; callers treat unknown kinds as a conservative
/// compromise indicator (insert into `CompromisedFileSet`).
impl TryFrom<u8> for TamperingKind {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, ()> {
        match value {
            1 => Ok(Self::Signature),
            2 => Ok(Self::Crc),
            3 => Ok(Self::Deleted),
            4 => Ok(Self::ImmutableRemoved),
            5 => Ok(Self::Replaced),
            6 => Ok(Self::Moved),
            7 => Ok(Self::Restored),
            8 => Ok(Self::ForensicAccess),
            9 => Ok(Self::SegmentRejected),
            _ => Err(()),
        }
    }
}

/// Detection source per ADR-017 §E1.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DetectionSource {
    Inotify             = 1,
    PeriodicRecheck     = 2,
    VerifyAtFirstOpen   = 3,
    Startup             = 4,
    /// Restored / ForensicAccess events triggered by operator CLI.
    OperatorCli         = 5,
}

impl TryFrom<u8> for DetectionSource {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, ()> {
        match value {
            1 => Ok(Self::Inotify),
            2 => Ok(Self::PeriodicRecheck),
            3 => Ok(Self::VerifyAtFirstOpen),
            4 => Ok(Self::Startup),
            5 => Ok(Self::OperatorCli),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum SegmentFileKind {
    Current,
    Archive
}


/// 64-byte segment header per ADR-017 §Decision (as amended 2026-05-09).
///
/// `checksum` is always the last field at offset 60 = SIZE - 4 (I-001).
/// CRC32C covers bytes `[0..60)`. `_reserved` and `_pad1` are zero-
/// written by every constructor so the CRC is deterministic (I-023).
#[repr(C, align(64))]
#[derive(Clone, Copy)]
pub struct TamperingLogFileHeader {
    /// TAMPERING_LOG_MAGIC — self-describing segment identifier.
    pub magic: u64,
    pub format_version: u32,
    pub _pad1: u32,
    /// Unique identifier for the server instance that wrote this segment.
    ///
    /// Verified at startup against `config.storage.file-protection.instance-id`.
    /// A mismatch causes the segment to be skipped — it belongs to a different
    /// instance and could poison the local `CompromisedFileSet` (H2 defence).
    /// Zero value on either side disables the check (backward compatibility).
    ///
    /// **For defence:** zero disables rather than hard-errors for backward
    /// compatibility — existing deployments that have not yet set `instance-id`
    /// must continue to function without skipping all their logs. `[u8; 16]`
    /// is chosen over `hash(hostname)` because hostnames are not stable
    /// (containers, renames); a UUID v7 assigned by the operator is immutable
    /// per-instance and fits in 16 bytes with 128-bit collision resistance.
    ///
    /// See ADR-017 §Amendment 2026-05-09 for rationale.
    pub instance_id: [u8; 16],
    /// Monotonically increasing segment counter across rotations.
    pub file_seq: u64,
    pub created_at_ns: u64,
    /// Phase 3 forward-compat reservation per I-023 (I-023 table updated round 2).
    ///
    /// 12 bytes = 8 B (`meta_chain_seq_at_creation: u64`, Phase 3 Meta Chain
    /// cross-reference) + 4 B alignment buffer. See I-023 and ADR-017 §Amendment
    /// 2026-05-09 for the reservation rationale.
    pub _reserved: [u8; 12],
    /// CRC32C over bytes `[0..SIZE-4)` = `[0..60)`. Always last field per I-001.
    pub checksum: u32,
}
const _: () = assert!(
    std::mem::size_of::<TamperingLogFileHeader>()
        == size_of::<u64>() * 3
                + size_of::<u32>() * 3
                + size_of::<[u8; 16]>()
                + size_of::<[u8; 12]>(),
    "TamperingLogFileHeader is larger than its fields: the compiler inserted alignment \
     padding. Declare it as an explicit field so the layout is stated, and \
     so the checksum stays the record's final bytes",
);


const _: () = assert!(
    std::mem::offset_of!(TamperingLogFileHeader, checksum)
        == TamperingLogFileHeader::SIZE - 4,
    "I-001: checksum must be last field in TamperingLogFileHeader"
);

impl TamperingLogFileHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    /// All-zero instance for the Preference-B read shape (I-043): the caller
    /// fills it via `read_exact(header.as_bytes_mut())`, so the bytes land in
    /// their final aligned home with no intermediate buffer and no
    /// `read_unaligned` copy.
    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }

    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self as *mut Self as *mut u8,
                Self::SIZE
            )
        }
    }

    /// Constructs a new header for a fresh segment.
    ///
    /// `instance_id`: from `config.storage.instance_id` or `ZERO_INSTANCE_ID` when
    /// instance identity is not configured. `ZERO_INSTANCE_ID` disables the H2 check.
    pub fn new(file_seq: u64, instance_id: [u8; 16]) -> Self {
        let mut h = Self {
            magic: TAMPERING_LOG_MAGIC,
            format_version: TAMPERING_LOG_VERSION,
            _pad1: 0,
            instance_id,
            file_seq,
            created_at_ns: now_ns(),
            _reserved: [0; 12],
            checksum: 0,
        };
        h.checksum = h.compute_checksum();
        h
    }

    /// CRC32C over header bytes `[0..SIZE-4)`, excluding the `checksum` field.
    ///
    /// Two invariants make the single `unsafe` line sound:
    /// (1) `self` is fully initialized — every constructor zero-fills `_pad1`,
    ///     `_reserved`, and `checksum` before calling this method (I-023 rule).
    /// (2) `offset_of!(checksum) == SIZE - 4` — enforced by the compile-time
    ///     `assert!` immediately after the struct definition (I-001).
    pub fn compute_checksum(&self) -> u32 {
        unsafe {
            crc32c(
                self as *const Self as *const u8,
                Self::SIZE - 4,
            )
        }
    }

    pub fn verify_checksum(&self) -> bool {
        self.checksum == self.compute_checksum()
    }

    /// Writes the header to `file` and calls `fdatasync`.
    pub fn write_to(&self, file: &mut File) -> std::io::Result<()> {
        let bytes = unsafe {
            std::slice::from_raw_parts(self as *const Self as *const u8, Self::SIZE)
        };
        file.write_all(bytes)?;
        file.sync_data()
    }
}


/// 128-byte tampering event record per ADR-017 §Decision.
///
/// Field layout (repr(C), no explicit align):
/// - magic(4) + kind(1) + source(1) + _pad1(2) = 8 B  [offset 0]
/// - file_seq(8)                                = 8 B  [offset 8]
/// - detected_at_ns(8)                          = 8 B  [offset 16]
/// - ls_path(64)                                = 64 B [offset 24]
/// - operator_id(16)                            = 16 B [offset 88]
/// - _reserved(16)                              = 16 B [offset 104] (I-023)
/// - _tail_pad(4)                               = 4 B  [offset 120] (I-001)
/// - checksum(4)                                = 4 B  [offset 124]
/// Total: 128 B. `offset_of!(checksum) = 124 = SIZE - 4`. I-001 satisfied.
///
/// `_tail_pad` absorbs the 4-byte compiler trailing padding that `repr(C)`
/// would otherwise insert after `checksum` (to align `sizeof` to u64=8).
/// Per I-001: when natural alignment causes compiler trailing padding after
/// `checksum`, an explicit `_tail_pad: [u8; N]` MUST be placed before
/// `checksum` so that `offset_of!(checksum) == SIZE - 4` holds and the
/// CRC formula `[0..SIZE-4)` is unambiguous.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TamperingLogEntry {
    /// TAMPERING_ENTRY_MAGIC = 0x544C4745 ("TLGE"). Entry sync marker.
    pub magic: u32,
    pub kind: u8,
    pub source: u8,
    pub _pad1: [u8; 2],
    pub file_seq: u64,
    pub detected_at_ns: u64,
    /// Null-padded inline path of the affected file (basename or relative path).
    /// Full path reconstructed at runtime from `watch_directory + ls_path`.
    pub ls_path: [u8; 64],
    /// Zero for detection kinds (1–6). Populated for Restored / ForensicAccess.
    pub operator_id: [u8; 16],
    /// Phase 3 forward-compat reservation (I-023). Zero-initialised by every writer.
    /// Phase 3 planned: `meta_seq: u64` (8 B) + 8 B future (Guard cross-reference).
    pub _reserved: [u8; 16],
    /// Explicit padding per I-001: absorbs the 4-byte compiler trailing alignment
    /// gap so that `checksum` lands at offset 124 = SIZE - 4. Zero-initialised.
    ///
    /// **For defence:** `_tail_pad` is NOT part of the I-023 Phase 3 reservation
    /// budget (`_reserved: [u8; 16]`). It exists solely because `repr(C)` without
    /// `align(N)` would otherwise insert 4 bytes of implicit trailing padding
    /// *after* `checksum`, making `offset_of!(checksum) = 120 ≠ SIZE - 4 = 124`.
    /// The I-001 trailing-pad rule mandates explicit `_tail_pad` before `checksum`
    /// to keep the CRC formula `[0..SIZE-4)` unambiguous.
    pub _tail_pad: [u8; 4],
    /// CRC32C over bytes `[0..SIZE-4)` = `[0..124)`. Always last field per I-001.
    pub checksum: u32,
}
const _: () = assert!(
    std::mem::size_of::<TamperingLogEntry>()
        == size_of::<u32>() * 2
                + size_of::<u8>() * 2
                + size_of::<[u8; 2]>()
                + size_of::<u64>() * 2
                + size_of::<[u8; 64]>()
                + size_of::<[u8; 16]>() * 2
                + size_of::<[u8; 4]>(),
    "TamperingLogEntry is larger than its fields: the compiler inserted alignment \
     padding. Declare it as an explicit field so the layout is stated, and \
     so the checksum stays the record's final bytes",
);


const _: () = assert!(
    std::mem::offset_of!(TamperingLogEntry, checksum)
        == TamperingLogEntry::SIZE - 4,
    "I-001: checksum must be at offset SIZE-4 in TamperingLogEntry"
);

impl TamperingLogEntry {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    /// All-zero instance for the Preference-B read shape (I-043); filled by
    /// the caller via `read_exact(entry.as_bytes_mut())` or, for an in-memory
    /// slice of exactly `SIZE` bytes, `copy_from_slice`.
    pub fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }

    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self as *mut Self as *mut u8,
                Self::SIZE
            )
        }
    }

    pub fn new(
        file_seq: u64,
        detected_at_ns: u64,
        kind: TamperingKind,
        source: DetectionSource,
        ls_path: &str,
    ) -> Self {
        Self::new_with_operator(file_seq, detected_at_ns, kind, source, ls_path, [0u8; 16])
    }

    /// Constructs an entry with an explicit `operator_id` (Restored / ForensicAccess).
    pub fn new_with_operator(
        file_seq: u64,
        detected_at_ns: u64,
        kind: TamperingKind,
        source: DetectionSource,
        ls_path_str: &str,
        operator_id: [u8; 16],
    ) -> Self {
        let mut entry = Self {
            magic: TAMPERING_ENTRY_MAGIC,
            kind: kind as u8,
            source: source as u8,
            _pad1: [0; 2],
            file_seq,
            detected_at_ns,
            ls_path: [0; 64],
            operator_id,
            _reserved: [0; 16],
            _tail_pad: [0; 4],
            checksum: 0,
        };
        let bytes = ls_path_str.as_bytes();
        let len = bytes.len().min(64);
        entry.ls_path[..len].copy_from_slice(&bytes[..len]);
        entry.checksum = entry.compute_checksum();
        entry
    }

    /// CRC32C over entry bytes `[0..SIZE-4)` = `[0..124)`.
    ///
    /// `checksum` is at offset 124 = SIZE - 4 (I-001). `_tail_pad` at
    /// offset 120 is zero-initialised and included in the CRC — the CRC
    /// covers every declared field up to (but not including) `checksum`.
    ///
    /// Two invariants make the single `unsafe` line sound:
    /// (1) `self` is fully initialized — `new_with_operator` zero-fills all
    ///     padding fields (`_pad1`, `_tail_pad`, `_reserved`) before calling
    ///     this method. The compile-time struct layout guarantees no hidden
    ///     uninitialised bytes between declared fields in a `repr(C)` struct.
    /// (2) `offset_of!(checksum) == SIZE - 4 = 124` — enforced by the
    ///     compile-time `assert!` immediately after the struct definition (I-001).
    pub fn compute_checksum(&self) -> u32 {
        unsafe {
            crc32c(
                self as *const Self as *const u8,
                Self::SIZE - 4,
            )
        }
    }

    pub fn verify_checksum(&self) -> bool {
        self.checksum == self.compute_checksum()
    }

    /// Returns the null-terminated ls_path as a str slice.
    pub fn ls_path_str(&self) -> Result<&str, std::str::Utf8Error> {
        let end = self.ls_path.iter().position(|&b| b == 0).unwrap_or(64);
        std::str::from_utf8(&self.ls_path[..end])
    }
}


pub struct TamperingLog {
    directory: PathBuf,
    current_file: File,
    current_path: PathBuf,
    current_size: u64,
    max_size_bytes: u64,
    /// Sequence number of the currently-open `tampering.log` segment.
    ///
    /// Cached from the header written (or read) when `open()` constructed
    /// this instance. Used by `rotate()` to name the rotated archive file
    /// `tampering-{current_file_seq}-<ts>.log` so that the archive filename
    /// matches the header `file_seq` field (D-fix-1, 2026-05-09 round 12).
    current_file_seq: u64,
    next_file_seq: u64,
    instance_id: [u8; 16],
}

impl TamperingLog {
    /// Opens a `tampering*.log` segment file for append.
    ///
    /// On Unix the file is opened with `O_NOFOLLOW`: if the path's final
    /// component is a symlink, the open fails with `ELOOP` instead of
    /// following it. This closes a symlink-redirect surface on the
    /// integrity log — an attacker who can plant a symlink at
    /// `tampering.log` could otherwise redirect appends to a file of
    /// their choosing, with no CRC or magic failure to signal it.
    /// `O_NOFOLLOW` rejects only a final-component symlink; for a
    /// regular file the behaviour is identical to a plain open.
    ///
    /// **For defence:** `O_NOFOLLOW` guards the *path resolution* step,
    /// not the file contents. Its semantics — reject a final-component
    /// symlink with `ELOOP`, behave identically to a plain open for a
    /// regular file — are the `open(2)` contract (R-029). The reason to
    /// set it here is the symlink-redirect threat: an attacker with
    /// storage-directory write access who plants a symlink at
    /// `tampering.log` would otherwise silently redirect appends, with no
    /// CRC or magic failure. This is hardening within the ADR-016 layered
    /// file-integrity model, but it is NOT mandated by ADR-016 §Decision
    /// (which specifies `chattr +i` as L1 and inotify as L2, and exempts
    /// `tampering.log` from L1/L3 for circular-trust reasons). It does not
    /// replace the per-entry CRC32C or the `instance_id` provenance check.
    ///
    /// Two scope boundaries are deliberate:
    /// - `O_NOFOLLOW` rejects only a *final-component* symlink. A
    ///   symlinked *parent directory* is still followed; full path
    ///   hardening would need `openat2(RESOLVE_NO_SYMLINKS)`. Replacing
    ///   a parent directory requires directory-rename privilege — a
    ///   higher bar than planting one file — so this residual is
    ///   accepted here.
    /// - This helper is used only for the *write* path
    ///   (`open` / `rotate`). The read-only archive opens
    ///   (`find_max_segment_seq`, `load_compromised_set`,
    ///   `scan_segment_into`) deliberately keep plain `File::open`:
    ///   applying `O_NOFOLLOW` there would make a symlinked archive
    ///   fail to open and be *silently skipped*, dropping its tamper
    ///   events from `CompromisedFileSet` — a worse failure than the
    ///   redirect it would prevent. Read-path archive content integrity
    ///   against an attacker with storage-directory write access is
    ///   delegated, per ADR-017, to the Phase 3 Meta Chain and the
    ///   external Guard (the log cannot self-protect — circular trust).
    fn open_segment_file(path: &Path) -> std::io::Result<File> {
        let mut options = OpenOptions::new();
        options.read(true).create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        options.open(path)
    }

    /// Opens (or creates) the active `tampering.log` in `directory`.
    ///
    /// If the file is new (zero length) a fresh header is written and fsynced.
    /// `instance_id` is written into every header. `max_size_mb` controls
    /// the rotation threshold. The file is opened via `open_segment_file`,
    /// which sets `O_NOFOLLOW` on Unix (GAP1 closed — symlink-redirect
    /// surface removed).
    pub fn open(directory: &str, max_size_mb: usize, instance_id: [u8; 16]) -> std::io::Result<Self> {
        let directory = PathBuf::from(directory);
        std::fs::create_dir_all(&directory)?;

        let current_path = directory.join("tampering.log");
        let mut file = Self::open_segment_file(&current_path)?;

        let file_len = file.metadata()?.len();
        let (current_size, current_file_seq, next_file_seq) = if file_len == 0 {
            let header = TamperingLogFileHeader::new(1, instance_id);
            header.write_to(&mut file)?;
            (TamperingLogFileHeader::SIZE as u64, 1u64, 2u64)
        } else {
            let max_seq = Self::find_max_segment_seq(&directory);
            (file_len, max_seq, max_seq + 1)
        };

        Ok(Self {
            directory,
            current_file: file,
            current_path,
            current_size,
            max_size_bytes: (max_size_mb as u64) * 1024 * 1024,
            current_file_seq,
            next_file_seq,
            instance_id,
        })
    }

    fn find_max_segment_seq(directory: &Path) -> u64 {
        let mut max_file_seq = 0u64;
        let Ok(entries) = std::fs::read_dir(directory) else {
            return max_file_seq;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !name.starts_with("tampering") || !name.ends_with(".log") {
                continue;
            }
            let Ok(mut segment_file) = File::open(&path) else {
                continue;
            };

            let mut header = TamperingLogFileHeader::zeroed();
            if segment_file.read_exact(header.as_bytes_mut()).is_ok()
                && header.magic == TAMPERING_LOG_MAGIC
                && header.verify_checksum()
                && header.file_seq > max_file_seq
            {
                max_file_seq = header.file_seq;
            }
        }
        max_file_seq
    }

    /// Appends `entry` to the active segment, fsyncing before returning.
    ///
    /// Per I-041 §2, fdatasync happens before any downstream side-effect.
    /// If the file would exceed `max_size_bytes`, the segment is rotated
    /// first (new header written and fsynced).
    ///
    /// **For defence:** the single-writer guarantee (I-012) is *structural*:
    /// `TamperingLog` is owned by value inside `FileWatcher` (not behind
    /// `Arc` or `Mutex`), so the type system prevents any second writer from
    /// ever holding a reference. This is stronger than a runtime lock: it is
    /// enforced at compile time and eliminates the lock contention analysis.
    pub fn append(&mut self, entry: &TamperingLogEntry) -> std::io::Result<()> {
        if self.current_size + TamperingLogEntry::SIZE as u64 > self.max_size_bytes {
            self.rotate()?;
        }

        let bytes = unsafe {
            std::slice::from_raw_parts(
                entry as *const TamperingLogEntry as *const u8,
                TamperingLogEntry::SIZE,
            )
        };
        self.current_file.write_all(bytes)?;
        self.current_file.sync_data()?;
        self.current_size += TamperingLogEntry::SIZE as u64;
        Ok(())
    }

    fn rotate(&mut self) -> std::io::Result<()> {
        self.current_file.sync_all()?;

        let timestamp = format_utc_timestamp(now_ns());
        let rotated_name = format!("tampering-{}-{}.log", self.current_file_seq, timestamp);
        let rotated_path = self.directory.join(&rotated_name);
        std::fs::rename(&self.current_path, &rotated_path)?;

        self.current_file = Self::open_segment_file(&self.current_path)?;

        let new_seq = self.next_file_seq;
        self.next_file_seq += 1;
        let header = TamperingLogFileHeader::new(new_seq, self.instance_id);

        if let Err(e) = header.write_to(&mut self.current_file) {
            let _ = std::fs::remove_file(&self.current_path);
            return Err(e);
        }

        self.current_file_seq = new_seq;
        self.current_size = TamperingLogFileHeader::SIZE as u64;
        Ok(())
    }

    /// Replays every `tampering*.log` segment to reconstruct the
    /// `CompromisedFileSet` per I-040 §5 / I-041 §10 / §11.
    ///
    /// `&mut self` per ADR-017 Amendment 2026-05-10c: rejecting an alien
    /// archive segment appends a `SegmentRejected` event, so the replay
    /// needs write access to `self`.
    ///
    /// Archive segments are scanned first, in ascending `file_seq`
    /// order; the current (live, never-rotated) segment is scanned last
    /// and **unconditionally** — even if its header is unreadable. A
    /// current segment that cannot be read+validated is itself a halt
    /// condition (variant C / M1): silently skipping the live
    /// `tampering.log` drops the previous run's tamper events from the
    /// `CompromisedFileSet` — the exact correctness bug variant C
    /// removes.
    ///
    /// Failure modes (ADR-017 §Amendment 2026-05-18; ADR-018 E9):
    /// - Archive header CRC / magic failure: skip with a warning
    ///   (ADR-017 §J2).
    /// - Archive with a foreign `instance_id`: H2 rejection — entries
    ///   skipped, a `SegmentRejected` event appended to the current
    ///   segment, `file_seq` marked compromised.
    /// - Current segment with a foreign `instance_id` or a
    ///   header-integrity failure (truncated / unreadable header, bad
    ///   magic, bad header CRC): `Err(TamperingLogError::SecurityHalt)`
    ///   — recovery halts (I-041 §11, I-042 §9).
    /// - `Restored` entries remove `file_seq`; all other kinds add it.
    pub fn load_compromised_set(
        &mut self,
        instance_id: [u8; 16],
    ) -> Result<HashSet<u64>, TamperingLogError> {
        if !self.directory.exists() {
            return Ok(HashSet::new());
        }

        let mut archives: Vec<(u64, std::path::PathBuf)> = Vec::new();
        for dir_entry in std::fs::read_dir(&self.directory)? {
            let dir_entry = dir_entry?;
            let path = dir_entry.path();
            let name = match path.file_name().and_then(|name| name.to_str()) {
                Some(name) => name.to_owned(),
                None => continue,
            };
            if !name.starts_with("tampering") || !name.ends_with(".log") {
                continue;
            }
            if path == self.current_path {
                continue;
            }
            let Ok(mut segment_file) = File::open(&path) else {
                continue;
            };
            let mut header = TamperingLogFileHeader::zeroed();
            if segment_file.read_exact(header.as_bytes_mut()).is_err() {
                continue;
            }
            if header.magic == TAMPERING_LOG_MAGIC {
                archives.push(
                    (header.file_seq, path)
                );
            }
        }
        archives.sort_by_key(|(file_seq, _)| *file_seq);

        let mut compromised: HashSet<u64> = HashSet::new();
        for (_, path) in archives {
            self.scan_segment_into(&path, SegmentFileKind::Archive, &mut compromised, instance_id)?;
        }
        let current_path = self.current_path.clone();
        self.scan_segment_into(&current_path, SegmentFileKind::Current, &mut compromised, instance_id)?;
        Ok(compromised)
    }

    /// Builds the `SecurityHalt` error for a current-segment failure
    /// and emits the matching FATAL log line.
    ///
    /// Variant C / M1: any current (live) `tampering.log` segment that
    /// cannot be read+validated halts recovery for operator
    /// investigation — see ADR-017 §Amendment 2026-05-18, I-041 §11,
    /// I-042 §9, ADR-018 E9. `detail` is a short noun phrase describing
    /// the specific failure (e.g. `"has an invalid magic number"`).
    fn current_segment_halt(path: &Path, detail: &str) -> TamperingLogError {
        eprintln!(
            "[tampering-log] FATAL: SECURITY: current TamperingLog segment {path:?} \
             {detail} — halting recovery for operator investigation",
        );
        TamperingLogError::SecurityHalt {
            reason: format!(
                "current TamperingLog segment {path:?} {detail}; recovery halted",
            ),
        }
    }

    /// Replays one segment's entries into `compromised`.
    ///
    /// `&mut self` per ADR-017 Amendment 2026-05-10c: rejecting an alien
    /// archive appends a `SegmentRejected` event via `self.append`.
    ///
    /// `is_current` selects the response to every "cannot read+validate
    /// this segment" condition:
    /// - archive (`false`) → tolerant: a broken header / entry is
    ///   skipped per ADR-017 §J2 and yields `Ok(())`; a foreign
    ///   `instance_id` is an H2 rejection (skip entries, append a
    ///   `SegmentRejected` event, mark `file_seq` compromised);
    /// - current segment (`true`) → strict: a header-integrity failure
    ///   (truncated / unreadable header, bad magic, bad header CRC) or a
    ///   foreign `instance_id` returns
    ///   `Err(TamperingLogError::SecurityHalt)` and recovery halts. The
    ///   current segment is the live `tampering.log`; silently skipping
    ///   it drops live tamper events — the exact bug variant C removes
    ///   (ADR-017 §Amendment 2026-05-18, I-041 §11, I-042 §9). It is
    ///   never "rejected" (there is no fallback source for its events)
    ///   and never appended-to-self.
    ///
    /// **For defence:** the `instance_id` check guards cross-instance
    /// transplant (threat H2 — a segment from a different server planted
    /// into this directory). It does NOT detect intra-instance forgery:
    /// an attacker who knows the server's `instance_id` can forge a
    /// segment with a matching header. Full segment-level authenticity
    /// is a Phase 3 Meta Chain concern (ADR-028); here the CRC32C +
    /// `instance_id` pair is a structural integrity + provenance check,
    /// not a cryptographic one.
    fn scan_segment_into(
        &mut self,
        path: &Path,
        segment_file_kind: SegmentFileKind,
        compromised: &mut HashSet<u64>,
        instance_id: [u8; 16],
    ) -> Result<(), TamperingLogError> {
        let mut segment_file = match File::open(path) {
            Ok(segment_file) => segment_file,
            Err(error) => {
                if segment_file_kind == SegmentFileKind::Current {
                    return Err(error.into());
                }
                eprintln!("[tampering-log] WARNING: cannot open {path:?}: {error}");
                return Ok(());
            }
        };

        let mut header = TamperingLogFileHeader::zeroed();
        if segment_file.read_exact(header.as_bytes_mut()).is_err() {
            if segment_file_kind == SegmentFileKind::Current {
                return Err(
                    Self::current_segment_halt(
                        path,
                        "has a truncated / unreadable header",
                    )
                );
            }
            eprintln!("[tampering-log] WARNING: truncated header in {path:?}, skipping");
            return Ok(());
        }
        if header.magic != TAMPERING_LOG_MAGIC {
            if segment_file_kind == SegmentFileKind::Current {
                return Err(
                    Self::current_segment_halt(
                        path,
                        "has an invalid magic number",
                    )
                );
            }
            eprintln!("[tampering-log] WARNING: invalid magic in {path:?}, skipping");
            return Ok(());
        }
        if !header.verify_checksum() {
            if segment_file_kind == SegmentFileKind::Current {
                return Err(
                    Self::current_segment_halt(
                        path,
                        "failed header CRC verification",
                    )
                );
            }
            eprintln!("[tampering-log] WARNING: header CRC failure in {path:?}, skipping");
            return Ok(());
        }
        if instance_id != ZERO_INSTANCE_ID
            && header.instance_id != ZERO_INSTANCE_ID
            && header.instance_id != instance_id {
            if segment_file_kind == SegmentFileKind::Current {
                return Err(
                    Self::current_segment_halt(
                        path,
                        &format!(
                            "carries a foreign instance_id (expected {instance_id:02x?}, got {:02x?})",
                            header.instance_id,
                        ),
                    )
                );
            }
            eprintln!(
                "[tampering-log] ERROR: SECURITY: instance_id mismatch in archive {path:?} \
                 — segment belongs to a different server instance; possible planted-file attack. \
                 expected={instance_id:02x?} got={:02x?} Rejecting (event appended to current log).",
                header.instance_id,
            );
            let rejected_name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("<unknown>");
            let detected_at_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos() as u64)
                .unwrap_or(0);
            let entry = TamperingLogEntry::new(
                header.file_seq,
                detected_at_ns,
                TamperingKind::SegmentRejected,
                DetectionSource::Startup,
                rejected_name,
            );
            self.append(&entry)?;
            compromised.insert(header.file_seq);
            return Ok(());
        }

        let mut consecutive_bad = 0usize;
        loop {
            let mut entry = TamperingLogEntry::zeroed();
            match segment_file.read_exact(entry.as_bytes_mut()) {
                Ok(()) => {}
                Err(ref error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(error) => {
                    eprintln!("[tampering-log] WARNING: I/O error reading {path:?}: {error}");
                    break;
                }
            }
            if entry.magic != TAMPERING_ENTRY_MAGIC || !entry.verify_checksum() {
                consecutive_bad += 1;
                if consecutive_bad >= MAX_CONSECUTIVE_BAD_ENTRIES {
                    eprintln!(
                        "[tampering-log] WARNING: {MAX_CONSECUTIVE_BAD_ENTRIES} consecutive bad entries in {path:?}, stopping scan",
                    );
                    break;
                }
                continue;
            }
            consecutive_bad = 0;

            match TamperingKind::try_from(entry.kind) {
                Ok(TamperingKind::Restored) => {
                    compromised.remove(&entry.file_seq);
                }
                Ok(_) => {
                    compromised.insert(entry.file_seq);
                }
                Err(_) => {
                    compromised.insert(entry.file_seq);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn unique_test_dir(prefix: &str) -> String {
        let n = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let dir = format!("/tmp/solidus-tlog-test-{}-{}-{}", prefix, pid, n);
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn zero_instance_id() -> [u8; 16] {
        [0u8; 16]
    }

    fn test_instance_id() -> [u8; 16] {
        [0x42u8; 16]
    }

    #[test]
    fn test_tampering_log_file_header_size_is_64_bytes() {
        assert_eq!(TamperingLogFileHeader::SIZE, 64);
    }

    #[test]
    fn test_tampering_log_entry_size_is_128_bytes() {
        assert_eq!(TamperingLogEntry::SIZE, 128);
    }

    #[test]
    fn test_tampering_log_file_header_checksum_is_at_offset_60() {
        assert_eq!(
            std::mem::offset_of!(TamperingLogFileHeader, checksum),
            60,
        );
    }

    #[test]
    fn test_tampering_log_entry_checksum_is_at_offset_124() {
        assert_eq!(
            std::mem::offset_of!(TamperingLogEntry, checksum),
            124,
        );
        assert_eq!(TamperingLogEntry::SIZE - 4, 124);
    }

    #[test]
    fn test_tampering_log_file_header_checksum_roundtrip() {
        let hdr = TamperingLogFileHeader::new(1, test_instance_id());
        assert!(hdr.verify_checksum());
        assert_eq!(hdr.magic, TAMPERING_LOG_MAGIC);
        assert_eq!(hdr.format_version, TAMPERING_LOG_VERSION);
        assert_eq!(hdr.file_seq, 1);
    }

    #[test]
    fn test_tampering_log_file_header_tampered_checksum_fails() {
        let mut hdr = TamperingLogFileHeader::new(1, zero_instance_id());
        hdr.file_seq = 999;
        assert!(!hdr.verify_checksum());
    }

    #[test]
    fn test_tampering_entry_checksum_roundtrip() {
        let entry = TamperingLogEntry::new(
            42,
            1_700_000_000_000_000_000,
            TamperingKind::Signature,
            DetectionSource::PeriodicRecheck,
            "ls_20260403-120000-000-0-42.ls",
        );
        assert!(entry.verify_checksum());
        assert_eq!(entry.file_seq, 42);
        assert_eq!(entry.magic, TAMPERING_ENTRY_MAGIC);
    }

    #[test]
    fn test_tampering_entry_tampered_checksum_fails() {
        let mut entry = TamperingLogEntry::new(
            42, 0, TamperingKind::Crc, DetectionSource::Inotify, "test.ls",
        );
        entry.file_seq = 99;
        assert!(!entry.verify_checksum());
    }

    #[test]
    fn test_tampering_entry_ls_path_str_roundtrip() {
        let entry = TamperingLogEntry::new(
            1, 0, TamperingKind::Deleted, DetectionSource::Inotify,
            "ls_20260403-120000-000-0-1.ls",
        );
        assert_eq!(
            entry.ls_path_str().unwrap(),
            "ls_20260403-120000-000-0-1.ls",
        );
    }

    #[test]
    fn test_tampering_entry_long_path_truncated_to_64_bytes() {
        let long_path = "a".repeat(100);
        let entry = TamperingLogEntry::new(
            1, 0, TamperingKind::Deleted, DetectionSource::Inotify, &long_path,
        );
        assert_eq!(entry.ls_path_str().unwrap().len(), 64);
        assert!(entry.verify_checksum());
    }

    #[test]
    fn test_tampering_log_open_creates_header_and_appends_entry() {
        let dir = unique_test_dir("open");
        let mut log = TamperingLog::open(&dir, 64, zero_instance_id()).unwrap();

        let entry = TamperingLogEntry::new(
            1, 100, TamperingKind::Deleted,
            DetectionSource::Inotify, "a.ls",
        );
        log.append(&entry).unwrap();

        let compromised = log.load_compromised_set(zero_instance_id()).unwrap();
        assert!(compromised.contains(&1));
    }

    #[test]
    fn test_tampering_log_restored_entry_removes_from_compromised_set() {
        let dir = unique_test_dir("restore");
        let mut log = TamperingLog::open(&dir, 64, zero_instance_id()).unwrap();

        let tamper = TamperingLogEntry::new(
            7, 100, TamperingKind::Crc,
            DetectionSource::Inotify, "b.ls",
        );
        log.append(&tamper).unwrap();

        let restored = TamperingLogEntry::new_with_operator(
            7, 200, TamperingKind::Restored,
            DetectionSource::OperatorCli, "b.ls", test_instance_id(),
        );
        log.append(&restored).unwrap();

        let compromised = log.load_compromised_set(zero_instance_id()).unwrap();
        assert!(!compromised.contains(&7), "Restored must clear file_seq from set");
    }

    #[test]
    fn test_tampering_log_rotation_on_size_overflow() {
        let dir = unique_test_dir("rotate");
        const MAX_SIZE_BYTES: u64 = 4 * 1024;
        let mut log = TamperingLog::open(&dir, 1, zero_instance_id()).unwrap();
        log.max_size_bytes = MAX_SIZE_BYTES;

        for i in 0..40u64 {
            let entry = TamperingLogEntry::new(
                i, i * 1000, TamperingKind::Crc,
                DetectionSource::PeriodicRecheck, "test.ls",
            );
            log.append(&entry).unwrap();
        }

        let segments: Vec<_> = std::fs::read_dir(&dir).unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name().to_str()
                    .map(|n| n.starts_with("tampering") && n.ends_with(".log"))
                    .unwrap_or(false)
            })
            .collect();
        assert!(
            segments.len() >= 2,
            "expected rotation to produce >=2 segments, got {}",
            segments.len(),
        );

        let compromised = log.load_compromised_set(zero_instance_id()).unwrap();
        assert_eq!(compromised.len(), 40, "load must merge entries across all segments");
        for i in 0..40u64 {
            assert!(compromised.contains(&i), "missing file_seq {}", i);
        }
    }

    #[test]
    fn test_tampering_log_bad_crc_entry_skipped_good_entries_preserved() {
        let dir = unique_test_dir("bad-crc");
        let mut log = TamperingLog::open(&dir, 64, zero_instance_id()).unwrap();

        let good = TamperingLogEntry::new(
            1, 100, TamperingKind::Signature,
            DetectionSource::Inotify, "good.ls",
        );
        log.append(&good).unwrap();

        let mut bad = TamperingLogEntry::new(
            2, 200, TamperingKind::Crc,
            DetectionSource::Inotify, "bad.ls",
        );
        bad.file_seq = 99;
        let bad_bytes = unsafe {
            std::slice::from_raw_parts(
                &bad as *const TamperingLogEntry as *const u8,
                TamperingLogEntry::SIZE,
            )
        };
        use std::io::Write;
        log.current_file.write_all(bad_bytes).unwrap();
        log.current_file.sync_data().unwrap();

        let good2 = TamperingLogEntry::new(
            3, 300, TamperingKind::Signature,
            DetectionSource::Inotify, "good2.ls",
        );
        log.append(&good2).unwrap();

        let compromised = log.load_compromised_set(zero_instance_id()).unwrap();
        assert!(compromised.contains(&1), "good entry before bad must be loaded");
        assert!(compromised.contains(&3), "good entry after bad must be loaded (J2)");
        assert!(!compromised.contains(&99), "bad entry must be skipped");
        assert!(!compromised.contains(&2), "bad entry file_seq must not appear");
    }

    #[test]
    fn load_compromised_set_halts_on_current_segment_instance_id_mismatch() {
        let dir = unique_test_dir("instance-id-halt");
        let id_a = [0x11u8; 16];
        let id_b = [0x22u8; 16];

        {
            let mut log = TamperingLog::open(&dir, 64, id_a).unwrap();
            let entry = TamperingLogEntry::new(
                5, 100, TamperingKind::Crc,
                DetectionSource::Inotify, "x.ls",
            );
            log.append(&entry).unwrap();
        }

        let mut log = TamperingLog::open(&dir, 64, id_b).unwrap();
        let result = log.load_compromised_set(id_b);
        assert!(
            matches!(result, Err(TamperingLogError::SecurityHalt { .. })),
            "current segment with a foreign instance_id must halt recovery \
             with SecurityHalt, got {result:?}",
        );
    }

    #[test]
    fn test_tampering_kind_try_from_roundtrip() {
        assert_eq!(TamperingKind::try_from(1), Ok(TamperingKind::Signature));
        assert_eq!(TamperingKind::try_from(7), Ok(TamperingKind::Restored));
        assert_eq!(TamperingKind::try_from(8), Ok(TamperingKind::ForensicAccess));
        assert_eq!(TamperingKind::try_from(0), Err(()));
        assert_eq!(TamperingKind::try_from(9), Ok(TamperingKind::SegmentRejected));
        assert_eq!(TamperingKind::try_from(10), Err(()));
    }

    #[test]
    fn test_detection_source_try_from_roundtrip() {
        assert_eq!(DetectionSource::try_from(1), Ok(DetectionSource::Inotify));
        assert_eq!(DetectionSource::try_from(5), Ok(DetectionSource::OperatorCli));
        assert_eq!(DetectionSource::try_from(0), Err(()));
        assert_eq!(DetectionSource::try_from(6), Err(()));
    }

    /// Verifies D-fix-1 (2026-05-09 round 12): the rotated archive filename's
    /// seq component matches the header `file_seq` of that archived file.
    ///
    /// Before D-fix-1, `rotate()` used `next_file_seq` for the filename but
    /// wrote `next_file_seq + 1` into the new header, so the filename did NOT
    /// match the header. This test asserts the corrected behaviour.
    #[test]
    fn test_tampering_log_rotation_filename_matches_header_file_seq() {
        let dir = unique_test_dir("rotate-seq-match");
        const MAX_SIZE_BYTES: u64 = 4 * 1024;
        let mut log = TamperingLog::open(&dir, 1, zero_instance_id()).unwrap();
        log.max_size_bytes = MAX_SIZE_BYTES;

        for i in 0..32u64 {
            let entry = TamperingLogEntry::new(
                i, i * 1000, TamperingKind::Crc,
                DetectionSource::PeriodicRecheck, "x.ls",
            );
            log.append(&entry).unwrap();
        }
        drop(log);

        let mut archive_files: Vec<_> = std::fs::read_dir(&dir).unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name();
                let n = name.to_str().unwrap_or("");
                n.starts_with("tampering-") && n.ends_with(".log")
            })
            .collect();
        assert_eq!(archive_files.len(), 1, "expected exactly one rotated archive");

        let archive_path = archive_files.pop().unwrap().path();
        let archive_name = archive_path.file_name().unwrap().to_str().unwrap().to_owned();

        let parts: Vec<&str> = archive_name.splitn(3, '-').collect();
        assert_eq!(parts.len(), 3, "archive name must be tampering-N-timestamp.log");
        let filename_seq: u64 = parts[1].parse().expect("filename seq must be a u64");

        let mut archive_file = std::fs::File::open(&archive_path).unwrap();
        use std::io::Read as _;
        let mut header = TamperingLogFileHeader::zeroed();
        archive_file.read_exact(header.as_bytes_mut()).unwrap();
        assert!(header.verify_checksum(), "archived file header must have valid CRC");

        assert_eq!(
            filename_seq, header.file_seq,
            "rotated archive filename seq ({}) must match header file_seq ({})",
            filename_seq, header.file_seq,
        );
    }

    /// Verifies D-fix-2 (2026-05-09 round 12): if `header.write_to()` fails
    /// during `rotate()`, the empty `tampering.log` is removed. The rotated
    /// archive is preserved. On the next `open()`, a fresh file is created.
    ///
    /// Note: this test cannot easily inject a real I/O error into `write_to`
    /// without a mock `File`. It instead verifies the post-rotation state
    /// after a SUCCESSFUL rotate (asserting `tampering.log` is recreated with
    /// a fresh valid header) and separately documents the crash-recovery
    /// invariant in prose. Full fault-injection coverage requires integration
    /// tests with filesystem error simulation (deferred to step 8-10-6e).
    #[test]
    fn test_tampering_log_rotation_creates_fresh_active_file_with_valid_header() {
        let dir = unique_test_dir("rotate-fresh-header");
        const MAX_SIZE_BYTES: u64 = 4 * 1024;
        let mut log = TamperingLog::open(&dir, 1, zero_instance_id()).unwrap();
        log.max_size_bytes = MAX_SIZE_BYTES;

        for i in 0..32u64 {
            let entry = TamperingLogEntry::new(
                i, i * 1000, TamperingKind::Crc,
                DetectionSource::PeriodicRecheck, "y.ls",
            );
            log.append(&entry).unwrap();
        }

        let active_path = std::path::Path::new(&dir).join("tampering.log");
        assert!(active_path.exists(), "tampering.log must be recreated after rotation");

        let mut active_file = std::fs::File::open(&active_path).unwrap();
        use std::io::Read as _;
        let mut header = TamperingLogFileHeader::zeroed();
        active_file.read_exact(header.as_bytes_mut()).unwrap();
        assert!(header.verify_checksum(), "active tampering.log header must have valid CRC after rotation");
        assert_eq!(header.magic, TAMPERING_LOG_MAGIC, "active tampering.log magic must be correct");
    }

    #[test]
    fn segment_rejected_event_written_on_instance_id_mismatch() {
        let dir = unique_test_dir("seg-reject-write");
        let alien_dir = unique_test_dir("seg-reject-alien-src");
        let server_id: [u8; 16] = [0x42; 16];
        let alien_id: [u8; 16] = [0x99; 16];

        let alien_archive: PathBuf = {
            let mut alien_log = TamperingLog::open(&alien_dir, 1, alien_id)
                .expect("open alien log");
            let entry = TamperingLogEntry::new(
                7, 100, TamperingKind::Crc, DetectionSource::Inotify, "alien.ls",
            );
            alien_log.append(&entry).expect("append to alien log");
            alien_log.rotate().expect("rotate alien to archive");
            std::fs::read_dir(&alien_dir).unwrap()
                .filter_map(|dir_entry| dir_entry.ok())
                .map(|dir_entry| dir_entry.path())
                .find(|path| {
                    path.file_name().and_then(|name| name.to_str())
                        .map(|name| name.starts_with("tampering-") && name.ends_with(".log"))
                        .unwrap_or(false)
                })
                .expect("alien archive file")
        };

        let mut server_log = TamperingLog::open(&dir, 1, server_id)
            .expect("open server log");
        let planted = PathBuf::from(&dir)
            .join(alien_archive.file_name().unwrap());
        std::fs::copy(&alien_archive, &planted).expect("plant alien archive");

        let compromised = server_log
            .load_compromised_set(server_id)
            .expect("load compromised set");

        assert!(
            compromised.contains(&1),
            "the rejected alien archive's file_seq (1, from its header) \
             must be marked compromised",
        );
        assert!(
            !compromised.contains(&7),
            "the alien archive's own entry (file_seq 7) must NOT be \
             replayed — the whole archive is rejected, only the segment \
             file_seq is marked",
        );

        let current_path = PathBuf::from(&dir).join("tampering.log");
        let bytes = std::fs::read(&current_path).expect("read current segment");
        let payload = &bytes[TamperingLogFileHeader::SIZE..];
        let mut found_segment_rejected = false;
        for chunk in payload.chunks_exact(TamperingLogEntry::SIZE) {
            let mut entry = TamperingLogEntry::zeroed();
            entry.as_bytes_mut().copy_from_slice(chunk);
            if entry.magic == TAMPERING_ENTRY_MAGIC
                && entry.verify_checksum()
                && entry.kind == TamperingKind::SegmentRejected as u8
                && entry.source == DetectionSource::Startup as u8
            {
                found_segment_rejected = true;
                break;
            }
        }
        assert!(
            found_segment_rejected,
            "SegmentRejected event must be appended to the current segment",
        );
    }

    #[test]
    fn segment_rejected_event_replayed_into_compromised_set() {
        let dir = unique_test_dir("seg-reject-replay");
        let server_id: [u8; 16] = [0x42; 16];
        let rejected_seq: u64 = 0xDEAD_BEEF;

        {
            let mut log = TamperingLog::open(&dir, 1, server_id).expect("open log");
            let entry = TamperingLogEntry::new(
                rejected_seq,
                0,
                TamperingKind::SegmentRejected,
                DetectionSource::Startup,
                "tampering-rejected-archive.log",
            );
            log.append(&entry).expect("append SegmentRejected");
            log.rotate().expect("rotate to archive");
        }

        let mut log = TamperingLog::open(&dir, 1, server_id).expect("reopen log");
        let compromised = log
            .load_compromised_set(server_id)
            .expect("load compromised set");
        assert!(
            compromised.contains(&rejected_seq),
            "SegmentRejected.file_seq must replay into the compromised set; got {:?}",
            compromised,
        );
    }

    #[test]
    fn tampering_kind_segment_rejected_try_from_roundtrip() {
        assert_eq!(
            TamperingKind::try_from(9),
            Ok(TamperingKind::SegmentRejected),
        );
        assert_eq!(TamperingKind::SegmentRejected as u8, 9);
    }
}

#[cfg(all(test, unix))]
mod open_segment_file_nofollow_tests {
    use super::*;
    use std::os::unix::fs::symlink;

    fn make_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "solidus-nofollow-test-{}-{}",
            std::process::id(),
            label,
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create test dir");
        dir
    }

    /// A symlink planted at `tampering.log` MUST cause `open()` to fail with
    /// `ELOOP` under `O_NOFOLLOW`, NOT silently redirect appends to the
    /// symlink target. Against a plain open (no flag) this test would fail —
    /// the open would succeed by following the link. (`open(2)`
    /// final-component-symlink semantics, R-029.)
    #[test]
    fn open_rejects_symlinked_path_with_eloop() {
        let dir = make_dir("symlink");
        let target = dir.join("attacker-controlled.txt");
        std::fs::write(&target, b"redirected").expect("write target");

        let link = dir.join("tampering.log");
        symlink(&target, &link).expect("plant symlink");

        let instance_id = [0u8; 16];
        let result = TamperingLog::open(
            dir.to_str().unwrap(),
            1,
            instance_id,
        );

        assert!(
            result.is_err(),
            "open() MUST reject a symlinked tampering.log under O_NOFOLLOW",
        );
        let error = result.err().unwrap();
        assert_eq!(
            error.raw_os_error(),
            Some(libc::ELOOP),
            "expected ELOOP from O_NOFOLLOW, got {error:?}",
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// For a regular (non-symlink) path `O_NOFOLLOW` behaves identically to a
    /// plain open — the normal write path is unaffected. (`open(2)`
    /// regular-file semantics, R-029.)
    #[test]
    fn open_succeeds_on_regular_path() {
        let dir = make_dir("regular");
        let instance_id = [0u8; 16];

        let result = TamperingLog::open(
            dir.to_str().unwrap(),
            1,
            instance_id,
        );
        assert!(
            result.is_ok(),
            "open() on a regular path MUST succeed with O_NOFOLLOW set: {:?}",
            result.err(),
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}