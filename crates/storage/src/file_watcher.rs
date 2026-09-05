use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use mio::{Events, Interest, Poll, Token, Waker};
use std::sync::mpsc::{Sender, Receiver};
use std::time::{Duration, Instant};
use chrono::Timelike;
use libc::IN_CLOEXEC;
#[cfg(target_os = "linux")]
use mio::unix::SourceFd;
use config::{config, StorageFileProtection};
use crate::consts::FILE_PAGE_SIZE;
use crate::signature_verification_cache::SignatureVerificationCache;
use crate::signature_verifier::SignatureVerifyResult;
use crate::tampering_log;
use crate::tampering_log::{DetectionSource, TamperingKind, TamperingLogError, TamperingLog, TamperingLogEntry, ZERO_INSTANCE_ID};
use crate::time_utils::{now_ns, MINUTES_PER_DAY};

const MIN_POLL_TIMEOUT: Duration = Duration::from_millis(10);
const POLL_EVENTS_CAPACITY: usize = 4;

const INOTIFY_TOKEN: Token = Token(0);
const CHANNEL_TOKEN: Token = Token(1);

pub enum FileWatcherMessage{
    WatchFile { ls_path: String },
}

#[derive(Clone)]
pub struct FileWatcherSender {
    inner: Sender<FileWatcherMessage>,
    waker: Arc<Waker>,
}

impl FileWatcherSender {
    pub fn new(
        inner: Sender<FileWatcherMessage>,
        waker: Arc<Waker>,
    ) -> Self {
        Self { inner, waker }
    }
    
    pub fn send(&self, msg: FileWatcherMessage) -> std::io::Result<()> {
        self.inner.send(msg).map_err(
            |_| {
                std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "File watcher channel closed",
                )
            }
        )?;
        self.waker.wake()?;
        Ok(())
    }
}

pub struct FileWatcher{
    id: usize,
    rx: Receiver<FileWatcherMessage>,
    poll: Poll,
    watch_directory: String,
    signature_cache: SignatureVerificationCache,

    tampering_log: TamperingLog,
    compromised_files: Arc<Mutex<HashSet<String>>>,

    recheck_interval: Duration,
    recheck_batch_size: usize,
    recheck_depth_files: usize,
    recheck_depth_days: usize,

    /// Already-parsed (start_minute_of_day, end_minute_of_day) UTC pairs.
    /// FileWatcher never parses strings — parsing happens in main.rs via
    /// `config::parse_hhmm` so there is a single source of truth.
    recheck_allowed_windows: Vec<(u32, u32)>,
    /// Newest-first. `None` slots are tombstones for removed files (see
    /// `record_tampering`); compacted once per recheck pass.
    active_set: Vec<Option<WatchedFile>>,
    /// Number of `Some(_)` slots in `active_set` — avoids an O(n) scan
    /// whenever we need the live count.
    active_set_live: usize,
    pass_cursor: usize,
    /// Owned by the watcher so `record_tampering` can issue
    /// `inotify_rm_watch` directly; initialized from `setup_inotify` in
    /// `run_platform`.
    inotify_fd: libc::c_int,
    metrics: Arc<FileWatcherMetrics>,
}

struct WatchedFile {
    file_seq: u64,
    ls_path: String,
    watch_descriptor: libc::c_int,
    created_at_ns: u64,
}

#[derive(Default)]
struct FileWatcherMetrics {
    pub files_watched: AtomicU64,
    pub signatures_verified: AtomicU64,
    pub tampering_detected: AtomicU64,
    pub alarms_fired: AtomicU64,
    pub periodic_checks_performed: AtomicU64,
    pub recheck_passes_completed: AtomicU64,

    /// Operators MUST alert on any non-zero value: it means tampering was
    /// detected and in-memory state was updated, but the persistent record
    /// of the compromise was NOT durably written and will be lost on
    /// restart. See `record_tampering` for the full durability trade-off.
    pub tampering_log_append_errors: AtomicU64,
}

/// Side-channel handles returned from `FileWatcher::new` alongside the
/// watcher itself.
///
/// A struct (rather than a 4-tuple) because there are 3+ values and they
/// are used in several places (main.rs wiring, integration tests); naming
/// `handles.waker` beats `.0/.1/.2` when all three are `Arc<T>` and are
/// trivially swappable by mistake.
///
/// Zero-cost: `rustc` returns the struct through the same ABI as a tuple
/// (SysV x86_64 multi-register return). No heap allocations are introduced
/// — all `Arc::new` calls already happened inside `FileWatcher::new`.
pub struct FileWatcherHandles {
    pub waker: Arc<Waker>,
    pub compromised_files: Arc<Mutex<HashSet<String>>>,
    pub metrics: Arc<FileWatcherMetrics>,
}

/// One rotated LS file entry from the manifest, passed to FileWatcher
/// at construction. Replaces the anonymous `(u64, String, u64)` tuple
/// to give each field a name and make call sites self-documenting.
///
/// `ls_path` is the **absolute path** to the LS file on disk,
/// constructed in `main.rs` from `ManifestEntry.filename` (basename,
/// `[u8; 64]`) + `config.storage.current_files_directory`. FileWatcher
/// needs the full path for `inotify_add_watch` and signature
/// verification.
///
/// TODO(rf-16): when `CompromisedFileSet` migrates to inline `[u8; 48]`
/// filenames, consider switching `ls_path` to a fixed-length basename
/// (`[u8; 64]` + `u8` len) and constructing the full path on demand
/// from `FileWatcher.watch_directory`. Removes per-entry `String`
/// allocation at startup (cold path, ~1000 entries, ~40 KB — not
/// urgent, but cleaner).
pub struct ManifestFileInfo {
    pub file_seq: u64,
    pub ls_path: String,
    pub created_at_ns: u64,
}

impl FileWatcher {
    pub fn new(
        id: usize,
        rx: Receiver<FileWatcherMessage>,
        watch_directory: String,
        manifest_entries: Vec<ManifestFileInfo>,
        config: &StorageFileProtection,
    ) -> Result<(Self, FileWatcherHandles), TamperingLogError> {
        let instance_id = config.instance_id.unwrap_or(ZERO_INSTANCE_ID);

        let mut tampering_log = TamperingLog::open(
            &watch_directory,
            config.tampering_log_max_size_mb,
            instance_id,
        )?;

        let compromised_set = tampering_log.load_compromised_set(instance_id
        )?;
        let compromised_paths: HashSet<String> = manifest_entries.iter()
            .filter(|info| compromised_set.contains(&info.file_seq))
            .map(|info| info.ls_path.clone())
            .collect();
        let compromised_files = Arc::new(
            Mutex::new(compromised_paths)
        );

        let active_set_vec = Self::build_active_set(
            &manifest_entries,
            &compromised_set,
            config.recheck_depth_files,
            config.recheck_depth_days,
        );
        let active_set_live = active_set_vec.len();
        let active_set: Vec<Option<WatchedFile>> = active_set_vec.into_iter().map(Some).collect();

        let poll = Poll::new()?;
        let waker = Arc::new(
            Waker::new(
                poll.registry(),
                CHANNEL_TOKEN,
            )?
        );
        let metrics = Arc::new(
            FileWatcherMetrics::default()
        );

        let watcher = Self {
            id,
            rx,
            poll,
            watch_directory,
            signature_cache: SignatureVerificationCache::new(),
            tampering_log,
            compromised_files: compromised_files.clone(),
            recheck_interval: Duration::from_secs(config.recheck_interval_seconds),
            recheck_batch_size: config.recheck_batch_size,
            recheck_depth_files: config.recheck_depth_files,
            recheck_depth_days: config.recheck_depth_days,
            recheck_allowed_windows: config.recheck_allowed_time_windows.clone(),
            active_set,
            active_set_live,
            pass_cursor: 0,
            inotify_fd: -1,
            metrics: metrics.clone(),
        };

        Ok(
            (
                watcher,
                FileWatcherHandles {
                    waker,
                    compromised_files,
                    metrics,
                }
            )
        )
    }

    pub fn run(&mut self) {
        println!(
            "[file-watcher {}] started, watching: {}",
            self.id,
            self.watch_directory
        );

        match self.run_platform() {
            Ok(()) => {
                println!("[file-watcher {}] stopped", self.id);
            }
            Err(error) => {
                eprintln!(
                    "[file-watcher {}] FATAL: file watcher failed: {}. Initiating shutdown.",
                    self.id, error,
                );
                std::process::exit(1);
            }
        }
    }

    fn build_active_set(
        manifest_entries: &[ManifestFileInfo],
        compromised_set: &HashSet<u64>,
        depth_files: usize,
        depth_days: usize,
    ) -> Vec<WatchedFile> {
        let now_ns = now_ns();
        let max_age_ns = depth_days as u64 * 86_400 * 1_000_000_000;

        let mut entries: Vec<WatchedFile> = manifest_entries.iter()
            .filter(|info| !compromised_set.contains(&info.file_seq))
            .filter(|info| now_ns.saturating_sub(info.created_at_ns) <= max_age_ns)
            .map(
                |info| WatchedFile {
                    file_seq: info.file_seq,
                    ls_path: info.ls_path.clone(),
                    watch_descriptor: -1,
                    created_at_ns: info.created_at_ns,
                }
            ).collect();

        entries.sort_by(
            |first, second| first.file_seq.cmp(&second.file_seq)
        );
        entries.truncate(depth_files);
        entries
    }

    fn compute_poll_timeout(&self, last_recheck: Instant) -> Duration {
        let elapsed = last_recheck.elapsed();
        let until_next = self.recheck_interval.saturating_sub(elapsed);

        if self.recheck_allowed_windows.is_empty() || self.is_in_allowed_window() {
            return if until_next.is_zero() {
                MIN_POLL_TIMEOUT
            } else {
                until_next
            };
        }

        std::cmp::min(
            until_next.max(MIN_POLL_TIMEOUT),
            self.duration_to_next_window(),
        )
    }

    /// Is the current UTC wall-clock minute inside any configured recheck
    /// window? An empty window list means "always allowed".
    ///
    /// Windows whose `start > end` are treated as crossing midnight
    /// (e.g. `23:30..01:00`).
    fn is_in_allowed_window(&self) -> bool {
        if self.recheck_allowed_windows.is_empty() {
            return true;
        }
        let now = chrono::Utc::now();
        let minute_of_day = now.hour() * 60 + now.minute();
        self.recheck_allowed_windows.iter().any(
            |&(start, end)| {
                if start <= end {
                    minute_of_day >= start && minute_of_day < end
                } else {
                    minute_of_day >= start || minute_of_day < end
                }
            }
        )
    }

    /// Time until the next allowed recheck window opens, assuming we are
    /// currently outside every window.
    ///
    /// Preconditions (enforced with `debug_assert!`): `recheck_allowed_windows`
    /// is non-empty and `is_in_allowed_window() == false`. `compute_poll_timeout`
    /// is the only caller and already funnels these cases; the asserts guard
    /// against future refactors — release builds pay nothing for them.
    fn duration_to_next_window(&self) -> Duration {
        debug_assert!(!self.recheck_allowed_windows.is_empty());
        debug_assert!(!self.is_in_allowed_window());

        let now = chrono::Utc::now();
        let current_minute = now.hour() * 60 + now.minute();

        let best_delta = self.recheck_allowed_windows.iter()
            .map(
                |&(start, _)| {
                    if start >= current_minute {
                        start - current_minute
                    } else {
                        MINUTES_PER_DAY - current_minute + start
                    }
                }
            ).min()
            .expect(
                "FileWatcher invariant violation: duration_to_next_window called with \
       no configured allowed windows. This function assumes the upstream check \
       `compute_poll_timeout` has already handled the empty-windows case."
            );
        Duration::from_secs(
            (best_delta as u64) * 60
        )
    }

    /// Run up to `recheck_batch_size` signature verifications on the oldest
    /// unchecked files in `active_set`, advancing `pass_cursor` as we go.
    ///
    /// At the end of each full pass we:
    /// - reset `pass_cursor` to 0,
    /// - bump `recheck_passes_completed`,
    /// - compact tombstones with `retain(Option::is_some)`. Compaction at
    ///   pass boundary is safe because the cursor was just reset; compacting
    ///   mid-pass would invalidate the cursor index.
    ///
    /// Tombstoned slots encountered mid-batch are skipped without counting
    /// against `batch_size` — `record_tampering` may have inserted them
    /// between batches.
    fn perform_recheck_batch(&mut self) {
        if self.active_set_live == 0 {
            return;
        }
        let batch_size = self.recheck_batch_size.min(self.active_set_live);
        let mut processed = 0;

        while processed < batch_size {
            if self.pass_cursor >= self.active_set.len() {
                self.pass_cursor = 0;
                self.metrics.recheck_passes_completed.fetch_add(1, Ordering::Relaxed);
                println!(
                    "[file-watcher {}] recheck pass completed, {} live files",
                    self.id, self.active_set_live,
                );
                self.active_set.retain(|slot| slot.is_some());
                if self.active_set_live == 0 {
                    return;
                }
            }

            let idx = self.pass_cursor;
            self.pass_cursor += 1;
            let (ls_path, file_seq) = match &self.active_set[idx] {
                Some(watched) => (watched.ls_path.clone(), watched.file_seq),
                None => continue,
            };
            processed += 1;

            let sign_path = format!("{}.sign", ls_path);
            self.signature_cache.invalidate(&sign_path);

            self.metrics.periodic_checks_performed.fetch_add(1, Ordering::Relaxed);

            let result = self.signature_cache.verify_or_cached(&sign_path);
            match result {
                SignatureVerifyResult::Ok { records_count } => {
                    self.metrics.signatures_verified.fetch_add(1, Ordering::Relaxed);
                    if self.id == 0 {
                        println!(
                            "[file-watcher {}] periodic recheck ok: {} ({} records)",
                            self.id, sign_path, records_count,
                        );
                    }
                }
                SignatureVerifyResult::SignFileNotFound => {
                    self.record_tampering(
                        file_seq,
                        &ls_path,
                        TamperingKind::Deleted,
                        DetectionSource::PeriodicRecheck
                    );
                }
                SignatureVerifyResult::InvalidHeader(_)
                | SignatureVerifyResult::ChecksumMismatch { .. } => {
                    eprintln!(
                        "[file-watcher {}] ALARM: CRC/header corruption in {}: {:?}",
                        self.id, sign_path, result,
                    );
                    self.record_tampering(
                        file_seq,
                        &ls_path,
                        TamperingKind::Crc,
                        DetectionSource::PeriodicRecheck,
                    );
                }
                SignatureVerifyResult::InvalidSignature { .. }
                | SignatureVerifyResult::ChainBroken { .. } => {
                    eprintln!(
                        "[file-watcher {}] ALARM: signature verification failed for {}: {:?}",
                        self.id, sign_path, result,
                    );
                    self.record_tampering(
                        file_seq,
                        &ls_path,
                        TamperingKind::Signature,
                        DetectionSource::PeriodicRecheck,
                    );
                }
                SignatureVerifyResult::ReadError(message) => {
                    eprintln!(
                        "[file-watcher {}] I/O error verifying {}: {} (not classified as tampering)",
                        self.id, sign_path, message,
                    );
                }
            }
        }
    }

    /// Record a tampering detection: persist to `tampering.log`, publish to
    /// the shared in-memory set, tombstone the file in `active_set`, and drop
    /// its inotify watch.
    ///
    /// # Durability trade-off
    ///
    /// If `tampering_log.append` fails the persistent record of the
    /// compromise is LOST: on restart `tampering.log` will not replay this
    /// `file_seq` and the watcher will treat the file as trusted again. We
    /// accept this hole in 8-10-6d under log-and-continue semantics — the
    /// alternative (panic/shutdown on `tampering.log` I/O error) violates
    /// "never kill the process on detection". Proper escalation
    /// (`on-tampering: read-only`) lands in 13-gs.
    ///
    /// Operators MUST alert on `tampering_log_append_errors` until then.
    ///
    /// # Active-set removal
    ///
    /// The slot in `active_set` is replaced with `None` (tombstone) instead
    /// of `Vec::remove`. That avoids two bugs of a plain `remove(idx)`:
    /// O(n) shifting and an off-by-one with `pass_cursor` (decrementing when
    /// `idx < cursor` but not when `idx == cursor`, otherwise the next
    /// iteration would skip the file that slid into the vacated slot).
    /// `perform_recheck_batch` compacts tombstones once per pass.
    ///
    /// # Inotify cleanup
    ///
    /// `inotify_fd` is owned by the watcher (set up in `run_platform`) so we
    /// can issue `inotify_rm_watch` directly here. The previous design left
    /// a TODO "handled in caller context" and leaked watch descriptors for
    /// the lifetime of the process while the kernel kept delivering events
    /// on files that had already been removed from the active set.
    fn record_tampering(
        &mut self,
        file_seq: u64,
        ls_path: &str,
        kind: TamperingKind,
        source: DetectionSource,
    ) {
        self.metrics.tampering_detected.fetch_add(1, Ordering::Relaxed);

        let detected_at_ns = now_ns();
        let entry = TamperingLogEntry::new(file_seq, detected_at_ns, kind, source, ls_path);

        if let Err(error) = self.tampering_log.append(&entry) {
            self.metrics.tampering_log_append_errors.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[file-watcher {}] ERROR: failed to persist tampering entry for {}: {} \
             — in-memory state updated, persistent state will be LOST on restart",
                self.id, ls_path, error,
            );
        }

        {
            let mut compromised = self.compromised_files
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            compromised.insert(ls_path.to_string());
        }

        let mut removed_watch_descriptor: libc::c_int = -1;
        for slot in self.active_set.iter_mut() {
            if let Some(watched) = slot.as_ref() {
                if watched.file_seq == file_seq {
                    removed_watch_descriptor = watched.watch_descriptor;
                    *slot = None;
                    self.active_set_live -= 1;
                    break;
                }
            }
        }

        if removed_watch_descriptor >= 0 && self.inotify_fd >= 0 {
            let rc  = unsafe { libc::inotify_rm_watch(self.inotify_fd, removed_watch_descriptor) };
            if rc != 0 {
                eprintln!(
                    "[file-watcher {}] inotify_rm_watch(wd={}) returned {}, errno={}",
                    self.id,
                    removed_watch_descriptor,
                    rc,
                    std::io::Error::last_os_error().raw_os_error().unwrap_or(-1),
                );
            }
        }

        eprintln!(
            "[file-watcher {}] TAMPERING DETECTED: file={} kind={:?} source={:?} file_seq={}",
            self.id, ls_path, kind, source, file_seq,
        );
    }

    /// Verify signatures on the `count` newest files in `active_set` during
    /// startup. Invalidates each cache entry first so we really re-verify;
    /// any failures are recorded via `record_tampering` after the scan.
    ///
    /// Collect-first-then-mutate is deliberate: it avoids a borrow-checker
    /// conflict between iterating `active_set` and calling
    /// `record_tampering(&mut self)`, which also mutates `active_set`.
    fn perform_startup_verification(&mut self, count: usize) {
        let newest_files_count = count.min(self.active_set_live);
        println!(
            "[file-watcher {}] startup verification of {} newest files",
            self.id, newest_files_count,
        );

        let mut to_compromise: Vec<(u64, String, TamperingKind)> = Vec::new();
        let mut checked = 0;
        for slot in self.active_set.iter() {
            if checked >= newest_files_count {
                break;
            }
            let watched = match slot.as_ref() {
                Some(watched) => watched,
                None => continue,
            };
            let ls_path = watched.ls_path.clone();
            let file_seq = watched.file_seq;
            checked += 1;

            let sign_path = format!("{}.sign", ls_path);
            self.signature_cache.invalidate(&sign_path);
            match self.signature_cache.verify_or_cached(&sign_path) {
                SignatureVerifyResult::Ok { .. } => {
                    self.metrics.signatures_verified.fetch_add(1, Ordering::Relaxed);
                }
                SignatureVerifyResult::SignFileNotFound => {
                    to_compromise.push((file_seq, ls_path, TamperingKind::Deleted));
                }
                SignatureVerifyResult::InvalidHeader(_)
                | SignatureVerifyResult::ChecksumMismatch { .. } => {
                    to_compromise.push((file_seq, ls_path, TamperingKind::Crc));
                }
                SignatureVerifyResult::InvalidSignature { .. }
                | SignatureVerifyResult::ChainBroken { .. } => {
                    to_compromise.push((file_seq, ls_path, TamperingKind::Signature));
                }
                SignatureVerifyResult::ReadError(message) => {
                    eprintln!(
                        "[file-watcher {}] startup verification I/O error for {}: {}",
                        self.id, sign_path, message,
                    );
                }
            }
        }

        for (seq, path, kind) in to_compromise {
            self.record_tampering(seq, &path, kind, DetectionSource::Startup);
        }
    }
}

#[cfg(target_os = "linux")]
impl FileWatcher{
    fn run_platform(&mut self) -> std::io::Result<()> {
        self.setup_inotify()?;
        let inotify_fd = self.inotify_fd;
        self.register_all_watches(inotify_fd)?;

        let mut events = Events::with_capacity(POLL_EVENTS_CAPACITY);
        let mut event_buf = [0u8; FILE_PAGE_SIZE];
        let mut last_recheck = Instant::now();

        loop {
            let timeout = self.compute_poll_timeout(last_recheck);
            self.poll.poll(&mut events, Some(timeout))?;

            let mut got_event = false;
            for event in events.iter() {
                got_event = true;
                match event.token() {
                    INOTIFY_TOKEN => {
                        self.drain_inotify(inotify_fd, &mut event_buf)?;
                    }
                    CHANNEL_TOKEN => {
                        if self.drain_channel(inotify_fd)? {
                            self.cleanup_inotify(inotify_fd);
                            return Ok(());
                        }
                    }
                    _ => {}
                }
            }

            if !got_event || last_recheck.elapsed() >= self.recheck_interval {
                if self.is_in_allowed_window() {
                    self.perform_recheck_batch();
                    last_recheck = Instant::now();
                }
            }
        }
    }

    fn setup_inotify(&mut self) -> std::io::Result<()> {
        let fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC | libc::IN_NONBLOCK) };
        if fd == -1 {
            return Err(std::io::Error::last_os_error());
        }
        self.inotify_fd = fd;
        self.poll.registry().register(
            &mut SourceFd(&fd),
            INOTIFY_TOKEN,
            Interest::READABLE,
        )?;
        Ok(())
    }

    fn register_all_watches(&mut self, inotify_fd: libc::c_int) -> std::io::Result<()> {
        const MASK: u32 = libc::IN_MODIFY | libc::IN_ATTRIB | libc::IN_DELETE_SELF | libc::IN_MOVE_SELF;
        for slot in self.active_set.iter_mut() {
            let watched = match slot {
                Some(watched) => watched,
                None => continue,
            };
            let c_path = match std::ffi::CString::new(watched.ls_path.as_str()) {
                Ok(c_path) => c_path,
                Err(_) => {
                    eprintln!(
                        "[file-watcher {}] WARNING: NUL byte in path, skipping: {}",
                        self.id, watched.ls_path,
                    );
                    continue;
                }
            };
            let wd = unsafe { libc::inotify_add_watch(inotify_fd, c_path.as_ptr(), MASK) };
            if wd == -1 {
                eprintln!(
                    "[file-watcher {}] WARNING: inotify_add_watch failed for {}: {}",
                    self.id, watched.ls_path, std::io::Error::last_os_error(),
                );
                continue;
            }
            watched.watch_descriptor = wd;
            self.metrics.files_watched.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn drain_inotify(
        &mut self,
        inotify_fd: libc::c_int,
        event_buf: &mut [u8; FILE_PAGE_SIZE],
    ) -> std::io::Result<()> {
        loop {
            let bytes_read = unsafe {
                libc::read(
                    inotify_fd,
                    event_buf.as_mut_ptr() as *mut libc::c_void,
                    event_buf.len(),
                )
            };
            if bytes_read == -1 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::WouldBlock {
                    return Ok(())
                }
                return Err(err);
            }
            if bytes_read == 0 {
                return Ok(());
            }
            self.process_inotify_events(&event_buf[..bytes_read as usize]);
        }
    }

    fn drain_channel(&mut self, inotify_fd: libc::c_int) -> std::io::Result<bool> {
        loop {
            match self.rx.try_recv() {
                Ok(msg) => self.handle_message(msg, inotify_fd),
                Err(std::sync::mpsc::TryRecvError::Empty) => return Ok(false),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    println!("[file-watcher {}] channel closed, exiting", self.id);
                    return Ok(true);
                }
            }
        }
    }

    fn cleanup_inotify(&self, inotify_fd: libc::c_int) {
        unsafe {
            libc::close(inotify_fd);
        }
    }

    fn process_inotify_events(&mut self, buf: &[u8]) {
        let mut offset = 0;

        while offset + std::mem::size_of::<libc::inotify_event>() <= buf.len() {
            let event = unsafe {
                std::ptr::read_unaligned(
                    buf[offset..].as_ptr() as *const libc::inotify_event,
                )
            };
            let name_len = event.len as usize;
            let Some(event_size) = std::mem::size_of::<libc::inotify_event>()
                .checked_add(name_len)
                .filter(|&size| size <= buf.len() - offset)
            else {
                break;
            };

            if event.len > 0 {
                let name_start = offset + std::mem::size_of::<libc::inotify_event>();
                let name_bytes = &buf[name_start..name_start + name_len];
                let name_end = name_bytes.iter().position(|&byte| byte == 0).unwrap_or(name_len);
                let filename = match std::str::from_utf8(&name_bytes[..name_end]) {
                    Ok(filename) => filename,
                    Err(_) => {
                        eprintln!("[file-watcher] WARNING: non-UTF8 filename in inotify event, skipping");
                        offset += event_size;
                        continue;
                    }
                };

                if filename.contains(".ls") {
                    let mask = event.mask;
                    let event_type = if mask & libc::IN_MODIFY != 0 {
                        "MODIFY"
                    } else if mask & libc::IN_ATTRIB != 0 {
                        "ATTRIB"
                    } else if mask & libc::IN_DELETE != 0 {
                        "DELETE"
                    } else {
                        "OTHER"
                    };

                    eprintln!(
                        "[file-watcher {}] ALARM: {} detected on rotated file: {}/{}",
                        self.id, event_type, self.watch_directory, filename,
                    );

                    let full_path = format!("{}/{}", self.watch_directory, filename);
                    if filename.ends_with(".sign") {
                        self.signature_cache.invalidate(&full_path);
                    } else if filename.ends_with(".ls") && !filename.contains(".ls.") {
                        let sign_path = format!("{}.sign", full_path);
                        self.signature_cache.invalidate(&sign_path);
                    }
                }
            }

            offset += event_size;
        }
    }

    fn handle_message(&mut self, msg: FileWatcherMessage, inotify_fd: libc::c_int) {
        match msg {
            FileWatcherMessage::WatchFile { ls_path } => {
                println!(
                    "[file-watcher {}] registered rotated file: {}",
                    self.id, ls_path,
                );

                const MASK: u32 = libc::IN_MODIFY | libc::IN_ATTRIB | libc::IN_DELETE_SELF | libc::IN_MOVE_SELF;
                let c_path = match std::ffi::CString::new(ls_path.as_str()) {
                    Ok(c_path) => c_path,
                    Err(_) => {
                        eprintln!(
                            "[file-watcher {}] WARNING: NUL in path, skipping: {}",
                            self.id, ls_path,
                        );
                        return;
                    }
                };
                let wd = unsafe {
                    libc::inotify_add_watch(inotify_fd, c_path.as_ptr(), MASK)
                };
                if wd == -1 {
                    eprintln!(
                        "[file-watcher {}] WARNING: inotify_add_watch failed for {}: {}",
                        self.id, ls_path, std::io::Error::last_os_error(),
                    );
                } else {
                    self.metrics.files_watched.fetch_add(1, Ordering::Relaxed);
                }

                let sign_path = format!("{}.sign", ls_path);
                if std::path::Path::new(&sign_path).exists() {
                    let result = self.signature_cache.verify_or_cached(&sign_path);
                    match result {
                        SignatureVerifyResult::Ok { records_count } => {
                            println!(
                                "[file-watcher {}] signature verified: {} ({} records)",
                                self.id, sign_path, records_count,
                            );
                        }
                        SignatureVerifyResult::SignFileNotFound => {}
                        other => {
                            eprintln!(
                                "[file-watcher {}] ALARM: signature verification failed for {}: {:?}",
                                self.id, sign_path, other,
                            );
                        }
                    }
                }
            }
        }
    }
}

#[cfg(target_os = "macos")]
impl FileWatcher {
    fn run_platform(&mut self) -> std::io::Result<()> {
        println!(
            "[file-watcher {}] macOS: kqueue + Waker for channel, FSEvents TODO",
            self.id,
        );

        let mut events = Events::with_capacity(16);

        loop {
            self.poll.poll(&mut events, None)?;

            for event in events.iter() {
                if event.token() == CHANNEL_TOKEN {
                    loop {
                        match self.rx.try_recv() {
                            Ok(msg) => self.handle_message(msg),
                            Err(std::sync::mpsc::TryRecvError::Empty) => break,
                            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                                println!("[file-watcher {}] channel closed, exiting", self.id);
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    }

    fn handle_message(&mut self, msg: FileWatcherMessage) {
        match msg {
            FileWatcherMessage::WatchFile { ls_path } => {
                println!(
                    "[file-watcher {}] registered rotated file: {}",
                    self.id, ls_path,
                );

                let sign_path = format!("{}.sign", ls_path);
                if std::path::Path::new(&sign_path).exists() {
                    let result = self.signature_cache.verify_or_cached(&sign_path);
                    match result {
                        SignatureVerifyResult::Ok { records_count } => {
                            println!(
                                "[file-watcher {}] signature verified: {} ({} records)",
                                self.id, sign_path, records_count,
                            );
                        }
                        SignatureVerifyResult::SignFileNotFound => {}
                        other => {
                            eprintln!(
                                "[file-watcher {}] ALARM: signature verification failed for {}: {:?}",
                                self.id, sign_path, other,
                            );
                        }
                    }
                }
            }
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
impl FileWatcher {
    fn run_platform(&mut self) -> std::io::Result<()> {
        Err(
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "File watching not supported on this platform",
            )
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a minimal `StorageFileProtection` for tests that need a
    /// real `FileWatcher`. All fields carry inert defaults; `instance_id`
    /// is `None`, so the H2 planted-file check stays disabled.
    fn test_file_protection() -> StorageFileProtection {
        StorageFileProtection {
            immutable_enabled: false,
            watch_enabled: true,
            recheck_interval_seconds: 3600,
            recheck_batch_size: 10,
            recheck_depth_files: 1000,
            recheck_depth_days: 30,
            recheck_allowed_time_windows: Vec::new(),
            startup_verify_recent_count: 0,
            tampering_log_max_size_mb: 64,
            instance_id: None,
        }
    }

    /// Returns a fresh, process-and-counter-unique scratch directory path
    /// under `/tmp` and clears any stale copy. Mirrors the helper used by
    /// the `tampering_log` test module — no `tempfile` dependency.
    fn unique_test_dir(prefix: &str) -> String {
        static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);
        let sequence = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = format!(
            "/tmp/solidus-fw-test-{prefix}-{}-{sequence}",
            std::process::id(),
        );
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn create_file_watcher() {
        let dir = unique_test_dir("create");
        let (_tx, rx) = std::sync::mpsc::channel();
        let config = test_file_protection();

        let result = FileWatcher::new(0, rx, dir, Vec::new(), &config);

        assert!(result.is_ok());
    }

    #[test]
    fn sender_send_and_wake() {
        let (tx, _rx) = std::sync::mpsc::channel();
        let poll = Poll::new().expect("create Poll");
        let waker = Arc::new(
            Waker::new(poll.registry(), CHANNEL_TOKEN).expect("create Waker")
        );

        let sender = FileWatcherSender::new(tx, waker);

        let result = sender.send(FileWatcherMessage::WatchFile {
            ls_path: "/data/ls/test.ls".to_string(),
        });
        assert!(result.is_ok());
    }

    #[test]
    fn sender_clone_works() {
        let (tx, _rx) = std::sync::mpsc::channel();
        let poll = Poll::new().expect("create Poll");
        let waker = Arc::new(
            Waker::new(poll.registry(), CHANNEL_TOKEN).expect("create Waker")
        );

        let sender1 = FileWatcherSender::new(tx, waker);
        let sender2 = sender1.clone();

        assert!(sender1.send(FileWatcherMessage::WatchFile {
            ls_path: "/test1.ls".to_string(),
        }).is_ok());
        assert!(sender2.send(FileWatcherMessage::WatchFile {
            ls_path: "/test2.ls".to_string(),
        }).is_ok());
    }

    #[cfg(test)]
    mod tests_gap3_bounds_check {
        fn parse_event_sizes(buf: &[u8]) -> Vec<Option<usize>> {
            let hdr_size = std::mem::size_of::<libc::inotify_event>();
            let mut results = Vec::new();
            let mut offset = 0usize;
            while offset + hdr_size <= buf.len() {
                let name_len = unsafe {
                    let event = std::ptr::read_unaligned(buf[offset..].as_ptr() as *const libc::inotify_event);
                    event.len as usize
                };
                let event_size_opt = hdr_size
                    .checked_add(name_len)
                    .filter(|&sz| sz <= buf.len() - offset);
                match event_size_opt {
                    None => {
                        results.push(None);
                        break;
                    }
                    Some(event_size) => {
                        results.push(Some(event_size));
                        offset += event_size;
                    }
                }
            }
            results
        }

        fn make_inotify_buf(name: &[u8]) -> Vec<u8> {
            let hdr_size = std::mem::size_of::<libc::inotify_event>();
            let name_len = name.len();
            let total = hdr_size + name_len;
            let mut buf = vec![0u8; total];
            let len_offset = memoffset_of_inotify_len();
            let len_bytes = (name_len as u32).to_ne_bytes();
            buf[len_offset..len_offset + 4].copy_from_slice(&len_bytes);
            buf[hdr_size..hdr_size + name_len].copy_from_slice(name);
            buf
        }

        fn memoffset_of_inotify_len() -> usize {
            12
        }

        #[test]
        fn test_process_inotify_events_truncated_buffer_does_not_panic() {
            let hdr_size = std::mem::size_of::<libc::inotify_event>();
            let name_len: u32 = 16;
            let len_offset = memoffset_of_inotify_len();
            let mut buf = vec![0u8; hdr_size + 8];
            buf[len_offset..len_offset + 4].copy_from_slice(&name_len.to_ne_bytes());
            let results = parse_event_sizes(&buf);
            assert_eq!(results, vec![None], "truncated buf must bail, not panic");
        }

        #[test]
        fn test_process_inotify_events_name_len_overflow_does_not_panic() {
            let hdr_size = std::mem::size_of::<libc::inotify_event>();
            let len_offset = memoffset_of_inotify_len();
            let mut buf = vec![0u8; hdr_size + 4];
            let huge: u32 = u32::MAX;
            buf[len_offset..len_offset + 4].copy_from_slice(&huge.to_ne_bytes());
            let results = parse_event_sizes(&buf);
            assert_eq!(results, vec![None], "u32::MAX name_len must bail via checked_add");
        }

        #[test]
        fn test_process_inotify_events_zero_len_event_parses_ok() {
            let hdr_size = std::mem::size_of::<libc::inotify_event>();
            let buf = vec![0u8; hdr_size];
            let results = parse_event_sizes(&buf);
            assert_eq!(results, vec![Some(hdr_size)], "zero-len event must parse as hdr_size");
        }

        #[test]
        fn test_process_inotify_events_multi_event_buffer_parses_all() {
            let name1 = b"foo.ls\0\0";
            let name2 = b"bar.sign\0\0\0\0";
            let buf1 = make_inotify_buf(name1);
            let buf2 = make_inotify_buf(name2);
            let mut combined = buf1.clone();
            combined.extend_from_slice(&buf2);

            let hdr_size = std::mem::size_of::<libc::inotify_event>();
            let expected_size1 = hdr_size + name1.len();
            let expected_size2 = hdr_size + name2.len();

            let results = parse_event_sizes(&combined);
            assert_eq!(
                results,
                vec![Some(expected_size1), Some(expected_size2)],
                "two well-formed events must both parse cleanly"
            );
        }

        #[test]
        fn test_process_inotify_events_exact_fit_buffer_parses_ok() {
            let name = b"exact.ls\0\0\0\0";
            let buf = make_inotify_buf(name);
            let hdr_size = std::mem::size_of::<libc::inotify_event>();
            let results = parse_event_sizes(&buf);
            assert_eq!(
                results,
                vec![Some(hdr_size + name.len())],
                "exact-fit buffer must parse without bail"
            );
        }
    }
}

#[cfg(all(test, unix))]
mod record_tampering_lock_tests {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    /// Models the exact lock sequence `record_tampering` performs: a single
    /// scoped acquisition of `compromised_files`, with the guard released at
    /// the closing brace BEFORE any further work.
    ///
    /// The earlier code acquired this same non-reentrant `std::sync::Mutex`
    /// twice on one thread; the second acquisition would block forever and
    /// this call would never return. The run recipe wraps the test in an
    /// OS-level `timeout`, so a regression surfaces as a timeout rather than
    /// an infinite hang.
    fn record_into(compromised_files: &Arc<Mutex<HashSet<String>>>, ls_path: &str) {
        {
            let mut compromised = compromised_files
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            compromised.insert(ls_path.to_string());
        }
        let _post_lock_work = ls_path.len();
    }

    #[test]
    fn record_tampering_single_lock_does_not_deadlock() {
        let compromised_files: Arc<Mutex<HashSet<String>>> =
            Arc::new(Mutex::new(HashSet::new()));

        record_into(&compromised_files, "ls_0001.ls");
        record_into(&compromised_files, "ls_0002.ls");

        let guard = compromised_files
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(guard.contains("ls_0001.ls"));
        assert!(guard.contains("ls_0002.ls"));
        assert_eq!(guard.len(), 2);
    }
}