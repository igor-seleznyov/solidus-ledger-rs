# Solidus Ledger — Implementation Steps

> Per-step roster of features. Status sections: §Completed,
> §In Progress, §Planned.

---

## Completed

### Step 1: Project Structure
Workspace + crates layout.

### Step 2: Echo Server
Single-threaded echo server on mio.

### Step 3: Boss/Worker + SPSC Ring Buffer
Acceptor thread dispatches connections to Worker threads via SPSC Ring Buffer.

### Step 4: Binary Protocol
Framing, handshake, batch validation, codec.

### Step 5: Pipeline + Internal Ring Buffer + YAML Configuration
- 5-1: YAML configuration (serde + serde_yaml)
- 5-2: mmap Arena + MPSC Ring Buffer (hot path, `crates/ringbuf/`)
- 5-3: IncomingSlot + Pipeline thread + Sequencer (GSN)
- 5-3t: Unit tests (Config, Ring Buffer SPSC/MPSC, Sequencer, IncomingSlot)
- 5-4: Worker → Pipeline integration
- 5-4-1: All-or-nothing batch mode + copy_nonoverlapping
- 5-4-2: Partial reject by transfer_sequence_id
- 5-4t: Integration tests (Worker → Incoming RB → Pipeline, end-to-end)

### Step 6: Partition Actor + PAHT
- 6-1: PAHT — AccountSlot (192 bytes, repr(C), align(64)) + Robin Hood hashing + SipHash-1-3 + unit tests
- 6-2: PartitionSlot (PREPARE command, 64 bytes = 1 cache line) + Partition RB (MPSC per partition). Layout: `sequence(u64), gsn(u64), transfer_id([u8;16]), account_id([u8;16]), amount(i64), entry_type(u8), msg_type(u8), shard_id(u8), _pad(u8), tht_offset(u32)`. Fields `tht_offset`, `shard_id`, `msg_type` are stubs (0) in this step and populated for real in step 7
- 6-3: Pipeline routing — RuleTable (stub: 1 transfer → 2 postings DEBIT+CREDIT), `resolve_partition(account_id)` via `sipHash & mask`, routing into Partition RB. SipHash implementation is moved into the `common` crate so it can be shared across crates
- 6-4: Partition Actor thread — drain Partition RB, PREPARE (balance check), logging (stub)
- 6-5: main.rs wiring — Partition RBs, Actor threads, component linking. Random seed for the partition resolver is generated at startup and logged on start so runs are reproducible if needed
- 6-6: PartitionAssignmentsOverrides — `HashMap<[u8;16], usize>` (~200 entries, fits in L1), priority over SipHash, separate YAML override file (`accounts-assignment-overrides-path`), UUID parsing lives in the `config` crate
- 6-t: Integration tests for Step 6 are deferred to Step 7 — without the Coordinator RB there is no way to observe the Actor's result from outside

### Step 7: Decision Maker + THT + 2PC
- 7-1: TransferSlot (384 bytes, 6 cache lines) + TransferHashTableEntry (32 bytes) in `pipeline`. TransferHashTable (Robin Hood → Hopscotch, Arena, UnsafeCell, overflow parallel array) in `ledger`. Counts (not bitmasks) for the decision. 8 inline entries + overflow once count > 8. 18 unit tests
- 7-2: CoordinatorSlot (64 bytes, contract in `pipeline`) + Coordinator RB (MPSC per DM shard)
- 7-3: Pipeline changes — THT insert + `release_store(ready=1)`, populate `tht_offset` / `shard_id` in PartitionSlot
- 7-4: Actor changes — COMMIT / ROLLBACK handling (balance apply, `ordinal++`, unstage), send PREPARE_OK/FAIL and COMMIT_OK/ROLLBACK_OK into the Coordinator RB
- 7-5: Decision Maker thread — drain CoordRB, radix sort, group processing, `checkAndDecide` (COMMIT / ROLLBACK → Partition RB)
- 7-6: DM: all COMMIT_OK → PostingRecords (LS Writer RB stub), all ROLLBACK_OK → THT remove. Response stub
- 7-7: PVT (Partition Version Table) — balance per GSN for durable queries. PVTSlot (160 bytes, Robin Hood), `VersionRec {gsn, balance}` × 8 inline + overflow. Actor writes on COMMIT, reader uses `acquire_load(pvt_tail) + global_committed_gsn`. Solves "optimistic balance in PAHT vs durable balance for client queries"
- 7-8: main.rs wiring — create THT (per DM shard), Coordinator RB, PVT (per partition), DM threads, wire everything up
- 7-t: Integration tests (TCP → Pipeline → Actor → DM → COMMIT/ROLLBACK, including the deferred 6-t tests)

**THT design:** Arena + raw pointer; synchronization via the `ready` field
(`release_store_u8` / `acquire_load_u8`); `unsafe impl Send + Sync` is provided,
`UnsafeCell<usize>` wraps `count`. Flow: Pipeline insert → `fill_entry` × N →
`fill_overflow` × M → `publish(ready=1)` → DM reads. After `ready = 1` the DM is
the single writer of counts and of the decision. Counts (`u8`) are used instead
of masks (`u64`) for the decision — this fixes the "two postings in the same
partition" case. Routing is driven by `entries[]`, not by a `partitions_mask`. 8
inline entries fit in a 384-byte `TransferSlot`; overflow is kept in a parallel
array, and Robin Hood swap only happens when `entries_count > 8`. THT stores
`transfer_datetime` and `transfer_sequence_id` (needed for `PostingRecord` in
the LS); `ordinal` itself is taken from PAHT and incremented on COMMIT.

### Step 8-HT: Hopscotch for THT (unplanned, blocking bug fix — Robin Hood swap corrupts `tht_offset`)
- THT: Robin Hood → Hopscotch. Stable `tht_offset`, O(1) remove via bitmap
- PAHT: stays on Robin Hood + tombstones (`SLOT_DELETED`). No external references, so it is correct. Re-evaluate Hopscotch for PAHT only if load testing (step 14) shows a need for offset-based lookups in the Actor
- PVT: stays on Robin Hood + tombstones. No external references, no use case for passing offsets
- Consider extracting the shared core into a generic via a trait
- Create a `datastruct` crate (ringbuf + hash tables + `slot_status`), renamed from the current `ringbuf`
- Differences between the tables (each solves a different problem scope):

  |                | THT             | PAHT       | PVT        | IFMH index |
  | -------------- | --------------- | ---------- | ---------- | ---------- |
  | Storage        | Arena           | Arena      | Arena      | Vec        |
  | Access         | `&self` (shared)| `&mut self`| `&mut self`| `&mut self`|
  | Ready barrier  | yes             | no         | no         | no         |
  | Overflow       | parallel array  | none       | linked list| none       |
  | Fingerprint    | yes             | no         | no         | no         |
  | Count          | `UnsafeCell`    | `usize`    | `usize`    | `usize`    |

### Step 8-HT-1: ROLLBACK Fix
- entry_index in PartitionSlot/CoordinatorSlot
- prepare_success_bitmap in TransferSlot
- DM sends ROLLBACK only for entries with PREPARE_SUCCESS

### Miri + Loom Testing
- **Miri** (unsafe correctness, single-thread):
  - PAHT: Robin Hood swap, ptr::write, lookup through raw pointers (4 tests)
  - PVT: Robin Hood swap, inline/overflow write, read_balance (4 tests)
  - THT: Hopscotch insert/lookup/remove, bitmap, copy_nonoverlapping (6 tests)
  - SPSC Ring Buffer: claim/publish/read/release, batch, wrap-around (8 tests)
  - MPSC Ring Buffer: claim/publish/read, batch, fetch_add correctness (9 tests)
  - IFMH: existing tests (Vec-based, Miri-compatible)
- **Loom** (concurrency correctness, C11 abstract memory model, all interleavings exhaustively verified):
  - THT ready barrier: Pipeline release_store(ready=1) → DM acquire_load(ready) (2 tests)
  - global_committed_gsn: LS Writer release → DM acquire, sequential flushes (2 tests)
  - MPSC sequence barrier: Release store → Acquire load (not Relaxed+fence — correct per C11) (2 tests)
  - Three-thread Pipeline→Actor→DM: transitive happens-before chain across two independent release/acquire pairs verified under all interleavings (3 tests)
  - Three-thread LS Writer→DM→IO: transitive durability guarantee — if client sees COMMITTED, fdatasync is proven complete under all interleavings (2 tests)
  - MPSC two writers: fetch_add atomicity — no double claim under any interleaving (1 test)

### Step 8: LS Writer + Persistence + Signing + CRC32C
- 8-1: PostingRecord (128 bytes, CRC32C) + LS Writer RB slot types ✅
- 8-2: IFMH (In-Flight Min-Heap) + Robin Hood index ✅
- 8-3: Flush Done RB slot (SPSC, LS Writer → DM) + global_committed_gsn Arena ✅
- 8-4-1: LS Writer thread — drain RB (`spin_loop`), buffer, group commit flush trigger (`flush-timeout-ms` OR `buffer >= flush-max-buffer-posting-records × 128`), `pwrite + fdatasync` (portable fallback for macOS / Windows). Config: `storage` section (`flush-timeout-ms`, `flush-max-buffer-posting-records`, `current-ls-directory`, `previous-ls-directory`) ✅
- 8-4-2: io_uring backend (IO_LINK, SQPOLL, O_DIRECT), FlushBackend trait ✅
- 8-5: DM integration — PostingRecords + FlushMarker, Flush Done ✅
- 8-6: CRC32C hw-accelerated (SSE4.2) + Ed25519 + SHA256 chain + signing integration ✅
- 8-7: LSFileHeader + LSSignFileHeader + file creation/open ✅
- 8-8: LS metadata, Strategy pattern (SigningStrategy + MetadataStrategy), Uniform FlushBackend API, runtime backend selection, main.rs wiring ✅
- 8-9: LS rotation — should_rotate(), rotate(), datetime filenames, handle reuse, CheckpointRecord + CheckpointFileHeader, on_rotation() signing/metadata cross-file chain ✅
- 8-9-5: Manifest (ManifestHeader + ManifestEntry + read/write/finalize/update_entry_min_values), Startup logic (first launch / reopen / config mismatch rotation), GSN/timestamp tracking, Recovery write_offset (checkpoint scan + LS scan by PostingRecord magic+CRC32C) ✅
- 8-10-1: Index Builder thread infrastructure — mpsc channel, IndexBuilderTask, LS Writer sends task at rotation ✅
- 8-10-2: LS scan refactoring (PostingScanVisitor trait, scan_ls_postings), two-pass Index Builder (CountingVisitor + PlacingVisitor + compute_offsets), durable structures (AccountIndexRecord 40B, OrdinalIndexEntry 16B, TimestampIndexEntry 16B, IndexFileHeader 64B) ✅
- 8-10-3: Index file writing — index_writer.rs, per-account sort + batch write .posting-accounts / .ordinal / .timestamp, IndexFileHeader with three magics (LDSTIDXA/LDSTIDXO/LDSTIDXT) ✅
- 8-10-3-rf: In-memory accumulation — LS Writer накапливает index entries в Arena (mmap+mlock) на hot path (~3ns per posting, zero page fault). При ротации copy Arena→Vec, move через channel в Index Builder. Без scan LS файла при построении индексов ✅
- 8-10-3-miri: Miri tests for IndexBufferEntry (Arena-style ptr write/read/copy/reset, 6 tests) and PostingScanVisitor (copy_nonoverlapping unaligned→aligned, 3 tests) ✅
- 8-10-4a: MmapReader — read-only file mmap (PROT_READ, MAP_PRIVATE, no mlock, OS page cache, MADV_SEQUENTIAL) ✅
- 8-10-4b: Page-aligned binary search in .posting-accounts — two-level (page-level first/last check → record-level binary search), MmapReader, compare_account_id ✅
- 8-10-4c: Range queries in .ordinal/.timestamp — lower_bound/upper_bound binary search, generic key_fn (impl Fn, zero-cost), inclusive range [from..to] ✅
- 8-10-5-fix-ifmh: IFMH adaptive resize (Vec-based Robin Hood rehash, configurable max_resize_count + growth_factor, backpressure on overflow) ✅
- 8-10-5: Integration tests — rotation → index build → lookup → range query, real Index Builder thread, 100 accounts, 180 postings across 3 accounts ✅

## In Progress

### Step 8: LS Writer + Persistence (continued) ← current
- 8-10-5: Integration tests (rotation → index build → lookup)
- 8-10-6: Signature verification + file integrity protection (split into substeps)
  - 8-10-6a: Verify-at-first-open + SignatureVerificationCache ✅
  - 8-10-6b: inotify/kqueue FileWatcher + cache invalidation ✅
  - 8-10-6c: chattr +i / chflags UF_IMMUTABLE on rotated files ✅
  - 8-10-6c-fix: thread `immutable_enabled` from config through LsWriter → IndexBuilderTask ✅
  - 8-10-6-it: integration test spec for File Watcher + Immutable + Signature (implemented in 8-IT-5)
  - **8-10-6d**: Periodic recheck + Tampering Log
    - TamperingLog module (append-only, CRC32C per entry, size-based rotation, single owner = FileWatcher)
    - Periodic recheck loop in FileWatcher via `mio::Poll` timeout (newest-first cursor, batch size, passes)
    - Detached files: `depth-files AND depth-days` filter; detached = not in active set, not under inotify, only verify-at-first-open guards them
    - Allowed windows: minute precision, UTC, `HH:MM` format, optional
    - log-and-continue on detection (no process exit); shared `Arc<Mutex<HashSet<String>>>` for query path
    - Startup verification: default `startup-verify-recent-count: 0` (guard will own this later)
    - FileWatcherMetrics: files_watched, signatures_verified, tampering_detected, alarms_fired, periodic_checks_performed, recheck_passes_completed, tampering_log_append_errors
  - **8-10-6d-rf** — partial. Migrated to the canonical pattern: `PostingRecord`, `LsFileHeader`. Pending: `LsSignFileHeader`, `LsMetaFileHeader`, `IndexFileHeader`, `CheckpointFileHeader`, `ManifestEntry`, `SigRecord` + call-site cleanup across `crates/storage` + integration tests.

    Scope:
    - **Motivation.** The codebase has accumulated three different CRC patterns for the same problem:
      1. `*const Self → *mut Self` cast + temporary zero of `self.checksum` — present in `crates/storage/src/tampering_log.rs` before 8-10-6d. **Undefined behaviour**: writes through a pointer derived from `&self`, breaks the Rust aliasing model, segfaults on read-only mmap, can be optimised wrong by LLVM.
      2. `&mut self` + temporary zero + restore — current `PostingRecord::compute_checksum` / `verify_checksum`. Safe but wasteful (two extra stores per call) and forbids verification on read-only memory.
      3. `&self` + stack copy of `Self` + zero — intermediate fix that landed first with the 8-10-6d Tampering Log. UB-free but pays a 128-byte memcpy per call.
      All three solve the same problem and confuse reviewers. We pick one canonical form and rewrite the rest.
    - **Canonical pattern.** `checksum: u32` MUST be the LAST field of every `repr(C)` struct that carries a CRC. `compute_checksum(&self) -> u32` and `verify_checksum(&self) -> bool` take `&self`, build a single contiguous slice over `[0..SIZE - 4)`, and call `crc32c` on that slice. No mutation of `self`. No temporary zero. No stack copy. No `*mut` cast. The `unsafe` block is a single line that constructs the byte slice; the function itself is safe.
    - **Why this wins.**
      - Works on read-only memory (`PROT_READ` mmap) — required for the sequential LS scan in 8-13 recovery, and for any future read-side service (`solidus-ledger-query`, step 22).
      - Hot path saves 1–2 stores per call on `compute_checksum` and `verify_checksum`, plus drops 4 bytes from the CRC input — small but measurable at 1 M TPS.
      - Smaller `unsafe` surface — `unsafe fn` becomes plain `fn`, callers stop spreading `unsafe { ... }` blocks across the codebase.
      - Field extensions become mechanical: new fields go before `checksum` and eat into reserved padding, so the offset of `checksum` (and therefore the CRC formula) never changes. When reserved padding runs out, bump the file format version and add a version dispatch in the scanner — exactly once per format generation.
      - One canonical pattern instead of three is cheaper to review and to teach.
    - **Scope.**
      - `PostingRecord` (`crates/pipeline/src/posting_record.rs`) — `checksum` is already at offset 124, only the function bodies need rewriting. Hot path: this is the main beneficiary.
      - `SigRecord` (`crates/storage/src/sig_record.rs`) — verify layout, move `checksum` to end if needed, rewrite functions.
      - `LsFileHeader`, `LsSignFileHeader`, `LsMetaFileHeader`, `IndexFileHeader`, `CheckpointFileHeader` (`crates/storage/`) — verify layout for each, move `checksum` to end if needed, rewrite.
      - `ManifestEntry` (designed in 8-10-6e) — designed with `checksum` at the end from day one.
      - `TamperingLogEntry` (8-10-6d) — already follows the canonical pattern after the layout change. No further work.
    - **Reserved padding convention.** Every affected struct gains a `_reserved: [u8; N]` (or `_reserved_small` / `_reserved_large` if there are multiple natural insertion points) sized so that future fields can be added without changing `SIZE`. All writers MUST zero-initialise reserved bytes so the CRC stays deterministic across binaries.
    - **Compile-time invariant.** Each affected struct gets `const _: () = assert!(std::mem::offset_of!(Self, checksum) == Self::SIZE - 4);` (or an equivalent unit test) so the layout invariant cannot drift unnoticed.
    - **Breaking change.** New CRC values do NOT match old CRC values: the previous code computed CRC over `SIZE` bytes with `checksum` zeroed, the new code computes CRC over `SIZE - 4` bytes excluding `checksum` entirely. Every existing LS / sig / meta / manifest / tampering file in dev environments must be discarded after this refactoring. **Acceptable** because there is no production deployment yet — this is the right moment to do the refactor, before any binary on-disk format becomes load-bearing.
    - **Tests.** Existing unit tests on each `compute_checksum` / `verify_checksum` are rewritten against the new formula. Add Miri tests on `verify_checksum(&self)` for every type to prove UB-freedom. Add the layout assert above.
    - **Permanent rule.** Added to `CLAUDE.md` under *Non-negotiable project rules* so every future `repr(C)` type with a checksum follows the pattern from day one without re-litigation.
  - **8-10-6e**: Manifest integrity (auto-restore) + `ManifestEntry.merkle_root` Phase 3 reservation
    - CRC32C per `ManifestEntry` already canonical (delivered by `rf-canonical-crc-bundle` 8-10-6d-rf — checksum at end, no SIZE change there)
    - `manifest-auto-restore: bool` config (default `true`)
    - `Manifest::restore_from_ls` — reconstruction from LS file headers + checkpoint records
    - **ManifestEntry layout: SIZE 128 → 192**, add `merkle_root: [u8; 32]` Phase 3 reservation per I-023 / ADR-029 §F3 (zero-init by all writers; activated by Index Builder at Phase 3 rotation pass). `Manifest::restore_from_ls` reads the final SIZE 192 entries from day one — no transient layout step.
    - TamperingLog CRC already covered in 8-10-6d (cross-linked)
  - Full cryptographic integrity (Ed25519 sign chain) for manifest + tampering.log — **scope of `solidus-ledger-guard`** (separate service, step 22+)
  - **8-10-6d-fix**: Two pre-existing bugs in 8-10-6d-delivered code. (1) `record_tampering` double-`lock()` deadlock (`file_watcher.rs` — `std::sync::Mutex` is not re-entrant; the second `lock()` on the same thread hangs every call). (2) `O_NOFOLLOW` not set on `tampering.log` open / rotate (GAP1 — symlink-redirect surface on the integrity log). Sequenced after `8-10-6d-block8`; `record_tampering` / `open` / `rotate` are not modified by block8, so the fix lands as a localised follow-up.
  - **8-10-6d-cat4**: Category-4 `repr(C)`-from-buffer alignment fix + new invariant **I-0NN**. Fixes every `&*(ptr as *const T)`-on-an-align-1-buffer site in one consistent pass so all sites cite the same invariant: `process_inotify_events` (`file_watcher.rs`) and CheckpointRecord Findings 1-3 (`checkpoint_record.rs` / `checkpoint_file_header.rs` / `manifest_header.rs` — Findings 2/3 also reorder `checksum` to last, a breaking on-disk format change). The invariant lands only once no site violates it.
- **8-crc32c-portable**: Portable CRC32C with runtime SSE4.2 detection.
  - **Why.** `crates/common/src/crc32c.rs` previously was
    `#[target_feature(enable = "sse4.2")]` and called `_mm_crc32_u64`
    / `_mm_crc32_u8` unconditionally — no software fallback. Miri's
    x86_64 target has no SSE4.2, so any Miri test reaching
    `compute_checksum` / `verify_checksum` → `crc32c` aborted with
    "calling a function that requires unavailable target features:
    sse4.2". Same problem on ARM and any non-SSE4.2 CPU. This
    blocked Miri verification of every checksummed `repr(C)` type.
  - **Design.** `crc32c` is a portable dispatcher selecting between a
    hardware (SSE4.2) and a software path via a `static mut bool`
    flag set once at startup. Software path — table-based Castagnoli
    CRC-32C (reversed polynomial `0x82F63B78`), pure safe Rust, no
    `target_feature` — runs under Miri and on non-SSE4.2 CPUs.
    Hardware path — the existing `_mm_crc32` SSE4.2 code as the
    x86_64 specialization.
  - **Dispatch storage.** `static mut USE_HARDWARE_CRC32C: bool =
    false`, set to `true` by `pub fn init()` from `main()` only when
    `is_x86_feature_detected!("sse4.2")` returns true.
  - **Publish-before-spawn pattern.** `init()` writes once on the
    main thread before any `thread::spawn`; the spawn happens-
    before barrier propagates the value to worker threads; no atomic,
    no fence on the hot path. Per `invariants.md` hot-path discipline
    («no atomic — single writer»).
  - **Rejected alternatives.** (a) `AtomicUsize` + `transmute<usize,
    FnType>`: `fn as usize` is rejected by const-eval in static
    initialisers; `transmute<usize, fn>` + call is UB under Miri
    strict-provenance (recovered fn-pointer has no provenance tag);
    duplicates `std::detect`'s internal cache for the feature-detection
    result. (b) `OnceLock` / `LazyLock`: per-load branch cost;
    over-engineered for the single-writer / publish-before-spawn case.
  - **Hot-path dispatch.** One plain `mov` (load `static mut bool`) +
    one predicted branch + one direct call (LLVM-inlinable into the
    dispatcher). Public signature `unsafe fn crc32c(*const u8, usize)
    -> u32` unchanged — zero call-site changes anywhere in the
    workspace.
  - **Default-to-software safety.** The flag starts `false`; forgetting
    `init()` is a performance bug, never a correctness bug. Under Miri,
    `is_x86_feature_detected!` returns false → `init()` leaves the flag
    `false` → dispatcher exclusively takes the software branch → no
    SSE4.2 abort. Non-x86_64 (ARM) always takes the software path; this
    step is also the foundation for step 15 (multi-platform).
- **8-padN-naming-unification**: project-wide rename of `_reserved` / `_tail_pad` → `_pad` / `_padN` in all `repr(C)` on-disk structures. Rule: padding fields use `_pad` if there is exactly one, `_padN` if there are several (N = 1-based sequence number in declaration order); the role (alignment gap / Phase 3 forward-compat budget / CRC32C trailing absorber) is documented in a comment next to the field, not in the field name. Affected structures: `manifest_header.rs`, `ls_file_header.rs`, `sig_record.rs`, `manifest_entry.rs`, `ls_sign_file_header.rs`, `ls_meta_file_header.rs`, `index_file_header.rs`, `tampering_log.rs` (header + entry), `checkpoint_file_header.rs`, `checkpoint_record.rs`. `posting_record.rs` already conforms after the canonical-CRC migration. Includes amendments to `CLAUDE.md` §"Phase 3 forward-compat padding" (replace `_reserved` reference) and `docs/invariants.md` I-001 §"_tail_pad" (abolish the separate `_tail_pad` name; trailing-absorber role documented in the comment on the `_pad{next}` field) + I-023 §"Required reservations" table (replace `_reserved` column with `_pad{next}` per struct). Sequenced after the canonical-CRC migration completes (so rename runs once over a consistent layout); before `8-clippy` (rename touches struct field names referenced by tests).
- **8-clippy**: Workspace-wide clippy debt closure. Sequenced after `8-10-6d-block8`, `8-10-6d-fix`, and `8-10-6d-cat4`. Run `cargo clippy --workspace` and eliminate **every** warning and error across the whole workspace — a one-time full closure of the accumulated debt (~107 warnings + `deny`-level errors, e.g. `not_unsafe_ptr_arg_deref` in `portable_flush_backend.rs`). **Rationale:** closing the baseline debt once means every later step fixes only the handful of newly-introduced lints — never the whole project again. The canonical-CRC migration (`8-10-6d-rf`), if sequenced first, removes the large `missing_safety_doc` cluster on the `compute_checksum` / `verify_checksum` `unsafe fn`s as a side effect — less work for `8-clippy`, but `8-clippy` owns whatever remains; no clippy debt is deferred to `13-rf`. Exit criterion: `cargo clippy --workspace` reports zero warnings and zero errors. After `8-clippy`, the clippy gate is enforced strictly for every subsequent step. Enforcement belongs in `[workspace.lints]` deny-escalation and/or a CI / pre-commit `cargo clippy --workspace -- -D warnings` gate.
- 8-11: LS Sign Index — *.ls_sign_idx (sorted array by transfer_id, if signing enabled)
- 8-14: Metadata Index Builder — metadata schema parsing from config, `.meta_idx_{name}` per field, binary search by byte ranges. User-defined schema (field name, type, offset in metadata block)
- 8-IT: Focused integration tests before snapshots (foundation for safe 8-13 development)
  - 8-IT-1: Audit existing integration tests (`ls_rotation`, `ls_index`, etc.) + gap analysis
  - 8-IT-2: LS Writer + flush + crash scenarios (covers 8-t)
  - 8-IT-3: Signing + metadata end-to-end (signing chain, cross-file)
  - 8-IT-4: Index Builder + rotation + lookup (covers 8-10-5)
  - 8-IT-5: File Watcher + Immutable + Signature verification (covers 8-10-6-it)
- 8-13: Snapshots + Recovery
  - 8-13-1: SLS file format — `SnapshotRecord` (per account: balance, ordinal, `last_gsn`, `ls_offset`), SLS FileHeader
  - 8-13-2: Snapshot Writer — **continuous background thread** (not tied to rotation). Dirty flag + timestamp in `AccountSlot`. Actor sends dirty accounts through an RB. Max-heap ordered by last-snapshot time. LS rotation raises priority (non-blocking). Incremental snapshots (only dirty accounts). Compaction: periodic merge of deltas → base SLS. Scale target: 100 M accounts
  - 8-13-3: SLS Checkpoint — index for recovery (sorted array, `account → sls_offset`)
  - 8-13-4: Recovery — load snapshot → **sequential pass** over LS files (sequential scan, NOT per-account random seeks) → update PAHT for each account seen. The set of LS files to replay is determined from the SLS checkpoint
  - 8-13-5: Recovery — signing state restoration (`last_tx_hash` from the last `SigRecord`)
  - 8-13-t: Integration tests (write → crash simulation → recovery → verify balances)

**Step 8 rationale notes** (apply across sub-steps):
- CRC32C per posting (hardware-accelerated via SSE4.2 `_mm_crc32_u64`, ~2–3 ns)
  detects storage corruption.
- Ed25519 signing provides integrity protection, tamper detection, and audit.
  CRC and signing address different threats — both are required.
- **Flush ordering:** `signing (ls_sign + fdatasync) → metadata (ls_meta +
  fdatasync) → postings (ls + fdatasync)`. This guarantees that if the `ls`
  file is on disk, then `ls_sign` and `ls_meta` are definitely on disk already.

## Planned

### Step 9: Rule Engine
- Configurable chart-of-accounts rules: transfer → N postings
- Rule loading from YAML configuration at startup
- Determines `max_entries_per_transfer` for THT overflow
- Replaces the stub (2 postings) with real logic in `LedgerPipelineHandler`
- `RuleTable`: Robin Hood hash table, lookup by `rule_id`
- Lives in the `ledger` crate
- **Transfer format:** a collection of entries `[{account, type, amount}]` instead of debit+credit+amount. Variable length
- **Technical decision (variant B):** wire protocol uses variable length (header + N entries). Worker parses TCP and writes entries into a shared pre-allocated Arena (bump allocator). `IncomingSlot` stays fixed-size: `transfer_id + entries_offset + entries_count`. Pipeline reads entries from the Arena by offset. Ring Buffers remain fixed-size. This step refactors the wire protocol, the Worker, `IncomingSlot`, and the Pipeline
- **Rules:** decide *which* system accounts participate (capital, correspondent, liabilities, etc.); they do NOT compute amounts. Amounts arrive from the client in each entry. The ledger validates `sum(amounts) == 0`
- **Client entries vs rule entries:** client entries hold `account_id + amount` (the client knows the amounts). Rule entries describe additional system accounts from the chart of accounts, and their amount is taken from the corresponding client entry (by posting type or by reference)
- **The ledger does not round** — all amounts arrive fully rounded from the calling service
- **Metadata end-to-end:** DM sends `MSG_METADATA_REF { staging_slot: u32 }` into the LS Writer RB — a lightweight reference to the MetadataStaging Arena. LS Writer reads the payload from the Arena by slot index without touching THT. The MetadataStaging Arena, the Pipeline / Worker → THT integration, and `MSG_METADATA_REF` are all introduced in this step

### Step 10: TLS
rustls over mio. TLS 1.3, mTLS.

### Step 11: SIMD Ring Buffer Optimizations
copy_nonoverlapping, _mm256_load/store, transformers between RBs.

### Step 12: Pipeline Scaling
N_PIPELINE > 1, AtomicU64 GSN, block-based reservation.

### Step 13: Adaptive Resize for All Hash Tables
- 13-1: Configuration — initial capacity, max_resize_count, growth_factor per structure in config.yaml (config first, then code)
- 13-2: Capacity check — PAHT, PVT, THT return Result/bool instead of panic/infinite loop
- 13-3: Arena resize infrastructure — allocate new Arena, migrate data, free old Arena
- 13-4: PAHT incremental resize — lazy rehash (primary + secondary Arena, write to primary, read with migration from secondary)
- 13-5: PVT resize — similar to PAHT (Robin Hood, Arena)
- 13-6: THT resize — Hopscotch, stable offsets (swap-free rehash, bitmap reset)
- 13-7: Backpressure propagation — Pipeline → Actor → DM → LS Writer chain on any table overflow
- 13-8: Integration tests — overflow each structure, verify resize trigger, backpressure, recovery after resize
- 13-9: Miri tests — verify unsafe correctness during resize (Arena ptr migration, rehash)
- 13-10: Loom tests — verify concurrent access during PAHT lazy rehash (Actor reads while migration in progress)
- Inactive accounts: move to separate table {account_id, last_version} — replay protection, PAHT size reduction, cache locality

### Step 13-rf: Codebase Refactoring
- Large file decomposition: `ls_writer.rs` (2800+ lines), `main.rs` → modules / subpackages
- Constructors with parameters for frequently created structs (`PostingRecord`, `LsWriterSlot`) — eliminate `zeroed()` + field-by-field assignment
- Hot path audit: redundant operations, intermediate instances, unnecessary copies
- Storage crate reorganization: `backend/`, `headers/`, `manifest/`, `strategy/`, `writer/`, `records/` (see `review-2026-04-05.md`)
- Rotated filename cleanup: remove the duplicated `.ls.` suffix (`.ls.sign` → `.sign`, `.ls.checkpoint` → `.checkpoint`, `.ls.ordinal` → `.ordinal`, etc.). The extension already conveys the type, the `.ls.` is redundant and simplifies parsing in `file_watcher`
- Config errors: full migration from `Box<dyn Error>` to `thiserror` + `ConfigError` enum. **Why thiserror:** industry standard (serde / tokio / sqlx / reqwest / axum), proc macro with zero runtime overhead, authored by David Tolnay, DRY (error message next to variant), `#[from]` for auto-conversion, `#[source]` for error chains, smaller surface for bugs (compiler-checked exhaustive match). Typed errors enable exhaustive match, better CLI exit codes, structured error handling, integration via `?`. Invasive — touches `Config::validate()` and all call sites. Starting point: `validate_hhmm` in 8-10-6d already uses `thiserror` locally; in 13-rf that scales out to the whole `config` crate

### Step 13-gs: Graceful Shutdown
- Replace every `process::exit(1)` with a coordinated shutdown driven by a channel or `AtomicBool`
- Affected sites: Index Builder on `protect_rotated_files` failure, File Watcher on `inotify` / `kqueue` failure, and any other fatal error that currently aborts the process
- **Extended tamper reactions:** config knob `file-protection.on-tampering: log-and-continue | shutdown | read-only`. 8-10-6d only supports `log-and-continue`; 13-gs adds `shutdown` and `read-only`
- Algorithm: receive shutdown signal → stop accepting new requests → drain in-flight work → `fdatasync` the final batch → exit
- Without graceful shutdown: data in an inconsistent state, Ring Buffers not drained, files not closed, possible data loss

### Step 14: Load Testing (Benchmarks)
- Micro-benchmarks (`criterion`): Ring Buffer throughput (single and batch), `claim + publish` latency, drain latency, hash table lookup / insert
- End-to-end benchmark: TPS (target 1 M), latency percentiles (p50 / p99 / p999), backpressure behavior
- Profiling: `perf` / flamegraph, cache misses, branch mispredictions
- Environment requirements: `ulimit -l unlimited` (for `mlock`), CPU pinning, isolated cores

### Step 15: Multi-Platform Support
- **Windows** — server support: no io_uring, with group commit. Replacements for Linux-specific APIs (`mmap → VirtualAlloc`, `mlock → VirtualLock`, `getrandom → BCryptGenRandom`)
- **macOS** — development support: with group commit. Replacements for Linux-specific APIs (`getrandom → getentropy`, `mlock → mlock`)
- Platform abstraction layer for Arena, random, and disk I/O

### Step 16: Deduplication (IdempotencyCheck)
- 3-tier system: Hopscotch hash table (in-memory), Bloom filter (fast reject), `.ikey` files (persistent)
- See `solidus-ledger-idempotency-dedup (2).md` for the full architecture

### Step 17: Key Management
- Three-tier hierarchy: KEK → DEK → `private_key`
- Shamir Secret Sharing 3-of-5 for KEK
- KEK / DEK live in RAM only, in protected memory (`mlock` + `zeroize` at shutdown)
- Argon2id for derivation, AES-256-GCM for DEK encryption
- See `solidus-ledger-architecture-v8.md §6` for details

### Step 18: SHM Transport
Shared Memory transport for co-located clients (alternative to TCP). Minimal latency.

### Step 19: Reconciliation
End-of-day reconciliation, business day, balance reporting by date. Architecture design required.

### Step 20: Metrics and Monitoring
- Metrics export (TPS, latency p50/p99/p999, queue depths, PAHT load factor, THT count, IFMH size)
- Alerting: backpressure, THT overflow, IFMH stale entries, disk latency spikes
- Prometheus-compatible endpoint or push-based

### Step 21: Adaptive Buffer Sizes (optional, research)
Dynamic RB capacity, batch size, flush buffer based on current load. ML-based forecasting for proactive scaling.

### Step 22: solidus-ledger-query Service Extraction
- Extract read/index/query code from ledger into separate service
- Modules to extract: index_builder, index_writer, mmap_reader, index_reader
- Query API: lookup by account, range queries by timestamp/ordinal, metadata search
- Independent index building (ordinal, timestamp, metadata, sign)
- Signature verification at lookup with caching
- Reporting: balance at date, posting history

### Step 23: Distributed Replication (hybrid approach)
- Consensus: `openraft` (Rust, battle-tested)
- State machine replication over Raft
- Superblock: 4 copies (~4KB × 4 = 16KB) — node metadata
- VOPR-like simulator for consensus + state machine testing

### Step 24: VSR — Viewstamped Replication (optional, challenge, ~6–8 months)
- Port VSR + VOPR from TigerBeetle (Zig → Rust) as an experiment
- TigerBeetle: Apache 2.0; VSR ~15–20 K lines, VOPR ~5–10 K lines
- Replace `openraft` with a custom VSR for maximum control and optimization against the ledger's needs
- Deterministic simulation (VOPR) is mandatory for consensus verification
