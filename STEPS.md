# Solidus Ledger — Implementation Steps

## Completed

### Step 1: Project Structure
Workspace + crates layout.

### Step 2: Echo Server
Single-threaded echo server on mio.

### Step 3: Bo1ss/Worker + SPSC Ring Buffer
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
- 6-2: PartitionSlot (PREPARE command, 64 bytes = 1 cache line) + Partition RB (MPSC per partition)
- 6-3: Pipeline routing — RuleTable (stub: 1 transfer → 2 postings DEBIT+CREDIT), resolve_partition via SipHash & mask
- 6-4: Partition Actor thread — drain Partition RB, PREPARE (balance check)
- 6-5: main.rs wiring — Partition RBs, Actor threads, component linking
- 6-6: PartitionAssignmentsOverrides — HashMap<[u8;16], usize>, priority over SipHash, separate YAML override file

### Step 7: Decision Maker + THT + 2PC
- 7-1: TransferSlot (384 bytes) + TransferHashTableEntry (32 bytes) + TransferHashTable (Robin Hood → Hopscotch). 8 inline entries + overflow
- 7-2: CoordinatorSlot (64 bytes) + Coordinator RB (MPSC per DM shard)
- 7-3: Pipeline changes — THT insert + release_store(ready=1), tht_offset/shard_id in PartitionSlot
- 7-4: Actor changes — COMMIT/ROLLBACK handling (balance apply, ordinal++, unstage)
- 7-5: Decision Maker thread — drain CoordRB, radix sort, group processing, checkAndDecide
- 7-6: DM: all COMMIT_OK → PostingRecords to LS Writer RB, all ROLLBACK_OK → THT remove
- 7-7: PVT (Partition Version Table) — per-GSN balance for durable queries. Robin Hood, VersionRec inline + overflow
- 7-8: main.rs wiring — THT, Coordinator RB, PVT, DM threads
- 7-t: Integration tests (TCP → Pipeline → Actor → DM → COMMIT/ROLLBACK, full 2PC cycle)

### Step 8-HT: Hopscotch for THT (unplanned, blocking bug fix)
- THT: Robin Hood → Hopscotch. Stable tht_offset, O(1) remove via bitmap
- PAHT/PVT: remain Robin Hood + tombstones (no external references)

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
- 8-4-1: LS Writer thread — drain RB, buffer, group commit, pwrite + fdatasync ✅
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

## In Progress

### Step 8: LS Writer + Persistence (continued) ← current
- 8-10-4: Page-aligned binary search lookup + range queries
- 8-10-5: Integration tests (rotation → index build → lookup)
- 8-10-6: Signature verification during scan
- 8-11: LS Sign Index — *.ls_sign_idx (sorted array by transfer_id, if signing enabled)
- 8-14: Metadata Index Builder — metadata schema parsing from config, `.meta_idx_{name}` per field, binary search by byte ranges. User-defined schema (field name, type, offset in metadata block)
- 8-t: Integration tests (DM → LS Writer → fdatasync → Flush Done → DM → THT cleanup)
- 8-13: Snapshots + Recovery
  - 8-13-1: SLS file format — SnapshotRecord (per account: balance, ordinal, ls_offset)
  - 8-13-2: Snapshot Writer — continuous background thread, dirty flag, max-heap
  - 8-13-3: SLS Checkpoint — index for recovery
  - 8-13-4: Recovery — load snapshot → sequential LS scan → restore PAHT
  - 8-13-5: Recovery — signing state restoration (last_tx_hash from last SigRecord)
  - 8-13-t: Integration tests (write → crash simulation → recovery → verify balances)

## Planned

### Step 9: Rule Engine
- Configurable chart-of-accounts rules: transfer → N postings
- Rule loading from YAML configuration at startup
- Determines max_entries_per_transfer for THT overflow
- Replaces stub (2 postings) with real logic in LedgerPipelineHandler
- RuleTable: Robin Hood hash table, lookup by rule_id
- Variable-length transfer format: collection of entries [{account, type, amount}]
- Wire protocol refactoring: variable length (header + N entries), shared Arena between Worker and Pipeline

### Step 10: TLS
rustls over mio. TLS 1.3, mTLS.

### Step 11: SIMD Ring Buffer Optimizations
copy_nonoverlapping, _mm256_load/store, transformers between RBs.

### Step 12: Pipeline Scaling
N_PIPELINE > 1, AtomicU64 GSN, block-based reservation.

### Step 13: PAHT Incremental Resize
Lazy rehash: new Arena, write to primary, read with migration from secondary. Inactive accounts moved to separate table.

### Step 14: Load Testing (Benchmarks)
- Micro-benchmarks (criterion): Ring Buffer throughput, claim+publish latency, hash table lookup/insert
- End-to-end benchmark: TPS (target 1M), latency percentiles (p50/p99/p999)
- Profiling: perf/flamegraph, cache misses, branch mispredictions

### Step 15: Multi-Platform Support
- Windows: server support without io_uring, with group commit (mmap → VirtualAlloc, mlock → VirtualLock)
- macOS: development support with group commit (getrandom → getentropy)
- Platform abstraction layer for Arena, random, disk I/O

### Step 16: Deduplication (IdempotencyCheck)
3-tier system: Hopscotch hash table (in-memory), Bloom filter (fast reject), .ikey files (persistent).

### Step 17: Key Management
- Three-tier hierarchy: KEK → DEK → private_key
- Shamir Secret Sharing 3-of-5 for KEK
- KEK/DEK in RAM only, protected memory (mlock + zeroize on shutdown)
- Argon2id for derivation, AES-256-GCM for DEK encryption

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

### Step 22: Distributed Replication (hybrid approach)
- Consensus: `openraft` (Rust, battle-tested)
- State machine replication over Raft
- Superblock: 4 copies (~4KB x 4 = 16KB) — node metadata
- VOPR-like simulator for consensus + state machine testing

### Step 23: VSR — Viewstamped Replication (optional, challenge, ~6-8 months)
- Port VSR + VOPR from TigerBeetle (Zig → Rust) as experiment
- Replace openraft with custom VSR for maximum control
- Deterministic simulation (VOPR) required for consensus verification
