# Solidus Ledger

### High-Performance Double-Entry Ledger with Durable Persistence

Solidus Ledger is a high-performance double-entry ledger with guaranteed durability, designed for financial systems requiring low latency and high throughput. Target: 1,000,000 transactions per second with sub-millisecond latency.

Primary data is kept in memory for fast access. Every transfer is written to disk before acknowledgment — no data is lost on crash.

---

## Overview

A ledger is the core of any financial system. It atomically executes postings between accounts while guaranteeing balance integrity.

```
                         Solidus Ledger

  Client ─TCP/SHM─> [ IO ] ──> [ Pipeline ] ──> [ Partition Actors ]
                                    |                    |
                                Sequencer          Balance Check
                                    |                    |
                                [ THT ]           [ Decision Maker ]
                                                         |
                                                    COMMIT / ROLLBACK
                                                         |
                                                  [ LS Writer ] ──> Disk
                                                         |
                                                     Response
                                                         |
  Client <─TCP/SHM─ [ IO ] <────────────────────────────┘
```

---

## Key Features

**Double-Entry Bookkeeping**
- Every transfer produces at least two postings: DEBIT and CREDIT
- Invariant: sum of all postings in a transfer = 0
- Up to more than 2 postings per transfer (extensible by rules for chart of accounts)
- Flexible Rule Engine for chart of accounts logic: generating postings from transfers based on configurable rules
- User-defined metadata per transfer

**Custom Binary Protocol**
- Compact binary wire protocol with minimal overhead
- Batch requests (multiple transfers per message)
- Protocol-level validation: deduplication, field checks, sequence groups
- TCP transport with TLS
- SHM (Shared Memory) transport for co-located clients with minimal latency

**Atomicity**
- A transfer is either fully applied or fully rolled back
- No partial states — balances are always consistent
- Consistent balance

**Durable Persistence**
- Client receives COMMITTED only after disk write (fdatasync)
- Group commit: batches postings to amortize fsync cost
- io_uring on Linux for minimal write latency
- Per-posting CRC32C (hardware-accelerated) for storage corruption detection

**Metadata**
- User-defined metadata of arbitrary structure per transfer
- Metadata schema described in configuration (fields, types, offsets)
- Separate LS-META file for metadata storage
- Metadata search via indexes (binary search indexes any field of user schema in metadata block)

**Cryptographic Integrity (optional)**
- Ed25519 signature per transfer
- SHA-256 hash chain — detects gaps or tampering in the record sequence
- Separate signature storage (LS-SIGN files) for independent audit
- Key management: three-tier hierarchy KEK → DEK → private key
- Shamir Secret Sharing (3-of-5) for master key protection
- Signing keys stored only in protected memory (mlock), zeroed on shutdown

**Single-node Scaling via Sharding**
- Accounts are distributed across partitions (shards)
- Each partition is processed by a dedicated thread
- Hot accounts can be pinned to specific partitions via configuration
- Linear scaling: more partitions = more throughput

**Indexes and Search**
- Immutable indexes built at LS file rotation
- Account posting lookup: account index → binary search → O(log N)
- Time-range posting search: index + binary search by timestamp
- Metadata search: metadata index → binary search by fields
- Transfer signature lookup: sign index → binary search by transfer_id

**Crash Recovery**
- Periodic account state snapshots (SLS files)
- Recovery: load snapshot + replay postings from LS
- Signing chain restoration from last LS-SIGN record
- Idempotency: re-submitting a transfer is safe
- Three-tier deduplication: in-memory hash table, Bloom filter, persistent .ikey files

---

## Architecture

### Threading Model

Solidus Ledger uses explicit threads pinned to CPU cores. No async/await, no thread pools. Each component runs on a dedicated thread with predictable latency.

```
┌──────────────┐
│   Acceptor   │  1 thread: accepts TCP/SHM connections
└──────┬───────┘
       │
┌──────▼───────┐
│   Workers    │  N threads: protocol parsing, validation
└──────┬───────┘
       │
┌──────▼───────┐
│   Pipeline   │  1-2 threads: sequencer (GSN), routing
└──────┬───────┘
       │
┌──────▼───────┐
│   Actors     │  16+ threads: balance check, PREPARE/COMMIT
└──────┬───────┘
       │
┌──────▼───────┐
│ Decision     │  1-2 threads: COMMIT/ROLLBACK decision
│ Maker        │
└──────┬───────┘
       │
┌──────▼───────┐
│  LS Writer   │  1 thread: disk write, signing, fsync
└──────────────┘
```

### Lock-Free Ring Buffer Communication

Threads communicate exclusively through lock-free ring buffers. No shared mutable state, no mutexes on the hot path. Synchronization uses memory barriers on sequence fields.

### Account Partitioning

Each account belongs to exactly one partition. Routing uses a hash function on the account identifier. Hot accounts can be pinned to specific partitions via a configuration file (override).

### Storage Format

```
data/ls/current/
  LS_{datetime}.ls                # postings (append-only, fixed-size records)
  LS_{datetime}.ls_meta           # transfer metadata (if enabled)
  LS_{datetime}.ls_sign           # Ed25519 signatures (if enabled)
  LS_{datetime}.checkpoint        # checkpoint at rotation
  LS_{datetime}.idx               # index: account_id → {ordinal, timestamp, count}
  LS_{datetime}.ordinal           # posting offsets sorted by ordinal per account
  LS_{datetime}.timestamp         # posting offsets sorted by timestamp per account
  LS_{datetime}.meta_idx_{name}   # indexes by metadata fields
  LS_{datetime}.ls_sign_idx       # signature index: transfer_id → offset

data/ls/previous/                 # rotated files (may be on a different storage)

data/sls/
  SLS_{datetime}.sls              # account snapshots
  SLS_{datetime}.sls_sign         # snapshot signatures (if enabled)
  SLS_{datetime}.checkpoint       # recovery index
  SLS_{datetime}.idx              # index: account_id → {offset, count}
  SLS_{datetime}.timestamp         # snapshot offsets sorted by timestamp per account

data/ikey/
  *.ikey                          # persistent idempotency keys
```

---

## Project Status

Actively developed. See [Implementation Steps](steps.md) for the full plan.

**Completed:**
- Network layer (mio, binary protocol, batch validation)
- Pipeline with sequencer and routing
- Partition Actors with balance checking (PAHT, Robin Hood hashing)
- Decision Maker with 2PC protocol (THT, Hopscotch hashing)
- Partition Version Table for durable balance queries
- LS Writer with group commit, io_uring backend, O_DIRECT
- CRC32C hardware-accelerated (SSE4.2)
- Ed25519 signing with SHA-256 hash chain
- LS/LS-SIGN file headers with CRC32C checksums
- LS metadata infrastructure (LS-META files, MetaRecord, PostingMetadataStrategy)
- Strategy pattern: SigningStrategy + MetadataStrategy (zero-cost static dispatch)
- Uniform FlushBackend API: single interface for all files (ls, sign, meta)
- Runtime backend selection: io_uring on Linux, portable fallback on other platforms
- Parallel sign+meta flush via io_uring
- Checkpoint files (CheckpointRecord + CheckpointFileHeader, write per flush batch)
- LS rotation: should_rotate(), rotate(), datetime filenames, handle reuse in FlushBackend
- on_rotation() in SigningStrategy/MetadataStrategy, cross-file signing chain
- ManifestHeader (64 bytes) + ManifestEntry (128 bytes) + Manifest file (create, open, append, finalize, fsync)
- Startup logic: manifest-based first launch / reopen / config mismatch rotation
- GSN/timestamp tracking: min at first posting (persisted to manifest), max at rotation
- Miri testing: all hash tables (PAHT, PVT, THT) and ring buffers (SPSC, MPSC) verified for unsafe correctness
- Loom testing: happens-before correctness verified in C11 abstract memory model with exhaustive interleaving exploration — three-thread transitive chains (Pipeline→Actor→DM, LS Writer→DM→IO), release/acquire barriers, MPSC fetch_add atomicity

**In progress (Step 8):**
- Recovery: write_offset from checkpoint + LS scan by PostingRecord magic ← current
- LS Index Builder, LS Sign Index
- Snapshots and crash recovery

**Planned:**
- Rule Engine (configurable chart-of-accounts)
- TLS (rustls over mio)
- Deduplication (IdempotencyCheck)
- Key Management (KEK/DEK/Shamir)
- SHM transport
- Distributed replication (Raft / VSR)

---

## Getting Started

### Requirements

- Rust 2024 edition (1.85+)
- Linux (io_uring), macOS or Windows (portable fallback)
- `ulimit -l <size>` for mlock (recommended)

### Build

```bash
cargo build --release
```

### Run

```bash
cargo run --release
```

### Test

```bash
cargo test --workspace
```

### Configuration

Main parameters in `config.yaml`:

```yaml
server:
  bind-address: "127.0.0.1"
  port: 9100

workers:
  count: 4
  tcp-rb-capacity: 1024

pipeline:
  count: 1
  incoming-rb-capacity: 32768
  incoming-rb-batch-size: 64

partitions:
  count: 16
  initial-accounts-count: 65536
  partition-rb-capacity: 4096
  partition-rb-batch-size: 64
  # accounts-assignment-overrides-path: "overrides.yaml"

protocol:
  metadata-size: 0

batch-accept:
  all-or-nothing: true
  partial-reject-by-transfer-sequence-id: false

decision-maker:
  count: 1
  transfer-hash-table-capacity: 16384
  coordinator-rb-capacity: 65536
  coordinator-rb-batch-size: 128
  flush-done-rb-capacity: 4096

storage:
  flush-timeout-ms: 2
  flush-max-buffer-posting-records: 512
  current-files-directory: "data/ls"
  previous-files-directory: "data/ls"
  max-ls-file-size-mb: 256
  signing-enabled: false
  posting-metadata:
    enabled: false
    record-size: 256
```

---

## Project Structure

```
crates/
  common/           shared utilities (SipHash, CRC32C, random, radix sort)
  config/           YAML configuration, override file loading
  protocol/         binary protocol: framing, handshake, batch codec
  ringbuf/          lock-free ring buffers (MPSC/SPSC), Arena (mmap)
  net/              network layer (mio, TCP acceptor, workers)
  pipeline/         pipeline infrastructure (sequencer, data contracts)
  ledger/           domain logic (actors, decision maker, hash tables)
  storage/          persistence (LS Writer, file formats, signing)
  integration_tests/  end-to-end tests
```

---

## Protocol

Solidus Ledger uses a custom binary protocol over TCP.

See [Protocol Specification](PROTOCOL.md) (coming soon)

Client SDKs (planned):
- Java
- Go
- Node.js
- Rust

---

## License

Apache License 2.0
