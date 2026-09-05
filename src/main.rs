use std::thread;
use std::sync::mpsc;
use common::consts::CPU_CACHE_LINE_SIZE;
use common::generate_random_u64::generate_random_u64;
use config::config::{Config, PartitionAccountAssignmentConfig};
use ledger::ledger_pipeline_handler::LedgerPipelineHandler;
use ledger::partition_accounts_hash_table::PartitionAccountsHashTable;
use ledger::partition_actor::PartitionActor;
use ledger::partition_overrides::PartitionAssignmentsOverrides;
use ledger::partition_version_table::PartitionVersionTable;
use ledger::transfer_hash_table::TransferHashTable;
use ledger::decision_maker::DecisionMaker;
use net::ring_buffer::RingBuffer;
use pipeline::incoming_slot::IncomingSlot;
use pipeline::pipeline::Pipeline;
use pipeline::partition_slot::PartitionSlot;
use pipeline::coordinator_slot::CoordinatorSlot;
use ringbuf::mpsc_ring_buffer::MpscRingBuffer;
use storage::flush_done_slot::FlushDoneSlot;
#[cfg(target_os = "linux")]
use storage::io_uring_flush_backend::IoUringFlushBackend;
use storage::ls_writer_slot::LsWriterSlot;
use storage::portable_flush_backend::PortableFlushBackend;
use storage::ls_writer::LsWriter;
use storage::no_signing_strategy::NoSigningStrategy;
use storage::no_metadata_strategy::NoMetadataStrategy;
use storage::ed25519_signing_strategy::Ed25519SigningStrategy;
use storage::posting_metadata_strategy::PostingMetadataStrategy;
use storage::signing_state::SigningState;
use storage::flush_backend::FlushBackend;
use storage::manifest::Manifest;
use storage::signing_strategy::SigningStrategy;
use storage::metadata_strategy::MetadataStrategy;
use storage::index_builder::{IndexBuilder, IndexBuilderTask};
use storage::file_watcher::{FileWatcher, FileWatcherMessage, FileWatcherSender, ManifestFileInfo};
use storage::tampering_log::TamperingLogError;
use storage::manifest_entry::MANIFEST_STATUS_ROTATED;

fn main() {
    common::crc32c::init();
    let config = Config::load("config.yaml").expect("Failed to load config");
    println!(
        "Config loaded: {} workers, {} pipeline(s), {} partitions, metadata_size={}",
        config.workers.count,
        config.pipeline.count,
        config.partitions.count,
        config.protocol.metadata_size,
    );

    let assignment_overrides_path = config.partitions.accounts_assignment_overrides_path.clone();

    let coordinator_rb_batch_size = config.decision_maker.coordinator_rb_batch_size;

    let partition_seed_k0 = generate_random_u64();
    let partition_seed_k1 = generate_random_u64();
    let partition_accounts_hash_table_seed_k0 = generate_random_u64();
    let partition_accounts_hash_table_seed_k1 = generate_random_u64();
    let transfer_hash_table_seed_k0 = generate_random_u64();
    let transfer_hash_table_seed_k1 = generate_random_u64();
    let partition_version_table_seed_k0 = generate_random_u64();
    let partition_version_table_seed_k1 = generate_random_u64();

    println!(
        "Seeds: partition=({:#018X}, {:#018X}), paht=({:#018X}, {:#018X}), tht=({:#018X}, {:#018X}), pvt=({:#018X}, {:#018X})",
        partition_seed_k0,
        partition_seed_k1,
        partition_accounts_hash_table_seed_k0,
        partition_accounts_hash_table_seed_k1,
        transfer_hash_table_seed_k0,
        transfer_hash_table_seed_k1,
        partition_version_table_seed_k0,
        partition_version_table_seed_k1,
    );

    let partitions_count = config.partitions.count;
    let partition_rb_capacity = config.partitions.partition_rb_capacity;
    let partition_rb_batch = config.partitions.partition_rb_batch_size;
    let initial_accounts_count = config.partitions.initial_accounts_count;
    let incoming_rb_capacity = config.pipeline.incoming_rb_capacity;
    let incoming_rb_batch = config.pipeline.incoming_rb_batch_size;
    let worker_count = config.workers.count;
    let batch_accept_config = config.batch_accept;
    let pipeline_wait_max_yields = config.workers.pipeline_wait_max_yields;
    let max_message_payload_bytes = config.protocol.max_message_payload_bytes;
    let bind_address = format!("{}:{}", config.server.bind_address, config.server.port);

    let overrides = PartitionAssignmentsOverrides::from_map(
        match assignment_overrides_path {
            Some(ref path) => {
                let assignment_config = PartitionAccountAssignmentConfig::load(path)
                    .expect("Failed to load partition overrides");
                assignment_config
            }
            None => PartitionAccountAssignmentConfig::empty(),
        }.convert_to_u8_key_map()
    );

    let partition_rbs: Vec<MpscRingBuffer<PartitionSlot>> = (0..partitions_count)
        .map(
            |_| MpscRingBuffer::<PartitionSlot>::new(partition_rb_capacity)
                .expect("failed to create partition ring buffer")
        ).collect();

    let incoming_rb = MpscRingBuffer::<IncomingSlot>::new(incoming_rb_capacity)
        .expect("Failed to create incoming ring buffer");

    println!(
        "Incoming RB created: capacity={incoming_rb_capacity}, slot_size={} bytes",
        std::mem::size_of::<IncomingSlot>(),
    );

    let decision_maker_shards = config.decision_maker.count;
    let transfer_hash_table_capacity = config.decision_maker.transfer_hash_table_capacity;

    let transfer_hash_tables: Vec<TransferHashTable> = (0..decision_maker_shards)
        .map(
            |_| TransferHashTable::new(
                transfer_hash_table_capacity,
                transfer_hash_table_seed_k0,
                transfer_hash_table_seed_k1,
                2,
            ).expect("Failed to create transfer hash table")
        ).collect();

    let ls_writer_rbs: Vec<MpscRingBuffer<LsWriterSlot>> = (0..decision_maker_shards)
        .map(
            |_| MpscRingBuffer::<LsWriterSlot>::new(config.decision_maker.flush_done_rb_capacity)
                .expect("Failed to create LS Writer Ring Buffer")
        ).collect();

    let flush_done_rbs: Vec<MpscRingBuffer<FlushDoneSlot>> = (0..decision_maker_shards)
        .map(
            |_| MpscRingBuffer::<FlushDoneSlot>::new(config.decision_maker.flush_done_rb_capacity)
                .expect("Failed to create Flush Done Ring Buffer")
        ).collect();

    let coordinator_rbs: Vec<MpscRingBuffer<CoordinatorSlot>> = (0..decision_maker_shards)
        .map(
            |_| MpscRingBuffer::<CoordinatorSlot>::new(
                config.decision_maker.coordinator_rb_capacity
            ).expect("Failed to create coordinator RB")
        ).collect();

    let worker_tcp_queues: Vec<_> = (0..worker_count)
        .map(|_| RingBuffer::new(config.workers.tcp_rb_capacity))
        .collect();

    let committed_gsn_arena = ringbuf::arena::Arena::new(decision_maker_shards * CPU_CACHE_LINE_SIZE)
        .expect("Failed to create committed gsn arena");

    let partition_version_table_arena = ringbuf::arena::Arena::new(partitions_count * CPU_CACHE_LINE_SIZE)
        .expect("Failed to create partition version table arena");
    let partition_version_table_tails_base = partition_version_table_arena.as_ptr() as *mut u64;

    let ls_files_directory = config.storage.current_files_directory.clone();
    std::fs::create_dir_all(&ls_files_directory)
        .expect("Failed to create LS storage files directory");

    let mut manifests: Vec<Manifest> = (0..decision_maker_shards)
        .map(
            |shard_index| {
                if Manifest::exists(&ls_files_directory, shard_index) {
                    Manifest::open(&ls_files_directory, shard_index)
                } else {
                    Manifest::create(&ls_files_directory, shard_index)
                }
            }
        ).collect();

    let mut file_watcher: Option<FileWatcher> = None;
    let file_watcher_sender: Option<FileWatcherSender> = if config.storage.file_protection.watch_enabled {
        let (tx, rx) = mpsc::channel::<FileWatcherMessage>();
        let watch_dir = config.storage.current_files_directory.clone();

        let mut manifest_entries: Vec<ManifestFileInfo> = Vec::new();
        for manifest in manifests.iter_mut() {
            for entry_index in 0..manifest.entries_count() {
                let entry = manifest.read_entry(entry_index);
                if entry.status != MANIFEST_STATUS_ROTATED {
                    continue;
                }
                manifest_entries.push(
                    ManifestFileInfo {
                        file_seq: entry.file_seq,
                        ls_path: format!(
                            "{ls_files_directory}/{}",
                            entry.filename_str(),
                        ),
                        created_at_ns: entry.timestamp_min_ns
                    }
                );
            }
        }

        let (watcher, handles) = match FileWatcher::new(
            0,
            rx,
            watch_dir,
            manifest_entries,
            &config.storage.file_protection,
        ) {
            Ok(watcher_and_handles) => watcher_and_handles,
            Err(TamperingLogError::SecurityHalt { reason }) => {
                eprintln!(
                    "[main] FATAL: SECURITY: file watcher startup halted — {reason}. \
                         The ledger will NOT start; a tamper indication must be \
                         investigated by an operator before restart.",
                );
                std::process::exit(1);
            }
            Err(TamperingLogError::Io(error)) => {
                eprintln!(
                    "[main] FATAL: failed to create file watcher: {error}. \
                         Initiating shutdown.",
                );
                std::process::exit(1);
            }
        };

        file_watcher = Some(watcher);
        Some(FileWatcherSender::new(tx, handles.waker))
    } else {
        None
    };

    let (index_builder_tx, index_builder_rx) = mpsc::channel::<IndexBuilderTask>();

    let mut acceptor = net::acceptor::Acceptor::new(&bind_address, &worker_tcp_queues).unwrap();

    std::panic::set_hook(Box::new(|panic_info| {
        let current_thread = std::thread::current();
        let thread_name = current_thread.name().unwrap_or("unnamed");
        eprintln!("[main] FATAL: thread '{thread_name}' panicked: {panic_info}. Aborting the process (fail-stop policy).");
        std::process::abort();
    }));

    thread::scope(|scope| {
        let pipeline_incoming = &incoming_rb;
        let pipeline_partition_rbs: &[MpscRingBuffer<PartitionSlot>] = &partition_rbs;

        let handler = LedgerPipelineHandler::new(
            partition_seed_k0,
            partition_seed_k1,
            partitions_count,
            overrides,
            &transfer_hash_tables,
            &ls_writer_rbs,
            decision_maker_shards,
        );

        thread::Builder::new()
            .name("pipeline-0".to_string())
            .spawn_scoped(
                scope,
                move || {
                    let mut pipeline = Pipeline::new(
                        0,
                        pipeline_incoming,
                        incoming_rb_batch,
                        pipeline_partition_rbs,
                        handler,
                    );
                    pipeline.run();
                }
            ).expect("Failed to spawn pipeline-0 thread");

        println!("[main] Pipeline thread started");

        for (i, actor_rb) in partition_rbs.iter().enumerate() {
            let partition_accounts_hash_table = PartitionAccountsHashTable::new(
                initial_accounts_count,
                partition_accounts_hash_table_seed_k0,
                partition_accounts_hash_table_seed_k1,
            ).expect("Failed to create partition accounts hash table");

            let partition_version_table = PartitionVersionTable::new(
                initial_accounts_count,
                partition_version_table_seed_k0,
                partition_version_table_seed_k1,
            ).expect("Failed to create partition version table for actor");

            let partition_version_table_tail_addr = unsafe { partition_version_table_tails_base.add(i * 8) } as usize;

            let actor_coordinator_rbs: &[MpscRingBuffer<CoordinatorSlot>] = &coordinator_rbs;
            let actor_ls_writer_rbs: &[MpscRingBuffer<LsWriterSlot>] = &ls_writer_rbs;

            thread::Builder::new()
                .name(format!("partition-{i}"))
                .spawn_scoped(
                    scope,
                    move || {
                        let mut actor = PartitionActor::new(
                            i,
                            actor_rb,
                            partition_accounts_hash_table,
                            partition_version_table,
                            actor_coordinator_rbs,
                            partition_version_table_tail_addr as *mut u64,
                            actor_ls_writer_rbs,
                            partition_rb_batch,
                        );
                        actor.run();
                    }
                ).expect("Failed to spawn partition actor thread");
        }

        println!("[main] {partitions_count} partition actor threads started");

        if let Some(mut watcher) = file_watcher {
            thread::Builder::new()
                .name("file-watcher".to_string())
                .spawn_scoped(
                    scope,
                    move || {
                        watcher.run();
                    }
                ).expect("[main] Failed to spawn file-watcher thread");

            println!("[main] file watcher thread started");
        }

        thread::Builder::new()
            .name("index-builder".to_string())
            .spawn_scoped(
                scope,
                move || {
                    let builder = IndexBuilder::new(0, index_builder_rx, file_watcher_sender);
                    builder.run();
                }
            ).expect("[main] Failed to spawn index-builder thread");

        println!("[main] index builder thread started");

        for (i, manifest) in manifests.into_iter().enumerate() {
            let decision_maker_coordinator_rb = &coordinator_rbs[i];
            let decision_maker_transfer_hash_table = &transfer_hash_tables[i];
            let decision_maker_partition_rbs: &[MpscRingBuffer<PartitionSlot>] = &partition_rbs;

            let decision_maker_ls_writer_rb = &ls_writer_rbs[i];
            let decision_maker_flush_done_rb = &flush_done_rbs[i];

            thread::Builder::new()
                .name(format!("decision-maker-{i}"))
                .spawn_scoped(
                    scope,
                    move || {
                        let mut decision_maker = DecisionMaker::new(
                            i,
                            decision_maker_coordinator_rb,
                            decision_maker_transfer_hash_table,
                            decision_maker_partition_rbs,
                            decision_maker_ls_writer_rb,
                            decision_maker_flush_done_rb,
                            coordinator_rb_batch_size,
                        );
                        decision_maker.run();
                    }
                ).expect("Failed to spawn decision-maker thread");

            let ls_writer_rb = &ls_writer_rbs[i];
            let ls_writer_flush_done_rb = &flush_done_rbs[i];

            let ls_directory = config.storage.current_files_directory.clone();

            let rules_checksum: u32 = 0;

            let max_ls_file_size = config.storage.max_ls_file_size_mb * 1024 * 1024;
            let flush_timeout_ms = config.storage.flush_timeout_ms;
            let flush_max_buffer = config.storage.flush_max_buffer_posting_records;
            let partition_count = config.partitions.count as u16;

            let ls_writer_committed_gsn = unsafe {
                committed_gsn_arena.as_ptr().add(i * CPU_CACHE_LINE_SIZE) as usize
            };

            let signing_enabled = config.storage.signing_enabled;
            let metadata_enabled = config.storage.posting_metadata.enabled;
            let metadata_record_size = config.storage.posting_metadata.record_size;
            let immutable_enabled = config.storage.file_protection.immutable_enabled;

            let checkpoint_prealloc_multiplier = config.storage.checkpoint_prealloc_multiplier;

            #[cfg(target_os = "linux")]
            match IoUringFlushBackend::new() {
                Ok(backend) => {
                    if i == 0 {
                        println!("[main] Using io_uring flush backend");
                    }
                    spawn_with_strategies(
                        scope,
                        i, ls_writer_rb, ls_writer_flush_done_rb, ls_writer_committed_gsn,
                        backend, ls_directory, max_ls_file_size,
                        flush_timeout_ms, flush_max_buffer, partition_count,
                        signing_enabled, metadata_enabled, metadata_record_size,
                        checkpoint_prealloc_multiplier,
                        rules_checksum, manifest,
                        immutable_enabled,
                        index_builder_tx.clone(),
                    );
                }
                Err(error) => {
                    if i == 0 {
                        println!("[main] io_uring not available ({error}), using portable flush backend");
                    }
                    let backend = PortableFlushBackend::new();
                    spawn_with_strategies(
                        scope,
                        i, ls_writer_rb, ls_writer_flush_done_rb, ls_writer_committed_gsn,
                        backend, ls_directory, max_ls_file_size,
                        flush_timeout_ms, flush_max_buffer, partition_count,
                        signing_enabled, metadata_enabled, metadata_record_size,
                        checkpoint_prealloc_multiplier,
                        rules_checksum, manifest,
                        immutable_enabled,
                        index_builder_tx.clone(),
                    );
                }
            }

            #[cfg(not(target_os = "linux"))]
            {
                if i == 0 {
                    println!("[main] Using portable flush backend");
                }
                let backend = PortableFlushBackend::new();
                spawn_with_strategies(
                    scope,
                    i, ls_writer_rb, ls_writer_flush_done_rb, ls_writer_committed_gsn,
                    backend, ls_directory, max_ls_file_size,
                    flush_timeout_ms, flush_max_buffer, partition_count,
                    signing_enabled, metadata_enabled, metadata_record_size,
                    checkpoint_prealloc_multiplier,
                    rules_checksum, manifest,
                    immutable_enabled,
                    index_builder_tx.clone(),
                );
            }
        }

        println!("[main] {decision_maker_shards} decision maker threads started");

        for (i, worker_tcp_queue) in worker_tcp_queues.iter().enumerate() {
            let worker_pipeline_rb = &incoming_rb;

            thread::Builder::new()
                .name(format!("worker-{i}"))
                .spawn_scoped(
                    scope,
                    move || {
                        let mut worker = net::worker::Worker::new(
                            i,
                            worker_tcp_queue,
                            worker_pipeline_rb,
                            batch_accept_config,
                            pipeline_wait_max_yields,
                            max_message_payload_bytes,
                        ).unwrap();
                        println!("Worker {i} started");
                        worker.run().unwrap();
                    }
                ).expect("Failed to spawn worker thread");
        }

        println!("Listening on {bind_address}");
        thread::Builder::new()
            .name("acceptor".to_string())
            .spawn_scoped(
                scope,
                move || {
                    acceptor.run().unwrap();
                }
            ).expect("Failed to spawn acceptor thread");
    });
}

fn spawn_ls_writer_thread<'scope, 'env, T, S, M>(
    scope: &'scope thread::Scope<'scope, 'env>,
    id: usize,
    ls_writer_rb: &'scope MpscRingBuffer<LsWriterSlot>,
    flush_done_rb: &'scope MpscRingBuffer<FlushDoneSlot>,
    commit_gsn_addr: usize,
    backend: T,
    signing: S,
    metadata: M,
    ls_directory: String,
    max_ls_file_size: usize,
    flush_timeout_ms: u64,
    flush_max_buffer_posting_records: usize,
    partition_count: u16,
    checkpoint_prealloc_multiplier: usize,
    rules_checksum: u32,
    manifest: Manifest,
    immutable_enabled: bool,
    index_builder_tx: mpsc::Sender<IndexBuilderTask>,
) where
    T: FlushBackend + Send + 'scope,
    S: SigningStrategy + Send + 'scope,
    M: MetadataStrategy + Send + 'scope, {
    thread::Builder::new()
        .name(format!("ls-writer-{id}"))
        .spawn_scoped(
            scope,
            move || {
                let mut writer = LsWriter::new(
                    id,
                    ls_writer_rb,
                    flush_done_rb,
                    commit_gsn_addr as *mut u64,
                    backend,
                    signing,
                    metadata,
                    ls_directory,
                    max_ls_file_size,
                    1024,
                    generate_random_u64(),
                    generate_random_u64(),
                    CPU_CACHE_LINE_SIZE,
                    flush_timeout_ms,
                    flush_max_buffer_posting_records,
                    partition_count,
                    checkpoint_prealloc_multiplier,
                    rules_checksum,
                    manifest,
                    immutable_enabled,
                    index_builder_tx,
                );
                writer.run();
            }
        ).expect("[main] Failed to spawn ls-writer thread");
}

fn spawn_with_strategies<'scope, 'env, T: FlushBackend + Send + 'scope>(
    scope: &'scope thread::Scope<'scope, 'env>,
    id: usize,
    ls_writer_rb: &'scope MpscRingBuffer<LsWriterSlot>,
    flush_done_rb: &'scope MpscRingBuffer<FlushDoneSlot>,
    committed_gsn_addr: usize,
    backend: T,
    ls_directory: String,
    max_ls_file_size: usize,
    flush_timeout_ms: u64,
    flush_max_buffer: usize,
    partition_count: u16,
    signing_enabled: bool,
    metadata_enabled: bool,
    metadata_record_size: usize,
    checkpoint_prealloc_multiplier: usize,
    rules_checksum: u32,
    manifest: Manifest,
    immutable_enabled: bool,
    index_builder_tx: mpsc::Sender<IndexBuilderTask>,
) {
    if signing_enabled {
        let key = ed25519_dalek::SigningKey::from_bytes(&[0x42u8; 32]);
        let genesis = [0u8; 32];
        let signing = Ed25519SigningStrategy::new(
            SigningState::new(key, genesis),
            flush_max_buffer,
        );

        if metadata_enabled {
            let metadata = PostingMetadataStrategy::new(metadata_record_size, flush_max_buffer);
            spawn_ls_writer_thread(
                scope,
                id, ls_writer_rb, flush_done_rb, committed_gsn_addr,
                backend, signing, metadata,
                ls_directory, max_ls_file_size,
                flush_timeout_ms, flush_max_buffer, partition_count,
                checkpoint_prealloc_multiplier,
                rules_checksum, manifest,
                immutable_enabled,
                index_builder_tx,
            );
        } else {
            spawn_ls_writer_thread(
                scope,
                id, ls_writer_rb, flush_done_rb, committed_gsn_addr,
                backend, signing, NoMetadataStrategy,
                ls_directory, max_ls_file_size,
                flush_timeout_ms, flush_max_buffer, partition_count,
                checkpoint_prealloc_multiplier,
                rules_checksum, manifest,
                immutable_enabled,
                index_builder_tx,
            );
        }
    } else {
        if metadata_enabled {
            let metadata = PostingMetadataStrategy::new(metadata_record_size, flush_max_buffer);
            spawn_ls_writer_thread(
                scope,
                id, ls_writer_rb, flush_done_rb, committed_gsn_addr,
                backend, NoSigningStrategy, metadata,
                ls_directory, max_ls_file_size,
                flush_timeout_ms, flush_max_buffer, partition_count,
                checkpoint_prealloc_multiplier,
                rules_checksum, manifest,
                immutable_enabled,
                index_builder_tx,
            );
        } else {
            spawn_ls_writer_thread(
                scope,
                id, ls_writer_rb, flush_done_rb, committed_gsn_addr,
                backend, NoSigningStrategy, NoMetadataStrategy,
                ls_directory, max_ls_file_size,
                flush_timeout_ms, flush_max_buffer, partition_count,
                checkpoint_prealloc_multiplier,
                rules_checksum, manifest,
                immutable_enabled,
                index_builder_tx
            );
        }
    }
}
