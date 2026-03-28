use std::sync::Arc;
use std::thread;
use common::consts::CPU_CACHE_LINE_SIZE;
use common::generate_random_u64::generate_random_u64;
use config::{Config, PartitionAccountAssignmentConfig};
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
use storage::signing_strategy::SigningStrategy;
use storage::metadata_strategy::MetadataStrategy;

fn main() {
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

    let partition_rb: Vec<Arc<MpscRingBuffer<PartitionSlot>>> = (0..partitions_count)
        .map(
            |_| Arc::new(
                MpscRingBuffer::<PartitionSlot>::new(partition_rb_capacity)
                .expect("failed to create partition ring buffer")
            )
        ).collect();

    let actor_rb_vec: Vec<Arc<MpscRingBuffer<PartitionSlot>>> = partition_rb
        .iter()
        .map(Arc::clone)
        .collect();

    let decision_maker_partition_rbs: Vec<Arc<MpscRingBuffer<PartitionSlot>>> =
        partition_rb.iter().map(Arc::clone).collect();

    let incoming_rb = Arc::new(
        MpscRingBuffer::<IncomingSlot>::new(incoming_rb_capacity)
            .expect("Failed to create incoming ring buffer"),
    );

    println!(
        "Incoming RB created: capacity={}, slot_size={} bytes",
        incoming_rb_capacity,
        std::mem::size_of::<IncomingSlot>(),
    );

    let pipeline_incoming = Arc::clone(&incoming_rb);

    let decision_maker_shards = config.decision_maker.count;
    let transfer_hash_table_capacity = config.decision_maker.transfer_hash_table_capacity;

    let transfer_hash_tables: Vec<Arc<TransferHashTable>> = (0..decision_maker_shards)
        .map(
            |_| {
                Arc::new(
                    TransferHashTable::new(
                        transfer_hash_table_capacity,
                        transfer_hash_table_seed_k0,
                        transfer_hash_table_seed_k1,
                        2,
                    ).expect("Failed to create transfer hash table"),
                )
            }
        ).collect();

    let decision_maker_transfer_hash_tables: Vec<Arc<TransferHashTable>> =
        transfer_hash_tables.iter().map(Arc::clone).collect();

    let ls_writer_rbs_source: Vec<Arc<MpscRingBuffer<LsWriterSlot>>> = (0..decision_maker_shards)
        .map(
            |_| Arc::new(
                MpscRingBuffer::<LsWriterSlot>::new(config.decision_maker.flush_done_rb_capacity)
                    .expect("Failed to create LS Writer Ring Buffer")
            )
        ).collect();

    let flush_done_rbs: Vec<Arc<MpscRingBuffer<FlushDoneSlot>>> = (0..decision_maker_shards)
        .map(
            |_| Arc::new(
                MpscRingBuffer::<FlushDoneSlot>::new(config.decision_maker.flush_done_rb_capacity)
                    .expect("Failed to create Flush Done Ring Buffer")
            )
        ).collect();

    let handler_ls_writer_rbs: Vec<Arc<MpscRingBuffer<LsWriterSlot>>> =
        ls_writer_rbs_source.iter().map(Arc::clone).collect();

    let handler = LedgerPipelineHandler::new(
        partition_seed_k0,
        partition_seed_k1,
        partitions_count,
        overrides,
        transfer_hash_tables,
        handler_ls_writer_rbs,
        decision_maker_shards,
    );

    thread::Builder::new()
        .name("pipeline-0".to_string())
        .spawn(
            move || {
                let mut pipeline = Pipeline::new(
                    0,
                    pipeline_incoming,
                    incoming_rb_batch,
                    partition_rb,
                    handler,
                );
                pipeline.run();
            }
        ).expect("Failed to spawn pipeline-0 thread");

    println!("[main] Pipeline thread started");

    let coordinator_rbs: Vec<Arc<MpscRingBuffer<CoordinatorSlot>>> =
        (0..decision_maker_shards)
            .map(
                |_| Arc::new(
                    MpscRingBuffer::<CoordinatorSlot>::new(
                        config.decision_maker.coordinator_rb_capacity
                    ).expect("Failed to create coordinator RB")
                )
            ).collect();

    let committed_gsn_arena = ringbuf::arena::Arena::new(decision_maker_shards * CPU_CACHE_LINE_SIZE)
        .expect("Failed to create committed gsn arena");
    let global_committed_gsn: *mut u64 = committed_gsn_arena.as_ptr() as *mut u64;
    
    let partition_version_table_arena = ringbuf::arena::Arena::new(partitions_count * 64)
        .expect("Failed to create partition version table arena");
    let partition_version_table_tails_base = partition_version_table_arena.as_ptr() as *mut u64;

    for (i, actor_rb) in actor_rb_vec.into_iter().enumerate() {
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

        let actor_coordinator_rbs: Vec<Arc<MpscRingBuffer<CoordinatorSlot>>> =
            coordinator_rbs.iter().map(Arc::clone).collect();

        let actor_ls_writer_rbs: Vec<Arc<MpscRingBuffer<LsWriterSlot>>> =
            ls_writer_rbs_source.iter().map(Arc::clone).collect();

        thread::Builder::new()
            .name(format!("partition-{}", i))
            .spawn(
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
    println!("[main] {} partition actor threads started", partitions_count);

    for i in 0..decision_maker_shards {
        let decision_maker_coordinator_rb = Arc::clone(&coordinator_rbs[i]);
        let decision_transfer_hash_table = Arc::clone(&decision_maker_transfer_hash_tables[i]);
        let decision_maker_partition_rbs: Vec<Arc<MpscRingBuffer<PartitionSlot>>> =
            decision_maker_partition_rbs.iter().map(Arc::clone).collect();

        let dm_ls_writer_rb = Arc::clone(&ls_writer_rbs_source[i]);
        let dm_flush_done_rb = Arc::clone(&flush_done_rbs[i]);

        thread::Builder::new()
            .name(format!("decision-maker-{}", i))
            .spawn(
                move || {
                    let mut decision_maker = DecisionMaker::new(
                        i,
                        decision_maker_coordinator_rb,
                        decision_transfer_hash_table,
                        decision_maker_partition_rbs,
                        dm_ls_writer_rb,
                        dm_flush_done_rb,
                        coordinator_rb_batch_size,
                    );
                    decision_maker.run();
                }
            ).expect("Failed to spawn decision-maker thread");

        std::fs::create_dir_all(&config.storage.current_files_directory)
            .expect("Failed to create LS storage files directory");

        let ls_writer_rb = Arc::clone(&ls_writer_rbs_source[i]);
        let ls_writer_flush_done_rb = Arc::clone(&flush_done_rbs[i]);

        let ls_file_path = format!(
            "{}/ls_{}_{}.ls",
            config.storage.current_files_directory, i, 0
        );
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
        
        let checkpoint_prealloc_multiplier = config.storage.checkpoint_prealloc_multiplier;

        #[cfg(target_os = "linux")]
        match IoUringFlushBackend::new() {
            Ok(backend) => {
                if i == 0 {
                    println!("[main] Using io_uring flush backend");
                }
                spawn_with_strategies(
                    i, ls_writer_rb, ls_writer_flush_done_rb, ls_writer_committed_gsn,
                    backend, ls_file_path, max_ls_file_size,
                    flush_timeout_ms, flush_max_buffer, partition_count,
                    signing_enabled, metadata_enabled, metadata_record_size,
                    checkpoint_prealloc_multiplier,
                );
            }
            Err(e) => {
                if i == 0 {
                    println!("[main] io_uring not available ({}), using portable flush backend", e);
                }
                let backend = PortableFlushBackend::new();
                spawn_with_strategies(
                    i, ls_writer_rb, ls_writer_flush_done_rb, ls_writer_committed_gsn,
                    backend, ls_file_path, max_ls_file_size,
                    flush_timeout_ms, flush_max_buffer, partition_count,
                    signing_enabled, metadata_enabled, metadata_record_size,
                    checkpoint_prealloc_multiplier,
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
                i, ls_writer_rb, ls_writer_flush_done_rb, ls_writer_committed_gsn,
                backend, ls_file_path, max_ls_file_size,
                flush_timeout_ms, flush_max_buffer, partition_count,
                signing_enabled, metadata_enabled, metadata_record_size,
            );
        }
    }

    println!("[main] {} decision maker threads started", decision_maker_shards);
    
    

    let mut queues = Vec::new();

    for i in 0..worker_count {
        let queue = Arc::new(RingBuffer::new(config.workers.tcp_rb_capacity));
        queues.push(queue.clone());
        let worker_pipeline_rb = Arc::clone(&incoming_rb);

        thread::spawn(
            move || {
                let mut worker = net::worker::Worker::new(
                    i,
                    queue,
                    worker_pipeline_rb,
                    batch_accept_config,
                ).unwrap();
                println!("Worker {} started", i);
                worker.run().unwrap();
            }
        );
    }
    println!("Listening on {}", bind_address);
    let mut acceptor = net::acceptor::Acceptor::new(&bind_address, queues).unwrap();
    acceptor.run().unwrap();
}

fn spawn_ls_writer_thread<T, S, M>(
    id: usize,
    ls_writer_rb: Arc<MpscRingBuffer<LsWriterSlot>>,
    flush_done_rb: Arc<MpscRingBuffer<FlushDoneSlot>>,
    commit_gsn_addr: usize,
    backend: T,
    signing: S,
    metadata: M,
    ls_file_path: String,
    max_ls_file_size: usize,
    flush_timeout_ms: u64,
    flush_max_buffer_posting_records: usize,
    partition_count: u16,
    checkpoint_prealloc_multiplier: usize,
) where
    T: FlushBackend + Send + 'static,
    S: SigningStrategy + Send + 'static,
    M: MetadataStrategy + Send + 'static, {
    thread::Builder::new()
        .name(format!("ls-writer-{}", id))
        .spawn(
            move || {
                let mut writer = LsWriter::new(
                    id,
                    ls_writer_rb,
                    flush_done_rb,
                    commit_gsn_addr as *mut u64,
                    backend,
                    signing,
                    metadata,
                    ls_file_path,
                    max_ls_file_size,
                    1024,
                    generate_random_u64(),
                    generate_random_u64(),
                    CPU_CACHE_LINE_SIZE,
                    flush_timeout_ms,
                    flush_max_buffer_posting_records,
                    partition_count,
                    checkpoint_prealloc_multiplier,
                );
                writer.run();
            }
        ).expect("[main] Failed to spawn ls-writer thread");
}

fn spawn_with_strategies<T: FlushBackend + Send + 'static>(
    id: usize,
    ls_writer_rb: Arc<MpscRingBuffer<LsWriterSlot>>,
    flush_done_rb: Arc<MpscRingBuffer<FlushDoneSlot>>,
    committed_gsn_addr: usize,
    backend: T,
    ls_file_path: String,
    max_ls_file_size: usize,
    flush_timeout_ms: u64,
    flush_max_buffer: usize,
    partition_count: u16,
    signing_enabled: bool,
    metadata_enabled: bool,
    metadata_record_size: usize,
    checkpoint_prealloc_multiplier: usize,
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
                id, ls_writer_rb, flush_done_rb, committed_gsn_addr,
                backend, signing, metadata,
                ls_file_path, max_ls_file_size,
                flush_timeout_ms, flush_max_buffer, partition_count,
                checkpoint_prealloc_multiplier,
            );
        } else {
            spawn_ls_writer_thread(
                id, ls_writer_rb, flush_done_rb, committed_gsn_addr,
                backend, signing, NoMetadataStrategy,
                ls_file_path, max_ls_file_size,
                flush_timeout_ms, flush_max_buffer, partition_count,
                checkpoint_prealloc_multiplier,
            );
        }
    } else {
        if metadata_enabled {
            let metadata = PostingMetadataStrategy::new(metadata_record_size, flush_max_buffer);
            spawn_ls_writer_thread(
                id, ls_writer_rb, flush_done_rb, committed_gsn_addr,
                backend, NoSigningStrategy, metadata,
                ls_file_path, max_ls_file_size,
                flush_timeout_ms, flush_max_buffer, partition_count,
                checkpoint_prealloc_multiplier,
            );
        } else {
            spawn_ls_writer_thread(
                id, ls_writer_rb, flush_done_rb, committed_gsn_addr,
                backend, NoSigningStrategy, NoMetadataStrategy,
                ls_file_path, max_ls_file_size,
                flush_timeout_ms, flush_max_buffer, partition_count,
                checkpoint_prealloc_multiplier,
            );
        }
    }
}