use std::sync::Arc;
use std::thread;
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
use storage::io_uring_flush_backend::IoUringFlushBackend;
use storage::portable_flush_backend::PortableFlushBackend;

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
                        2, // TODO max_entries_per_transfer, заглушка для RuleEngine
                    ).expect("Failed to create transfer hash table"),
                )
            }
        ).collect();

    let decision_maker_transfer_hash_tables: Vec<Arc<TransferHashTable>> =
        transfer_hash_tables.iter().map(Arc::clone).collect();

    let handler = LedgerPipelineHandler::new(
        partition_seed_k0,
        partition_seed_k1,
        partitions_count,
        overrides,
        transfer_hash_tables,
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

    let committed_gsn_arena = ringbuf::arena::Arena::new(64)
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

        thread::Builder::new()
            .name(format!("decision-maker-{}", i))
            .spawn(
                move || {
                    let mut decision_maker = DecisionMaker::new(
                        i,
                        decision_maker_coordinator_rb,
                        decision_transfer_hash_table,
                        decision_maker_partition_rbs,
                        coordinator_rb_batch_size,
                    );
                    decision_maker.run();
                }
            ).expect("Failed to spawn decision-maker thread");
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
