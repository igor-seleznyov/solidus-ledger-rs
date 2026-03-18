use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use ledger::decision_maker::DecisionMaker;
use ledger::partition_accounts_hash_table::PartitionAccountsHashTable;
use ledger::partition_actor::PartitionActor;
use ledger::partition_version_table::PartitionVersionTable;
use pipeline::coordinator_slot::CoordinatorSlot;

use config::BatchAcceptConfig;
use net::acceptor::Acceptor;
use net::ring_buffer::RingBuffer;
use net::worker::Worker;
use pipeline::incoming_slot::IncomingSlot;
use ringbuf::mpsc_ring_buffer::MpscRingBuffer;
use protocol::codec::Codec;
use protocol::consts::*;
use protocol::transfer::TRANSFER_BASE_SIZE;
use protocol::consts::REJECT_SEQUENCE_GROUP_FAILED;
use ledger::ledger_pipeline_handler::LedgerPipelineHandler;
use ledger::partition_overrides::PartitionAssignmentsOverrides;
use pipeline::partition_slot::PartitionSlot;
use ledger::transfer_hash_table::TransferHashTable;
use pipeline::pipeline::Pipeline;

const PARTITION_SEED_K0: u64 = 0x0123456789ABCDEF;
const PARTITION_SEED_K1: u64 = 0xFEDCBA9876543210;

struct TestServer {
    addr: String,
    pipeline_rb: Arc<MpscRingBuffer<IncomingSlot>>,
}

struct FullTestServer {
    addr: String,
    pipeline_rb: Arc<MpscRingBuffer<IncomingSlot>>,
    transfer_hash_tables: Vec<Arc<TransferHashTable>>,
}

fn start_full_server(batch_accept: BatchAcceptConfig) -> FullTestServer {
    let partitions_num = 4;
    let dm_shards = 1;

    // Partition RBs
    let partition_rb: Vec<Arc<MpscRingBuffer<PartitionSlot>>> = (0..partitions_num)
        .map(|_| Arc::new(MpscRingBuffer::<PartitionSlot>::new(64).unwrap()))
        .collect();

    // Клоны для Actor'ов и DM
    let actor_rbs: Vec<Arc<MpscRingBuffer<PartitionSlot>>> =
        partition_rb.iter().map(Arc::clone).collect();
    let dm_partition_rbs: Vec<Arc<MpscRingBuffer<PartitionSlot>>> =
        partition_rb.iter().map(Arc::clone).collect();

    // Coordinator RBs
    let coordinator_rbs: Vec<Arc<MpscRingBuffer<CoordinatorSlot>>> = (0..dm_shards)
        .map(|_| Arc::new(MpscRingBuffer::<CoordinatorSlot>::new(64).unwrap()))
        .collect();

    // THT
    let transfer_hash_tables: Vec<Arc<TransferHashTable>> = (0..dm_shards)
        .map(|_| Arc::new(TransferHashTable::new(64, PARTITION_SEED_K0, PARTITION_SEED_K1, 8).unwrap()))
        .collect();
    let dm_thts: Vec<Arc<TransferHashTable>> =
        transfer_hash_tables.iter().map(Arc::clone).collect();

    // Overrides
    let overrides = PartitionAssignmentsOverrides::empty();

    // Handler
    let handler = LedgerPipelineHandler::new(
        PARTITION_SEED_K0, PARTITION_SEED_K1, partitions_num,
        overrides, transfer_hash_tables.clone(), dm_shards,
    );

    // Incoming RB
    let pipeline_rb = Arc::new(
        MpscRingBuffer::<IncomingSlot>::new(1024).unwrap(),
    );

    // Pipeline thread
    let pipeline_incoming = Arc::clone(&pipeline_rb);
    thread::spawn(move || {
        let mut pipeline = Pipeline::new(0, pipeline_incoming, 64, partition_rb, handler);
        pipeline.run();
    });

    // PVT tails
    let pvt_tails_arena = ringbuf::arena::Arena::new(partitions_num * 64).unwrap();
    let pvt_tails_base = pvt_tails_arena.as_ptr() as *mut u64;

    // Actor threads
    for (i, actor_rb) in actor_rbs.into_iter().enumerate() {
        let paht = PartitionAccountsHashTable::new(64, PARTITION_SEED_K0, PARTITION_SEED_K1).unwrap();
        let pvt = PartitionVersionTable::new(64, PARTITION_SEED_K0, PARTITION_SEED_K1).unwrap();
        let pvt_tail_addr = unsafe { pvt_tails_base.add(i * 8) } as usize;
        let actor_coord_rbs: Vec<Arc<MpscRingBuffer<CoordinatorSlot>>> =
            coordinator_rbs.iter().map(Arc::clone).collect();

        thread::spawn(move || {
            let mut actor = PartitionActor::new(
                i, actor_rb, paht, pvt, actor_coord_rbs,
                pvt_tail_addr as *mut u64, 64,
            );
            actor.run();
        });
    }

    // DM threads
    for i in 0..dm_shards {
        let dm_coord_rb = Arc::clone(&coordinator_rbs[i]);
        let dm_tht = Arc::clone(&dm_thts[i]);
        let dm_prbs: Vec<Arc<MpscRingBuffer<PartitionSlot>>> =
            dm_partition_rbs.iter().map(Arc::clone).collect();

        thread::spawn(move || {
            let mut dm = DecisionMaker::new(i, dm_coord_rb, dm_tht, dm_prbs, 64);
            dm.run();
        });
    }

    // Worker + Acceptor
    let queue = Arc::new(RingBuffer::new(64));
    let worker_queue = Arc::clone(&queue);
    let worker_rb = Arc::clone(&pipeline_rb);
    thread::spawn(move || {
        let mut worker = Worker::new(0, worker_queue, worker_rb, batch_accept).unwrap();
        worker.run().unwrap();
    });

    let acceptor_queues = vec![Arc::clone(&queue)];
    let mut acceptor = Acceptor::new("127.0.0.1:0", acceptor_queues).unwrap();
    let addr = acceptor.local_addr().unwrap().to_string();

    thread::spawn(move || {
        acceptor.run().unwrap();
    });

    thread::sleep(Duration::from_millis(50));

    FullTestServer {
        addr,
        pipeline_rb,
        transfer_hash_tables,
    }
}

#[test]
fn handshake_ok() {
    let server = start_server(BatchAcceptConfig {
        all_or_nothing: true,
        partial_reject_by_transfer_sequence_id: false,
    });

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

    let status = send_handshake(&mut stream);
    assert_eq!(status, HS_OK);
}

#[test]
fn batch_all_valid_accepted() {
    let server = start_server(BatchAcceptConfig {
        all_or_nothing: true,
        partial_reject_by_transfer_sequence_id: false,
    });

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let status = send_handshake(&mut stream);
    assert_eq!(status, HS_OK);

    let batch_id = uuid_from_u64(100);
    let t1 = make_transfer(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 500);
    let t2 = make_transfer(uuid_from_u64(2), uuid_from_u64(10), uuid_from_u64(20), 300);

    let (status, reject_count, _) = send_batch(&mut stream, batch_id, &[t1, t2]);

    assert_eq!(status, BATCH_ACCEPTED);
    assert_eq!(reject_count, 0);

    // Проверяем что данные дошли до Pipeline RB
    // Даём Pipeline немного времени обработать
    // (Pipeline thread не запущен в этом тесте — читаем напрямую из RB)
    thread::sleep(Duration::from_millis(50));

    let drain = server.pipeline_rb.drain_batch(64);
    assert_eq!(drain.len(), 2, "expected 2 transfers in pipeline RB");

    // Проверяем первый трансфер
    let slot0 = drain.slot(0);
    assert_eq!(slot0.transfer_id, uuid_from_u64(1));
    assert_eq!(slot0.debit_account_id, uuid_from_u64(10));
    assert_eq!(slot0.credit_account_id, uuid_from_u64(20));
    assert_eq!(i64::from_be_bytes(slot0.amount), 500);
    assert_eq!(slot0.batch_id, batch_id);

    // Проверяем второй трансфер
    let slot1 = drain.slot(1);
    assert_eq!(slot1.transfer_id, uuid_from_u64(2));
    assert_eq!(i64::from_be_bytes(slot1.amount), 300);
    assert_eq!(slot1.batch_id, batch_id);

    drain.release();
}

#[test]
fn batch_all_or_nothing_with_invalid_rejected() {
    let server = start_server(BatchAcceptConfig {
        all_or_nothing: true,
        partial_reject_by_transfer_sequence_id: false,
    });

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(200);
    let t1 = make_transfer(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 500);
    let t2 = make_transfer(uuid_from_u64(2), uuid_from_u64(10), uuid_from_u64(20), 0); // amount=0!

    let (status, reject_count, rejects) = send_batch(&mut stream, batch_id, &[t1, t2]);

    assert_eq!(status, BATCH_FAILED);
    assert_eq!(reject_count, 1);
    assert_eq!(rejects[0].0, REJECT_INVALID_AMOUNT);
    assert_eq!(rejects[0].1, uuid_from_u64(2)); // transfer_id второго трансфера

    // Pipeline RB должен быть пуст — all-or-nothing отклонил весь batch
    thread::sleep(Duration::from_millis(50));
    let drain = server.pipeline_rb.drain_batch(64);
    assert_eq!(drain.len(), 0, "pipeline RB should be empty after rejected batch");
    drain.release();
}

#[test]
fn batch_partial_with_rejects() {
    let server = start_server(BatchAcceptConfig {
        all_or_nothing: false,
        partial_reject_by_transfer_sequence_id: false,
    });

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(300);
    let t1 = make_transfer(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 500); // OK
    let t2 = make_transfer(uuid_from_u64(2), uuid_from_u64(10), uuid_from_u64(20), 0);   // BAD
    let t3 = make_transfer(uuid_from_u64(3), uuid_from_u64(10), uuid_from_u64(20), 200); // OK

    let (status, reject_count, rejects) = send_batch(
        &mut stream, batch_id, &[t1, t2, t3],
    );

    assert_eq!(status, BATCH_WITH_REJECTS);
    assert_eq!(reject_count, 1);
    assert_eq!(rejects[0].0, REJECT_INVALID_AMOUNT);
    assert_eq!(rejects[0].1, uuid_from_u64(2));

    // В Pipeline RB должны быть 2 валидных трансфера
    thread::sleep(Duration::from_millis(50));
    let drain = server.pipeline_rb.drain_batch(64);
    assert_eq!(drain.len(), 2, "expected 2 valid transfers in pipeline RB");

    assert_eq!(drain.slot(0).transfer_id, uuid_from_u64(1));
    assert_eq!(i64::from_be_bytes(drain.slot(0).amount), 500);

    assert_eq!(drain.slot(1).transfer_id, uuid_from_u64(3));
    assert_eq!(i64::from_be_bytes(drain.slot(1).amount), 200);

    drain.release();
}

#[test]
fn batch_duplicate_transfer_id_rejected() {
    let server = start_server(BatchAcceptConfig {
        all_or_nothing: true,
        partial_reject_by_transfer_sequence_id: false,
    });

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(400);
    let same_id = uuid_from_u64(42);
    let t1 = make_transfer(same_id, uuid_from_u64(10), uuid_from_u64(20), 100);
    let t2 = make_transfer(same_id, uuid_from_u64(10), uuid_from_u64(20), 200); // дубль!

    let (status, reject_count, rejects) = send_batch(&mut stream, batch_id, &[t1, t2]);

    assert_eq!(status, BATCH_FAILED);
    assert_eq!(reject_count, 1);
    assert_eq!(rejects[0].0, REJECT_DUPLICATE_TRANSFER_ID_IN_BATCH);

    // Pipeline RB пуст
    thread::sleep(Duration::from_millis(50));
    let drain = server.pipeline_rb.drain_batch(64);
    assert_eq!(drain.len(), 0);
    drain.release();
}

#[test]
fn batch_zero_transfer_id_rejected() {
    let server = start_server(BatchAcceptConfig {
        all_or_nothing: true,
        partial_reject_by_transfer_sequence_id: false,
    });

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(500);
    let t1 = make_transfer([0u8; 16], uuid_from_u64(10), uuid_from_u64(20), 100); // zero id!

    let (status, reject_count, rejects) = send_batch(&mut stream, batch_id, &[t1]);

    assert_eq!(status, BATCH_FAILED);
    assert_eq!(reject_count, 1);
    assert_eq!(rejects[0].0, REJECT_INVALID_TRANSFER_ID);
}

#[test]
fn batch_zero_account_id_rejected() {
    let server = start_server(BatchAcceptConfig {
        all_or_nothing: true,
        partial_reject_by_transfer_sequence_id: false,
    });

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(600);
    let t1 = make_transfer(uuid_from_u64(1), [0u8; 16], uuid_from_u64(20), 100); // zero debit!

    let (status, reject_count, rejects) = send_batch(&mut stream, batch_id, &[t1]);

    assert_eq!(status, BATCH_FAILED);
    assert_eq!(reject_count, 1);
    assert_eq!(rejects[0].0, REJECT_INVALID_ACCOUNT_ID);
}

#[test]
fn handshake_unsupported_version() {
    let server = start_server(BatchAcceptConfig {
        all_or_nothing: true,
        partial_reject_by_transfer_sequence_id: false,
    });

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

    // Отправляем handshake с version=99
    let client_id = [1u8; 16];
    let mut payload = Vec::new();
    payload.extend_from_slice(&client_id);
    payload.push(CONN_COMMAND);
    payload.extend_from_slice(&99u16.to_be_bytes()); // unsupported!

    let mut frame = Vec::new();
    Codec::encode_request(MSG_HANDSHAKE_REQUEST, &payload, &mut frame);
    stream.write_all(&frame).unwrap();

    let mut resp_buf = [0u8; 256];
    let n = stream.read(&mut resp_buf).unwrap();
    assert!(n >= HEADER_SIZE + 1);

    let status = resp_buf[HEADER_SIZE];
    assert_eq!(status, HS_UNSUPPORTED_VERSION);
}

#[test]
fn batch_partial_sequence_group_rejected() {
    let server = start_server(BatchAcceptConfig {
        all_or_nothing: false,
        partial_reject_by_transfer_sequence_id: true,
    });

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(800);
    let seq_a = uuid_from_u64(100);   // группа A
    let seq_b = uuid_from_u64(200);   // группа B

    // Группа A: два трансфера, оба валидны
    let t1 = make_transfer_with_seq(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 500, seq_a);
    let t2 = make_transfer_with_seq(uuid_from_u64(2), uuid_from_u64(10), uuid_from_u64(20), 300, seq_a);

    // Группа B: два трансфера, второй невалидный (amount=0)
    let t3 = make_transfer_with_seq(uuid_from_u64(3), uuid_from_u64(10), uuid_from_u64(20), 100, seq_b);
    let t4 = make_transfer_with_seq(uuid_from_u64(4), uuid_from_u64(10), uuid_from_u64(20), 0,   seq_b);

    let (status, reject_count, rejects) = send_batch(
        &mut stream, batch_id, &[t1, t2, t3, t4],
    );

    assert_eq!(status, BATCH_WITH_REJECTS);
    // t4 отклонён по REJECT_INVALID_AMOUNT, t3 — по REJECT_SEQUENCE_GROUP_FAILED
    assert_eq!(reject_count, 2);

    // Ищем reject для t4 (причина ошибки)
    let t4_reject = rejects.iter().find(|(_, tid)| *tid == uuid_from_u64(4)).unwrap();
    assert_eq!(t4_reject.0, REJECT_INVALID_AMOUNT);

    // Ищем reject для t3 (невиновный, но группа отклонена)
    let t3_reject = rejects.iter().find(|(_, tid)| *tid == uuid_from_u64(3)).unwrap();
    assert_eq!(t3_reject.0, REJECT_SEQUENCE_GROUP_FAILED);

    // В Pipeline RB — только группа A (2 трансфера)
    thread::sleep(Duration::from_millis(50));
    let drain = server.pipeline_rb.drain_batch(64);
    assert_eq!(drain.len(), 2, "only group A should be in pipeline RB");
    assert_eq!(drain.slot(0).transfer_id, uuid_from_u64(1));
    assert_eq!(drain.slot(1).transfer_id, uuid_from_u64(2));
    drain.release();
}

#[test]
fn batch_partial_zero_sequence_not_grouped() {
    let server = start_server(BatchAcceptConfig {
        all_or_nothing: false,
        partial_reject_by_transfer_sequence_id: true,
    });

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(900);

    // Два трансфера с нулевым sequence_id, один невалидный
    let t1 = make_transfer_with_seq(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 500, [0u8; 16]);
    let t2 = make_transfer_with_seq(uuid_from_u64(2), uuid_from_u64(10), uuid_from_u64(20), 0,   [0u8; 16]); // BAD

    let (status, reject_count, rejects) = send_batch(
        &mut stream, batch_id, &[t1, t2],
    );

    assert_eq!(status, BATCH_WITH_REJECTS);
    assert_eq!(reject_count, 1); // только t2, t1 НЕ отклонён
    assert_eq!(rejects[0].0, REJECT_INVALID_AMOUNT);
    assert_eq!(rejects[0].1, uuid_from_u64(2));

    // t1 в Pipeline RB
    thread::sleep(Duration::from_millis(50));
    let drain = server.pipeline_rb.drain_batch(64);
    assert_eq!(drain.len(), 1);
    assert_eq!(drain.slot(0).transfer_id, uuid_from_u64(1));
    drain.release();
}

#[test]
fn batch_partial_all_groups_rejected() {
    let server = start_server(BatchAcceptConfig {
        all_or_nothing: false,
        partial_reject_by_transfer_sequence_id: true,
    });

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(1000);
    let seq_a = uuid_from_u64(100);

    // Одна группа, один трансфер невалиден — вся группа (= весь batch) отклонена
    let t1 = make_transfer_with_seq(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 500, seq_a);
    let t2 = make_transfer_with_seq(uuid_from_u64(2), uuid_from_u64(10), uuid_from_u64(20), 0,   seq_a);

    let (status, reject_count, _) = send_batch(
        &mut stream, batch_id, &[t1, t2],
    );

    assert_eq!(status, BATCH_FAILED);
    assert_eq!(reject_count, 2);

    // Pipeline RB пуст
    thread::sleep(Duration::from_millis(50));
    let drain = server.pipeline_rb.drain_batch(64);
    assert_eq!(drain.len(), 0);
    drain.release();
}

//-----------------THT-------------------

#[test]
fn full_pipeline_tht_cleanup() {
    let server = start_full_server(BatchAcceptConfig {
        all_or_nothing: true,
        partial_reject_by_transfer_sequence_id: false,
    });

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(2000);
    let t1 = make_transfer(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 500);
    let t2 = make_transfer(uuid_from_u64(2), uuid_from_u64(30), uuid_from_u64(40), 300);

    let (status, _, _) = send_batch(&mut stream, batch_id, &[t1, t2]);
    assert_eq!(status, BATCH_ACCEPTED);

    // Ждём полный 2PC цикл: Pipeline → Actor(PREPARE) → DM(COMMIT) → Actor(COMMIT) → DM(cleanup)
    thread::sleep(Duration::from_millis(500));

    // THT должен быть пуст — все трансферы прошли полный цикл и удалены
    assert_eq!(
        server.transfer_hash_tables[0].count(), 0,
        "THT should be empty after full 2PC cycle",
    );
}

//---------few batched as serial---------

#[test]
fn full_pipeline_multiple_batches() {
    let server = start_full_server(BatchAcceptConfig {
        all_or_nothing: true,
        partial_reject_by_transfer_sequence_id: false,
    });

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    // 3 батча по 2 трансфера
    for batch_num in 0..3u64 {
        let batch_id = uuid_from_u64(3000 + batch_num);
        let t1 = make_transfer(
            uuid_from_u64(batch_num * 10 + 1),
            uuid_from_u64(100),
            uuid_from_u64(200),
            500,
        );
        let t2 = make_transfer(
            uuid_from_u64(batch_num * 10 + 2),
            uuid_from_u64(300),
            uuid_from_u64(400),
            300,
        );

        let (status, _, _) = send_batch(&mut stream, batch_id, &[t1, t2]);
        assert_eq!(status, BATCH_ACCEPTED);
    }

    // Ждём обработки всех батчей
    thread::sleep(Duration::from_millis(500));

    assert_eq!(
        server.transfer_hash_tables[0].count(), 0,
        "THT should be empty after processing all batches",
    );
}

//------------Reject batches-------------

#[test]
fn full_pipeline_rejected_batch_no_tht_entry() {
    let server = start_full_server(BatchAcceptConfig {
        all_or_nothing: true,
        partial_reject_by_transfer_sequence_id: false,
    });

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    // Невалидный батч (amount=0)
    let batch_id = uuid_from_u64(4000);
    let t1 = make_transfer(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 0);

    let (status, _, _) = send_batch(&mut stream, batch_id, &[t1]);
    assert_eq!(status, BATCH_FAILED);

    thread::sleep(Duration::from_millis(200));

    // THT пуст — rejected batch не дошёл до Pipeline
    assert_eq!(server.transfer_hash_tables[0].count(), 0);
}

//------------- Smoke tests--------------

#[test]
fn full_pipeline_smoke_test() {
    let server = start_full_server(BatchAcceptConfig {
        all_or_nothing: true,
        partial_reject_by_transfer_sequence_id: false,
    });

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

    let status = send_handshake(&mut stream);
    assert_eq!(status, HS_OK);

    let batch_id = uuid_from_u64(1000);
    let t1 = make_transfer(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 500);

    let (status, reject_count, _) = send_batch(&mut stream, batch_id, &[t1]);
    assert_eq!(status, BATCH_ACCEPTED);
    assert_eq!(reject_count, 0);

    // Ждём полный 2PC цикл
    thread::sleep(Duration::from_millis(200));

    // Не упали — OK
}

//---------------------------------------------------



fn start_server(batch_accept: BatchAcceptConfig) -> TestServer {
    let partitions_num = 4;
    let partition_rb: Vec<Arc<MpscRingBuffer<PartitionSlot>>> = (0..partitions_num)
        .map(
            |_| Arc::new(
                MpscRingBuffer::<PartitionSlot>::new(64)
                    .expect("failed to create partition ring buffer")
            )
        ).collect();

    const K0: u64 = 0x0123456789ABCDEF;
    const K1: u64 = 0xFEDCBA9876543210;

    let overrides = PartitionAssignmentsOverrides::empty();
    let tht = vec![
        Arc::new(TransferHashTable::new(64, K0, K1, 8).unwrap())
    ];

    let handler = LedgerPipelineHandler::new(
        PARTITION_SEED_K0, PARTITION_SEED_K1, partitions_num, overrides, tht, 1
    );

    let pipeline_rb = Arc::new(
        MpscRingBuffer::<IncomingSlot>::new(1024)
            .expect("Failed to create pipeline RB"),
    );

    let queue = Arc::new(RingBuffer::new(64));

    // Worker thread
    let worker_queue = Arc::clone(&queue);
    let worker_rb = Arc::clone(&pipeline_rb);
    thread::spawn(move || {
        let mut worker = Worker::new(0, worker_queue, worker_rb, batch_accept).unwrap();
        worker.run().unwrap();
    });

    // Acceptor thread — биндится к порту 0
    let acceptor_queues = vec![Arc::clone(&queue)];
    let mut acceptor = Acceptor::new("127.0.0.1:0", acceptor_queues)
        .expect("Failed to create acceptor");
    let addr = acceptor.local_addr().unwrap().to_string();

    thread::spawn(move || {
        acceptor.run().unwrap();
    });

    // Даём серверу время подняться
    thread::sleep(Duration::from_millis(50));

    TestServer { addr, pipeline_rb }
}

fn send_handshake(stream: &mut TcpStream) -> u8 {
    let client_id = [1u8; 16];
    let conn_type: u8 = CONN_COMMAND;
    let protocol_version: u16 = 1;

    let mut payload = Vec::new();
    payload.extend_from_slice(&client_id);
    payload.push(conn_type);
    payload.extend_from_slice(&protocol_version.to_be_bytes());

    let mut frame = Vec::new();
    Codec::encode_request(MSG_HANDSHAKE_REQUEST, &payload, &mut frame);
    stream.write_all(&frame).unwrap();

    // Читаем response
    let mut resp_buf = [0u8; 256];
    let n = stream.read(&mut resp_buf).unwrap();
    assert!(n >= HEADER_SIZE + 1, "handshake response too short: {} bytes", n);

    // Проверяем magic
    assert_eq!(&resp_buf[0..8], &MAGIC_RESPONSE);
    // Проверяем msg_type
    assert_eq!(resp_buf[8], MSG_HANDSHAKE_RESPONSE);
    // payload_len
    let payload_len = u32::from_be_bytes([resp_buf[9], resp_buf[10], resp_buf[11], resp_buf[12]]) as usize;
    assert_eq!(payload_len, 1);

    // status
    resp_buf[HEADER_SIZE]
}

fn make_transfer(
    transfer_id: [u8; 16],
    debit: [u8; 16],
    credit: [u8; 16],
    amount: i64,
) -> Vec<u8> {
    make_transfer_with_seq(transfer_id, debit, credit, amount, [0u8; 16])
    /*let mut data = Vec::with_capacity(TRANSFER_BASE_SIZE);

    data.extend_from_slice(&transfer_id);            // 0..16
    data.extend_from_slice(&[0u8; 16]);              // 16..32  idempotency_key
    data.extend_from_slice(&debit);                  // 32..48  debit_account_id
    data.extend_from_slice(&credit);                 // 48..64  credit_account_id
    data.extend_from_slice(&amount.to_be_bytes());   // 64..72  amount
    data.extend_from_slice(&[0u8; 16]);              // 72..88  currency
    data.extend_from_slice(&[0u8; 16]);              // 88..104 transfer_sequence_id
    data.extend_from_slice(&1u64.to_be_bytes());     // 104..112 transfer_datetime

    assert_eq!(data.len(), TRANSFER_BASE_SIZE);
    data*/
}

fn send_batch(
    stream: &mut TcpStream,
    batch_id: [u8; 16],
    transfers: &[Vec<u8>],
) -> (u8, u16, Vec<(u8, [u8; 16])>) {
    // Формируем payload: BatchRequestHeader + transfers
    let mut payload = Vec::new();

    // BatchRequestHeader: batch_id(16) + count(2)
    payload.extend_from_slice(&batch_id);
    payload.extend_from_slice(&(transfers.len() as u16).to_be_bytes());

    // Трансферы
    for t in transfers {
        payload.extend_from_slice(t);
    }

    // Оборачиваем в frame
    let mut frame = Vec::new();
    Codec::encode_request(MSG_BATCH_REQUEST, &payload, &mut frame);
    stream.write_all(&frame).unwrap();

    // Читаем response
    let mut resp_buf = [0u8; 4096];
    let n = stream.read(&mut resp_buf).unwrap();

    assert!(n >= HEADER_SIZE, "batch response too short");
    assert_eq!(&resp_buf[0..8], &MAGIC_RESPONSE);
    assert_eq!(resp_buf[8], MSG_BATCH_RESPONSE);

    let payload_len = u32::from_be_bytes(
        [resp_buf[9], resp_buf[10], resp_buf[11], resp_buf[12]]
    ) as usize;

    // Парсим BatchResponse payload:
    // batch_id(16) + status(1) + reject_count(2) + rejects(17 * N)
    let p = &resp_buf[HEADER_SIZE..HEADER_SIZE + payload_len];

    let mut resp_batch_id = [0u8; 16];
    resp_batch_id.copy_from_slice(&p[0..16]);
    assert_eq!(resp_batch_id, batch_id, "batch_id mismatch in response");

    let status = p[16];
    let reject_count = u16::from_be_bytes([p[17], p[18]]);

    let mut rejects = Vec::new();
    let mut offset = 19;
    for _ in 0..reject_count {
        let mut tid = [0u8; 16];
        tid.copy_from_slice(&p[offset..offset + 16]);
        let reason = p[offset + 16];
        rejects.push((reason, tid));
        offset += 17;
    }

    (status, reject_count, rejects)
}

fn uuid_from_u64(val: u64) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[8..16].copy_from_slice(&val.to_be_bytes());
    id
}

fn make_transfer_with_seq(
    transfer_id: [u8; 16],
    debit: [u8; 16],
    credit: [u8; 16],
    amount: i64,
    transfer_sequence_id: [u8; 16],
) -> Vec<u8> {
    let mut data = Vec::with_capacity(TRANSFER_BASE_SIZE);

    data.extend_from_slice(&transfer_id);                    // 0..16
    data.extend_from_slice(&[0u8; 16]);                      // 16..32  idempotency_key
    data.extend_from_slice(&debit);                          // 32..48  debit_account_id
    data.extend_from_slice(&credit);                         // 48..64  credit_account_id
    data.extend_from_slice(&amount.to_be_bytes());           // 64..72  amount
    data.extend_from_slice(&[0u8; 16]);                      // 72..88  currency
    data.extend_from_slice(&transfer_sequence_id);           // 88..104 transfer_sequence_id
    data.extend_from_slice(&1u64.to_be_bytes());             // 104..112 transfer_datetime

    assert_eq!(data.len(), TRANSFER_BASE_SIZE);
    data
}



