use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use common::u64_pair_to_bytes::u64_pair_to_bytes;
use ledger::decision_maker::DecisionMaker;
use ledger::partition_accounts_hash_table::PartitionAccountsHashTable;
use ledger::partition_actor::PartitionActor;
use ledger::partition_version_table::PartitionVersionTable;
use pipeline::coordinator_slot::CoordinatorSlot;

use config::config::BatchAcceptConfig;
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
use ringbuf::arena::Arena;
use storage::flush_done_slot::FlushDoneSlot;
use storage::ls_writer_slot::LsWriterSlot;

const PARTITION_SEED_K0: u64 = 0x0123456789ABCDEF;
const PARTITION_SEED_K1: u64 = 0xFEDCBA9876543210;

struct TestServer {
    addr: String,
    pipeline_rb: &'static MpscRingBuffer<IncomingSlot>,
}

struct FullTestServer {
    addr: String,
    pipeline_rb: &'static MpscRingBuffer<IncomingSlot>,
    transfer_hash_tables: &'static [TransferHashTable],
}

/// The bound a test uses when the bound is not what it is testing.
///
/// Well below the 1024-slot ingress ring the test servers build, so a
/// test that is not about the bound never meets it.
const TEST_MAX_TRANSFERS_PER_BATCH: usize = 64;

/// How many yielding steps a test Worker spends on a full ring before
/// it refuses. Matches the shipped configuration.
const TEST_PIPELINE_WAIT_MAX_YIELDS: u32 = 64;

/// The byte budget a test Worker's codec is built with. Generous
/// enough that no test meets it except the one that is about it.
const TEST_MAX_MESSAGE_PAYLOAD_BYTES: usize = 1_048_576;

fn all_or_nothing_batch_accept_with_bound(max_transfers_per_batch: usize) -> BatchAcceptConfig {
    BatchAcceptConfig {
        all_or_nothing: true,
        partial_reject_by_transfer_sequence_id: false,
        max_transfers_per_batch,
    }
}

fn all_or_nothing_batch_accept() -> BatchAcceptConfig {
    all_or_nothing_batch_accept_with_bound(TEST_MAX_TRANSFERS_PER_BATCH)
}

fn partial_batch_accept() -> BatchAcceptConfig {
    BatchAcceptConfig { all_or_nothing: false, ..all_or_nothing_batch_accept() }
}

fn partial_batch_accept_grouped_by_sequence_id() -> BatchAcceptConfig {
    BatchAcceptConfig {
        partial_reject_by_transfer_sequence_id: true,
        ..partial_batch_accept()
    }
}

fn start_full_server(batch_accept: BatchAcceptConfig) -> FullTestServer {
    let partitions_num = 4;
    let dm_shards = 1;

    let partition_rbs: &'static [MpscRingBuffer<PartitionSlot>] = (0..partitions_num)
        .map(|_| MpscRingBuffer::<PartitionSlot>::new(64).unwrap())
        .collect::<Vec<_>>()
        .leak();

    let coordinator_rbs: &'static [MpscRingBuffer<CoordinatorSlot>] = (0..dm_shards)
        .map(|_| MpscRingBuffer::<CoordinatorSlot>::new(64).unwrap())
        .collect::<Vec<_>>()
        .leak();

    let transfer_hash_tables: &'static [TransferHashTable] = (0..dm_shards)
        .map(|_| TransferHashTable::new(64, PARTITION_SEED_K0, PARTITION_SEED_K1, 8).unwrap())
        .collect::<Vec<_>>()
        .leak();

    let overrides = PartitionAssignmentsOverrides::empty();

    let ls_writer_rbs: &'static [MpscRingBuffer<LsWriterSlot>] = (0..dm_shards)
        .map(|_| MpscRingBuffer::<LsWriterSlot>::new(64).unwrap())
        .collect::<Vec<_>>()
        .leak();

    let flush_done_rbs: &'static [MpscRingBuffer<FlushDoneSlot>] = (0..dm_shards)
        .map(|_| MpscRingBuffer::<FlushDoneSlot>::new(64).unwrap())
        .collect::<Vec<_>>()
        .leak();

    let handler = LedgerPipelineHandler::new(
        PARTITION_SEED_K0, PARTITION_SEED_K1, partitions_num,
        overrides, transfer_hash_tables, ls_writer_rbs, dm_shards,
    );

    let pipeline_rb: &'static MpscRingBuffer<IncomingSlot> = Box::leak(Box::new(
        MpscRingBuffer::<IncomingSlot>::new(1024).unwrap(),
    ));

    thread::spawn(move || {
        let mut pipeline = Pipeline::new(0, pipeline_rb, 64, partition_rbs, handler);
        pipeline.run();
    });

    let pvt_tails_arena: &'static Arena = Box::leak(Box::new(
        ringbuf::arena::Arena::new(partitions_num * 64).unwrap()
    ));
    let pvt_tails_base = pvt_tails_arena.as_ptr() as *mut u64;

    for (i, actor_rb) in partition_rbs.iter().enumerate() {
        let paht = PartitionAccountsHashTable::new(64, PARTITION_SEED_K0, PARTITION_SEED_K1).unwrap();
        let pvt = PartitionVersionTable::new(64, PARTITION_SEED_K0, PARTITION_SEED_K1).unwrap();
        let pvt_tail_addr = unsafe { pvt_tails_base.add(i * 8) } as usize;

        thread::spawn(move || {
            let mut actor = PartitionActor::new(
                i, actor_rb, paht, pvt, coordinator_rbs,
                pvt_tail_addr as *mut u64, ls_writer_rbs, 64,
            );
            actor.run();
        });
    }

    for i in 0..dm_shards {
        let dm_coord_rb = &coordinator_rbs[i];
        let dm_tht = &transfer_hash_tables[i];
        let dm_ls_writer_rb = &ls_writer_rbs[i];
        let dm_flush_done_rb = &flush_done_rbs[i];

        thread::spawn(move || {
            let mut dm = DecisionMaker::new(i, dm_coord_rb, dm_tht, partition_rbs, dm_ls_writer_rb, dm_flush_done_rb, 64);
            dm.run();
        });
    }

    let tcp_queue: &'static RingBuffer<_> = Box::leak(Box::new(RingBuffer::new(64)));
    thread::spawn(move || {
        let mut worker = Worker::new(
            0, tcp_queue, pipeline_rb, batch_accept,
            TEST_PIPELINE_WAIT_MAX_YIELDS, TEST_MAX_MESSAGE_PAYLOAD_BYTES,
        ).unwrap();
        worker.run().unwrap();
    });

    let acceptor_queues = std::slice::from_ref(tcp_queue);
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
    let server = start_server(all_or_nothing_batch_accept());

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

    let status = send_handshake(&mut stream);
    assert_eq!(status, HS_OK);
}

#[test]
fn batch_all_valid_accepted() {
    let server = start_server(all_or_nothing_batch_accept());

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let status = send_handshake(&mut stream);
    assert_eq!(status, HS_OK);

    let batch_id = uuid_from_u64(100);
    let t1 = make_transfer(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 500);
    let t2 = make_transfer(uuid_from_u64(2), uuid_from_u64(10), uuid_from_u64(20), 300);

    let (status, reject_count, _) = send_batch(&mut stream, batch_id, &[t1, t2]);

    assert_eq!(status, BatchStatus::Accepted.as_byte());
    assert_eq!(reject_count, 0);

    thread::sleep(Duration::from_millis(50));

    let drain = server.pipeline_rb.drain_batch(64);
    assert_eq!(drain.len(), 2, "expected 2 transfers in pipeline RB");

    let slot0 = drain.slot(0);
    assert_eq!(slot0.transfer_id, uuid_from_u64(1));
    assert_eq!(slot0.debit_account_id, uuid_from_u64(10));
    assert_eq!(slot0.credit_account_id, uuid_from_u64(20));
    assert_eq!(i64::from_be_bytes(slot0.amount), 500);
    assert_eq!(slot0.batch_id, batch_id);

    let slot1 = drain.slot(1);
    assert_eq!(slot1.transfer_id, uuid_from_u64(2));
    assert_eq!(i64::from_be_bytes(slot1.amount), 300);
    assert_eq!(slot1.batch_id, batch_id);

    drain.release();
}

#[test]
fn batch_all_or_nothing_with_invalid_rejected() {
    let server = start_server(all_or_nothing_batch_accept());

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(200);
    let t1 = make_transfer(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 500);
    let t2 = make_transfer(uuid_from_u64(2), uuid_from_u64(10), uuid_from_u64(20), 0);

    let (status, reject_count, rejects) = send_batch(&mut stream, batch_id, &[t1, t2]);

    assert_eq!(status, BatchStatus::Failed.as_byte());
    assert_eq!(reject_count, 1);
    assert_eq!(rejects[0].0, REJECT_INVALID_AMOUNT);
    assert_eq!(rejects[0].1, uuid_from_u64(2));

    thread::sleep(Duration::from_millis(50));
    let drain = server.pipeline_rb.drain_batch(64);
    assert_eq!(drain.len(), 0, "pipeline RB should be empty after rejected batch");
    drain.release();
}

#[test]
fn batch_partial_with_rejects() {
    let server = start_server(partial_batch_accept());

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(300);
    let t1 = make_transfer(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 500);
    let t2 = make_transfer(uuid_from_u64(2), uuid_from_u64(10), uuid_from_u64(20), 0);
    let t3 = make_transfer(uuid_from_u64(3), uuid_from_u64(10), uuid_from_u64(20), 200);

    let (status, reject_count, rejects) = send_batch(
        &mut stream, batch_id, &[t1, t2, t3],
    );

    assert_eq!(status, BatchStatus::WithRejects.as_byte());
    assert_eq!(reject_count, 1);
    assert_eq!(rejects[0].0, REJECT_INVALID_AMOUNT);
    assert_eq!(rejects[0].1, uuid_from_u64(2));

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
    let server = start_server(all_or_nothing_batch_accept());

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(400);
    let same_id = uuid_from_u64(42);
    let t1 = make_transfer(same_id, uuid_from_u64(10), uuid_from_u64(20), 100);
    let t2 = make_transfer(same_id, uuid_from_u64(10), uuid_from_u64(20), 200);

    let (status, reject_count, rejects) = send_batch(&mut stream, batch_id, &[t1, t2]);

    assert_eq!(status, BatchStatus::Failed.as_byte());
    assert_eq!(reject_count, 1);
    assert_eq!(rejects[0].0, REJECT_DUPLICATE_TRANSFER_ID_IN_BATCH);

    thread::sleep(Duration::from_millis(50));
    let drain = server.pipeline_rb.drain_batch(64);
    assert_eq!(drain.len(), 0);
    drain.release();
}

#[test]
fn batch_zero_transfer_id_rejected() {
    let server = start_server(all_or_nothing_batch_accept());

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(500);
    let t1 = make_transfer([0u8; 16], uuid_from_u64(10), uuid_from_u64(20), 100);

    let (status, reject_count, rejects) = send_batch(&mut stream, batch_id, &[t1]);

    assert_eq!(status, BatchStatus::Failed.as_byte());
    assert_eq!(reject_count, 1);
    assert_eq!(rejects[0].0, REJECT_INVALID_TRANSFER_ID);
}

#[test]
fn batch_zero_account_id_rejected() {
    let server = start_server(all_or_nothing_batch_accept());

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(600);
    let t1 = make_transfer(uuid_from_u64(1), [0u8; 16], uuid_from_u64(20), 100);

    let (status, reject_count, rejects) = send_batch(&mut stream, batch_id, &[t1]);

    assert_eq!(status, BatchStatus::Failed.as_byte());
    assert_eq!(reject_count, 1);
    assert_eq!(rejects[0].0, REJECT_INVALID_ACCOUNT_ID);
}

#[test]
fn handshake_unsupported_version() {
    let server = start_server(all_or_nothing_batch_accept());

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

    let client_id = [1u8; 16];
    let mut payload = Vec::new();
    payload.extend_from_slice(&client_id);
    payload.push(CONN_COMMAND);
    payload.extend_from_slice(&99u16.to_be_bytes());

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
    let server = start_server(partial_batch_accept_grouped_by_sequence_id());

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(800);
    let seq_a = uuid_from_u64(100);
    let seq_b = uuid_from_u64(200);

    let t1 = make_transfer_with_seq(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 500, seq_a);
    let t2 = make_transfer_with_seq(uuid_from_u64(2), uuid_from_u64(10), uuid_from_u64(20), 300, seq_a);

    let t3 = make_transfer_with_seq(uuid_from_u64(3), uuid_from_u64(10), uuid_from_u64(20), 100, seq_b);
    let t4 = make_transfer_with_seq(uuid_from_u64(4), uuid_from_u64(10), uuid_from_u64(20), 0,   seq_b);

    let (status, reject_count, rejects) = send_batch(
        &mut stream, batch_id, &[t1, t2, t3, t4],
    );

    assert_eq!(status, BatchStatus::WithRejects.as_byte());
    assert_eq!(reject_count, 2);

    let t4_reject = rejects.iter().find(|(_, tid)| *tid == uuid_from_u64(4)).unwrap();
    assert_eq!(t4_reject.0, REJECT_INVALID_AMOUNT);

    let t3_reject = rejects.iter().find(|(_, tid)| *tid == uuid_from_u64(3)).unwrap();
    assert_eq!(t3_reject.0, REJECT_SEQUENCE_GROUP_FAILED);

    thread::sleep(Duration::from_millis(50));
    let drain = server.pipeline_rb.drain_batch(64);
    assert_eq!(drain.len(), 2, "only group A should be in pipeline RB");
    assert_eq!(drain.slot(0).transfer_id, uuid_from_u64(1));
    assert_eq!(drain.slot(1).transfer_id, uuid_from_u64(2));
    drain.release();
}

#[test]
fn batch_partial_zero_sequence_not_grouped() {
    let server = start_server(partial_batch_accept_grouped_by_sequence_id());

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(900);

    let t1 = make_transfer_with_seq(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 500, [0u8; 16]);
    let t2 = make_transfer_with_seq(uuid_from_u64(2), uuid_from_u64(10), uuid_from_u64(20), 0,   [0u8; 16]);

    let (status, reject_count, rejects) = send_batch(
        &mut stream, batch_id, &[t1, t2],
    );

    assert_eq!(status, BatchStatus::WithRejects.as_byte());
    assert_eq!(reject_count, 1);
    assert_eq!(rejects[0].0, REJECT_INVALID_AMOUNT);
    assert_eq!(rejects[0].1, uuid_from_u64(2));

    thread::sleep(Duration::from_millis(50));
    let drain = server.pipeline_rb.drain_batch(64);
    assert_eq!(drain.len(), 1);
    assert_eq!(drain.slot(0).transfer_id, uuid_from_u64(1));
    drain.release();
}

#[test]
fn batch_partial_all_groups_rejected() {
    let server = start_server(partial_batch_accept_grouped_by_sequence_id());

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(1000);
    let seq_a = uuid_from_u64(100);

    let t1 = make_transfer_with_seq(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 500, seq_a);
    let t2 = make_transfer_with_seq(uuid_from_u64(2), uuid_from_u64(10), uuid_from_u64(20), 0,   seq_a);

    let (status, reject_count, _) = send_batch(
        &mut stream, batch_id, &[t1, t2],
    );

    assert_eq!(status, BatchStatus::Failed.as_byte());
    assert_eq!(reject_count, 2);

    thread::sleep(Duration::from_millis(50));
    let drain = server.pipeline_rb.drain_batch(64);
    assert_eq!(drain.len(), 0);
    drain.release();
}


#[test]
fn full_pipeline_tht_cleanup() {
    let server = start_full_server(all_or_nothing_batch_accept());

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(2000);
    let t1 = make_transfer(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 500);
    let t2 = make_transfer(uuid_from_u64(2), uuid_from_u64(30), uuid_from_u64(40), 300);

    let (status, _, _) = send_batch(&mut stream, batch_id, &[t1, t2]);
    assert_eq!(status, BatchStatus::Accepted.as_byte());

    thread::sleep(Duration::from_millis(500));

    assert_eq!(
        server.transfer_hash_tables[0].count(), 0,
        "THT should be empty after full 2PC cycle",
    );
}


#[test]
fn full_pipeline_multiple_batches() {
    let server = start_full_server(all_or_nothing_batch_accept());

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

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
        assert_eq!(status, BatchStatus::Accepted.as_byte());
    }

    thread::sleep(Duration::from_millis(2000));

    assert_eq!(
        server.transfer_hash_tables[0].count(), 0,
        "THT should be empty after processing all batches",
    );
}


#[test]
fn full_pipeline_rejected_batch_no_tht_entry() {
    let server = start_full_server(all_or_nothing_batch_accept());

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let batch_id = uuid_from_u64(4000);
    let t1 = make_transfer(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 0);

    let (status, _, _) = send_batch(&mut stream, batch_id, &[t1]);
    assert_eq!(status, BatchStatus::Failed.as_byte());

    thread::sleep(Duration::from_millis(200));

    assert_eq!(server.transfer_hash_tables[0].count(), 0);
}


#[test]
fn full_pipeline_smoke_test() {
    let server = start_full_server(all_or_nothing_batch_accept());

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

    let status = send_handshake(&mut stream);
    assert_eq!(status, HS_OK);

    let batch_id = uuid_from_u64(1000);
    let t1 = make_transfer(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 500);

    let (status, reject_count, _) = send_batch(&mut stream, batch_id, &[t1]);
    assert_eq!(status, BatchStatus::Accepted.as_byte());
    assert_eq!(reject_count, 0);

    thread::sleep(Duration::from_millis(200));

}


#[test]
fn empty_batch_is_refused_whole_and_the_connection_survives() {
    let server = start_server(all_or_nothing_batch_accept());

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let (status, reject_count, _) = send_batch(&mut stream, uuid_from_u64(1100), &[]);

    assert_eq!(status, BatchStatus::Failed.as_byte());
    assert_eq!(reject_count, 0, "a batch-level refusal names no transfer");

    thread::sleep(Duration::from_millis(50));
    let drain = server.pipeline_rb.drain_batch(64);
    assert_eq!(drain.len(), 0, "an empty batch must reach the ingress ring as nothing");
    drain.release();

    let t1 = make_transfer(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 500);
    let (status, _, _) = send_batch(&mut stream, uuid_from_u64(1101), &[t1]);
    assert_eq!(status, BatchStatus::Accepted.as_byte(), "the connection must survive the refusal");
}

#[test]
fn batch_above_the_configured_bound_is_refused_whole() {
    let server = start_server(all_or_nothing_batch_accept_with_bound(2));

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let t1 = make_transfer(uuid_from_u64(1), uuid_from_u64(10), uuid_from_u64(20), 100);
    let t2 = make_transfer(uuid_from_u64(2), uuid_from_u64(10), uuid_from_u64(20), 200);
    let t3 = make_transfer(uuid_from_u64(3), uuid_from_u64(10), uuid_from_u64(20), 300);

    let (status, reject_count, _) = send_batch(&mut stream, uuid_from_u64(1200), &[t1, t2, t3]);

    assert_eq!(status, BatchStatus::Failed.as_byte());
    assert_eq!(reject_count, 0, "a batch-level refusal names no transfer");

    thread::sleep(Duration::from_millis(50));
    let drain = server.pipeline_rb.drain_batch(64);
    assert_eq!(drain.len(), 0, "an oversized batch must cost no per-transfer work");
    drain.release();

    let t4 = make_transfer(uuid_from_u64(4), uuid_from_u64(10), uuid_from_u64(20), 400);
    let t5 = make_transfer(uuid_from_u64(5), uuid_from_u64(10), uuid_from_u64(20), 500);
    let (status, _, _) = send_batch(&mut stream, uuid_from_u64(1201), &[t4, t5]);
    assert_eq!(status, BatchStatus::Accepted.as_byte(), "a batch at the bound must still be admitted");
}

#[test]
fn a_full_ingress_ring_refuses_the_batch_as_retryable() {
    let server = start_server(all_or_nothing_batch_accept_with_bound(64));

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    send_handshake(&mut stream);

    let mut next_transfer_id = 1u64;
    for batch_number in 0..16u64 {
        let mut transfers = Vec::with_capacity(64);
        for _ in 0..64 {
            transfers.push(make_transfer(
                uuid_from_u64(next_transfer_id),
                uuid_from_u64(10),
                uuid_from_u64(20),
                500,
            ));
            next_transfer_id += 1;
        }
        let (status, _, _) = send_batch(&mut stream, uuid_from_u64(5000 + batch_number), &transfers);
        assert_eq!(status, BatchStatus::Accepted.as_byte(), "batch {batch_number} should still fit the ring");
    }

    let one_more = make_transfer(
        uuid_from_u64(next_transfer_id), uuid_from_u64(10), uuid_from_u64(20), 500,
    );
    let (status, reject_count, _) = send_batch(&mut stream, uuid_from_u64(6000), &[one_more]);

    assert_eq!(status, BatchStatus::Busy.as_byte());
    assert_eq!(reject_count, 0, "a ring-busy refusal names no transfer");

    let drain = server.pipeline_rb.drain_batch(2048);
    assert_eq!(drain.len(), 1024, "the refused batch must not have taken turns");
    drain.release();
}

#[test]
fn a_message_declaring_more_than_the_byte_budget_is_refused_on_its_header() {
    let server = start_server(all_or_nothing_batch_accept());

    let mut stream = TcpStream::connect(&server.addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    send_handshake(&mut stream);

    let declared_payload_len = (TEST_MAX_MESSAGE_PAYLOAD_BYTES + 1) as u32;
    let mut message_header = Vec::with_capacity(HEADER_SIZE);
    message_header.extend_from_slice(&MAGIC_REQUEST);
    message_header.push(MSG_BATCH_REQUEST);
    message_header.extend_from_slice(&declared_payload_len.to_be_bytes());
    assert_eq!(message_header.len(), HEADER_SIZE);
    stream.write_all(&message_header).unwrap();

    let mut response = [0u8; 64];
    match stream.read(&mut response) {
        Ok(0) => {}
        Ok(bytes_read) => {
            panic!("expected the connection to close, got {bytes_read} bytes")
        }
        Err(error) => assert_eq!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset,
            "expected a clean close or a reset, got {error}",
        ),
    }
}




fn start_server(batch_accept: BatchAcceptConfig) -> TestServer {
    let partitions_num = 4;
    let partition_rb: Vec<MpscRingBuffer<PartitionSlot>> = (0..partitions_num)
        .map(
            |_| MpscRingBuffer::<PartitionSlot>::new(64)
                .expect("failed to create partition ring buffer")
        ).collect();

    const K0: u64 = 0x0123456789ABCDEF;
    const K1: u64 = 0xFEDCBA9876543210;

    let overrides = PartitionAssignmentsOverrides::empty();
    let tht = vec![
        TransferHashTable::new(64, K0, K1, 8).unwrap()
    ];

    let ls_writer_rbs: Vec<MpscRingBuffer<LsWriterSlot>> = vec![
        MpscRingBuffer::<LsWriterSlot>::new(64).unwrap()
    ];

    let handler = LedgerPipelineHandler::new(
        PARTITION_SEED_K0, PARTITION_SEED_K1, partitions_num, overrides, &tht, &ls_writer_rbs, 1
    );

    let pipeline_rb: &'static MpscRingBuffer<IncomingSlot> = Box::leak(Box::new(
        MpscRingBuffer::<IncomingSlot>::new(1024)
            .expect("Failed to create pipeline RB"),
    ));

    let tcp_queue: &'static RingBuffer<_> = Box::leak(Box::new(RingBuffer::new(64)));

    thread::spawn(move || {
        let mut worker = Worker::new(
            0, tcp_queue, pipeline_rb, batch_accept,
            TEST_PIPELINE_WAIT_MAX_YIELDS, TEST_MAX_MESSAGE_PAYLOAD_BYTES,
        ).unwrap();
        worker.run().unwrap();
    });

    let acceptor_queues = std::slice::from_ref(tcp_queue);
    let mut acceptor = Acceptor::new("127.0.0.1:0", acceptor_queues)
        .expect("Failed to create acceptor");
    let addr = acceptor.local_addr().unwrap().to_string();

    thread::spawn(move || {
        acceptor.run().unwrap();
    });

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

    let mut resp_buf = [0u8; 256];
    let n = stream.read(&mut resp_buf).unwrap();
    assert!(n >= HEADER_SIZE + 1, "handshake response too short: {} bytes", n);

    assert_eq!(&resp_buf[0..8], &MAGIC_RESPONSE);
    assert_eq!(resp_buf[8], MSG_HANDSHAKE_RESPONSE);
    let payload_len = u32::from_be_bytes([resp_buf[9], resp_buf[10], resp_buf[11], resp_buf[12]]) as usize;
    assert_eq!(payload_len, 1);

    resp_buf[HEADER_SIZE]
}

fn make_transfer(
    transfer_id: [u8; 16],
    debit: [u8; 16],
    credit: [u8; 16],
    amount: i64,
) -> Vec<u8> {
    make_transfer_with_seq(transfer_id, debit, credit, amount, [0u8; 16])


}

fn send_batch(
    stream: &mut TcpStream,
    batch_id: [u8; 16],
    transfers: &[Vec<u8>],
) -> (u8, u16, Vec<(u8, [u8; 16])>) {
    let mut payload = Vec::new();

    payload.extend_from_slice(&batch_id);
    payload.extend_from_slice(&(transfers.len() as u16).to_be_bytes());

    for t in transfers {
        payload.extend_from_slice(t);
    }

    let mut frame = Vec::new();
    Codec::encode_request(MSG_BATCH_REQUEST, &payload, &mut frame);
    stream.write_all(&frame).unwrap();

    let mut resp_buf = [0u8; 4096];
    let n = stream.read(&mut resp_buf).unwrap();

    assert!(n >= HEADER_SIZE, "batch response too short");
    assert_eq!(&resp_buf[0..8], &MAGIC_RESPONSE);
    assert_eq!(resp_buf[8], MSG_BATCH_RESPONSE);

    let payload_len = u32::from_be_bytes(
        [resp_buf[9], resp_buf[10], resp_buf[11], resp_buf[12]]
    ) as usize;

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
    u64_pair_to_bytes(val, 0)
}

fn make_transfer_with_seq(
    transfer_id: [u8; 16],
    debit: [u8; 16],
    credit: [u8; 16],
    amount: i64,
    transfer_sequence_id: [u8; 16],
) -> Vec<u8> {
    let mut data = Vec::with_capacity(TRANSFER_BASE_SIZE);

    data.extend_from_slice(&transfer_id);
    data.extend_from_slice(&[0u8; 16]);
    data.extend_from_slice(&debit);
    data.extend_from_slice(&credit);
    data.extend_from_slice(&amount.to_be_bytes());
    data.extend_from_slice(&[0u8; 16]);
    data.extend_from_slice(&transfer_sequence_id);
    data.extend_from_slice(&1u64.to_be_bytes());

    assert_eq!(data.len(), TRANSFER_BASE_SIZE);
    data
}



