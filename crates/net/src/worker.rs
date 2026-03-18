use std::collections::{HashMap, HashSet};
use std::io;
use std::io::{Read, Write};
use std::sync::Arc;
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};
use protocol::codec::{Codec, FrameResult};
use protocol::consts::{BATCH_ACCEPTED, BATCH_FAILED, BATCH_WITH_REJECTS, HS_OK, HS_UNSUPPORTED_VERSION, MSG_BATCH_RESPONSE, MSG_HANDSHAKE_REQUEST, MSG_HANDSHAKE_RESPONSE, REJECT_DUPLICATE_TRANSFER_ID_IN_BATCH, REJECT_INVALID_ACCOUNT_ID, REJECT_INVALID_AMOUNT, REJECT_INVALID_TRANSFER_ID, MSG_BATCH_REQUEST, REJECT_SEQUENCE_GROUP_FAILED};
use protocol::message::{HandshakeRequest, HandshakeResponse, Uuid};
use protocol::reject::Reject;
use protocol::request::BatchRequestHeader;
use protocol::response::BatchResponse;
use protocol::transfer::{Transfer, TRANSFER_BASE_SIZE};
use ringbuf::mpsc_ring_buffer::MpscRingBuffer;
use pipeline::incoming_slot::IncomingSlot;
use config::BatchAcceptConfig;
use crate::ring_buffer::RingBuffer;

const METADATA_SIZE: usize = 0;

const ZERO_UUID: Uuid = [0u8; 16];

enum ConnState {
    AwaitingHandshake,
    Active { conn_type: u8 }
}

struct Connection {
    stream: TcpStream,
    codec: Codec,
    state: ConnState,
    write_buf: Vec<u8>,
    payload_buf: Vec<u8>,
    seen_ids: HashSet<Uuid>,
    connection_id: u64,
}

pub struct Worker {
    id: usize,
    poll: Poll,
    connections: HashMap<Token, Connection>,
    next_token: usize,
    incoming: Arc<RingBuffer<TcpStream>>,
    pipeline_rb: Arc<MpscRingBuffer<IncomingSlot>>,
    batch_accept_config: BatchAcceptConfig,
    valid_indices: Vec<u32>,
    rejected_sequences: HashSet<Uuid>,
}

impl Worker {
    pub fn new(
        id: usize,
        incoming: Arc<RingBuffer<TcpStream>>,
        pipeline_rb: Arc<MpscRingBuffer<IncomingSlot>>,
        batch_accept_config: BatchAcceptConfig,
    ) -> io::Result<Self> {
        Ok(
            Self {
                id,
                poll: Poll::new()?,
                connections: HashMap::new(),
                next_token: 1,
                incoming,
                pipeline_rb,
                batch_accept_config,
                valid_indices: Vec::new(),
                rejected_sequences: HashSet::new(),
            }
        )
    }

    pub fn run(&mut self) -> io::Result<()> {
        let mut events = Events::with_capacity(128);

        loop {
            self.poll.poll(&mut events, Some(std::time::Duration::from_millis(10)))?;

            while let Some(mut stream) = self.incoming.pop() {
                let connection_id = (self.id as u64) << 32 | self.next_token as u64;
                let token = Token(self.next_token);
                self.next_token += 1;
                self.poll.registry().register(&mut stream, token, Interest::READABLE)?;
                println!("New connection from {} registered by worker {} with token {:?}", stream.peer_addr()?, self.id, token);
                self.connections.insert(
                    token,
                    Connection {
                        stream,
                        codec: Codec::new(),
                        state: ConnState::AwaitingHandshake,
                        write_buf: Vec::with_capacity(256),
                        payload_buf: Vec::with_capacity(64),
                        seen_ids: HashSet::new(),
                        connection_id,
                    }
                );
            }

            let mut to_remove = Vec::new();

            for event in events.iter() {
                let token = event.token();
                if let Some(conn) = self.connections.get_mut(&token) {
                    loop {
                        match conn.stream.read(conn.codec.read_buf()) {
                            Ok(0) => {
                                println!("Connection closed: {:?} by worker {}", token, self.id);
                                to_remove.push(token);
                                break;
                            }
                            Ok(n) => {
                                conn.codec.advance(n);
                                loop {
                                    match conn.codec.try_decode_request() {
                                        FrameResult::Complete(msg_type, start, end) => {
                                            let payload = conn.codec.payload(start, end).to_vec();

                                            println!(
                                                "Worker {} frame: type = 0x{:02X}, payload = {} bytes",
                                                self.id, msg_type, end - start,
                                            );
                                            conn.codec.consume(end);
                                            let should_close = Self::handle_frame(
                                                conn,
                                                msg_type,
                                                &payload,
                                                self.id,
                                                &self.pipeline_rb,
                                                self.batch_accept_config,
                                                &mut self.valid_indices,
                                                &mut self.rejected_sequences,
                                            );

                                            if should_close {
                                                to_remove.push(token);
                                                break;
                                            }
                                        }
                                        FrameResult::Incomplete => break,
                                        FrameResult::Error(err) => {
                                            println!(
                                                "Worker {} protocol error: {:?}",
                                                self.id, err
                                            );
                                            to_remove.push(token);
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                            Err(e) => {
                                println!("Error reading in worker {} from stream: {}", self.id, e);
                                to_remove.push(token);
                                break;
                            }
                        }
                    }
                }
            }

            for token in &to_remove {
                self.connections.remove(token);
            }
            to_remove.clear();
        }
    }

    fn handle_frame(
        conn: &mut Connection,
        msg_type: u8,
        payload: &[u8],
        worker_id: usize,
        pipeline_rb: &MpscRingBuffer<IncomingSlot>,
        batch_accept_config: BatchAcceptConfig,
        valid_indices: &mut Vec<u32>,
        rejected_sequences: &mut HashSet<Uuid>,
    ) -> bool {
        match conn.state {
            ConnState::AwaitingHandshake => {
                Self::handle_handshake(conn, msg_type, payload, worker_id)
            }
            ConnState::Active { conn_type } => {
                match msg_type {
                    MSG_BATCH_REQUEST => {
                        Self::handle_batch(
                            conn,
                            payload,
                            worker_id,
                            pipeline_rb,
                            batch_accept_config,
                            valid_indices,
                            rejected_sequences,
                        )
                    }
                    _ => {
                        println!(
                            "[worker {}] message type=0x{:02X}, {} bytes (conn_type={})",
                            worker_id, msg_type, payload.len(), conn_type,
                        );
                        false
                    }
                }
            }
        }
    }

    fn write_all_to_pipeline(
        pipeline_rb: &MpscRingBuffer<IncomingSlot>,
        payload: &[u8],
        header: &BatchRequestHeader,
        transfer_size: usize,
        connection_id: u64,
    ) {
        let count = header.count as usize;
        let mut batch = pipeline_rb.claim_batch(count);
        let mut offset = BatchRequestHeader::SIZE;

        for i in 0..count {
            let transfer_data = &payload[offset..offset + transfer_size];
            Self::fill_slot(batch.slot_mut(i), transfer_data, &header.batch_id, connection_id);
            offset += transfer_size;
        }

        batch.publish();
    }

    fn handle_batch(
        conn: &mut Connection,
        payload: &[u8],
        worker_id: usize,
        pipeline_rb: &MpscRingBuffer<IncomingSlot>,
        batch_accept_config: BatchAcceptConfig,
        valid_indices: &mut Vec<u32>,
        rejected_sequences: &mut HashSet<Uuid>,
    ) -> bool {
        let header = match BatchRequestHeader::decode(payload) {
            Some(header) => header,
            None => {
                println!("[worker {}] invalid batch header — closing", worker_id);
                return true;
            }
        };

        println!(
            "[worker {}] batch {:02X?}, count={}",
            worker_id,
            &header.batch_id[..4],
            header.count,
        );

        let transfer_size = TRANSFER_BASE_SIZE + METADATA_SIZE;
        let mut offset = BatchRequestHeader::SIZE;
        let mut rejects: Vec<Reject> = Vec::new();
        valid_indices.clear();
        conn.seen_ids.clear();
        rejected_sequences.clear();

        if batch_accept_config.all_or_nothing {
            for i in 0..header.count {
                if offset + transfer_size > payload.len() {
                    println!(
                        "[worker {}] batch truncated at transfer {} — closing",
                        worker_id, i,
                    );
                    return true;
                }

                let transfer_data = &payload[offset..offset + transfer_size];

                match Transfer::decode(transfer_data, METADATA_SIZE) {
                    Some(transfer) => {
                        if let Some(reason) = Self::validate_transfer(
                            &transfer, &mut conn.seen_ids,
                        ) {
                            rejects.push(
                                Reject {
                                    transfer_id: transfer.transfer_id,
                                    reason,
                                }
                            );
                            break;
                        }
                    }
                    None => {
                        println!(
                            "[worker {}] failed to decode transfer {} — closing",
                            worker_id, i,
                        );
                        return true;
                    }
                }

                offset += transfer_size;
            }
        } else {
            for i in 0..header.count {
                if offset + transfer_size > payload.len() {
                    println!(
                        "[worker {}] batch truncated at transfer {} — closing",
                        worker_id, i,
                    );
                    return true;
                }

                let transfer_data = &payload[offset..offset + transfer_size];

                match Transfer::decode(transfer_data, METADATA_SIZE) {
                    Some(transfer) => {
                        if let Some(reason) = Self::validate_transfer(
                            &transfer, &mut conn.seen_ids
                        ) {
                            rejects.push(
                                Reject {
                                    transfer_id: transfer.transfer_id,
                                    reason,
                                }
                            );

                            if batch_accept_config.partial_reject_by_transfer_sequence_id
                                && transfer.transfer_sequence_id != ZERO_UUID {
                                rejected_sequences.insert(transfer.transfer_sequence_id);
                            }
                        } else {
                            valid_indices.push(i as u32);
                        }
                    }
                    None => {
                        println!(
                            "[worker {}] failed to decode transfer {} — closing",
                            worker_id, i,
                        );
                        return true;
                    }
                }

                offset += transfer_size;
            }

            if batch_accept_config.partial_reject_by_transfer_sequence_id
                && !rejected_sequences.is_empty() {
                let transfer_size = TRANSFER_BASE_SIZE + METADATA_SIZE;

                valid_indices.retain(|&ids| {
                    let offset = BatchRequestHeader::SIZE + (ids as usize) * transfer_size;
                    let seq_id_start = offset + 88;
                    let seq_id_end = seq_id_start + 16;
                    let mut seq_id = [0u8; 16];
                    seq_id.copy_from_slice(&payload[seq_id_start..seq_id_end]);

                    if seq_id != ZERO_UUID && rejected_sequences.contains(&seq_id) {
                        rejects.push(
                            Reject {
                                transfer_id: {
                                    let mut reject_transfer_id = [0u8; 16];
                                    reject_transfer_id.copy_from_slice(&payload[offset..offset + 16]);
                                    reject_transfer_id
                                },
                                reason: REJECT_SEQUENCE_GROUP_FAILED,
                            }
                        );
                        false
                    } else {
                        true
                    }
                });
            }
        }

        let (status, forwarded);

        if batch_accept_config.all_or_nothing {
            if rejects.is_empty() {
                Self::write_all_to_pipeline(
                    pipeline_rb,
                    payload,
                    &header,
                    transfer_size,
                    conn.connection_id,
                );
                status = BATCH_ACCEPTED;
                forwarded = header.count as usize;
            } else {
                status = BATCH_FAILED;
                forwarded = 0;
            }
        } else {
            if valid_indices.is_empty() {
                status = BATCH_FAILED;
                forwarded = 0;
            } else if rejects.is_empty() {
                Self::write_all_to_pipeline(
                    pipeline_rb,
                    payload,
                    &header,
                    transfer_size,
                    conn.connection_id,
                );
                status = BATCH_ACCEPTED;
                forwarded = header.count as usize;
            } else {
                Self::write_selected_to_pipeline(
                    pipeline_rb,
                    payload,
                    &header,
                    transfer_size,
                    conn.connection_id,
                    valid_indices,
                );
                status = BATCH_WITH_REJECTS;
                forwarded = valid_indices.len();
            }
        }

        let response = BatchResponse {
            batch_id: header.batch_id,
            status,
            rejects
        };

        conn.payload_buf.clear();
        response.encode(&mut conn.payload_buf);

        conn.write_buf.clear();
        Codec::encode_response(
            MSG_BATCH_RESPONSE,
            &conn.payload_buf,
            &mut conn.write_buf,
        );

        if let Err(err) = conn.stream.write_all(&conn.write_buf) {
            println!("[worker {}] write error: {}", worker_id, err);
            return true;
        }

        println!(
            "[worker {}] batch response: status = {}, rejects = {}, forwarded = {}",
            worker_id, status, response.rejects.len(), forwarded
        );

        false
    }

    fn write_selected_to_pipeline(
        pipeline_rb: &MpscRingBuffer<IncomingSlot>,
        payload: &[u8],
        header: &BatchRequestHeader,
        transfer_size: usize,
        connection_id: u64,
        valid_indices: &[u32],
    ) {
        let mut batch = pipeline_rb.claim_batch(valid_indices.len());

        for (slot_idx, &transfer_idx) in valid_indices.iter().enumerate() {
            let offset = BatchRequestHeader::SIZE + (transfer_idx as usize) * transfer_size;
            let transfer_data = &payload[offset..offset + transfer_size];
            Self::fill_slot(batch.slot_mut(slot_idx), transfer_data, &header.batch_id, connection_id);
        }

        batch.publish();
    }

    fn fill_slot(
        slot: &mut IncomingSlot,
        transfer_data: &[u8],
        batch_id: &[u8; 16],
        connection_id: u64,
    ) {
        slot.batch_id = *batch_id;
        slot.connection_id = connection_id;
        unsafe {
            std::ptr::copy_nonoverlapping(
                transfer_data.as_ptr(),
                (slot as *mut IncomingSlot as *mut u8).add(32),
                TRANSFER_BASE_SIZE,
            );
        }
    }

    fn handle_handshake(conn: &mut Connection, msg_type: u8, payload: &[u8], worker_id: usize) -> bool {
        if msg_type != MSG_HANDSHAKE_REQUEST {
            println!(
                "[worker {}] expected handshake, got 0x{:02X} — closing",
                worker_id, msg_type,
            );
            return true;
        }

        let request = match HandshakeRequest::decode(payload) {
            Some(r) => r,
            None => {
                println!("[worker {}] invalid handshake payload — closing", worker_id);
                return true;
            }
        };

        let status = if request.protocol_version != 1 {
            HS_UNSUPPORTED_VERSION
        } else {
            HS_OK
        };

        let response = HandshakeResponse { status };

        conn.payload_buf.clear();
        response.encode(&mut conn.payload_buf);

        conn.write_buf.clear();
        Codec::encode_response(
            MSG_HANDSHAKE_RESPONSE,
            &conn.payload_buf,
            &mut conn.write_buf
        );

        if let Err(e) = conn.stream.write_all(&conn.write_buf) {
            println!("[worker {}] write error: {}", worker_id, e);
            return true;
        }

        if status == HS_OK {
            println!(
                "[worker {}] handshake OK, conn_type={}",
                worker_id, request.conn_type,
            );
            conn.state = ConnState::Active { conn_type: request.conn_type };
            false
        } else {
            println!(
                "[worker {}] handshake rejected, status={}",
                worker_id, status,
            );
            true
        }
    }

    fn validate_transfer(transfer: &Transfer, seen_ids: &mut HashSet<Uuid>) -> Option<u8> {
        if transfer.transfer_id == ZERO_UUID {
            return Some(REJECT_INVALID_TRANSFER_ID);
        }

        if transfer.debit_account_id == ZERO_UUID
            || transfer.credit_account_id == ZERO_UUID {
            return Some(REJECT_INVALID_ACCOUNT_ID);
        }

        if transfer.amount == 0 {
            return Some(REJECT_INVALID_AMOUNT);
        }

        if !seen_ids.insert(transfer.transfer_id) {
            return Some(REJECT_DUPLICATE_TRANSFER_ID_IN_BATCH)
        }

        None
    }
}