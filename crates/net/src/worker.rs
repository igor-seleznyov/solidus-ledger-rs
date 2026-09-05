use std::collections::{HashMap, HashSet};
use std::io;
use std::io::{Read, Write};
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};
use protocol::codec::{Codec, FrameRange, FrameResult};
use protocol::consts::{
    BatchStatus,
    HS_OK,
    HS_UNSUPPORTED_VERSION,
    MSG_BATCH_REQUEST,
    MSG_BATCH_RESPONSE,
    MSG_HANDSHAKE_REQUEST,
    MSG_HANDSHAKE_RESPONSE,
    REJECT_DUPLICATE_TRANSFER_ID_IN_BATCH,
    REJECT_INVALID_ACCOUNT_ID,
    REJECT_INVALID_AMOUNT,
    REJECT_INVALID_TRANSFER_ID,
    REJECT_SEQUENCE_GROUP_FAILED,
};
use protocol::message::{HandshakeRequest, HandshakeResponse, Uuid};
use protocol::reject::Reject;
use protocol::request::BatchRequestHeader;
use protocol::response::BatchResponse;
use protocol::transfer::{WireTransfer, TRANSFER_BASE_SIZE};
use ringbuf::mpsc_ring_buffer::MpscRingBuffer;
use pipeline::incoming_slot::IncomingSlot;
use config::config::BatchAcceptConfig;
use crate::ring_buffer::RingBuffer;

const METADATA_SIZE: usize = 0;

const ZERO_UUID: Uuid = [0u8; 16];

const _: () = {
    assert!(
        std::mem::offset_of!(IncomingSlot, _padding)
            - std::mem::offset_of!(IncomingSlot, transfer_id)
            == TRANSFER_BASE_SIZE
    );
    assert!(
        std::mem::offset_of!(IncomingSlot, transfer_id) + TRANSFER_BASE_SIZE
            <= std::mem::size_of::<IncomingSlot>()
    );
};

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

pub struct Worker<'scope> {
    id: usize,
    poll: Poll,
    connections: HashMap<Token, Connection>,
    next_token: usize,
    incoming: &'scope RingBuffer<TcpStream>,
    pipeline_rb: &'scope MpscRingBuffer<IncomingSlot>,
    batch_accept_config: BatchAcceptConfig,
    pipeline_wait_max_yields: u32,
    valid_indices: Vec<u32>,
    rejected_sequences: HashSet<Uuid>,
    tokens_to_remove: Vec<Token>,

    /// The byte budget every new connection's codec is built with:
    /// how many payload bytes one message may accumulate before the
    /// codec refuses it on the message header. Configured, not
    /// derived — it answers a different question from the batch
    /// bound.
    max_message_payload_bytes: usize,
}

impl<'scope> Worker<'scope> {
    pub fn new(
        id: usize,
        incoming: &'scope RingBuffer<TcpStream>,
        pipeline_rb: &'scope MpscRingBuffer<IncomingSlot>,
        batch_accept_config: BatchAcceptConfig,
        pipeline_wait_max_yields: u32,
        max_message_payload_bytes: usize,
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
                pipeline_wait_max_yields,
                valid_indices: Vec::new(),
                rejected_sequences: HashSet::new(),
                tokens_to_remove: Vec::new(),
                max_message_payload_bytes,
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
                        codec: Codec::new(self.max_message_payload_bytes),
                        state: ConnState::AwaitingHandshake,
                        write_buf: Vec::with_capacity(256),
                        payload_buf: Vec::with_capacity(64),
                        seen_ids: HashSet::new(),
                        connection_id,
                    }
                );
            }

            for event in events.iter() {
                let token = event.token();
                let Some(conn) = self.connections.get_mut(&token) else {
                    continue;
                };

                loop {
                    match conn.stream.read(conn.codec.read_buf()) {
                        Ok(0) => {
                            println!("Connection closed: {:?} by worker {}", token, self.id);
                            self.tokens_to_remove.push(token);
                            break;
                        }
                        Ok(bytes_read) => {
                            conn.codec.advance(bytes_read);
                            loop {
                                match conn.codec.try_decode_request() {
                                    FrameResult::Complete(msg_type, frame) => {
                                        println!(
                                            "Worker {} frame: type = 0x{:02X}, payload = {} bytes",
                                            self.id, msg_type, frame.len(),
                                        );

                                        let should_close = Self::handle_frame(
                                            conn,
                                            msg_type,
                                            frame,
                                            self.id,
                                            self.pipeline_rb,
                                            self.batch_accept_config,
                                            self.pipeline_wait_max_yields,
                                            &mut self.valid_indices,
                                            &mut self.rejected_sequences,
                                        );

                                        conn.codec.consume(frame.end);

                                        if should_close {
                                            self.tokens_to_remove.push(token);
                                            break;
                                        }
                                    }
                                    FrameResult::Incomplete => break,
                                    FrameResult::Error(error) => {
                                        println!(
                                            "Worker {} protocol error: {:?}",
                                            self.id, error
                                        );
                                        self.tokens_to_remove.push(token);
                                        break;
                                    }
                                }
                            }
                        }
                        Err(ref error) if error.kind() == io::ErrorKind::WouldBlock => break,
                        Err(error) => {
                            println!("Error reading in worker {} from stream: {}", self.id, error);
                            self.tokens_to_remove.push(token);
                            break;
                        }
                    }
                }
            }

            for token in self.tokens_to_remove.drain(..) {
                self.connections.remove(&token);
            }
        }
    }

    fn handle_frame(
        conn: &mut Connection,
        msg_type: u8,
        frame: FrameRange,
        worker_id: usize,
        pipeline_rb: &MpscRingBuffer<IncomingSlot>,
        batch_accept_config: BatchAcceptConfig,
        pipeline_wait_max_yields: u32,
        valid_indices: &mut Vec<u32>,
        rejected_sequences: &mut HashSet<Uuid>,
    ) -> bool {
        match conn.state {
            ConnState::AwaitingHandshake => {
                Self::handle_handshake(conn, msg_type, frame, worker_id)
            }
            ConnState::Active { conn_type } => {
                match msg_type {
                    MSG_BATCH_REQUEST => {
                        Self::handle_batch(
                            conn,
                            frame,
                            worker_id,
                            pipeline_rb,
                            batch_accept_config,
                            pipeline_wait_max_yields,
                            valid_indices,
                            rejected_sequences,
                        )
                    }
                    _ => {
                        println!(
                            "[worker {}] message type=0x{:02X}, {} bytes (conn_type={})",
                            worker_id, msg_type, frame.len(), conn_type,
                        );
                        false
                    }
                }
            }
        }
    }

    /// Copies the whole batch into the ingress ring, or admits none of it.
    ///
    /// `count` is the caller's already-checked transfer count, passed
    /// rather than re-read from the header: the bound that makes the
    /// ring's own precondition hold is established one level up, and a
    /// second caller that re-derived the number would inherit no bound
    /// at all.
    ///
    /// **For defence:** the bounded claim is what makes the refusal
    /// possible at all. The unbounded form takes its turns with one
    /// unconditional increment and only then waits for room, which is
    /// wait-free and exactly right for a producer that cannot decline —
    /// but a producer that took turns and then walked away would leave a
    /// permanent hole in the sequence, and the consumer would wait on the
    /// first abandoned turn forever. There is no way to hand a turn back.
    /// So this path inverts the order: establish that the room exists,
    /// then take the turns with a compare-and-exchange that fails
    /// harmlessly when another producer moved first. What the yield
    /// allowance bounds is the wait for room — not the exchange. A lost
    /// exchange reloads the counter and retries at once, spending none
    /// of the allowance, and that is correct: another producer winning
    /// means the ring moved forward, not that it is stuck.
    fn write_all_to_pipeline(
        pipeline_rb: &MpscRingBuffer<IncomingSlot>,
        payload: &[u8],
        header: &BatchRequestHeader,
        count: usize,
        transfer_size: usize,
        connection_id: u64,
        pipeline_wait_max_yields: u32,
    ) -> bool {
        let Some(mut batch) = pipeline_rb.try_claim_batch(count, pipeline_wait_max_yields) else {
            return false;
        };
        let mut offset = BatchRequestHeader::SIZE;

        for i in 0..count {
            let transfer_data = &payload[offset..offset + transfer_size];
            Self::fill_slot(batch.slot_mut(i), transfer_data, &header.batch_id, connection_id);
            offset += transfer_size;
        }

        batch.publish();
        true
    }

    fn handle_batch(
        conn: &mut Connection,
        frame: FrameRange,
        worker_id: usize,
        pipeline_rb: &MpscRingBuffer<IncomingSlot>,
        batch_accept_config: BatchAcceptConfig,
        pipeline_wait_max_yields: u32,
        valid_indices: &mut Vec<u32>,
        rejected_sequences: &mut HashSet<Uuid>,
    ) -> bool {
        let Some(header) = BatchRequestHeader::decode(conn.codec.payload(frame)) else {
            println!("[worker {worker_id}] invalid batch header — closing");
            return true;
        };

        let count = header.count as usize;

        if count == 0 {
            return Self::send_whole_batch_refusal(
                conn, header.batch_id, BatchStatus::Failed, worker_id,
            );
        }

        if count > batch_accept_config.max_transfers_per_batch {
            return Self::send_whole_batch_refusal(
                conn, header.batch_id, BatchStatus::Failed, worker_id,
            );
        }

        println!(
            "[worker {}] batch {:02X?}, count={}",
            worker_id,
            &header.batch_id[..4],
            count,
        );

        let payload = conn.codec.payload(frame);

        let transfer_size = TRANSFER_BASE_SIZE + METADATA_SIZE;
        let mut offset = BatchRequestHeader::SIZE;
        BatchResponse::reserve(&header.batch_id, &mut conn.payload_buf);
        valid_indices.clear();
        conn.seen_ids.clear();
        rejected_sequences.clear();

        if batch_accept_config.all_or_nothing {
            for i in 0..count {
                if offset + transfer_size > payload.len() {
                    println!(
                        "[worker {}] batch truncated at transfer {} — closing",
                        worker_id, i,
                    );
                    return true;
                }

                let transfer_data = &payload[offset..offset + transfer_size];

                if let Some(reason) = Self::validate_transfer(transfer_data, &mut conn.seen_ids) {
                    Reject {
                        transfer_id: Self::identifier_at(
                            transfer_data,
                            WireTransfer::TRANSFER_ID_OFFSET,
                        ),
                        reason,
                    }.encode(&mut conn.payload_buf);
                    break;
                }

                offset += transfer_size;
            }
        } else {
            for i in 0..count {
                if offset + transfer_size > payload.len() {
                    println!(
                        "[worker {}] batch truncated at transfer {} — closing",
                        worker_id, i,
                    );
                    return true;
                }

                let transfer_data = &payload[offset..offset + transfer_size];

                if let Some(reason) = Self::validate_transfer(
                    transfer_data, &mut conn.seen_ids,
                ) {
                    Reject {
                        transfer_id: Self::identifier_at(
                            transfer_data,
                            WireTransfer::TRANSFER_ID_OFFSET,
                        ),
                        reason,
                    }.encode(&mut conn.payload_buf);

                    let sequence_is_set = !Self::field_is_zero(
                        transfer_data,
                        WireTransfer::TRANSFER_SEQUENCE_ID_OFFSET,
                        WireTransfer::TRANSFER_DATETIME_OFFSET,
                    );
                    if batch_accept_config.partial_reject_by_transfer_sequence_id
                        && sequence_is_set {
                        rejected_sequences.insert(
                            Self::identifier_at(
                                transfer_data,
                                WireTransfer::TRANSFER_SEQUENCE_ID_OFFSET,
                            )
                        );
                    }
                } else {
                    valid_indices.push(i as u32);
                }

                offset += transfer_size;
            }

            if batch_accept_config.partial_reject_by_transfer_sequence_id
                && !rejected_sequences.is_empty() {
                let transfer_size = TRANSFER_BASE_SIZE + METADATA_SIZE;
                let response_buf = &mut conn.payload_buf;

                valid_indices.retain(|&ids| {
                    let offset = BatchRequestHeader::SIZE + (ids as usize) * transfer_size;
                    let seq_id_start = offset + WireTransfer::TRANSFER_SEQUENCE_ID_OFFSET;
                    let seq_id_end = seq_id_start + 16;
                    let mut seq_id = [0u8; 16];
                    seq_id.copy_from_slice(&payload[seq_id_start..seq_id_end]);

                    if seq_id != ZERO_UUID && rejected_sequences.contains(&seq_id) {
                        Reject {
                            transfer_id: {
                                let mut reject_transfer_id = [0u8; 16];
                                reject_transfer_id.copy_from_slice(&payload[offset..offset + 16]);
                                reject_transfer_id
                            },
                            reason: REJECT_SEQUENCE_GROUP_FAILED,
                        }.encode(response_buf);
                        false
                    } else {
                        true
                    }
                });
            }
        }

        let rejected_any = conn.payload_buf.len() > BatchResponse::SIZE;

        let refused_by_validation = if batch_accept_config.all_or_nothing {
            rejected_any
        } else {
            valid_indices.is_empty()
        };

        let (status, forwarded) = if refused_by_validation {
            (BatchStatus::Failed, 0)
        } else if !rejected_any {
            if Self::write_all_to_pipeline(
                pipeline_rb,
                payload,
                &header,
                count,
                transfer_size,
                conn.connection_id,
                pipeline_wait_max_yields,
            ) {
                (BatchStatus::Accepted, count)
            } else {
                (BatchStatus::Busy, 0)
            }
        } else if Self::write_selected_to_pipeline(
            pipeline_rb,
            payload,
            &header,
            transfer_size,
            conn.connection_id,
            valid_indices,
            pipeline_wait_max_yields,
        ) {
            (BatchStatus::WithRejects, valid_indices.len())
        } else {
            (BatchStatus::Busy, 0)
        };

        if status == BatchStatus::Busy {
            conn.payload_buf.truncate(BatchResponse::SIZE);
        }

        BatchResponse::fill(status, &mut conn.payload_buf);
        let reject_count = BatchResponse::reject_count_in(&conn.payload_buf);

        if Self::send_batch_response(conn, worker_id) {
            return true;
        }

        println!(
            "[worker {worker_id}] batch response: status = {status:?}, \
             rejects = {reject_count}, forwarded = {forwarded}",
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
        pipeline_wait_max_yields: u32,
    ) -> bool {
        let Some(mut batch) =
            pipeline_rb.try_claim_batch(valid_indices.len(), pipeline_wait_max_yields)
        else {
            return false;
        };

        for (slot_index, &transfer_index) in valid_indices.iter().enumerate() {
            let offset = BatchRequestHeader::SIZE + (transfer_index as usize) * transfer_size;
            let transfer_data = &payload[offset..offset + transfer_size];
            Self::fill_slot(batch.slot_mut(slot_index), transfer_data, &header.batch_id, connection_id);
        }

        batch.publish();
        true
    }

    /// Frames and writes the response the caller has already built in the
    /// connection's payload buffer.
    ///
    /// The buffer arrives complete: head filled in, rejects appended
    /// behind it. This call only wraps it in a message frame and puts it
    /// on the socket, so nothing here allocates and nothing here decides
    /// what the response says.
    ///
    /// Returns whether the connection must close, which is true only if
    /// the write itself failed.
    fn send_batch_response(
        conn: &mut Connection,
        worker_id: usize,
    ) -> bool {
        conn.write_buf.clear();
        Codec::encode_response(
            MSG_BATCH_RESPONSE,
            &conn.payload_buf,
            &mut conn.write_buf,
        );

        if let Err(error) = conn.stream.write_all(&conn.write_buf) {
            println!("[worker {worker_id}] write error: {error}");
            return true;
        }

        false
    }

    /// Answers a batch that was refused as a whole, before any transfer
    /// was looked at or after none could be admitted.
    ///
    /// The response carries no rejects on purpose: a reject describes one
    /// transfer, and the cause here is the batch.
    fn send_whole_batch_refusal(
        conn: &mut Connection,
        batch_id: Uuid,
        status: BatchStatus,
        worker_id: usize,
    ) -> bool {
        BatchResponse::reserve(&batch_id, &mut conn.payload_buf);
        BatchResponse::fill(status, &mut conn.payload_buf);

        Self::send_batch_response(conn, worker_id)
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
                (slot as *mut IncomingSlot as *mut u8)
                    .add(std::mem::offset_of!(IncomingSlot, transfer_id)),
                TRANSFER_BASE_SIZE,
            );
        }
    }

    fn handle_handshake(
        conn: &mut Connection,
        msg_type: u8,
        frame: FrameRange,
        worker_id: usize,
    ) -> bool {
        if msg_type != MSG_HANDSHAKE_REQUEST {
            println!(
                "[worker {worker_id}] expected handshake, got 0x{msg_type:02X} — closing",
            );
            return true;
        }

        let Some(request) = HandshakeRequest::decode(conn.codec.payload(frame)) else {
            println!("[worker {worker_id}] invalid handshake payload — closing");
            return true;
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

    /// Copies one sixteen-byte identifier out of the record.
    ///
    /// Used only where an owned value is genuinely needed — the reject
    /// that travels back to the client, and the set that detects a
    /// repeated identifier within one batch. Every comparison below
    /// reads the bytes in place instead.
    #[inline]
    fn identifier_at(transfer_data: &[u8], offset: usize) -> Uuid {
        let mut identifier = ZERO_UUID;
        identifier.copy_from_slice(
            &transfer_data[offset..offset + size_of::<Uuid>()]
        );
        identifier
    }

    /// Whether the field at this offset is all zero bytes.
    ///
    /// This answers both questions validation asks of a field: an
    /// identifier of zero is absent, and an amount of zero is invalid.
    /// The amount case works on the bytes because the wire carries the
    /// number most significant byte first — a zero is a run of zero
    /// bytes whichever way the machine would read it — so the value
    /// never has to be assembled to be rejected.
    #[inline]
    fn field_is_zero(transfer_data: &[u8], from: usize, to: usize) -> bool {
        transfer_data[from..to].iter().all(
            |&byte| byte == 0
        )
    }

    /// Validates a transfer where it lies in the connection's read
    /// buffer, without assembling it into a record first.
    ///
    /// **For defence:** the earlier shape decoded all 112 bytes into a
    /// struct, of which validation then read four fields — and the same
    /// bytes were copied a second time, straight from this buffer into
    /// the ring slot, a few lines later. So every transfer crossed
    /// memory twice on the way in, and the first crossing was almost
    /// entirely waste: half the copied bytes were never looked at, and
    /// the half that was is only compared, which needs no copy at all.
    /// At the throughput this path is built for that is on the order of
    /// a hundred megabytes a second of pure overhead. What remains is
    /// one sixteen-byte copy per transfer, for the duplicate check that
    /// has to own what it stores.
    fn validate_transfer(transfer_data: &[u8], seen_ids: &mut HashSet<Uuid>) -> Option<u8> {
        debug_assert!(transfer_data.len() >= TRANSFER_BASE_SIZE);

        if Self::field_is_zero(
            transfer_data,
            WireTransfer::TRANSFER_ID_OFFSET,
            WireTransfer::IDEMPOTENCY_KEY_OFFSET,
        ) {
            return Some(REJECT_INVALID_TRANSFER_ID);
        }

        let debit_is_absent = Self::field_is_zero(
            transfer_data,
            WireTransfer::DEBIT_ACCOUNT_ID_OFFSET,
            WireTransfer::CREDIT_ACCOUNT_ID_OFFSET,
        );

        let credit_is_absent = Self::field_is_zero(
            transfer_data,
            WireTransfer::CREDIT_ACCOUNT_ID_OFFSET,
            WireTransfer::AMOUNT_OFFSET,
        );

        if debit_is_absent || credit_is_absent {
            return Some(REJECT_INVALID_ACCOUNT_ID);
        }

        if Self::field_is_zero(
            transfer_data,
            WireTransfer::AMOUNT_OFFSET,
            WireTransfer::CURRENCY_OFFSET,
        ) {
            return Some(REJECT_INVALID_AMOUNT);
        }

        let transfer_id = Self::identifier_at(transfer_data, WireTransfer::TRANSFER_ID_OFFSET);
        if !seen_ids.insert(transfer_id) {
            return Some(REJECT_DUPLICATE_TRANSFER_ID_IN_BATCH)
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire_transfer_with_distinct_bytes() -> [u8; TRANSFER_BASE_SIZE] {
        let mut wire_transfer = [0u8; TRANSFER_BASE_SIZE];
        for (index, byte) in wire_transfer.iter_mut().enumerate() {
            *byte = (index as u8).wrapping_add(1);
        }
        wire_transfer
    }

    #[test]
    fn miri_fill_slot_copies_the_whole_transfer_block_into_the_slot() {
        let ring = MpscRingBuffer::<IncomingSlot>::new(4)
            .expect("a four-slot ingress ring must be constructible");
        let wire_transfer = wire_transfer_with_distinct_bytes();
        let batch_id = [0xABu8; 16];
        let connection_id = 0x0102_0304_0506_0708u64;

        let mut batch = ring
            .try_claim_batch(1, 64)
            .expect("an empty ring must admit a batch of one");
        Worker::fill_slot(batch.slot_mut(0), &wire_transfer, &batch_id, connection_id);
        batch.publish();

        let drained = ring.drain_batch(1);
        assert_eq!(drained.len(), 1);

        let slot = drained.slot(0);
        assert_eq!(slot.batch_id, batch_id);
        assert_eq!(slot.connection_id, connection_id);
        assert_eq!(
            &slot.transfer_id[..],
            &wire_transfer[WireTransfer::TRANSFER_ID_OFFSET..WireTransfer::IDEMPOTENCY_KEY_OFFSET],
        );
        assert_eq!(
            &slot.transfer_datetime[..],
            &wire_transfer[WireTransfer::TRANSFER_DATETIME_OFFSET..TRANSFER_BASE_SIZE],
            "the last field of the block must land at the end of the copy",
        );

        drained.release();
    }

    #[test]
    fn miri_fill_slot_leaves_neighbouring_slots_untouched() {
        let ring = MpscRingBuffer::<IncomingSlot>::new(4)
            .expect("a four-slot ingress ring must be constructible");
        let wire_transfer = wire_transfer_with_distinct_bytes();

        let mut batch = ring
            .try_claim_batch(2, 64)
            .expect("an empty ring must admit a batch of two");
        Worker::fill_slot(batch.slot_mut(0), &wire_transfer, &[0x11u8; 16], 1);
        Worker::fill_slot(batch.slot_mut(1), &wire_transfer, &[0x22u8; 16], 2);
        batch.publish();

        let drained = ring.drain_batch(2);
        assert_eq!(drained.len(), 2);
        assert_eq!(drained.slot(0).batch_id, [0x11u8; 16]);
        assert_eq!(drained.slot(0).connection_id, 1);
        assert_eq!(drained.slot(1).batch_id, [0x22u8; 16]);
        assert_eq!(drained.slot(1).connection_id, 2);
        drained.release();
    }

    #[test]
    fn miri_fill_slot_accepts_a_codec_slice_while_a_sibling_field_is_borrowed() {
        struct FrameSource {
            codec: Codec,
            seen_ids: HashSet<Uuid>,
        }

        let wire_transfer = wire_transfer_with_distinct_bytes();

        let mut request_payload = Vec::with_capacity(BatchRequestHeader::SIZE + TRANSFER_BASE_SIZE);
        request_payload.extend_from_slice(&[0xCDu8; 16]);
        request_payload.extend_from_slice(&1u16.to_be_bytes());
        request_payload.extend_from_slice(&wire_transfer);

        let mut request_frame = Vec::new();
        Codec::encode_request(MSG_BATCH_REQUEST, &request_payload, &mut request_frame);

        let mut frame_source = FrameSource {
            codec: Codec::new(4096),
            seen_ids: HashSet::new(),
        };
        frame_source.codec.read_buf()[..request_frame.len()]
            .copy_from_slice(&request_frame);
        frame_source.codec.advance(request_frame.len());

        let FrameResult::Complete(_, frame) = frame_source.codec.try_decode_request() else {
            panic!("the frame must decode whole");
        };

        let seen_ids = &mut frame_source.seen_ids;
        let payload = frame_source.codec.payload(frame);
        let header = BatchRequestHeader::decode(payload).expect("header must decode");
        let transfer_data =
            &payload[BatchRequestHeader::SIZE..BatchRequestHeader::SIZE + TRANSFER_BASE_SIZE];

        let mut slot = IncomingSlot::zeroed();
        Worker::fill_slot(&mut slot, transfer_data, &header.batch_id, 7);

        seen_ids.insert(header.batch_id);
        assert_eq!(seen_ids.len(), 1);

        assert_eq!(slot.batch_id, [0xCDu8; 16]);
        assert_eq!(slot.connection_id, 7);
        assert_eq!(&slot.transfer_id[..], &wire_transfer[..16]);
        assert_eq!(
            &slot.transfer_datetime[..],
            &wire_transfer[WireTransfer::TRANSFER_DATETIME_OFFSET..TRANSFER_BASE_SIZE],
        );
    }
}