use crate::consts::*;

/// Where a decoded frame's payload lies inside the codec's buffer.
///
/// Two `usize` in a `Copy` wrapper rather than two bare parameters: on
/// the ingress path the start and the end travel together through
/// three signatures alongside other `usize` values, and a pair that
/// swaps silently at a call site is a way to read the wrong bytes. The
/// wrapper costs nothing — it passes in the same two registers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameRange {
    pub start: usize,
    pub end: usize,
}

impl FrameRange {
    #[inline]
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
}

pub struct Codec {
    buf: Vec<u8>,
    len: usize,

    /// How many payload bytes this connection may accumulate for one
    /// message.
    ///
    /// A memory budget, not a limit on what the message asks for. A
    /// batch's transfer count lives inside the payload, so judging it
    /// means having buffered the payload already — which is exactly
    /// the buffering this field exists to bound. A client declaring a
    /// large payload and then dribbling bytes keeps the decode
    /// returning Incomplete while `read_buf` doubles, and the buffer
    /// never shrinks.
    max_message_payload_bytes: usize,
}

pub enum FrameResult {
    Complete(u8, FrameRange),
    Incomplete,
    Error(ProtocolError),
}

#[derive(Debug)]
pub enum ProtocolError {
    InvalidMagic,
    PayloadTooLarge(u32),
    MessagePayloadTooLarge { declared: u32, allowed: usize },
    UnknownMsgType(u8),
}

impl Codec {
    pub fn new(max_message_payload_bytes: usize) -> Self {
        Self {
            buf: vec![0u8; 4096],
            len: 0,
            max_message_payload_bytes,
        }
    }

    pub fn read_buf(&mut self) -> &mut [u8] {
        if self.len == self.buf.len() {
            self.buf.resize(self.buf.len() * 2, 0);
        }
        &mut self.buf[self.len..]
    }

    pub fn advance(&mut self, bytes_read: usize) {
        self.len += bytes_read;
    }

    pub fn try_decode_request(&self) -> FrameResult {
        self.try_decode(&MAGIC_REQUEST)
    }

    pub fn try_decode_response(&self) -> FrameResult {
        self.try_decode(&MAGIC_RESPONSE)
    }

    fn try_decode(&self, expected_magic: &[u8; 8]) -> FrameResult {
        if self.len < HEADER_SIZE {
            return FrameResult::Incomplete;
        }

        if &self.buf[0..8] != expected_magic {
            return FrameResult::Error(ProtocolError::InvalidMagic);
        }

        let msg_type = self.buf[8];

        let payload_len = u32::from_be_bytes(
            [self.buf[9], self.buf[10], self.buf[11], self.buf[12]]
        );

        if payload_len > MAX_PAYLOAD_SIZE {
            return FrameResult::Error(ProtocolError::PayloadTooLarge(payload_len));
        }

        if payload_len as usize > self.max_message_payload_bytes {
            return FrameResult::Error(
                ProtocolError::MessagePayloadTooLarge {
                    declared: payload_len,
                    allowed: self.max_message_payload_bytes,
                }
            );
        }

        let total = HEADER_SIZE + payload_len as usize;

        if self.len < total {
            return FrameResult::Incomplete;
        }

        FrameResult::Complete(msg_type, FrameRange { start: HEADER_SIZE, end: total })
    }

    pub fn payload(&self, range: FrameRange) -> &[u8] {
        &self.buf[range.start..range.end]
    }

    pub fn consume(&mut self, total: usize) {
        self.buf.copy_within(total..self.len, 0);
        self.len -= total;
    }

    pub fn encode_response(msg_type: u8, payload: &[u8], out: &mut Vec<u8>) {
        out.extend_from_slice(&MAGIC_RESPONSE);
        out.push(msg_type);
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
    }

    pub fn encode_request(msg_type: u8, payload: &[u8], out: &mut Vec<u8>) {
        out.extend_from_slice(&MAGIC_REQUEST);
        out.push(msg_type);
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
    }
}