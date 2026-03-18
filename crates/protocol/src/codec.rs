use crate::consts::*;

pub struct Codec {
    buf: Vec<u8>,
    len: usize,
}

pub enum FrameResult {
    Complete(u8, usize, usize),
    Incomplete,
    Error(ProtocolError),
}

#[derive(Debug)]
pub enum ProtocolError {
    InvalidMagic,
    PayloadTooLarge(u32),
    UnknownMsgType(u8),
}

impl Codec {
    pub fn new() -> Self {
        Self {
            buf: vec![0u8; 4096],
            len: 0,
        }
    }

    pub fn read_buf(&mut self) -> &mut [u8] {
        if self.len == self.buf.len() {
            self.buf.resize(self.buf.len() * 2, 0);
        }
        &mut self.buf[self.len..]
    }

    pub fn advance(&mut self, n: usize) {
        self.len += n;
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

        let total = HEADER_SIZE + payload_len as usize;

        if self.len < total {
            return FrameResult::Incomplete;
        }

        FrameResult::Complete(msg_type, HEADER_SIZE, total)
    }

    pub fn payload(&self, start: usize, end: usize) -> &[u8] {
        &self.buf[start..end]
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