pub type Uuid = [u8; 16];

pub struct HandshakeRequest {
    pub client_id: Uuid,
    pub conn_type: u8,
    pub protocol_version: u16,
}

pub struct HandshakeResponse {
    pub status: u8,
}

impl HandshakeRequest {
    pub const SIZE: usize = 16 + 1 + 2;

    pub fn decode(payload: &[u8]) -> Option<Self> {
        if payload.len() < Self::SIZE {
            return None;
        }

        let mut client_id = [0u8; 16];
        client_id.copy_from_slice(&payload[0..16]);

        let conn_type = payload[16];

        let protocol_version = u16::from_be_bytes([payload[17], payload[18]]);

        Some(Self { client_id, conn_type, protocol_version })
    }
}

impl HandshakeResponse {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(self.status);
    }
}

