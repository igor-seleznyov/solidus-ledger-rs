use crate::message::Uuid;

pub struct BatchRequestHeader {
    pub batch_id: Uuid,
    pub count: u16,
}

impl BatchRequestHeader {
    pub const SIZE: usize = 16 + 2;
    
    pub fn decode(payload: &[u8]) -> Option<Self> {
        if payload.len() < Self::SIZE {
            return None;
        }
        
        let mut batch_id = [0u8; 16];
        
        batch_id.copy_from_slice(&payload[0..16]);
        let count = u16::from_be_bytes([payload[16], payload[17]]);
        Some(Self { batch_id, count })
    }
}