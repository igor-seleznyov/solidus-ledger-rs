use crate::message::Uuid;

pub struct Reject {
    pub transfer_id: Uuid,
    pub reason: u8,
}

impl Reject {
    pub const SIZE: usize = 16 + 1;
    
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.transfer_id);
        out.push(self.reason);
    }
}