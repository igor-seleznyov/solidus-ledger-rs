use crate::message::Uuid;
use crate::reject::Reject;

pub struct BatchResponse {
    pub batch_id: Uuid,
    pub status: u8,
    pub rejects: Vec<Reject>
}

impl BatchResponse {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.batch_id);
        out.push(self.status);
        out.extend_from_slice(&(self.rejects.len() as u16).to_be_bytes());
        for reject in &self.rejects {
            reject.encode(out);
        }
    }
}