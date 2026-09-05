use std::mem::{offset_of, size_of};

use crate::message::Uuid;

/// One rejected transfer as it travels on the wire: which transfer, and
/// why it was refused.
///
/// The layout is fixed rather than merely described, so the positions
/// below are read out of the type instead of written down beside it. A
/// field that moves moves its own offset with it, and the assertions
/// after the impl fail the build rather than letting the wire and the
/// struct drift apart in silence.
#[repr(C)]
pub struct Reject {
    pub transfer_id: Uuid,
    pub reason: u8,
}

impl Reject {
    pub const TRANSFER_ID_OFFSET: usize = offset_of!(Self, transfer_id);
    pub const REASON_OFFSET: usize = offset_of!(Self, reason);
    pub const SIZE: usize = size_of::<Self>();

    /// Appends this reject to the buffer that will be sent.
    ///
    /// The destination is the connection's own response buffer, written
    /// at the moment the rejection is discovered. Nothing collects
    /// rejects in between, so a batch full of them costs no allocation.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.transfer_id);
        out.push(self.reason);
    }
}

const _: () = assert!(Reject::TRANSFER_ID_OFFSET == 0);
const _: () = assert!(Reject::REASON_OFFSET == 16);
const _: () = assert!(Reject::SIZE == 17);
