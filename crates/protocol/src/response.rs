use std::mem::{offset_of, size_of};

use crate::consts::BatchStatus;
use crate::message::Uuid;
use crate::reject::Reject;

/// The fixed head of a batch response: which batch, how it ended, and how
/// many rejects follow.
///
/// On the wire the head is followed immediately by that many `Reject`
/// records. The count precedes them, so it is not known when the head is
/// written — the batch has not been walked yet. The head is therefore
/// reserved first with the two unknown fields left blank, the rejects are
/// appended straight into the same buffer as they are discovered, and the
/// blanks are filled in at the end from what the buffer now holds. The
/// alternative, collecting rejects somewhere and copying them in
/// afterwards, would be a second buffer holding what the first one has to
/// hold anyway, allocated once per batch on the busiest path in the
/// server.
#[repr(C)]
pub struct BatchResponse {
    pub batch_id: Uuid,
    pub status: u8,
    pub reject_count: [u8; 2],
}

impl BatchResponse {
    pub const BATCH_ID_OFFSET: usize = offset_of!(Self, batch_id);
    pub const STATUS_OFFSET: usize = offset_of!(Self, status);
    pub const REJECT_COUNT_OFFSET: usize = offset_of!(Self, reject_count);
    pub const SIZE: usize = size_of::<Self>();

    /// Starts a response in the buffer that will be sent, leaving the
    /// status and the reject count blank.
    pub fn reserve(batch_id: &Uuid, out: &mut Vec<u8>) {
        out.clear();
        out.extend_from_slice(batch_id);
        out.push(0);
        out.extend_from_slice(&0u16.to_be_bytes());
    }

    /// How many rejects the buffer holds beyond the head.
    pub fn reject_count_in(buffer: &[u8]) -> usize {
        (buffer.len() - Self::SIZE) / Reject::SIZE
    }

    /// Fills in the two fields left blank by `reserve`.
    ///
    /// The count is derived from the buffer rather than tracked
    /// separately, so it cannot disagree with the records that follow it.
    pub fn fill(status: BatchStatus, out: &mut [u8]) {
        let reject_count = Self::reject_count_in(out) as u16;
        out[Self::STATUS_OFFSET] = status.as_byte();
        out[Self::REJECT_COUNT_OFFSET..Self::SIZE]
            .copy_from_slice(&reject_count.to_be_bytes());
    }
}

const _: () = assert!(BatchResponse::BATCH_ID_OFFSET == 0);
const _: () = assert!(BatchResponse::STATUS_OFFSET == 16);
const _: () = assert!(BatchResponse::REJECT_COUNT_OFFSET == 17);
const _: () = assert!(BatchResponse::SIZE == 19);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consts::REJECT_INVALID_AMOUNT;

    #[test]
    fn a_whole_batch_refusal_encodes_to_the_head_and_nothing_else() {
        let mut encoded = Vec::new();
        BatchResponse::reserve(&[0x11u8; 16], &mut encoded);
        BatchResponse::fill(BatchStatus::Failed, &mut encoded);

        assert_eq!(
            encoded.len(), BatchResponse::SIZE,
            "batch_id (16) + status (1) + reject_count (2) and nothing else",
        );
        assert_eq!(&encoded[0..16], &[0x11u8; 16]);
        assert_eq!(encoded[BatchResponse::STATUS_OFFSET], BatchStatus::Failed.as_byte());
        assert_eq!(&encoded[BatchResponse::REJECT_COUNT_OFFSET..], &0u16.to_be_bytes());
    }

    #[test]
    fn the_count_in_the_head_matches_the_rejects_that_follow() {
        let mut encoded = Vec::new();
        BatchResponse::reserve(&[0x22u8; 16], &mut encoded);

        for reason_seed in 0..3u8 {
            Reject {
                transfer_id: [reason_seed; 16],
                reason: REJECT_INVALID_AMOUNT,
            }.encode(&mut encoded);
        }

        BatchResponse::fill(BatchStatus::WithRejects, &mut encoded);

        assert_eq!(encoded.len(), BatchResponse::SIZE + 3 * Reject::SIZE);
        assert_eq!(
            &encoded[BatchResponse::REJECT_COUNT_OFFSET..BatchResponse::SIZE],
            &3u16.to_be_bytes(),
        );
        assert_eq!(encoded[BatchResponse::STATUS_OFFSET], BatchStatus::WithRejects.as_byte());

        for (position, reason_seed) in (0..3u8).enumerate() {
            let start = BatchResponse::SIZE + position * Reject::SIZE;
            assert_eq!(&encoded[start..start + 16], &[reason_seed; 16]);
            assert_eq!(encoded[start + Reject::REASON_OFFSET], REJECT_INVALID_AMOUNT);
        }
    }
}
