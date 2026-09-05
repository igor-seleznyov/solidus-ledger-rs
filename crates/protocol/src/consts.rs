pub const MAGIC_REQUEST:  [u8; 8] = *b"SLDLGRRQ";
pub const MAGIC_RESPONSE: [u8; 8] = *b"SLDLGRRS";

pub const HEADER_SIZE: usize = 13;

pub const MAX_PAYLOAD_SIZE: u32 = 16 * 1024 * 1024;

pub const MSG_HANDSHAKE_REQUEST:  u8 = 0x01;
pub const MSG_HANDSHAKE_RESPONSE: u8 = 0x02;
pub const MSG_BATCH_REQUEST:      u8 = 0x10;
pub const MSG_BATCH_RESPONSE:     u8 = 0x11;
pub const MSG_SINGLE_REQUEST:     u8 = 0x12;
pub const MSG_SINGLE_RESPONSE:    u8 = 0x13;
pub const MSG_BATCH_RESULT:       u8 = 0x20;
pub const MSG_HEARTBEAT:          u8 = 0xFF;

pub const CONN_COMMAND: u8 = 1;
pub const CONN_RESULT:  u8 = 2;

pub const HS_OK:                   u8 = 0;
pub const HS_UNSUPPORTED_VERSION:  u8 = 1;
pub const HS_ALREADY_CONNECTED:    u8 = 2;


const BATCH_STATUS_ACCEPTED: u8 = 0;
const BATCH_STATUS_FAILED: u8 = 1;
const BATCH_STATUS_WITH_REJECTS: u8 = 2;
const BATCH_STATUS_BUSY: u8 = 3;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BatchStatus {
    /// Every transfer passed primary validation and the whole batch
    /// went to the pipeline. The client waits for the asynchronous
    /// per-transfer outcome and sends nothing again.
    Accepted = BATCH_STATUS_ACCEPTED,

    /// Nothing was admitted, and sending the same bytes again cannot
    /// change that — the request itself is wrong. Either every
    /// transfer failed validation, or the batch declared no transfers,
    /// or it declared more than this server takes at once. The client
    /// corrects the request; retrying it unchanged loops forever.
    Failed = BATCH_STATUS_FAILED,

    /// Part of the batch went through and part did not. The rejects
    /// list names every transfer that failed and why; the ones not
    /// named were admitted. The client resends only the named ones.
    WithRejects = BATCH_STATUS_WITH_REJECTS,

    /// Nothing was admitted, but the request is sound — the server
    /// could not take it at this moment, because the ring buffer
    /// carrying transfers into the pipeline stayed full longer than
    /// the worker waits. Nothing was applied, so the identical bytes
    /// may be sent again after a pause and will duplicate nothing.
    /// The connection stays open: closing it would turn a moment of
    /// load into a reconnect storm exactly when the server can least
    /// absorb one.
    Busy = BATCH_STATUS_BUSY,
}

impl BatchStatus {
    pub const fn as_byte(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for BatchStatus {
    type Error = u8;

    /// Returns the unrecognised byte as the error, so a caller can log
    /// what it actually received rather than that something was wrong.
    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        match byte {
            BATCH_STATUS_ACCEPTED => Ok(Self::Accepted),
            BATCH_STATUS_FAILED => Ok(Self::Failed),
            BATCH_STATUS_WITH_REJECTS => Ok(Self::WithRejects),
            BATCH_STATUS_BUSY => Ok(Self::Busy),
            other => Err(other),
        }
    }
}

pub const REJECT_INVALID_TRANSFER_ID:   u8 = 1;
pub const REJECT_INVALID_ACCOUNT_ID:    u8 = 2;
pub const REJECT_INVALID_AMOUNT:        u8 = 3;
pub const REJECT_INVALID_CURRENCY:      u8 = 4;
pub const REJECT_INVALID_DATETIME:      u8 = 5;
pub const REJECT_DUPLICATE_TRANSFER_ID_IN_BATCH: u8 = 6;
pub const REJECT_SEQUENCE_GROUP_FAILED: u8 = 7;

pub const REJECT_BUSY:                  u8 = 8;

pub const REJECT_INSUFFICIENT_FUNDS:       u8 = 10;
pub const REJECT_DUPLICATE_IDEMPOTENCY:    u8 = 11;
pub const REJECT_ACCOUNT_NOT_FOUND:        u8 = 12;
pub const REJECT_CURRENCY_MISMATCH:        u8 = 13;
pub const REJECT_RULE_VIOLATION:           u8 = 14;
pub const REJECT_INTERNAL_ERROR:           u8 = 15;

pub const RESULT_SUCCESS:       u8 = 0;
pub const RESULT_WITH_REJECTS:  u8 = 1;

#[cfg(test)]
mod tests {
    use super::*;

    /// Every batch status, listed once. The list is what the sweep
    /// below compares the conversion against, so a variant missing
    /// from here is caught rather than silently untested.
    const ALL_BATCH_STATUSES: [BatchStatus; 4] = [
        BatchStatus::Accepted,
        BatchStatus::Failed,
        BatchStatus::WithRejects,
        BatchStatus::Busy,
    ];

    /// The wire value of each status, written out rather than read
    /// from the type.
    ///
    /// The literals are deliberate. Deriving the expectation from the
    /// enum would make this check the code with the same code, and it
    /// would stop catching the one thing it exists to catch: a
    /// discriminant that moved without anyone intending it. Written
    /// files and deployed clients depend on these four numbers, so a
    /// second, independent statement of them is worth its duplication.
    ///
    /// The match carries no wildcard on purpose: adding a variant to
    /// `BatchStatus` stops this file compiling until the new one is
    /// given its number here.
    fn expected_wire_value(status: BatchStatus) -> u8 {
        match status {
            BatchStatus::Accepted => 0,
            BatchStatus::Failed => 1,
            BatchStatus::WithRejects => 2,
            BatchStatus::Busy => 3,
        }
    }

    #[test]
    fn every_status_survives_the_round_trip_through_its_wire_value() {
        for status in ALL_BATCH_STATUSES {
            let wire_value = status.as_byte();

            assert_eq!(
                wire_value,
                expected_wire_value(status),
                "{status:?} does not carry the wire value it is documented to carry",
            );

            assert_eq!(
                BatchStatus::try_from(wire_value),
                Ok(status),
                "{status:?} does not come back from its own wire value",
            );
        }
    }

    #[test]
    fn a_byte_converts_exactly_when_a_status_claims_it() {
        for byte in u8::MIN..=u8::MAX {
            let claimed_by_a_status =
                ALL_BATCH_STATUSES.iter().any(|status| status.as_byte() == byte);

            assert_eq!(
                BatchStatus::try_from(byte).is_ok(),
                claimed_by_a_status,
                "byte {byte} is accepted by the conversion and claimed by no status, \
                 or claimed by a status and refused by the conversion — the two \
                 halves of the mapping have drifted apart",
            );

            if !claimed_by_a_status {
                assert_eq!(
                    BatchStatus::try_from(byte),
                    Err(byte),
                    "a refused byte must be returned to the caller so it can be logged",
                );
            }
        }
    }
}
