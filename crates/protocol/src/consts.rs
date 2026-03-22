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

pub const BATCH_ACCEPTED:      u8 = 0;
pub const BATCH_FAILED:        u8 = 1;
pub const BATCH_WITH_REJECTS:  u8 = 2;

pub const REJECT_INVALID_TRANSFER_ID:   u8 = 1;
pub const REJECT_INVALID_ACCOUNT_ID:    u8 = 2;
pub const REJECT_INVALID_AMOUNT:        u8 = 3;
pub const REJECT_INVALID_CURRENCY:      u8 = 4;
pub const REJECT_INVALID_DATETIME:      u8 = 5;
pub const REJECT_DUPLICATE_TRANSFER_ID_IN_BATCH:   u8 = 6;
pub const REJECT_SEQUENCE_GROUP_FAILED: u8 = 7;

pub const REJECT_INSUFFICIENT_FUNDS:       u8 = 10;
pub const REJECT_DUPLICATE_IDEMPOTENCY:    u8 = 11;
pub const REJECT_ACCOUNT_NOT_FOUND:        u8 = 12;
pub const REJECT_CURRENCY_MISMATCH:        u8 = 13;
pub const REJECT_RULE_VIOLATION:           u8 = 14;
pub const REJECT_INTERNAL_ERROR:           u8 = 15;

pub const RESULT_SUCCESS:       u8 = 0;
pub const RESULT_WITH_REJECTS:  u8 = 1;