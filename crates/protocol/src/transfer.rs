pub const TRANSFER_BASE_SIZE: usize = 112;

/// The transfer exactly as it lies on the wire, field by field.
///
/// This type exists to be the single statement of that layout. Before it,
/// the same sequence of bytes was described twice — once as byte ranges
/// inside the decoder below, and once as the field order of the record the
/// worker copies a transfer into — and the two agreed only because someone
/// had once written them to agree. Nothing checked it, and when a field
/// was removed from the other side every incoming transfer began landing
/// eight bytes off its place, read as whatever the neighbouring field
/// happened to hold.
///
/// Every position is now taken from this declaration through the offset
/// constants below, so a field added, removed, or resized here moves every
/// dependent read with it.
///
/// The numeric fields are raw byte arrays rather than `u64` and `i64`
/// deliberately: the wire carries them most significant byte first, while
/// the machine reads them the other way round, so the bytes are converted
/// where they are used rather than reinterpreted in place.
#[repr(C)]
pub struct WireTransfer {
    pub transfer_id: [u8; 16],
    pub idempotency_key: [u8; 16],
    pub debit_account_id: [u8; 16],
    pub credit_account_id: [u8; 16],
    pub amount: [u8; 8],
    pub currency: [u8; 16],
    pub transfer_sequence_id: [u8; 16],
    pub transfer_datetime: [u8; 8],
}

impl WireTransfer {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    pub const TRANSFER_ID_OFFSET: usize = std::mem::offset_of!(Self, transfer_id);
    pub const IDEMPOTENCY_KEY_OFFSET: usize = std::mem::offset_of!(Self, idempotency_key);
    pub const DEBIT_ACCOUNT_ID_OFFSET: usize = std::mem::offset_of!(Self, debit_account_id);
    pub const CREDIT_ACCOUNT_ID_OFFSET: usize = std::mem::offset_of!(Self, credit_account_id);
    pub const AMOUNT_OFFSET: usize = std::mem::offset_of!(Self, amount);
    pub const CURRENCY_OFFSET: usize = std::mem::offset_of!(Self, currency);
    pub const TRANSFER_SEQUENCE_ID_OFFSET: usize = std::mem::offset_of!(Self, transfer_sequence_id);
    pub const TRANSFER_DATETIME_OFFSET: usize = std::mem::offset_of!(Self, transfer_datetime);
}

const _: () = assert!(WireTransfer::SIZE == TRANSFER_BASE_SIZE);

const _: () = assert!(WireTransfer::TRANSFER_ID_OFFSET == 0);
const _: () = assert!(WireTransfer::IDEMPOTENCY_KEY_OFFSET == 16);
const _: () = assert!(WireTransfer::DEBIT_ACCOUNT_ID_OFFSET == 32);
const _: () = assert!(WireTransfer::CREDIT_ACCOUNT_ID_OFFSET == 48);
const _: () = assert!(WireTransfer::AMOUNT_OFFSET == 64);
const _: () = assert!(WireTransfer::CURRENCY_OFFSET == 72);
const _: () = assert!(WireTransfer::TRANSFER_SEQUENCE_ID_OFFSET == 88);
const _: () = assert!(WireTransfer::TRANSFER_DATETIME_OFFSET == 104);
