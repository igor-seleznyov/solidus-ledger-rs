use crate::message::Uuid;

pub const TRANSFER_BASE_SIZE: usize = 112;

pub struct Transfer<'a> {
    pub transfer_id: Uuid,
    pub idempotency_key: Uuid,
    pub debit_account_id: Uuid,
    pub credit_account_id: Uuid,
    pub amount: i64,
    pub currency: [u8; 16],
    pub transfer_sequence_id: Uuid,
    pub transfer_datetime: u64,
    pub metadata: &'a[u8],
}

impl <'a> Transfer<'a> {
    pub fn decode(data: &'a[u8], metadata_size: usize) -> Option<Self> {
        let total = TRANSFER_BASE_SIZE + metadata_size;
        if data.len() < total {
            return None;
        }

        let mut transfer_id = [0u8; 16];
        transfer_id.copy_from_slice(&data[..16]);

        let mut idempotency_key = [0u8; 16];
        idempotency_key.copy_from_slice(&data[16..32]);

        let mut debit_account_id = [0u8; 16];
        debit_account_id.copy_from_slice(&data[32..48]);

        let mut credit_account_id = [0u8; 16];
        credit_account_id.copy_from_slice(&data[48..64]);

        let amount = i64::from_be_bytes(
            [
                data[64], data[65], data[66], data[67],
                data[68], data[69], data[70], data[71],
            ]
        );

        let mut currency = [0u8; 16];
        currency.copy_from_slice(&data[72..88]);

        let mut transfer_sequence_id = [0u8; 16];
        transfer_sequence_id.copy_from_slice(&data[88..104]);

        let transfer_datetime = u64::from_be_bytes(
            [
                data[104], data[105], data[106], data[107],
                data[108], data[109], data[110], data[111],
            ]
        );

        let metadata = &data[112..112 + metadata_size];

        Some(
            Self {
                transfer_id,
                idempotency_key,
                debit_account_id,
                credit_account_id,
                amount,
                currency,
                transfer_sequence_id,
                transfer_datetime,
                metadata,
            }
        )
    }
}