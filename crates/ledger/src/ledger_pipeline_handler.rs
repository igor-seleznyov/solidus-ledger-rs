use std::sync::Arc;
use common::raw_u128_to_u64::raw_u128_to_u64;
use ringbuf::mpsc_ring_buffer::MpscRingBuffer;
use pipeline::incoming_slot::IncomingSlot;
use pipeline::partition_slot::{PartitionSlot, ENTRY_TYPE_DEBIT, ENTRY_TYPE_CREDIT, MSG_TYPE_PREPARE};
use pipeline::pipeline_handler::PipelineHandler;
use common::resolve_partition::resolve_partition;
use pipeline::transfer_hash_table_entry::TransferHashTableEntry;
use storage::ls_writer_slot::{LsWriterSlot, LS_MSG_ADD_TO_HEAP};
use crate::partition_overrides::PartitionAssignmentsOverrides;
use crate::transfer_hash_table::TransferHashTable;

pub struct LedgerPipelineHandler {
    seed_k0: u64,
    seed_k1: u64,
    partition_mask: usize,
    overrides: PartitionAssignmentsOverrides,
    transfer_hash_tables: Vec<Arc<TransferHashTable>>,
    ls_writer_rbs: Vec<Arc<MpscRingBuffer<LsWriterSlot>>>,
    decision_maker_shard_mask: usize,
}

impl LedgerPipelineHandler {
    pub fn new(
        seed_k0: u64,
        seed_k1: u64,
        num_partitions: usize,
        overrides: PartitionAssignmentsOverrides,
        transfer_hash_tables: Vec<Arc<TransferHashTable>>,
        ls_writer_rbs: Vec<Arc<MpscRingBuffer<LsWriterSlot>>>,
        decision_maker_shards_count: usize,
    ) -> Self {
        assert!(num_partitions.is_power_of_two(), "num_partitions must be a power of two");
        assert!(decision_maker_shards_count.is_power_of_two());
        assert!(!transfer_hash_tables.is_empty());
        assert_eq!(transfer_hash_tables.len(), decision_maker_shards_count);
        Self {
            seed_k0,
            seed_k1,
            partition_mask: num_partitions - 1,
            overrides,
            transfer_hash_tables,
            ls_writer_rbs,
            decision_maker_shard_mask: decision_maker_shards_count - 1,
        }
    }
}

impl PipelineHandler for LedgerPipelineHandler {
    fn handle(
        &mut self,
        slot: &IncomingSlot,
        gsn: u64,
        partition_rb: &[Arc<MpscRingBuffer<PartitionSlot>>],
    ) {
        let amount = i64::from_be_bytes(slot.amount);

        let currency = slot.currency;

        let (id_hi, id_lo) = raw_u128_to_u64(&slot.transfer_id);

        let shard_id = (id_lo as usize) & self.decision_maker_shard_mask;

        let debit_partition = self.resolve_partition(&slot.debit_account_id);
        let credit_partition = self.resolve_partition(&slot.credit_account_id);

        let transfer_datetime = u64::from_be_bytes(slot.transfer_datetime);

        let transfer_hash_table = &self.transfer_hash_tables[shard_id];
        let transfer_hash_table_offset = unsafe {
            transfer_hash_table.insert(
                id_hi,
                id_lo,
                gsn,
                slot.connection_id,
                &slot.batch_id,
                &slot.currency,
                2, //TODO заглушка, без RuleEngine всегда 2
                transfer_datetime,
                &slot.transfer_sequence_id,
            )
        };

        let (debit_account_id_hi, debit_account_id_lo) = raw_u128_to_u64(&slot.debit_account_id);
        let (credit_account_id_hi, credit_account_id_lo) = raw_u128_to_u64(&slot.credit_account_id);

        unsafe {
            transfer_hash_table.fill_entry(
                transfer_hash_table_offset,
                0,
                &TransferHashTableEntry {
                    account_id_hi: debit_account_id_hi,
                    account_id_lo: debit_account_id_lo,
                    amount,
                    partition_id: debit_partition as u32,
                    entry_type: ENTRY_TYPE_DEBIT,
                    _pad: [0; 3],
                }
            );

            transfer_hash_table.fill_entry(
                transfer_hash_table_offset,
                1,
                &TransferHashTableEntry {
                    account_id_hi: credit_account_id_hi,
                    account_id_lo: credit_account_id_lo,
                    amount,
                    partition_id: credit_partition as u32,
                    entry_type: ENTRY_TYPE_CREDIT,
                    _pad: [0; 3],
                }
            );

            transfer_hash_table.publish(transfer_hash_table_offset);

            let mut claimed = self.ls_writer_rbs[shard_id].claim();
            let ls_slot = claimed.as_mut();
            ls_slot.msg_type = LS_MSG_ADD_TO_HEAP;
            ls_slot.gsn = gsn;
            claimed.publish();
        }

        self.send_prepare(
            partition_rb,
            gsn,
            &slot.transfer_id,
            &slot.debit_account_id,
            amount,
            ENTRY_TYPE_DEBIT,
            debit_partition,
            transfer_hash_table_offset,
            shard_id as u8,
        );

        self.send_prepare(
            partition_rb,
            gsn,
            &slot.transfer_id,
            &slot.credit_account_id,
            amount,
            ENTRY_TYPE_CREDIT,
            credit_partition,
            transfer_hash_table_offset,
            shard_id as u8,
        );
    }
}

impl LedgerPipelineHandler {
    fn send_prepare(
        &self,
        partition_rb: &[Arc<MpscRingBuffer<PartitionSlot>>],
        gsn: u64,
        transfer_id: &[u8; 16],
        account_id: &[u8; 16],
        amount: i64,
        entry_type: u8,
        partition_id: usize,
        transfer_hash_table_offset: u32,
        shard_id: u8,
    ) {
        let partition_id = match self.overrides.get(account_id) {
            Some(id) => id,
            None => {
                let id_hi = u64::from_be_bytes(
                    [
                        account_id[0], account_id[1], account_id[2], account_id[3],
                        account_id[4], account_id[5], account_id[6], account_id[7],
                    ]
                );

                let id_lo = u64::from_be_bytes(
                    [
                        account_id[8], account_id[9], account_id[10], account_id[11],
                        account_id[12], account_id[13], account_id[14], account_id[15],
                    ]
                );

                resolve_partition(
                    id_hi, id_lo, self.seed_k0, self.seed_k1, self.partition_mask,
                )
            }
        };

        let mut claimed = partition_rb[partition_id].claim();
        let slot = claimed.as_mut();
        slot.gsn = gsn;
        slot.transfer_id = *transfer_id;
        slot.account_id = *account_id;
        slot.amount = amount;
        slot.entry_type = entry_type;
        slot.msg_type = MSG_TYPE_PREPARE;
        slot.shard_id = shard_id;
        slot.transfer_hash_table_offset = transfer_hash_table_offset;
        claimed.publish();
    }

    fn resolve_partition(&self, account_id: &[u8; 16]) -> usize {
        match self.overrides.get(account_id) {
            Some(id) => id,
            None => {
                let (id_hi, id_lo) = raw_u128_to_u64(&account_id);
                resolve_partition(id_hi, id_lo, self.seed_k0, self.seed_k1, self.partition_mask)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipeline::incoming_slot::IncomingSlot;

    const K0: u64 = 0x0123456789ABCDEF;
    const K1: u64 = 0xFEDCBA9876543210;

    fn make_handler(num_partitions: usize) -> LedgerPipelineHandler {
        let overrides = PartitionAssignmentsOverrides::empty();
        let tht = vec![
            Arc::new(TransferHashTable::new(64, K0, K1, 8).unwrap())
        ];
        let ls_writer_rb = Arc::new(MpscRingBuffer::<LsWriterSlot>::new(64).unwrap());
        let ls_writer_rbs = vec![ls_writer_rb.clone()];
        LedgerPipelineHandler::new(
            K0,
            K1,
            num_partitions,
            overrides,
            tht,
            ls_writer_rbs,
            1
        )
    }

    fn make_incoming(debit: [u8; 16], credit: [u8; 16], amount: i64) -> IncomingSlot {
        let mut slot = IncomingSlot::zeroed();
        slot.transfer_id = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        slot.debit_account_id = debit;
        slot.credit_account_id = credit;
        slot.amount = amount.to_be_bytes();
        slot
    }

    fn account_id(val: u64) -> [u8; 16] {
        let mut id = [0u8; 16];
        id[8..16].copy_from_slice(&val.to_be_bytes());
        id
    }

    #[test]
    fn produces_two_partition_slots() {
        let num_partitions = 16;
        let partition_rb: Vec<Arc<MpscRingBuffer<PartitionSlot>>> = (0..num_partitions)
            .map(|_| Arc::new(MpscRingBuffer::new(64).unwrap()))
            .collect();

        let overrides = PartitionAssignmentsOverrides::empty();
        let tht = vec![
            Arc::new(TransferHashTable::new(64, K0, K1, 8).unwrap())
        ];
        let ls_writer_rb = Arc::new(MpscRingBuffer::<LsWriterSlot>::new(64).unwrap());
        let ls_writer_rbs = vec![ls_writer_rb.clone()];

        let mut handler = LedgerPipelineHandler::new(K0, K1, num_partitions, overrides, tht.clone(), ls_writer_rbs, 1);

        let slot = make_incoming(account_id(10), account_id(20), 500);
        handler.handle(&slot, 1, &partition_rb);

        let mut total = 0;
        for rb in &partition_rb {
            let batch = rb.drain_batch(64);
            total += batch.len();
            batch.release();
        }
        assert_eq!(total, 2, "one transfer must produce exactly 2 partition slots");
    }

    #[test]
    fn debit_and_credit_entries() {
        let num_partitions = 16;
        let partition_rb: Vec<Arc<MpscRingBuffer<PartitionSlot>>> = (0..num_partitions)
            .map(|_| Arc::new(MpscRingBuffer::new(64).unwrap()))
            .collect();

        let overrides = PartitionAssignmentsOverrides::empty();
        let tht = vec![
            Arc::new(TransferHashTable::new(64, K0, K1, 8).unwrap())
        ];
        let ls_writer_rb = Arc::new(MpscRingBuffer::<LsWriterSlot>::new(64).unwrap());
        let ls_writer_rbs = vec![ls_writer_rb.clone()];

        let mut handler = LedgerPipelineHandler::new(K0, K1, num_partitions, overrides, tht, ls_writer_rbs, 1);
        let debit_acc = account_id(10);
        let credit_acc = account_id(20);

        let slot = make_incoming(debit_acc, credit_acc, 500);
        handler.handle(&slot, 42, &partition_rb);

        let mut entries: Vec<PartitionSlot> = Vec::new();
        for rb in &partition_rb {
            let batch = rb.drain_batch(64);
            for i in 0..batch.len() {
                entries.push(*batch.slot(i));
            }
            batch.release();
        }

        assert_eq!(entries.len(), 2);

        let debit_entry = entries.iter().find(|e| e.entry_type == ENTRY_TYPE_DEBIT).unwrap();
        assert_eq!(debit_entry.account_id, debit_acc);
        assert_eq!(debit_entry.amount, 500);
        assert_eq!(debit_entry.gsn, 42);
        assert_eq!(debit_entry.msg_type, MSG_TYPE_PREPARE);

        let credit_entry = entries.iter().find(|e| e.entry_type == ENTRY_TYPE_CREDIT).unwrap();
        assert_eq!(credit_entry.account_id, credit_acc);
        assert_eq!(credit_entry.amount, 500);
        assert_eq!(credit_entry.gsn, 42);
    }

    #[test]
    fn same_account_same_partition() {
        let num_partitions = 16;
        let partition_rb: Vec<Arc<MpscRingBuffer<PartitionSlot>>> = (0..num_partitions)
            .map(|_| Arc::new(MpscRingBuffer::new(64).unwrap()))
            .collect();

        let overrides = PartitionAssignmentsOverrides::empty();
        let tht = vec![
            Arc::new(TransferHashTable::new(64, K0, K1, 8).unwrap())
        ];
        let ls_writer_rb = Arc::new(MpscRingBuffer::<LsWriterSlot>::new(64).unwrap());
        let ls_writer_rbs = vec![ls_writer_rb.clone()];

        let mut handler = LedgerPipelineHandler::new(K0, K1, num_partitions, overrides, tht, ls_writer_rbs, 1);
        let acc = account_id(42);

        let slot1 = make_incoming(acc, account_id(100), 300);
        let slot2 = make_incoming(acc, account_id(200), 700);

        handler.handle(&slot1, 1, &partition_rb);
        handler.handle(&slot2, 2, &partition_rb);

        let mut debit_partitions = Vec::new();
        for (p, rb) in partition_rb.iter().enumerate() {
            let batch = rb.drain_batch(64);
            for i in 0..batch.len() {
                let entry = batch.slot(i);
                if entry.entry_type == ENTRY_TYPE_DEBIT {
                    debit_partitions.push(p);
                }
            }
            batch.release();
        }

        assert_eq!(debit_partitions.len(), 2);
        assert_eq!(
            debit_partitions[0], debit_partitions[1],
            "same account must always go to the same partition"
        );
    }

    #[test]
    fn partition_routing_uses_account_not_transfer() {
        let num_partitions = 16;
        let partition_rb: Vec<Arc<MpscRingBuffer<PartitionSlot>>> = (0..num_partitions)
            .map(|_| Arc::new(MpscRingBuffer::new(64).unwrap()))
            .collect();

        let overrides = PartitionAssignmentsOverrides::empty();
        let tht = vec![
            Arc::new(TransferHashTable::new(64, K0, K1, 8).unwrap())
        ];
        let ls_writer_rb = Arc::new(MpscRingBuffer::<LsWriterSlot>::new(64).unwrap());
        let ls_writer_rbs = vec![ls_writer_rb.clone()];

        let mut handler = LedgerPipelineHandler::new(K0, K1, num_partitions, overrides, tht, ls_writer_rbs, 1);

        let slot = make_incoming(account_id(1), account_id(1000), 100);
        handler.handle(&slot, 1, &partition_rb);

        let mut entries = Vec::new();
        for rb in &partition_rb {
            let batch = rb.drain_batch(64);
            for i in 0..batch.len() {
                entries.push(*batch.slot(i));
            }
            batch.release();
        }

        assert_eq!(entries[0].gsn, entries[1].gsn);
        assert_eq!(entries[0].transfer_id, entries[1].transfer_id);
    }

    #[test]
    fn override_routes_to_specified_partition() {
        let num_partitions = 16;
        let partition_rb: Vec<Arc<MpscRingBuffer<PartitionSlot>>> = (0..num_partitions)
            .map(|_| Arc::new(MpscRingBuffer::new(64).unwrap()))
            .collect();

        let target_partition = 5;
        let hot_account = account_id(42);

        let mut overrides_map = std::collections::HashMap::new();
        overrides_map.insert(hot_account, target_partition);
        let overrides = PartitionAssignmentsOverrides::from_map(overrides_map);
        let tht = vec![
            Arc::new(TransferHashTable::new(64, K0, K1, 8).unwrap())
        ];
        let ls_writer_rb = Arc::new(MpscRingBuffer::<LsWriterSlot>::new(64).unwrap());
        let ls_writer_rbs = vec![ls_writer_rb.clone()];

        let mut handler = LedgerPipelineHandler::new(K0, K1, num_partitions, overrides, tht, ls_writer_rbs, 1);

        let slot = make_incoming(hot_account, account_id(100), 500);
        handler.handle(&slot, 1, &partition_rb);

        let batch = partition_rb[target_partition].drain_batch(64);
        let mut found_debit = false;
        for i in 0..batch.len() {
            if batch.slot(i).entry_type == ENTRY_TYPE_DEBIT {
                found_debit = true;
            }
        }
        batch.release();

        assert!(found_debit, "hot account must route to override partition");
    }

    #[test]
    fn partition_slot_has_tht_offset_and_shard_id() {
        let num_partitions = 16;
        let partition_rb: Vec<Arc<MpscRingBuffer<PartitionSlot>>> = (0..num_partitions)
            .map(|_| Arc::new(MpscRingBuffer::new(64).unwrap()))
            .collect();

        let tht = vec![
            Arc::new(TransferHashTable::new(64, K0, K1, 8).unwrap())
        ];
        let ls_writer_rb = Arc::new(MpscRingBuffer::<LsWriterSlot>::new(64).unwrap());
        let ls_writer_rbs = vec![ls_writer_rb.clone()];

        let overrides = PartitionAssignmentsOverrides::empty();
        let mut handler = LedgerPipelineHandler::new(K0, K1, num_partitions, overrides, tht.clone(), ls_writer_rbs, 1);

        let slot = make_incoming(account_id(10), account_id(20), 500);
        handler.handle(&slot, 1, &partition_rb);

        let mut entries = Vec::new();
        for rb in &partition_rb {
            let batch = rb.drain_batch(64);
            for i in 0..batch.len() {
                entries.push(*batch.slot(i));
            }
            batch.release();
        }

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].transfer_hash_table_offset, entries[1].transfer_hash_table_offset);
        assert_eq!(entries[0].shard_id, 0);
        assert_eq!(entries[1].shard_id, 0);
        assert_eq!(tht[0].count(), 1);
    }

    #[test]
    fn tht_slot_published_after_handle() {
        let num_partitions = 16;
        let partition_rb: Vec<Arc<MpscRingBuffer<PartitionSlot>>> = (0..num_partitions)
            .map(|_| Arc::new(MpscRingBuffer::new(64).unwrap()))
            .collect();

        let tht = vec![
            Arc::new(TransferHashTable::new(64, K0, K1, 8).unwrap())
        ];
        let ls_writer_rb = Arc::new(MpscRingBuffer::<LsWriterSlot>::new(64).unwrap());
        let ls_writer_rbs = vec![ls_writer_rb.clone()];
        let overrides = PartitionAssignmentsOverrides::empty();
        let mut handler = LedgerPipelineHandler::new(K0, K1, num_partitions, overrides, tht.clone(), ls_writer_rbs, 1);

        let slot = make_incoming(account_id(10), account_id(20), 500);
        handler.handle(&slot, 42, &partition_rb);

        let mut tht_offset = 0u32;
        for rb in &partition_rb {
            let batch = rb.drain_batch(64);
            if batch.len() > 0 {
                tht_offset = batch.slot(0).transfer_hash_table_offset;
            }
            batch.release();
        }

        unsafe {
            let tht_slot = tht[0].slot_ptr(tht_offset);
            assert_eq!((*tht_slot).ready, 1);
            assert_eq!((*tht_slot).gsn, 42);
            assert_eq!((*tht_slot).entries_count, 2);
            assert_eq!((*tht_slot).entries[0].entry_type, ENTRY_TYPE_DEBIT);
            assert_eq!((*tht_slot).entries[1].entry_type, ENTRY_TYPE_CREDIT);
        }
    }
}