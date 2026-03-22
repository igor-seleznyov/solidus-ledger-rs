use std::sync::Arc;
use common::raw_u128_to_u64::raw_u128_to_u64;
use ringbuf::mpsc_ring_buffer::MpscRingBuffer;
use pipeline::coordinator_slot::{
    CoordinatorSlot, COORD_PREPARE_SUCCESS, COORD_PREPARE_FAIL,
    COORD_COMMIT_SUCCESS, COORD_ROLLBACK_SUCCESS,
};
use pipeline::partition_slot::{
    PartitionSlot, ENTRY_TYPE_DEBIT, ENTRY_TYPE_CREDIT,
    MSG_TYPE_PREPARE, MSG_TYPE_COMMIT, MSG_TYPE_ROLLBACK
};
use crate::partition_accounts_hash_table::PartitionAccountsHashTable;
use protocol::consts::{ REJECT_INSUFFICIENT_FUNDS, RESULT_SUCCESS };
use common::mem_barrier::{release_store_u64, release_store_u8};
use storage::ls_writer_slot::{LsWriterSlot, LS_MSG_POSTING};
use crate::account_slot::AccountSlot;
use crate::partition_version_table::PartitionVersionTable;

pub struct PartitionActor {
    id: usize,
    partition_rb: Arc<MpscRingBuffer<PartitionSlot>>,
    partition_accounts_hash_table: PartitionAccountsHashTable,
    partition_version_table: PartitionVersionTable,
    coordinator_rbs: Vec<Arc<MpscRingBuffer<CoordinatorSlot>>>,
    partition_version_table_tail: *mut u64,
    ls_writer_rbs: Vec<Arc<MpscRingBuffer<LsWriterSlot>>>,
    batch_size: usize,
}

impl PartitionActor {
    pub fn new(
        id: usize,
        partition_rb: Arc<MpscRingBuffer<PartitionSlot>>,
        partition_accounts_hash_table: PartitionAccountsHashTable,
        partition_version_table: PartitionVersionTable,
        coordinator_rbs: Vec<Arc<MpscRingBuffer<CoordinatorSlot>>>,
        partition_version_table_tail: *mut u64,
        ls_writer_rbs: Vec<Arc<MpscRingBuffer<LsWriterSlot>>>,
        batch_size: usize,
    ) -> Self {
        Self {
            id,
            partition_rb,
            partition_accounts_hash_table,
            partition_version_table,
            coordinator_rbs,
            partition_version_table_tail,
            ls_writer_rbs,
            batch_size,
        }
    }
    
    pub fn run(&mut self) {
        println!("[partition-actor {}] started", self.id);
        
        loop {
            let batch = self.partition_rb.drain_batch(self.batch_size);
            
            if batch.is_empty() {
                std::hint::spin_loop();
                continue;
            }
            
            for i in 0..batch.len() {
                let slot = batch.slot(i);
                self.dispatch(slot);
            }
            
            batch.release();
        }
    }
    
    fn dispatch(&mut self, slot: &PartitionSlot) {
        match slot.msg_type {
            MSG_TYPE_PREPARE => self.handle_prepare(slot),
            MSG_TYPE_COMMIT => self.handle_commit(slot),
            MSG_TYPE_ROLLBACK => self.handle_rollback(slot),
            _ => {
                println!(
                    "[partition-actor {}] unknown msg_type={}, gsn={}",
                    self.id, slot.msg_type, slot.gsn
                );
            }
        }
    }

    fn send_coordinator_response(
        &self,
        slot: &PartitionSlot,
        msg_type: u8,
        reason: u8,
    ) {
        let (transfer_id_hi, transfer_id_lo) = raw_u128_to_u64(&slot.transfer_id);

        let mut claimed = self.coordinator_rbs[slot.shard_id as usize].claim();
        let coord = claimed.as_mut();
        coord.msg_type = msg_type;
        coord.partition_id = self.id as u32;
        coord.shard_id = slot.shard_id;
        coord.reason = reason;
        coord.transfer_hash_table_offset = slot.transfer_hash_table_offset;
        coord.transfer_id_hi = transfer_id_hi;
        coord.transfer_id_lo = transfer_id_lo;
        coord.gsn = slot.gsn;
        coord.entry_index = slot.entry_index;
        claimed.publish();
    }
    
    fn handle_prepare(&mut self, slot: &PartitionSlot) {
        let (id_hi, id_lo) = raw_u128_to_u64(&slot.account_id);

        let account = unsafe { self.partition_accounts_hash_table.get_or_create(id_hi, id_lo) };

        match slot.entry_type {
            ENTRY_TYPE_DEBIT => {
                let effective = unsafe {
                    (*account).balance
                        + (*account).staged_income
                        - (*account).staged_outcome
                };

                if effective >= slot.amount {
                    unsafe {
                        (*account).staged_outcome += slot.amount;
                        (*account).last_gsn = slot.gsn;
                    }
                    self.send_coordinator_response(slot, COORD_PREPARE_SUCCESS, RESULT_SUCCESS);
                } else {
                    self.send_coordinator_response(slot, COORD_PREPARE_FAIL, REJECT_INSUFFICIENT_FUNDS);
                }
            }
            ENTRY_TYPE_CREDIT => {
                unsafe {
                    (*account).staged_income += slot.amount;
                    (*account).last_gsn = slot.gsn;
                }
                self.send_coordinator_response(slot, COORD_PREPARE_SUCCESS, RESULT_SUCCESS);
                println!("[actor {}] PREPARE gsn={}, type=CREDIT, result=OK", self.id, slot.gsn);
            }
            _ => {
                println!(
                    "[partition-actor {}] unknown entry_type={}, gsn={}",
                    self.id, slot.entry_type, slot.gsn
                );
            }
        }
    }

    fn handle_commit(&mut self, slot: &PartitionSlot) {
        let (id_hi, id_lo) = raw_u128_to_u64(&slot.account_id);

        let account = unsafe { self.partition_accounts_hash_table.get_or_create(id_hi, id_lo) };

        match slot.entry_type {
            ENTRY_TYPE_DEBIT => {
                unsafe {
                    (*account).balance -= slot.amount;
                    (*account).staged_outcome -= slot.amount;
                }
            }
            ENTRY_TYPE_CREDIT => {
                unsafe {
                    (*account).balance += slot.amount;
                    (*account).staged_income -= slot.amount;
                }
            }
            _ => {}
        }

        unsafe {
            (*account).ordinal += 1;
            (*account).last_gsn = slot.gsn;
        }

        let new_balance = unsafe { (*account).balance };

        unsafe {
        self.partition_version_table.record_version(
                id_hi, id_lo, slot.gsn, new_balance,
            )
        };

        let tail = unsafe { std::ptr::read(self.partition_version_table_tail) };

        unsafe {
          release_store_u64(self.partition_version_table_tail, tail + 1);
        };

        self.send_posting_record(slot, account);

        self.send_coordinator_response(slot, COORD_COMMIT_SUCCESS, RESULT_SUCCESS);
    }

    fn handle_rollback(&mut self, slot: &PartitionSlot) {
        let (id_hi, id_lo) = raw_u128_to_u64(&slot.account_id);

        unsafe {
            let account = self.partition_accounts_hash_table.get_or_create(id_hi, id_lo);

            match slot.entry_type {
                ENTRY_TYPE_DEBIT => {
                    (*account).staged_outcome -= slot.amount;
                }
                ENTRY_TYPE_CREDIT => {
                    (*account).staged_income -= slot.amount;
                }
                _ => {}
            }

            (*account).last_gsn = slot.gsn;
        }

        self.send_coordinator_response(slot, COORD_ROLLBACK_SUCCESS, RESULT_SUCCESS);
    }

    fn send_posting_record(&self, partition_slot: &PartitionSlot, account: *mut AccountSlot) {
        let shard_id = partition_slot.shard_id as usize;
        let mut claimed = self.ls_writer_rbs[shard_id].claim();
        let ls_slot = claimed.as_mut();

        ls_slot.msg_type = LS_MSG_POSTING;
        ls_slot.gsn = partition_slot.gsn;

        let (posting_transfer_id_hi, posting_transfer_id_lo) = raw_u128_to_u64(&partition_slot.transfer_id);
        (ls_slot.posting.transfer_id_hi, ls_slot.posting.transfer_id_lo) = (posting_transfer_id_hi, posting_transfer_id_lo);

        let (posting_account_id_hi, posting_account_id_lo) = raw_u128_to_u64(&partition_slot.account_id);
        (ls_slot.posting.account_id_hi, ls_slot.posting.account_id_lo) = (posting_account_id_hi, posting_account_id_lo);

        ls_slot.posting.gsn = partition_slot.gsn;
        ls_slot.posting.amount = partition_slot.amount;
        ls_slot.posting.entry_type = partition_slot.entry_type;
        ls_slot.posting.sign = if partition_slot.entry_type == ENTRY_TYPE_DEBIT { -1 } else { 1 };

        ls_slot.posting.ordinal = unsafe { (*account).ordinal };
        ls_slot.posting.prev_posting_record_offset = unsafe { (*account).ls_offset };

        ls_slot.posting.timestamp_ns = 0;
        ls_slot.posting.transfer_sequence_id = [0u8; 16];
        ls_slot.posting.currency = [0u8; 16];
        ls_slot.posting.transfer_posting_records_count = 0;

        unsafe {
            ls_slot.posting.compute_checksum();
        }

        claimed.publish();

    }
}


#[cfg(test)]
mod tests {
    use common::u64_pair_to_bytes::u64_pair_to_bytes;
    use super::*;
    use pipeline::partition_slot::PartitionSlot;

    const PARTITION_ACCOUNTS_HASH_TABLE_K0: u64 = 0xAAAABBBBCCCCDDDD;
    const PARTITION_ACCOUNTS_HASH_TABLE_K1: u64 = 0x1111222233334444;

    const PARTITION_VERSION_TABLE_K0: u64 = 0xAAAABBBBCCCCDDDD;
    const PARTITION_VERSION_TABLE_K1: u64 = 0x1111222233334444;

    static mut TEST_PVT_TAIL: u64 = 0;

    fn account_id(val: u64) -> [u8; 16] {
        u64_pair_to_bytes(0, val)
    }

    fn make_actor_with_coordinator() -> (
        PartitionActor,
        Arc<MpscRingBuffer<PartitionSlot>>,
        Vec<Arc<MpscRingBuffer<CoordinatorSlot>>>,
    ) {
        let rb = Arc::new(MpscRingBuffer::<PartitionSlot>::new(64).unwrap());
        let coordinator_rb = Arc::new(MpscRingBuffer::<CoordinatorSlot>::new(64).unwrap());
        let coordinator_rbs = vec![coordinator_rb.clone()];
        let paht = PartitionAccountsHashTable::new(
            64, PARTITION_ACCOUNTS_HASH_TABLE_K0, PARTITION_ACCOUNTS_HASH_TABLE_K1,
        ).unwrap();
        let ls_writer_rb = Arc::new(MpscRingBuffer::<LsWriterSlot>::new(64).unwrap());
        let ls_writer_rbs = vec![ls_writer_rb.clone()];
        let pvt = PartitionVersionTable::new(64, PARTITION_VERSION_TABLE_K0, PARTITION_VERSION_TABLE_K1).unwrap();
        unsafe { TEST_PVT_TAIL = 0; }
        let pvt_tail = unsafe { &raw mut TEST_PVT_TAIL };

        let actor = PartitionActor::new(
            0,
            Arc::clone(&rb),
            paht,
            pvt,
            coordinator_rbs.clone(),
            pvt_tail,
            ls_writer_rbs,
            64
        );
        (actor, rb, coordinator_rbs)
    }

    fn make_partition_slot(
        account_id: [u8; 16],
        amount: i64,
        entry_type: u8,
        gsn: u64,
    ) -> PartitionSlot {
        let mut slot = PartitionSlot::zeroed();
        slot.account_id = account_id;
        slot.amount = amount;
        slot.entry_type = entry_type;
        slot.msg_type = MSG_TYPE_PREPARE;
        slot.gsn = gsn;
        slot.transfer_id = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        slot.shard_id = 0;
        slot.transfer_hash_table_offset = 0;
        slot
    }

    fn make_commit_slot(account_id: [u8; 16], amount: i64, entry_type: u8, gsn: u64) -> PartitionSlot {
        let mut slot = make_partition_slot(account_id, amount, entry_type, gsn);
        slot.msg_type = MSG_TYPE_COMMIT;
        slot
    }

    fn make_rollback_slot(account_id: [u8; 16], amount: i64, entry_type: u8, gsn: u64) -> PartitionSlot {
        let mut slot = make_partition_slot(account_id, amount, entry_type, gsn);
        slot.msg_type = MSG_TYPE_ROLLBACK;
        slot
    }

    fn make_actor(capacity: usize) -> (PartitionActor, Arc<MpscRingBuffer<PartitionSlot>>) {
        let rb = Arc::new(MpscRingBuffer::<PartitionSlot>::new(64).unwrap());
        let partition_accounts_hash_table = PartitionAccountsHashTable::new(capacity, PARTITION_ACCOUNTS_HASH_TABLE_K0, PARTITION_ACCOUNTS_HASH_TABLE_K1).unwrap();
        let (mut actor, _, coordinator_rbs) = make_actor_with_coordinator();

        (actor, rb)
    }

    #[test]
    fn credit_prepare_increases_staged_income() {
        let rb = Arc::new(MpscRingBuffer::<PartitionSlot>::new(64).unwrap());
        let partition_accounts_hash_table = PartitionAccountsHashTable::new(64, PARTITION_ACCOUNTS_HASH_TABLE_K0, PARTITION_ACCOUNTS_HASH_TABLE_K1).unwrap();
        let (mut actor, _, coordinator_rbs) = make_actor_with_coordinator();

        let slot = make_partition_slot(account_id(1), 500, ENTRY_TYPE_CREDIT, 1);
        actor.handle_prepare(&slot);

        unsafe {
            let account = actor.partition_accounts_hash_table.lookup(0, 1).expect("account should exist");
            assert_eq!((*account).staged_income, 500);
            assert_eq!((*account).staged_outcome, 0);
            assert_eq!((*account).balance, 0);
            assert_eq!((*account).last_gsn, 1);
        }
    }

    #[test]
    fn debit_prepare_rejected_on_zero_balance() {
        let rb = Arc::new(MpscRingBuffer::<PartitionSlot>::new(64).unwrap());
        let partition_accounts_hash_table = PartitionAccountsHashTable::new(64, PARTITION_ACCOUNTS_HASH_TABLE_K0, PARTITION_ACCOUNTS_HASH_TABLE_K1).unwrap();
        let (mut actor, _, coordinator_rbs) = make_actor_with_coordinator();

        let slot = make_partition_slot(account_id(1), 100, ENTRY_TYPE_DEBIT, 1);
        actor.handle_prepare(&slot);

        unsafe {
            let account = actor.partition_accounts_hash_table.lookup(0, 1).expect("account should exist");
            assert_eq!((*account).staged_outcome, 0);
            assert_eq!((*account).balance, 0);
        }
    }

    #[test]
    fn debit_prepare_accepted_with_sufficient_balance() {
        let rb = Arc::new(MpscRingBuffer::<PartitionSlot>::new(64).unwrap());
        let partition_accounts_hash_table = PartitionAccountsHashTable::new(64, PARTITION_ACCOUNTS_HASH_TABLE_K0, PARTITION_ACCOUNTS_HASH_TABLE_K1).unwrap();
        let (mut actor, _, coordinator_rbs) = make_actor_with_coordinator();

        unsafe {
            let account = actor.partition_accounts_hash_table.get_or_create(0, 1);
            (*account).balance = 1000;
        }

        let slot = make_partition_slot(account_id(1), 300, ENTRY_TYPE_DEBIT, 1);
        actor.handle_prepare(&slot);

        unsafe {
            let account = actor.partition_accounts_hash_table.lookup(0, 1).unwrap();
            assert_eq!((*account).staged_outcome, 300);
            assert_eq!((*account).balance, 1000);
            assert_eq!((*account).last_gsn, 1);
        }
    }

    #[test]
    fn multiple_prepares_accumulate_staged() {
        let rb = Arc::new(MpscRingBuffer::<PartitionSlot>::new(64).unwrap());
        let partition_accounts_hash_table = PartitionAccountsHashTable::new(64, PARTITION_ACCOUNTS_HASH_TABLE_K0, PARTITION_ACCOUNTS_HASH_TABLE_K1).unwrap();
        let (mut actor, _, coordinator_rbs) = make_actor_with_coordinator();

        unsafe {
            let account = actor.partition_accounts_hash_table.get_or_create(0, 1);
            (*account).balance = 1000;
        }

        actor.handle_prepare(&make_partition_slot(account_id(1), 300, ENTRY_TYPE_DEBIT, 1));
        actor.handle_prepare(&make_partition_slot(account_id(1), 400, ENTRY_TYPE_DEBIT, 2));
        actor.handle_prepare(&make_partition_slot(account_id(1), 300, ENTRY_TYPE_DEBIT, 3));

        unsafe {
            let account = actor.partition_accounts_hash_table.lookup(0, 1).unwrap();
            assert_eq!((*account).staged_outcome, 1000);
            assert_eq!((*account).last_gsn, 3);
        }
    }

    #[test]
    fn debit_reject_does_not_modify_staged() {
        let rb = Arc::new(MpscRingBuffer::<PartitionSlot>::new(64).unwrap());
        let partition_accounts_hash_table = PartitionAccountsHashTable::new(64, PARTITION_ACCOUNTS_HASH_TABLE_K0, PARTITION_ACCOUNTS_HASH_TABLE_K1).unwrap();
        let (mut actor, _, coordinator_rbs) = make_actor_with_coordinator();

        unsafe {
            let account = actor.partition_accounts_hash_table.get_or_create(0, 1);
            (*account).balance = 100;
        }

        actor.handle_prepare(&make_partition_slot(account_id(1), 50, ENTRY_TYPE_DEBIT, 1));
        actor.handle_prepare(&make_partition_slot(account_id(1), 60, ENTRY_TYPE_DEBIT, 2));

        unsafe {
            let account = actor.partition_accounts_hash_table.lookup(0, 1).unwrap();
            assert_eq!((*account).staged_outcome, 50);
            assert_eq!((*account).last_gsn, 1);
        }
    }

    #[test]
    fn staged_income_increases_effective_balance() {
        let rb = Arc::new(MpscRingBuffer::<PartitionSlot>::new(64).unwrap());
        let partition_accounts_hash_table = PartitionAccountsHashTable::new(64, PARTITION_ACCOUNTS_HASH_TABLE_K0, PARTITION_ACCOUNTS_HASH_TABLE_K1).unwrap();
        let (mut actor, _, coordinator_rbs) = make_actor_with_coordinator();

        actor.handle_prepare(&make_partition_slot(account_id(1), 500, ENTRY_TYPE_CREDIT, 1));

        actor.handle_prepare(&make_partition_slot(account_id(1), 300, ENTRY_TYPE_DEBIT, 2));

        unsafe {
            let account = actor.partition_accounts_hash_table.lookup(0, 1).unwrap();
            assert_eq!((*account).staged_income, 500);
            assert_eq!((*account).staged_outcome, 300);
            assert_eq!((*account).balance, 0);
        }
    }

    #[test]
    fn different_accounts_have_isolated_staged() {
        let rb = Arc::new(MpscRingBuffer::<PartitionSlot>::new(64).unwrap());
        let partition_accounts_hash_table = PartitionAccountsHashTable::new(64, PARTITION_ACCOUNTS_HASH_TABLE_K0, PARTITION_ACCOUNTS_HASH_TABLE_K1).unwrap();
        let (mut actor, _, coordinator_rbs) = make_actor_with_coordinator();

        actor.handle_prepare(&make_partition_slot(account_id(1), 100, ENTRY_TYPE_CREDIT, 1));
        actor.handle_prepare(&make_partition_slot(account_id(2), 200, ENTRY_TYPE_CREDIT, 2));

        unsafe {
            let acc1 = actor.partition_accounts_hash_table.lookup(0, 1).unwrap();
            let acc2 = actor.partition_accounts_hash_table.lookup(0, 2).unwrap();
            assert_eq!((*acc1).staged_income, 100);
            assert_eq!((*acc2).staged_income, 200);
        }
    }

    #[test]
    fn dispatch_routes_prepare() {
        let rb = Arc::new(MpscRingBuffer::<PartitionSlot>::new(64).unwrap());
        let partition_accounts_hash_table = PartitionAccountsHashTable::new(64, PARTITION_ACCOUNTS_HASH_TABLE_K0, PARTITION_ACCOUNTS_HASH_TABLE_K1).unwrap();
        let (mut actor, _, coordinator_rbs) = make_actor_with_coordinator();

        let slot = make_partition_slot(account_id(1), 500, ENTRY_TYPE_CREDIT, 1);
        actor.dispatch(&slot);

        unsafe {
            let account = actor.partition_accounts_hash_table.lookup(0, 1).unwrap();
            assert_eq!((*account).staged_income, 500);
        }
    }

    #[test]
    fn dispatch_commit_does_not_panic() {
        let rb = Arc::new(MpscRingBuffer::<PartitionSlot>::new(64).unwrap());
        let partition_accounts_hash_table = PartitionAccountsHashTable::new(64, PARTITION_ACCOUNTS_HASH_TABLE_K0, PARTITION_ACCOUNTS_HASH_TABLE_K1).unwrap();
        let (mut actor, _, coordinator_rbs) = make_actor_with_coordinator();

        let mut slot = PartitionSlot::zeroed();
        slot.msg_type = MSG_TYPE_COMMIT;
        actor.dispatch(&slot);

        slot.msg_type = MSG_TYPE_ROLLBACK;
        actor.dispatch(&slot);

    }

    #[test]
    fn prepare_success_sends_coordinator_message() {
        let (mut actor, _, coordinator_rbs) = make_actor_with_coordinator();

        unsafe {
            let account = actor.partition_accounts_hash_table.get_or_create(0, 1);
            (*account).balance = 1000;
        }

        let slot = make_partition_slot(account_id(1), 500, ENTRY_TYPE_DEBIT, 1);
        actor.handle_prepare(&slot);

        let batch = coordinator_rbs[0].drain_batch(64);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.slot(0).msg_type, COORD_PREPARE_SUCCESS);
        assert_eq!(batch.slot(0).gsn, 1);
        batch.release();
    }

    #[test]
    fn prepare_fail_sends_coordinator_message() {
        let (mut actor, _, coordinator_rbs) = make_actor_with_coordinator();

        let slot = make_partition_slot(account_id(1), 500, ENTRY_TYPE_DEBIT, 1);
        actor.handle_prepare(&slot);

        let batch = coordinator_rbs[0].drain_batch(64);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.slot(0).msg_type, COORD_PREPARE_FAIL);
        assert_eq!(batch.slot(0).reason, REJECT_INSUFFICIENT_FUNDS);
        assert_eq!(batch.slot(0).gsn, 1);
        batch.release();
    }

    #[test]
    fn commit_debit_applies_balance() {
        let (mut actor, _, coordinator_rbs) = make_actor_with_coordinator();

        unsafe {
            let account = actor.partition_accounts_hash_table.get_or_create(0, 1);
            (*account).balance = 1000;
            (*account).staged_outcome = 300;
        }

        let slot = make_commit_slot(account_id(1), 300, ENTRY_TYPE_DEBIT, 1);
        actor.handle_commit(&slot);

        unsafe {
            let account = actor.partition_accounts_hash_table.lookup(0, 1).unwrap();
            assert_eq!((*account).balance, 700);
            assert_eq!((*account).staged_outcome, 0);
            assert_eq!((*account).ordinal, 1);
            assert_eq!((*account).last_gsn, 1);
        }

        let batch = coordinator_rbs[0].drain_batch(64);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.slot(0).msg_type, COORD_COMMIT_SUCCESS);
        batch.release();
    }

    #[test]
    fn commit_credit_applies_balance() {
        let (mut actor, _, coordinator_rbs) = make_actor_with_coordinator();

        unsafe {
            let account = actor.partition_accounts_hash_table.get_or_create(0, 1);
            (*account).balance = 500;
            (*account).staged_income = 200;
        }

        let slot = make_commit_slot(account_id(1), 200, ENTRY_TYPE_CREDIT, 1);
        actor.handle_commit(&slot);

        unsafe {
            let account = actor.partition_accounts_hash_table.lookup(0, 1).unwrap();
            assert_eq!((*account).balance, 700);
            assert_eq!((*account).staged_income, 0);
            assert_eq!((*account).ordinal, 1);
        }

        let batch = coordinator_rbs[0].drain_batch(64);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.slot(0).msg_type, COORD_COMMIT_SUCCESS);
        batch.release();
    }

    #[test]
    fn rollback_debit_undoes_staged() {
        let (mut actor, _, coordinator_rbs) = make_actor_with_coordinator();

        unsafe {
            let account = actor.partition_accounts_hash_table.get_or_create(0, 1);
            (*account).balance = 1000;
            (*account).staged_outcome = 300;
        }

        let slot = make_rollback_slot(account_id(1), 300, ENTRY_TYPE_DEBIT, 1);
        actor.handle_rollback(&slot);

        unsafe {
            let account = actor.partition_accounts_hash_table.lookup(0, 1).unwrap();
            assert_eq!((*account).balance, 1000);
            assert_eq!((*account).staged_outcome, 0);
            assert_eq!((*account).ordinal, 0);
        }

        let batch = coordinator_rbs[0].drain_batch(64);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.slot(0).msg_type, COORD_ROLLBACK_SUCCESS);
        batch.release();
    }

    #[test]
    fn rollback_credit_undoes_staged() {
        let (mut actor, _, coordinator_rbs) = make_actor_with_coordinator();

        unsafe {
            let account = actor.partition_accounts_hash_table.get_or_create(0, 1);
            (*account).staged_income = 500;
        }

        let slot = make_rollback_slot(account_id(1), 500, ENTRY_TYPE_CREDIT, 1);
        actor.handle_rollback(&slot);

        unsafe {
            let account = actor.partition_accounts_hash_table.lookup(0, 1).unwrap();
            assert_eq!((*account).balance, 0);
            assert_eq!((*account).staged_income, 0);
            assert_eq!((*account).ordinal, 0);
        }

        let batch = coordinator_rbs[0].drain_batch(64);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.slot(0).msg_type, COORD_ROLLBACK_SUCCESS);
        batch.release();
    }

    #[test]
    fn full_cycle_prepare_commit() {
        let (mut actor, _, coordinator_rbs) = make_actor_with_coordinator();

        unsafe {
            let account = actor.partition_accounts_hash_table.get_or_create(0, 1);
            (*account).balance = 1000;
        }

        actor.handle_prepare(&make_partition_slot(account_id(1), 300, ENTRY_TYPE_DEBIT, 1));

        unsafe {
            let account = actor.partition_accounts_hash_table.lookup(0, 1).unwrap();
            assert_eq!((*account).staged_outcome, 300);
            assert_eq!((*account).balance, 1000);
        }

        let batch = coordinator_rbs[0].drain_batch(64);
        assert_eq!(batch.slot(0).msg_type, COORD_PREPARE_SUCCESS);
        batch.release();

        actor.handle_commit(&make_commit_slot(account_id(1), 300, ENTRY_TYPE_DEBIT, 1));

        unsafe {
            let account = actor.partition_accounts_hash_table.lookup(0, 1).unwrap();
            assert_eq!((*account).balance, 700);
            assert_eq!((*account).staged_outcome, 0);
            assert_eq!((*account).ordinal, 1);
        }

        let batch = coordinator_rbs[0].drain_batch(64);
        assert_eq!(batch.slot(0).msg_type, COORD_COMMIT_SUCCESS);
        batch.release();
    }

    #[test]
    fn full_cycle_prepare_rollback() {
        let (mut actor, _, coordinator_rbs) = make_actor_with_coordinator();

        unsafe {
            let account = actor.partition_accounts_hash_table.get_or_create(0, 1);
            (*account).balance = 1000;
        }

        actor.handle_prepare(&make_partition_slot(account_id(1), 300, ENTRY_TYPE_DEBIT, 1));

        coordinator_rbs[0].drain_batch(64).release();

        actor.handle_rollback(&make_rollback_slot(account_id(1), 300, ENTRY_TYPE_DEBIT, 1));

        unsafe {
            let account = actor.partition_accounts_hash_table.lookup(0, 1).unwrap();
            assert_eq!((*account).balance, 1000);
            assert_eq!((*account).staged_outcome, 0);
            assert_eq!((*account).ordinal, 0);
        }
    }

    #[test]
    fn commit_records_version_in_pvt() {
        let (mut actor, _, _coordinator_rbs) = make_actor_with_coordinator();

        unsafe {
            let account = actor.partition_accounts_hash_table.get_or_create(0, 1);
            (*account).balance = 1000;
            (*account).staged_outcome = 300;
        }

        let slot = make_commit_slot(account_id(1), 300, ENTRY_TYPE_DEBIT, 42);
        actor.handle_commit(&slot);

        unsafe {
            let balance = actor.partition_version_table.read_balance(0, 1, 42);
            assert_eq!(balance, Some(700));
        }
    }
}