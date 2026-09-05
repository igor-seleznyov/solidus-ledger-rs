use crate::transfer_hash_table::TransferHashTable;
use common::mem_barrier::acquire_load_u8;
use common::radix_sort::radix_sort_by_id_lo;
use common::u64_pair_to_bytes::u64_pair_to_bytes;
use pipeline::coordinator_slot::*;
use pipeline::partition_slot::*;
use pipeline::transfer_slot::*;
use ringbuf::mpsc_ring_buffer::MpscRingBuffer;
use ringbuf::slot::Slot;
use storage::flush_done_slot::FlushDoneSlot;
use storage::ls_writer_slot::*;

pub struct DecisionMaker<'scope> {
    id: usize,
    coordinator_rb: &'scope MpscRingBuffer<CoordinatorSlot>,
    transfer_hash_table: &'scope TransferHashTable,
    partition_rbs: &'scope [MpscRingBuffer<PartitionSlot>],
    ls_writer_rb: &'scope MpscRingBuffer<LsWriterSlot>,
    flush_done_rb: &'scope MpscRingBuffer<FlushDoneSlot>,
    batch_size: usize,
    keys: Vec<u64>,
    indices: Vec<u16>,
    temp: Vec<u16>,
}

impl<'scope> DecisionMaker<'scope> {
    pub fn new(
        id: usize,
        coordinator_rb: &'scope MpscRingBuffer<CoordinatorSlot>,
        transfer_hash_table: &'scope TransferHashTable,
        partition_rbs: &'scope [MpscRingBuffer<PartitionSlot>],
        ls_writer_rb: &'scope MpscRingBuffer<LsWriterSlot>,
        flush_done_rb: &'scope MpscRingBuffer<FlushDoneSlot>,
        batch_size: usize,
    ) -> Self {
        Self {
            id,
            coordinator_rb,
            transfer_hash_table,
            partition_rbs,
            ls_writer_rb,
            flush_done_rb,
            batch_size,
            keys: vec![0u64; batch_size],
            indices: vec![0u16; batch_size],
            temp: vec![0u16; batch_size],
        }
    }

    pub fn run(&mut self) {
        println!("[decision-maker {}] started", self.id);

        loop {
            self.drain_flush_done();

            let batch = self.coordinator_rb.drain_batch(self.batch_size);

            if batch.is_empty() {
                std::hint::spin_loop();
                continue;
            }

            let count = batch.len();

            for i in 0..count {
                self.keys[i] = batch.slot(i).transfer_id_lo;
            }

            radix_sort_by_id_lo(
                &self.keys[..count],
                &mut self.indices[..count],
                &mut self.temp[..count],
                count,
            );

            self.process_sorted_batch(&batch, count);

            batch.release();
        }
    }

    fn drain_flush_done(&self) {
        loop {
            let read = self.flush_done_rb.try_read();
            match read {
                Some(slot) => {
                    let transfer_hash_table_offset = slot.as_ref().transfer_hash_table_offset;
                    let transfer_id_hi = slot.as_ref().transfer_id_hi;
                    let transfer_id_lo = slot.as_ref().transfer_id_lo;

                    slot.release();

                    unsafe {
                        self.transfer_hash_table.remove(transfer_hash_table_offset);
                    }

                    println!(
                        "[decision-maker {}] PERSISTED transfer=({},{})",
                        self.id, transfer_id_hi, transfer_id_lo,
                    );

                }
                None => break,
            }
        }
    }

    fn process_sorted_batch(
        &self,
        batch: &ringbuf::batch::DrainBatch<CoordinatorSlot>,
        count: usize,
    ) {
        let mut group_start = 0;

        while group_start < count {
            let first = batch.slot(self.indices[group_start] as usize);
            let current_hi = first.transfer_id_hi;
            let current_lo = first.transfer_id_lo;

            let mut group_end = group_start + 1;
            while group_end < count {
                let msg = batch.slot(self.indices[group_end] as usize);
                if msg.transfer_id_hi != current_hi || msg.transfer_id_lo != current_lo {
                    break;
                }
                group_end += 1;
            }

            self.process_group(batch, group_start, group_end);

            group_start = group_end;
        }
    }

    pub fn process_batch(
        &mut self,
        batch: &ringbuf::batch::DrainBatch<CoordinatorSlot>,
        count: usize,
    ) {
        for i in 0..count {
            self.keys[i] = batch.slot(i).transfer_id_lo;
        }
        radix_sort_by_id_lo(
            &self.keys[..count],
            &mut self.indices[..count],
            &mut self.temp[..count],
            count,
        );
        self.process_sorted_batch(batch, count);
    }

    fn process_group(
        &self,
        batch: &ringbuf::batch::DrainBatch<CoordinatorSlot>,
        group_start: usize,
        group_end: usize,
    ) {
        let first = batch.slot(self.indices[group_start] as usize);
        let transfer_hash_table_offset = first.transfer_hash_table_offset;

        unsafe {
            let slot = self.transfer_hash_table.slot_ptr(transfer_hash_table_offset);

            if acquire_load_u8(&(*slot).ready as *const u8) == 0 {
                return;
            }

            for i in group_start..group_end {
                let msg = batch.slot(self.indices[i] as usize);

                match msg.msg_type {
                    COORD_PREPARE_SUCCESS => {
                        (*slot).confirmed_count += 1;
                        (*slot).prepare_success_bitmap |= 1u8 << msg.entry_index;
                    }
                    COORD_PREPARE_FAIL => {
                        (*slot).failed_count += 1;
                        (*slot).fail_reason = msg.reason;
                    }
                    COORD_COMMIT_SUCCESS => {
                        (*slot).commit_success_count += 1;
                    }
                    COORD_ROLLBACK_SUCCESS => {
                        (*slot).rollback_success_count += 1;
                    }
                    _ => {}
                }
            }

            self.check_and_decide(slot, transfer_hash_table_offset);
        }
    }

    unsafe fn check_and_decide(&self, slot: *mut TransferSlot, transfer_hash_table_offset: u32) {
        let entries_count = unsafe { (*slot).entries_count };
        let confirmed = unsafe { (*slot).confirmed_count };
        let failed = unsafe { (*slot).failed_count };
        let commit_success = unsafe { (*slot).commit_success_count };
        let rollback_success = unsafe { (*slot).rollback_success_count };
        let decision = unsafe { (*slot).decision };

        if confirmed + failed == entries_count && decision == DECISION_NONE {
            if failed == 0 {
                unsafe {
                    (*slot).decision = DECISION_COMMIT;
                    self.send_decision_to_partition(slot, transfer_hash_table_offset, MSG_TYPE_COMMIT);
                }
            } else {
                unsafe {
                    (*slot).decision = DECISION_ROLLBACK;
                    self.send_decision_to_partition(slot, transfer_hash_table_offset, MSG_TYPE_ROLLBACK);
                }
            }
            return;
        }

        if commit_success == entries_count && decision == DECISION_COMMIT {
            unsafe {
                (*slot).decision = DECISION_PENDING_FLUSH;
                self.send_flush_marker(slot, transfer_hash_table_offset);
            }
            return;
        }

        let expected_rollback_count = unsafe { (*slot).prepare_success_bitmap.count_ones() as u8 };
        if rollback_success == expected_rollback_count && decision == DECISION_ROLLBACK {
            let gsn = unsafe { (*slot).gsn };
            let fail_reason = unsafe { (*slot).fail_reason };
            self.send_remove_from_heap(gsn);
            unsafe {
                self.transfer_hash_table.remove(transfer_hash_table_offset);
            }
            println!(
                "[decision-maker {}] ROLLED BACK gsn={}, reason={}",
                self.id, gsn, fail_reason,
            );
            return;
        }
    }

    fn send_remove_from_heap(&self, gsn: u64) {
        let mut claimed = self.ls_writer_rb.claim();
        let ls_slot = claimed.as_mut();

        ls_slot.msg_type = LS_MSG_REMOVE_FROM_HEAP;
        ls_slot.gsn = gsn;

        claimed.publish();
    }

    unsafe fn send_flush_marker(
        &self,
        slot: *mut TransferSlot,
        transfer_hash_table_offset: u32,
    ) {
        let mut claimed = self.ls_writer_rb.claim();
        let ls_slot = claimed.as_mut();

        ls_slot.msg_type = LS_MSG_FLUSH_MARKER;
        ls_slot.gsn = unsafe { (*slot).gsn };
        ls_slot.transfer_id_hi = unsafe { (*slot).transfer_id_hi };
        ls_slot.transfer_id_lo = unsafe { (*slot).transfer_id_lo };
        ls_slot.transfer_hash_table_offset = transfer_hash_table_offset;

        claimed.publish();
    }

    unsafe fn send_decision_to_partition(
        &self,
        slot: *mut TransferSlot,
        transfer_hash_table_offset: u32,
        msg_type: u8,
    ) {
        let entries_count = unsafe { (*slot).entries_count as usize };

        for i in 0..entries_count {
            if msg_type == MSG_TYPE_ROLLBACK {
                let slot_prepare_success_bitmap = unsafe { (*slot).prepare_success_bitmap };
                if slot_prepare_success_bitmap & (1u8 << i) == 0 {
                    continue;
                }
            }

            let entry = unsafe { self.transfer_hash_table.get_entry(transfer_hash_table_offset, i) };
            let partition_id = entry.partition_id as usize;

            let mut claimed = self.partition_rbs[partition_id].claim();
            let partition_slot = claimed.as_mut();

            partition_slot.gsn = unsafe { (*slot).gsn };

            partition_slot.transfer_id = unsafe { u64_pair_to_bytes((*slot).transfer_id_hi, (*slot).transfer_id_lo) };
            partition_slot.account_id = u64_pair_to_bytes(entry.account_id_hi, entry.account_id_lo);

            partition_slot.amount = entry.amount;
            partition_slot.entry_type = entry.entry_type;
            partition_slot.msg_type = msg_type;
            partition_slot.shard_id = self.id as u8;
            partition_slot.transfer_hash_table_offset = transfer_hash_table_offset;
            partition_slot.entry_index = i as u8;

            claimed.publish();
        }
    }
}

#[cfg(test)]
#[cfg(not(miri))]
mod tests {
    use super::*;
    use pipeline::transfer_hash_table_entry::TransferHashTableEntry;

    const TRANSFER_HASH_TABLE_SEED_K0: u64 = 0x0123456789ABCDEF;
    const TRANSFER_HASH_TABLE_SEED_K1: u64 = 0xFEDCBA9876543210;

    fn account_id(val: u64) -> [u8; 16] {
        u64_pair_to_bytes(0, val)
    }

    fn currency() -> [u8; 16] {
        let mut currency = [0u8; 16];
        currency[..3].copy_from_slice(b"EUR");
        currency
    }

    unsafe fn make_entry(acc_lo: u64, amount: i64, partition: u32, etype: u8) -> TransferHashTableEntry {
        TransferHashTableEntry {
            account_id_hi: 0,
            account_id_lo: acc_lo,
            amount,
            partition_id: partition,
            entry_type: etype,
            _pad: [0; 3],
        }
    }

    /// Owns everything a Decision Maker borrows, for as long as the test
    /// body runs.
    ///
    /// The Decision Maker holds its rings and the transfer table as shared
    /// references now, so something has to own them and outlive it. In
    /// production that owner is `main`'s thread scope; here it is a local
    /// whose lifetime is the test body. A helper that built them and
    /// returned the Decision Maker cannot work at all — they would die at
    /// the helper's closing brace while it still pointed at them.
    struct DecisionMakerRingBuffersHolder {
        coordinator_rb: MpscRingBuffer<CoordinatorSlot>,
        transfer_hash_table: TransferHashTable,
        partition_rbs: Vec<MpscRingBuffer<PartitionSlot>>,
        ls_writer_rb: MpscRingBuffer<LsWriterSlot>,
        flush_done_rb: MpscRingBuffer<FlushDoneSlot>,
        /// Offset of the one transfer staged into the table by `with_transfer`.
        tht_offset: u32,
    }

    impl DecisionMakerRingBuffersHolder {
        /// Builds the rings with one transfer already inserted and published
        /// into the table, which is the starting state every 2PC test needs.
        fn with_transfer(num_partitions: usize) -> Self {
            let transfer_hash_table = TransferHashTable::new(64, TRANSFER_HASH_TABLE_SEED_K0, TRANSFER_HASH_TABLE_SEED_K1, 8).unwrap();

            let tht_offset = unsafe {
                let transfer_hash_table_offset = transfer_hash_table.insert(
                    0, 1, 100, 7,
                    &[0u8; 16],
                    &currency(),
                    2, 0,
                    &[0u8; 16],
                );
                transfer_hash_table.fill_entry(transfer_hash_table_offset, 0, &make_entry(10, 500, 0, ENTRY_TYPE_DEBIT));
                transfer_hash_table.fill_entry(transfer_hash_table_offset, 1, &make_entry(20, 500, 1, ENTRY_TYPE_CREDIT));
                transfer_hash_table.publish(transfer_hash_table_offset);
                transfer_hash_table_offset
            };

            Self {
                coordinator_rb: MpscRingBuffer::<CoordinatorSlot>::new(64).unwrap(),
                transfer_hash_table,
                partition_rbs: (0..num_partitions)
                    .map(|_| MpscRingBuffer::new(64).unwrap())
                    .collect(),
                ls_writer_rb: MpscRingBuffer::<LsWriterSlot>::new(64).unwrap(),
                flush_done_rb: MpscRingBuffer::<FlushDoneSlot>::new(64).unwrap(),
                tht_offset,
            }
        }

        fn decision_maker(&self) -> DecisionMaker<'_> {
            DecisionMaker::new(
                0,
                &self.coordinator_rb,
                &self.transfer_hash_table,
                &self.partition_rbs,
                &self.ls_writer_rb,
                &self.flush_done_rb,
                64,
            )
        }
    }

    fn make_coord_msg(
        msg_type: u8,
        tht_offset: u32,
        reason: u8,
        entry_index: u8,
    ) -> CoordinatorSlot {
        let mut slot = CoordinatorSlot::zeroed();
        slot.msg_type = msg_type;
        slot.transfer_hash_table_offset = tht_offset;
        slot.transfer_id_hi = 0;
        slot.transfer_id_lo = 1;
        slot.gsn = 100;
        slot.shard_id = 0;
        slot.reason = reason;
        slot.entry_index = entry_index;
        slot
    }


    #[test]
    fn all_prepare_success_sends_commit() {
        let rings = DecisionMakerRingBuffersHolder::with_transfer(4);
        let mut dm = rings.decision_maker();
        let (tht, coordinator_rb, partition_rbs, ls_writer_rb, flush_done_rb, tht_offset) = (
            &rings.transfer_hash_table,
            &rings.coordinator_rb,
            &rings.partition_rbs,
            &rings.ls_writer_rb,
            &rings.flush_done_rb,
            rings.tht_offset,
        );

        let msg1 = make_coord_msg(COORD_PREPARE_SUCCESS, tht_offset, 0, 0);
        let msg2 = make_coord_msg(COORD_PREPARE_SUCCESS, tht_offset, 0, 0);

        let mut first_claim = coordinator_rb.claim();
        *first_claim.as_mut() = msg1;
        first_claim.publish();
        let mut second_claim = coordinator_rb.claim();
        *second_claim.as_mut() = msg2;
        second_claim.publish();

        let batch = dm.coordinator_rb.drain_batch(64);
        let count = batch.len();
        assert_eq!(count, 2);

        dm.process_batch(&batch, count);

        batch.release();

        unsafe {
            let slot = tht.slot_ptr(tht_offset);
            assert_eq!((*slot).decision, DECISION_COMMIT);
        }

        let mut total_commits = 0;
        for rb in partition_rbs {
            let b = rb.drain_batch(64);
            for i in 0..b.len() {
                assert_eq!(b.slot(i).msg_type, MSG_TYPE_COMMIT);
                total_commits += 1;
            }
            b.release();
        }
        assert_eq!(total_commits, 2);
    }

    #[test]
    fn prepare_fail_sends_rollback() {
        let rings = DecisionMakerRingBuffersHolder::with_transfer(4);
        let mut dm = rings.decision_maker();
        let (tht, coordinator_rb, partition_rbs, ls_writer_rb, flush_done_rb, tht_offset) = (
            &rings.transfer_hash_table,
            &rings.coordinator_rb,
            &rings.partition_rbs,
            &rings.ls_writer_rb,
            &rings.flush_done_rb,
            rings.tht_offset,
        );

        let mut success_vote_claim = coordinator_rb.claim();
        *success_vote_claim.as_mut() = make_coord_msg(COORD_PREPARE_SUCCESS, tht_offset, 0, 0);
        success_vote_claim.publish();
        let mut fail_vote_claim = coordinator_rb.claim();
        *fail_vote_claim.as_mut() = make_coord_msg(COORD_PREPARE_FAIL, tht_offset, 10, 1);
        fail_vote_claim.publish();

        let batch = dm.coordinator_rb.drain_batch(64);
        let count = batch.len();
        dm.process_batch(&batch, count);
        batch.release();

        unsafe {
            let slot = tht.slot_ptr(tht_offset);
            assert_eq!((*slot).decision, DECISION_ROLLBACK);
            assert_eq!((*slot).fail_reason, 10);
        }

        let mut total_rollbacks = 0;
        for rb in partition_rbs {
            let b = rb.drain_batch(64);
            for i in 0..b.len() {
                assert_eq!(b.slot(i).msg_type, MSG_TYPE_ROLLBACK);
                total_rollbacks += 1;
            }
            b.release();
        }
        assert_eq!(total_rollbacks, 1);
    }

    #[test]
    fn all_commit_ok_removes_from_tht() {
        let rings = DecisionMakerRingBuffersHolder::with_transfer(4);
        let mut dm = rings.decision_maker();
        let (tht, coordinator_rb, partition_rbs, ls_writer_rb, flush_done_rb, tht_offset) = (
            &rings.transfer_hash_table,
            &rings.coordinator_rb,
            &rings.partition_rbs,
            &rings.ls_writer_rb,
            &rings.flush_done_rb,
            rings.tht_offset,
        );

        for _ in 0..2 {
            let mut claim = coordinator_rb.claim();
            *claim.as_mut() = make_coord_msg(COORD_PREPARE_SUCCESS, tht_offset, 0, 0);
            claim.publish();
        }
        let batch = dm.coordinator_rb.drain_batch(64);
        let count = batch.len();
        dm.process_batch(&batch, count);
        batch.release();

        for rb in partition_rbs {
            let b = rb.drain_batch(64);
            b.release();
        }

        for _ in 0..2 {
            let mut claim = coordinator_rb.claim();
            *claim.as_mut() = make_coord_msg(COORD_COMMIT_SUCCESS, tht_offset, 0, 0);
            claim.publish();
        }
        let batch = dm.coordinator_rb.drain_batch(64);
        let count = batch.len();
        dm.process_batch(&batch, count);
        batch.release();

        unsafe {
            let slot = tht.slot_ptr(tht_offset);
            assert_eq!((*slot).decision, DECISION_PENDING_FLUSH);
        }
        assert_eq!(tht.count(), 1);
    }

    #[test]
    fn all_rollback_ok_removes_from_tht() {
        let rings = DecisionMakerRingBuffersHolder::with_transfer(4);
        let mut dm = rings.decision_maker();
        let (tht, coordinator_rb, partition_rbs, ls_writer_rb, flush_done_rb, tht_offset) = (
            &rings.transfer_hash_table,
            &rings.coordinator_rb,
            &rings.partition_rbs,
            &rings.ls_writer_rb,
            &rings.flush_done_rb,
            rings.tht_offset,
        );

        let mut success_vote_claim = coordinator_rb.claim();
        *success_vote_claim.as_mut() = make_coord_msg(COORD_PREPARE_SUCCESS, tht_offset, 0, 0);
        success_vote_claim.publish();
        let mut fail_vote_claim = coordinator_rb.claim();
        *fail_vote_claim.as_mut() = make_coord_msg(COORD_PREPARE_FAIL, tht_offset, 10, 1);
        fail_vote_claim.publish();

        let batch = dm.coordinator_rb.drain_batch(64);
        let count = batch.len();
        dm.process_batch(&batch, count);
        batch.release();

        for rb in partition_rbs {
            rb.drain_batch(64).release();
        }

        let mut claim = coordinator_rb.claim();
        *claim.as_mut() = make_coord_msg(COORD_ROLLBACK_SUCCESS, tht_offset, 0, 0);
        claim.publish();

        let batch = dm.coordinator_rb.drain_batch(64);
        let count = batch.len();
        dm.process_batch(&batch, count);
        batch.release();

        assert_eq!(tht.count(), 0);
    }

    #[test]
    fn commit_sends_correct_entry_data() {
        let rings = DecisionMakerRingBuffersHolder::with_transfer(4);
        let mut dm = rings.decision_maker();
        let (tht, coordinator_rb, partition_rbs, ls_writer_rb, flush_done_rb, tht_offset) = (
            &rings.transfer_hash_table,
            &rings.coordinator_rb,
            &rings.partition_rbs,
            &rings.ls_writer_rb,
            &rings.flush_done_rb,
            rings.tht_offset,
        );

        for _ in 0..2 {
            let mut claim = coordinator_rb.claim();
            *claim.as_mut() = make_coord_msg(COORD_PREPARE_SUCCESS, tht_offset, 0, 0);
            claim.publish();
        }
        let batch = dm.coordinator_rb.drain_batch(64);
        let count = batch.len();
        dm.process_batch(&batch, count);
        batch.release();

        let mut commits: Vec<PartitionSlot> = Vec::new();
        for rb in partition_rbs {
            let b = rb.drain_batch(64);
            for i in 0..b.len() {
                commits.push(*b.slot(i));
            }
            b.release();
        }

        assert_eq!(commits.len(), 2);

        let debit = commits.iter().find(|command| command.entry_type == ENTRY_TYPE_DEBIT).unwrap();
        assert_eq!(debit.amount, 500);
        assert_eq!(debit.gsn, 100);
        assert_eq!(debit.msg_type, MSG_TYPE_COMMIT);
        assert_eq!(debit.transfer_hash_table_offset, tht_offset);
        assert_eq!(debit.account_id, account_id(10));

        let credit = commits.iter().find(|command| command.entry_type == ENTRY_TYPE_CREDIT).unwrap();
        assert_eq!(credit.amount, 500);
        assert_eq!(credit.account_id, account_id(20));
    }

    #[test]
    fn partial_prepare_no_decision_yet() {
        let rings = DecisionMakerRingBuffersHolder::with_transfer(4);
        let mut dm = rings.decision_maker();
        let (tht, coordinator_rb, partition_rbs, ls_writer_rb, flush_done_rb, tht_offset) = (
            &rings.transfer_hash_table,
            &rings.coordinator_rb,
            &rings.partition_rbs,
            &rings.ls_writer_rb,
            &rings.flush_done_rb,
            rings.tht_offset,
        );

        let mut claim = coordinator_rb.claim();
        *claim.as_mut() = make_coord_msg(COORD_PREPARE_SUCCESS, tht_offset, 0, 0);
        claim.publish();

        let batch = dm.coordinator_rb.drain_batch(64);
        let count = batch.len();
        dm.process_batch(&batch, count);
        batch.release();

        unsafe {
            let slot = tht.slot_ptr(tht_offset);
            assert_eq!((*slot).decision, DECISION_NONE);
            assert_eq!((*slot).confirmed_count, 1);
        }
    }

    #[test]
    fn all_commit_ok_sends_flush_marker_to_ls_writer() {
        let rings = DecisionMakerRingBuffersHolder::with_transfer(4);
        let mut dm = rings.decision_maker();
        let (tht, coordinator_rb, partition_rbs, ls_writer_rb, flush_done_rb, tht_offset) = (
            &rings.transfer_hash_table,
            &rings.coordinator_rb,
            &rings.partition_rbs,
            &rings.ls_writer_rb,
            &rings.flush_done_rb,
            rings.tht_offset,
        );

        for _ in 0..2 {
            let mut claim = coordinator_rb.claim();
            *claim.as_mut() = make_coord_msg(COORD_PREPARE_SUCCESS, tht_offset, 0, 0);
            claim.publish();
        }
        let batch = dm.coordinator_rb.drain_batch(64);
        dm.process_batch(&batch, batch.len());
        batch.release();

        for rb in partition_rbs {
            rb.drain_batch(64).release();
        }

        for _ in 0..2 {
            let mut claim = coordinator_rb.claim();
            *claim.as_mut() = make_coord_msg(COORD_COMMIT_SUCCESS, tht_offset, 0, 0);
            claim.publish();
        }
        let batch = dm.coordinator_rb.drain_batch(64);
        dm.process_batch(&batch, batch.len());
        batch.release();

        let ls_batch = ls_writer_rb.drain_batch(64);
        let mut flush_markers = 0;
        for i in 0..ls_batch.len() {
            if ls_batch.slot(i).msg_type == LS_MSG_FLUSH_MARKER {
                flush_markers += 1;
            }
        }
        ls_batch.release();

        assert_eq!(flush_markers, 1);

        assert_eq!(tht.count(), 1);
    }

    #[test]
    fn flush_done_removes_from_tht() {
        let rings = DecisionMakerRingBuffersHolder::with_transfer(4);
        let mut dm = rings.decision_maker();
        let (tht, coordinator_rb, partition_rbs, ls_writer_rb, flush_done_rb, tht_offset) = (
            &rings.transfer_hash_table,
            &rings.coordinator_rb,
            &rings.partition_rbs,
            &rings.ls_writer_rb,
            &rings.flush_done_rb,
            rings.tht_offset,
        );

        for _ in 0..2 {
            let mut claim = coordinator_rb.claim();
            *claim.as_mut() = make_coord_msg(COORD_PREPARE_SUCCESS, tht_offset, 0, 0);
            claim.publish();
        }
        let batch = dm.coordinator_rb.drain_batch(64);
        dm.process_batch(&batch, batch.len());
        batch.release();

        for rb in partition_rbs {
            rb.drain_batch(64).release();
        }

        for _ in 0..2 {
            let mut claim = coordinator_rb.claim();
            *claim.as_mut() = make_coord_msg(COORD_COMMIT_SUCCESS, tht_offset, 0, 0);
            claim.publish();
        }
        let batch = dm.coordinator_rb.drain_batch(64);
        dm.process_batch(&batch, batch.len());
        batch.release();

        assert_eq!(tht.count(), 1);

        {
            let mut claimed = flush_done_rb.claim();
            let slot = claimed.as_mut();
            slot.transfer_id_hi = 0;
            slot.transfer_id_lo = 1;
            slot.transfer_hash_table_offset = tht_offset;
            claimed.publish();
        }

        dm.drain_flush_done();

        assert_eq!(tht.count(), 0);
    }
}
