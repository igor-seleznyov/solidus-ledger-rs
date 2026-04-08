use pipeline::posting_record::PostingRecord;
use crate::signing_state::SigningState;
use crate::sig_record::SigRecord;
use crate::ls_sign_file_header::LsSignFileHeader;
use crate::consts::{FILE_PAGE_SIZE, STORAGE_FILE_TYPE_SIGNATURE};
use crate::signing_strategy::SigningStrategy;

pub struct Ed25519SigningStrategy {
    signing_state: SigningState,
    #[allow(dead_code)]
    sign_buffer_arena: ringbuf::arena::Arena,
    sign_buffer_ptr: *mut u8,
    sign_buffer_len: usize,
    sign_buffer_capacity: usize,
    sign_write_offset: u64,
    pending_padded_len: usize,
}

impl Ed25519SigningStrategy {
    pub fn new(
        signing_state: SigningState,
        flush_max_buffer_posting_records: usize,
    ) -> Self {
        let raw = flush_max_buffer_posting_records * SigRecord::SIZE + FILE_PAGE_SIZE;
        let capacity = (raw + 4095) & !4095;
        let arena = ringbuf::arena::Arena::new(capacity)
            .expect("failed to create signature buffer Arena");
        let ptr = arena.as_ptr();

        Self {
            signing_state,
            sign_buffer_arena: arena,
            sign_buffer_ptr: ptr,
            sign_buffer_len: 0,
            sign_buffer_capacity: capacity,
            sign_write_offset: 0,
            pending_padded_len: 0,
        }
    }

    pub fn public_key_bytes(&self) -> [u8; 32] {
        self.signing_state.public_key_bytes()
    }
}

unsafe impl Send for Ed25519SigningStrategy {}

impl SigningStrategy for Ed25519SigningStrategy {
    fn file_info(&self, file_path: &str) -> Option<(String, usize)> {
        let sign_path = format!("{}.sign", file_path);
        let prealloc = self.sign_buffer_capacity * 4;
        Some((sign_path, prealloc))
    }

    fn prepare_header(&mut self, file_seq: u64) -> Option<(*const u8, usize, u64)> {
        let sign_header = LsSignFileHeader::new(
            SigningState::ALGORITHM_ED25519,
            0,
            file_seq,
            self.signing_state.public_key_bytes(),
            *self.signing_state.last_tx_hash(),
        );

        let header_page = sign_header.to_page();

        unsafe {
            std::ptr::copy_nonoverlapping(
                header_page.as_ptr(),
                self.sign_buffer_ptr,
                FILE_PAGE_SIZE,
            );
        }

        Some(
            (self.sign_buffer_ptr as *const u8, FILE_PAGE_SIZE, 0)
        )
    }

    fn on_header_written(&mut self) {
        self.sign_write_offset = LsSignFileHeader::DATA_OFFSET as u64;
        self.sign_buffer_len = 0;
    }

    fn prepare_flush(
        &mut self,
        buffer_ptr: *mut u8,
        buffer_len: usize,
        write_offset: u64,
    ) -> Option<(*const u8, usize, u64)> {
        let record_count = buffer_len / PostingRecord::SIZE;
        if record_count == 0 {
            return None;
        }

        let records = unsafe {
            std::slice::from_raw_parts(
                buffer_ptr as *const PostingRecord,
                record_count,
            )
        };

        for i in 0..record_count {
            let record = &records[i];
            let ls_offset = write_offset + (i * PostingRecord::SIZE) as u64;

            let sig_record = self.signing_state.sign_posting(
                record.transfer_id_hi,
                record.transfer_id_lo,
                record.gsn,
                ls_offset,
                record,
            );

            let sig_bytes = unsafe {
                std::slice::from_raw_parts(
                    &sig_record as *const SigRecord as *const u8,
                    SigRecord::SIZE,
                )
            };

            unsafe {
                std::ptr::copy_nonoverlapping(
                    sig_bytes.as_ptr(),
                    self.sign_buffer_ptr.add(self.sign_buffer_len),
                    SigRecord::SIZE,
                );
            }
            self.sign_buffer_len += SigRecord::SIZE;
        }

        let padded_len = (self.sign_buffer_len + 4095) & !4095;
        if padded_len > self.sign_buffer_len {
            unsafe {
                std::ptr::write_bytes(
                    self.sign_buffer_ptr.add(self.sign_buffer_len),
                    0,
                    padded_len - self.sign_buffer_len,
                );
            }
        }

        self.pending_padded_len = padded_len;

        Some(
            (self.sign_buffer_ptr as *const u8, padded_len, self.sign_write_offset)
        )
    }

    fn on_flush_complete(&mut self) {
        self.sign_write_offset += self.pending_padded_len as u64;
        self.sign_buffer_len = 0;
        self.pending_padded_len = 0;
    }

    fn on_rotation(&mut self) {
        self.sign_write_offset = 0;
        self.sign_buffer_len = 0;
        self.pending_padded_len = 0;
    }

    fn is_enabled(&self) -> bool {
        true
    }

    fn public_key_hash(&self) -> u32 {
        let public_key = self.signing_state.public_key_bytes();
        unsafe {
            common::crc32c::crc32c(public_key.as_ptr(), public_key.len())
        }
    }

    fn chain_hash(&self) -> Option<&[u8; 32]> {
        Some(
            self.signing_state.last_tx_hash()
        )
    }
}