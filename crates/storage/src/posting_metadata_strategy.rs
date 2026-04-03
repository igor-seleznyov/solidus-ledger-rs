use crate::meta_record::MetaRecordWriter;
use crate::ls_meta_file_header::LsMetaFileHeader;
use crate::consts::LS_FILE_PAGE_SIZE;
use crate::metadata_strategy::MetadataStrategy;

pub struct PostingMetadataStrategy {
    record_writer: MetaRecordWriter,
    meta_buffer_arena: ringbuf::arena::Arena,
    meta_buffer_ptr: *mut u8,
    meta_buffer_len: usize,
    meta_buffer_capacity: usize,
    meta_write_offset: u64,
    record_size: usize,
    pending_padded_len: usize,
}

unsafe impl Send for PostingMetadataStrategy {}

impl PostingMetadataStrategy {
    pub fn new(
        record_size: usize,
        flush_max_buffer_posting_records: usize,
    ) -> Self {
        let record_writer = MetaRecordWriter::new(record_size);
        let raw = flush_max_buffer_posting_records * record_size + LS_FILE_PAGE_SIZE;
        let capacity = (raw + 4095) & !4095;
        let arena = ringbuf::arena::Arena::new(capacity)
            .expect("Failed to create posting metadata buffer Arena");
        let ptr = arena.as_ptr();

        Self {
            record_writer,
            meta_buffer_arena: arena,
            meta_buffer_ptr: ptr,
            meta_buffer_len: 0,
            meta_buffer_capacity: capacity,
            meta_write_offset: 0,
            record_size,
            pending_padded_len: 0,
        }
    }

    pub fn record_size(&self) -> usize {
        self.record_size
    }

    pub unsafe fn write_record(
        &mut self,
        transfer_id_hi: u64,
        transfer_id_lo: u64,
        has_data: bool,
        payload: Option<&[u8]>,
    ) {
        unsafe {
            self.record_writer.write_record(
                self.meta_buffer_ptr,
                self.meta_buffer_len,
                transfer_id_hi,
                transfer_id_lo,
                has_data,
                payload,
            );
            self.meta_buffer_len += self.record_size;
        }
    }
}

impl MetadataStrategy for PostingMetadataStrategy {
    fn file_info(&self, file_path: &str) -> Option<(String, usize)> {
        let meta_path = format!("{}.posting-metadata", file_path);
        let prealloc = self.meta_buffer_capacity * 4;
        Some((meta_path, prealloc))
    }

    fn prepare_header(&mut self, file_seq: u64, max_file_size: u64) -> Option<(*const u8, usize, u64)> {
        let meta_header = LsMetaFileHeader::new(
            self.record_size as u32,
            max_file_size,
            file_seq,
        );
        
        let header_page = meta_header.to_page();
        
        unsafe {
            std::ptr::copy_nonoverlapping(
                header_page.as_ptr(),
                self.meta_buffer_ptr,
                LS_FILE_PAGE_SIZE,
            );
        }
        
        Some((self.meta_buffer_ptr as *const u8, LS_FILE_PAGE_SIZE, 0))
    }
    
    fn on_header_written(&mut self) {
        self.meta_write_offset = LsMetaFileHeader::DATA_OFFSET as u64;
        self.meta_buffer_len = 0;
    }
    
    fn prepare_flush(&mut self) -> Option<(*const u8, usize, u64)> {
        if self.meta_buffer_len == 0 {
            return None;
        }
        
        let padded_len = (self.meta_buffer_len + 4095) & !4095;
        
        if padded_len > self.meta_buffer_len {
            unsafe {
                std::ptr::write_bytes(
                    self.meta_buffer_ptr.add(self.meta_buffer_len),
                    0,
                    padded_len - self.meta_buffer_len,
                );
            }
        }
        
        self.pending_padded_len = padded_len;
        
        Some(
            (self.meta_buffer_ptr as *const u8, padded_len, self.meta_write_offset)
        )
    }
    
    fn on_flush_complete(&mut self) {
        self.meta_write_offset += self.pending_padded_len as u64;
        self.meta_buffer_len = 0;
        self.pending_padded_len = 0;
    }
    
    fn on_rotation(&mut self) {
        self.meta_write_offset = 0;
        self.meta_buffer_len = 0;
        self.pending_padded_len = 0;
    }
    
    fn is_enabled(&self) -> bool {
        true
    }
}