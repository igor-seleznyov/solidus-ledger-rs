use common::crc32c::crc32c;

pub const META_RECORD_MAGIC: u64 = 0x5244_544D_5453_444C;

pub struct MetaRecordWriter {
    record_size: usize,
}

const MAGIC_OFFSET: usize = 0;
const TRANSFER_ID_HI_OFFSET: usize = 8;
const TRANSFER_ID_LO_OFFSET: usize = 16;
const HAS_DATA_OFFSET: usize = 24;
const PAD_OFFSET: usize = 25;
const PAYLOAD_OFFSET: usize = 32;

impl MetaRecordWriter {
    pub fn new(record_size: usize) -> Self {
        assert!(record_size.is_power_of_two());
        Self { record_size }
    }

    pub fn record_size(&self) -> usize {
        self.record_size
    }

    pub fn payload_size(&self) -> usize {
        self.record_size - PAYLOAD_OFFSET
    }

    pub unsafe fn write_record(
        &self,
        buffer: *mut u8,
        offset: usize,
        transfer_id_hi: u64,
        transfer_id_lo: u64,
        has_data: bool,
        payload: Option<&[u8]>,
    ) {
        let base = unsafe { buffer.add(offset) };

        unsafe {
            std::ptr::copy_nonoverlapping(
                &META_RECORD_MAGIC as *const u64 as *const u8,
                base.add(MAGIC_OFFSET),
                8,
            );
            std::ptr::copy_nonoverlapping(
                &transfer_id_hi as *const u64 as *const u8,
                base.add(TRANSFER_ID_HI_OFFSET),
                8,
            );
            std::ptr::copy_nonoverlapping(
                &transfer_id_lo as *const u64 as *const u8,
                base.add(TRANSFER_ID_LO_OFFSET),
                8,
            );

            *base.add(HAS_DATA_OFFSET) = if has_data { 1 } else { 0 };

            std::ptr::write_bytes(base.add(PAD_OFFSET), 0, 7);
        }

        match payload {
            Some(data) => {
                let copy_len = data.len().min(self.payload_size());
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        data.as_ptr(),
                        base.add(PAYLOAD_OFFSET),
                        copy_len,
                    );
                }
                if copy_len < self.payload_size() {
                    unsafe {
                        std::ptr::write_bytes(
                            base.add(PAYLOAD_OFFSET + copy_len),
                            0,
                            self.payload_size() - copy_len,
                        );
                    }
                }
            }
            None => {
                unsafe {
                    std::ptr::write_bytes(
                        base.add(PAYLOAD_OFFSET),
                        0,
                        self.payload_size(),
                    );
                }
            }
        }
    }

    pub unsafe fn write_empty_record(
        &self,
        buffer: *mut u8,
        offset: usize,
        transfer_id_hi: u64,
        transfer_id_lo: u64,
    ) {
        unsafe {
            self.write_record(
                buffer,
                offset,
                transfer_id_hi,
                transfer_id_lo,
                false,
                None,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_size_calculation() {
        assert_eq!(MetaRecordWriter::new(64).payload_size(), 32);
        assert_eq!(MetaRecordWriter::new(128).payload_size(), 96);
        assert_eq!(MetaRecordWriter::new(256).payload_size(), 224);
        assert_eq!(MetaRecordWriter::new(2048).payload_size(), 2016);
    }

    #[test]
    fn write_record_has_magic() {
        let writer = MetaRecordWriter::new(64);
        let mut buffer = vec![0u8; 64];

        unsafe {
            writer.write_record(
                buffer.as_mut_ptr(), 0,
                0xAAAA, 0xBBBB, true, Some(&[1, 2, 3]),
            );
        }

        let magic = u64::from_ne_bytes(buffer[0..8].try_into().unwrap());
        assert_eq!(magic, META_RECORD_MAGIC);

        assert_eq!(buffer[32], 1);
        assert_eq!(buffer[33], 2);
        assert_eq!(buffer[34], 3);
    }
}