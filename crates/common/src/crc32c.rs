use std::arch::x86_64::{_mm_crc32_u64, _mm_crc32_u8};

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
pub unsafe fn crc32c(data: *const u8, len: usize) -> u32 {
    let mut crc: u64 = 0xFFFF_FFFF;
    let mut ptr = data;
    let end8 = unsafe { data.add(len & !7) };
    let end = unsafe { data.add(len) };

    while ptr < end8 {
        crc = unsafe { _mm_crc32_u64(crc, std::ptr::read_unaligned(ptr as *const u64)) };
        ptr = unsafe { ptr.add(8) };
    }

    while ptr < end {
        crc = unsafe { _mm_crc32_u8(crc as u32, *ptr) as u64 };
        ptr = unsafe { ptr.add(1) };
    }

    !(crc as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_empty() {
        unsafe {
            let data: [u8; 0] = [];
            let crc = crc32c(data.as_ptr(), 0);
            assert_eq!(crc, 0x00000000);
        }
    }

    #[test]
    fn crc32c_deterministic() {
        unsafe {
            let data = [1u8, 2, 3, 4, 5, 6, 7, 8];
            let crc1 = crc32c(data.as_ptr(), data.len());
            let crc2 = crc32c(data.as_ptr(), data.len());
            assert_eq!(crc1, crc2);
        }
    }

    #[test]
    fn crc32c_different_data_different_crc() {
        unsafe {
            let data1 = [1u8; 16];
            let data2 = [2u8; 16];
            let crc1 = crc32c(data1.as_ptr(), data1.len());
            let crc2 = crc32c(data2.as_ptr(), data2.len());
            assert_ne!(crc1, crc2);
        }
    }

    #[test]
    fn crc32c_detects_single_bit_flip() {
        unsafe {
            let mut data = [0u8; 64];
            data[32] = 0xFF;
            let crc_original = crc32c(data.as_ptr(), data.len());

            data[32] = 0xFE;
            let crc_flipped = crc32c(data.as_ptr(), data.len());

            assert_ne!(crc_original, crc_flipped);
        }
    }

    #[test]
    fn crc32c_known_value() {
        unsafe {
            let data = b"123456789";
            let crc = crc32c(data.as_ptr(), data.len());
            assert_eq!(crc, 0xE3069283);
        }
    }

    #[test]
    fn crc32c_128_bytes() {
        unsafe {
            let data = [0xABu8; 128];
            let crc = crc32c(data.as_ptr(), data.len());
            assert_ne!(crc, 0);
            assert_eq!(crc, crc32c(data.as_ptr(), data.len()));
        }
    }
}