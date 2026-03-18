pub fn raw_u128_to_u64(bytes: &[u8; 16]) -> (u64, u64) {
    let hi = u64::from_be_bytes(
        [
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
        ]
    );
    
    let lo = u64::from_be_bytes(
        [
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15],
        ]
    );
    (hi, lo)
}