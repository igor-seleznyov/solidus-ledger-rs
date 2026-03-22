pub fn u64_pair_to_bytes(hi: u64, lo: u64) -> [u8; 16] {
    let mut buf = [0u8; 16];
    buf[0..8].copy_from_slice(&hi.to_be_bytes());
    buf[8..16].copy_from_slice(&lo.to_be_bytes());
    buf
}
