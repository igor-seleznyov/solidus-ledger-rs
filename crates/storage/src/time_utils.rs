pub const MINUTES_PER_DAY: u32 = 24 * 60;
pub const SECONDS_PER_HOUR: u32 = 60 * 60;
pub const SECONDS_PER_DAY: u32 = SECONDS_PER_HOUR * 24;

pub fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0)
}

pub fn format_utc_timestamp(nanos: u64) -> String {
    let secs = (nanos / 1_000_000_000) as i64;
    let nsec = (nanos % 1_000_000_000) as u32;
    let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsec)
        .unwrap_or_else(
            || chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap()
        );
    datetime.format("%Y%m%dT%H%M%SZ").to_string()
}

