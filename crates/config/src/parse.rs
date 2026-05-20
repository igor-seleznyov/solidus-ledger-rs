use serde::Deserialize;
use crate::time_window_config_error::TimeWindowConfigError;

pub fn parse_hhmm(value: &str) -> Result<u32, TimeWindowConfigError> {
    let parts: Vec<&str> = value.split(':').collect();
    if parts.len() != 2 {
        return Err(TimeWindowConfigError::InvalidFormat { value: value.to_string() });
    }
    let hh: u32 = parts[0].parse()
        .map_err(|_| TimeWindowConfigError::InvalidHour { value: value.to_string() })?;
    let mm: u32 = parts[1].parse()
        .map_err(|_| TimeWindowConfigError::InvalidMinute { value: value.to_string() })?;
    if hh > 23 {
        return Err(TimeWindowConfigError::HourOutOfRange { value: value.to_string() });
    }
    if mm > 59 {
        return Err(TimeWindowConfigError::MinuteOutOfRange { value: value.to_string() });
    }
    Ok(hh * 60 + mm)
}

pub fn validate_hhmm(value: &str) -> Result<(), TimeWindowConfigError> {
    parse_hhmm(value).map(|_| ())
}

use serde::de::Error;
use uuid::serde::urn::deserialize;

#[derive(Deserialize)]
struct RawTimeWindow {
    start: String,
    end: String,
}

pub fn deserialize_time_window<'de, D>(duration: D) -> Result<Vec<(u32, u32)>, D::Error>
where D: serde::Deserializer<'de> {
    let raw: Vec<RawTimeWindow> = Vec::deserialize(duration)?;
    raw.into_iter()
        .map(
            |window| {
                let start = parse_hhmm(&window.start).map_err(Error::custom)?;
                let end = parse_hhmm(&window.end).map_err(Error::custom)?;
                Ok((start, end))
            }
        ).collect()
}

pub fn deserialize_instance_id_hex<'de, D>(
    deserializer: D,
) -> Result<Option<[u8; 16]>, D::Error>
where D: serde::Deserializer<'de> {
    let opt: Option<String> = Option::deserialize(deserializer)?;
    match opt {
        None => Ok(None),
        Some(str) => {
            if str.len() != 32 {
                return Err(
                    serde::de::Error::custom(
                        "instance-id must be exactly 32 hex characters (16 bytes)"
                    )
                );
            }
            let mut bytes = [0u8; 16];
            for (i, chunk) in str.as_bytes().chunks(2).enumerate() {
                let hex_pair = std::str::from_utf8(chunk).map_err(serde::de::Error::custom)?;
                bytes[i] = u8::from_str_radix(hex_pair, 16).map_err(
                    |err| serde::de::Error::custom(
                        format!("invalid hex in instance-id at byte {}: {}", i, err)
                    )
                )?;
            }
            Ok(
                Some(bytes)
            )
        }
    }
}