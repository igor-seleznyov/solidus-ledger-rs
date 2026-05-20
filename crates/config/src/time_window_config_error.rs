use thiserror::Error;

#[derive(Debug, Error)]
pub enum TimeWindowConfigError {
    #[error("time window must be HH:MM, got: {value}")]
    InvalidFormat { value: String },

    #[error("invalid hour in time window: {value}")]
    InvalidHour { value: String },

    #[error("invalid minute in time window: {value}")]
    InvalidMinute { value: String },

    #[error("hour out of range 0..=23 in time window: {value}")]
    HourOutOfRange { value: String },

    #[error("minute out of range 0..=59 in time window: {value}")]
    MinuteOutOfRange { value: String },
}