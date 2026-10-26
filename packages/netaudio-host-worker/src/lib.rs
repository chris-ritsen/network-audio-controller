pub mod output;

#[cfg(unix)]
pub mod input;

pub fn timestamp() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

pub fn timestamp_milliseconds(milliseconds: u64) -> Option<String> {
    chrono::DateTime::from_timestamp_millis(i64::try_from(milliseconds).ok()?)
        .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

pub fn timestamp_microseconds(microseconds: i64) -> Option<String> {
    chrono::DateTime::from_timestamp_micros(microseconds)
        .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
}
