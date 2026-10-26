pub mod discovery;
pub mod h264;
pub mod inventory;
pub mod rtp;
pub mod source_control;

pub const RECEIVE_RETURN: &[u8; 11] = b"KEEP ALIVE\0";
pub const RETURN_INTERVAL_NANOSECONDS: i64 = 250_000_000;
pub const NANOSECONDS_PER_MILLISECOND: i64 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Error(pub &'static str);

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for Error {}

pub fn local_deadline(arrival_nanoseconds: i64, delay_milliseconds: u32) -> Result<i64, Error> {
    let delay = i64::from(delay_milliseconds)
        .checked_mul(NANOSECONDS_PER_MILLISECOND)
        .ok_or(Error("local video delay overflow"))?;

    arrival_nanoseconds
        .checked_add(delay)
        .ok_or(Error("local video deadline overflow"))
}

#[derive(Default)]
pub struct ReceiveReturn {
    last_attempt_nanoseconds: i64,
}

impl ReceiveReturn {
    pub fn attempt_due(&mut self, now_nanoseconds: i64) -> Result<bool, Error> {
        let elapsed = now_nanoseconds
            .checked_sub(self.last_attempt_nanoseconds)
            .filter(|elapsed| *elapsed >= 0)
            .ok_or(Error("video return clock moved backwards"))?;

        if elapsed <= RETURN_INTERVAL_NANOSECONDS {
            return Ok(false);
        }

        self.last_attempt_nanoseconds = now_nanoseconds;
        Ok(true)
    }
}
