use std::fmt;

#[derive(Debug)]
pub enum Error {
    Invalid(String),
    Conflict(String),
    Busy,
    Internal(anyhow::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) | Self::Conflict(message) => formatter.write_str(message),
            Self::Busy => formatter.write_str(
                "Preset processing is busy. Try again when the current request finishes.",
            ),
            Self::Internal(error) => write!(formatter, "{error:#}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        Self::Internal(error)
    }
}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self::Invalid(message)
    }
}

impl From<&str> for Error {
    fn from(message: &str) -> Self {
        Self::Invalid(message.into())
    }
}
