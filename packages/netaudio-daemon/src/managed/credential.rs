use super::Error;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;

#[derive(Clone)]
pub enum Credential {
    Value(String),
    File(PathBuf),
}

impl Credential {
    pub fn new(value: String) -> Result<Self, Error> {
        netaudio_core::managed_api::validate_credential(&value).map_err(Error::Credential)?;
        Ok(Self::Value(value))
    }

    pub async fn read(&self) -> Result<String, Error> {
        let value = match self {
            Self::Value(value) => value.clone(),
            Self::File(path) => {
                let mut options = tokio::fs::OpenOptions::new();
                options.read(true);

                #[cfg(unix)]
                options.custom_flags(nix::libc::O_NONBLOCK);

                let file = options.open(path).await.map_err(|_| {
                    Error::Credential("Managed API credential file could not be read")
                })?;

                if !file
                    .metadata()
                    .await
                    .map_err(|_| {
                        Error::Credential("Managed API credential file could not be inspected")
                    })?
                    .is_file()
                {
                    return Err(Error::Credential(
                        "Managed API credential must be stored in a regular file",
                    ));
                }

                let mut bytes = Vec::new();
                file.take(64 * 1024 + 3)
                    .read_to_end(&mut bytes)
                    .await
                    .map_err(|_| {
                        Error::Credential("Managed API credential file could not be read")
                    })?;

                if bytes.len() > 64 * 1024 + 2 {
                    return Err(Error::Credential(
                        "Managed API credential file is too large",
                    ));
                }

                let text = String::from_utf8(bytes)
                    .map_err(|_| Error::Credential("Managed API credential must be ASCII"))?;
                text.trim_end_matches(['\r', '\n']).to_owned()
            }
        };
        netaudio_core::managed_api::validate_credential(&value).map_err(Error::Credential)?;
        Ok(value)
    }
}
