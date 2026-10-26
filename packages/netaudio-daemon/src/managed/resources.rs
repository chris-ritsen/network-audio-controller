use super::{Credential, Error, configuration::Server};
pub(super) use crate::resources::{FileWatch as ResourceWatch, resource_paths};
use anyhow::Result;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::OpenOptions,
    io::Read,
    path::{Path, PathBuf},
};

pub(super) struct Resources {
    pub revision: [u8; 32],
    pub credential: Result<Credential, Error>,
    pub certificates: Result<Option<Vec<u8>>, Error>,
    pub paths: BTreeSet<PathBuf>,
}

impl Resources {
    pub fn read(server: &Server) -> Self {
        let mut paths = BTreeSet::new();
        let mut revision = Sha256::new();
        let mut read = |path: Option<&Path>, limit, label: &[u8]| {
            revision.update(label);
            let result = path
                .map(|path| {
                    paths.extend(resource_paths(path));
                    read_file(path, limit)
                })
                .transpose();
            match &result {
                Ok(Some(bytes)) => {
                    revision.update([1]);
                    revision.update((bytes.len() as u64).to_be_bytes());
                    revision.update(bytes);
                }
                Ok(None) => revision.update([0]),
                Err(error) => {
                    revision.update([2]);
                    revision.update(error.to_string());
                }
            }
            result
        };
        let credential = read(
            server.credential_file.as_deref(),
            64 * 1024 + 2,
            b"credential",
        )
        .map_err(|_| Error::Credential("Managed API credential file is unavailable or invalid"))
        .and_then(|bytes| {
            let bytes = bytes.ok_or(Error::Credential("DDM credential file is missing"))?;
            let value = String::from_utf8(bytes)
                .map_err(|_| Error::Credential("Managed API credential must be ASCII"))?;
            Credential::new(value.trim_end_matches(['\r', '\n']).to_owned())
        });
        let certificates = read(
            server.trust_certificate_file.as_deref(),
            1024 * 1024,
            b"certificates",
        )
        .map_err(|_| {
            Error::Tls("Managed service trust certificate file is unavailable or invalid")
        });
        Self {
            revision: revision.finalize().into(),
            credential,
            certificates,
            paths,
        }
    }
}

pub(super) fn read_file(path: &Path, maximum: usize) -> Result<Vec<u8>, Error> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(nix::libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .map_err(|_| Error::Persistence("Managed resource could not be opened"))?;
    if !file
        .metadata()
        .map_err(|_| Error::Persistence("Managed resource could not be inspected"))?
        .is_file()
    {
        return Err(Error::Persistence(
            "Managed resource must be a regular file",
        ));
    }
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::Persistence("Managed resource could not be read"))?;
    if bytes.len() > maximum {
        return Err(Error::TooLarge);
    }
    Ok(bytes)
}
