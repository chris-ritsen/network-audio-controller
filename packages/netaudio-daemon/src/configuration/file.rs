use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

pub const MAXIMUM_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Debug)]
pub enum Error {
    Busy,
    Invalid(&'static str),
    Persistence(&'static str),
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => formatter.write_str("Configuration is being changed by another request"),
            Self::Invalid(message) | Self::Persistence(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

pub struct Lock {
    _file: File,
}

impl Lock {
    pub fn acquire(path: &Path) -> Result<Self, Error> {
        let directory = path.parent().ok_or(Error::Persistence(
            "Configuration path has no parent directory",
        ))?;
        create_directory(directory)?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(nix::libc::O_NONBLOCK | nix::libc::O_NOFOLLOW);
        }
        let file = options
            .open(directory.join(".configuration.lock"))
            .map_err(|_| Error::Persistence("Configuration lock could not be opened"))?;
        if !file
            .metadata()
            .map_err(|_| Error::Persistence("Configuration lock could not be inspected"))?
            .is_file()
        {
            return Err(Error::Persistence(
                "Configuration lock must be a regular file",
            ));
        }
        fs4::FileExt::try_lock(&file).map_err(|error| match error {
            fs4::TryLockError::WouldBlock => Error::Busy,
            fs4::TryLockError::Error(_) => {
                Error::Persistence("Configuration lock could not be acquired")
            }
        })?;
        Ok(Self { _file: file })
    }
}

pub fn read(path: &Path) -> Result<String, Error> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(nix::libc::O_NONBLOCK);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(String::new()),
        Err(_) => return Err(Error::Persistence("Saved configuration could not be read")),
    };
    if !file
        .metadata()
        .map_err(|_| Error::Persistence("Saved configuration could not be inspected"))?
        .is_file()
    {
        return Err(Error::Persistence(
            "Saved configuration must be a regular file",
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAXIMUM_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::Persistence("Saved configuration could not be read"))?;
    if bytes.len() as u64 > MAXIMUM_BYTES {
        return Err(Error::Invalid("Saved configuration exceeds 8 MiB"));
    }
    String::from_utf8(bytes).map_err(|_| Error::Invalid("Saved configuration must be UTF-8"))
}

pub fn create_directory(path: &Path) -> Result<(), Error> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(path)
        .map_err(|_| Error::Persistence("Configuration directory could not be created"))
}

pub fn atomic_write(path: &Path, bytes: &[u8], installed: &mut bool) -> Result<(), Error> {
    if bytes.len() as u64 > MAXIMUM_BYTES {
        return Err(Error::Invalid("Saved configuration exceeds 8 MiB"));
    }
    let directory = path.parent().ok_or(Error::Persistence(
        "Configuration path has no parent directory",
    ))?;
    let mut temporary = tempfile::Builder::new()
        .prefix(".configuration-")
        .tempfile_in(directory)
        .map_err(|_| Error::Persistence("Configuration file could not be created"))?;
    temporary
        .write_all(bytes)
        .map_err(|_| Error::Persistence("Configuration file could not be written"))?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|_| Error::Persistence("Configuration file could not be flushed"))?;
    temporary
        .persist(path)
        .map_err(|_| Error::Persistence("Configuration file could not be installed"))?;
    *installed = true;
    sync_directory(directory)
}

pub fn sync_directory(path: &Path) -> Result<(), Error> {
    #[cfg(unix)]
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|_| Error::Persistence("Configuration directory could not be flushed"))?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}
