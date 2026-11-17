use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use base64::Engine;
use flate2::read::ZlibDecoder;

use super::cabinet;
use super::scan::{Found, Scanner, Stream};

const BLOCK_COMMENT: u32 = 0x7FFF_FFFE;
const BLOCK_END: u32 = 0xFFFF_FFFF;
const BLOCK_IGNORED: u32 = 0x0000_0002;
const BLOCK_RAW: u32 = 0x0000_0001;
const BLOCK_ZERO: u32 = 0x0000_0000;
const BLOCK_ZLIB: u32 = 0x8000_0005;
const CABINET: [u8; 4] = *b"MSCF";
const CABINET_HEADER_LENGTH: u64 = 36;
const CABINET_SIGNATURE: [u8; 8] = *b"MSCF\0\0\0\0";
const CHUNK_LENGTH: usize = 1 << 20;
const COMPOUND: [u8; 8] = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
const DATA_CLOSE: &str = "</data>";
const DATA_OPEN: &str = "<data>";
const EXECUTABLE: [u8; 2] = *b"MZ";
const MAXIMUM_DEPTH: usize = 4;
const MAXIMUM_SPOOL_LENGTH: u64 = 4 << 30;
const MAXIMUM_TABLE_LENGTH: u64 = 64 << 20;
const TABLE: [u8; 4] = *b"mish";
const TABLE_BLOCK_LENGTH: usize = 40;
const TABLE_HEADER_LENGTH: usize = 204;
const TRAILER: [u8; 4] = *b"koly";
const TRAILER_LENGTH: u64 = 512;

pub trait Source: Read + Seek {}

impl<R: Read + Seek> Source for R {}

struct Window<'a> {
    inner: &'a mut dyn Source,
    length: u64,
    position: u64,
    start: u64,
}

impl Read for Window<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let remaining = self.length.saturating_sub(self.position);
        let limit = buffer
            .len()
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));

        if limit == 0 {
            return Ok(0);
        }

        self.inner
            .seek(SeekFrom::Start(self.start + self.position))?;
        let length = self.inner.read(&mut buffer[..limit])?;
        self.position += length as u64;
        Ok(length)
    }
}

impl Seek for Window<'_> {
    fn seek(&mut self, target: SeekFrom) -> io::Result<u64> {
        let position = match target {
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
            SeekFrom::End(offset) => self.length.checked_add_signed(offset),
            SeekFrom::Start(offset) => Some(offset),
        };
        self.position = position.ok_or_else(|| invalid("seek before the start"))?;
        Ok(self.position)
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn big_endian_32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap_or_default())
}

fn big_endian_64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap_or_default())
}

fn fill(input: &mut dyn Read, buffer: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;

    while filled < buffer.len() {
        match input.read(&mut buffer[filled..])? {
            0 => break,
            length => filled += length,
        }
    }

    Ok(filled)
}

fn pump(input: &mut dyn Read, mut accept: impl FnMut(&[u8])) -> io::Result<()> {
    let mut chunk = vec![0_u8; CHUNK_LENGTH];

    loop {
        match input.read(&mut chunk)? {
            0 => return Ok(()),
            length => accept(&chunk[..length]),
        }
    }
}

pub fn scratch_directory() -> PathBuf {
    let disk = Path::new("/var/tmp");

    if cfg!(all(unix, not(target_os = "macos"))) && disk.is_dir() {
        disk.to_owned()
    } else {
        std::env::temp_dir()
    }
}

pub fn scan(input: &mut dyn Source) -> io::Result<Found> {
    source(input, 0)
}

fn source(input: &mut dyn Source, depth: usize) -> io::Result<Found> {
    let length = input.seek(SeekFrom::End(0))?;
    let mut magic = [0_u8; 8];
    input.rewind()?;
    fill(input, &mut magic)?;
    let mut trailer = [0_u8; 4];

    if length >= TRAILER_LENGTH {
        input.seek(SeekFrom::Start(length - TRAILER_LENGTH))?;
        fill(input, &mut trailer)?;
    }

    input.rewind()?;
    let contained = if depth >= MAXIMUM_DEPTH {
        None
    } else if trailer == TRAILER {
        Some(disk_image(input, length))
    } else if magic == COMPOUND {
        Some(compound(input, depth))
    } else if magic[..4] == CABINET {
        Some(cabinet(input, depth))
    } else if magic[..2] == EXECUTABLE {
        Some(executable(input, length, depth))
    } else {
        None
    };

    match contained {
        Some(Ok(found)) => Ok(found),
        Some(Err(error)) => {
            input.rewind()?;
            let mut found = stream(input)?;
            found.problems.push(error.to_string());
            Ok(found)
        }
        None => stream(input),
    }
}

fn stream(input: &mut dyn Source) -> io::Result<Found> {
    let mut stream = Stream::default();
    pump(input, |chunk| stream.accept(chunk))?;
    Ok(stream.found())
}

fn block_tables(text: &str) -> Vec<Vec<u8>> {
    let mut tables: Vec<Vec<u8>> = text
        .split(DATA_OPEN)
        .skip(1)
        .filter_map(|rest| rest.split(DATA_CLOSE).next())
        .filter_map(|encoded| {
            let compact: String = encoded
                .chars()
                .filter(|character| !character.is_whitespace())
                .collect();
            base64::engine::general_purpose::STANDARD
                .decode(compact)
                .ok()
        })
        .filter(|table| table.len() >= TABLE_HEADER_LENGTH && table[..4] == TABLE)
        .collect();
    tables.sort_by_key(|table| big_endian_64(table, 8));
    tables
}

fn disk_image(input: &mut dyn Source, length: u64) -> io::Result<Found> {
    let mut trailer = [0_u8; TRAILER_LENGTH as usize];
    input.seek(SeekFrom::Start(length - TRAILER_LENGTH))?;
    input.read_exact(&mut trailer)?;
    let data_offset = big_endian_64(&trailer, 24);
    let list_offset = big_endian_64(&trailer, 216);
    let list_length = big_endian_64(&trailer, 224);

    if list_length == 0 || list_length > MAXIMUM_TABLE_LENGTH {
        return Err(invalid("the disk image has no block list"));
    }

    let mut list = vec![0_u8; list_length as usize];
    input.seek(SeekFrom::Start(list_offset))?;
    input.read_exact(&mut list)?;
    let tables = block_tables(&String::from_utf8_lossy(&list));

    if tables.is_empty() {
        return Err(invalid("the disk image has no block list"));
    }

    let mut stream = Stream::default();

    for table in tables {
        let count = big_endian_32(&table, 200) as usize;
        let blocks = table[TABLE_HEADER_LENGTH..]
            .as_chunks::<TABLE_BLOCK_LENGTH>()
            .0
            .iter()
            .take(count);

        for block in blocks {
            let offset = data_offset
                .checked_add(big_endian_64(block, 24))
                .ok_or_else(|| invalid("the disk image block list is damaged"))?;
            let stored = big_endian_64(block, 32);

            match big_endian_32(block, 0) {
                BLOCK_COMMENT | BLOCK_END => {}
                BLOCK_IGNORED | BLOCK_ZERO => stream.accept(&[0]),
                BLOCK_RAW => {
                    input.seek(SeekFrom::Start(offset))?;
                    pump(&mut (&mut *input).take(stored), |chunk| {
                        stream.accept(chunk)
                    })?;
                }
                BLOCK_ZLIB => {
                    input.seek(SeekFrom::Start(offset))?;
                    let mut decoder = ZlibDecoder::new((&mut *input).take(stored));
                    pump(&mut decoder, |chunk| stream.accept(chunk))?;
                }
                kind => {
                    return Err(invalid(format!(
                        "the disk image uses compression {kind:#x}, which is not supported"
                    )));
                }
            }
        }
    }

    Ok(stream.found())
}

fn cabinet_length(input: &mut dyn Source, offset: u64, length: u64) -> io::Result<Option<u64>> {
    let mut header = [0_u8; CABINET_HEADER_LENGTH as usize];
    input.seek(SeekFrom::Start(offset))?;

    if fill(input, &mut header)? < header.len() {
        return Ok(None);
    }

    let size = u64::from(u32::from_le_bytes(
        header[8..12].try_into().unwrap_or_default(),
    ));
    let valid = header[24] == 3
        && header[25] == 1
        && size >= CABINET_HEADER_LENGTH
        && offset + size <= length;
    Ok(valid.then_some(size))
}

fn executable(input: &mut dyn Source, length: u64, depth: usize) -> io::Result<Found> {
    let mut matched = 0;
    let mut offsets = Vec::new();
    let mut position = 0_u64;
    let mut scanner = Scanner::default();
    pump(input, |chunk| {
        scanner.accept(chunk);

        for &byte in chunk {
            position += 1;
            matched = if byte == CABINET_SIGNATURE[matched] {
                matched + 1
            } else {
                usize::from(byte == CABINET_SIGNATURE[0])
            };

            if matched == CABINET_SIGNATURE.len() {
                offsets.push(position - CABINET_SIGNATURE.len() as u64);
                matched = 0;
            }
        }
    })?;
    let mut found = scanner.found();
    let mut covered = 0;

    for offset in offsets {
        if offset < covered {
            continue;
        }

        let Some(size) = cabinet_length(input, offset, length)? else {
            continue;
        };
        let mut window = Window {
            inner: &mut *input,
            length: size,
            position: 0,
            start: offset,
        };

        match cabinet(&mut window, depth + 1) {
            Ok(inner) => {
                found.extend(inner);
                covered = offset + size;
            }
            Err(error) => found
                .problems
                .push(format!("could not read the cabinet at {offset}: {error}")),
        }
    }

    Ok(found)
}

fn cabinet(input: &mut dyn Source, depth: usize) -> io::Result<Found> {
    let layout = cabinet::layout(input)?;
    let mut found = Found::default();

    for folder in &layout.folders {
        if let Err(error) = folder_members(input, &layout, folder, depth, &mut found) {
            found
                .problems
                .push(format!("could not read a cabinet folder: {error}"));
        }
    }

    Ok(found)
}

fn folder_members(
    input: &mut dyn Source,
    layout: &cabinet::Layout,
    folder: &cabinet::Folder,
    depth: usize,
    found: &mut Found,
) -> io::Result<()> {
    let mut reader = cabinet::open(input, layout, folder)?;
    let mut position = 0;

    for entry in &folder.entries {
        if entry.offset < position {
            continue;
        }

        io::copy(
            &mut (&mut reader).take(entry.offset - position),
            &mut io::sink(),
        )?;
        let mut contents = (&mut reader).take(entry.size);

        match member(&mut contents, depth) {
            Ok(inner) => found.extend(inner),
            Err(error) => found
                .problems
                .push(format!("could not read {}: {error}", entry.name)),
        }

        io::copy(&mut contents, &mut io::sink())?;
        position = entry.offset + entry.size;
    }

    Ok(())
}

fn member(contents: &mut dyn Read, depth: usize) -> io::Result<Found> {
    let mut magic = [0_u8; 8];
    let read = fill(contents, &mut magic)?;
    let container = read == magic.len() && (magic == COMPOUND || magic[..4] == CABINET);

    if container && depth < MAXIMUM_DEPTH {
        let mut spool = tempfile::tempfile_in(scratch_directory())?;
        spool.write_all(&magic)?;
        io::copy(&mut contents.take(MAXIMUM_SPOOL_LENGTH), &mut spool)?;
        return source(&mut spool, depth + 1);
    }

    let mut scanner = Scanner::default();
    scanner.accept(&magic[..read]);
    pump(contents, |chunk| scanner.accept(chunk))?;
    Ok(scanner.found())
}

fn compound(input: &mut dyn Source, depth: usize) -> io::Result<Found> {
    let mut file = cfb::CompoundFile::open(input)?;
    let paths: Vec<PathBuf> = file
        .walk()
        .filter(|entry| entry.is_stream())
        .map(|entry| entry.path().to_owned())
        .collect();
    let mut found = Found::default();

    for path in paths {
        let scanned = file
            .open_stream(&path)
            .and_then(|mut stream| source(&mut stream, depth + 1));

        match scanned {
            Ok(inner) => found.extend(inner),
            Err(error) => found
                .problems
                .push(format!("could not read {}: {error}", path.display())),
        }
    }

    Ok(found)
}
