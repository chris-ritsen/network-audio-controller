use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};

use flate2::{Decompress, FlushDecompress};
use lzxd::{Lzxd, WindowSize};

use super::installer::Source;

const BLOCK_HEADER_LENGTH: usize = 8;
const COMPRESSION_LZX: u16 = 3;
const COMPRESSION_NONE: u16 = 0;
const COMPRESSION_ZIP: u16 = 1;
const DICTIONARY_LENGTH: usize = 0x8000;
const FLAG_NEXT_CABINET: u16 = 0x0002;
const FLAG_PREVIOUS_CABINET: u16 = 0x0001;
const FLAG_RESERVE: u16 = 0x0004;
const FOLDER_CONTINUED: u16 = 0xFFFD;
const HEADER_LENGTH: usize = 36;
const SIGNATURE: [u8; 4] = *b"MSCF";
const ZIP_SIGNATURE: [u8; 2] = *b"CK";

pub struct Entry {
    pub name: String,
    pub offset: u64,
    pub size: u64,
}

pub struct Folder {
    blocks: u16,
    compression: u16,
    pub entries: Vec<Entry>,
    offset: u64,
}

pub struct Layout {
    block_reserve: usize,
    pub folders: Vec<Folder>,
}

enum Decoder {
    Lzx(Box<Lzxd>),
    Stored,
    Zip {
        dictionary: Vec<u8>,
        inflater: Box<Decompress>,
    },
}

pub struct Reader<'a> {
    block_reserve: usize,
    blocks: u16,
    buffer: Vec<u8>,
    consumed: usize,
    decoder: Decoder,
    input: &'a mut dyn Source,
    next: u64,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn little_endian_16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap_or_default())
}

fn little_endian_32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap_or_default())
}

fn skip(input: &mut dyn BufRead, length: usize) -> io::Result<()> {
    let copied = io::copy(&mut input.take(length as u64), &mut io::sink())?;

    if copied == length as u64 {
        Ok(())
    } else {
        Err(invalid("the cabinet ends early"))
    }
}

fn text(input: &mut dyn BufRead) -> io::Result<String> {
    let mut bytes = Vec::new();
    input.read_until(0, &mut bytes)?;

    if bytes.pop() != Some(0) {
        return Err(invalid("the cabinet ends early"));
    }

    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

pub fn layout(input: &mut dyn Source) -> io::Result<Layout> {
    input.rewind()?;
    let mut reader = BufReader::new(input);
    let mut header = [0_u8; HEADER_LENGTH];
    reader.read_exact(&mut header)?;

    if header[..4] != SIGNATURE {
        return Err(invalid("not a cabinet"));
    }

    let files_offset = u64::from(little_endian_32(&header, 16));
    let folder_count = little_endian_16(&header, 26);
    let file_count = little_endian_16(&header, 28);
    let flags = little_endian_16(&header, 30);
    let mut block_reserve = 0;
    let mut folder_reserve = 0;

    if flags & FLAG_RESERVE != 0 {
        let mut reserve = [0_u8; 4];
        reader.read_exact(&mut reserve)?;
        folder_reserve = usize::from(reserve[2]);
        block_reserve = usize::from(reserve[3]);
        skip(&mut reader, usize::from(little_endian_16(&reserve, 0)))?;
    }

    for flag in [FLAG_PREVIOUS_CABINET, FLAG_NEXT_CABINET] {
        if flags & flag != 0 {
            text(&mut reader)?;
            text(&mut reader)?;
        }
    }

    let mut folders = Vec::with_capacity(usize::from(folder_count));

    for _ in 0..folder_count {
        let mut folder = [0_u8; 8];
        reader.read_exact(&mut folder)?;
        skip(&mut reader, folder_reserve)?;
        folders.push(Folder {
            blocks: little_endian_16(&folder, 4),
            compression: little_endian_16(&folder, 6),
            entries: Vec::new(),
            offset: u64::from(little_endian_32(&folder, 0)),
        });
    }

    reader.seek(SeekFrom::Start(files_offset))?;

    for _ in 0..file_count {
        let mut file = [0_u8; 16];
        reader.read_exact(&mut file)?;
        let name = text(&mut reader)?;
        let folder = little_endian_16(&file, 8);

        if folder >= FOLDER_CONTINUED {
            continue;
        }

        folders
            .get_mut(usize::from(folder))
            .ok_or_else(|| invalid("the cabinet names a folder it does not have"))?
            .entries
            .push(Entry {
                name,
                offset: u64::from(little_endian_32(&file, 4)),
                size: u64::from(little_endian_32(&file, 0)),
            });
    }

    for folder in &mut folders {
        folder.entries.sort_by_key(|entry| entry.offset);
    }

    Ok(Layout {
        block_reserve,
        folders,
    })
}

fn window(compression: u16) -> io::Result<WindowSize> {
    Ok(match (compression >> 8) & 0x1F {
        15 => WindowSize::KB32,
        16 => WindowSize::KB64,
        17 => WindowSize::KB128,
        18 => WindowSize::KB256,
        19 => WindowSize::KB512,
        20 => WindowSize::MB1,
        21 => WindowSize::MB2,
        22 => WindowSize::MB4,
        23 => WindowSize::MB8,
        24 => WindowSize::MB16,
        25 => WindowSize::MB32,
        size => return Err(invalid(format!("LZX window {size} is not valid"))),
    })
}

pub fn open<'a>(
    input: &'a mut dyn Source,
    layout: &Layout,
    folder: &Folder,
) -> io::Result<Reader<'a>> {
    let decoder = match folder.compression & 0x000F {
        COMPRESSION_LZX => Decoder::Lzx(Box::new(Lzxd::new(window(folder.compression)?))),
        COMPRESSION_NONE => Decoder::Stored,
        COMPRESSION_ZIP => Decoder::Zip {
            dictionary: Vec::with_capacity(DICTIONARY_LENGTH),
            inflater: Box::new(Decompress::new(false)),
        },
        kind => {
            return Err(invalid(format!(
                "cabinet compression {kind} is not supported"
            )));
        }
    };
    Ok(Reader {
        block_reserve: layout.block_reserve,
        blocks: folder.blocks,
        buffer: Vec::new(),
        consumed: 0,
        decoder,
        input,
        next: folder.offset,
    })
}

impl Decoder {
    fn decode(&mut self, data: Vec<u8>, size: usize) -> io::Result<Vec<u8>> {
        match self {
            Decoder::Lzx(decoder) => decoder
                .decompress_next(&data, size)
                .map(<[u8]>::to_vec)
                .map_err(|error| invalid(error.to_string())),
            Decoder::Stored => Ok(data),
            Decoder::Zip {
                dictionary,
                inflater,
            } => {
                let Some(body) = data.strip_prefix(&ZIP_SIGNATURE) else {
                    return Err(invalid("a cabinet block has no MSZIP signature"));
                };
                inflater.reset(false);

                if !dictionary.is_empty() {
                    let length = dictionary.len() as u16;
                    let mut primer = vec![0_u8];
                    primer.extend_from_slice(&length.to_le_bytes());
                    primer.extend_from_slice(&(!length).to_le_bytes());
                    primer.extend_from_slice(dictionary);
                    let mut discarded = Vec::with_capacity(dictionary.len());
                    inflater
                        .decompress_vec(&primer, &mut discarded, FlushDecompress::Sync)
                        .map_err(|error| invalid(error.to_string()))?;
                }

                let mut output = Vec::with_capacity(size);
                inflater
                    .decompress_vec(body, &mut output, FlushDecompress::Finish)
                    .map_err(|error| invalid(error.to_string()))?;

                if output.len() != size {
                    return Err(invalid("a cabinet block has the wrong length"));
                }

                dictionary.extend_from_slice(&output);
                let excess = dictionary.len().saturating_sub(DICTIONARY_LENGTH);
                dictionary.drain(..excess);
                Ok(output)
            }
        }
    }
}

impl Reader<'_> {
    fn load(&mut self) -> io::Result<bool> {
        if self.blocks == 0 {
            return Ok(false);
        }

        let mut header = [0_u8; BLOCK_HEADER_LENGTH];
        self.input.seek(SeekFrom::Start(self.next))?;
        self.input.read_exact(&mut header)?;
        let stored = usize::from(little_endian_16(&header, 4));
        let size = usize::from(little_endian_16(&header, 6));
        let mut data = vec![0_u8; self.block_reserve + stored];
        self.input.read_exact(&mut data)?;
        data.drain(..self.block_reserve);
        self.next += (BLOCK_HEADER_LENGTH + self.block_reserve + stored) as u64;
        self.blocks -= 1;
        self.buffer = self.decoder.decode(data, size)?;
        self.consumed = 0;
        Ok(true)
    }
}

impl Read for Reader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        while self.consumed == self.buffer.len() {
            if !self.load()? {
                return Ok(0);
            }
        }

        let length = buffer.len().min(self.buffer.len() - self.consumed);
        buffer[..length].copy_from_slice(&self.buffer[self.consumed..self.consumed + length]);
        self.consumed += length;
        Ok(length)
    }
}
