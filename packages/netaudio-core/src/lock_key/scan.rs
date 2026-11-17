use std::collections::BTreeSet;

use flate2::{Decompress, FlushDecompress, Status};

use crate::lock::KEY_LENGTH;

const COMMENT_FLAG: u8 = 0x10;
const CHECKSUM_FLAG: u8 = 0x02;
const EXTRA_FLAG: u8 = 0x04;
const FIXED_LENGTH: usize = 10;
const MAXIMUM_MEMBERS: usize = 8;
const MAXIMUM_TEXT_LENGTH: usize = 1 << 16;
const MINIMUM_DISTINCT_STEPS: usize = 8;
const NAME_FLAG: u8 = 0x08;
const OUTPUT_LENGTH: usize = 1 << 16;
const RESERVED_FLAGS: u8 = 0xE0;
const RUN_LENGTH: usize = KEY_LENGTH * 2;
const SIGNATURE: [u8; 3] = [0x1F, 0x8B, 0x08];

#[derive(Default)]
pub struct Found {
    pub problems: Vec<String>,
    pub terminated: BTreeSet<String>,
    pub unterminated: BTreeSet<String>,
}

impl Found {
    pub fn extend(&mut self, other: Found) {
        self.problems.extend(other.problems);
        self.terminated.extend(other.terminated);
        self.unterminated.extend(other.unterminated);
    }

    pub fn is_empty(&self) -> bool {
        self.terminated.is_empty() && self.unterminated.is_empty()
    }

    pub fn keys(&self) -> &BTreeSet<String> {
        if self.terminated.is_empty() {
            &self.unterminated
        } else {
            &self.terminated
        }
    }
}

#[derive(Default)]
pub struct Scanner {
    preceded_by_terminator: bool,
    previous: Option<u8>,
    run: Vec<u8>,
    run_length: usize,
    terminated: BTreeSet<String>,
    unterminated: BTreeSet<String>,
}

impl Scanner {
    pub fn accept(&mut self, chunk: &[u8]) {
        for &byte in chunk {
            if byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte) {
                if self.run_length == 0 {
                    self.preceded_by_terminator = self.previous == Some(0);
                    self.run.clear();
                }

                if self.run_length < RUN_LENGTH {
                    self.run.push(byte);
                }

                self.run_length += 1;
            } else {
                self.finish(byte == 0);
            }

            self.previous = Some(byte);
        }
    }

    fn finish(&mut self, followed_by_terminator: bool) {
        if self.run_length == RUN_LENGTH && !is_table(&self.run) {
            let key = String::from_utf8_lossy(&self.run[..KEY_LENGTH]).into_owned();

            if self.preceded_by_terminator && followed_by_terminator {
                self.terminated.insert(key);
            } else {
                self.unterminated.insert(key);
            }
        }

        self.run_length = 0;
    }

    pub fn found(mut self) -> Found {
        self.finish(false);
        Found {
            problems: Vec::new(),
            terminated: self.terminated,
            unterminated: self.unterminated,
        }
    }
}

fn is_table(run: &[u8]) -> bool {
    let bytes: Vec<i16> = run
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| i16::from(nibble(pair[0]) << 4 | nibble(pair[1])))
        .collect();
    let steps: BTreeSet<i16> = bytes.windows(2).map(|pair| pair[1] - pair[0]).collect();
    steps.len() < MINIMUM_DISTINCT_STEPS
}

fn nibble(character: u8) -> u8 {
    match character {
        b'0'..=b'9' => character - b'0',
        _ => character - b'a' + 10,
    }
}

enum Progress {
    Active,
    Failed,
    Finished,
}

enum Stage {
    Body,
    Checksum { remaining: usize },
    Comment { seen: usize },
    Extra { remaining: usize },
    ExtraLength { length: usize, seen: usize },
    Fixed { seen: usize },
    Name { seen: usize },
}

struct Member {
    flags: u8,
    inflater: Decompress,
    output: Vec<u8>,
    scanner: Scanner,
    stage: Stage,
}

impl Member {
    fn new() -> Self {
        Self {
            flags: 0,
            inflater: Decompress::new(false),
            output: vec![0; OUTPUT_LENGTH],
            scanner: Scanner::default(),
            stage: Stage::Fixed { seen: 0 },
        }
    }

    fn after(&self, step: usize) -> Stage {
        if step < 1 && self.flags & EXTRA_FLAG != 0 {
            Stage::ExtraLength { length: 0, seen: 0 }
        } else if step < 2 && self.flags & NAME_FLAG != 0 {
            Stage::Name { seen: 0 }
        } else if step < 3 && self.flags & COMMENT_FLAG != 0 {
            Stage::Comment { seen: 0 }
        } else if step < 4 && self.flags & CHECKSUM_FLAG != 0 {
            Stage::Checksum { remaining: 2 }
        } else {
            Stage::Body
        }
    }

    fn feed(&mut self, mut input: &[u8]) -> Progress {
        while let Some((&byte, rest)) = input.split_first() {
            self.stage = match self.stage {
                Stage::Body => return self.inflate(input),
                Stage::Checksum { remaining: 1 } => Stage::Body,
                Stage::Checksum { remaining } => Stage::Checksum {
                    remaining: remaining - 1,
                },
                Stage::Comment { .. } if byte == 0 => self.after(3),
                Stage::Comment { seen } if seen < MAXIMUM_TEXT_LENGTH => {
                    Stage::Comment { seen: seen + 1 }
                }
                Stage::Comment { .. } => return Progress::Failed,
                Stage::Extra { remaining: 1 } => self.after(1),
                Stage::Extra { remaining } => Stage::Extra {
                    remaining: remaining - 1,
                },
                Stage::ExtraLength { length, seen: 0 } => Stage::ExtraLength {
                    length: length | usize::from(byte),
                    seen: 1,
                },
                Stage::ExtraLength { length, .. } => match length | usize::from(byte) << 8 {
                    0 => self.after(1),
                    remaining => Stage::Extra { remaining },
                },
                Stage::Fixed { seen } => {
                    let valid = match seen {
                        0..=2 => byte == SIGNATURE[seen],
                        3 => byte & RESERVED_FLAGS == 0,
                        _ => true,
                    };

                    if !valid {
                        return Progress::Failed;
                    }

                    if seen == 3 {
                        self.flags = byte;
                    }

                    if seen + 1 == FIXED_LENGTH {
                        self.after(0)
                    } else {
                        Stage::Fixed { seen: seen + 1 }
                    }
                }
                Stage::Name { .. } if byte == 0 => self.after(2),
                Stage::Name { seen } if seen < MAXIMUM_TEXT_LENGTH => {
                    Stage::Name { seen: seen + 1 }
                }
                Stage::Name { .. } => return Progress::Failed,
            };
            input = rest;
        }

        Progress::Active
    }

    fn inflate(&mut self, mut input: &[u8]) -> Progress {
        loop {
            let consumed_before = self.inflater.total_in();
            let produced_before = self.inflater.total_out();
            let Ok(status) =
                self.inflater
                    .decompress(input, &mut self.output, FlushDecompress::None)
            else {
                return Progress::Failed;
            };
            let consumed = (self.inflater.total_in() - consumed_before) as usize;
            let produced = (self.inflater.total_out() - produced_before) as usize;
            self.scanner.accept(&self.output[..produced]);
            input = &input[consumed..];

            if matches!(status, Status::StreamEnd) {
                return Progress::Finished;
            }

            let idle = consumed == 0 && produced == 0;
            let drained = input.is_empty() && produced < self.output.len();

            if idle || drained {
                return Progress::Active;
            }
        }
    }
}

#[derive(Default)]
pub struct Stream {
    carry: Vec<u8>,
    finished: Found,
    members: Vec<Member>,
    scanner: Scanner,
}

impl Stream {
    pub fn accept(&mut self, chunk: &[u8]) {
        self.scanner.accept(chunk);
        let mut index = 0;

        while index < self.members.len() {
            if matches!(self.members[index].feed(chunk), Progress::Active) {
                index += 1;
            } else {
                let member = self.members.swap_remove(index);
                self.finished.extend(member.scanner.found());
            }
        }

        let carry = std::mem::take(&mut self.carry);

        for start in 0..carry.len() {
            let edge: Vec<u8> = carry[start..]
                .iter()
                .chain(chunk.iter().take(SIGNATURE.len()))
                .copied()
                .collect();

            if edge.starts_with(&SIGNATURE) {
                self.begin(&carry[start..], chunk);
            }
        }

        let mut start = 0;

        while let Some(offset) = chunk[start..].iter().position(|&byte| byte == SIGNATURE[0]) {
            let position = start + offset;

            if chunk[position..].starts_with(&SIGNATURE) {
                self.begin(&[], &chunk[position..]);
            }

            start = position + 1;
        }

        let mut tail = carry;
        tail.extend_from_slice(&chunk[chunk.len().saturating_sub(SIGNATURE.len() - 1)..]);
        let excess = tail.len().saturating_sub(SIGNATURE.len() - 1);
        tail.drain(..excess);
        self.carry = tail;
    }

    fn begin(&mut self, head: &[u8], rest: &[u8]) {
        if self.members.len() >= MAXIMUM_MEMBERS {
            return;
        }

        let mut member = Member::new();

        for part in [head, rest] {
            match member.feed(part) {
                Progress::Active => {}
                Progress::Failed | Progress::Finished => {
                    self.finished.extend(member.scanner.found());
                    return;
                }
            }
        }

        self.members.push(member);
    }

    pub fn found(self) -> Found {
        let mut found = self.scanner.found();
        found.extend(self.finished);

        for member in self.members {
            found.extend(member.scanner.found());
        }

        found
    }
}
