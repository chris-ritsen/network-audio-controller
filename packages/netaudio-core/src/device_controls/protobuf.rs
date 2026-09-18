//! Bounded protobuf primitives. Unknown fields remain available in the raw record.
#[derive(Debug, Clone)]
pub enum Value<'a> {
    Varint(u64),
    Bytes(&'a [u8]),
    Fixed,
}
#[derive(Debug, Clone)]
pub struct Field<'a> {
    pub number: u32,
    pub value: Value<'a>,
}
pub type Message<'a> = Vec<Field<'a>>;
fn varint(data: &[u8], pos: &mut usize) -> Option<u64> {
    let mut value = 0;
    for shift in (0..70).step_by(7) {
        let byte = *data.get(*pos)?;
        *pos += 1;
        if shift == 63 && byte > 1 {
            return None;
        }
        value |= u64::from(byte & 127) << shift;
        if byte < 128 {
            return Some(value);
        }
    }
    None
}
pub fn parse(data: &[u8]) -> Option<Message<'_>> {
    let mut pos = 0;
    let mut fields = Vec::new();
    while pos < data.len() {
        let key = varint(data, &mut pos)?;
        let number = u32::try_from(key >> 3).ok()?;
        if number == 0 || number > 0x1fff_ffff {
            return None;
        }
        let value = match key & 7 {
            0 => Value::Varint(varint(data, &mut pos)?),
            2 => {
                let len = usize::try_from(varint(data, &mut pos)?).ok()?;
                let end = pos.checked_add(len)?;
                let bytes = data.get(pos..end)?;
                pos = end;
                Value::Bytes(bytes)
            }
            1 | 5 => {
                let end = pos.checked_add(if key & 7 == 1 { 8 } else { 4 })?;
                data.get(pos..end)?;
                pos = end;
                Value::Fixed
            }
            _ => return None,
        };
        fields.push(Field { number, value });
    }
    Some(fields)
}
pub fn uint(m: &Message<'_>, n: u32) -> Option<u32> {
    let mut result = 0;
    for f in m.iter().filter(|f| f.number == n) {
        let Value::Varint(v) = f.value else {
            return None;
        };
        result = u32::try_from(v).ok()?;
    }
    Some(result)
}
pub fn bytes<'a>(m: &Message<'a>, n: u32) -> Option<Option<&'a [u8]>> {
    let mut result = None;
    for f in m.iter().filter(|f| f.number == n) {
        let Value::Bytes(v) = f.value else {
            return None;
        };
        result = Some(v);
    }
    Some(result)
}
pub fn nested<'a>(m: &Message<'a>, n: u32) -> Option<Option<Message<'a>>> {
    bytes(m, n)?.map(parse).transpose_option()
}
trait Transpose<T> {
    fn transpose_option(self) -> Option<Option<T>>;
}
impl<T> Transpose<T> for Option<Option<T>> {
    fn transpose_option(self) -> Option<Option<T>> {
        match self {
            None => Some(None),
            Some(v) => v.map(Some),
        }
    }
}
pub fn string(m: &Message<'_>, n: u32) -> Option<String> {
    Some(
        std::str::from_utf8(bytes(m, n)?.unwrap_or_default())
            .ok()?
            .to_owned(),
    )
}
pub fn repeated_uint(m: &Message<'_>, n: u32) -> Option<Vec<u32>> {
    let mut result = Vec::new();
    for f in m.iter().filter(|f| f.number == n) {
        match f.value {
            Value::Varint(v) => result.push(u32::try_from(v).ok()?),
            Value::Bytes(b) => {
                let mut p = 0;
                while p < b.len() {
                    result.push(u32::try_from(varint(b, &mut p)?).ok()?);
                }
            }
            _ => return None,
        }
    }
    Some(result)
}
pub fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 128 {
        out.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    out.push(value as u8);
}
pub fn put_uint(out: &mut Vec<u8>, n: u32, v: u32) {
    if v == 0 {
        return;
    }
    put_varint(out, u64::from(n) << 3);
    put_varint(out, u64::from(v));
}
pub fn put_bytes(out: &mut Vec<u8>, n: u32, v: &[u8]) {
    put_varint(out, (u64::from(n) << 3) | 2);
    put_varint(out, v.len() as u64);
    out.extend_from_slice(v);
}
pub fn message(n: u32, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    put_bytes(&mut out, n, value);
    out
}
pub fn scalar(n: u32, value: u32) -> Vec<u8> {
    let mut out = Vec::new();
    put_uint(&mut out, n, value);
    out
}

pub fn put_uint_explicit(out: &mut Vec<u8>, n: u32, v: u32) {
    put_varint(out, u64::from(n) << 3);
    put_varint(out, u64::from(v));
}
