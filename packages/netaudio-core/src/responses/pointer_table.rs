use super::*;

pub(super) fn parse_pointer_table_page<R>(
    response: &[u8],
    table_offset: usize,
    record_count: u8,
    record_length: impl Fn(&R) -> usize,
    parse_record: impl Fn(&[u8], u16, usize) -> Option<R>,
) -> Option<Vec<R>> {
    let table_end = table_offset.checked_add(usize::from(record_count).checked_mul(2)?)?;
    response.get(table_offset..table_end)?;

    let mut record_pointers = Vec::with_capacity(usize::from(record_count));
    let mut seen_record_pointers = HashSet::with_capacity(usize::from(record_count));
    for index in 0..record_count {
        let pointer_offset = table_offset.checked_add(usize::from(index).checked_mul(2)?)?;
        let record_pointer = read_u16(response, pointer_offset)?;
        if usize::from(record_pointer) < table_end || !seen_record_pointers.insert(record_pointer) {
            return None;
        }
        record_pointers.push(record_pointer);
    }

    let mut records = Vec::with_capacity(usize::from(record_count));
    let mut record_ranges = Vec::with_capacity(usize::from(record_count));
    for record_pointer in record_pointers {
        let record = parse_record(response, record_pointer, table_end)?;
        let record_start = usize::from(record_pointer);
        let record_end = record_start.checked_add(record_length(&record))?;
        if record_end > response.len() {
            return None;
        }
        record_ranges.push((record_start, record_end));
        records.push(record);
    }
    record_ranges.sort_unstable();
    if record_ranges
        .windows(2)
        .any(|ranges| ranges[0].1 > ranges[1].0)
    {
        return None;
    }
    Some(records)
}
