use crate::error::{LogError, Result};
use crate::types::{FetchedRecord, Record};

const MAGIC: &[u8; 8] = b"mlogseg1";

pub fn encode_segment(base_offset: u64, records: &[Record]) -> Result<Vec<u8>> {
    if records.is_empty() {
        return Err(LogError::InvalidArgument(
            "cannot encode an empty segment".to_string(),
        ));
    }

    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&base_offset.to_le_bytes());
    bytes.extend_from_slice(&(records.len() as u32).to_le_bytes());

    for record in records {
        checked_len(record.key.len(), "record key")?;
        checked_len(record.value.len(), "record value")?;
        bytes.extend_from_slice(&(record.key.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&(record.value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&record.key);
        bytes.extend_from_slice(&record.value);
    }

    Ok(bytes)
}

pub fn decode_segment(bytes: &[u8]) -> Result<Vec<FetchedRecord>> {
    let mut cursor = Cursor::new(bytes);
    cursor.expect(MAGIC)?;
    let base_offset = cursor.u64()?;
    let count = cursor.u32()? as usize;
    let mut records = Vec::with_capacity(count);

    for index in 0..count {
        let key_len = cursor.u32()? as usize;
        let value_len = cursor.u32()? as usize;
        let key = cursor.bytes(key_len)?.to_vec();
        let value = cursor.bytes(value_len)?.to_vec();
        records.push(FetchedRecord {
            offset: base_offset + index as u64,
            key,
            value,
        });
    }

    if cursor.remaining() != 0 {
        return Err(LogError::CorruptSegment(
            "segment has trailing bytes".to_string(),
        ));
    }

    Ok(records)
}

pub fn encode_sparse_index(base_offset: u64, byte_position: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"mlogidx1");
    bytes.extend_from_slice(&base_offset.to_le_bytes());
    bytes.extend_from_slice(&byte_position.to_le_bytes());
    bytes
}

fn checked_len(len: usize, field: &str) -> Result<()> {
    if len > u32::MAX as usize {
        return Err(LogError::InvalidArgument(format!("{field} is too large")));
    }
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn expect(&mut self, expected: &[u8]) -> Result<()> {
        let actual = self.bytes(expected.len())?;
        if actual != expected {
            return Err(LogError::CorruptSegment("bad segment magic".to_string()));
        }
        Ok(())
    }

    fn u32(&mut self) -> Result<u32> {
        let bytes = self.bytes(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().expect("slice length")))
    }

    fn u64(&mut self) -> Result<u64> {
        let bytes = self.bytes(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().expect("slice length")))
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| LogError::CorruptSegment("segment cursor overflowed".to_string()))?;
        if end > self.bytes.len() {
            return Err(LogError::CorruptSegment(
                "segment ended unexpectedly".to_string(),
            ));
        }

        let chunk = &self.bytes[self.position..end];
        self.position = end;
        Ok(chunk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_round_trips_records() {
        let encoded =
            encode_segment(42, &[Record::new("a", "first"), Record::new("b", "second")]).unwrap();

        let decoded = decode_segment(&encoded).unwrap();

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].offset, 42);
        assert_eq!(decoded[0].key, b"a");
        assert_eq!(decoded[0].value, b"first");
        assert_eq!(decoded[1].offset, 43);
        assert_eq!(decoded[1].key, b"b");
        assert_eq!(decoded[1].value, b"second");
    }
}
