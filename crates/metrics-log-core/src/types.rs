#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicConfig {
    pub name: String,
    pub partitions: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionState {
    pub topic: String,
    pub partition_id: u32,
    pub log_start_offset: u64,
    pub high_watermark: u64,
    pub next_offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentMetadata {
    pub topic: String,
    pub partition_id: u32,
    pub start_offset: u64,
    pub end_offset: u64,
    pub object_key: String,
    pub index_key: String,
    pub size_bytes: u64,
    pub record_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Record {
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProduceRequest {
    pub topic: String,
    pub partition_id: u32,
    pub records: Vec<Record>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProduceResponse {
    pub topic: String,
    pub partition_id: u32,
    pub base_offset: u64,
    pub last_offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchRequest {
    pub topic: String,
    pub partition_id: u32,
    pub offset: u64,
    pub max_records: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchResponse {
    pub topic: String,
    pub partition_id: u32,
    pub records: Vec<FetchedRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchedRecord {
    pub offset: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
