use crate::types::{PartitionState, SegmentMetadata, TopicConfig};
use crate::Result;

pub trait ObjectStore: Send + Sync {
    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<()>;
    fn get(&self, key: &str) -> Result<Vec<u8>>;
    fn get_range(&self, key: &str, offset: u64, length: usize) -> Result<Vec<u8>>;
}

pub trait ManifestStore: Send + Sync {
    fn create_topic(&self, config: TopicConfig) -> Result<()>;
    fn topic(&self, topic: &str) -> Result<TopicConfig>;
    fn partition_state(&self, topic: &str, partition_id: u32) -> Result<PartitionState>;
    fn commit_segment(
        &self,
        expected: &PartitionState,
        next: &PartitionState,
        segment: SegmentMetadata,
    ) -> Result<()>;
    fn find_segment(&self, topic: &str, partition_id: u32, offset: u64) -> Result<SegmentMetadata>;
}
