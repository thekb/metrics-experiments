use crate::error::{LogError, Result};
use crate::ports::{ManifestStore, ObjectStore};
use crate::segment::{decode_segment, encode_segment, encode_sparse_index};
use crate::types::{
    FetchRequest, FetchResponse, ProduceRequest, ProduceResponse, SegmentMetadata, TopicConfig,
};

pub struct LogEngine<O, M> {
    objects: O,
    manifests: M,
}

impl<O, M> LogEngine<O, M>
where
    O: ObjectStore,
    M: ManifestStore,
{
    pub fn new(objects: O, manifests: M) -> Self {
        Self { objects, manifests }
    }

    pub fn create_topic(&self, name: impl Into<String>, partitions: u32) -> Result<()> {
        if partitions == 0 {
            return Err(LogError::InvalidArgument(
                "topic must have at least one partition".to_string(),
            ));
        }

        self.manifests.create_topic(TopicConfig {
            name: name.into(),
            partitions,
        })
    }

    pub fn produce(&self, request: ProduceRequest) -> Result<ProduceResponse> {
        if request.records.is_empty() {
            return Err(LogError::InvalidArgument(
                "produce request must include at least one record".to_string(),
            ));
        }

        let topic = self.manifests.topic(&request.topic)?;
        if request.partition_id >= topic.partitions {
            return Err(LogError::InvalidArgument(format!(
                "partition {} is outside topic {} partition count {}",
                request.partition_id, request.topic, topic.partitions
            )));
        }

        let current = self
            .manifests
            .partition_state(&request.topic, request.partition_id)?;
        let base_offset = current.next_offset;
        let last_offset = base_offset + request.records.len() as u64 - 1;
        let object_key = segment_key(&request.topic, request.partition_id, base_offset);
        let index_key = index_key(&request.topic, request.partition_id, base_offset);
        let segment_bytes = encode_segment(base_offset, &request.records)?;
        let index_bytes = encode_sparse_index(base_offset, 0);

        self.objects.put_if_absent(&object_key, &segment_bytes)?;
        self.objects.put_if_absent(&index_key, &index_bytes)?;

        let segment = SegmentMetadata {
            topic: request.topic.clone(),
            partition_id: request.partition_id,
            start_offset: base_offset,
            end_offset: last_offset,
            object_key,
            index_key,
            size_bytes: segment_bytes.len() as u64,
            record_count: request.records.len() as u64,
        };

        let next = crate::types::PartitionState {
            high_watermark: last_offset + 1,
            next_offset: last_offset + 1,
            ..current.clone()
        };

        self.manifests.commit_segment(&current, &next, segment)?;

        Ok(ProduceResponse {
            topic: request.topic,
            partition_id: request.partition_id,
            base_offset,
            last_offset,
        })
    }

    pub fn fetch(&self, request: FetchRequest) -> Result<FetchResponse> {
        if request.max_records == 0 {
            return Err(LogError::InvalidArgument(
                "fetch max_records must be greater than zero".to_string(),
            ));
        }

        let segment =
            self.manifests
                .find_segment(&request.topic, request.partition_id, request.offset)?;
        let bytes = self.objects.get(&segment.object_key)?;
        let records = decode_segment(&bytes)?
            .into_iter()
            .filter(|record| record.offset >= request.offset)
            .take(request.max_records)
            .collect();

        Ok(FetchResponse {
            topic: request.topic,
            partition_id: request.partition_id,
            records,
        })
    }
}

fn segment_key(topic: &str, partition_id: u32, base_offset: u64) -> String {
    format!("logs/topic={topic}/partition={partition_id:06}/segment-{base_offset:020}.log")
}

fn index_key(topic: &str, partition_id: u32, base_offset: u64) -> String {
    format!("logs/topic={topic}/partition={partition_id:06}/index-{base_offset:020}.idx")
}
