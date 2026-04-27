use metrics_log_core::{
    LogError, ManifestStore, ObjectStore, PartitionState, Result, SegmentMetadata, TopicConfig,
};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct FsObjectStore {
    root: PathBuf,
}

impl FsObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path_for(&self, key: &str) -> PathBuf {
        self.root.join("objects").join(key)
    }
}

impl ObjectStore for FsObjectStore {
    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let path = self.path_for(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| match error.kind() {
                std::io::ErrorKind::AlreadyExists => {
                    LogError::Conflict(format!("object already exists: {key}"))
                }
                _ => LogError::Io(error),
            })?;
        file.write_all(bytes)?;
        file.sync_all()?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        fs::read(self.path_for(key)).map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => LogError::NotFound(format!("object not found: {key}")),
            _ => LogError::Io(error),
        })
    }

    fn get_range(&self, key: &str, offset: u64, length: usize) -> Result<Vec<u8>> {
        let mut file = fs::File::open(self.path_for(key)).map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => LogError::NotFound(format!("object not found: {key}")),
            _ => LogError::Io(error),
        })?;
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = vec![0; length];
        let read = file.read(&mut bytes)?;
        bytes.truncate(read);
        Ok(bytes)
    }
}

#[derive(Clone, Debug)]
pub struct FsManifestStore {
    root: PathBuf,
}

impl FsManifestStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn metadata_root(&self) -> PathBuf {
        self.root.join("metadata")
    }

    fn topic_dir(&self, topic: &str) -> PathBuf {
        self.metadata_root().join("topics").join(topic)
    }

    fn topic_config_path(&self, topic: &str) -> PathBuf {
        self.topic_dir(topic).join("topic.txt")
    }

    fn partition_path(&self, topic: &str, partition_id: u32) -> PathBuf {
        self.topic_dir(topic)
            .join("partitions")
            .join(format!("{partition_id:06}.txt"))
    }

    fn segments_path(&self, topic: &str, partition_id: u32) -> PathBuf {
        self.topic_dir(topic)
            .join("segments")
            .join(format!("{partition_id:06}.txt"))
    }
}

impl ManifestStore for FsManifestStore {
    fn create_topic(&self, config: TopicConfig) -> Result<()> {
        validate_name(&config.name)?;
        let topic_dir = self.topic_dir(&config.name);
        fs::create_dir_all(topic_dir.join("partitions"))?;
        fs::create_dir_all(topic_dir.join("segments"))?;

        let config_path = self.topic_config_path(&config.name);
        write_new_file(
            &config_path,
            format!("{}|{}\n", config.name, config.partitions).as_bytes(),
        )?;

        for partition_id in 0..config.partitions {
            let state = PartitionState {
                topic: config.name.clone(),
                partition_id,
                log_start_offset: 0,
                high_watermark: 0,
                next_offset: 0,
            };
            write_partition_state(&self.partition_path(&config.name, partition_id), &state)?;
            write_new_file(&self.segments_path(&config.name, partition_id), b"")?;
        }

        Ok(())
    }

    fn topic(&self, topic: &str) -> Result<TopicConfig> {
        let text = read_to_string(&self.topic_config_path(topic))?;
        let fields: Vec<&str> = text.trim_end().split('|').collect();
        if fields.len() != 2 {
            return Err(LogError::Store(format!(
                "invalid topic metadata for {topic}"
            )));
        }

        Ok(TopicConfig {
            name: fields[0].to_string(),
            partitions: parse_u32(fields[1], "partition count")?,
        })
    }

    fn partition_state(&self, topic: &str, partition_id: u32) -> Result<PartitionState> {
        read_partition_state(&self.partition_path(topic, partition_id))
    }

    fn commit_segment(
        &self,
        expected: &PartitionState,
        next: &PartitionState,
        segment: SegmentMetadata,
    ) -> Result<()> {
        let path = self.partition_path(&expected.topic, expected.partition_id);
        let current = read_partition_state(&path)?;
        if &current != expected {
            return Err(LogError::Conflict(format!(
                "partition state changed for {}/{}",
                expected.topic, expected.partition_id
            )));
        }

        append_segment_to_file(
            &self.segments_path(&segment.topic, segment.partition_id),
            &segment,
        )?;
        write_partition_state(&path, next)?;
        Ok(())
    }

    fn find_segment(&self, topic: &str, partition_id: u32, offset: u64) -> Result<SegmentMetadata> {
        let text = read_to_string(&self.segments_path(topic, partition_id))?;
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let segment = parse_segment(line)?;
            if segment.start_offset <= offset && offset <= segment.end_offset {
                return Ok(segment);
            }
        }

        Err(LogError::NotFound(format!(
            "no segment for {topic}/{partition_id} at offset {offset}"
        )))
    }
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(LogError::InvalidArgument(format!(
            "invalid name {name:?}; use ascii letters, numbers, '-' or '_'"
        )));
    }
    Ok(())
}

fn read_to_string(path: &Path) -> Result<String> {
    fs::read_to_string(path).map_err(|error| match error.kind() {
        std::io::ErrorKind::NotFound => {
            LogError::NotFound(format!("metadata file not found: {}", path.display()))
        }
        _ => LogError::Io(error),
    })
}

fn write_new_file(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| match error.kind() {
            std::io::ErrorKind::AlreadyExists => {
                LogError::Conflict(format!("file already exists: {}", path.display()))
            }
            _ => LogError::Io(error),
        })?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn write_partition_state(path: &Path, state: &PartitionState) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temp = path.with_extension("tmp");
    fs::write(
        &temp,
        format!(
            "{}|{}|{}|{}|{}\n",
            state.topic,
            state.partition_id,
            state.log_start_offset,
            state.high_watermark,
            state.next_offset
        ),
    )?;
    fs::rename(temp, path)?;
    Ok(())
}

fn read_partition_state(path: &Path) -> Result<PartitionState> {
    let text = read_to_string(path)?;
    let fields: Vec<&str> = text.trim_end().split('|').collect();
    if fields.len() != 5 {
        return Err(LogError::Store(format!(
            "invalid partition metadata: {}",
            path.display()
        )));
    }

    Ok(PartitionState {
        topic: fields[0].to_string(),
        partition_id: parse_u32(fields[1], "partition id")?,
        log_start_offset: parse_u64(fields[2], "log start offset")?,
        high_watermark: parse_u64(fields[3], "high watermark")?,
        next_offset: parse_u64(fields[4], "next offset")?,
    })
}

fn append_segment_to_file(path: &Path, segment: &SegmentMetadata) -> Result<()> {
    let mut file = OpenOptions::new().append(true).open(path)?;
    writeln!(
        file,
        "{}|{}|{}|{}|{}|{}|{}|{}",
        segment.topic,
        segment.partition_id,
        segment.start_offset,
        segment.end_offset,
        segment.object_key,
        segment.index_key,
        segment.size_bytes,
        segment.record_count
    )?;
    file.sync_all()?;
    Ok(())
}

fn parse_segment(line: &str) -> Result<SegmentMetadata> {
    let fields: Vec<&str> = line.split('|').collect();
    if fields.len() != 8 {
        return Err(LogError::Store("invalid segment metadata".to_string()));
    }

    Ok(SegmentMetadata {
        topic: fields[0].to_string(),
        partition_id: parse_u32(fields[1], "partition id")?,
        start_offset: parse_u64(fields[2], "start offset")?,
        end_offset: parse_u64(fields[3], "end offset")?,
        object_key: fields[4].to_string(),
        index_key: fields[5].to_string(),
        size_bytes: parse_u64(fields[6], "size bytes")?,
        record_count: parse_u64(fields[7], "record count")?,
    })
}

fn parse_u32(value: &str, field: &str) -> Result<u32> {
    value
        .parse()
        .map_err(|_| LogError::Store(format!("invalid {field}: {value}")))
}

fn parse_u64(value: &str, field: &str) -> Result<u64> {
    value
        .parse()
        .map_err(|_| LogError::Store(format!("invalid {field}: {value}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_log_core::{FetchRequest, LogEngine, ProduceRequest, Record};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn filesystem_adapter_produces_and_fetches_records() {
        let root = temp_root();
        let engine = LogEngine::new(FsObjectStore::new(&root), FsManifestStore::new(&root));

        engine.create_topic("metrics_raw", 1).unwrap();
        let produced = engine
            .produce(ProduceRequest {
                topic: "metrics_raw".to_string(),
                partition_id: 0,
                records: vec![
                    Record::new("series-a", "sample-1"),
                    Record::new("series-a", "sample-2"),
                ],
            })
            .unwrap();

        assert_eq!(produced.base_offset, 0);
        assert_eq!(produced.last_offset, 1);

        let fetched = engine
            .fetch(FetchRequest {
                topic: "metrics_raw".to_string(),
                partition_id: 0,
                offset: 1,
                max_records: 10,
            })
            .unwrap();

        assert_eq!(fetched.records.len(), 1);
        assert_eq!(fetched.records[0].offset, 1);
        assert_eq!(fetched.records[0].key, b"series-a");
        assert_eq!(fetched.records[0].value, b"sample-2");

        fs::remove_dir_all(root).unwrap();
    }

    fn temp_root() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "metrics-log-fs-test-{}-{nanos}",
            std::process::id()
        ))
    }
}
