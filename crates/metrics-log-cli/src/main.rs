use metrics_log_core::{FetchRequest, LogEngine, ProduceRequest, Record, Result};
use metrics_log_fs::{FsManifestStore, FsObjectStore};
use std::env;
use std::path::PathBuf;

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let mut args = env::args().skip(1);
    let root = match args.next().as_deref() {
        Some("--root") => PathBuf::from(required_arg(&mut args, "root")?),
        _ => PathBuf::from(".metrics-log"),
    };

    let command = required_arg(&mut args, "command")?;
    let engine = LogEngine::new(FsObjectStore::new(&root), FsManifestStore::new(&root));

    match command.as_str() {
        "create-topic" => {
            let topic = required_arg(&mut args, "topic")?;
            let partitions = parse_u32(&required_arg(&mut args, "partitions")?, "partitions")?;
            engine.create_topic(topic.clone(), partitions)?;
            println!("created topic {topic} with {partitions} partition(s)");
        }
        "produce" => {
            let topic = required_arg(&mut args, "topic")?;
            let partition_id = parse_u32(&required_arg(&mut args, "partition")?, "partition")?;
            let key = required_arg(&mut args, "key")?;
            let value = required_arg(&mut args, "value")?;
            let response = engine.produce(ProduceRequest {
                topic,
                partition_id,
                records: vec![Record::new(key, value)],
            })?;
            println!(
                "{} {} {} {}",
                response.topic, response.partition_id, response.base_offset, response.last_offset
            );
        }
        "fetch" => {
            let topic = required_arg(&mut args, "topic")?;
            let partition_id = parse_u32(&required_arg(&mut args, "partition")?, "partition")?;
            let offset = parse_u64(&required_arg(&mut args, "offset")?, "offset")?;
            let max_records = parse_usize(&required_arg(&mut args, "max_records")?, "max_records")?;
            let response = engine.fetch(FetchRequest {
                topic,
                partition_id,
                offset,
                max_records,
            })?;
            for record in response.records {
                println!(
                    "{}\t{}\t{}",
                    record.offset,
                    String::from_utf8_lossy(&record.key),
                    String::from_utf8_lossy(&record.value)
                );
            }
        }
        "help" | "--help" | "-h" => print_help(),
        other => {
            print_help();
            return Err(metrics_log_core::LogError::InvalidArgument(format!(
                "unknown command: {other}"
            )));
        }
    }

    Ok(())
}

fn print_help() {
    println!(
        "usage:
  metrics-log [--root PATH] create-topic TOPIC PARTITIONS
  metrics-log [--root PATH] produce TOPIC PARTITION KEY VALUE
  metrics-log [--root PATH] fetch TOPIC PARTITION OFFSET MAX_RECORDS"
    );
}

fn required_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String> {
    args.next().ok_or_else(|| {
        metrics_log_core::LogError::InvalidArgument(format!("missing argument: {name}"))
    })
}

fn parse_u32(value: &str, field: &str) -> Result<u32> {
    value.parse().map_err(|_| {
        metrics_log_core::LogError::InvalidArgument(format!("invalid {field}: {value}"))
    })
}

fn parse_u64(value: &str, field: &str) -> Result<u64> {
    value.parse().map_err(|_| {
        metrics_log_core::LogError::InvalidArgument(format!("invalid {field}: {value}"))
    })
}

fn parse_usize(value: &str, field: &str) -> Result<usize> {
    value.parse().map_err(|_| {
        metrics_log_core::LogError::InvalidArgument(format!("invalid {field}: {value}"))
    })
}
