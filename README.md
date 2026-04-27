# Metrics Experiments

Experimental metrics collection system with a PromQL-facing architecture, an
object-store-backed ingest log, and Parquet-backed metric storage.

## Rust Workspace

The first implementation work is focused on the append log.

Workspace crates:

- `metrics-log-core`: provider-neutral log types, segment codec, and storage
  interfaces.
- `metrics-log-fs`: filesystem-backed implementation of the object and manifest
  stores for local development.
- `metrics-log-cli`: single binary that wires the core log engine to an adapter.

The intent is to keep cloud-provider integrations behind tight, shallow
interfaces so the same binary can run against different storage providers.

## Local Log Demo

```sh
cargo run -p metrics-log-cli -- --root /tmp/metrics-log-demo create-topic metrics_raw 1
cargo run -p metrics-log-cli -- --root /tmp/metrics-log-demo produce metrics_raw 0 series_a sample_1
cargo run -p metrics-log-cli -- --root /tmp/metrics-log-demo fetch metrics_raw 0 0 10
```

