# ADR 0001: Prefer Stateless Compute and Object-Store-Backed State

- Status: Accepted
- Date: 2026-04-27

## Context

The metrics collection system needs to ingest high-volume metric streams, expose
a PromQL-compatible query interface, and store durable data in cloud object
storage using formats such as log segments and Parquet blocks.

The system should be portable across cloud providers and should avoid depending
on heavyweight provider-specific services where a small abstraction over object
storage is sufficient. Compute nodes should be easy to scale, replace, and
recover without preserving local disks or process-local durable state.

Cloud object stores provide durable, low-cost, widely available storage, but they
do not provide all the semantics of a database or local append log. The system
therefore needs to be explicit about which state belongs in object storage, which
small pieces of coordination or metadata may require stronger semantics, and how
cloud-provider features are accessed.

## Decision

We will design the system to be as stateless as practical.

Durable system state should live primarily in object storage. This includes:

- Raw append-log segments.
- Sparse log indexes.
- Metric Parquet blocks.
- Compacted blocks.
- Downsampled blocks.
- Durable manifests where object-store semantics are sufficient.

Compute services should be horizontally scalable and disposable. This includes:

- Ingest gateways.
- Log produce and fetch brokers.
- Parquet writers.
- Compaction workers.
- Query API servers.
- Query workers.
- Admin and control-plane services.

When object storage alone is not sufficient, the system may use a minimal
metadata or coordination store for narrow responsibilities such as:

- Object manifests.
- Writer leases.
- High-watermark tracking.
- Consumer offsets.
- Series and label indexes.
- Compaction state.

Cloud-provider access must be hidden behind tight, shallow interfaces. The core
system should depend on small internal interfaces rather than provider SDKs
spread throughout the codebase.

Examples of these interfaces include:

```text
ObjectStore
ManifestStore
LeaseStore
CatalogStore
Clock
```

Each interface should expose only the operations the system actually needs. The
interfaces should preserve the semantics required by the architecture rather
than mirroring all capabilities of any specific provider.

## Rationale

Keeping compute stateless improves:

- Failure recovery.
- Horizontal scaling.
- Operational simplicity.
- Rolling deploys.
- Cloud portability.
- Local and integration testing.

Keeping durable bytes in object storage improves:

- Cost efficiency.
- Data durability.
- Retention management.
- Disaster recovery.
- Reprocessing from raw inputs.
- Separation of compute and storage.

Using only a minimal provider feature set reduces lock-in and keeps the design
focused on the system's own storage and query semantics. Small provider
interfaces also make it easier to add local implementations for tests and to
support multiple clouds later.

## Consequences

This decision means the system should not rely on local broker disks as the
source of truth.

It also means the system will accept some complexity and latency tradeoffs:

- Object stores are not efficient for tiny appends.
- Fresh data may have seconds of visibility latency.
- Metadata lookups are required to avoid expensive object listing and scanning.
- Compaction is required to control small files.
- Strong coordination must be isolated to a small metadata or lease layer.

The architecture should optimize for durable, replayable, cloud-portable storage
rather than Kafka-like single-digit millisecond latency.

## Implementation Guidelines

- Treat object storage as the durable byte store.
- Write immutable objects whenever possible.
- Commit visibility through manifests or metadata records.
- Avoid overwriting objects as part of the normal write path.
- Keep local process state as a cache or short-lived buffer only.
- Make caches disposable and rebuildable.
- Keep provider SDK usage inside infrastructure adapters.
- Keep cloud interfaces small and semantic.
- Prefer explicit consistency and lease semantics over implicit assumptions.
- Test core logic against local in-memory or filesystem-backed adapters.

## Rejected Alternatives

### Stateful Brokers with Local Durable Disks

This would provide lower-latency append and fetch behavior, but it would make
broker placement, recovery, replication, and disk lifecycle central operational
concerns. That conflicts with the goal of mostly stateless compute.

### Deep Provider Integration

Using many cloud-specific services directly could accelerate a single-provider
implementation, but it would make the core architecture harder to reason about,
test, and port. Provider-specific code should be isolated behind small adapters.

### Object Storage Without Any Metadata Layer

Using object storage alone is attractive, but PromQL queries, log fetches,
leases, consumer offsets, and compaction state all need efficient discovery and
coordination. A narrow metadata layer is allowed where object storage alone is
too blunt.

