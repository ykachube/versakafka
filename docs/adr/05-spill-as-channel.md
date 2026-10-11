# ADR-05: Spill as Channel with Reconciliation

## Status
Accepted

## Context
How to handle downstream Kafka unavailability?
1. **Buffer in memory** - Simple, but limited by RAM
2. **Spill to local disk** - Larger buffer, but single-node
3. **Spill to object storage (S3)** - Unlimited, multi-node, but adds latency
4. **Spill as a channel** - Treat spill as first-class channel with reconciler

## Decision
**Spill as a channel with reconciliation**.

### Architecture
- Spill implements `Channel` trait (like Kafka, Kinesis)
- WAL → Spill uploader → S3/MinIO
- Manifest tracks spilled segments
- Reconciler periodically checks channel consistency
- Auto-replays gaps when channel recovers

### Ack Policies
- `AllOrSpill`: All channels ack, or failed channel durably spilled
- Spill channel always succeeds (local WAL write)
- Downstream channels async

## Consequences
### Positive
- Unified channel abstraction
- Unlimited spill capacity (S3)
- Automatic gap recovery
- Works with multi-region

### Negative
- Added latency for spill path
- S3 costs for long-term spill
- Reconciler complexity

## Implementation
- `SpillChannel` implements `Channel` trait
- `Uploader` batches WAL segments to S3 multipart
- `ManifestManager` tracks segment metadata
- `Replayer` reads from S3, sends to target channel
- `ReconcilerChecker` compares channel offsets
- `GapReplayer` replays missing segments