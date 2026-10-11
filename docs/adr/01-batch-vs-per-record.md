# ADR-01: Batch-Level Processing vs Per-Record

## Status
Accepted

## Context
The proxy must process Kafka produce requests. We have two options:
1. **Per-record processing**: Decode each record, apply filters, generate event IDs, re-encode
2. **Batch-level processing**: Keep records in batch form, apply filters at batch level, only decode when necessary

## Decision
**Batch-level processing by default**. Per-record processing only when filters explicitly require it (configured per-topic).

## Consequences
### Positive
- Lower latency overhead (~100µs vs ~1ms per batch)
- Better throughput (less decode/encode overhead)
- Natural alignment with Kafka's batch semantics
- Zero-copy for passthrough topics

### Negative
- Filters must be batch-aware
- More complex filter API
- Event ID generation must work on batches

## Implementation
- `FilterChain::process(FilterBatch)` receives batch with topic, partition, records
- `FilterBatch.per_record` flag controls per-record mode
- `topic_config.per_record_processing` enables per-record for specific topics