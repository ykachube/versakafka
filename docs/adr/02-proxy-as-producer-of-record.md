# ADR-02: Proxy as Producer of Record

## Status
Accepted

## Context
Who acknowledges the produce request to the client?
1. **Proxy acknowledges** (producer of record) - Proxy takes ownership, forwards to Kafka
2. **Kafka acknowledges** - Proxy is transparent, client talks directly to Kafka through proxy

## Decision
**Proxy acts as producer of record**. The proxy:
1. Receives produce request from client
2. Acknowledges immediately after local processing (WAL write, event ID assignment)
3. Forwards to downstream channels asynchronously
4. Reconciles any gaps via reconciler

## Consequences
### Positive
- Lower client latency (no waiting for Kafka acks)
- Producer resilience (client can't distinguish proxy from broker)
- Enables spill-to-disk without client timeout
- Supports multi-channel fan-out with different ack policies

### Negative
- Proxy must durably persist before ack (WAL)
- More complex failure handling
- Requires idempotent client or deduplication

## Implementation
- `ProduceRequest` handled by `Connection::handle_produce()`
- Writes to WAL segment
- Assigns event ID if missing
- Returns `ProduceResponse` with offset = WAL offset
- Background tasks fan out to channels