# ADR-04: Local Breaker + ARC Regional Coordination

## Status
Accepted

## Context
Circuit breaking strategy for Kafka producer resilience:
1. **Local only** - Each proxy instance breaks independently
2. **Centralized** - Single breaker service (ZooKeeper/etcd)
3. **Hybrid** - Local adaptive + AWS ARC regional coordination

## Decision
**Hybrid: Local adaptive breaker + AWS ARC regional coordination**.

### Local Breaker (per broker/channel)
- Adaptive based on error rate, latency, timeouts
- Sliding window (configurable, default 60s)
- States: Closed → HalfOpen → Open
- Per-broker and per-channel instances

### ARC Integration (regional)
- Polls ARC endpoints for regional health
- Composition: `AnyOpen` (default) or `RegionalPriority`
- Fail-static: if ARC unreachable, use cached state

## Consequences
### Positive
- Fast local reaction (< 100ms)
- Regional coordination for AZ failures
- No single point of failure
- Works without ARC (local only mode)

### Negative
- Two-layer complexity
- Potential split-brain (local vs regional)
- ARC adds AWS dependency

## Implementation
- `LocalBreaker` with sliding window metrics
- `ArcClient` polling ARC endpoints
- `BreakerManager` composing both with configurable strategy
- Metrics exposed for both layers