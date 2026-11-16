# Chaos Scenario Implementation Plan

## Overview
Implement 30 chaos scenarios across 4 phases, each building on the previous. Each phase adds new failure injection capabilities to the test framework.

---

## Phase 1: Core Failure Injection (Week 1-2)
**Goal**: Basic chaos primitives - broker failure, network, latency, consumer lag

| # | Scenario | New Capability | Files to Modify |
|---|----------|----------------|-----------------|
| 1 | `broker-failure-primary` | Kill broker container | `fake_broker.rs`, `cluster_fixture.rs` |
| 2 | `network-partition` | Toxiproxy reset_peer | `toxiproxy-scenarios/`, `cluster_fixture.rs` |
| 3 | `slow-broker` | Toxiproxy latency/jitter | `toxiproxy-scenarios/`, `load_generator.rs` |
| 4 | `slow-consumer-lag` | Consumer pause injection | `fake_broker.rs`, `scenario_runner.rs` |

**Deliverables**:
- [ ] Toxiproxy client wrapper in Rust
- [ ] Container kill/restart via Docker API
- [ ] Consumer group pause/resume via admin API
- [ ] Baseline chaos test runner

**Exit Criteria**: All 4 scenarios run and pass invariants I1, I2, I5

---

## Phase 2: Storage & Channel Failures (Week 2-3)
**Goal**: S3, WAL, secondary channel, disk failures

| # | Scenario | New Capability | Files to Modify |
|---|----------|----------------|-----------------|
| 5 | `secondary-unavailable` | Secondary Kafka cluster down | `cluster_fixture.rs`, `kafka_channel.rs` |
| 6 | `s3-unavailable` | MinIO/ LocalStack S3 down | `toxiproxy-scenarios/`, `fake_s3.rs` |
| 7 | `disk-full` | FS failure injection | `fs_failures.rs`, `wal.rs` |
| 8 | `s3-multipart-upload-failures` | Partial S3 upload failures | `fake_s3.rs`, `spill/uploader.rs` |

**Deliverables**:
- [ ] Secondary Kafka cluster in docker-compose.test.yml
- [ ] FS failure layer (torn writes, fsync failures, disk full)
- [ ] S3 multipart upload failure injection
- [ ] Spill recovery verification

**Exit Criteria**: All 4 scenarios pass I1, I2, I5, I7

---

## Phase 3: Advanced Protocol & Cloud Failures (Week 3-4)
**Goal**: Transactions, metadata, TLS, auth, KMS, ARC

| # | Scenario | New Capability | Files to Modify |
|---|----------|----------------|-----------------|
| 9 | `transaction-timeout-exhaustion` | Txn ID exhaustion, timeout | `fake_broker.rs`, `produce.rs` |
| 10 | `metadata-cache-staleness` | Broker add/remove, partition reassign | `metadata.rs`, `cluster_fixture.rs` |
| 11 | `tls-certificate-expiration` | Cert rotation, expired cert | `tls.rs`, `cluster_fixture.rs` |
| 12 | `sasl-authentication-failures` | SASL auth errors, reauth | `fake_broker.rs`, `connection.rs` |
| 13 | `wal-disk-io-errors` | Torn writes, fsync failures | `fs_failures.rs`, `wal/reader.rs` |
| 14 | `kms-throttling` | KMS rate limiting | `toxiproxy-scenarios/`, `kms.rs` |
| 15 | `event-id-collision-deterministic` | EID collision, fallback | `eventid/modes.rs`, `checker.rs` |
| 16 | `breaker-flapping` | Breaker state thrashing | `breaker/local.rs`, `scenario_runner.rs` |

**Deliverables**:
- [ ] Transactional producer simulation
- [ ] Metadata cache with TTL and forced refresh
- [ ] TLS cert rotation via container reload
- [ ] SASL auth error injection
- [ ] KMS throttle via LocalStack config
- [ ] Breaker flapping detection metrics

**Exit Criteria**: All 8 scenarios pass relevant invariants

---

## Phase 4: Complex Multi-Failure & Edge Cases (Week 4-5)
**Goal**: Rebalances, compaction, replication, resources, clock skew

| # | Scenario | New Capability | Files to Modify |
|---|----------|----------------|-----------------|
| 17 | `rebalance-during-produce` | Partition reassignment during produce | `cluster_fixture.rs`, `produce.rs` |
| 18 | `log-compaction-offset-shift` | Log compaction offset translation | `fake_broker.rs`, `checker.rs` |
| 19 | `mirror-maker-replication-lag` | MM lag injection | `kafka_channel.rs`, `toxiproxy-scenarios/` |
| 20 | `consumer-group-rebalance-storm` | Rapid join/leave | `fake_broker.rs`, `scenario_runner.rs` |
| 21 | `fetch-session-timeout` | Fetch session expiry | `fake_broker.rs`, `scenario_runner.rs` |
| 22 | `produce-request-too-large` | Message size rejection | `fake_broker.rs`, `connection.rs` |
| 23 | `compression-codec-mismatch` | Unsupported codec | `fake_broker.rs`, `produce.rs` |
| 24 | `clock-skew-ntp` | Clock skew injection | `scenario_runner.rs`, `eventid/modes.rs` |
| 25 | `resource-exhaustion-fd-memory` | FD/memory limits | `scenario_runner.rs`, `backpressure.rs` |
| 26 | `network-partition-partial` | Partial connectivity | `toxiproxy-scenarios/`, `cluster_fixture.rs` |
| 27 | `slow-consumer-lag` | Consumer backpressure | `fake_broker.rs`, `load_generator.rs` |
| 28 | `proxy-crash-during-replay` | Crash during replay | `scenario_runner.rs`, `reconciler/replay.rs` |
| 29 | `arc-flapping` | ARC state flapping | `fake_arc.rs`, `breaker/arc.rs` |
| 30 | `combined-chaos` | All together | Integration test |

**Deliverables**:
- [ ] Partition reassignment via admin API
- [ ] Log compaction simulation
- [ ] MirrorMaker lag injection
- [ ] Consumer group storm generator
- [ ] Fetch session management
- [ ] Message size/compression validation
- [ ] Clock skew via container time manipulation
- [ ] Resource limit enforcement via cgroups
- [ ] Full combined chaos orchestration

**Exit Criteria**: All 30 scenarios pass, combined-chaos runs 100 seeds without invariant violations

---

## Cross-Cutting Infrastructure (Parallel)

| Task | Phase | Owner |
|------|-------|-------|
| Toxiproxy Rust client | 1 | |
| Docker API integration (kill/restart) | 1 | |
| FS failure layer | 2 | |
| Secondary Kafka cluster | 2 | |
| Scenario runner with deterministic seeds | 1 | |
| Invariant checker enhancements | 1-4 | |
| Test result aggregation & reporting | 4 | |
| CI integration (L8 gate) | 4 | |

---

## Phase Gates

| Phase | Gate | Criteria |
|-------|------|----------|
| 1 → 2 | All 4 core scenarios pass | I1, I2, I5 green |
| 2 → 3 | All 4 storage scenarios pass | I1, I2, I5, I7 green |
| 3 → 4 | All 8 protocol scenarios pass | Respective invariants green |
| 4 → Done | All 30 + combined-chaos 100 seeds | Zero invariant violations |

---

## Timeline

| Week | Phase | Focus |
|------|-------|-------|
| 1-2 | Phase 1 | Core chaos primitives |
| 2-3 | Phase 2 | Storage & channel failures |
| 3-4 | Phase 3 | Protocol & cloud failures |
| 4-5 | Phase 4 | Edge cases & orchestration |
| 5 | Hardening | Combined chaos, CI integration |

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Docker API flakiness on Windows | Use named volumes, avoid bind mounts for scenario files |
| Toxiproxy not in PATH | Bundle toxiproxy-cli in test runner image |
| Kafka KRaft metadata corruption | Unique cluster IDs per test run |
| Flaky consumer group tests | Increase timeouts, add retry logic |
| Combined chaos non-determinism | Fixed seeds, artifact capture on failure |

---

## File: `chaos-implementation-plan.md`