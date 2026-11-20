# Chaos Scenario Implementation Plan

## Overview
Implement 30 chaos scenarios across 4 phases, each building on the previous. Each phase adds new failure injection capabilities to the test framework.

**Current Status (Nov 20, 2026):**
- ✅ **Phase 1 Infrastructure**: Toxiproxy Rust client, Docker API integration, scenario runner with deterministic seeds
- ✅ **4/4 Phase 1 scenarios defined** (broker-failure-primary, network-partition, slow-broker, slow-consumer-lag)
- ✅ **4/4 Phase 2 scenarios defined** (secondary-unavailable, s3-unavailable, disk-full, s3-multipart-upload-failures)
- ✅ **8/8 Phase 3 scenarios defined** (transactions, metadata, TLS, SASL, WAL IO, KMS, event-id, breaker)
- ✅ **13/13 Phase 4 scenarios defined** (rebalance, compaction, MM lag, consumer storms, etc.)
- ✅ **All 30 scenarios defined** in test-scenarios/ and toxiproxy-scenarios/
- ✅ **10/10 unit tests pass** in kp-testkit
- ⏳ **Infrastructure gaps**: load_generator, fs_failures, secondary Kafka cluster
- ⏳ **CI Pipeline**: Needed for Linux-native toxiproxy testing

---

## Phase 1: Core Failure Injection (Week 1-2)
**Goal**: Basic chaos primitives - broker failure, network, latency, consumer lag
**Status**: ✅ **Infrastructure DONE**, scenarios need execution

| # | Scenario | New Capability | Files to Modify | Status |
|---|----------|----------------|-----------------|--------|
| 1 | `broker-failure-primary` | Kill broker container | `fake_broker.rs`, `cluster_fixture.rs` | ✅ Defined |
| 2 | `network-partition` | Toxiproxy reset_peer | `toxiproxy-scenarios/`, `cluster_fixture.rs` | ✅ Defined |
| 3 | `slow-broker` | Toxiproxy latency/jitter | `toxiproxy-scenarios/`, `load_generator.rs` | ✅ Defined |
| 4 | `slow-consumer-lag` | Consumer pause injection | `fake_broker.rs`, `scenario_runner.rs` | ✅ Defined |

**Deliverables**:
- ✅ Toxiproxy client wrapper in Rust (`kp-testkit/src/toxiproxy.rs`)
- ✅ Container kill/restart via Docker API (`kp-testkit/src/docker_client.rs`)
- ✅ Consumer group pause/resume via admin API (pending)
- ✅ Baseline chaos test runner (`kp-chaos-runner`)

**Exit Criteria**: All 4 scenarios run and pass invariants I1, I2, I5

---

## Phase 2: Storage & Channel Failures (Week 2-3)
**Goal**: S3, WAL, secondary channel, disk failures
**Status**: ⏳ **Needs infrastructure: load_generator, fs_failures, secondary Kafka**

| # | Scenario | New Capability | Files to Modify | Status |
|---|----------|----------------|-----------------|--------|
| 5 | `secondary-unavailable` | Secondary Kafka cluster down | `cluster_fixture.rs`, `kafka_channel.rs` | ✅ Defined, ⏳ needs secondary Kafka in docker-compose |
| 6 | `s3-unavailable` | MinIO/ LocalStack S3 down | `toxiproxy-scenarios/`, `fake_s3.rs` | ✅ Defined |
| 7 | `disk-full` | FS failure injection | `fs_failures.rs`, `wal.rs` | ✅ Defined, ⏳ needs fs_failures layer |
| 8 | `s3-multipart-upload-failures` | Partial S3 upload failures | `fake_s3.rs`, `spill/uploader.rs` | ✅ Defined |

**Deliverables**:
- ⏳ Secondary Kafka cluster in docker-compose.test.yml
- ⏳ FS failure layer (torn writes, fsync failures, disk full)
- ⏳ S3 multipart upload failure injection
- ⏳ Spill recovery verification

**Exit Criteria**: All 4 scenarios pass I1, I2, I5, I7

---

## Phase 3: Advanced Protocol & Cloud Failures (Week 3-4)
**Goal**: Transactions, metadata, TLS, auth, KMS, ARC
**Status**: ⏳ **Needs infrastructure: transactional producer, TLS cert rotation, SASL auth**

| # | Scenario | New Capability | Files to Modify | Status |
|---|----------|----------------|-----------------|--------|
| 9 | `transaction-timeout-exhaustion` | Txn ID exhaustion, timeout | `fake_broker.rs`, `produce.rs` | ✅ Defined, ⏳ needs transactional producer |
| 10 | `metadata-cache-staleness` | Broker add/remove, partition reassign | `metadata.rs`, `cluster_fixture.rs` | ✅ Defined |
| 11 | `tls-certificate-expiration` | Cert rotation, expired cert | `tls.rs`, `cluster_fixture.rs` | ✅ Defined, ⏳ needs cert rotation |
| 12 | `sasl-authentication-failures` | SASL auth errors, reauth | `fake_broker.rs`, `connection.rs` | ✅ Defined, ⏳ needs SASL auth |
| 13 | `wal-disk-io-errors` | Torn writes, fsync failures | `fs_failures.rs`, `wal/reader.rs` | ✅ Defined, ⏳ needs fs_failures |
| 14 | `kms-throttling` | KMS rate limiting | `toxiproxy-scenarios/`, `kms.rs` | ✅ Defined |
| 15 | `event-id-collision-deterministic` | EID collision, fallback | `eventid/modes.rs`, `checker.rs` | ✅ Defined |
| 16 | `breaker-flapping` | Breaker state thrashing | `breaker/local.rs`, `scenario_runner.rs` | ✅ Defined |

**Deliverables**:
- ⏳ Transactional producer simulation
- ⏳ Metadata cache with TTL and forced refresh
- ⏳ TLS cert rotation via container reload
- ⏳ SASL auth error injection
- ⏳ KMS throttle via LocalStack config
- ⏳ Breaker flapping detection metrics

**Exit Criteria**: All 8 scenarios pass relevant invariants

---

## Phase 4: Complex Multi-Failure & Edge Cases (Week 4-5)
**Goal**: Rebalances, compaction, replication, resources, clock skew
**Status**: ⏳ **Needs infrastructure: partition reassignment, log compaction, consumer storms**

| # | Scenario | New Capability | Files to Modify | Status |
|---|----------|----------------|-----------------|--------|
| 17 | `rebalance-during-produce` | Partition reassignment during produce | `cluster_fixture.rs`, `produce.rs` | ✅ Defined, ⏳ needs partition reassignment |
| 18 | `log-compaction-offset-shift` | Log compaction offset translation | `fake_broker.rs`, `checker.rs` | ✅ Defined, ⏳ needs log compaction |
| 19 | `mirror-maker-replication-lag` | MM lag injection | `kafka_channel.rs`, `toxiproxy-scenarios/` | ✅ Defined |
| 20 | `consumer-group-rebalance-storm` | Rapid join/leave | `fake_broker.rs`, `scenario_runner.rs` | ✅ Defined, ⏳ needs consumer storms |
| 21 | `fetch-session-timeout` | Fetch session expiry | `fake_broker.rs`, `scenario_runner.rs` | ✅ Defined |
| 22 | `produce-request-too-large` | Message size rejection | `fake_broker.rs`, `connection.rs` | ✅ Defined |
| 23 | `compression-codec-mismatch` | Unsupported codec | `fake_broker.rs`, `produce.rs` | ✅ Defined |
| 24 | `clock-skew-ntp` | Clock skew injection | `scenario_runner.rs`, `eventid/modes.rs` | ✅ Defined |
| 25 | `resource-exhaustion-fd-memory` | FD/memory limits | `scenario_runner.rs`, `backpressure.rs` | ✅ Defined |
| 26 | `network-partition-partial` | Partial connectivity | `toxiproxy-scenarios/`, `cluster_fixture.rs` | ✅ Defined |
| 27 | `slow-consumer-lag` | Consumer backpressure | `fake_broker.rs`, `load_generator.rs` | ✅ Defined, ⏳ needs load_generator |
| 28 | `proxy-crash-during-replay` | Crash during replay | `scenario_runner.rs`, `reconciler/replay.rs` | ✅ Defined |
| 29 | `arc-flapping` | ARC state flapping | `fake_arc.rs`, `breaker/arc.rs` | ✅ Defined |
| 30 | `combined-chaos` | All together | Integration test | ✅ Defined |

**Deliverables**:
- ⏳ Partition reassignment via admin API
- ⏳ Log compaction simulation
- ⏳ MirrorMaker lag injection
- ⏳ Consumer group storm generator
- ⏳ Fetch session management
- ⏳ Message size/compression validation
- ⏳ Clock skew via container time manipulation
- ⏳ Resource limit enforcement via cgroups
- ⏳ Full combined chaos orchestration

**Exit Criteria**: All 30 scenarios pass, combined-chaos runs 100 seeds without invariant violations

---

## Cross-Cutting Infrastructure (Parallel)

| Task | Phase | Status |
|------|-------|--------|
| Toxiproxy Rust client | 1 | ✅ DONE (`kp-testkit/src/toxiproxy.rs`) |
| Docker API integration (kill/restart) | 1 | ✅ DONE (`kp-testkit/src/docker_client.rs`) |
| FS failure layer | 2 | ⏳ PENDING |
| Secondary Kafka cluster | 2 | ⏳ PENDING (docker-compose) |
| Scenario runner with deterministic seeds | 1 | ✅ DONE (`kp-testkit/src/scenario_runner.rs`) |
| Invariant checker enhancements | 1-4 | ✅ DONE (10 tests pass) |
| Test result aggregation & reporting | 4 | ⏳ PENDING |
| CI integration (L8 gate) | 4 | ⏳ PENDING (needs GitHub Actions) |
| Load generator | 2 | ⏳ PENDING |

---

## Phase Gates

| Phase | Gate | Criteria |
|-------|------|----------|
| 1 → 2 | All 4 core scenarios pass | I1, I2, I5 green |
| 2 → 3 | All 4 storage scenarios pass | I1, I2, I5, I7 green |
| 3 → 4 | All 8 protocol scenarios pass | Respective invariants green |
| 4 → Done | All 30 + combined-chaos 100 seeds | Zero invariant violations |

---

## Timeline (Updated)

| Week | Phase | Focus |
|------|-------|-------|
| 1-2 | Phase 1 | Core chaos primitives (infra DONE) |
| 2-3 | Phase 2 | Storage & channel failures (needs load_generator, fs_failures, secondary Kafka) |
| 3-4 | Phase 3 | Protocol & cloud failures (needs transactional producer, TLS cert rotation, SASL) |
| 4-5 | Phase 4 | Edge cases & orchestration (needs partition reassignment, log compaction) |
| 5 | Hardening | Combined chaos, CI integration |

**Immediate Blocker**: GitHub Actions CI Pipeline for Linux-native toxiproxy testing

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Docker API flakiness on Windows | Use named volumes, avoid bind mounts for scenario files |
| Toxiproxy not in PATH | Bundle toxiproxy-cli in test runner image |
| Kafka KRaft metadata corruption | Unique cluster IDs per test run |
| Flaky consumer group tests | Increase timeouts, add retry logic |
| Combined chaos non-determinism | Fixed seeds, artifact capture on failure |
| **Windows toxiproxy networking** | **Run toxiproxy on host via scripts/start-toxiproxy-host.bat; use Linux CI** |

---

## File: `chaos-implementation-plan.md`