# Resilient Kafka Proxy (kp-proxy)

A producer-resilience proxy for Apache Kafka with circuit breaking, spill-to-object-storage, dual-channel writes, and WASM-based extensibility.

## Architecture Overview

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│  Producer   │────▶│   kp-proxy       │────▶│  Primary Kafka      │
│  (Client)   │     │  (Producer of    │     │  (Primary Channel)  │
└─────────────┘     │   Record)        │     └─────────────────────┘
                    │                  │
                    │  ┌────────────┐  │     ┌─────────────────────┐
                    │  │  Filters   │  │     │  Secondary Kafka    │
                    │  │  (WASM)    │  │     │  (Secondary Channel)│
                    │  └────────────┘  │     └─────────────────────┘
                    │                  │
                    │  ┌────────────┐  │     ┌─────────────────────┐
                    │  │  Breakers  │──┼────▶│  ARC (Regional      │
                    │  │ (Local+ARC)│  │     │  Coordination)      │
                    │  └────────────┘  │     └─────────────────────┘
                    │                  │
                    │  ┌────────────┐  │     ┌─────────────────────┐
                    │  │    WAL     │──┼────▶│  S3/MinIO Spill     │
                    │  └────────────┘  │     │  (Spill Channel)    │
                    └──────────────────┘     └─────────────────────┘
```

## Key Features

- **Producer Resilience**: Proxy acts as producer of record, acknowledging before downstream
- **Circuit Breaking**: Local adaptive breaker + AWS ARC regional coordination
- **Spill to Object Storage**: WAL → S3/MinIO with automatic replay and reconciliation
- **Dual-Channel Writes**: Per-topic ack policies (PrimarySyncSecondaryAsync, Any, All, AllOrSpill)
- **Event ID Integrity**: 5 modes (preserve, uuidv7, deterministic, content-hash, reject-duplicate) + HMAC
- **WASM Extensibility**: Filter chain with hot-reloadable WASM plugins
- **Observability**: OpenTelemetry metrics, traces, structured logging
- **Multi-Region**: EKS-ready with regional failover

## Quick Start

### Prerequisites
- Rust 1.80+ (via `rustup`)
- Docker Compose (for dev environment)

### Development Environment
```bash
# Start Kafka, MinIO, Prometheus, Grafana, Jaeger, Toxiproxy
make dev-up

# Start full test environment with chaos tooling (LocalStack, 2nd Kafka, Fake ARC)
make dev-test-up

# Build and run proxy
make build
make run
```

### Configuration
Copy `config.toml` and adjust:
```toml
[proxy]
listen_addr = "0.0.0.0:9092"

[[channels]]
name = "primary"
channel_type = "kafka"
ack_policy = "primary-sync-secondary-async"
```

## Crate Structure

| Crate | Description |
|-------|-------------|
| `kp-bin` | Main proxy binary |
| `kp-proxy-core` | Connection handling, TLS, framing |
| `kp-protocol` | Kafka protocol codec |
| `kp-metadata` | Metadata rewriting |
| `kp-telemetry` | Metrics, tracing, Prometheus |
| `kp-filters` | Filter chain & hooks |
| `kp-wasm` | WASM host (wasmtime) |
| `kp-eventid` | Event ID generation (5 modes) |
| `kp-breaker` | Circuit breakers (local + ARC) |
| `kp-channel` | Channel trait & implementations |
| `kp-wal` | Write-ahead log |
| `kp-spill` | S3 spill & replay |
| `kp-reconciler` | Gap detection & replay |
| `kp-crypto` | Envelope encryption (KMS) |
| `kp-testkit` | Test harness (fake broker, chaos) |

## Commands

```bash
# Build & check
make build          # Release build
make check          # Compile check
make fmt            # Format code
make clippy         # Lint
make test           # Run tests

# Development
make dev-up         # Start docker-compose
make dev-down       # Stop docker-compose
make run            # Run proxy

# Benchmarks
make bench-baseline KAFKA_BROKER=localhost:9092
make bench-proxy PROXY_HOST=localhost:9092 KAFKA_BROKER=localhost:9092

# CI
make ci             # Full CI pipeline
```

## Architecture Decisions

See [ADR](docs/adr/):
- [ADR-01](docs/adr/01-batch-vs-per-record.md): Batch-level processing by default
- [ADR-02](docs/adr/02-proxy-as-producer-of-record.md): Proxy as producer of record
- [ADR-03](docs/adr/03-event-id-assigned-once.md): Event ID assigned once at first receipt
- [ADR-04](docs/adr/04-local-breaker-arc.md): Local breaker + ARC regional coordination
- [ADR-05](docs/adr/05-spill-as-channel.md): Spill as channel with reconciliation

## Testing

### Test Levels (L0-L9)
| Level | Name | Command | CI Gate |
|-------|------|---------|---------|
| L0 | Static analysis | `make test-L0` | PR |
| L1 | Unit + Property | `make test-L1` | PR |
| L2 | Concurrency (loom) | `make test-L2` | PR |
| L3 | Deterministic sim | `make test-L3` | PR |
| L4 | Component (fake broker) | `make test-L4` | PR |
| L5 | Integration (Kafka, MinIO, ARC) | `make test-L5` | Main |
| L6 | Compatibility matrix | `make test-L6` | Main |
| L7 | Fuzzing | `make test-L7` | Nightly |
| L8 | Chaos scenarios | `make test-L8` | Nightly |
| L9 | Perf/Soak | `make test-L9` | Weekly |

### Running Tests
```bash
# Unit tests (L1)
cargo test --workspace --lib

# All PR gate tests (L0, L1, L4)
make ci-pr

# Integration tests (L5) - requires docker
make dev-test-up
make test-L5

# Chaos scenarios (L8) - 30 deterministic scenarios
make test-chaos

# Run single chaos scenario
make test-scenario SCENARIO=broker-failure-primary

# Inject chaos manually
make toxiproxy-scenario SCENARIO=kafka-primary-latency
make fake-arc-scenario SCENARIO=arc-flapping
```

### Chaos Scenarios (30 total)
| Category | Scenarios |
|----------|-----------|
| **Broker/Network** | broker-failure-primary, network-partition, slow-broker, network-partition-partial |
| **Storage/S3/WAL** | s3-unavailable, disk-full, s3-multipart-upload-failures, wal-disk-io-errors |
| **Channel/Replication** | secondary-unavailable, mirror-maker-replication-lag |
| **Protocol/Edge** | transaction-timeout-exhaustion, metadata-cache-staleness, tls-certificate-expiration, sasl-authentication-failures, produce-request-too-large, compression-codec-mismatch |
| **AWS/Cloud** | kms-throttling, arc-flapping |
| **Event ID** | event-id-collision-deterministic |
| **Breaker** | breaker-flapping |
| **Rebalance** | rebalance-during-produce, log-compaction-offset-shift, consumer-group-rebalance-storm, fetch-session-timeout |
| **Resources** | resource-exhaustion-fd-memory, slow-consumer-lag, clock-skew-ntp |
| **Combined** | combined-chaos |

### Test Environment
```bash
# Start full test environment with chaos tooling
make dev-test-up

# Services included:
# - Kafka (primary + secondary clusters)
# - LocalStack (KMS, S3, DynamoDB mocks)
# - MinIO (S3-compatible)
# - Toxiproxy (chaos injection)
# - Fake ARC (5 regional endpoints)
# - Prometheus, Grafana, Jaeger
```

## CI Gates

| Stage | Tests | Blocking | Budget |
|-------|-------|----------|--------|
| PR | L0, L1, L4, golden, doc-tests | Yes | ≤ 10 min |
| Main | All PR + L5, L6 slice, L7 smoke, L9 microbench | Yes | ≤ 45 min |
| Nightly | Full L6, L7 (1-2h), L8 (100 seeds), mutation | No (tickets) | Overnight |
| Weekly | Soak 24h, EKS load test | No (tickets) | 24h |

## Monitoring

- **Prometheus**: `http://localhost:9090`
- **Grafana**: `http://localhost:3000` (admin/admin)
- **Jaeger**: `http://localhost:16686`
- **MinIO Console**: `http://localhost:9001` (minioadmin/minioadmin)
- **Toxiproxy API**: `http://localhost:8474`
- **LocalStack**: `http://localhost:4566`
- **Fake ARC**: `http://localhost:8080-8084`

## License

MIT OR Apache-2.0