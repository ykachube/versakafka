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

```bash
# Unit tests
cargo test --workspace --lib

# Integration tests (requires docker)
cargo test --workspace --test integration

# Chaos scenarios (requires toxiproxy)
cargo test --workspace --test chaos
```

## Monitoring

- **Prometheus**: `http://localhost:9090`
- **Grafana**: `http://localhost:3000` (admin/admin)
- **Jaeger**: `http://localhost:16686`
- **MinIO Console**: `http://localhost:9001` (minioadmin/minioadmin)

## License

MIT OR Apache-2.0