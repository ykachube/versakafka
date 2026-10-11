# Resilient Kafka Proxy: Proposal

A producer-resilience proxy for Kafka with circuit breaking, spill-to-object-storage, dual-channel writes, reliable event-IDs, customizable event hooks and telemetry. It is deployable in an HA, multi-region setup on EKS.

---

## 1. Build vs. buy

- **Kroxylicious** (Java, filter chain, record encryption) and **Conduktor Gateway** already cover much of this. Neither offers ARC-style circuit breaking plus S3 spill out of the box.
- **Envoy's Kafka filter** is mostly L7 metrics and routing, not record rewriting.
- Build only if you need low-overhead Rust/Go, spill-on-outage, dual-channel writes, or tight AWS integration. That is a legitimate gap, so frame the project as a **"producer-resilience proxy"**, not a generic Kafka proxy.

## 2. Language choice

**Rust (recommended)**
- No GC gives predictable p99 on a hot path where batches may be decompressed and re-compressed.
- Libraries: `tokio`, `kafka-protocol`, `rustls`, `aws-sdk-*`, `object_store`, `wasmtime` for plugins.
- The compiler catches many LLM-generated mistakes, which suits vibe coding.

**Go (alternative)**
- Faster to iterate; `franz-go/kmsg` is an excellent protocol codec.
- Fine if p99 isn't critical. GC pauses are small these days.

## 3. Core architecture

A stateless L7 proxy that decodes Kafka frames and runs them through a filter chain.

```
client → [TLS/SASL] → pre-receive → decode → [assign event-id] → post-receive
       → filter / enrich → re-encrypt → pre-send
       → WAL (if spill-capable) → fan-out → channel A / channel B
       → post-send → ack policy → client
```

- **Hooks:** built-in ones are Rust traits. Custom logic (pre-receive, post-receive, pre-send, post-send, re-encryption, metadata enrichment, filtering) runs as **WASM plugins**: sandboxed, hot-reloadable, with CPU and time budgets.
- **Batch-level by default.** Per-record hooks force decompression and CRC recompute, so make them opt-in per topic.
- **Metadata rewriting is the hard part.** Rewrite Metadata and FindCoordinator responses so clients route through the proxy. Use SNI-based routing on one NLB, or a port-per-broker mapping.
- **v1 scope is the produce path:** Produce, Metadata, ApiVersions, auth, and Init/transactions passthrough. Fetch and consumer groups are transparent passthrough initially.

## 4. Circuit breaker

Two layers:

1. **Local adaptive breaker** per broker/cluster/channel. It tracks error rate, latency and timeouts, with half-open probing, and reacts in milliseconds.
2. **Regional breaker** driven by **AWS ARC routing controls**, polled via the 5 regional cluster endpoints with a cached last-known-good state. Fail-static if ARC is unreachable.
   - Alternatives: Route 53 health checks, or a signed flag in S3/SSM/etcd.

## 5. Spill

- When a breaker opens, write batches to a **local WAL** (NVMe or EBS), then flush to **S3** (or MinIO/GCS/R2 via the `object_store` crate) as segment files.
- A **replayer** drains spill when the breaker closes, with rate limiting.
- **Define the ack contract explicitly:**
  - "acked = durably spilled" (at-least-once, possible reordering across spill and live), or
  - "backpressure only".
- **Main correctness risks:** per-partition ordering and idempotent-producer sequence numbers. Ship an opt-in "ordering-unsafe spill" mode first and document it.

## 6. Dual-channel writes

**A channel** is any durable sink the proxy can write a batch to: a primary Kafka cluster, a secondary cluster in another region, MSK plus self-managed Kafka, or a non-Kafka sink (S3, Kinesis, SQS). The spill WAL is effectively a third channel with the same interface.

### Per-topic ack policy

| Policy | Client ack when | Trade-off |
|---|---|---|
| `primary-sync, secondary-async` | primary acks | Lowest latency; secondary may lag or lose a window |
| `any` | first channel durably acks | Best availability; the other channel catches up |
| `all` | every channel acks | Strongest durability; latency is the slowest channel, and availability drops if one fails |
| `all-or-spill` | all ack, or the failed channel is durably spilled | Good default for "never lose, never block" |

### Design points

- **Each channel has its own circuit breaker, bounded send queue and spill path.** A broken channel degrades per policy and never blocks the healthy one. Per-channel lag and spill depth go into telemetry.
- **The proxy becomes producer-of-record.** Producer IDs and sequence numbers are per cluster, so the client's idempotence state can't be passed through to two clusters. The proxy terminates the client's idempotent session and runs its own idempotent producer per channel.
- **No cross-channel transactions.** Offsets differ between channels. Per-partition order on the second channel needs the same key-based partitioner and partition count.
- **Consumers must tolerate duplicates**, which is why the event-ID matters.
- **Reconciler (optional job):** compares event-ID counts or checksums per topic and time window across channels, and replays gaps from the WAL or S3.
- **Implementation:** fan out at batch level by cloning the buffer reference, not the bytes. Each channel has its own producer task.

## 7. Reliable event-ID

**Rule:** assign the ID exactly once, at first receipt, before any filter, re-encrypt, spill, retry or fan-out. Persist it in the WAL so replays carry the identical ID.

### Modes (per topic)

1. **`preserve` (default):** keep a producer-supplied `event-id` header and generate one only if missing.
2. **`generate-uuidv7`:** time-ordered and fast, but random. Client retries get a new ID, so this doesn't dedupe client-side retries.
3. **`deterministic` (recommended for critical topics):** a hash of stable producer identity plus sequence (client-session ID, partition, base sequence + record offset in batch). It survives client retries, so it gives true end-to-end dedupe. It requires the idempotent-producer path.
4. **`content-hash`:** a hash of topic, key and value. It dedupes naturally but collapses legitimately identical events, so use it only where that's acceptable.
5. **`reject-duplicate`:** a short-TTL seen-ID cache (in-memory plus optional Redis/DynamoDB) drops repeats. Best-effort only.

### Format and metadata

- Store as a 16-byte binary header (`event-id`) to keep overhead small.
- Optionally stamp `ingest-ts`, `proxy-region`, `proxy-id` and `channel` headers for tracing and forensics.
- Optionally add an HMAC over the ID and metadata for tamper evidence. It is cheap if the key is cached.

### Cost

Record headers live inside the record, so adding them means decode, rewrite, CRC and recompress. This moves the topic off the batch-level fast path. Make it opt-in per topic and benchmark it.

### Consumer side

Ship a small dedupe helper (a Flink/Kafka Streams snippet, or an idempotent-sink pattern keyed by event-ID with a TTL store). Without it, dual-channel gives at-least-once with duplicates.

## 8. Re-encryption and enrichment

- Envelope encryption with KMS-wrapped DEKs, cached with TTL, and key IDs stored in record headers.
- Enrichment (headers, metadata) is a built-in filter, and custom enrichment runs as a WASM plugin.

## 9. Telemetry

- OpenTelemetry metrics and traces, with per-topic, per-client, per-channel and breaker-state labels.
- Prometheus endpoint and structured logs.
- **Key SLIs:** spill depth, per-channel lag, proxy-added p99 latency, breaker state transitions, dedupe/duplicate rate.

## 10. Deployment (EKS, multi-region)

- **Per region:** a stateless Deployment with pod topology spread across AZs, PDBs, HPA on connections/CPU, Karpenter, and graceful drain (SIGTERM → stop accepting, finish in-flight).
- **Ingress:** an **NLB** (TCP/TLS passthrough) per region.
- **Region failover:** **Route 53 + ARC**, or **Global Accelerator** for faster failover with static anycast IPs.
- **Data locality:** each region's proxy talks to its local cluster as the primary channel. Cross-region replication belongs to the Kafka layer (MSK Replicator, MM2, Cluster Linking) or to the dual-channel feature above.

### Faster or simpler alternatives

- **Sidecar or in-process library** for producers. It removes the hop and the metadata-rewriting problem, but loses central control.
- **EC2 ASG or ECS on Fargate** behind an NLB, if you don't need Kubernetes.
- **Cell-based layout**, where each cell serves a subset of clients, to limit blast radius.

## 11. Phased plan

| Phase | Deliverable |
|---|---|
| 1 | Pass-through proxy with Metadata rewriting, TLS and metrics (validate with `kcat` and a Java client) |
| 2 | Filter chain and WASM hooks, enrichment and filtering, **event-ID modes** (`preserve`, `uuidv7`, `deterministic`) |
| 3 | Local circuit breaker, then ARC integration |
| 3b | **Channel abstraction**, per-channel breakers, `primary-sync` and `any` policies |
| 4 | Spill as a channel, replay, `all-or-spill`, reconciler, and chaos tests (broker kill, partition, slow broker, one channel down) |
| 5 | Re-encryption, multi-region hardening, load testing against p99 targets |

## 12. Key risks and mitigations

| Risk | Mitigation |
|---|---|
| Protocol compatibility across client versions (API version negotiation, flexible versions) | Golden-packet tests; compatibility matrix against librdkafka, Java and franz-go |
| Transactions, idempotence and ordering across spill and channels | Explicit ack contract; proxy as producer-of-record; document ordering-unsafe mode |
| Divergence between channels under partial failure | Event-ID plus reconciler |
| Deterministic IDs depend on idempotent producers | Non-idempotent clients fall back to UUIDv7 and may duplicate on retry; document it |
| Latency under `all` is bounded by the slowest channel | Per-channel timeouts; prefer `all-or-spill` |
| Proxy as SPOF and added latency | Multi-AZ HA, budget about 1ms p99 overhead and measure it |
| Vibe-coding risk in protocol edge cases | Strong test suite, real-client compatibility tests, fuzzing of the codec |

## 13. Open questions

- What are the target p99 overhead and throughput per pod?
- Is client-visible ordering required during spill and replay?
- Which topics need deterministic IDs and dual-channel, and which stay on the batch-level fast path?
- Is the secondary channel Kafka (same partition count and partitioner) or a different sink?
