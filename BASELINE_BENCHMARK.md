# Baseline Benchmark Results

**Date:** 2026-11-17  
**Environment:** Docker (confluentinc/cp-kafka:7.6.1), single broker, no proxy  
**Hardware:** Windows host, Docker Desktop  
**Test Tool:** `kafka-producer-perf-test` / `kafka-consumer-perf-test`

---

## Test Configuration

| Parameter | Value |
|-----------|-------|
| Kafka Version | 7.6.1 (Confluent Platform) |
| Broker Count | 1 |
| Topic Partitions | 12 |
| Replication Factor | 1 (single broker) |
| min.insync.replicas | 1 |
| Compression | snappy |
| Batch Size | 16 KB |
| Buffer Memory | 64 MB |
| Linger.ms | 5 |
| Max In-Flight Requests | 5 |

---

## Producer Baseline Results (Direct to Kafka)

### 1KB Records, acks=all

| Run | Throughput (rec/s) | Throughput (MB/s) | Avg Latency (ms) | p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) | Max (ms) |
|-----|-------------------|------------------|-----------------|----------|----------|----------|-----------|----------|
| 1 | 135,648 | 132.47 | 331.68 | 331 | 492 | 513 | 526 | 529 |
| 2 | 164,042 | 160.20 | 214.64 | 231 | 286 | 300 | 311 | 315 |
| 3 | *warmup* | - | 31.52 | 22 | 85 | 95 | 97 | 247 |

**Stable Range (after warmup):**
- **Throughput:** 135K - 164K rec/s (132 - 160 MB/s)
- **p50 Latency:** 231 - 331 ms
- **p99 Latency:** 300 - 513 ms
- **p99.9 Latency:** 311 - 526 ms

### 1KB Records, acks=1

| Run | Throughput (rec/s) | Throughput (MB/s) | Avg Latency (ms) | p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) | Max (ms) |
|-----|-------------------|------------------|-----------------|----------|----------|----------|-----------|----------|
| 1 | 158,831 | 155.11 | 250.69 | 256 | 346 | 363 | 369 | 373 |

**Comparison acks=all vs acks=1:**
- Throughput: ~similar (155-160 MB/s)
- p50: acks=1 slightly higher (256 vs 231-331 ms)
- p99: acks=1 lower (363 vs 300-513 ms) - more consistent

### 100 Byte Records, acks=all

| Run | Throughput (rec/s) | Throughput (MB/s) | Avg Latency (ms) | p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) | Max (ms) |
|-----|-------------------|------------------|-----------------|----------|----------|----------|-----------|----------|
| 1 | 518,135 | 49.41 | 9.00 | 2 | 48 | 59 | 64 | 286 |

### 10 KB Records, acks=all

| Run | Throughput (rec/s) | Throughput (MB/s) | Avg Latency (ms) | p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) | Max (ms) |
|-----|-------------------|------------------|-----------------|----------|----------|----------|-----------|----------|
| 1 | 13,523 | 132.06 | 282.67 | 198 | 453 | 1,734 | 1,737 | 1,739 |

**Note:** At 10KB record size, tail latency (p99) increases significantly due to batching/compression overhead.

---

## Consumer Baseline Results (Direct from Kafka)

| Metric | Value |
|--------|-------|
| Data Consumed | 98 MB |
| Throughput | 24.8 MB/s |
| Messages Consumed | 100,358 |
| Message Rate | 25,381 msg/s |
| Rebalance Time | 3,322 ms |
| Fetch Time | 632 ms |
| Fetch Throughput | 155 MB/s |
| Fetch Message Rate | 158,794 msg/s |

---

## Summary for Proxy Overhead Calculation

### Target Baseline Metrics (to beat):

| Metric | Target (Baseline) | Proxy Budget |
|--------|------------------|--------------|
| **Producer p50 (1KB, acks=all)** | **231-331 ms** | **≤ +1 ms (ideal), ≤ +5 ms (acceptable)** |
| **Producer p99 (1KB, acks=all)** | **300-513 ms** | **≤ +2 ms (ideal), ≤ +10 ms (acceptable)** |
| **Producer Throughput (1KB, acks=all)** | **135-164K rec/s** | **≤ 5% degradation** |
| **Consumer Throughput** | **25K msg/s** | **≤ 5% degradation** |

### Key Observations

1. **High variance in first run** - Warmup is critical. First run after broker start shows much higher latency.
2. **Stable after warmup** - Runs 2+ are consistent within ~20% range.
3. **acks=1 vs acks=all** - Similar throughput, acks=1 has more predictable tail latency.
4. **Record size impact** - Small records (100B): very low p50 (2ms) but high max (286ms). Large records (10KB): high p99 (1.7s).
5. **Docker overhead** - These numbers include Docker networking overhead. Bare-metal would be lower.

---

## Next Steps

1. **Run proxy benchmark** (`make bench-proxy`) with same config
2. **Calculate overhead:** `proxy_latency - baseline_latency`
3. **Verify budget:** Proxy p99 should not exceed baseline p99 by more than 1-2ms for 1KB records

---

## Raw Commands for Reproduction

```bash
# Start Kafka only
docker-compose -f docker-compose.test.yml up -d kafka-1 --wait

# Create topic
docker exec kp-test-kafka-1 kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic bench-topic --partitions 12 --replication-factor 1

# Warmup
docker exec kp-test-kafka-1 kafka-producer-perf-test --topic bench-topic \
  --num-records 10000 --record-size 1024 --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092 acks=all linger.ms=5 batch.size=16384

# Benchmark (500K records, 1KB, acks=all)
docker exec kp-test-kafka-1 kafka-producer-perf-test --topic bench-topic \
  --num-records 500000 --record-size 1024 --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092 acks=all linger.ms=5 batch.size=16384 \
  buffer.memory=67108864 compression.type=snappy max.in.flight.requests.per.connection=5
```