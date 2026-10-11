#!/usr/bin/env bash
# Proxy benchmark: client -> proxy -> Kafka
# Usage: ./benchmark-proxy.sh [proxy-host] [kafka-broker] [topic] [num-records] [record-size] [acks]

set -euo pipefail

PROXY_HOST="${1:-localhost:9092}"
KAFKA_BROKER="${2:-localhost:9092}"
TOPIC="${3:-bench-topic}"
NUM_RECORDS="${4:-1000000}"
RECORD_SIZE="${5:-1024}"
ACKS="${6:-all}"

echo "=== Proxy Benchmark ==="
echo "Proxy: $PROXY_HOST"
echo "Kafka: $KAFKA_BROKER"
echo "Topic: $TOPIC"
echo "Records: $NUM_RECORDS"
echo "Record size: $RECORD_SIZE bytes"
echo "Acks: $ACKS"
echo ""

# Create topic on Kafka
kafka-topics --bootstrap-server "$KAFKA_BROKER" \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions 12 \
  --replication-factor 3 \
  --config min.insync.replicas=2

echo "Waiting for topic to be ready..."
sleep 5

# Warmup
echo "Warmup..."
kafka-producer-perf-test \
  --topic "$TOPIC" \
  --num-records 10000 \
  --record-size "$RECORD_SIZE" \
  --throughput -1 \
  --producer-props \
    bootstrap.servers="$PROXY_HOST" \
    acks="$ACKS" \
    linger.ms=5 \
    batch.size=16384 \
    buffer.memory=67108864 \
    compression.type=snappy

sleep 3

# Actual benchmark
echo ""
echo "=== Running benchmark ==="
kafka-producer-perf-test \
  --topic "$TOPIC" \
  --num-records "$NUM_RECORDS" \
  --record-size "$RECORD_SIZE" \
  --throughput -1 \
  --producer-props \
    bootstrap.servers="$PROXY_HOST" \
    acks="$ACKS" \
    linger.ms=5 \
    batch.size=16384 \
    buffer.memory=67108864 \
    compression.type=snappy \
    max.in.flight.requests.per.connection=5

echo ""
echo "=== Benchmark complete ==="