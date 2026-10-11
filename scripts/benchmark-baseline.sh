#!/usr/bin/env bash
# Baseline benchmark: client directly to Kafka (no proxy)
# Usage: ./benchmark-baseline.sh [kafka-broker] [topic] [num-records] [record-size] [acks]

set -euo pipefail

KAFKA_BROKER="${1:-localhost:9092}"
TOPIC="${2:-bench-topic}"
NUM_RECORDS="${3:-1000000}"
RECORD_SIZE="${4:-1024}"
ACKS="${5:-all}"

echo "=== Baseline Benchmark ==="
echo "Broker: $KAFKA_BROKER"
echo "Topic: $TOPIC"
echo "Records: $NUM_RECORDS"
echo "Record size: $RECORD_SIZE bytes"
echo "Acks: $ACKS"
echo ""

# Create topic
kafka-topics --bootstrap-server "$KAFKA_BROKER" \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions 12 \
  --replication-factor 3 \
  --config min.insync.replicas=2

echo "Waiting for topic to be ready..."
sleep 5

# Generate test data
PAYLOAD=$(head -c "$RECORD_SIZE" /dev/zero | tr '\0' 'x')

# Warmup
echo "Warmup..."
kafka-producer-perf-test \
  --topic "$TOPIC" \
  --num-records 10000 \
  --record-size "$RECORD_SIZE" \
  --throughput -1 \
  --producer-props \
    bootstrap.servers="$KAFKA_BROKER" \
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
    bootstrap.servers="$KAFKA_BROKER" \
    acks="$ACKS" \
    linger.ms=5 \
    batch.size=16384 \
    buffer.memory=67108864 \
    compression.type=snappy \
    max.in.flight.requests.per.connection=5

echo ""
echo "=== Benchmark complete ==="