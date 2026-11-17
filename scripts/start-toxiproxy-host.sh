#!/usr/bin/env bash
# Quick start toxiproxy on Windows/macOS host for local development
# Run this script before running kp-chaos-runner on non-Linux hosts

set -euo pipefail

echo "Starting Toxiproxy on host..."

# Check if toxiproxy is available
if ! command -v toxiproxy-server &> /dev/null; then
    echo "toxiproxy-server not found. Install it:"
    echo "  Windows: scoop install toxiproxy"
    echo "  macOS: brew install toxiproxy"
    echo "  Linux: download from https://github.com/Shopify/toxiproxy/releases"
    exit 1
fi

# Start toxiproxy in background
echo "Starting toxiproxy-server on 0.0.0.0:8474..."
toxiproxy-server -host 0.0.0.0 &
TOXIPROXY_PID=$!

# Wait for it to start
sleep 2

# Create proxies
echo "Creating proxies..."
toxiproxy-cli create kafka-primary -l 0.0.0.0:8666 -u localhost:9092
toxiproxy-cli create localstack -l 0.0.0.0:8668 -u localhost:4566
toxiproxy-cli create minio -l 0.0.0.0:8669 -u localhost:9000

echo ""
echo "Toxiproxy running on localhost:8474"
echo "Proxies created:"
echo "  - kafka-primary: localhost:8666 -> localhost:9092"
echo "  - localstack: localhost:8668 -> localhost:4566"
echo "  - minio: localhost:8669 -> localhost:9000"
echo ""
echo "Press Ctrl+C to stop"

# Wait for interrupt
trap "kill $TOXIPROXY_PID; echo 'Stopped toxiproxy'; exit 0" INT TERM
wait $TOXIPROXY_PID