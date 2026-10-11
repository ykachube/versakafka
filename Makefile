# Makefile for Resilient Kafka Proxy

.PHONY: help build test check fmt clippy audit deny clean run dev-up dev-down bench-baseline bench-proxy

# Default target
help:
	@echo "Resilient Kafka Proxy - Development Commands"
	@echo ""
	@echo "Build & Check:"
	@echo "  build        - Build workspace in release mode"
	@echo "  check        - Check compilation (no build)"
	@echo "  fmt          - Format code with rustfmt"
	@echo "  clippy       - Run clippy lints"
	@echo "  audit        - Run cargo-audit for security"
	@echo "  deny         - Run cargo-deny for dependency checks"
	@echo ""
	@echo "Test:"
	@echo "  test         - Run all tests"
	@echo "  test-unit    - Run unit tests only"
	@echo "  test-integration - Run integration tests (requires docker)"
	@echo ""
	@echo "Development:"
	@echo "  dev-up       - Start dev environment (docker-compose)"
	@echo "  dev-down     - Stop dev environment"
	@echo "  dev-logs     - Follow dev environment logs"
	@echo "  run          - Run proxy with local config"
	@echo ""
	@echo "Benchmark:"
	@echo "  bench-baseline - Run baseline benchmark (client -> Kafka)"
	@echo "  bench-proxy    - Run proxy benchmark (client -> proxy -> Kafka)"
	@echo ""
	@echo "CI:"
	@echo "  ci           - Run all CI checks locally"

# Build
build:
	cargo build --workspace --release

check:
	cargo check --workspace --all-targets

# Code quality
fmt:
	cargo fmt --all

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

audit:
	cargo audit

deny:
	cargo deny check

# Test
test:
	cargo test --workspace

test-unit:
	cargo test --workspace --lib

test-integration:
	cargo test --workspace --test integration

# Development environment
dev-up:
	docker-compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@echo "Services ready!"
	@echo "  Kafka:     localhost:9092,9094,9096"
	@echo "  MinIO:     localhost:9000 (console: 9001)"
	@echo "  Prometheus: localhost:9090"
	@echo "  Grafana:   localhost:3000 (admin/admin)"
	@echo "  Jaeger:    localhost:16686"
	@echo "  Toxiproxy: localhost:8474"

dev-down:
	docker-compose down -v

dev-logs:
	docker-compose logs -f

# Run proxy
run:
	cargo run --release --bin kp-proxy -- --config config.toml

# Benchmarks
bench-baseline:
	@if [ -z "$(KAFKA_BROKER)" ]; then \
		echo "Usage: make bench-baseline KAFKA_BROKER=host:port [TOPIC=topic] [RECORDS=1000000] [SIZE=1024] [ACKS=all]"; \
		exit 1; \
	fi
	./scripts/benchmark-baseline.sh $(KAFKA_BROKER) $(TOPIC) $(RECORDS) $(SIZE) $(ACKS)

bench-proxy:
	@if [ -z "$(PROXY_HOST)" ]; then \
		echo "Usage: make bench-proxy PROXY_HOST=host:port KAFKA_BROKER=host:port [TOPIC=topic] [RECORDS=1000000] [SIZE=1024] [ACKS=all]"; \
		exit 1; \
	fi
	./scripts/benchmark-proxy.sh $(PROXY_HOST) $(KAFKA_BROKER) $(TOPIC) $(RECORDS) $(SIZE) $(ACKS)

# CI
ci: fmt clippy check test audit deny

# Clean
clean:
	cargo clean
	rm -rf target
	docker-compose down -v 2>/dev/null || true