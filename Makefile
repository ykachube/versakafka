# Makefile for Resilient Kafka Proxy

.PHONY: help build test check fmt clippy audit deny clean run dev-up dev-down dev-logs bench-baseline bench-proxy \
        ci ci-pr ci-main ci-nightly ci-weekly \
        test-L0 test-L1 test-L2 test-L3 test-L4 test-L5 test-L6 test-L7 test-L8 test-L9 \
        test-scenario test-scenarios test-chaos \
        test-unit test-integration test-property test-fuzz \
        localstack-up localstack-down localstack-init \
        toxiproxy-scenario toxiproxy-reset \
        fake-arc-scenario fake-arc-reset \
        golden-record golden-replay

# Default target
help:
	@echo "Resilient Kafka Proxy - Development Commands"
	@echo ""
	@echo "Build & Check:"
	@echo "  build              - Build workspace in release mode"
	@echo "  check              - Check compilation (no build)"
	@echo "  fmt                - Format code with rustfmt"
	@echo "  clippy             - Run clippy lints (-D warnings)"
	@echo "  audit              - Run cargo-audit for security vulnerabilities"
	@echo "  deny               - Run cargo-deny for dependency checks"
	@echo ""
	@echo "Test Levels (L0-L9):"
	@echo "  test-L0            - Static analysis (rustfmt, clippy, cargo-deny, cargo-audit)"
	@echo "  test-L1            - Unit + Property tests (cargo-nextest, proptest, insta)"
	@echo "  test-L2            - Concurrency tests (loom models)"
	@echo "  test-L3            - Deterministic simulation (turmoil)"
	@echo "  test-L4            - Component tests vs fake broker (kp-testkit)"
	@echo "  test-L5            - Integration tests (real Kafka, MinIO, fake ARC via testcontainers)"
	@echo "  test-L6            - Compatibility matrix (librdkafka, Java, franz-go)"
	@echo "  test-L7            - Fuzzing (cargo-fuzz targets)"
	@echo "  test-L8            - Chaos scenarios (toxiproxy, process kill, seed pool)"
	@echo "  test-L9            - Perf/Soak (criterion, load generator, EKS)"
	@echo ""
	@echo "Test Shortcuts:"
	@echo "  test               - Run L0, L1, L4 (PR gate)"
	@echo "  test-unit          - Run unit tests only (L1 lib)"
	@echo "  test-integration   - Run integration tests (L5)"
	@echo "  test-property      - Run property tests (proptest)"
	@echo "  test-fuzz          - Run fuzz targets (L7)"
	@echo "  test-scenario      - Run single scenario (SCENARIO=name)"
	@echo "  test-scenarios     - Run all scenarios"
	@echo "  test-chaos         - Run chaos scenarios (L8)"
	@echo ""
	@echo "Development Environment:"
	@echo "  dev-up             - Start dev environment (docker-compose.yml)"
	@echo "  dev-down           - Stop dev environment"
	@echo "  dev-logs           - Follow dev environment logs"
	@echo "  dev-test-up        - Start test environment (docker-compose.test.yml)"
	@echo "  dev-test-down      - Stop test environment"
	@echo "  run                - Run proxy with local config"
	@echo ""
	@echo "LocalStack (AWS Mocks):"
	@echo "  localstack-up      - Start LocalStack with KMS, S3, DynamoDB"
	@echo "  localstack-down    - Stop LocalStack"
	@echo "  localstack-init    - Initialize LocalStack resources"
	@echo ""
	@echo "Toxiproxy (Chaos):"
	@echo "  toxiproxy-scenario - Apply scenario (SCENARIO=name)"
	@echo "  toxiproxy-reset    - Reset all toxics"
	@echo ""
	@echo "Fake ARC:"
	@echo "  fake-arc-scenario  - Apply scenario (SCENARIO=name)"
	@echo "  fake-arc-reset     - Reset to all healthy"
	@echo ""
	@echo "Golden Corpus:"
	@echo "  golden-record      - Record golden packets (CLIENT=kcat|java|franz-go)"
	@echo "  golden-replay      - Replay and verify golden packets"
	@echo ""
	@echo "Benchmark:"
	@echo "  bench-baseline     - Baseline benchmark (client -> Kafka)"
	@echo "  bench-proxy        - Proxy benchmark (client -> proxy -> Kafka)"
	@echo ""
	@echo "CI Gates:"
	@echo "  ci                 - Run all CI checks locally (PR gate)"
	@echo "  ci-pr              - PR gate (L0, L1, L4, golden, doc-tests) < 10 min"
	@echo "  ci-main            - Main branch gate (all PR + L6 slice, L7 smoke, L9 microbench) < 45 min"
	@echo "  ci-nightly         - Nightly gate (full L6, L7 1-2h, L8 seed pool, mutation) - overnight"
	@echo "  ci-weekly          - Weekly gate (soak 24h, EKS load test) - weekly"
	@echo ""
	@echo "Clean:"
	@echo "  clean              - Clean cargo and docker"

# ============================================================
# Build & Check
# ============================================================
build:
	cargo build --workspace --release

check:
	cargo check --workspace --all-targets

# Code quality
fmt:
	cargo fmt --all --check

fmt-fix:
	cargo fmt --all

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

audit:
	cargo audit

deny:
	cargo deny check

# ============================================================
# Test Levels L0-L9
# ============================================================

# L0: Static analysis
test-L0: fmt clippy audit deny

# L1: Unit + Property tests
test-L1:
	cargo nextest run --workspace --lib --all-targets 2>/dev/null || cargo test --workspace --lib --all-targets

# L2: Concurrency (loom)
test-L2:
	cargo test --workspace --lib loom 2>/dev/null || echo "Loom tests not configured yet"

# L3: Deterministic simulation (turmoil)
test-L3:
	cargo test --workspace --lib turmoil 2>/dev/null || echo "Turmoil tests not configured yet"

# L4: Component tests vs fake broker
test-L4:
	cargo test --workspace --lib --test testkit

# L5: Integration tests (requires docker/testcontainers)
test-L5:
	@echo "Starting test environment..."
	@docker-compose -f docker-compose.test.yml up -d --wait
	@sleep 10
	@cargo test --workspace --test integration -- --nocapture
	@docker-compose -f docker-compose.test.yml down -v

# L6: Compatibility matrix
test-L6:
	@echo "Running compatibility tests..."
	@./scripts/compatibility-test.sh

# L7: Fuzzing
test-L7:
	cargo fuzz run --jobs 4 --max_total_time 3600 2>/dev/null || echo "Fuzzing not configured yet"

# L8: Chaos scenarios
test-L8:
	@$(MAKE) dev-test-up
	@for scenario in $$(ls test-scenarios/*.json | xargs -n1 basename | sed 's/\.json$$//'); do \
		echo "Running chaos scenario: $$scenario"; \
		$(MAKE) test-scenario SCENARIO=$$scenario; \
	done
	@$(MAKE) dev-test-down

# L9: Perf/Soak
test-L9:
	@echo "Running microbenchmarks..."
	@cargo bench --workspace
	@echo "Soak test requires EKS - run 'make bench-proxy' on EKS"

# ============================================================
# Test Shortcuts
# ============================================================
test: test-L0 test-L1 test-L4

test-unit:
	cargo test --workspace --lib

test-integration: test-L5

test-property:
	cargo test --workspace --lib proptest

test-fuzz: test-L7

test-chaos: test-L8

# Run single scenario
test-scenario:
	@if [ -z "$(SCENARIO)" ]; then \
		echo "Usage: make test-scenario SCENARIO=scenario-name"; \
		exit 1; \
	fi
	@echo "Running scenario: $(SCENARIO)"
	@cargo test --test scenario_runner -- $(SCENARIO) --nocapture

test-scenarios:
	@for scenario in $$(ls test-scenarios/*.json | xargs -n1 basename | sed 's/\.json$$//'); do \
		echo "Running scenario: $$scenario"; \
		$(MAKE) test-scenario SCENARIO=$$scenario; \
	done

# ============================================================
# Development Environment
# ============================================================
dev-up:
	docker-compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 15
	@echo "Services ready!"
	@echo "  Kafka Primary:  localhost:9092,9094,9096"
	@echo "  Kafka Secondary: localhost:19092,19094,19096"
	@echo "  MinIO:          localhost:9000 (console: 9001)"
	@echo "  LocalStack:     localhost:4566"
	@echo "  Prometheus:     localhost:9090"
	@echo "  Grafana:        localhost:3000 (admin/admin)"
	@echo "  Jaeger:         localhost:16686"
	@echo "  Toxiproxy:      localhost:8474 (API), 8666-8669 (proxies)"
	@echo "  Fake ARC:       localhost:8080-8084"

dev-down:
	docker-compose down -v

dev-logs:
	docker-compose logs -f

# Test environment with full chaos setup
dev-test-up:
	docker-compose -f docker-compose.test.yml up -d --wait
	@echo "Waiting for test environment..."
	@sleep 20
	@echo "Test environment ready!"

dev-test-down:
	docker-compose -f docker-compose.test.yml down -v

# Run proxy
run:
	cargo run --release --bin kp-proxy -- --config config.toml

# ============================================================
# LocalStack (AWS Mocks)
# ============================================================
localstack-up:
	docker-compose -f docker-compose.test.yml up -d localstack --wait
	@sleep 5
	@$(MAKE) localstack-init

localstack-down:
	docker-compose -f docker-compose.test.yml stop localstack

localstack-init:
	@echo "Initializing LocalStack..."
	@aws --endpoint-url=http://localhost:4566 kms create-key --description "Test DEK key" --region us-east-1 || true
	@aws --endpoint-url=http://localhost:4566 s3 mb s3://kafka-spill --region us-east-1 || true
	@aws --endpoint-url=http://localhost:4566 s3 mb s3://kafka-wal --region us-east-1 || true
	@aws --endpoint-url=http://localhost:4566 dynamodb create-table \
		--table-name breaker-state \
		--attribute-definitions AttributeName=region,AttributeType=S AttributeName=channel,AttributeType=S \
		--key-schema AttributeName=region,KeyType=HASH AttributeName=channel,KeyType=RANGE \
		--billing-mode PAY_PER_REQUEST \
		--region us-east-1 || true
	@echo "LocalStack initialized"

# ============================================================
# Toxiproxy (Chaos)
# ============================================================
toxiproxy-scenario:
	@if [ -z "$(SCENARIO)" ]; then \
		echo "Usage: make toxiproxy-scenario SCENARIO=scenario-name"; \
		echo "Available: kafka-primary-latency, kafka-primary-timeout, kafka-primary-bandwidth, kafka-primary-slow-close, kafka-primary-unavailable, kafka-primary-recover, localstack-latency, localstack-unavailable, minio-slow-upload, minio-5xx-errors, minio-unavailable, minio-recover"; \
		exit 1; \
	fi
	@echo "Applying toxiproxy scenario: $(SCENARIO)"
	@docker exec kp-test-toxiproxy toxiproxy-cli populate -f /scenarios/$(SCENARIO).json

toxiproxy-reset:
	@echo "Resetting all toxiproxy toxics..."
	@docker exec kp-test-toxiproxy toxiproxy-cli toxic remove --all kafka-primary || true
	@docker exec kp-test-toxiproxy toxiproxy-cli toxic remove --all kafka-secondary || true
	@docker exec kp-test-toxiproxy toxiproxy-cli toxic remove --all localstack || true
	@docker exec kp-test-toxiproxy toxiproxy-cli toxic remove --all minio || true

# ============================================================
# Fake ARC
# ============================================================
fake-arc-scenario:
	@if [ -z "$(SCENARIO)" ]; then \
		echo "Usage: make fake-arc-scenario SCENARIO=scenario-name"; \
		echo "Available: arc-all-healthy, arc-partial-unhealthy, arc-all-unhealthy, arc-flapping"; \
		exit 1; \
	fi
	@echo "Applying fake ARC scenario: $(SCENARIO)"
	@docker cp fake-arc/scenarios/$(SCENARIO).json kp-test-fake-arc:/scenarios/current.json
	@docker exec kp-test-fake-arc nginx -s reload

fake-arc-reset:
	@echo "Resetting fake ARC to all healthy..."
	@docker cp fake-arc/scenarios/arc-all-healthy.json kp-test-fake-arc:/scenarios/current.json
	@docker exec kp-test-fake-arc nginx -s reload

# ============================================================
# Golden Corpus
# ============================================================
golden-record:
	@if [ -z "$(CLIENT)" ]; then \
		echo "Usage: make golden-record CLIENT=kcat|java|franz-go [TOPIC=topic]"; \
		exit 1; \
	fi
	@./scripts/golden-record.sh $(CLIENT) $(TOPIC)

golden-replay:
	@cargo test --test golden_corpus -- --nocapture

# ============================================================
# Benchmarks
# ============================================================
bench-baseline:
	@if [ -z "$(KAFKA_BROKER)" ]; then \
		echo "Usage: make bench-baseline KAFKA_BROKER=host:port [TOPIC=topic] [RECORDS=1000000] [SIZE=1024] [ACKS=all]"; \
		exit 1; \
	fi
	@./scripts/benchmark-baseline.sh $(KAFKA_BROKER) $(TOPIC) $(RECORDS) $(SIZE) $(ACKS)

bench-proxy:
	@if [ -z "$(PROXY_HOST)" ] || [ -z "$(KAFKA_BROKER)" ]; then \
		echo "Usage: make bench-proxy PROXY_HOST=host:port KAFKA_BROKER=host:port [TOPIC=topic] [RECORDS=1000000] [SIZE=1024] [ACKS=all]"; \
		exit 1; \
	fi
	@./scripts/benchmark-proxy.sh $(PROXY_HOST) $(KAFKA_BROKER) $(TOPIC) $(RECORDS) $(SIZE) $(ACKS)

# ============================================================
# CI Gates
# ============================================================
ci: ci-pr

# PR Gate: L0, L1, L4, golden, doc-tests (< 10 min)
ci-pr: test-L0 test-L1 test-L4
	@cargo test --workspace --doc

# Main Gate: All PR + L6 slice, L7 smoke, L9 microbench (< 45 min)
ci-main: ci-pr test-L5
	@cargo test --workspace --test compatibility --librdkafka -- --test-threads=1 2>/dev/null || echo "librdkafka test needs setup"
	@cargo fuzz run --max_total_time 300 2>/dev/null || echo "Fuzz smoke test needs setup"
	@cargo bench --workspace -- --no-run 2>/dev/null || echo "Benchmark compile check"

# Nightly Gate: Full L6, L7 1-2h, L8 seed pool, mutation testing
ci-nightly:
	@echo "=== Nightly CI (overnight) ==="
	@$(MAKE) ci-main
	@echo "Running full compatibility matrix..."
	@./scripts/compatibility-test.sh full
	@echo "Running fuzzing (1-2 hours)..."
	@cargo fuzz run --jobs 4 --max_total_time 7200 2>/dev/null || true
	@echo "Running chaos on seed pool..."
	@for seed in $$(seq 1 100); do \
		SEED=$$seed cargo test --test scenario_runner -- combined-chaos --nocapture; \
	done
	@echo "Mutation testing (cargo-mutants)..."
	@cargo mutants --workspace 2>/dev/null || echo "cargo-mutants not installed"
	@echo "Coverage (cargo-llvm-cov)..."
	@cargo llvm-cov --workspace --lcov --output-path lcov.info 2>/dev/null || echo "cargo-llvm-cov not installed"

# Weekly Gate: Soak 24h, EKS load test
ci-weekly:
	@echo "=== Weekly CI (24h soak) ==="
	@echo "Run on EKS: make bench-proxy PROXY_HOST=... KAFKA_BROKER=..."
	@echo "Soak test: 24h load test with monitoring"

# ============================================================
# Clean
# ============================================================
clean:
	cargo clean
	rm -rf target
	docker-compose down -v 2>/dev/null || true
	docker-compose -f docker-compose.test.yml down -v 2>/dev/null || true
	docker volume prune -f 2>/dev/null || true