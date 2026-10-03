# AGENTS.md for babylon_data_loader

This guide provides instructions for building, running, testing, and developing within the `babylon_data_loader` repository.

---

## Architecture Overview

`babylon_data_loader` is a high-performance financial data ingestion pipeline designed for batch transaction feeds, local desktop execution, and serverless cloud execution.

1. **Go Ingestion Engine (`pkg/ingest`, `pkg/csv`, `pkg/datasource`)**:
   - Parses, validates, and normalizes transactional financial feeds from diverse sources.
   - Maintains strict financial precision (no floating-point types for currency) and guarantees idempotent record processing.
2. **MongoDB Datalake Repository (`pkg/datalake`)**:
   - High-throughput persistence layer interfacing with MongoDB Atlas.
   - Uses bulk upserts with deterministic idempotency keys to eliminate duplicate transaction records.
3. **AWS Lambda S3 Adapter (`cmd/lambda`, `pkg/storage`, `pkg/config`)**:
   - Event-driven serverless runner triggered by Amazon S3 `ObjectCreated` events.
   - Resolves database credentials dynamically from AWS Secrets Manager with in-memory thread-safe caching.
   - Stages landing files in `/tmp/unprocessed`, executes ingestion, and archives completed feeds to S3 `processed/`.
4. **Wails Desktop Application (`desktop/`)**:
   - Local developer and operations UI built with Go and Wails v2 with a Vite/frontend interface.
   - Allows interactive data ingestion, local debugging, and manual data inspections.

---

## Makefile Automation Targets

The repository includes a comprehensive `makefile` for build, test, and container workflows:

### Quality & Testing
- `make` / `make all`: Runs code quality checks (`lint`, `fmt`, `vet`), unit tests, and builds the CLI binary.
- `make check-quality`: Runs `golangci-lint`, `goimports`/`gofumpt`, and `go vet`.
- `make fmt`: Formats code strictly with `goimports` and `gofumpt`.
- `make lint`: Runs `golangci-lint` against all packages.
- `make vet`: Runs standard `go vet ./...`.
- `make unit-test`: Executes unit test suite across all packages with coverage reporting.
- `make test-lambda`: Runs tests for Lambda handler (`cmd/lambda`) and configuration (`config`) with Go race detection (`-race`).

### Build & Container Packaging
- `make build`: Compiles the core CLI executable into `out/data-loader`.
- `make build-lambda`: Compiles static Linux ARM64 binary into `out/bootstrap` for AWS Lambda Graviton.
- `make docker-build-lambda`: Builds the multi-stage ARM64 container image (`babylon-data-loader-lambda:latest`) based on Amazon Linux 2023.
- `make clean`: Cleans compiled binaries (`out/`), coverage artifacts, and Go test cache.

### Execution & Development
- `make run` / `make run-ingest`: Builds and executes the data loader in ingestion mode.
- `make run-generate`: Generates local synthetic CSV transaction records for testing.
- `make run-generate-mongo`: Generates synthetic transactions and writes directly to MongoDB.
- `make run-desktop`: Launches the Wails desktop application in local development mode.

---

## Security & PII Guidelines

1. **Zero Real PII in Repository**:
   - Never commit, push, or store real financial transaction records, account numbers, or personal identifying information.
2. **Log Safety & Scrubbing**:
   - Never output raw PII (names, sensitive transaction memos, raw account identifiers, or account balances) into logs.
   - Use structured `slog` logging with aggregate counts, status indicators, and non-sensitive identifiers.
3. **Synthetic Data for Development and Testing**:
   - Always rely on synthetic data generators (`make run-generate`) or mock fixtures for testing and development.
4. **Credential Isolation**:
   - Never hardcode database URIs or credentials in configuration files.
   - In cloud deployments, credentials are automatically resolved from AWS Secrets Manager. Locally, fallback to environment variables (`MONGO_URI`, `MONGO_HOST`, `MONGO_USER`, `MONGO_PASSWORD`).

---

## Coding Standards

- **Financial Precision**: Do not use floating-point types (`float32`, `float64`) for financial currency amounts.
- **Modularity & DDD**: Keep clear separation between data lake repositories, storage adapters, parsing engines, and invocation entry points.
- **Pre-commit Quality**: Always verify changes by running `make check-quality` and `make unit-test`.
