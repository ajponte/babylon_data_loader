# AGENTS.md for babylon_data_loader

This guide provides instructions for building, running, testing, and developing within the `babylon_data_loader` repository.

---

## Architecture Overview

`babylon_data_loader` is a high-performance financial data ingestion system designed for batch transaction feeds, local desktop execution, and serverless cloud execution, structured around Hexagonal Architecture (Ports and Adapters).

1. **Entrypoints & Driving Adapters (`cmd/cli`, `cmd/lambda`, `desktop`)**:
   - `cmd/cli`: Command-line interface providing `ingest` and `generate` commands for local batch processing and test data creation.
   - `cmd/lambda`: Event-driven serverless runner triggered by Amazon S3 `ObjectCreated` events. Resolves database credentials dynamically from AWS Secrets Manager (`MONGO_SECRET_ID`) with in-memory thread-safe caching. Streams S3 objects directly in-memory via `port.ObjectStore` (eliminating `/tmp` staging) and archives completed feeds to `processed/`.
   - `desktop`: Local developer and operations UI built with Go and Wails v2 with a React/Vite frontend. Provides interactive data ingestion, real-time progress events (`ingest:progress`), audit history queries, and graceful `OnShutdown` resource teardown.
2. **Application Layer (`internal/app/pipeline`, `internal/app/ingest`)**:
   - `internal/app/pipeline`: Orchestrates file streaming, source parsing, batch upserts, audit history logging (`dataSync`), and archival.
   - `internal/app/ingest`: Application-level coordinator binding source resolution and storage dependencies.
3. **Domain Core (`internal/domain/model`, `internal/domain/port`)**:
   - `internal/domain/model`: Pure domain entities (`TransactionRecord`, `FidelityPositionRecord`, `DataSyncLog`, `LineageMetadata`, `SourceType`) maintaining strict financial precision (no floating-point currency representation).
   - `internal/domain/port`: Cohesive interface contracts for repository persistence (`port.Repository`), audit log queries (`port.SyncLogReader`), object storage (`port.ObjectStore`), progress notifications (`port.ProgressNotifier`), and source extraction (`port.SourceExtractor`).
4. **Driven Infrastructure Adapters (`internal/infra/sources/*`, `internal/infra/storage/*`)**:
   - Sources (`internal/infra/sources/chase`, `internal/infra/sources/fidelity`, `internal/infra/sources/synthetic`): Extract and normalize bank, brokerage, and deterministic synthetic datasets into canonical domain records.
   - Storage (`internal/infra/storage/mongo`, `internal/infra/storage/s3`, `internal/infra/storage/localfs`): High-throughput persistence to MongoDB Atlas / local Docker with deterministic idempotency keys, in-memory S3 streaming, and local filesystem management.
5. **Composition Roots (`config`, `appcontext`)**:
   - Environment and secrets configuration management, and structured `slog` context propagation.

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
- `make test-lambda`: Runs tests for Lambda handler (`cmd/lambda`), configuration (`config`), and S3 storage (`internal/infra/storage/s3`) with Go race detection (`-race`).
- Desktop Testing:
  - `(cd desktop/frontend && npm test)`: Runs Vitest suite for React components.
  - `(cd desktop && go test -race ./...)`: Runs race-detected Go tests for desktop Wails adapter.

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

- **Clean & Comprehensive Coding Patterns**:
  - **Function Decomposition**: Decompose long functions into smaller, single-responsibility helper functions with clear domain boundaries.
  - **Low Cognitive Complexity**: Maintain low cyclomatic complexity; use early returns and guard clauses; avoid deeply nested loops and conditionals.
  - **Test Boilerplate Elimination**: Eliminate repetitive struct-literal construction and assertion boilerplate in unit tests using builders (`newValidTest*`) and focused assertion helpers (`assert*`).
  - **Declarative Table-Driven Tests**: Keep table-driven test cases declarative, concise, and focused on verifying a single invariant.
  - **Token Normalization Isolation**: Decouple raw string token sanitization/normalization from core data parsing, business validation, and domain models.
- **Financial Precision**: Do not use floating-point types (`float32`, `float64`) for financial currency amounts.
- **Modularity & DDD**: Keep clear separation between data lake repositories, storage adapters, parsing engines, and invocation entry points.
- **Pre-commit Quality**: Always verify changes by running `make check-quality` and `make unit-test`.

---

## Git & Pull Request Governance

> [!IMPORTANT]
> **PULL REQUEST MERGE POLICY**:
> Agents must **NEVER** merge pull requests (`gh pr merge`, GitHub API merges, or direct merges into `main`) on the user's behalf without explicit, unambiguous permission from the user.

- **Human-in-the-Loop Review**: After opening a PR and verifying that automated CI checks pass, report the PR URL, summary of changes, and verification records to the user.
- **Await Instruction**: Stop execution and allow the user to review and merge the pull request manually, or wait for explicit instructions to merge.

