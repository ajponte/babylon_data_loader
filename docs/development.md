# Local Development & Testing Guide

This document describes how to build, test, and troubleshoot the `babylon_data_loader` locally.

## Prerequisite Tools

- **Go**: Version 1.26 (or matching version specified in [go.mod](../go.mod))
- **MongoDB**: A running MongoDB instance. Default credentials and hosts can be overridden via env variables:
  - `MONGO_HOST=localhost`
  - `MONGO_USER=<MONGO_USER>`
  - `MONGO_PASSWORD=<MONGO_PASSWORD>`
- **GNU Make**: Used to run commands easily.

## Development Tasks

### 1. Code Formatting
Before committing, always run the formatters to check formatting rules:
```bash
make fmt
```
This runs `goimports` and `gofumpt` to format code according to our Go style standards.

### 2. Linting & Vet
To verify package imports, styling rules, and common programming errors:
```bash
make lint
make vet
```
Both are included in the quality checklist:
```bash
make check-quality
```

### 3. Testing

#### Unit Testing
To run all hermetic unit tests:
```bash
make unit-test
```

To run tests with CI JSON output:
```bash
make test-ci
```

To see test coverage as an interactive webpage:
```bash
make coverage
```

#### Integration Testing
Storage integration tests verify repository operations, unique index deduplication, conflict detection, and BSON type parity against a live MongoDB instance (such as the local Docker container in `babylon_deploy/local`):
```bash
MONGO_IT_URI="mongodb://localhost:27017" make test-integration
```

##### Safety Guards & Ephemeral Databases
The integration test suite ([`internal/infra/storage/mongo/integration_test.go`](../internal/infra/storage/mongo/integration_test.go)) enforces strict isolation guards:
- **Build Tag Separation**: Tagged with `//go:build integration`. Standard `make test-ci` and `go test ./...` exclude integration tests, ensuring unit tests remain 100% hermetic and network-free.
- **Protocol Guard**: Rejects connection URIs with the `mongodb+srv://` scheme to prevent accidental execution against remote MongoDB Atlas clusters.
- **Database Name Guard**: Forbids running against any database named `datalake` (case-insensitive check).
- **Ephemeral Database Lifecycle**: Dynamically provisions a distinct database named `babylon_it_<unixnano>` for each test run and registers an automated cleanup hook via `t.Cleanup()` that drops the database and closes repository connections upon completion.
- **Environment Gating**: The suite requires `MONGO_IT_URI`. The `make test-integration` target fails fast if the variable is unset, while the Go test runner invokes `t.Skip` if run directly without it.

##### Continuous Integration (CI)
Integration tests execute automatically in GitHub Actions ([`.github/workflows/go.yml`](../.github/workflows/go.yml)) on all pushes and pull requests targeting `main`. The `integration-test` job provisions a healthy containerized MongoDB service (`mongo:latest`) on port `27017` and executes `make test-integration` with `MONGO_IT_URI: mongodb://localhost:27017`.

## Troubleshooting

- **MongoDB connection issues**: If you encounter connection errors during ingestion, verify the `MONGO_URI` environment variable or check that your local MongoDB container is running.
- **Go version conflicts**: If you upgraded your machine's Go compiler and run into linter issues, check the [README.md](../README.md) instructions on upgrading the Go environment.
