# Local Development & Testing Guide

This document describes how to build, test, and troubleshoot the `babylon_data_loader` locally.

## Prerequisite Tools

- **Go**: Version 1.26 (or matching version specified in [go.mod](file:///Users/aponte/personal_workspace/babylon-2.0/babylon_data_loader/go.mod))
- **MongoDB**: A running MongoDB instance. Default credentials and hosts can be overridden via env variables:
  - `MONGO_HOST=localhost`
  - `MONGO_USER=babylon`
  - `MONGO_PASSWORD=babylonpass`
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
To run all unit tests:
```bash
make unit-test
```

To see test coverage as an interactive webpage:
```bash
make coverage
```

## Troubleshooting

- **MongoDB connection issues**: If you encounter connection errors during ingestion, verify the `MONGO_URI` environment variable or check that your local MongoDB container is running.
- **Go version conflicts**: If you upgraded your machine's Go compiler and run into linter issues, check the [README.md](file:///Users/aponte/personal_workspace/babylon-2.0/babylon_data_loader/README.md) instructions on upgrading the Go environment.
