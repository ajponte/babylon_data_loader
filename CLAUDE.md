# CLAUDE.md for babylon_data_loader

This guide provides instructions for building, running, testing, and developing within the `babylon_data_loader` repository.

## Documentation & Agent Harness

This project contains a comprehensive agent documentation harness under the [docs/](file:///Users/aponte/personal_workspace/babylon-2.0/babylon_data_loader/docs) folder. Refer to these files for deeper context:
- **[Harness Index (README)](file:///Users/aponte/personal_workspace/babylon-2.0/babylon_data_loader/docs/README.md)**: Main entry point for project docs.
- **[Agent Personas](file:///Users/aponte/personal_workspace/babylon-2.0/babylon_data_loader/docs/agent_personas.md)**: Specific instructions, prompts, and expectations for Software Engineering and DevOps agent roles.
- **[Architecture & Data Flow](file:///Users/aponte/personal_workspace/babylon-2.0/babylon_data_loader/docs/architecture.md)**: Details on packages, data pathways, and components.
- **[PII & Security Guidelines](file:///Users/aponte/personal_workspace/babylon-2.0/babylon_data_loader/docs/pii_handling.md)**: Rules for preserving PII confidentiality, logging securely, and generating mock datasets.
- **[Development & Testing Guide](file:///Users/aponte/personal_workspace/babylon-2.0/babylon_data_loader/docs/development.md)**: Prerequisites, formatting tools, linting, and troubleshooting.

---

## Commands

### Build and Clean
- **Build application**: `make build` (creates executable in `out/data-loader`)
- **Clean build artifacts**: `make clean`
- **Tidy Go modules**: `make tidy`
- **Vendoring dependencies**: `make vendor`

### Run and Ingestion
- **Run data ingestion**: `make run` or `make run-ingest`
- **Generate synthetic CSV data**: `make run-generate` (creates synthetic transaction CSVs in `tmp/`)
- **Generate and persist synthetic data to Mongo**: `make run-generate-mongo`

### Test and Quality
- **All Quality Checks (Lint, Format, Vet)**: `make check-quality`
- **Run unit tests**: `make unit-test`
- **Run tests with JSON output (CI)**: `make test-ci`
- **Show test coverage in HTML**: `make coverage`
- **Format code**: `make fmt` (runs `goimports` and `gofumpt`)
- **Lint code**: `make lint` (runs `golangci-lint`)
- **Vet code**: `make vet` (runs `go vet`)

---

## Security & PII Guidelines

> [!IMPORTANT]
> The `babylon` data lake processes Personally Identifiable Information (PII) including Account IDs, balance amounts, and transaction descriptions. Follow these guidelines strictly:

1. **No Real PII**: Do not hardcode, commit, or check in real datasets, transaction records, or any production database dumps to this repository.
2. **Log Safety**: Never include real PII (such as account details, names, sensitive description fields, or exact balances) in logs. Keep `slog` log fields generic and aggregate.
3. **Use Synthetic Data for Dev/Test**: Always use the built-in synthetic generators to produce non-sensitive mock datasets for testing and local environment setups:
   - To generate local CSVs for testing: `go run main.go generate-synthetic-data --rows 100 --dir tmp/synthetic`
   - To populate local MongoDB: `go run main.go generate-synthetic-data --rows 100 --persist-to-mongo`
4. **Environment Variables**: Never hardcode credentials. Ensure Mongo/DB configurations are fetched from the environment:
   - `MONGO_HOST`
   - `MONGO_USER`
   - `MONGO_PASSWORD`
   - `MONGO_URI`

---

## Code Guidelines & Formatting

- **Formatting**: We enforce strict formatting rules. Always run `make fmt` before committing.
- **Function Comments**: Write clear comments on exported package functions and structs.
- **Inline Comments**: Use inline comments sparsely and only when describing complex business logic or edge cases.
- **Testing**: Always run `make unit-test` after making changes to verify correctness.
