# CLAUDE.md for babylon_data_loader

This guide provides instructions for building, running, testing, and developing within the `babylon_data_loader` repository.

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
