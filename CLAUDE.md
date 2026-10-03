# CLAUDE.md for babylon_data_loader

This guide provides instructions for building, running, testing, and developing within the `babylon_data_loader` repository.

## Documentation & Agent Harness

This project contains a comprehensive agent documentation harness under the [docs/](docs) folder. Refer to these files for deeper context:
- **[Harness Index (README)](docs/README.md)**: Main entry point for project docs.
- **[Agent Personas](docs/agent_personas.md)**: Specific instructions, prompts, and expectations for Software Engineering and DevOps agent roles.
- **[Architecture & Data Flow](docs/architecture.md)**: Details on packages, data pathways, and components.
- **[PII & Security Guidelines](docs/pii_handling.md)**: Rules for preserving PII confidentiality, logging securely, and generating mock datasets.
- **[Development & Testing Guide](docs/development.md)**: Prerequisites, formatting tools, linting, and troubleshooting.

---

## Commands

### Build and Clean
- **Build application**: `make build` (creates executable in `out/data-loader`)
- **Build Lambda binary**: `make build-lambda` (compiles static Linux/ARM64 binary to `out/bootstrap`)
- **Build Lambda container**: `make docker-build-lambda` (builds multi-stage ARM64 Docker container image)
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
- **Run Lambda tests**: `make test-lambda` (runs race-detected unit tests for Lambda handler, config, and storage)
- **Run tests with JSON output (CI)**: `make test-ci`
- **Show test coverage in HTML**: `make coverage`
- **Format code**: `make fmt` (runs `goimports` and `gofumpt`)
- **Lint code**: `make lint` (runs `golangci-lint`)
- **Vet code**: `make vet` (runs `go vet`)

---

## Continuous Delivery & Deployment

The delivery pipeline is automated via GitHub Actions ([`.github/workflows/cd.yml`](.github/workflows/cd.yml)):

- **Automated Production Deployment (`main`)**:
  - Merging or pushing to `main` executes pre-flight checks, SemVer resolution, cross-compiled CLI binary publishing to GitHub Releases, and Linux ARM64 container image compilation.
  - The container image is pushed to Amazon ECR (`ajp/babylon`) tagged with SemVer, `data-loader-latest`, and immutable commit SHA `data-loader-sha-<short_sha>`.
  - [`.github/scripts/deploy-lambda.sh`](.github/scripts/deploy-lambda.sh) automatically updates the production AWS Lambda function (`babylon-data-loader`) with the commit SHA image and awaits confirmation that the function state is `Active` and update status is `Successful`.
- **Branch Test Execution (`workflow_dispatch`)**:
  - Developers can trigger container builds from any feature branch using GitHub CLI:
    ```bash
    gh workflow run cd.yml \
      --ref feature/my-feature-branch \
      -f deploy_binaries=false \
      -f deploy_ecr=true \
      -f is_test_image=true
    ```
  - Pushes test images tagged `data-loader-test-<branch>-<short_sha>` and `data-loader-test-latest` to Amazon ECR.
  - Automatically bypasses GitHub Releases and suppresses Lambda deployment (`update_lambda` evaluates to `false`), isolating production Lambda from branch experiments.
  - Ephemeral test images are governed by an automated 14-day Amazon ECR lifecycle expiration policy.

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

---

## Git & Pull Request Safety Constraints

- **No Unauthorized PR Merges**: Never merge pull requests (`gh pr merge` or direct merges into `main`) on the user's behalf without explicit permission.
- **Review Handoff**: When a PR is created and checks pass, provide the pull request link to the user and wait for human review and merge instructions.

