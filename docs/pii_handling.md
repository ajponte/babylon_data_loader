# PII & Security Guidelines

The `babylon` data lake stores Personally Identifiable Information (PII) to support data loader requirements. This document outlines rules and practices to maintain data security and avoid leaks.

## Identifying PII in Babylon

The following fields in the transaction schema are considered PII or sensitive:
- **AccountID**: Internal bank/account identifiers.
- **Balance / Amount**: Financial details which must be treated with high confidentiality.
- **Description / Details**: May contain names, vendor descriptors, or other tracking tokens containing personal information.

## Safety Rules for Developers & Agents

### 1. No Production Data in the Repository
Never copy, download, or commit raw transaction records, CSVs, or MongoDB exports from a production or staging environment into the repository. 

### 2. Log Scrubbing
Ensure logs do not print actual PII values:
- Use structured log fields in `slog`.
- Do not output entire records or transaction bodies in `slog.ErrorContext` or `slog.InfoContext`. Instead, log metadata like row count, collection names, file names, or truncated non-sensitive ID formats.

### 3. Generate Mock Data Locally
Utilize the synthetic data pipeline for all manual and automated testing. 

- To output a synthetic CSV:
  ```bash
  make run-generate
  ```
- To populate local MongoDB with synthetic data:
  ```bash
  make run-generate-mongo
  ```

This ensures zero risk of leakages while maintaining representative schemas and volumes.
