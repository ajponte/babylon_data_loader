# Architecture & Data Flow

This document details the system design, components, runtime environments, and data flow of the `babylon_data_loader`.

---

## 1. System Architecture

The `babylon_data_loader` operates in two execution modes:
1. **Cloud Serverless Runtime (Production)**: Event-driven AWS Lambda ARM64 container invoked by Amazon S3 object uploads, persisting transaction datasets to **MongoDB Atlas**.
2. **Local Workstation Runtime (Development & Desktop)**: Standalone Go CLI or Wails desktop application interacting with a local filesystem and local MongoDB instance.

```mermaid
flowchart TD
    subgraph Cloud ["Production Ingestion (AWS & MongoDB Atlas)"]
        S3In[S3 Landing Bucket<br/>unprocessed/*.csv] -->|ObjectCreated Event| Lambda[AWS Lambda<br/>babylon-data-loader (ARM64)]
        SM[AWS Secrets Manager<br/>babylon/datalake/credentials] -.->|Resolve Credentials| Lambda
        Lambda -->|Parse & Ingest| Atlas[(MongoDB Atlas Cluster<br/>Database: datalake)]
        Lambda -->|Archive Processed| S3Out[S3 Landing Bucket<br/>processed/*.csv]
    end

    subgraph Local ["Local Development & CLI"]
        LocalFiles[Local CSV Files<br/>tmp/ or data/unprocessed/] -->|make run-ingest| LocalCLI[Data Loader CLI<br/>main.go]
        LocalCLI -->|Direct Connection| LocalMongo[(Local MongoDB Docker<br/>localhost:27017)]
        SynthGen[Synthetic Generator] -->|make run-generate| LocalFiles
        SynthGen -->|make run-generate-mongo| LocalMongo
    end
```

---

## 2. Core Components

1. **Entrypoint ([`main.go`](../main.go))**: CLI router that handles command-line arguments and routes requests to either the synthetic data generator or the local file ingestion process.
2. **Lambda Handler ([`cmd/lambda/`](../cmd/lambda))**: Serverless entrypoint subscribing to S3 event notifications (`events.S3Event`), downloading objects to `/tmp`, orchestrating ingestion, and archiving to `processed/`.
3. **Context & Logging ([`appcontext/`](../appcontext))**: Standardizes structured context propagation with `slog` logger instances attached.
4. **Configuration & Secrets ([`config/`](../config))**: Resolves database connection credentials dynamically from AWS Secrets Manager (`MONGO_SECRET_ID`) or local environment fallback (`MONGO_URI`) with thread-safe in-memory caching.
5. **CSV Parser ([`csv/`](../csv))**: Flexibly parses bank and credit card CSV statements with dynamic header mapping and type coercion.
6. **Datalake Engine ([`datalake/`](../datalake))**: Represents core business entities, interfaces, and extraction services (like [`datasource/`](../datalake/datasource)).
7. **Ingest Sink ([`ingest/`](../ingest))**: Coordinates the flow of reading unprocessed files, invoking the CSV parser, and calling the repository to bulk upsert records.
8. **Storage Repository ([`storage/`](../storage))**: Interacts with MongoDB (both local instances and MongoDB Atlas). Implements bulk upserts into `transactions_<datasource>` collections and writes sync audits to `dataSync`.
9. **Synthetic Generator ([`synthetic/`](../synthetic))**: Generates deterministic mock transaction datasets to allow realistic QA testing without handling real financial PII.

---

## 3. Data Ingestion Lifecycles

### 3.1. Cloud Event-Driven Flow (AWS Lambda)
1. A financial statement CSV is uploaded to `s3://babylon-datalake-landing-${account_id}/unprocessed/` (e.g. via [`tools/upload-csv-s3.sh`](../../tools/upload-csv-s3.sh)).
2. S3 fires an `ObjectCreated` event notification triggering `babylon-data-loader`.
3. The Lambda handler checks its in-memory credential cache. If cold, it fetches connection details from AWS Secrets Manager secret `babylon/datalake/credentials`.
4. The handler downloads the S3 file to local scratch storage (`/tmp/unprocessed/`).
5. The `ingest.Sink` invokes the CSV parser and bulk upserts records into MongoDB Atlas under database `datalake` and collection `transactions_<datasource>`.
6. Upon successful persistence, the Lambda function copies the S3 object to `processed/`, deletes the original object from `unprocessed/`, and cleans up `/tmp`.

For detailed cloud datalake specifications, see [`docs/specs/mongodb-atlas-datalake.md`](specs/mongodb-atlas-datalake.md).

### 3.2. Local CLI Ingestion Flow
1. The developer executes `make run-ingest` (which runs `data-loader ingest`).
2. The `ingest.Sink` scans the configured local directory (e.g., `data/unprocessed/`) for CSV files.
3. For each file, the `csv.Parser` maps columns to structured transaction entities.
4. The repository performs a bulk upsert of records into the target MongoDB collections.
5. On completion, the local files are relocated to `data/processed/`.
