# Hexagonal Architecture Specification: Application Layer & Entrypoint Pipeline

**Target Packages**: `internal/app/pipeline/`, `internal/app/ingest/`, `internal/infra/storage/localfs/`, `cmd/cli/`, `cmd/lambda/`, `desktop/`  
**Architectural Role**: Hexagonal Application Core (Orchestrator & Ingestion Use Cases) and Primary/Secondary Adapters  
**Document Status**: Active / Canonical Application Pipeline Specification (Phase 4)  
**Primary Inbound Ports / Consumers**: CLI (`cmd/cli`), Lambda (`cmd/lambda`), Desktop (`desktop`)  
**Driven Ports**: `port.Extractor`, `port.Repository`, `port.ObjectStore`, `port.ProgressNotifier`, `port.SyncLogReader`  
**Parent Document**: [`docs/specs/hexagonal-storage-adapters.md`](./hexagonal-storage-adapters.md), [`docs/specs/hexagonal-domain-core.md`](./hexagonal-domain-core.md)  

---

## 1. Executive Summary & Headline Invariant

Phase 4 executes the application-layer cutover. It wires the extractors implemented in Phase 2 and secondary storage adapters built in Phase 3 into a unified application orchestrator (`internal/app/pipeline`), drives it via domain use cases (`internal/app/ingest`), and migrates all three composition entrypoints: AWS Lambda (`cmd/lambda`), the local CLI (`cmd/cli`), and the Wails desktop application (`desktop`). Upon completion of Phase 4, no entrypoint imports legacy packages (`ingest/`, `datalake/`, `csv/`, or `storage/`), unblocking Phase 5 cleanup.

**Headline Invariant**: For the same input file, the cut-over runtimes write identical record documents and identical `dataSync` audit rows as the legacy runtimes. The only approved differences are the Phase 3 exceptions (D4 synthetic collection shape unified with Chase, and `source.s3Uri` now populated from entrypoint metadata) and the divergences explicitly recorded in §6.

---

## 2. Layer Map & Package Architecture

The application pipeline is organized into strict hexagonal layers enforced by `depguard`:

| Directory / Component | Layer | Allowed Imports / Isolation Boundary | Primary Responsibility |
| :--- | :--- | :--- | :--- |
| `internal/app/pipeline/` | Application Orchestration | `$gostd`, `internal/domain/**`, `internal/app/**` | Generic single-stream batch execution, extractor registry, failure-auditing policy, and progress telemetry. |
| `internal/app/ingest/` | Application Use Cases | `$gostd`, `internal/domain/**`, `internal/app/**` | Filename resolution (`ResolveSource`), stream fingerprinting (`Fingerprint`), object download loop, archive dispatch, and `RunStats`. |
| `internal/infra/storage/localfs/` | Secondary Driven Adapter | `$gostd`, `internal/domain/**`, `internal/infra/storage/localfs/**` | Implements `port.ObjectStore` over local filesystem directories (`List`, `DownloadStream`, `ArchiveProcessed`). |
| `cmd/cli/` | Primary Driving Adapter | `internal/**`, `config`, `appcontext` | CLI composition root (`ingest`, `generate-synthetic-data`). Drives `ingest.Ingestor` with `localfs.Store` and CLI `slog` progress. |
| `cmd/lambda/` | Primary Driving Adapter | `internal/**`, `config`, `appcontext` | AWS Lambda composition root (`handler.go`). Consumes S3 events, resolves streams via `s3.Client`, and writes to `mongo.Repository`. |
| `desktop/` | Primary Driving Adapter | `internal/**`, `desktop/**` (module `babylon/dataloader/desktop`) | Wails desktop application. Ingests local files in place via `ingest.Ingestor` and reports progress via `WailsProgressNotifier`. |

---

## 3. `pipeline.Run` Contract Table & Failure-Audit Policy

Authoritative implementation reference: `internal/app/pipeline/pipeline.go`.

The `pipeline.Pipeline` orchestrates stream extraction, provenance copying, repository persistence, archival finalization, and audit logging.

### 3.1 `pipeline.Run` Contract Table

| Step | Trigger / Condition | Return Value | Audited? | Audit Characteristics & Policy |
| :--- | :--- | :--- | :---: | :--- |
| **Context Check** | `ctx.Err() != nil` prior to execution | `(nil, ctx.Err())` | No | Short-circuit; no side effects. |
| **Validation** | `req.Reader == nil` or `req.Meta.Validate() != nil` | `(nil, err)` | No | Cannot construct valid `SyncLog` (U1). |
| **Source Check** | Unregistered `req.SourceType` | `(nil, ErrUnknownSourceType)` | No | Pre-resolution failure; excluded from audit (U1). |
| **Extraction Failure** | `ext.Extract(ctx, r, meta)` returns error | `(failedLog, err)` | Yes | Audited with fallback collection, `recordCount: 0`, detached context (P3). |
| **Provenance Copy** | Extraction succeeds | — | — | Sets `batch.SourceURI = req.Meta.SourceURI` (P8). |
| **SaveBatch Error (With Log)** | `repo.SaveBatch` returns `(failedLog, err)` | `(failedLog, err)` | Yes | Restamps `SyncedAt` and `DurationMs` (P4); audited with detached context (P3). |
| **SaveBatch Error (Nil Log)** | `repo.SaveBatch` returns `(nil, err)` | `(failedLog, err)` | Yes | Constructs failed log with fallback collection; audited with detached context (P3). |
| **Finalize Failure** | `req.Finalize(ctx)` returns error after save | `(failedLog, wrappedErr)` | Yes | Audits single FAILED log: `storage succeeded but file archival failed: <err>` (P6). **No** success log. |
| **Success** | Persistence and finalize complete | `(successLog, nil)` | Yes | Restamps `SyncedAt` and `DurationMs` (P4); calls `LogSync(ctx, successLog)`. |
| **Success Audit Failure** | `LogSync` fails on success log | `(successLog, ErrAuditWrite)` | Yes | Non-fatal: returns success log with wrapped `ErrAuditWrite` (P5). |
| **Failure Audit Failure** | `LogSync` fails during failure audit | `(failedLog, joinedErr)` | Yes | `errors.Join(cause, fmt.Errorf("%w: %w", ErrAuditWrite, auditErr))` (P3). |

### 3.2 Failure-Audit Policy & Invariants

1. **Detached Context (P3, Phase 3 E5)**: Every failure audit is written using `context.WithoutCancel(ctx)` with a detached timeout (`DefaultAuditTimeout = 5s`). If the parent context was cancelled mid-flight, the failure audit write is still guaranteed 5 seconds to persist to `dataSync`.
2. **Error Joining (P3)**: If the failure-audit write itself fails, the resulting error is joined via `errors.Join(cause, ...)` to the original cause rather than swallowed.
3. **Audit Timing Parity (P4)**: The orchestrator restamps `SyncedAt` with the pipeline start timestamp and `DurationMs` with the end-to-end execution duration for both success and failure logs, overriding storage adapter write-only timings.
4. **Non-Fatal Success Audit (P5)**: When data records are persisted successfully but writing the success `SyncLog` fails, `Run` returns `(successLog, fmt.Errorf("%w: %w", ErrAuditWrite, err))`. Callers treat the ingestion as succeeded while logging the audit warning.
5. **Pre-Extraction Failure Audit (`RecordFailure`)**: For failures occurring prior to batch creation (such as `ErrInputTooLarge`), `RecordFailure` writes a FAILED audit log with the registered fallback collection and empty `FileHash` using the detached context.

---

## 4. Source Resolution, Fingerprinting & Registration Rules

Authoritative implementation reference: `internal/app/ingest/source.go`.

### 4.1 Source Resolution Rules (`ResolveSource`)

Filename resolution matches legacy `datalake/datasource.GenericExtractor.ExtractInfo` semantics using standard base-name extraction across S3 keys and OS paths. Resolution evaluates in strict legacy order:

1. **Chase**: Regexp `chase(\d{4})` on lower-cased base name $\rightarrow$ `("chase", accountID)`.
2. **Fidelity**: `fidelity` anywhere in lower-cased path OR prefix `portfolio_positions` on base name $\rightarrow$ `("fidelity", "portfolio")`.
3. **Synthetic**: `synthetic` anywhere in base name $\rightarrow$ `("synthetic", "0000")`.
4. **Fallback**: If no rule matches, returns `ErrUnresolvableSource`. Pre-resolution failures are logged and counted in stats, but unaudited (U1).

### 4.2 Stream Fingerprinting (`Fingerprint`)

- Reads stream into an in-memory buffer up to `DefaultMaxInputBytes = 32 MiB` (P7).
- Computes SHA-256 hash returned as a lowercase hex string.
- If stream length exceeds the cap, immediately returns `ErrInputTooLarge` without hashing.
- Returns `*bytes.Reader` allowing downstream extractors to read the buffered stream from offset 0.

### 4.3 Extractor & Destination Registration Table

Composition roots register extractors using `pipeline.Register(sourceType, ext, fallbackDestination)`.

| Source Type | Extractor Adapter | Target Collection / Fallback Destination | Record Model | Description |
| :--- | :--- | :--- | :--- | :--- |
| `chase` | `chase.Extractor` | `model.DestChaseChecking` / `transactions_chase` | `model.ChaseTransaction` | Checking and credit card statements. |
| `fidelity` | `fidelity.Extractor` | `model.DestFidelityPositions` / `positions_fidelity` | `model.FidelityPosition` | Brokerage position exports. |
| `synthetic` | `synthetic.CSVExtractor` (2G) | `model.DestSynthetic` / `transactions_synthetic` | `model.ChaseTransaction` | Dedicated parser preserving `details`, `balance`, `checkOrSlipNum` (U2). |

---

## 5. Runtime Wiring & Flow

### 5.1 Lambda Runtime Flow (`cmd/lambda`)

```mermaid
sequenceDiagram
    autonumber
    participant S3 as AWS S3
    participant H as Lambda Handler
    participant I as ingest.Ingestor
    participant S3C as s3.Client
    participant P as pipeline.Pipeline
    participant R as mongo.Repository

    S3->>H: S3 Object Created Event
    H->>I: IngestObject(ctx, bucket, key)
    I->>S3C: DownloadStream(ctx, key)
    S3C-->>I: io.ReadCloser (stream)
    I->>I: Fingerprint (SHA-256 <= 32MB)
    I->>P: Run(ctx, Request{SourceURI: s3://bucket/key})
    P->>R: SaveBatch(ctx, batch)
    R-->>P: SyncLog (records persisted)
    P->>S3C: ArchiveProcessed(ctx, key)
    S3C-->>P: Object archived to processed/
    P->>R: LogSync(ctx, successSyncLog)
    R-->>P: nil
    P-->>I: (SyncLog, nil)
    I-->>H: IngestResult
    H-->>S3: 200 OK
```

### 5.2 CLI Runtime Flow (`cmd/cli`)

```mermaid
sequenceDiagram
    autonumber
    participant CLI as cmd/cli
    participant LFS as localfs.Store
    participant I as ingest.Ingestor
    participant P as pipeline.Pipeline
    participant R as mongo.Repository
    participant Notifier as Terminal slog Notifier

    CLI->>LFS: List(ctx, "data/unprocessed")
    LFS-->>CLI: []fileKeys
    loop For each file
        CLI->>I: IngestObject(ctx, "", key)
        I->>LFS: DownloadStream(ctx, key)
        LFS-->>I: io.ReadCloser
        I->>I: Fingerprint (SHA-256)
        I->>P: Run(ctx, Request{Finalize: ArchiveProcessed})
        P->>Notifier: NotifyProgress(StageParsing)
        P->>R: SaveBatch(ctx, batch)
        R-->>P: SyncLog
        P->>Notifier: NotifyProgress(StageSaving)
        P->>LFS: ArchiveProcessed(ctx, key)
        LFS-->>P: Moved to data/processed/
        P->>R: LogSync(ctx, SyncLog)
        R-->>P: nil
        P->>Notifier: NotifyProgress(StageComplete)
        P-->>I: (SyncLog, nil)
        I-->>CLI: File summary
    end
```

### 5.3 Desktop Runtime Flow (`desktop`)

```mermaid
sequenceDiagram
    autonumber
    participant UI as Wails Frontend
    participant App as desktop/app.go
    participant I as ingest.Ingestor
    participant P as pipeline.Pipeline
    participant R as mongo.Repository
    participant WPN as WailsProgressNotifier

    UI->>App: ProcessFile(filePath)
    App->>I: IngestFileInPlace(ctx, filePath)
    Note over I: Reads file directly in place without local copy (U3)
    I->>I: Fingerprint (SHA-256)
    I->>P: Run(ctx, Request{Notifier: WPN, Finalize: nil})
    P->>WPN: NotifyProgress(StageParsing)
    WPN-->>UI: Event "progress" {stage: "parsing"}
    P->>R: SaveBatch(ctx, batch)
    R-->>P: SyncLog
    P->>WPN: NotifyProgress(StageSaving)
    WPN-->>UI: Event "progress" {stage: "saving"}
    P->>R: LogSync(ctx, SyncLog)
    R-->>P: nil
    P->>WPN: NotifyProgress(StageComplete)
    WPN-->>UI: Event "progress" {stage: "complete"}
    P-->>I: (SyncLog, nil)
    I-->>App: RunStats
    App-->>UI: Result JSON
```

---

## 6. Decision Record & Divergences

### 6.1 Architectural Decision Summary

- **U1 (Audit Scope)**: Attempts past source resolution are audited. Unresolvable filenames are logged in run stats but unaudited.
- **U2 (Synthetic Parser)**: Dedicated `synthetic.CSVExtractor` (2G) parses synthetic CSVs to `transactions_synthetic` retaining `details`, `balance`, and `checkOrSlipNum`.
- **U3 (Desktop In-Place Read)**: Desktop reads selected file in place; eliminates disk copying to `data/unprocessed/` and `data/processed/`.
- **U4 (Release Binaries)**: Release binaries build root `main.go` until Phase 5; `make build` uses `cmd/cli`.
- **U5 (User-Run Steps)**: D4 collection drop and Atlas verifications are executed exclusively by human operators.
- **U6 (Lambda Failure Handling)**: Transient errors error out for Lambda retry; deterministic errors audit and log, leaving object in `unprocessed/`.
- **P1–P13 (Engineering Policies)**: Layer boundaries (P1), fallback registration (P2), detached failure context (P3), timing restamp (P4), non-fatal success audit (P5), finalize before audit (P6), 32 MiB cap (P7), `source.s3Uri` provenance (P8), lazy Mongo client (P9), lazy indexes (P10), desktop module rename (P11), `port.SyncLogReader` (P12), additive cutover (P13).

### 6.2 Recorded Divergences from Legacy

1. **Pre-resolution Audits Dropped**: Non-CSV and unresolvable filenames produce no empty `collectionName` audit row.
2. **Collection Name Accuracy**: Parse failure audits record the target collection name (`transactions_chase`) rather than data source tag (`chase`).
3. **Strict Synthetic Validation**: Synthetic files use the unified D4 Chase shape and fail on single-row validation errors.
4. **Coarse Progress Stages**: Desktop progress reports phase stages (`parsing`, `saving`, `complete`) rather than row-level ticks.
5. **Desktop Archiving Eliminated**: Desktop leaves source files untouched on local disk.
6. **S3 URI Provenance**: `source.s3Uri` populated on all Lambda-ingested records.
7. **Failed S3 Objects Retained**: Lambda leaves deterministic failures in `unprocessed/` for debugging.
8. **Transient Download Retries**: `DownloadStream` failures are retried without audit log creation.
9. **Too-Large File Auditing**: Files $> 32\text{ MiB}$ fail with `ErrInputTooLarge` and write a FAILED audit row with empty `FileHash`.
10. **Desktop History DTO**: `id` omitted from JSON responses; canonical and dual fields match legacy UI expectations.

---

## 7. Verification Matrix

| Slice | Focus Area | Key Verification Tests |
| :--- | :--- | :--- |
| **2G** | Synthetic CSV Extractor | `TestCSVExtractor_ExtractSuccess`, `TestCSVExtractor_PreservesFields`, `TestCSVExtractor_RejectsInvalidRow` |
| **4A** | Spec & Guardrails | `TestDepguard_AppIsolation`, `TestDepguard_InfraNoApp`, `make check-quality` |
| **4B** | S3 URI Provenance | `TestBuildWritePlan_SetsS3URI`, `TestSaveBatch_S3URIPersistedAndPreserved` |
| **4C** | Pipeline Orchestrator | `TestRun_SuccessPersistsAndAudits`, `TestRun_FailureAuditUsesDetachedContext`, `TestRun_FinalizeRunsBeforeSuccessAudit` |
| **4D** | Resolution & Fingerprint | `TestResolveSource_LegacyParity`, `TestFingerprint_HashMatchesSHA256`, `TestFingerprint_RejectsOversize` |
| **4E** | LocalFS Store | `TestLocalFS_List`, `TestLocalFS_DownloadStream`, `TestLocalFS_ArchiveProcessed` |
| **4F** | Object Ingestion | `TestIngestor_IngestObject_Success`, `TestIngestor_TooLargeAuditsFailure`, `TestIngestor_DeterministicFailureRetainsObject` |
| **4G** | CLI Entrypoint | `TestCLI_IngestCommand`, `TestCLI_GenerateSyntheticData`, `TestCLI_SlogProgress` |
| **4L** | Local E2E Verification | `TestE2E_CLIIngest_RealMongo`, `TestE2E_CLIIngest_RerunIsIdempotent`, `TestSyntheticCSV_RoutesToSyntheticDestination` |
| **4H** | Lambda Cutover | `TestHandler_S3Event_Success`, `TestHandler_DeterministicFailure_ReturnsNil`, `TestHandler_TransientFailure_ReturnsError` |
| **4I** | Desktop Notifier | `TestWailsProgressNotifier_MapsStages`, `TestWailsProgressNotifier_ThreadSafe` |
| **4J** | History Read Port | `TestToHistoryEntries_LegacyJSONGolden`, `TestHistoryService_UsesDefaultLimit`, `TestHistoryService_PropagatesError` |
| **4K** | Desktop Cutover | `TestApp_ProcessFile_Success`, `TestApp_GetHistory_Parity`, `TestApp_E2E_LocalMongo` |

---

## 8. Downstream Consumers & Cross-Repo Prerequisites

### 8.1 Downstream Consumers (`babylon_features`)
- `babylon_features/daemon.py` periodically scans all datalake MongoDB collections using default empty prefix `""`, including `dataSync` and `transactions_synthetic`.
- **Impact**: The unified D4 Chase document shape in `transactions_synthetic` and the newly populated `source.s3Uri` field are read by downstream chunking and embedding pipelines.
- **Governance**: Notify `babylon_features` maintainers prior to running the D4 collection drop in production.

### 8.2 Cross-Repository Prerequisites (`babylon_deploy`)
- **Prerequisite DP1**: Serverless pipeline alerting defined in `babylon_deploy` (`terraform/modules/data-loader/alerting.tf`).
- **Components**: CloudWatch `Errors` alarm dispatched to SNS email topic, plus asynchronous on-failure destination for the data-loader Lambda.
- **Requirement**: DP1 must be merged and applied before cutting over Lambda in Phase 4H.

---

<a id="synclogreader-consumer-port"></a>
## 9. `SyncLogReader` Consumer Port Specification

Authoritative implementation reference: `internal/domain/port/sync_log_reader.go` (lands in Phase 4J).

### 9.1 Consumer Read Port Contract

```go
package port

import (
	"context"

	"babylon/dataloader/internal/domain/model"
)

// SyncLogReader is the driven read port for ingestion audit history.
// Concrete implementations (such as mongo.Repository) retrieve audit records
// from the dataSync datalake collection.
type SyncLogReader interface {
	// ListSyncLogs returns up to limit audit records, newest first.
	// A non-positive limit (limit <= 0) defaults to the adapter default (100).
	ListSyncLogs(ctx context.Context, limit int64) ([]model.SyncLog, error)
}
```

### 9.2 Interface Segregation & Composition
- **Interface Segregation**: Segregates query access (`ListSyncLogs`) from command persistence (`port.Repository` defining `SaveBatch` and `LogSync`).
- **Adapter Conformance**: Satisfied by `*mongo.Repository.ListSyncLogs` without modifying the core storage adapter.
- **Composition Root Assertion**: Validated in `desktop/history.go` via:
  ```go
  var _ port.SyncLogReader = (*mongo.Repository)(nil)
  ```
- **Desktop History DTO**: Desktop service maps `[]model.SyncLog` to `[]historyEntry`, preserving legacy JSON field compatibility for the frontend.
