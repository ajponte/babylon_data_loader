# DataSync Ingestion Audit & Observability Specification

## 1. Executive Summary & Architectural Context

This specification defines the architecture, domain models, indexing strategies, storage contracts, and observability tooling for the ingestion audit trail within the Babylon Datalake (`dataSync` collection).

The `dataSync` collection serves as the definitive, immutable operational registry for all statement ingestion processes executed by `babylon_data_loader`. Every file evaluated by the ingestion engine—whether originating from AWS S3 in serverless Lambda deployments or local directories in desktop environments—generates an audit document capturing cryptographic provenance, processing duration, detailed mutation counts, and operational health status.

### 1.1. Ingestion Audit Evolution

Historical iterations of `babylon_data_loader` maintained a minimal, unindexed 3-field audit schema (`collection_name`, `sync_timestamp`, `records_uploaded`). While functional for basic tracking, this legacy format created critical operational bottlenecks:

1. **Unindexed Collection Scans**: Operational health checks and monitoring dashboards sorting by timestamp (such as `db.dataSync.find().sort({syncedAt: -1}).limit(1)`) triggered full collection scans (`COLLSCAN`), increasing database CPU utilization as historical volume accumulated.
2. **Absence of Provenance & Deduplication Coordinates**: File names and cryptographic hashes (SHA-256) were not captured in `dataSync`, preventing deterministic file deduplication detection at the audit layer.
3. **Coarse Accounting**: Only total rows uploaded were recorded. Granular database mutations—such as new documents inserted (`upsertedCount`) versus existing documents matched and updated (`matchedCount`)—were indistinguishable.
4. **Silent Ingestion Failures**: Processing errors, CSV format anomalies, and storage exceptions aborted the pipeline without persisting a permanent failure record in `dataSync`.
5. **Coupled Storage Side-Effects**: Bulk upsert methods in the storage repository internally wrote audit entries, creating tight coupling and preventing centralized lifecycle tracking across extraction, validation, and parsing errors.
6. **Specification Discrepancy**: The data format diverged from Sections 5.2 and 6.1 of the authoritative MongoDB Atlas Datalake specification (`docs/specs/mongodb-atlas-datalake.md`).

#### The Two-Phase Modernization:
- **Phase 1 (Domain Model & Automated Indexing)**: Established the canonical 11-field `SyncLog` domain model ([`datalake/model/sync_log.go`](../../datalake/model/sync_log.go)), double-checked thread-safe automated index provisioning on `dataSync` (`idx_datasync_synced_at` and `idx_datasync_filename_synced_at`), bidirectional backward-compatible schema normalization (`Normalize()`), and the expanded repository contract.
- **Phase 2 (Decoupled Storage & Centralized Orchestration)**: Decoupled storage repository methods (`storage/mongo_repository.go`) so that `BulkUpsertTransactions`, `BulkUpsertChaseTransactions`, and `BulkUpsertFidelityPositions` return pure `(repository.UpsertStats, error)` with zero audit side-effects (`recordSyncAuditLog` removed). Centralized complete ownership of audit log emission within `CSVFileProcessor` (`datalake/datalake.go`), wrapping the entire ingestion lifecycle (validation, extraction, parsing, mapping, storage, and archival) with end-to-end wall-clock timing and forensic failure capture. Aligned the desktop UI (`desktop/frontend/src/components/HistoryPanel.tsx`) and TypeScript bindings (`desktop/frontend/wailsjs/go/models.ts`) to display rich status badges, duration, duplicate counts, and file lineage verified by 17 Vitest tests.

> [!NOTE]
> **Zero-Downtime Backward Compatibility Guarantee**  
> Complete backward compatibility is maintained across all legacy documents stored in MongoDB and frontend bindings (such as `desktop/frontend/src/components/HistoryPanel.tsx`). Dual BSON tagging and bidirectional in-memory normalization (`Normalize()`) allow legacy and canonical records to co-exist transparently.

### 1.2. High-Level Architectural Flow

```mermaid
flowchart TD
    subgraph IngestionOrchestrator ["Centralized Orchestrator (datalake.CSVFileProcessor)"]
        A["Start Timer: startTime = time.Now().UTC()"] --> B{"File Validation & Parsing"}
        B -->|Validation/Parse Failure| C["Construct model.NewFailedSyncLog"]
        B -->|Parse Success| D["Route to Mapper: Compute SHA-256 fileHash"]
        D --> E["Invoke Pure Bulk Upsert on Repo"]
        E -->|Storage Error| C
        E -->|Storage Success| F["Construct model.NewSuccessSyncLog"]
    end

    subgraph StorageLayer ["Decoupled Storage (storage.MongoRepository)"]
        E -.->|Pure Mutation: Return UpsertStats| G[("MongoDB: Partitioned Collections")]
        C -->|Explicit RecordSyncLog| H["storage.MongoRepository.RecordSyncLog"]
        F -->|Explicit RecordSyncLog| H
        H --> I["ensureDataSyncIndexes: Double-Checked RWMutex"]
        I --> J[("MongoDB: dataSync Collection")]
        I --> K["idx_datasync_synced_at"]
        I --> L["idx_datasync_filename_synced_at"]
    end

    subgraph AuditConsumers ["Audit Observability Consumers"]
        M["Desktop UI (HistoryPanel.tsx) / CLI / mongosh"] --> N["GetSyncLogs: Compound Sort"]
        N --> J
        J --> O["Decode & log.Normalize()"]
        O --> P["Aligned UI Display & Telemetry"]
    end
```

---

## 2. Ingestion Audit Requirements & Operational Challenges

### 2.1. Operational Provenance & Lineage Tracking

Ingestion audit logs must link directly to the physical statement source. The canonical model requires:
- **`fileName`**: The base name of the ingested CSV file (e.g., `Chase6896_Activity_20230312.CSV` or `Portfolio_Positions_20261017.csv`).
- **`fileHash`**: Cryptographic SHA-256 checksum calculated over the raw file content prior to parsing, establishing immutable file identity and enabling fast deduplication validation.
- **`collectionName`**: The target MongoDB collection where records were routed (e.g., `transactions_chase`, `positions_fidelity`, `transactions_synthetic`).

### 2.2. Granular Record Accounting

The legacy `records_uploaded` metric obscured the distinction between newly discovered transactions and re-ingested idempotent rows. The canonical model establishes three explicit metrics:
- **`recordCount`**: Total number of data rows parsed from the source CSV file (excluding headers, trailers, and legal disclaimers).
- **`upsertedCount`**: Number of new documents created in the datalake collection during bulk upsert execution.
- **`matchedCount`**: Number of existing documents identified and updated/verified during bulk upsert execution.

**Invariance Rule**: For successful runs, `recordCount` must equal `upsertedCount + matchedCount` (excluding explicitly dropped invalid rows).

### 2.3. Failure & Status Tracking

Operational monitoring requires distinguishing between healthy ingestions and failed attempts directly within the database:
- **`status`**: Strongly-typed operational status enum: `"SUCCESS"` or `"FAILED"`.
- **`errorMessage`**: Sanitized error message capturing the failure root cause (e.g., corrupted headers, schema validation violations, S3 retrieval timeouts, or MongoDB connection rejections). When `status == "SUCCESS"`, this field is omitted from BSON storage.

### 2.4. Performance Observability & Latency Profiling

Each audit record tracks pipeline execution characteristics:
- **`syncedAt`**: Wall-clock UTC timestamp recording when the ingestion completed or terminated.
- **`durationMs`**: Elapsed wall-clock time in milliseconds from statement parsing initiation to final database persistence and archival.

### 2.5. Elimination of Unindexed Collection Scans

The canonical datalake health check defined in `docs/specs/mongodb-atlas-datalake.md` Section 6.1 queries:
```javascript
db.dataSync.find().sort({ syncedAt: -1 }).limit(1);
```
Without dedicated indexing, MongoDB performs a collection scan (`COLLSCAN`) followed by an in-memory sort (`SORT`). As ingestion runs multiply, this degrades performance and risks query termination if the sort buffer memory limit is exceeded.

To eliminate scans, the storage layer automates index provisioning:
1. **`idx_datasync_synced_at`**: Single-field descending index `{ syncedAt: -1 }` ensuring $O(\log N)$ point lookups for latest sync timestamps and date-bounded timeline queries.
2. **`idx_datasync_filename_synced_at`**: Compound index `{ fileName: 1, syncedAt: -1 }` complying with the Equality-Sort-Range (ESR) rule for querying the complete execution history of a specific statement file.

### 2.6. Zero-Downtime Backward Compatibility Guarantee

The ingestion pipeline must run safely against datalake instances containing historical 3-field documents without requiring database migrations or downtime:
- **Dual BSON Tags**: Model fields carry both canonical tags (`syncedAt`, `recordCount`, `collectionName`) and legacy tags (`sync_timestamp`, `records_uploaded`, `collection_name`).
- **Frontend Bindings**: The desktop user interface (`desktop/frontend/src/components/HistoryPanel.tsx`) relies on JSON keys `syncTimestamp` and `recordsUploaded`. The canonical model exposes these via JSON tags while mapping internal state bidirectionally.
- **Compound Query Sorting**: The storage engine sorts by `[{ syncedAt: -1 }, { sync_timestamp: -1 }]` to interleave historical legacy documents and new canonical documents in proper chronological sequence.

---

## 3. Domain Model Specification (`datalake/model/sync_log.go`)

### 3.1. `SyncStatus` Enumeration

Operational status is modeled as a strongly-typed string enum:

```go
type SyncStatus string

const (
    SyncStatusSuccess SyncStatus = "SUCCESS"
    SyncStatusFailed  SyncStatus = "FAILED"
)
```

### 3.2. Canonical `SyncLog` Schema Specification

Defined in [`datalake/model/sync_log.go`](../../datalake/model/sync_log.go):

| Field Name | BSON Tag | JSON Tag | Go Type | Description | Spec Section |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `ID` | `_id,omitempty` | `id,omitempty` | `primitive.ObjectID` | MongoDB auto-generated document identifier | Section 5.2 |
| `FileName` | `fileName` | `fileName` | `string` | Base filename of the ingested statement | Section 5.2 |
| `FileHash` | `fileHash,omitempty` | `fileHash,omitempty` | `string` | Cryptographic SHA-256 hash of raw file content | Section 5.2 |
| `CollectionName` | `collectionName,omitempty` | `collectionName` | `string` | Target collection name | Section 5.2 |
| `RecordCount` | `recordCount` | `recordCount` | `int64` | Total data rows processed | Section 5.2 |
| `UpsertedCount` | `upsertedCount` | `upsertedCount` | `int64` | New records inserted | Section 5.2 |
| `MatchedCount` | `matchedCount` | `matchedCount` | `int64` | Existing records updated / matched | Section 5.2 |
| `Status` | `status` | `status` | `string` | Execution state (`"SUCCESS"` or `"FAILED"`) | Section 5.2 |
| `ErrorMessage` | `errorMessage,omitempty` | `errorMessage,omitempty` | `string` | Sanitized error diagnostic string | Section 5.2 |
| `SyncedAt` | `syncedAt` | `syncedAt` | `time.Time` | Ingestion timestamp in UTC | Section 5.2 |
| `DurationMs` | `durationMs` | `durationMs` | `int64` | Total processing wall-clock time in milliseconds | Section 5.2 |
| `LegacyCollectionName` | `collection_name,omitempty` | `-` | `string` | Historical snake_case alias for collection name | Compatibility |
| `LegacySyncTimestamp` | `sync_timestamp,omitempty` | `syncTimestamp` | `time.Time` | Historical snake_case alias for sync timestamp | Compatibility |
| `LegacyRecordsUploaded`| `records_uploaded,omitempty` | `recordsUploaded` | `int64` | Historical snake_case alias for uploaded count | Compatibility |

#### Complete Go Struct Definition:

```go
type SyncLog struct {
    ID             primitive.ObjectID `bson:"_id,omitempty"            json:"id,omitempty"`
    FileName       string             `bson:"fileName"                 json:"fileName"`
    FileHash       string             `bson:"fileHash,omitempty"       json:"fileHash,omitempty"`
    CollectionName string             `bson:"collectionName,omitempty" json:"collectionName"`
    RecordCount    int64              `bson:"recordCount"              json:"recordCount"`
    UpsertedCount  int64              `bson:"upsertedCount"            json:"upsertedCount"`
    MatchedCount   int64              `bson:"matchedCount"             json:"matchedCount"`
    Status         string             `bson:"status"                   json:"status"`
    ErrorMessage   string             `bson:"errorMessage,omitempty"   json:"errorMessage,omitempty"`
    SyncedAt       time.Time          `bson:"syncedAt"                 json:"syncedAt"`
    DurationMs     int64              `bson:"durationMs"               json:"durationMs"`

    // Legacy backward compatibility fields for historical documents and frontend bindings
    LegacyCollectionName  string    `bson:"collection_name,omitempty"  json:"-"`
    LegacySyncTimestamp   time.Time `bson:"sync_timestamp,omitempty"   json:"syncTimestamp"`
    LegacyRecordsUploaded int64     `bson:"records_uploaded,omitempty" json:"recordsUploaded"`
}
```

### 3.3. Deterministic Domain Constructors

Constructing `SyncLog` instances via raw struct literals is prohibited. Constructors guarantee invariant enforcement and legacy alias synchronization:

#### Successful Ingestion Constructor (`NewSuccessSyncLog`)
```go
func NewSuccessSyncLog(
    fileName string,
    fileHash string,
    collectionName string,
    recordCount int64,
    upsertedCount int64,
    matchedCount int64,
    syncedAt time.Time,
    durationMs int64,
) SyncLog {
    utcSyncedAt := syncedAt.UTC()
    return SyncLog{
        FileName:              fileName,
        FileHash:              fileHash,
        CollectionName:        collectionName,
        RecordCount:           recordCount,
        UpsertedCount:         upsertedCount,
        MatchedCount:          matchedCount,
        Status:                string(SyncStatusSuccess),
        SyncedAt:              utcSyncedAt,
        DurationMs:            durationMs,
        LegacyCollectionName:  collectionName,
        LegacySyncTimestamp:   utcSyncedAt,
        LegacyRecordsUploaded: upsertedCount,
    }
}
```

#### Failed Ingestion Constructor (`NewFailedSyncLog`)
```go
func NewFailedSyncLog(
    fileName string,
    fileHash string,
    collectionName string,
    errorMessage string,
    recordCount int64,
    syncedAt time.Time,
    durationMs int64,
) SyncLog {
    utcSyncedAt := syncedAt.UTC()
    return SyncLog{
        FileName:              fileName,
        FileHash:              fileHash,
        CollectionName:        collectionName,
        RecordCount:           recordCount,
        UpsertedCount:         0,
        MatchedCount:          0,
        Status:                string(SyncStatusFailed),
        ErrorMessage:          errorMessage,
        SyncedAt:              utcSyncedAt,
        DurationMs:            durationMs,
        LegacyCollectionName:  collectionName,
        LegacySyncTimestamp:   utcSyncedAt,
        LegacyRecordsUploaded: 0,
    }
}
```

### 3.4. Bidirectional Schema Normalization (`Normalize()`)

When documents are decoded from MongoDB or prepared for write operations, `Normalize()` ensures mutual synchronization between canonical and legacy fields:

```go
func (l *SyncLog) Normalize() {
    if l == nil {
        return
    }
    // Normalize timestamps to UTC
    if l.SyncedAt.IsZero() && !l.LegacySyncTimestamp.IsZero() {
        l.SyncedAt = l.LegacySyncTimestamp.UTC()
    } else if !l.SyncedAt.IsZero() && l.LegacySyncTimestamp.IsZero() {
        l.LegacySyncTimestamp = l.SyncedAt.UTC()
    }

    // Normalize collection name
    if l.CollectionName == "" && l.LegacyCollectionName != "" {
        l.CollectionName = l.LegacyCollectionName
    } else if l.CollectionName != "" && l.LegacyCollectionName == "" {
        l.LegacyCollectionName = l.CollectionName
    }

    // Normalize counts
    if l.RecordCount == 0 && l.LegacyRecordsUploaded > 0 {
        l.RecordCount = l.LegacyRecordsUploaded
        l.UpsertedCount = l.LegacyRecordsUploaded
    }
    if l.LegacyRecordsUploaded == 0 && l.UpsertedCount > 0 {
        l.LegacyRecordsUploaded = l.UpsertedCount
    }

    // Default status if reading legacy document
    if l.Status == "" {
        l.Status = string(SyncStatusSuccess)
    }
}
```

**Key Operational Properties**:
- **Nil Safety**: If invoked on a `nil` pointer, returns immediately with zero panics.
- **Timestamp Priority**: If `SyncedAt` is zero, populates it from `LegacySyncTimestamp` in UTC. Conversely, if `LegacySyncTimestamp` is zero, populates it from `SyncedAt`.
- **Count Backfill**: When reading a legacy document with only `records_uploaded`, sets both `RecordCount` and `UpsertedCount` to that value.
- **Default Status**: Historical documents lack a `status` field. `Normalize()` defaults missing status to `"SUCCESS"`.

---

## 4. Repository Contract & Storage Implementation

### 4.1. Repository Contract (`datalake/repository/repository.go`)

The repository contract defines the interface boundary for audit persistence:

```go
type Repository interface {
    BulkUpsertTransactions(ctx context.Context, transactions []model.Transaction) (UpsertStats, error)
    BulkUpsertChaseTransactions(ctx context.Context, transactions []model.ChaseTransaction) (UpsertStats, error)
    BulkUpsertFidelityPositions(ctx context.Context, positions []model.FidelityPosition) (UpsertStats, error)
    RecordSyncLog(ctx context.Context, log model.SyncLog) error
    GetSyncLogs(ctx context.Context) ([]model.SyncLog, error)
}
```

### 4.2. Automated Thread-Safe Index Provisioning (`storage/mongo_repository.go`)

To avoid manual operational overhead during deployment, `MongoRepository` manages indexes lazily on first access using a double-checked `sync.RWMutex` locking pattern:

```go
func (r *MongoRepository) ensureDataSyncIndexes(ctx context.Context) error {
    // Fast path: Read lock check
    r.syncIndexMu.RLock()
    if r.syncIndexesCreated {
        r.syncIndexMu.RUnlock()
        return nil
    }
    r.syncIndexMu.RUnlock()

    // Slow path: Write lock check and index creation
    r.syncIndexMu.Lock()
    defer r.syncIndexMu.Unlock()

    if r.syncIndexesCreated {
        return nil
    }

    collection := r.provider.Collection(syncTableName)

    syncedAtIndex := mongo.IndexModel{
        Keys:    bson.D{{Key: fieldSyncedAt, Value: -1}},
        Options: options.Index().SetName("idx_datasync_synced_at"),
    }

    fileSyncedIndex := mongo.IndexModel{
        Keys: bson.D{
            {Key: "fileName", Value: 1},
            {Key: fieldSyncedAt, Value: -1},
        },
        Options: options.Index().SetName("idx_datasync_filename_synced_at"),
    }

    _, err := collection.Indexes().CreateMany(ctx, []mongo.IndexModel{syncedAtIndex, fileSyncedIndex})
    if err != nil {
        return fmt.Errorf("failed to create indexes on %s: %w", syncTableName, err)
    }

    r.syncIndexesCreated = true
    return nil
}
```

### 4.3. Index Topologies & Operational Access Patterns

```mermaid
flowchart TD
    subgraph SingleIndex ["Single-Field Descending Index"]
        S1["Name: idx_datasync_synced_at"]
        S2["Keys: { syncedAt: -1 }"]
        S3["Use Case: Global latest sync checks, health queries"]
    end

    subgraph CompoundIndex ["Compound Index (ESR Compliant)"]
        C1["Name: idx_datasync_filename_synced_at"]
        C2["Keys: { fileName: 1, syncedAt: -1 }"]
        C3["Use Case: Per-file historical audit lookups, deduplication"]
    end

    SingleIndex --- CompoundIndex
```

1. **`idx_datasync_synced_at`**:
   - Keys: `{ syncedAt: -1 }`
   - Optimizes queries sorting by ingestion time across the entire datalake. Eliminates `COLLSCAN` and in-memory sorting.
2. **`idx_datasync_filename_synced_at`**:
   - Keys: `{ fileName: 1, syncedAt: -1 }`
   - Optimizes prefix filtering on `fileName` with descending sort on `syncedAt`. Enables rapid verification of whether a specific statement file has already been ingested.

### 4.4. Audit Persistence Engine (`RecordSyncLog`)

The write pathway enforces index existence, normalizes fields, guarantees UTC timestamps, and persists to the collection:

```go
func (r *MongoRepository) RecordSyncLog(ctx context.Context, log model.SyncLog) error {
    if err := r.ensureDataSyncIndexes(ctx); err != nil {
        return err
    }

    log.Normalize()
    if log.SyncedAt.IsZero() {
        log.SyncedAt = time.Now().UTC()
        log.LegacySyncTimestamp = log.SyncedAt
    }

    syncCollection := r.provider.Collection(syncTableName)
    if _, err := syncCollection.InsertOne(ctx, log); err != nil {
        return fmt.Errorf("failed to insert audit log into %s: %w", syncTableName, err)
    }

    return nil
}
```

### 4.5. Query Optimization & Normalized Fetch (`GetSyncLogs`)

The read pathway executes a compound descending sort across canonical and legacy timestamps, guaranteeing correct chronological order regardless of whether historical or canonical documents are returned:

```go
func (r *MongoRepository) GetSyncLogs(ctx context.Context) ([]model.SyncLog, error) {
    if err := r.ensureDataSyncIndexes(ctx); err != nil {
        return nil, err
    }

    syncCollection := r.provider.Collection(syncTableName)

    opts := options.Find().SetSort(bson.D{
        {Key: fieldSyncedAt, Value: -1},
        {Key: "sync_timestamp", Value: -1},
    })

    cursor, err := syncCollection.Find(ctx, bson.M{}, opts)
    if err != nil {
        return nil, fmt.Errorf("failed to query sync logs: %w", err)
    }
    defer cursor.Close(ctx)

    var syncLogs []model.SyncLog
    if err = cursor.All(ctx, &syncLogs); err != nil {
        return nil, fmt.Errorf("failed to decode sync logs: %w", err)
    }

    for i := range syncLogs {
        syncLogs[i].Normalize()
    }

    return syncLogs, nil
}
```

### 4.6. Storage Decoupling & Pure Mutation Contracts (`storage/mongo_repository.go`)

In Phase 2, the storage layer was completely decoupled from ingestion audit logging to satisfy the Single Responsibility Principle:

1. **Pure Mutation Return Types**: The ingestion methods `BulkUpsertTransactions`, `BulkUpsertChaseTransactions`, and `BulkUpsertFidelityPositions` return pure `(repository.UpsertStats, error)`:
   ```go
   type UpsertStats struct {
       UpsertedCount int64
       MatchedCount  int64
       ModifiedCount int64
   }
   ```
2. **Removal of `recordSyncAuditLog`**: The internal helper method `recordSyncAuditLog` has been completely eliminated from `storage/mongo_repository.go`. Storage methods execute bulk writes and return raw stats, leaving `dataSync` mutation exclusively to `RecordSyncLog`.
3. **Decoupled Bulk Upsert Implementation**:
   ```go
   func (r *MongoRepository) BulkUpsertChaseTransactions(
       ctx context.Context,
       transactions []model.ChaseTransaction,
   ) (repository.UpsertStats, error) {
       if err := r.ensureChaseIndexes(ctx); err != nil {
           return repository.UpsertStats{}, err
       }

       models := buildChaseWriteModels(transactions)
       res, err := r.executeBulkWrite(ctx, ChaseTransactionsCollection, models)
       return toUpsertStats(res), err
   }
   ```
4. **Decoupling Rationale**:
   - **Isolation of Concerns**: Database drivers handle document persistence and index enforcement; business orchestrators handle provenance, lifecycle timing, and audit records.
   - **End-to-End Elapsed Duration**: Wall-clock duration (`durationMs`) measures total statement processing time (from file validation to final database commit), rather than just the latency of a single MongoDB bulk write.
   - **Comprehensive Failure Capture**: Failures occurring before storage (such as bad CSV syntax or missing account metadata) are captured in `dataSync` with identical fidelity.

---

## 5. Pipeline Integration Architecture & Orchestrator Lifecycle

### 5.1. Centralized Ingestion Lifecycle

The sequence diagram below details the integration between the centralized orchestrator (`CSVFileProcessor`), statement mappers (Fidelity, Chase, Generic), and the decoupled `storage.MongoRepository`:

```mermaid
sequenceDiagram
    autonumber
    participant Orchestrator as CSVFileProcessor (datalake.go)
    participant Extractor as InfoExtractor
    participant Parser as CSVParser
    participant Mapper as DataSource Mapper (Chase/Fidelity/Generic)
    participant Repo as MongoRepository (storage)
    participant Atlas as MongoDB Atlas

    Note over Orchestrator: Step 1: Start Wall-Clock Timer
    Orchestrator->>Orchestrator: startTime = time.Now().UTC()

    alt File Validation Failure (!validateCSVFile)
        Orchestrator->>Orchestrator: durationMs = time.Since(startTime)
        Orchestrator->>Orchestrator: NewFailedSyncLog(fileName, "", "", "Not a valid CSV file", 0, startTime, durationMs)
        Orchestrator->>Repo: RecordSyncLog(ctx, failLog)
        Repo->>Atlas: Insert failure record into dataSync
    else File Validation Passed
        Note over Orchestrator: Step 2: Extract Source Metadata
        Orchestrator->>Extractor: ExtractInfo(fileName)
        alt Source Info Extraction Error
            Extractor-->>Orchestrator: Return extraction error
            Orchestrator->>Repo: RecordSyncLog(ctx, NewFailedSyncLog(...))
        else Extraction Success
            Extractor-->>Orchestrator: Return SourceInfo (dataSource, accountID)
            Note over Orchestrator: Step 3: Parse CSV Rows
            Orchestrator->>Parser: Parse(ctx, filePath, dataSource, accountID)
            alt CSV Parsing Error
                Parser-->>Orchestrator: Return parse error
                Orchestrator->>Repo: RecordSyncLog(ctx, NewFailedSyncLog(...))
            else Parsing Success
                Parser-->>Orchestrator: Return rawRecords []map[string]string
                Note over Orchestrator: Step 4: Route, Map & Pure Bulk Upsert
                Orchestrator->>Mapper: NewMapper(filePath, fileName) & MapRecords(rawRecords)
                alt Mapper Error
                    Mapper-->>Orchestrator: Return mapper error
                    Orchestrator->>Repo: RecordSyncLog(ctx, NewFailedSyncLog(...))
                else Mapper Success
                    Mapper-->>Orchestrator: Return typed models + fileHash (SHA-256)
                    Orchestrator->>Repo: Pure BulkUpsert (Chase / Fidelity / Generic)
                    Repo->>Atlas: BulkWrite (ordered=false) to target collection
                    Atlas-->>Repo: Return BulkWriteResult
                    Repo-->>Orchestrator: Return (UpsertStats, error) [No dataSync side-effect]
                    alt Storage Error
                        Orchestrator->>Repo: RecordSyncLog(ctx, NewFailedSyncLog(fileHash, collectionName, ...))
                    else Storage Success
                        Note over Orchestrator: Step 5: Record Canonical Success Audit
                        Orchestrator->>Orchestrator: durationMs = time.Since(startTime)
                        Orchestrator->>Orchestrator: NewSuccessSyncLog(fileName, fileHash, collectionName, recordCount, upsertedCount, matchedCount, startTime, durationMs)
                        Orchestrator->>Repo: RecordSyncLog(ctx, successLog)
                        Repo->>Atlas: Insert success record into dataSync
                        Note over Orchestrator: Step 6: Archive File
                        Orchestrator->>Orchestrator: moveFile (unprocessed -> processed)
                    end
                end
            end
        end
    end
```

### 5.2. Batch Ingestion Lifecycle (`datalake/datalake.go`)

In batch and serverless environments, `CSVFileProcessor` owns the end-to-end audit lifecycle across validation, parsing, routing, and storage:

#### Ingestion Initiation & Validation Timing (`ingestCSVFile`):
```go
func (p *CSVFileProcessor) ingestCSVFile(ctx context.Context, file os.DirEntry) error {
    startTime := time.Now().UTC()
    fileName := file.Name()

    if !validateCSVFile(file) {
        reason := "Not a valid CSV file"
        p.Stats.AddFailure(fileName, reason)
        p.Logger.WarnContext(ctx, "file was not processed", "fileName", fileName, "reason", reason)

        durationMs := time.Since(startTime).Milliseconds()
        failLog := model.NewFailedSyncLog(fileName, "", "", reason, 0, startTime, durationMs)
        _ = p.Repo.RecordSyncLog(ctx, failLog)

        return fmt.Errorf("file %s is not a valid CSV file", fileName)
    }

    err := p.processFileWithAudit(ctx, file, startTime)
    if err != nil {
        p.Stats.AddFailure(fileName, err.Error())
        p.Logger.ErrorContext(ctx, "failed to process file", "file", fileName, "error", err)
        return fmt.Errorf("failed to process file %s: %w", fileName, err)
    }

    p.Stats.IncrementProcessed()
    return nil
}
```

#### Coordinated Processing & Forensic Failure Interception (`processFileWithAudit`):
```go
func (p *CSVFileProcessor) processFileWithAudit(
    ctx context.Context,
    unprocessedFile os.DirEntry,
    startTime time.Time,
) error {
    fileName := unprocessedFile.Name()
    unprocessedFilePath := sanitizeFilePath(unprocessedFile, p.UnprocessedDir)

    // 1. Source Info Extraction
    sourceInfo, err := p.Extractor.ExtractInfo(fileName)
    if err != nil {
        durationMs := time.Since(startTime).Milliseconds()
        _ = p.Repo.RecordSyncLog(ctx, model.NewFailedSyncLog(
            fileName, "", "", err.Error(), 0, startTime, durationMs,
        ))
        return fmt.Errorf("failed to extract source info: %w", err)
    }

    // 2. CSV Parsing
    rawRecords, _, err := p.Parser.Parse(ctx, unprocessedFilePath, sourceInfo.DataSource, sourceInfo.AccountID)
    if err != nil {
        durationMs := time.Since(startTime).Milliseconds()
        _ = p.Repo.RecordSyncLog(ctx, model.NewFailedSyncLog(
            fileName, "", sourceInfo.DataSource, err.Error(), 0, startTime, durationMs,
        ))
        return err
    }

    // 3. Routing, Transformation & Storage
    upsertStats, collectionName, fileHash, err := p.routeAndUpsertRecordsWithStats(
        ctx, sourceInfo, unprocessedFilePath, fileName, rawRecords,
    )
    if err != nil {
        durationMs := time.Since(startTime).Milliseconds()
        _ = p.Repo.RecordSyncLog(ctx, model.NewFailedSyncLog(
            fileName, fileHash, collectionName, err.Error(), int64(len(rawRecords)), startTime, durationMs,
        ))
        return err
    }

    // 4. Record Successful Audit Log
    durationMs := time.Since(startTime).Milliseconds()
    successLog := model.NewSuccessSyncLog(
        fileName, fileHash, collectionName, int64(len(rawRecords)),
        upsertStats.UpsertedCount, upsertStats.MatchedCount,
        startTime, durationMs,
    )
    if logErr := p.Repo.RecordSyncLog(ctx, successLog); logErr != nil {
        p.Logger.WarnContext(ctx, "failed to record success sync log", "fileName", fileName, "error", logErr)
    }

    // 5. Move Processed File
    if p.MoveProcessedFiles {
        if moveErr := p.moveFile(ctx, unprocessedFilePath); moveErr != nil {
            return fmt.Errorf("failed to move file: %w", moveErr)
        }
    }

    return nil
}
```

#### Routing & Pure Repository Upsert (`routeAndUpsertRecordsWithStats`):
```go
func (p *CSVFileProcessor) routeAndUpsertRecordsWithStats(
    ctx context.Context,
    sourceInfo *datasource.SourceInfo,
    unprocessedFilePath, fileName string,
    rawRecords []map[string]string,
) (repository.UpsertStats, string, string, error) {
    switch datasource.DataSource(sourceInfo.DataSource) {
    case datasource.Fidelity:
        mapper, err := NewFidelityMapper(ctx, unprocessedFilePath, fileName, p.S3URI)
        if err != nil {
            return repository.UpsertStats{}, FidelityPositionsCollection, "", fmt.Errorf("failed to initialize Fidelity mapper: %w", err)
        }
        positions, mapErr := mapper.MapRecordsWithContext(ctx, rawRecords)
        if mapErr != nil {
            return repository.UpsertStats{}, FidelityPositionsCollection, mapper.FileHash(), fmt.Errorf("failed to map Fidelity positions: %w", mapErr)
        }
        if ctxErr := ctx.Err(); ctxErr != nil {
            return repository.UpsertStats{}, FidelityPositionsCollection, mapper.FileHash(), ctxErr
        }
        stats, upsertErr := p.Repo.BulkUpsertFidelityPositions(ctx, positions)
        return stats, FidelityPositionsCollection, mapper.FileHash(), upsertErr

    case datasource.Chase:
        mapper, err := NewChaseMapper(ctx, unprocessedFilePath, fileName, p.S3URI, sourceInfo.AccountID)
        if err != nil {
            return repository.UpsertStats{}, ChaseTransactionsCollection, "", fmt.Errorf("failed to initialize Chase mapper: %w", err)
        }
        transactions, mapErr := mapper.MapRecordsWithContext(ctx, rawRecords)
        if mapErr != nil {
            return repository.UpsertStats{}, ChaseTransactionsCollection, mapper.FileHash(), fmt.Errorf("failed to map Chase transactions: %w", mapErr)
        }
        if ctxErr := ctx.Err(); ctxErr != nil {
            return repository.UpsertStats{}, ChaseTransactionsCollection, mapper.FileHash(), ctxErr
        }
        stats, upsertErr := p.Repo.BulkUpsertChaseTransactions(ctx, transactions)
        return stats, ChaseTransactionsCollection, mapper.FileHash(), upsertErr

    case datasource.Generic, datasource.Synthetic:
        fallthrough
    default:
        collectionName := fmt.Sprintf("%s_%s", TransactionsCollection, sourceInfo.DataSource)
        transactions, mapErr := mapGenericTransactions(ctx, rawRecords, sourceInfo.DataSource, sourceInfo.AccountID)
        if mapErr != nil {
            return repository.UpsertStats{}, collectionName, "", mapErr
        }
        if ctxErr := ctx.Err(); ctxErr != nil {
            return repository.UpsertStats{}, collectionName, "", ctxErr
        }
        stats, upsertErr := p.Repo.BulkUpsertTransactions(ctx, transactions)
        return stats, collectionName, "", upsertErr
    }
}
```

### 5.3. Desktop Single-File Lifecycle (`processSingleFile`)

For the desktop application, `CSVFileProcessor.processSingleFile` coordinates real-time frontend notifications alongside persistent `dataSync` audit tracking:

1. **Validation Phase (`PhaseValidating`)**: Checks `.csv` extension and emits `ProgressEvent`. If invalid, constructs `model.NewFailedSyncLog` and delegates to `handleFailure`.
2. **Parsing Phase (`PhaseParsing`)**: Streams CSV parsing. If syntax fails, constructs `model.NewFailedSyncLog` and surfaces error through progress events.
3. **Upserting Phase (`PhaseUpserting`)**: Routes records to dedicated mapper, computes `fileHash`, and executes pure repository upsert. Emits `ProgressEvent` containing `UpsertedCount` and `DuplicateCount` (`MatchedCount`). If upsert fails, records `model.NewFailedSyncLog`.
4. **Finalization (`finalizeSingleFileSuccess`)**:
   ```go
   func (p *CSVFileProcessor) finalizeSingleFileSuccess(
       ctx context.Context,
       fileName, fileHash, collectionName, unprocessedFilePath string,
       rawRecords []map[string]string,
       upsertStats repository.UpsertStats,
       startTime time.Time,
       reporter ProgressReporter,
   ) error {
       durationMs := time.Since(startTime).Milliseconds()
       successLog := model.NewSuccessSyncLog(
           fileName,
           fileHash,
           collectionName,
           int64(len(rawRecords)),
           upsertStats.UpsertedCount,
           upsertStats.MatchedCount,
           startTime,
           durationMs,
       )
       if logErr := p.Repo.RecordSyncLog(ctx, successLog); logErr != nil {
           p.Logger.WarnContext(ctx, "failed to record success sync log", "fileName", fileName, "error", logErr)
       }

       if p.MoveProcessedFiles {
           p.reportProgress(reporter, ProgressEvent{Phase: PhaseMoving, FileName: fileName})
           if moveErr := p.moveFile(ctx, unprocessedFilePath); moveErr != nil {
               return p.handleFailure(fileName, fmt.Errorf("failed to move file: %w", moveErr), reporter)
           }
       }

       p.reportProgress(reporter, ProgressEvent{Phase: PhaseDone, FileName: fileName})
       return nil
   }
   ```

### 5.4. Desktop UI History Alignment (`HistoryPanel.tsx`, `models.ts`, `App.css`)

The desktop application presents the `dataSync` audit log in real time through the `HistoryPanel` React component:

- **Wails Binding (`GetHistory`)**: The frontend calls Go `GetHistory()`, which queries MongoDB Atlas using compound sort (`{ syncedAt: -1, sync_timestamp: -1 }`) and returns normalized `SyncLog` domain models.
- **TypeScript Model Alignment (`desktop/frontend/wailsjs/go/models.ts`)**:
  ```typescript
  export namespace model {
      export class SyncLog {
          id?: string;
          fileName?: string;
          fileHash?: string;
          collectionName: string;
          recordCount?: number;
          upsertedCount?: number;
          matchedCount?: number;
          status?: string;
          errorMessage?: string;
          syncedAt?: any;
          durationMs?: number;
          syncTimestamp?: any;
          recordsUploaded?: number;
          // ... constructor & value converter
      }
  }
  ```
- **Table Visual Layout**:
  Renders an aligned 7-column operational table:
  1. **Status**: High-contrast badges (`SUCCESS` in `.badge-success` green or `FAILED` in `.badge-failed` red).
  2. **File Name**: Statement file name (`log.fileName || 'N/A'`).
  3. **Collection**: Target collection (`log.collectionName`).
  4. **Records / Upserted**: New documents inserted (`upsertedCount ?? recordsUploaded ?? 0`).
  5. **Duplicates**: Existing documents matched and verified (`matchedCount ?? 0`).
  6. **Duration**: Total wall-clock time in milliseconds (`${durationMs}ms` or `'N/A'`).
  7. **Timestamp**: Wall-clock ingestion time (`syncedAt || syncTimestamp`) formatted to the user's locale.
- **Backward Compatibility Handling**: When displaying historical legacy documents (which lack `fileName`, `status`, `matchedCount`, and `durationMs`), `HistoryPanel` gracefully falls back to default values (`'SUCCESS'`, `'N/A'`, `0`, and legacy timestamp).
- **Automated Verification**: Verified across 17 Vitest tests (`HistoryPanel.test.tsx`, `UploadForm.test.tsx`, `ProgressPanel.test.tsx`), asserting proper badge rendering, column presence, legacy format compatibility, and error alert handling.

---

## 6. Verification & Operational Tooling

### 6.1. Inspecting Live Data via `mongosh`

Database administrators and operations teams can inspect and verify the `dataSync` collection directly using Atlas connection strings:

```bash
# 1. Fetch the latest sync audit document (Section 6.1 of Atlas spec)
mongosh "<mongo-uri>" --eval 'db.dataSync.find().sort({syncedAt: -1}).limit(1)'

# 2. Inspect recently failed ingestions
mongosh "<mongo-uri>" --eval '
  db.dataSync.find({ status: "FAILED" })
    .projection({ fileName: 1, errorMessage: 1, syncedAt: 1 })
    .sort({ syncedAt: -1 })
    .limit(5)
'

# 3. Query ingestion history for a specific statement file (utilizes compound index)
mongosh "<mongo-uri>" --eval '
  db.dataSync.find({ fileName: "Chase6896_Activity_20230312.CSV" })
    .sort({ syncedAt: -1 })
'

# 4. Verify index utilization via executionStats
mongosh "<mongo-uri>" --eval '
  db.dataSync.find().sort({ syncedAt: -1 }).limit(1).explain("executionStats")
'
```

#### Verifying Index Utilization (`explain()` Output)
A successful index scan exhibits the following winning plan characteristics:
- **`stage`**: `"IXSCAN"` (using index `idx_datasync_synced_at`)
- **`totalDocsExamined`**: `1` (for `limit(1)`)
- **`totalKeysExamined`**: `1`
- **`executionStages.stage`**: `"FETCH"` without any `"SORT"` stage

### 6.2. Telemetry Signatures in CloudWatch / Structured Logger

When `babylon_data_loader` processes a file and writes audit records, `slog` telemetry logs emit the following signature:

```text
INFO msg="Data ingestion process completed successfully."
INFO msg="Recording dataSync audit log" fileName="Chase6896_Activity_20230312.CSV" status="SUCCESS" records=93 upserted=93 matched=0 durationMs=412
INFO msg="Successfully inserted audit log into dataSync"
```

In the event of an ingestion failure:
```text
ERROR msg="Statement parsing rejected" fileName="Corrupted_Statement.csv" error="record on line 4: wrong number of fields"
WARN msg="Recording failed ingestion log" fileName="Corrupted_Statement.csv" status="FAILED" error="record on line 4: wrong number of fields"
```

### 6.3. Hermetic Quality Gates & Test Suite Matrix

Every component specified in this document is validated by hermetic unit tests with race detection:

#### Phase 1 Domain Model & Storage Repository Tests:
| Test Identifier | Test Function | Test File | Architectural Validation Target |
| :--- | :--- | :--- | :--- |
| `TC-SL-01` | `TestSyncLog_NewSuccessSyncLog` | `datalake/model/sync_log_test.go` | Asserts all 11 fields, canonical keys, UTC timestamping, and legacy alias synchronization. |
| `TC-SL-02` | `TestSyncLog_NewFailedSyncLog` | `datalake/model/sync_log_test.go` | Asserts `Status="FAILED"`, error message capture, zeroed counts, and legacy alias synchronization. |
| `TC-SL-03` | `TestSyncLog_Normalize_LegacyRecord` | `datalake/model/sync_log_test.go` | Verifies bidirectional backfilling between legacy and canonical BSON formats. |
| `TC-SL-04` | `TestSyncLog_Normalize_NilReceiver` | `datalake/model/sync_log_test.go` | Asserts nil-safety and zero panic behavior on nil receivers. |
| `TC-SL-05` | `TestSyncLog_BSONSerialization` | `datalake/model/sync_log_test.go` | Confirms round-trip BSON marshaling with exact camelCase tags matching spec. |
| `TC-SR-01` | `TestRecordSyncLog_Success` | `storage/mongo_repository_test.go` | Verifies automated lazy creation of both indexes and document insertion into `dataSync`. |
| `TC-SR-02` | `TestRecordSyncLog_FailedEntry` | `storage/mongo_repository_test.go` | Verifies insertion of failed sync records with error message and zero upserts. |
| `TC-SR-03` | `TestRecordSyncLog_IndexError` | `storage/mongo_repository_test.go` | Verifies proper error propagation and write abortion when index creation fails. |
| `TC-SR-04` | `TestGetSyncLogs_MixedCanonicalAndLegacy` | `storage/mongo_repository_test.go` | Verifies compound sorting (`syncedAt` then `sync_timestamp`) and automatic normalization of mixed documents. |

#### Phase 2 Centralized Orchestrator Integration Test Suite:
| Test Identifier | Test Function | Test File | Architectural Validation Target |
| :--- | :--- | :--- | :--- |
| `TC-DL-SYNC-01` | `TestTC_DL_SYNC_01_SuccessfulIngestionAudit` | `datalake/datalake_test.go` | Verifies successful ingestion calls `RecordSyncLog` with `Status="SUCCESS"`, non-zero `DurationMs`, accurate `UpsertedCount`, `MatchedCount`, and `FileHash`. |
| `TC-DL-SYNC-02` | `TestTC_DL_SYNC_02_CSVParseFailureAudit` | `datalake/datalake_test.go` | Verifies CSV parse failure calls `RecordSyncLog` with `Status="FAILED"`, error details preserved, and `UpsertedCount=0`. |
| `TC-DL-SYNC-03` | `TestTC_DL_SYNC_03_EmptyFileIngestionAudit` | `datalake/datalake_test.go` | Verifies empty file ingestion calls `RecordSyncLog` with `Status="SUCCESS"`, `RecordCount=0`, and `UpsertedCount=0`. |
| `TC-DL-SYNC-04` | `TestTC_DL_SYNC_04_StorageFailureAudit` | `datalake/datalake_test.go` | Verifies storage failure calls `RecordSyncLog` with `Status="FAILED"` when database bulk write fails. |
| `TC-DL-SYNC-05` | `TestTC_DL_SYNC_05_SingleFileIngestionProgressAndAudit` | `datalake/datalake_test.go` | Verifies `processSingleFile` emits both real-time progress events (`PhaseValidating`, `PhaseParsing`, `PhaseUpserting`, `PhaseDone`) and final `dataSync` record. |

#### Desktop UI Frontend Test Suite (Vitest):
| Component | Vitest Test File | Tests Passed | Architectural Validation Target |
| :--- | :--- | :--- | :--- |
| `HistoryPanel.tsx` | `desktop/frontend/src/components/HistoryPanel.test.tsx` | 6 | Verifies loading spinner, empty run state, 7 table headers, legacy record fallback, canonical success/failed badges (`badge-success`/`badge-failed`) with stats, and error alert handling. |
| `UploadForm.tsx` | `desktop/frontend/src/components/UploadForm.test.tsx` | 7 | Verifies inputs, empty submission validation, 4-digit Account ID constraint, browse dialog mock, and DB offline/retry banner. |
| `ProgressPanel.tsx` | `desktop/frontend/src/components/ProgressPanel.test.tsx` | 4 | Verifies initial placeholder, dynamic percentage/labels during phases, color-coded live logs, and completion callback. |

#### Quality Verification Commands:
```bash
# Execute static quality, linter, and format gates
make check-quality

# Execute full Go suite under race detector
go test -count=1 -race ./...

# Target domain model tests
go test -v -race -count=1 ./datalake/model/...

# Target storage repository tests
go test -v -race -count=1 ./storage/...

# Target datalake orchestrator sync audit integration tests
go test -v -race -count=1 -run "TestTC_DL_SYNC" ./datalake/...

# Target desktop frontend Vitest suite (17 passed tests)
cd desktop/frontend && npm test -- --run
```

---

## 7. Specifications & References

- **MongoDB Atlas Datalake Specification**: [`docs/specs/mongodb-atlas-datalake.md`](./mongodb-atlas-datalake.md) (Section 5.2 `dataSync` Schema and Section 6.1 Operational Tooling)
- **Fidelity Portfolio Positions Ingestion**: [`docs/specs/fidelity-portfolio-ingestion.md`](./fidelity-portfolio-ingestion.md)
- **Desktop Application Phase 3 & 4 Specification**: [`docs/specs/desktop/phase3-specification.md`](./desktop/phase3-specification.md)
- **Continuous Delivery Pipeline Specification**: [`docs/specs/continuous-delivery-pipeline.md`](./continuous-delivery-pipeline.md)
- **Datalake Architectural Context**: [`docs/architecture.md`](../architecture.md)
