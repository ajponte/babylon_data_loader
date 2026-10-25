# Hexagonal Architecture Specification: Secondary Storage Adapters (Persistence & Object Store)

**Target Packages**: `internal/infra/storage/mongo/` and `internal/infra/storage/s3/`  
**Architectural Role**: Hexagonal Secondary Driven Adapters (Persistence & Datalake Storage)  
**Document Status**: Active / Canonical Storage Specification (Phase 3)  
**Primary Ports**: [`internal/domain/port/repository.go`](../../internal/domain/port/repository.go) (`port.Repository`), [`internal/domain/port/object_store.go`](../../internal/domain/port/object_store.go) (`port.ObjectStore`)  
**Parent Document**: [`docs/specs/hexagonal-domain-core.md`](./hexagonal-domain-core.md)  

---

## 1. Executive Summary & Behavior-Preservation Invariant

The secondary storage adapters encapsulate all persistence and datalake interactions behind technology-agnostic domain ports. They isolate MongoDB Atlas driver mechanics and AWS S3 SDK calls from the hexagonal core.

**Headline Invariant**: For identical inputs, storage adapters write identical documents (field names, BSON types, upsert keys) to identical collections and identical `dataSync` audit records as the legacy code. This guarantees that Phase 4 cuts over without a data migration. The only approved divergences are D4 (synthetic collection shape unified with Chase) and `source.s3Uri` on new documents, which Phase 4 supplies from Lambda entrypoint metadata.

---

## 2. Package Layout & Import Aliases

| Directory | Package Name | Driver / SDK Import Alias | Primary Responsibility |
| :--- | :--- | :--- | :--- |
| `internal/infra/storage/mongo/` | `mongo` | `mongodrv "go.mongodb.org/mongo-driver/mongo"` | MongoDB repository, bulk upserts, `$set` builder, index lifecycle, and `dataSync` audit logging. |
| `internal/infra/storage/s3/` | `s3` | `awss3 "github.com/aws/aws-sdk-go-v2/service/s3"` | AWS S3 object store adapter, streaming download, and archive copy-delete operations. |

### 2.1 Repository Architecture & Driver Seams

Authoritative implementation reference: [`internal/infra/storage/mongo/repository.go`](../../internal/infra/storage/mongo/repository.go).

The MongoDB persistence adapter [`Repository`](../../internal/infra/storage/mongo/repository.go) implements [`port.Repository`](../../internal/domain/port/repository.go) with decoupled driver interfaces and defensive lifecycle management:

1. **Configuration & Defaults**:
   - `Config` specifies `URI` (required), `Database` (defaults to `"datalake"` via `withDefaults()`), and `Timeout` (connect/ping budget, defaults to `10s`).
   - Constructor `NewRepository(ctx, cfg)` validates that `URI` is non-empty (`ErrMissingURI`), applies defaults, establishes connection with context timeout, and verifies connectivity via `client.Ping`. Zero indexes are created at construction time.
2. **Unexported Driver Seams (`database` & `collection`)**:
   - To enable 100% hermetic unit testing without running a live MongoDB instance or external testcontainers during `make test-ci`, driver operations are mediated through unexported interfaces:
     ```go
     type database interface {
         Collection(name string) collection
     }

     type collection interface {
         InsertOne(ctx context.Context, doc any, opts ...*options.InsertOneOptions) (*mongodrv.InsertOneResult, error)
         CreateIndexes(ctx context.Context, models []mongodrv.IndexModel) error
     }
     ```
   - Production instances wrap the official Go MongoDB driver via `realDatabase` and `realCollection`.
   - Unit test suites instantiate repositories via unexported `newRepository(db database)` using `fakeDatabase` and `fakeCollection` mocks.
3. **Nil-Safe Resource Disposal (`Close`)**:
   - `Close(ctx context.Context) error` is strictly nil-safe: calling `Close()` on a nil `*Repository` pointer or an uninitialized repository instance returns `nil` without panicking.
4. **Credential Leak Defense**:
   - Connection URIs, plaintext passwords, and usernames are strictly sanitized from all error messages returned by `NewRepository` via `sanitizeURIError`.
   - Verifies against raw connection string leaks and embedded credentials (`mongodb://user:password@host/`), replacing sensitive fragments with `[REDACTED_URI]`, `[REDACTED]`, or `://[REDACTED]:[REDACTED]@` (`TestNewRepository_ErrorDoesNotLeakCredentials`).
5. **Retry-Safe Deterministic Index Lifecycle**:
   - Indexes are registered in an immutable catalog `indexCatalog() []collectionIndexes`.
   - `ensureIndexes(ctx, coll)` protects index creation with a mutex (`r.mu.Lock()`). Crucially, `r.ensured[coll] = true` is set **strictly after** driver `CreateIndexes` returns without error. If index creation fails due to a transient error, the flag remains unset, allowing subsequent retry calls to re-attempt index creation (`TestEnsureIndexes_RetriesAfterFailure`).
   - Concurrent callers synchronize safely via the mutex, ensuring indexes are created exactly once under concurrent traffic (`TestEnsureIndexes_ConcurrentCallsCreateOnce`).
   - Public `EnsureIndexes(ctx)` iterates the catalog in deterministic slice order to ensure upfront initialization when desired.

### 2.2 S3 Client Architecture & Driver Seams

Authoritative implementation reference: [`internal/infra/storage/s3/client.go`](../../internal/infra/storage/s3/client.go).

The AWS S3 datalake storage adapter [`Client`](../../internal/infra/storage/s3/client.go) implements [`port.ObjectStore`](../../internal/domain/port/object_store.go) (`var _ port.ObjectStore = (*Client)(nil)`), isolating AWS SDK dependencies behind a mockable interface seam:

1. **Configuration & Constructors**:
   - `Config` specifies:
     - `Bucket` (string, required): target S3 datalake bucket name.
     - `Region` (string, optional): explicit AWS region. When omitted or empty, constructor falls back to standard AWS SDK default configuration credentials and region resolution chain.
   - `NewClient(api ObjectAPI, bucket string) (*Client, error)`: constructor accepting an explicit `ObjectAPI` driver implementation and bucket string. Validates that `api` is non-nil (`ErrNilAPI`) and `bucket` is non-empty after trimming whitespace (`ErrMissingBucket`).
   - `NewClientFromConfig(ctx context.Context, cfg Config) (*Client, error)`: constructs a production client using `awsconfig.LoadDefaultConfig(ctx, optFns...)` with optional region overrides, instantiating the SDK client via `awss3.NewFromConfig(awsCfg)`. Validates `cfg.Bucket` before attempting configuration loading.
2. **Mockable `ObjectAPI` Driver Seam**:
   - To achieve 100% hermetic unit testing without real AWS network calls, live cloud infrastructure, or external credentials in CI (`make test-ci`), driver interactions are decoupled via the `ObjectAPI` interface:
     ```go
     type ObjectAPI interface {
         GetObject(ctx context.Context, in *awss3.GetObjectInput, optFns ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
         CopyObject(ctx context.Context, in *awss3.CopyObjectInput, optFns ...func(*awss3.Options)) (*awss3.CopyObjectOutput, error)
         DeleteObject(ctx context.Context, in *awss3.DeleteObjectInput, optFns ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error)
     }
     ```
   - Production instances bind standard `*awss3.Client` (which satisfies `ObjectAPI` natively).
   - Test suites instantiate `Client` via `NewClient` using test double `fakeObjectAPI` ([`client_test.go`](../../internal/infra/storage/s3/client_test.go)), intercepting requests, capturing parameter payloads, and injecting deterministic error responses.

---

## 3. Decision Record (M0 Decisions)

> [!NOTE]
> **Milestone Status (Phase 3D)**: Milestone 1 (M1 - Contract Frozen DTO mappers), Milestone 2 (Phase 3C - Repository skeleton & dataSync audit sink), and Milestone 3 (Phase 3D - S3 Object Store adapter) are fully implemented and verified with 96.2% statement test coverage in [`internal/infra/storage/s3/`](../../internal/infra/storage/s3/) and 87.9% in [`internal/infra/storage/mongo/`](../../internal/infra/storage/mongo/). See [§9. Implementation Milestones & Verification](#9-implementation-milestones--verification) for milestone roadmap and test coverage evidence.

### 3.1 User Decisions (D1–D8, A1)

| ID | Decision | Rationale |
| :--- | :--- | :--- |
| **D1** | **Index Parity**: Preserve legacy index names, keys, and options exactly: `idx_chase_account_posting_date`, `idx_chase_account_post_desc_amount_row` (unique, sparse, includes `source.rowNumber`), `idx_account_symbol_asof`, `idx_datasync_synced_at`, `idx_datasync_filename_synced_at`. No index on `transactions_synthetic`. | Avoids `IndexOptionsConflict` (code 85) on live Atlas cluster. Retaining `source.rowNumber` prevents collapsing legitimate identical same-day transactions. |
| **D2** | **Fidelity Snapshot History**: Preserve one document per `(accountNumber, symbol, asOfDate)`. Requires `AsOfDate` parsed from statement footer. | Prevents subsequent portfolio imports from overwriting historical snapshots. |
| **D3** | **Live-Code BSON Field Parity**: Maintain live field names (`accountNumber`, `transactionType`, `source.{fileName,rowNumber,ingestedAt,sha256}`, `dataSource`, `costBasisTotal`), including legacy `dataSync` dual fields (`collection_name`, `sync_timestamp`, `records_uploaded`). | Upsert filters on renamed fields would create duplicate records instead of updating. |
| **D4** | **Synthetic Shape Parity**: Synthetic statements use Chase document shape and mapper (`dataSource: "synthetic"`). Non-PII test collection `transactions_synthetic` is manually dropped once by the user at Phase 4 cutover. | Extractor already emits `[]model.ChaseTransaction`. |
| **D5** | **`SaveBatch` Failure Contract**: Valid batches returning an error yield `(failedSyncLog, wrappedErr)` built via `model.NewFailedSyncLog`. Nil or invalid batches return `(nil, err)`. `LogSync` strictly rejects zero `SyncedAt` (`model.ErrMissingSyncTime`). | Guarantees auditable attempts with routed collection name while enforcing validation. |
| **D6** | **Hermetic CI & Integration Isolation**: Integration tests use `//go:build integration` under `make test-integration` against local containerized Mongo (`babylon_deploy/local`). Excluded from hermetic `make test-ci`. | Proves unique index and E11000 semantics without slow testcontainer dependencies in CI. |
| **D7** | **S3 Adapter Parallelism**: S3 adapter implemented in `internal/infra/storage/s3/` behind `port.ObjectStore` in parallel. | Independent adapter with low coupling to MongoDB persistence. |
| **D8** | **`ListSyncLogs` Read Path**: Implemented in storage layer for desktop History panel parity before Phase 5 cleanup. | Precludes regression when legacy `storage/` package is decommissioned. |
| **A1** | **Legacy-Only Fields Resolution (Option C)**: Fidelity performance/allocation metrics restored via Phase 2E; Chase `TransactionDate` restored via Phase 2F. Provenance `source.s3Uri` declared as empty `omitempty` placeholder in Phase 3, populated in Phase 4. | Eliminates silent financial data loss on new ingests while isolating file transport concerns. |

### 3.2 Derived Engineering Decisions (E1–E8)

- **E1**: Constructors return concrete types (`*mongo.Repository`, `*s3.Client`) with compile-time `var _ port.X = (*T)(nil)` assertions.
- **E2**: Unit tests utilize unexported driver interfaces (`database`, `collection`) and exported `s3.ObjectAPI` (0 network calls in `make test-ci`).
- **E3**: Indexes ensured lazily before first write per collection, plus idempotent public `EnsureIndexes(ctx)`. Flag set only on success (no `sync.Once`).
- **E4**: Storage adapters do not log. Connection URIs never appear in errors or strings.
- **E5**: Failure-audit context policies (`context.WithoutCancel` + 5s) reside in pipeline orchestration (Phase 4), not the adapter.
- **E6**: `$set` updates write `source.*` as dotted paths; legacy-only top-level fields are omitted from DTOs to preserve existing data on disk.
- **E7**: Partial `BulkWriteException` results are discarded (legacy parity); failed `SyncLog` records zero upserted/matched counts.
- **E8**: Layer isolation enforced via `depguard` rules in `.golangci.yml`.

---

## 4. Authoritative Legacy-Compatibility Tables

### 4.1 Index Catalog

Authoritative implementation references: [`internal/infra/storage/mongo/repository.go`](../../internal/infra/storage/mongo/repository.go) and [`internal/infra/storage/mongo/indexes.go`](../../internal/infra/storage/mongo/indexes.go).

| Collection | Index Name | Keys (Ordered) | Unique | Sparse | Upsert Filter |
| :--- | :--- | :--- | :---: | :---: | :--- |
| `transactions_chase` | `idx_chase_account_posting_date` | `{ accountId: 1, postingDate: -1 }` | no | no | — |
| `transactions_chase` | `idx_chase_account_post_desc_amount_row` | `{ accountId: 1, postingDate: 1, description: 1, amount: 1, "source.rowNumber": 1 }` | **yes** | **yes** | same 5 fields |
| `positions_fidelity` | `idx_account_symbol_asof` | `{ accountNumber: 1, symbol: 1, asOfDate: 1 }` | **yes** | no | same 3 fields |
| `transactions_synthetic` | *(none)* | — | — | — | Chase 5-field filter (D4) |
| `dataSync` | `idx_datasync_synced_at` | `{ syncedAt: -1 }` | no | no | — |
| `dataSync` | `idx_datasync_filename_synced_at` | `{ fileName: 1, syncedAt: -1 }` | no | no | — |

### 4.2 BSON Document Schemas & DTO Mappings

Authoritative implementation reference: [`internal/infra/storage/mongo/mapper.go`](../../internal/infra/storage/mongo/mapper.go).

#### `lineageDoc` (Embedded Subdocument)
```go
type lineageDoc struct {
	// S3URI is a Phase 4 placeholder: batch/file metadata supplied by the Lambda entrypoint.
	// Always empty in Phase 3, so omitempty keeps it out of inserts and $set (E6 preserves
	// existing values). Field order matches legacy datalake/model.SourceLineage.
	S3URI      string    `bson:"s3Uri,omitempty"`
	FileName   string    `bson:"fileName"`
	RowNumber  int64     `bson:"rowNumber"` // int64, NOT int: Go int encodes as BSON int32.
	IngestedAt time.Time `bson:"ingestedAt"`
	SHA256     string    `bson:"sha256,omitempty"` // from Lineage.FileHash.
}
```

#### `chaseDoc` (`transactions_chase` & `transactions_synthetic`)
```go
type chaseDoc struct {
	AccountID       string     `bson:"accountId"`
	PostingDate     time.Time  `bson:"postingDate"`
	TransactionDate *time.Time `bson:"transactionDate,omitempty"` // 2F; nil for checking/synthetic.
	Description     string     `bson:"description"`
	Amount          float64    `bson:"amount"`
	TransactionType string     `bson:"transactionType"`
	Balance         *float64   `bson:"balance,omitempty"`
	Category        string     `bson:"category,omitempty"`
	Details         string     `bson:"details,omitempty"`
	CheckOrSlipNum  string     `bson:"checkOrSlipNum,omitempty"`
	Memo            string     `bson:"memo,omitempty"`
	DataSource      string     `bson:"dataSource"`
	Source          lineageDoc `bson:"source"`
}
```

#### `fidelityDoc` (`positions_fidelity`)
```go
// Field order matches legacy datalake/model.FidelityPosition. No omitempty on the 2E
// fields: legacy always emits them (placeholders stored as 0.0 / "").
type fidelityDoc struct {
	AccountNumber        string     `bson:"accountNumber"`
	AccountName          string     `bson:"accountName"`
	Symbol               string     `bson:"symbol"`
	Description          string     `bson:"description"`
	Quantity             float64    `bson:"quantity"`
	LastPrice            float64    `bson:"lastPrice"`
	LastPriceChange      float64    `bson:"lastPriceChange"` // 2E.
	CurrentValue         float64    `bson:"currentValue"`
	TodaysGainLossDollar float64    `bson:"todaysGainLossDollar"` // 2E.
	TodaysGainLossPct    float64    `bson:"todaysGainLossPct"`    // 2E, percentage points.
	TotalGainLossDollar  float64    `bson:"totalGainLossDollar"`  // 2E.
	TotalGainLossPct     float64    `bson:"totalGainLossPct"`     // 2E, percentage points.
	PercentOfAccount     float64    `bson:"percentOfAccount"`     // 2E, percentage points.
	CostBasisTotal       float64    `bson:"costBasisTotal"`
	AverageCostBasis     float64    `bson:"averageCostBasis"` // 2E.
	Type                 string     `bson:"type"`             // 2E, emitted even when "".
	AsOfDate             time.Time  `bson:"asOfDate"`
	DataSource           string     `bson:"dataSource"`
	Source               lineageDoc `bson:"source"`
}
```

#### Pure Mapper Signatures

Persistence transformations are implemented as pure, side-effect-free functions in [`internal/infra/storage/mongo/mapper.go`](../../internal/infra/storage/mongo/mapper.go) and [`internal/infra/storage/mongo/audit.go`](../../internal/infra/storage/mongo/audit.go). They accept immutable domain models by value or reference and return unexported BSON DTOs:

- `toChaseDoc(tx model.ChaseTransaction, dataSource string) chaseDoc`: Maps domain Chase transactions (and synthetic transactions) to persistence DTO `chaseDoc`, parameterizing `dataSource` (`"chase"` or `"synthetic"`).
- `toFidelityDoc(p model.FidelityPosition) fidelityDoc`: Maps domain Fidelity positions to persistence DTO `fidelityDoc`, binding `dataSource` to `"fidelity"`.
- `toLineageDoc(l model.SourceLineage) lineageDoc`: Maps domain lineage metadata to embedded subdocument `lineageDoc`, converting `RowNumber` to `int64` (BSON int64 compatibility) and leaving `S3URI` empty for Phase 4.
- `toSyncLogDoc(log *model.SyncLog) syncLogDoc`: Maps domain audit logs to persistence DTO `syncLogDoc` in [`internal/infra/storage/mongo/audit.go`](../../internal/infra/storage/mongo/audit.go), converting `SyncedAt` to UTC and mapping legacy desktop dual fields (`collection_name`, `sync_timestamp`, and conditional `records_uploaded`).

#### Memory Safety & Immutability Invariants

1. **Pointer Un-aliasing (Optional Field Isolation)**:
   - For optional pointer fields (`TransactionDate *time.Time` and `Balance *float64` in `chaseDoc`), mappers allocate fresh heap memory and copy the dereferenced value (deep copy) rather than aliasing the pointer from the incoming domain model.
   - This guarantees complete pointer isolation between caller and callee: subsequent mutations of domain model pointers (or reuse across concurrent worker goroutines) will never mutate persistence DTO values, and vice versa.
2. **Timestamp Normalization & BSON Millisecond Truncation**:
   - All temporal fields (`PostingDate`, `TransactionDate`, `AsOfDate`, `Source.IngestedAt`, and `SyncedAt`) are normalized to UTC instants using `.UTC()`.
   - MongoDB BSON `DateTime` stores timestamps as signed 64-bit integers representing milliseconds since Unix epoch (UTC). Any sub-millisecond nanosecond components in Go `time.Time` values are cleanly truncated by the BSON encoder (`t.UnixMilli()`) without shifting the calendar date, hour, or second across timezones.
3. **Zero Mutation Guarantee**:
   - All mappers guarantee that caller domain entities remain strictly immutable and free from side effects.

#### `syncLogDoc` (`dataSync` with Legacy Dual Fields)

Authoritative implementation reference: [`internal/infra/storage/mongo/audit.go`](../../internal/infra/storage/mongo/audit.go).

```go
type syncLogDoc struct {
	ID             primitive.ObjectID `bson:"_id,omitempty"`
	FileName       string             `bson:"fileName"`
	FileHash       string             `bson:"fileHash,omitempty"`
	CollectionName string             `bson:"collectionName,omitempty"`
	RecordCount    int64              `bson:"recordCount"`
	UpsertedCount  int64              `bson:"upsertedCount"`
	MatchedCount   int64              `bson:"matchedCount"`
	Status         string             `bson:"status"`
	ErrorMessage   string             `bson:"errorMessage,omitempty"`
	SyncedAt       time.Time          `bson:"syncedAt"`
	DurationMs     int64              `bson:"durationMs"`

	DualCollectionName  string    `bson:"collection_name,omitempty"`
	DualSyncTimestamp   time.Time `bson:"sync_timestamp,omitempty"`
	DualRecordsUploaded int64     `bson:"records_uploaded,omitempty"`
}
```

##### Mapping Rules & Dual-Field Compatibility (`toSyncLogDoc`)
Implemented in [`internal/infra/storage/mongo/audit.go`](../../internal/infra/storage/mongo/audit.go), the pure mapper `toSyncLogDoc(log *model.SyncLog) syncLogDoc` translates domain audit logs to BSON documents:
1. **Canonical Attributes**:
   - `FileName`, `FileHash`, `CollectionName`, `RecordCount`, `UpsertedCount`, `MatchedCount`, and `DurationMs` map directly from the domain model.
   - `Status` converts the domain `model.SyncStatus` enum to string (`"SUCCESS"` or `"FAILED"`).
   - `ErrorMessage` is emitted when populated; `omitempty` omits it on successful synchronizations.
2. **UTC Normalization**:
   - `SyncedAt` is converted to UTC (`log.SyncedAt.UTC()`) before assignment to both canonical `SyncedAt` and legacy `DualSyncTimestamp`.
3. **Desktop Compatibility Dual Fields**:
   - `DualCollectionName` (`collection_name,omitempty`): mirrors `CollectionName`.
   - `DualSyncTimestamp` (`sync_timestamp,omitempty`): mirrors UTC `SyncedAt`.
   - `DualRecordsUploaded` (`records_uploaded,omitempty`): mirrors `UpsertedCount`. Because of `omitempty`, when `UpsertedCount == 0` (e.g., in failure logs), `records_uploaded` is excluded from the inserted BSON document, preserving exact bit-for-bit parity with legacy ingestion logs.

#### Dotted `$set` Invariant
To prevent overwriting unmapped legacy lineage attributes (such as historical `source.s3Uri`), `$set` updates flatten the `source` struct to dotted field paths (`source.fileName`, `source.rowNumber`, `source.ingestedAt`, `source.sha256`).

### 4.3 Record Write Keys, Upsert Filters, Flattened Dotted `$set` Builder & Batch Router

Authoritative implementation reference: [`internal/infra/storage/mongo/indexes.go`](../../internal/infra/storage/mongo/indexes.go).

1. **Upsert Filter Builders**:
   - `chaseFilter(d chaseDoc) bson.D`: Returns 5-field filter matching the unique compound index keys: `{accountId, postingDate, description, amount, source.rowNumber}`. Value types match stored BSON types (string, UTC BSON Date, string, float64, int64).
   - `fidelityFilter(d fidelityDoc) bson.D`: Returns 3-field filter matching the unique compound index keys: `{accountNumber, symbol, asOfDate}`. Value types match stored BSON types (string, string, UTC BSON Date).
2. **Flattened Dotted `$set` Builder (`setDocument`)**:
   - Marshals the persistence DTO to BSON, decodes into `bson.D`, and flattens embedded `source` subdocument elements into dotted paths (`source.fileName`, `source.rowNumber`, `source.ingestedAt`, `source.sha256`).
   - `source.s3Uri` is omitted when empty due to `lineageDoc.S3URI` `omitempty` tag (decision E6), preserving any existing historical values in MongoDB Atlas.
   - Restored A1 fields (2E zero-value numeric fields `0.0`, empty string `""`, and 2F `transactionDate` for credit card) are preserved in `$set`.
   - Wraps the flattened document in `bson.D{{Key: "$set", Value: flattened}}`.
3. **Destination Router & Write Planner**:
   - `collectionFor(dest model.Destination) (string, error)`: Maps destinations to target collection names. `DestChaseChecking` and `DestChaseCreditCard` share the identical underlying string value (`"transactions_chase"`); matching `DestChaseChecking` avoids Go duplicate-case compile errors while covering both destinations.
   - `buildWritePlan(batch *model.IngestionBatch) (writePlan, error)`: Routes batches into collection names and unordered `mongodrv.UpdateOneModel` slices with `Upsert = true`.
   - Zero `AsOfDate` guard on Fidelity batches returns `model.ErrMissingAsOfDate` (defense-in-depth for decision D2). Empty batches (`RecordCount == 0`) return a valid plan with 0 models and nil error.

---

## 5. Storage Adapter Contracts

### 5.1 `SaveBatch(ctx, batch)` (`port.Repository`)
1. Context canceled -> return `(nil, ctx.Err())`.
2. Invalid batch (`batch.Validate() != nil`) -> return `(nil, err)`.
3. Routing/planning failure (zero `AsOfDate`, payload mismatch) -> return `(failedSyncLog, wrappedErr)`.
4. Empty batch (`RecordCount == 0`) -> return `(successSyncLog{0,0,0}, nil)` without driver calls.
5. Ensures indexes lazily before first collection write.
6. Executes unordered bulk upsert (`SetOrdered(false)`).
7. Driver failure -> return `(failedSyncLog, classify(err))`; partial counts discarded.
8. Success -> return `(successSyncLog, nil)`. Returned logs always satisfy `model.SyncLog.Validate()`.

### 5.2 `LogSync(ctx, log)` (`port.Repository`)

Authoritative implementation reference: [`internal/infra/storage/mongo/audit.go`](../../internal/infra/storage/mongo/audit.go).

Writes an audit record to the `dataSync` datalake collection. The contract enforces the following verified invariants:
1. **Context Cancellation**: Evaluates `ctx.Err()` immediately before any operations; returns the cancellation or deadline error without side effects (`TestLogSync_CancelledContext`).
2. **Nil Guard**: Rejects nil log pointers with sentinel `model.ErrNilSyncLog` (`TestLogSync_RejectsNilAndInvalid`).
3. **Zero Timestamp Rejection**: Validates domain rules via `log.Validate()`. A zero `SyncedAt` value returns `model.ErrMissingSyncTime`; timestamps are never defaulted or synthesized per decision D5 (`TestLogSync_RejectsZeroSyncedAt`).
4. **Validation Wrapping**: Non-nil validation errors (e.g., missing `ErrorMessage` on failed sync) are wrapped with descriptive prefix `mongo: invalid sync log: %w`.
5. **Lazy Index Creation**: Lazily ensures `dataSync` indexes (`idx_datasync_synced_at`, `idx_datasync_filename_synced_at`) via `ensureIndexes(ctx, dataSyncCollection)` before inserting.
6. **Insertion & Error Wrapping**: Encodes document via `toSyncLogDoc` and executes `InsertOne(ctx, doc)`. Driver errors are wrapped with collection context: `mongo: log sync to dataSync: %w` (`TestLogSync_InsertErrorWrapped`).
7. **Input Immutability**: The caller's `*model.SyncLog` instance is treated as read-only and is never mutated during mapping or insertion (`TestLogSync_DoesNotMutateInput`).

### 5.3 `ListSyncLogs(ctx, limit)` (`port.Repository`)
1. Non-positive `limit <= 0` defaults to `DefaultSyncLogLimit = 100`.
2. Queries `dataSync` sorted by `syncedAt: -1`.
3. Normalizes legacy dual fields where canonical fields are absent (`sync_timestamp` -> `SyncedAt`, `collection_name` -> `CollectionName`, `records_uploaded` -> `RecordCount`/`UpsertedCount`).

### 5.4 S3 Object Store Contract (`port.ObjectStore`)

Authoritative implementation references: [`internal/domain/port/object_store.go`](../../internal/domain/port/object_store.go) and [`internal/infra/storage/s3/client.go`](../../internal/infra/storage/s3/client.go).

The datalake object store adapter provides streaming raw statement retrieval and transactional-safe post-processing archiving:

#### `DownloadStream(ctx context.Context, key string) (io.ReadCloser, error)`
1. **Key Validation**: Rejects empty or whitespace-only keys with sentinel `s3.ErrMissingKey` without invoking SDK calls.
2. **Context Propagation**: Passes caller context directly to `api.GetObject`, ensuring client cancellations or execution timeouts abort active network transfers.
3. **Streaming Download Semantics**: Returns unbuffered `io.ReadCloser` (`out.Body`) directly to the caller. Unlike legacy [`storage/s3.go`](../../storage/s3.go) (which wrote downloaded objects to temporary local disk files via `os.Create` / `io.Copy`), the hexagonal adapter streams raw bytes directly into memory buffers or stream parsers. This eliminates local disk allocations, filesystem contention, and temp-file leakage in serverless/container environments. Caller owns closing the returned reader.
4. **Hermetic Error Classification**: Translates AWS SDK `*types.NoSuchKey`, `*types.NotFound`, and coded error strings `"NoSuchKey"` / `"NotFound"` into wrapped sentinel `port.ErrObjectNotFound`. Transient or network failures wrap the underlying driver error.

#### `ArchiveFile(ctx context.Context, srcKey, destKey string) error`
1. **Key Validation**: Rejects empty or whitespace-only `srcKey` or `destKey` with `s3.ErrMissingKey`.
2. **URL-Escaped `CopySource` Parity**: Computes `CopySource` as `<bucket>/<url.PathEscape(segment)>...` where leading slashes are stripped and each path segment is escaped individually. This guarantees bit-for-bit parity with legacy [`storage/s3.go`](../../storage/s3.go) across keys with spaces (`bank report.csv` -> `bank%20report.csv`), unicode characters (`résumé.csv` -> `r%C3%A9sum%C3%A9.csv`), and percent literals (`100%real.csv` -> `100%25real.csv`).
3. **Strict Copy-Then-Delete Ordering**: Executes `CopyObject` first and verifies clean completion before calling `DeleteObject`. If `CopyObject` fails or encounters an error, the source object is **never deleted**, preserving datalake durability and preventing catastrophic data loss.
4. **Non-Atomic Idempotence & Safe Retries**: While S3 lacks multi-operation transactional atomicity, `ArchiveFile` is idempotent and safe for retries: `CopyObject` unconditionally overwrites destination keys, and `DeleteObject` is idempotent in S3. If copy succeeds but delete fails (e.g., delete permission denial), the delete error is surfaced immediately to the caller for operational triage.
5. **Driver Error Normalization**: If `CopyObject` fails due to a missing source (`*types.NoSuchKey`, `*types.NotFound`, or error codes `"NoSuchKey"` / `"NotFound"`), it is classified as `port.ErrObjectNotFound`. Error classification avoids direct dependencies on `smithy-go` types by using duck-typed interfaces (`type coder interface{ ErrorCode() string }`).

---

## 6. Error Taxonomy

- **Domain Sentinels (Passthrough)**: Domain errors pass through unchanged (`model.ErrMissingSyncTime`, `model.ErrNilSyncLog`, `model.ErrNilBatch`, `model.ErrMissingAsOfDate`, `model.ErrPayloadTypeMismatch`, `model.ErrInvalidDestination`).
- **Mongo Sentinels**:
  - `mongo.ErrTransient`: Transient failure (network partition, socket timeout); caller retry safe.
  - `mongo.ErrDuplicateKey`: Unique compound index violation (MongoDB code 11000).
  - `mongo.ErrMissingURI`: Constructor error when configuration URI is empty.
- **Object Store Sentinels**:
  - `port.ErrObjectNotFound`: Target S3 source key does not exist. Classified from `*types.NoSuchKey`, `*types.NotFound`, and duck-typed error codes `"NoSuchKey"` / `"NotFound"`.
  - `s3.ErrMissingBucket`: Constructor error when configuration bucket is empty or whitespace-only.
  - `s3.ErrMissingKey`: Operation error when source or destination key is empty or whitespace-only.
  - `s3.ErrNilAPI`: Constructor error when provided `ObjectAPI` interface implementation is nil.

---

## 7. Recorded Divergences from Legacy

1. **2E Fidelity Placeholder Superset**: Accepts extended placeholder tokens (`-`, `pending`, `unknown`, accounting parens) mapping to `0.0` or `""`.
2. **2F Strict Transaction Date Rejection**: Rejects unparseable non-placeholder dates with `ErrInvalidRow` instead of silently storing null.
3. **`source.s3Uri` Deferred**: New records omit `source.s3Uri` until Phase 4 supplies it from entrypoint metadata.
4. **S3 Streaming Download vs. Disk Spooling**: Legacy [`storage/s3.go`](../../storage/s3.go) forced S3 object downloads into local disk files via `os.Create` and `io.Copy`. The hexagonal adapter [`s3.Client`](../../internal/infra/storage/s3/client.go) returns an unbuffered `io.ReadCloser` stream directly to the caller (`port.ObjectStore.DownloadStream`), eliminating disk I/O bottlenecks and temporary file leakage in serverless execution environments.

---

## 8. Supersession Notes

- **`docs/specs/mongodb-atlas-datalake.md` §5.1**: Superseded regarding speculative field names (`sourceFile`, `importedAt`, `rawDetails`). Live datalake fields (`fileName`, `ingestedAt`, `source.*`) specified herein are canonical.
- **`docs/specs/hexagonal-domain-core.md` §5.1**: Superseded regarding working index names (`idx_chase_dedup`, `idx_fidelity_dedup`). Live Atlas index names (`idx_chase_account_post_desc_amount_row`, `idx_account_symbol_asof`) specified herein are canonical.

---

## 9. Implementation Milestones & Verification

| Milestone | Phase | Description | Status | Verification Criteria |
| :--- | :--- | :--- | :--- | :--- |
| **M0** | Phase 3A | Architecture specification, legacy parity tables, and depguard rules. | **Complete** | Layer isolation validated via `golangci-lint` depguard rules. |
| **M1** | Phase 3B | Contract Frozen: BSON persistence DTOs, pure mappers, and BSON serialization schemas in [`internal/infra/storage/mongo/mapper.go`](../../internal/infra/storage/mongo/mapper.go). | **Complete** | 100% statement test coverage in [`internal/infra/storage/mongo/mapper_test.go`](../../internal/infra/storage/mongo/mapper_test.go); pointer un-aliasing, UTC normalization, BSON int64 types, and field parity verified. |
| **M2** | Phase 3C / 3E | Repository skeleton, index lifecycle, `dataSync` audit sink, record write keys, upsert filters, flattened dotted `$set` builder, and batch router in [`internal/infra/storage/mongo/repository.go`](../../internal/infra/storage/mongo/repository.go), [`internal/infra/storage/mongo/audit.go`](../../internal/infra/storage/mongo/audit.go), and [`internal/infra/storage/mongo/indexes.go`](../../internal/infra/storage/mongo/indexes.go) (`SaveBatch` driver execution scheduled for Phase 3F). | **Complete** | 89.3% statement test coverage in [`internal/infra/storage/mongo/`](../../internal/infra/storage/mongo/); legacy index parity, upsert filters equal to unique keys, flattened lineage, and pure batch routing verified. |
| **M3** | Phase 3D | AWS S3 object store adapter behind `port.ObjectStore` in [`internal/infra/storage/s3/client.go`](../../internal/infra/storage/s3/client.go). | **Complete** | 96.2% statement test coverage in [`internal/infra/storage/s3/client_test.go`](../../internal/infra/storage/s3/client_test.go); hermetic S3 API mocks, streaming download, URL-escaping parity, and copy-then-delete ordering verified. |
| **M4** | Phase 4 | Pipeline cutover, Lambda entrypoint wiring, and legacy decommissioning. | *Planned* | Zero data migration cutover and end-to-end ingestion verification. |

### 9.1 Phase 3B Verification & Test Coverage Evidence

Milestone 1 (M1 - Contract Frozen) DTO mappers are fully implemented in [`internal/infra/storage/mongo/mapper.go`](../../internal/infra/storage/mongo/mapper.go) and achieve 100% statement test coverage across all branches in [`internal/infra/storage/mongo/mapper_test.go`](../../internal/infra/storage/mongo/mapper_test.go) (`go test -cover -race ./internal/infra/storage/mongo/...`).

Key verified invariants include:
- **Round-Trip BSON Schema Parity**: All field names, BSON types, and embedded subdocuments match live MongoDB Atlas collections exactly.
- **Pointer Un-aliasing Isolation**: Optional `TransactionDate` and `Balance` fields are verified to allocate distinct heap pointers, preventing shared memory mutation.
- **UTC Normalization & Millisecond Truncation**: Timestamps across non-UTC timezones convert to UTC, and sub-millisecond nanoseconds truncate cleanly without shifting calendar date or hour.
- **BSON Type Fidelity**: `source.rowNumber` is asserted as BSON `int64` (not `int32`), and legacy 2E numeric fields (`0.0`) and string fields (`""`) are explicitly emitted without omission.

### 9.2 Phase 3C Verification & Test Coverage Evidence

Phase 3C implements the repository skeleton, driver seams, index lifecycle management, and `dataSync` audit sink in [`internal/infra/storage/mongo/repository.go`](../../internal/infra/storage/mongo/repository.go) and [`internal/infra/storage/mongo/audit.go`](../../internal/infra/storage/mongo/audit.go). It achieves **87.9% statement test coverage** across the package (`go test -cover -race ./internal/infra/storage/mongo/...`) with 100% hermetic mock execution during `make check-quality` / `make test-ci`.

Key verified invariants include:
- **Credential Leak Defense**: `TestNewRepository_ErrorDoesNotLeakCredentials` asserts that connection URIs, usernames, and passwords never leak into constructor error messages or error string outputs upon connection or ping failures.
- **Retry-Safe Index Lifecycle**: `TestEnsureIndexes_RetriesAfterFailure` validates that transient index creation errors do not mark the collection as ensured; subsequent retries re-execute index creation until successful.
- **Thread-Safe Concurrent Index Creation**: `TestEnsureIndexes_ConcurrentCallsCreateOnce` executes 20 concurrent goroutines calling `EnsureIndexes`, verifying race-free execution and guaranteeing exactly 1 driver `CreateIndexes` call occurs.
- **Audit Sink Parity & Dual-Field Output**: `TestSyncLogDoc_LegacyBSONParity` and `TestLogSync_WritesDualFields` assert exact BSON field types and desktop compatibility dual fields (`collection_name`, `sync_timestamp`, and conditional `records_uploaded`).
- **Strict Domain Validation & Zero Defaulting**: `TestLogSync_RejectsZeroSyncedAt` and `TestLogSync_RejectsNilAndInvalid` verify that nil logs and zero `SyncedAt` timestamps are rejected with sentinel errors (`model.ErrMissingSyncTime`, `model.ErrNilSyncLog`) without synthesizing or defaulting timestamps.
- **Context Cancellation & Input Immutability**: `TestLogSync_CancelledContext` and `TestLogSync_DoesNotMutateInput` verify context cancellation short-circuits execution and caller domain objects remain strictly immutable.

### 9.3 Phase 3D Verification & Test Coverage Evidence

Phase 3D implements the AWS S3 object store adapter behind `port.ObjectStore` in [`internal/infra/storage/s3/client.go`](../../internal/infra/storage/s3/client.go). It achieves **96.2% statement test coverage** across `internal/infra/storage/s3` (`go test -cover -race ./internal/infra/storage/s3/...`) with zero race conditions, zero `depguard` violations, and 100% hermetic unit testing during `make check-quality` / `make test-ci`.

Key verified invariants include:
- **Hermetic Mock Seam (`fakeObjectAPI`)**: All operations (`GetObject`, `CopyObject`, `DeleteObject`) execute against the mockable `ObjectAPI` seam with 0 AWS network calls and 0 AWS credentials required (`TestNewClient_Validation`, `TestDownloadStream_ContextPropagated`).
- **Unbuffered Streaming Download**: `TestDownloadStream_ReturnsBodyUnbuffered` verifies that `DownloadStream` returns the S3 body `io.ReadCloser` directly without spooling to local disk or buffering in memory.
- **Strict Copy-Then-Delete Ordering**: `TestArchiveFile_CopyThenDelete` asserts exact call sequence `[CopyObject, DeleteObject]`. `TestArchiveFile_NoDeleteWhenCopyFails` proves that if `CopyObject` encounters an error, `DeleteObject` is never invoked, preventing data loss.
- **Delete Failure Propagation**: `TestArchiveFile_DeleteFailureSurfaced` validates that when `CopyObject` succeeds but `DeleteObject` fails, the error is surfaced to caller for retry handling.
- **Bit-for-Bit URL-Escaped `CopySource` Parity**: `TestArchiveFile_CopySourceEscaping_LegacyParity` table-driven test verifies segment-by-segment escaping (`url.PathEscape`) matching legacy [`storage/s3.go`](../../storage/s3.go) across spaces, plus signs, percent signs, unicode characters, leading slashes, and nested directories.
- **Hermetic Error Classification**: `TestDownloadStream_NoSuchKey` and `TestArchiveFile_SourceMissing` assert that `*types.NoSuchKey`, `*types.NotFound`, and coded error strings `"NoSuchKey"` / `"NotFound"` map to sentinel `port.ErrObjectNotFound` without importing smithy-go.
- **Layer Isolation & Architectural Invariants**: Clean `depguard` linting confirms that `internal/infra/storage/s3` only imports allowed packages (`internal/domain/port`, standard library, and approved AWS SDK packages), maintaining strict hexagonal isolation and unwired adapter status until Phase 4.

### 9.4 Phase 3E Verification & Test Coverage Evidence

Phase 3E implements the record write keys, legacy index catalog registration, unique upsert filters, flattened dotted `$set` builder, and batch write planner in [`internal/infra/storage/mongo/indexes.go`](../../internal/infra/storage/mongo/indexes.go). It achieves **89.3% statement test coverage** across `internal/infra/storage/mongo` (`go test -cover -race ./internal/infra/storage/mongo/...`) with zero race conditions, zero `depguard` violations, and 100% hermetic unit testing during `make check-quality` / `make test-ci`.

Key verified invariants include:
- **Legacy Index Catalog Parity**: `TestIndexCatalog_LegacyParity` and `TestIndexCatalog_RegisteredInRepository` verify that `chaseIndexes()` and `fidelityIndexes()` match Atlas indexes byte-for-byte in names, key order, sort direction, unique constraints, and sparse options. Indexes are ensured in deterministic order (`dataSync`, `transactions_chase`, `positions_fidelity`), and `transactions_synthetic` has zero index specifications (decision D1).
- **Upsert Filter Invariant**: `TestUpsertFilter_ChaseLegacyParity`, `TestUpsertFilter_FidelityLegacyParity`, and `TestUpsertFilter_EqualsUniqueIndexKeys` prove that for all collections, the upsert filter keys match the unique compound index keys in order and data types (UTC dates, float64 amounts, int64 row numbers).
- **Dotted `$set` Lineage Preservation**: `TestSetDocument_FlattensSourceLineage` and `TestSetDocument_OmitsS3URIPlaceholder` verify that lineage fields are flattened to dotted paths (`source.fileName`, `source.rowNumber`, `source.ingestedAt`, `source.sha256`) with no top-level `source` object and no empty `source.s3Uri`, preserving historical unmapped lineage in MongoDB Atlas (decision E6).
- **Restored A1 Fields**: `TestSetDocument_IncludesRestoredA1Fields` verifies that all eight restored 2E fields appear in Fidelity `$set` updates (even when zero), and 2F `transactionDate` is present for credit cards and omitted for checking.
- **Pure Batch Router & Write Planner**: `TestCollectionFor_AllDestinations`, `TestBuildWritePlan_PayloadMismatch`, `TestBuildWritePlan_SyntheticUsesChaseShape`, `TestBuildWritePlan_UpsertModels`, `TestBuildWritePlan_FidelityMissingAsOfDate`, and `TestBuildWritePlan_EmptyPayload` verify destination mapping without duplicate switch cases, payload type assertion guards, synthetic document routing with Chase shape (decision D4), zero `AsOfDate` sentinel error enforcement (decision D2), and empty batch handling with zero allocations.


