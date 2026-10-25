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

---

## 3. Decision Record (M0 Decisions)

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

| Collection | Index Name | Keys (Ordered) | Unique | Sparse | Upsert Filter |
| :--- | :--- | :--- | :---: | :---: | :--- |
| `transactions_chase` | `idx_chase_account_posting_date` | `{ accountId: 1, postingDate: -1 }` | no | no | — |
| `transactions_chase` | `idx_chase_account_post_desc_amount_row` | `{ accountId: 1, postingDate: 1, description: 1, amount: 1, "source.rowNumber": 1 }` | **yes** | **yes** | same 5 fields |
| `positions_fidelity` | `idx_account_symbol_asof` | `{ accountNumber: 1, symbol: 1, asOfDate: 1 }` | **yes** | no | same 3 fields |
| `transactions_synthetic` | *(none)* | — | — | — | Chase 5-field filter (D4) |
| `dataSync` | `idx_datasync_synced_at` | `{ syncedAt: -1 }` | no | no | — |
| `dataSync` | `idx_datasync_filename_synced_at` | `{ fileName: 1, syncedAt: -1 }` | no | no | — |

### 4.2 BSON Document Schemas & DTO Mappings

#### `lineageDoc` (Embedded Subdocument)
```go
type lineageDoc struct {
    S3URI      string    `bson:"s3Uri,omitempty"` // Phase 4 placeholder; omitted when empty in Phase 3
    FileName   string    `bson:"fileName"`
    RowNumber  int64     `bson:"rowNumber"`        // Must be int64 (Go int encodes as BSON int32)
    IngestedAt time.Time `bson:"ingestedAt"`
    SHA256     string    `bson:"sha256,omitempty"` // Mapped from Lineage.FileHash
}
```

#### `chaseDoc` (`transactions_chase` & `transactions_synthetic`)
- `accountId` (string), `postingDate` (UTC date), `transactionDate` (UTC date, `omitempty`, 2F), `description` (string), `amount` (double), `transactionType` (string), `balance` (double, `omitempty`), `category` (string, `omitempty`), `details` (string, `omitempty`), `checkOrSlipNum` (string, `omitempty`), `memo` (string, `omitempty`), `dataSource` (string: `"chase"` or `"synthetic"`), `source` (`lineageDoc`).

#### `fidelityDoc` (`positions_fidelity`)
- `accountNumber` (string), `accountName` (string), `symbol` (string), `description` (string), `quantity` (double), `lastPrice` (double), `lastPriceChange` (double, 2E), `currentValue` (double), `todaysGainLossDollar` (double, 2E), `todaysGainLossPct` (double, 2E), `totalGainLossDollar` (double, 2E), `totalGainLossPct` (double, 2E), `percentOfAccount` (double, 2E), `costBasisTotal` (double), `averageCostBasis` (double, 2E), `type` (string, 2E, emitted even if `""`), `asOfDate` (UTC date, 2D), `dataSource` (string: `"fidelity"`), `source` (`lineageDoc`).
- Note: Phase 2E numeric and string fields do not use `omitempty`; zero values are explicitly stored (`0.0` and `""`).

#### `syncLogDoc` (`dataSync` with Legacy Dual Fields)
- Canonical: `_id` (ObjectId, `omitempty`), `fileName` (string), `fileHash` (string, `omitempty`), `collectionName` (string, `omitempty`), `recordCount` (int64), `upsertedCount` (int64), `matchedCount` (int64), `status` (string), `errorMessage` (string, `omitempty`), `syncedAt` (date), `durationMs` (int64).
- Dual Fields (desktop compatibility): `collection_name` (string, `omitempty`), `sync_timestamp` (date, `omitempty`), `records_uploaded` (int64, `omitempty`, omitted on failure when 0).

#### Dotted `$set` Invariant
To prevent overwriting unmapped legacy lineage attributes (such as historical `source.s3Uri`), `$set` updates flatten the `source` struct to dotted field paths (`source.fileName`, `source.rowNumber`, `source.ingestedAt`, `source.sha256`).

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
1. Context canceled -> return `ctx.Err()`.
2. Nil log -> return `model.ErrNilSyncLog`.
3. Zero timestamp -> reject with `model.ErrMissingSyncTime` (no defaulting per D5).
4. Validation failure -> return wrapped `log.Validate()` error.
5. Lazily ensures `dataSync` indexes and executes `InsertOne`. Input struct is never mutated.

### 5.3 `ListSyncLogs(ctx, limit)` (`port.Repository`)
1. Non-positive `limit <= 0` defaults to `DefaultSyncLogLimit = 100`.
2. Queries `dataSync` sorted by `syncedAt: -1`.
3. Normalizes legacy dual fields where canonical fields are absent (`sync_timestamp` -> `SyncedAt`, `collection_name` -> `CollectionName`, `records_uploaded` -> `RecordCount`/`UpsertedCount`).

---

## 6. Error Taxonomy

- **Domain Sentinels (Passthrough)**: Domain errors pass through unchanged (`model.ErrMissingSyncTime`, `model.ErrNilSyncLog`, `model.ErrNilBatch`, `model.ErrMissingAsOfDate`, `model.ErrPayloadTypeMismatch`, `model.ErrInvalidDestination`).
- **Mongo Sentinels**:
  - `mongo.ErrTransient`: Transient failure (network partition, socket timeout); caller retry safe.
  - `mongo.ErrDuplicateKey`: Unique compound index violation (MongoDB code 11000).
  - `mongo.ErrMissingURI`: Constructor error when configuration URI is empty.
- **Object Store Sentinels**:
  - `port.ErrObjectNotFound`: Target S3 key does not exist.

---

## 7. Recorded Divergences from Legacy

1. **2E Fidelity Placeholder Superset**: Accepts extended placeholder tokens (`-`, `pending`, `unknown`, accounting parens) mapping to `0.0` or `""`.
2. **2F Strict Transaction Date Rejection**: Rejects unparseable non-placeholder dates with `ErrInvalidRow` instead of silently storing null.
3. **`source.s3Uri` Deferred**: New records omit `source.s3Uri` until Phase 4 supplies it from entrypoint metadata.

---

## 8. Supersession Notes

- **`docs/specs/mongodb-atlas-datalake.md` §5.1**: Superseded regarding speculative field names (`sourceFile`, `importedAt`, `rawDetails`). Live datalake fields (`fileName`, `ingestedAt`, `source.*`) specified herein are canonical.
- **`docs/specs/hexagonal-domain-core.md` §5.1**: Superseded regarding working index names (`idx_chase_dedup`, `idx_fidelity_dedup`). Live Atlas index names (`idx_chase_account_post_desc_amount_row`, `idx_account_symbol_asof`) specified herein are canonical.
