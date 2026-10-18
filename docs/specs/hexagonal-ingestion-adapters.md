# Hexagonal Architecture Specification: Secondary Ingestion Adapters (Sources)

**Target Package**: `babylon_data_loader/internal/infra/sources/`  
**Architectural Role**: Hexagonal Secondary Driven Adapters (Input Source Extractors)  
**Document Status**: Active / Implemented (Hardened in Phases 1–3)  
**Primary Port**: [`internal/domain/port/extractor.go`](../../internal/domain/port/extractor.go)  
**Modular Architecture Directory**: [`docs/specs/architecture/`](./architecture/README.md)  

---

## 1. Executive Summary & Architectural Motivation

The ingestion layer in `babylon_data_loader` serves as the secondary driven boundary for ingesting heterogeneous financial statements, transactions, and portfolio records. To prevent banking format idiosyncrasies, malformed CSV records, and institution-specific schemas from leaking into core domain logic, all stream consumption, dialect classification, data sanitization, and lineage attribution are encapsulated inside dedicated secondary adapters.

```mermaid
flowchart TD
    subgraph CoreDomain ["Hexagonal Core Domain (internal/domain)"]
        PORT["port.Extractor<br/>• Extract(ctx, io.Reader, FileMetadata) (*IngestionBatch, error)<br/>• SourceType() string"]
        BATCH["model.IngestionBatch<br/>• Destination<br/>• AccountID<br/>• SourceFile<br/>• FileHash<br/>• IngestedAt<br/>• RecordCount<br/>• Payload"]
    end

    subgraph SecondaryAdapters ["Secondary Source Adapters (internal/infra/sources)"]
        CHASE["Chase Adapter<br/>internal/infra/sources/chase/"]
        FIDELITY["Fidelity Adapter<br/>internal/infra/sources/fidelity/"]
        SYNTHETIC["Synthetic Adapter<br/>internal/infra/sources/synthetic/"]
    end

    CHASE -.->|implements| PORT
    FIDELITY -.->|implements| PORT
    SYNTHETIC -.->|implements| PORT
    PORT -->|yields| BATCH
```

> [!NOTE]
> Detailed, composable specifications for each architectural subsystem have been organized into the [`docs/specs/architecture/`](./architecture/README.md) directory. This master specification provides the architectural catalog, system boundaries, and summary contracts.

---

## 2. Ingestion Architecture & Data Flow

Every source adapter executes a standardized 4-stage data acquisition lifecycle:

```mermaid
sequenceDiagram
    autonumber
    participant App as Ingestion Orchestrator / Worker
    participant Adapter as Secondary Source Adapter (port.Extractor)
    participant Engine as Dedicated Parsing Engine
    participant Batch as Canonical IngestionBatch

    App->>Adapter: Extract(ctx, io.Reader, FileMetadata)
    Adapter->>Adapter: 1. Validate parameters & context short-circuit
    Adapter->>Adapter: 2. In-stream non-destructive classification (4KB Peek)
    Adapter->>Engine: 3. Streaming deserialization, stack rowReader, & bounds safety
    Engine-->>Adapter: Yield typed domain records ([]Transaction / []Position)
    Adapter->>Batch: 4. Construct & validate canonical batch envelope
    Adapter-->>App: *model.IngestionBatch
```

For the comprehensive data flow specification, stream lifecycle contracts, and forbidden dependency matrices, consult:
👉 **[Ingestion Overview Specification](./architecture/ingestion-overview.md)**

---

## 3. Package Structure & Responsibilities

The ingestion adapter package layout in `babylon_data_loader` strictly separates responsibilities:

| Directory | Package / Component | Architectural Role | Composable Spec Link |
| :--- | :--- | :--- | :--- |
| `internal/infra/sources/chase/` | `chase.Extractor` | Chase deposit and credit card statement extraction, dialect classification, zero-allocation parsing primitives, and date fallbacks. | **[Chase Adapter Spec](./architecture/chase-adapter.md)** |
| `internal/infra/sources/fidelity/` | `fidelity.Extractor` | Fidelity brokerage portfolio holdings extraction, UTF-8 BOM stripping, disclaimer filtering, and cash sweep normalization. | **[Fidelity Adapter Spec](./architecture/fidelity-adapter.md)** |
| `internal/infra/sources/synthetic/` | `synthetic.Extractor` | In-memory deterministic fixture generation via `math/rand/v2` PCG generator for load testing and benchmark harnesses. | **[Synthetic Adapter Spec](./architecture/synthetic-adapter.md)** |
| *(Cross-Cutting)* | Quality & Verification | Standard library isolation, race safety, comparative benchmarks, and cognitive complexity gates. | **[Verification & Quality Gates](./architecture/verification-and-quality.md)** |
| *(Cross-Cutting)* | Remediation History | Formal audit trails and remediation logs across Phases 1, 2, and 3. | **[Remediation Log](./architecture/remediation-log.md)** |

---

## 4. Chase Statement Ingestion Adapter (`internal/infra/sources/chase/`)

The Chase adapter handles CSV exports from Chase Bank for checking, savings, and credit card accounts.

### Key Architectural Invariants:
1. **Declarative 4-Stage Pipeline**: `Extractor.Extract` coordinates execution across `validateExtractParams` -> `classifyStream` -> `parseDialect` -> `buildBatch`.
2. **Pure Static Strategy Router**: `resolveDialectRoute` uses an immutable Go switch statement, eliminating mutable package globals and achieving compile-time safety with 0 allocations.
3. **Pluggable Classifier Interface**: Non-destructive 4KB peeking (`bufio.Reader.Peek(4096)`) evaluates credit card markers (`category`, `memo`, `transaction date`) before checking balance markers.
4. **Stack-Allocated `rowReader` Value Struct**: 40-byte value struct (`row []string` 24B, `colIndex map[string]int` 8B, `lineNum int` 8B) with uniform value receivers, yielding **0 B/op and 0 allocs/op**.
5. **Tabular Missing-Value & Unicode Dash Detection (`isPlaceholderNumeric`)**: Centralized detection of missing tokens (`"na"`, `"n/a"`, `"#n/a"`, `"none"`, `"null"`, `"nil"`, `"--"`, `"---"`, `"-$"`) and Unicode typographic dashes (`\u2013`, `\u2014`, `\u2212`, `\u2015`).
6. **Ergonomic Dialect Row Mappers**: `parseCheckingRow` and `parseCreditCardRow` use `rowReader`, reducing cognitive complexity to 2.
7. **Pending Date Fallbacks (`resolveCreditCardDate`)**: Falls back from unsettled `"Post Date"` to `"Transaction Date"` for credit card rows.
8. **Unwrapped Context Cancellation**: Raw `context.Canceled` and `context.DeadlineExceeded` are returned strictly unwrapped across pipeline stages and header/row parsing (`handleReadError`).

```go
// Primary interface implementation in internal/infra/sources/chase/extractor.go
type Extractor struct {
    classifier Classifier
}

func NewExtractor(c Classifier) port.Extractor
```

For the complete technical blueprint, dialect column mappings, and code listings:  
👉 **[Complete Chase Adapter Architecture Specification](./architecture/chase-adapter.md)**

---

## 5. Fidelity Portfolio Ingestion Adapter (`internal/infra/sources/fidelity/`)

The Fidelity adapter transforms brokerage portfolio holdings exports into strongly-typed `model.FidelityPosition` domain models.

### Key Architectural Invariants:
1. **UTF-8 BOM Detection & Propagation**: `stripUTF8BOM` detects `0xef, 0xbb, 0xbf`, gracefully handling empty files and surfacing non-EOF stream errors.
2. **Multi-Line Disclaimer Filtering**: `isDisclaimerText` filters out legal disclaimers, footnote annotations (`"* "`, `"1. "`), and portfolio summary rows (`"Total"`, `"Account Total"`).
3. **Cash Sweep Normalization**: Normalizes non-standard markers (`"**"`, `"FCASH"`, `"CORE"`) to `"CASH"`, while preserving valid mutual fund tickers with descriptions containing `"Total"` (e.g. `VTI`, `BND`, `TTE`).
4. **Mandatory Schema Signatures**: Requires both a symbol alias (`"symbol"` / `"ticker"`) and a valuation alias (`"current value"`, `"market value"`, etc.) before parsing.
5. **Single-Account Batch Inference**: Automatically infers batch `AccountID` when homogeneous accounts are present.

For the complete technical blueprint and disclaimer filtering rules:  
👉 **[Complete Fidelity Adapter Architecture Specification](./architecture/fidelity-adapter.md)**

---

## 6. Synthetic Deterministic Ingestion Adapter (`internal/infra/sources/synthetic/`)

The synthetic adapter provides an in-memory, deterministic generator implementing `port.Extractor` for offline testing, CI integration, and benchmarks.

### Key Architectural Invariants:
1. **Deterministic PCG PRNG**: Built on Go 1.22's `math/rand/v2` PCG generator, guaranteeing platform-independent reproducible outputs.
2. **Sequential PRNG Continuity & Concurrency Safety**: The PRNG state is encapsulated on the struct and synchronized with a `sync.Mutex`.
3. **Memory Safety Clamping**: Enforces a strict ceiling of `MaxSyntheticRowCount = 100_000` and clamps initial allocation at 10,000 to prevent OOM.

For the complete generator specification:  
👉 **[Complete Synthetic Adapter Architecture Specification](./architecture/synthetic-adapter.md)**

---

## 7. Architectural Decisions & Remediation Log

All architectural decisions, defect remediations, and code review hardening logs are systematically tracked:
- **Phase 2 Consolidated Ingestion Findings**: 10 initial architecture findings (F-01 through F-10).
- **Phase 2B Fidelity Hardening Log**: 10 findings covering BOM errors, cash normalization, and disclaimer filters.
- **Phase 2C Chase Hardening Log**: 9 findings covering in-memory test fixtures, sentinel errors, and non-finite number defense.
- **Phase 1 Readability Cleanup (PR #51)**: 9 findings establishing zero-allocation `rowReader` primitives and Unicode dash detection.
- **Phase 2 Readability Cleanup (PR #52)**: 7 findings establishing ergonomic row parsers and date fallback hardening.
- **Phase 3 Readability Cleanup (Pipeline & Quality Gates)**: 7 findings establishing the 4-stage pipeline, static strategy router, nil stream protection, and unwrapped context errors.

For the complete decision and remediation matrix:  
👉 **[Complete Architectural Remediation Log](./architecture/remediation-log.md)**

---

## 8. Verification Harness & Local Quality Gates

All source adapters are validated against 5 automated local quality gates:

```bash
# 1. Standard library isolation gate (100% standard library purity)
go list -f '{{.ImportPath}} -> {{.Imports}}' ./internal/infra/sources/...

# 2. Race detection and statement coverage gate (chase: 95.0%, fidelity: 91.8%, synthetic: 91.1%)
go test -v -race -cover ./internal/infra/sources/...

# 3. Static analysis and linter gate (0 issues, zero waivers)
golangci-lint run ./internal/infra/sources/...

# 4. CI automated test suite
make test-ci

# 5. Comparative memory allocation benchmark (rowReader vs direct baseline: 16 B/op, 0 from rowReader)
go test -bench=. -benchmem ./internal/infra/sources/chase/...
```

For detailed benchmark analysis and gate specifications:  
👉 **[Complete Verification Harness & Quality Gates Specification](./architecture/verification-and-quality.md)**

---

## 9. Related Documentation & References

- [Architecture Catalog & Document Index](./architecture/README.md)
- [Phase 1 Specification: Domain Core & Port Contracts](./hexagonal-domain-core.md)
- [Fidelity Portfolio Positions Ingestion Specification](./fidelity-portfolio-ingestion.md)
- [MongoDB Atlas Datalake Specification](./mongodb-atlas-datalake.md)
- [Hexagonal Architecture Refactoring Plan (Parent Strategy)](../../../agent-docs/hex-refactor/README.md)
- [Phase 2 Implementation Plan: Ingestion Adapters & Dialects](../../../agent-docs/hex-refactor/PHASE2-INGESTION-ADAPTERS.md)
- [Phase 2C Chase Review Findings & Hardening Synthesis](../../../agent-docs/reviews/hex-phase2c-chase/CHASE-REVIEW-FINDINGS.md)
- [Phase 1 Review Findings & Primitives Cleanup](../../../agent-docs/reviews/hex-phase2c-chase--readability-cleanup-phase1/PHASE1-REVIEW-FINDINGS.md)
- [Phase 2 Review Findings & Row Parser Ergonomics](../../../agent-docs/reviews/hex-phase2c-chase--readability-cleanup-phase2/PHASE2-REVIEW-FINDINGS.md)
- [Phase 3 Review Findings & Strategy Router Pipeline](../../../agent-docs/reviews/hex-phase2c-chase--readability-cleanup-phase3/PHASE3-REVIEW-FINDINGS.md)
- [Pull Request #51: Chase Readability Cleanup Phase 1](https://github.com/ajponte/babylon_data_loader/pull/51)
- [Pull Request #52: Chase Readability Cleanup Phase 2](https://github.com/ajponte/babylon_data_loader/pull/52)
- [Phase 3 Plan: Storage Adapters & MongoDB Router](../../../agent-docs/hex-refactor/PHASE3-STORAGE-ADAPTERS.md)
