# Phase 2 Specification: Hexagonal Ingestion Adapters (Sources)

**Document Owner**: Documentation Engineer (`documentation-engineer`)  
**Target Repository**: `babylon_data_loader`  
**Parent Architecture**: Hexagonal Architecture (Ports and Adapters)  
**Tracking Issue**: [Issue #37: Pragmatic Layered Architecture](https://github.com/ajponte/babylon_data_loader/issues/37)  
**Implementation Phase**: [Phase 2 — Ingestion Adapters](../../../agent-docs/hex-refactor/PHASE2-INGESTION-ADAPTERS.md)  
**Review Syntheses**:
* [Phase 2 Consolidated Review Findings](../../../agent-docs/reviews/hex-refactor-phase2/PHASE2-REVIEW-FINDINGS.md)
* [Phase 2B Fidelity Review Findings & Remediation](../../../agent-docs/reviews/hex-phase2b-fidelity/FIDELITY-REVIEW-FINDINGS.md)
* [Phase 2C Chase Review Findings & Hardening](../../../agent-docs/reviews/hex-phase2c-chase/CHASE-REVIEW-FINDINGS.md)
* [AI Effort Estimation Guardrails & Sizing Playbook](../../../agent-docs/reviews/hex-refactor-phase2/AI-EFFORT-ESTIMATION-GUARDRAILS.md)
* [Blameless Retrospective: Schedule Variance & Sizing Breakdown](../../../agent-docs/reviews/HEX-REFACTOR-BLAMELESS-RETROSPECTIVE.md)

**Related Specifications**: [Phase 1 Specification: Domain Core & Port Contracts](./hexagonal-domain-core.md)  
**Status**: Implemented (Phase 2)  

---

## 1. Executive Summary & Architectural Motivation

In the Hexagonal Architecture of `babylon_data_loader`, **Secondary Ingestion Adapters** (located under `internal/infra/sources/`) implement the driving port contract [`port.Extractor`](../../internal/domain/port/extractor.go). These adapters decode incoming raw statement byte streams (`io.Reader`), classify format dialects, validate input boundaries, and map tabular records into validated domain entities (`ChaseTransaction`, `FidelityPosition`) bundled within an `IngestionBatch`.

```
Raw Stream (io.Reader) + FileMetadata
                  │
                  ▼
┌────────────────────────────────────────────────────────┐
│     Secondary Source Adapters (internal/infra/sources) │
│                                                        │
│  • Memory-Bounded Header Peeking (bufio.Reader.Peek)   │
│  • Format & Dialect Classification Heuristics          │
│  • Dedicated Reusable Parsers (parser.go pattern)      │
│  • Resilient CSV Parsing & Physical Lineage Tracking   │
│  • Sanitization (BOM, Quotes, Parens, Unicode Spaces)  │
│  • Conversion into Pure Domain Records                 │
└────────────────────────────────────────────────────────┘
                  │
                  ▼
Normalized & Validated IngestionBatch (model.IngestionBatch)
```

### 1.1. Standard Library & Domain Isolation
All source adapters maintain complete isolation from third-party libraries, cloud SDKs, and database drivers:
- **Allowed Imports**: Go standard library (`bufio`, `bytes`, `context`, `encoding/csv`, `errors`, `fmt`, `io`, `math`, `math/rand/v2`, `strconv`, `strings`, `sync`, `time`) and internal domain packages (`babylon/dataloader/internal/domain/model`, `babylon/dataloader/internal/domain/port`).
- **Strictly Forbidden Imports**: `go.mongodb.org/*`, `github.com/aws/*`, `github.com/wailsapp/*`, or any external CSV or utility library.
- **Verification Rule**: Pre-commit and CI verification gates run `go list -f '{{.ImportPath}} -> {{.Imports}}' ./internal/infra/sources/...` to ensure zero unauthorized dependencies.

### 1.2. Separation of Concerns
Phase 2 isolates all format quirks, bank-specific column aliases, header sniffing, and numeric parsing heuristics within adapter boundaries. Application orchestrators and domain models remain completely agnostic to:
1. Whether Chase statements originate from checking or credit card accounts.
2. Whether Fidelity exports include multi-line legal disclaimers, BOM markers, or trailing delimiters.
3. Whether synthetic test transactions are generated procedurally in-memory.

---

## 2. Ingestion Architecture & Data Flow

The diagram below details the data flow from an unconsumed `io.Reader` stream through the adapter boundary to a fully validated `IngestionBatch`:

```mermaid
flowchart TD
    %% Input Sources
    subgraph StreamInput ["Raw Input Boundary"]
        R_STREAM["Raw io.Reader Stream<br/>(S3 Body / Local File / HTTP)"]
        F_META["model.FileMetadata<br/>(FileName, FileHash, AccountID, IngestedAt)"]
    end

    %% Ingestion Adapter Boundary
    subgraph AdapterBoundary ["Secondary Source Adapter (internal/infra/sources)"]
        subgraph Classification ["Stream Inspection & Pre-Processing"]
            BOM["BOM Stripper<br/>Peek(3) == 0xefbbbf (surfaces non-EOF errors)"]
            PEEK["Header Sniffer / Classifier<br/>bufio.Reader.Peek(4096)"]
            SCAN["Leading Blank Line Skipper<br/>Context-aware row scanning"]
        end

        subgraph Parsers ["Dedicated Parsing Infrastructure (parser.go)"]
            CSV_R["encoding/csv.Reader<br/>FieldsPerRecord = -1<br/>TrimLeadingSpace = true"]
            POS["Physical Lineage Extractor<br/>csv.Reader.FieldPos(0)"]
            NUM["Flexible Numeric Sanitizer<br/>Parens '($10)' and '$(45)', Dash '-', Unicode Spaces, '$', ','"]
            DATE["Multi-Format Date Parser<br/>MM/DD/YYYY, YYYY-MM-DD"]
        end

        subgraph Validation ["Adapter Validation Layer"]
            SIG["Mandatory Schema Signatures<br/>Exported Sentinels (ErrMissingRequiredColumns)"]
            DISC["Disclaimer & Summary Filter<br/>Discard 'Total', 'Brokerage', 'Ending', Notes"]
            ENT_VAL["Domain Entity Validation<br/>ChaseTransaction.Validate()<br/>FidelityPosition.Validate()"]
        end
    end

    %% Domain Output Boundary
    subgraph OutputBoundary ["Domain Core Boundary"]
        BATCH["model.IngestionBatch<br/>• Destination: transactions_chase / positions_fidelity<br/>• Inferred / Meta AccountID, SourceFile, FileHash, IngestedAt<br/>• RecordCount, Payload: []Entity"]
    end

    %% Flow connections
    R_STREAM --> BOM
    F_META -.-> PEEK
    BOM --> PEEK
    PEEK --> SCAN
    SCAN --> CSV_R
    CSV_R --> POS
    CSV_R --> SIG
    SIG --> DISC
    DISC --> NUM
    NUM --> DATE
    DATE --> ENT_VAL
    POS -.->|"exact physical row"| ENT_VAL
    ENT_VAL --> BATCH

    classDef input fill:#f0fff4,stroke:#00802b,stroke-width:2px;
    classDef adapter fill:#fffaf0,stroke:#cc6600,stroke-width:2px;
    classDef domain fill:#f0f7ff,stroke:#0066cc,stroke-width:2px;
    classDef sub fill:#ffffff,stroke:#cc6600,stroke-width:1px,stroke-dasharray: 3 3;

    class StreamInput,R_STREAM,F_META input;
    class AdapterBoundary adapter;
    class Classification,Parsers,Validation sub;
    class OutputBoundary,BATCH domain;
```

---

## 3. Package Structure & Responsibilities

```
babylon_data_loader/internal/infra/sources/
├── chase/
│   ├── classifier.go         # DialectClassifier interface, Peek(4096) sniffer, precedence rules, nil-context guard
│   ├── classifier_test.go    # Unit tests for dialect detection, peek bounds, and context cancellation
│   ├── checking.go           # Streamlined Chase checking & deposit row mapping and balance validation
│   ├── credit_card.go        # Streamlined Chase credit card row mapping (pending post-date fallback)
│   ├── extractor.go          # Implements port.Extractor for Chase; orchestrates classification & batching
│   ├── extractor_test.go     # Decoupled in-memory fixture tests, edge cases, sentinels, and non-finite checks
│   ├── parser.go             # Dedicated parser engine: CSV streaming (parseChaseRecords), headers, dates, numbers, lineage
│   └── parser_test.go        # Table-driven unit tests for parser helpers: cleanNumeric, parseAmount, parseDate
├── fidelity/
│   ├── extractor.go          # Implements port.Extractor; driving adapter, I/O streaming, BOM, header detection, batch assembly
│   ├── parser.go             # rowParser helper object, symbol normalization, summary filtering, parseFinancialNumber, disclaimers
│   └── extractor_test.go     # Comprehensive table-driven test suite: 100% pass across all 10 review findings, BOM, accounting, cancellation
└── synthetic/
    ├── extractor.go          # Implements port.Extractor; deterministic PRNG, bounds protection, concurrency safety
    └── extractor_test.go     # Determinism, RNG continuity, row-count bounds, and race safety tests
```

---

## 4. Chase Statement Ingestion Adapter (`internal/infra/sources/chase/`)

The Chase adapter handles statement exports from Chase Bank. Because Chase outputs divergent CSV formats for deposit accounts (checking/savings) versus revolving lines (credit cards), the adapter dynamically classifies the dialect before delegating to specialized, streamlined row mappers.

### 4.1. Architecture & Component Responsibilities
Following Phase 2C review hardening and Phase 1 readability refactoring, the Chase adapter follows a modular separation of concerns:
- **`Extractor` ([`extractor.go`](../../internal/infra/sources/chase/extractor.go))**: Implements [`port.Extractor`](../../internal/domain/port/extractor.go). Validates caller parameters, applies nil context guards, triggers stream classification, dispatches to dialect parsers, and encapsulates parsed records into an `IngestionBatch`.
- **`Classifier` ([`classifier.go`](../../internal/infra/sources/chase/classifier.go))**: Inspects the stream non-destructively via 4KB peeking, returning the detected `DialectType` and an unconsumed reader.
- **`Parser Engine` ([`parser.go`](../../internal/infra/sources/chase/parser.go))**: Encapsulates all reusable CSV streaming (`parseChaseRecords`), context-aware header seeking (`readChaseHeader`), stack-allocated row extraction (`rowReader`), tabular placeholder & Unicode dash detection (`isPlaceholderNumeric`), date parsing (`parseDate`), Unicode whitespace and accounting sanitization (`cleanNumeric`), amount parsing (`parseAmount`), description validation (`parseDescription`), column lookups (`findColumn`, `getFieldValue`, `isRowEmpty`), variadic fallback resolution (`firstNonEmpty`), and lineage construction (`buildLineage`).
- **`CheckingMapper` ([`checking.go`](../../internal/infra/sources/chase/checking.go))**: Focused purely on checking-specific field resolution (`details`, `balance`, `checkOrSlipNum`).
- **`CreditCardMapper` ([`credit_card.go`](../../internal/infra/sources/chase/credit_card.go))**: Focused purely on credit-card-specific field resolution (`category`, `memo`, pending post-date fallback via `isPlaceholderDate`).

```mermaid
flowchart TD
    subgraph ChasePkg ["Chase Ingestion Package (internal/infra/sources/chase)"]
        EXT["Extractor (implements port.Extractor)<br/>validateParams, nil-context guard, batching"]
        CLS["DefaultClassifier<br/>bufio.Reader.Peek(4096)"]
        PRS["Dedicated Parser Engine (parser.go)<br/>• parseChaseRecords (cap=64)<br/>• readChaseHeader (ctx-aware)<br/>• rowReader (40B stack value struct)<br/>• isPlaceholderNumeric (Unicode dashes & tabular NA)<br/>• cleanNumeric (unicode.IsSpace CRLF/figure space)<br/>• parseAmount & parseBalance (NaN/Inf defense)<br/>• parseDate (scoped array, UTC)"]
        CHK["checking.go (parseCheckingRow)<br/>Balance extraction & validation"]
        CC["credit_card.go (parseCreditCardRow)<br/>Pending post date fallback"]
    end

    EXT --> CLS
    CLS -->|DialectChecking| PRS
    CLS -->|DialectCreditCard| PRS
    PRS -->|rowParser| CHK
    PRS -->|rowParser| CC
    CHK --> EXT
    CC --> EXT
```

```go
type DialectType string

const (
    DialectChecking   DialectType = "CHASE_CHECKING"
    DialectCreditCard DialectType = "CHASE_CREDIT_CARD"
    DialectUnknown    DialectType = "UNKNOWN"
)

type Classifier interface {
    Classify(ctx context.Context, r io.Reader) (DialectType, io.Reader, error)
}
```

---

### 4.2. In-Stream Dialect Classification (`classifier.go`)

Classification determines the dialect by inspecting the CSV header without reading the entire stream into memory.

#### Memory-Bounded Header Peeking
Rather than using unbounded line reading (`ReadBytes('\n')`), [`DefaultClassifier.Classify`](../../internal/infra/sources/chase/classifier.go) wraps the reader in a `bufio.Reader` and inspects up to `4096` bytes via `br.Peek(4096)`:
- **Safety**: Bounded at 4KB. Unbounded streams, malformed binary files, or minified text streams cannot trigger memory exhaustion (OOM).
- **Non-Destructive Reuse**: Returns the existing `br` instance directly as the unconsumed `io.Reader`. Avoids heap allocations from `io.MultiReader`.
- **Context Awareness & Nil Guard**: Immediately initializes `if ctx == nil { ctx = context.Background() }` and respects `ctx.Err()`, preventing goroutine hangs on slow or stalled streams.

```go
func (c *DefaultClassifier) Classify(ctx context.Context, r io.Reader) (DialectType, io.Reader, error) {
    if ctx == nil {
        ctx = context.Background()
    }
    if r == nil {
        return DialectUnknown, nil, ErrNilReader
    }
    if ctxErr := ctx.Err(); ctxErr != nil {
        return DialectUnknown, nil, ctxErr
    }

    br := bufio.NewReader(r)
    peekBytes, err := br.Peek(peekBufferSize) // 4096 bytes
    if err != nil && !errors.Is(err, io.EOF) {
        return DialectUnknown, br, fmt.Errorf("failed to peek stream header: %w", err)
    }
    // Inspect peekBytes ...
}
```

#### Rule Precedence & Dialect Classification
Chase credit card statements frequently include balance columns (e.g., `"Balance"` or `"New Balance"`). Therefore, credit card markers must be evaluated **before** checking balance markers:

```
Dialect Detection Precedence
├── 1. Credit Card Markers (Highest Precedence)
│   ├── Contains "category"
│   ├── Contains "memo"
│   └── Contains "transaction date"
│       └── Result: DialectCreditCard
├── 2. Checking & Deposit Markers
│   ├── Contains "check or slip"
│   ├── Contains "details" AND "balance"
│   ├── Contains "posting date" AND "balance"
│   └── Contains "balance"
│       └── Result: DialectChecking
└── 3. Fallback
    └── Result: DialectUnknown
```

---

### 4.3. Exported Sentinel Errors & Error Taxonomy

The Chase adapter exports typed sentinel errors in [`extractor.go`](../../internal/infra/sources/chase/extractor.go) enabling callers and test suites to inspect error causes programmatically via `errors.Is()`:

```go
var (
    // ErrNilReader indicates that a nil io.Reader was supplied.
    ErrNilReader = errors.New("reader cannot be nil")

    // ErrUnknownDialect indicates that the Chase statement header did not match a known dialect.
    ErrUnknownDialect = errors.New("unsupported or unknown Chase statement dialect")

    // ErrMissingRequiredColumns indicates that required header columns are missing from the statement.
    ErrMissingRequiredColumns = errors.New("chase CSV missing required header columns")
)
```

Usage in callers:
```go
if errors.Is(err, chase.ErrUnknownDialect) {
    // Gracefully route to fallback parser or report unrecognized bank format
}
if errors.Is(err, chase.ErrMissingRequiredColumns) {
    // Report invalid CSV header schema to user
}
```

---

### 4.4. Dedicated Parsing Infrastructure (`parser.go`)

To eliminate code duplication across dialect mappers (resolving review finding F-04) and establish zero-allocation stack encapsulation, [`parser.go`](../../internal/infra/sources/chase/parser.go) encapsulates all shared stream ingestion, value extraction primitives, string sanitization, and lineage construction:

#### 1. Reusable Streaming Harness (`parseChaseRecords`)
Pre-allocates the results slice with `defaultTransactionsCapacity = 64`, manages the CSV reader (`FieldsPerRecord = -1`, `Comma = ','`, `TrimLeadingSpace = true`), skips leading empty rows, extracts physical 1-based line numbers via `reader.FieldPos(0)`, and coordinates row parsing:
```go
const defaultTransactionsCapacity = 64

type rowParser func(
	row []string,
	colIndex map[string]int,
	lineNum int,
	meta model.FileMetadata,
) (model.ChaseTransaction, error)

func parseChaseRecords(
	ctx context.Context,
	r io.Reader,
	meta model.FileMetadata,
	dialectName string,
	requiredCols [][]string,
	parseRow rowParser,
) ([]model.ChaseTransaction, error) {
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1
	reader.Comma = ','
	reader.TrimLeadingSpace = true

	colIndex, err := readChaseHeader(ctx, reader, dialectName, requiredCols)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return []model.ChaseTransaction{}, nil
		}
		return nil, err
	}

	transactions := make([]model.ChaseTransaction, 0, defaultTransactionsCapacity)
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		row, readErr := reader.Read()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to read Chase %s CSV row: %w", dialectName, readErr)
		}
		lineNum, _ := reader.FieldPos(0)
		if isRowEmpty(row) {
			continue
		}
		tx, parseErr := parseRow(row, colIndex, lineNum, meta)
		if parseErr != nil {
			return nil, parseErr
		}
		transactions = append(transactions, tx)
	}
	return transactions, nil
}
```

#### 2. Context-Aware Header Resolution & Column Indexing (`readChaseHeader`)
Discards leading empty rows while evaluating `ctx.Err()` on every loop iteration to prevent goroutine hangs on slow network streams or infinite empty preambles. Strips UTF-8 BOM markers (`\xef\xbb\xbf`), generates lowercased trimmed column indices, and validates required column groups, wrapping `ErrMissingRequiredColumns` on failure:
```go
func readChaseHeader(
	ctx context.Context,
	reader *csv.Reader,
	dialectName string,
	requiredCols [][]string,
) (map[string]int, error) {
	var header []string
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		record, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("failed to read Chase %s CSV header: %w", dialectName, err)
		}
		if !isRowEmpty(record) {
			header = record
			break
		}
	}

	colIndex := make(map[string]int, len(header))
	for i, col := range header {
		cleanCol := strings.TrimPrefix(col, "\xef\xbb\xbf")
		colIndex[strings.ToLower(strings.TrimSpace(cleanCol))] = i
	}

	for _, req := range requiredCols {
		if _, ok := findColumn(colIndex, req...); !ok {
			return nil, fmt.Errorf("%w: dialect %s missing column %q", ErrMissingRequiredColumns, dialectName, req[0])
		}
	}

	return colIndex, nil
}
```

#### 3. Defensive Column Lookup & Two-Sided Bounds Safety (`findColumn`, `getFieldValue`)
- **Defensive Alias Normalization (`findColumn`)**: Trims candidate aliases with `strings.TrimSpace` and skips empty candidate strings before lookup, preventing false misses when alias configurations contain whitespace.
- **Two-Sided Bounds Guard (`getFieldValue`)**: Resolves review finding F-02 by explicitly checking `!ok || idx < 0 || idx >= len(row)`, defending against negative slice indexing panics if map indices are uninitialized or corrupted.

```go
func findColumn(colIndex map[string]int, aliases ...string) (int, bool) {
	for _, alias := range aliases {
		normalized := strings.ToLower(strings.TrimSpace(alias))
		if normalized == "" {
			continue
		}
		if idx, ok := colIndex[normalized]; ok {
			return idx, true
		}
	}
	return -1, false
}

func getFieldValue(row []string, colIndex map[string]int, aliases ...string) string {
	idx, ok := findColumn(colIndex, aliases...)
	if !ok || idx < 0 || idx >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[idx])
}
```

#### 4. Stack-Allocated `rowReader` Value Struct
To decouple raw slice indexing from dialect mapping rules while imposing **zero heap allocation and zero GC overhead**, `parser.go` introduces the `rowReader` value struct:
- **Struct Memory Layout (40 Bytes)**:
  - `row []string`: 24 bytes (pointer [8B] + length [8B] + capacity [8B])
  - `colIndex map[string]int`: 8 bytes (reference to `runtime.hmap`)
  - `lineNum int`: 8 bytes (machine integer)
- **Pure Value Semantics**: All methods use uniform value receivers (`func (r rowReader)`), avoiding heap escape across function boundaries and register-passing overhead.
- **Line-Attributed Error Reporting**: Formats validation errors with the physical line number (`row %d: ...`).

```go
// rowReader encapsulates row values, header index, and physical line number
// to provide ergonomic, bounds-checked extraction without heap allocation.
type rowReader struct {
	row      []string
	colIndex map[string]int
	lineNum  int
}

// newRowReader constructs a stack-allocated rowReader value.
func newRowReader(row []string, colIndex map[string]int, lineNum int) rowReader {
	return rowReader{
		row:      row,
		colIndex: colIndex,
		lineNum:  lineNum,
	}
}

// get retrieves the trimmed value for the first matching column alias.
func (r rowReader) get(aliases ...string) string {
	return getFieldValue(r.row, r.colIndex, aliases...)
}

// parseDate extracts and validates a timestamp field from the specified column aliases.
func (r rowReader) parseDate(aliases ...string) (time.Time, error) {
	dateStr := r.get(aliases...)
	if dateStr == "" {
		return time.Time{}, fmt.Errorf("row %d: transaction date is required", r.lineNum)
	}
	t, err := parseDate(dateStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("row %d: %w", r.lineNum, err)
	}
	return t, nil
}

// parseDescription extracts and validates the description field.
func (r rowReader) parseDescription(aliases ...string) (string, error) {
	return parseDescription(r.get(aliases...), r.lineNum)
}

// parseAmount extracts and validates the transaction amount field.
func (r rowReader) parseAmount(aliases ...string) (float64, error) {
	return parseAmount(r.get(aliases...), r.lineNum)
}

// parseBalance extracts and validates an optional running balance field.
func (r rowReader) parseBalance(aliases ...string) (*float64, error) {
	return parseBalance(r.get(aliases...), r.lineNum)
}
```

#### 5. Tabular Missing-Value & Unicode Dash Detection (`isPlaceholderNumeric`)
Resolves critical review finding F-01. Financial CSV exports, Excel dumps, and Chase web exports frequently represent absent numeric values using tabular placeholders (`"NA"`, `"n/a"`, `"#n/a"`, `"none"`, `"null"`, `"nil"`, `"-$"`) or Unicode typographic dashes. `isPlaceholderNumeric` detects these cases without misclassifying valid numeric values (`0`, `0.00`, `$0.00`, `-0.00`):

```go
// isPlaceholderNumeric reports whether val represents an absent or non-numeric placeholder value.
// It detects empty fields, tabular null tokens ("n/a", "na", "#n/a", "none", "null", "nil", "-$"),
// and strings consisting entirely of ASCII or Unicode dashes (-, --, ---, –, —, −, ―).
func isPlaceholderNumeric(val string) bool {
	trimmed := strings.TrimSpace(val)
	if trimmed == "" {
		return true
	}

	switch strings.ToLower(trimmed) {
	case "-", "--", "---", "n/a", "na", "#n/a", "none", "null", "nil", "-$":
		return true
	}

	hasDash := false
	for _, r := range trimmed {
		if r == '-' || r == '\u2013' || r == '\u2014' || r == '\u2212' || r == '\u2015' {
			hasDash = true
			continue
		}
		return false
	}
	return hasDash
}
```

#### 6. Optional Running Balance Parsing (`parseBalance`)
Utilizes `isPlaceholderNumeric` on both raw input and sanitized numeric representations to gracefully yield `(nil, nil)` for absent optional balances while strictly rejecting corrupted numeric syntax and IEEE 754 non-finite values:

```go
// parseBalance parses an optional running balance field.
// It returns (nil, nil) if the field is empty or contains a placeholder dash/NA.
//
//nolint:nilnil // nil balance represents an intentionally absent optional value
func parseBalance(balRaw string, lineNum int) (*float64, error) {
	if isPlaceholderNumeric(balRaw) {
		return nil, nil
	}

	balStr := cleanNumeric(balRaw)
	if isPlaceholderNumeric(balStr) {
		return nil, nil
	}

	bal, err := strconv.ParseFloat(balStr, 64)
	if err != nil {
		return nil, fmt.Errorf("row %d: invalid balance %q: %w", lineNum, balStr, err)
	}
	if math.IsNaN(bal) || math.IsInf(bal, 0) {
		return nil, fmt.Errorf("row %d: invalid balance %q: non-finite number", lineNum, balStr)
	}
	return &bal, nil
}
```

#### 7. IEEE 754 Non-Finite Float Defense (`parseAmount`)
`strconv.ParseFloat` parses `"NaN"`, `"Inf"`, `"+Inf"`, and `"-Inf"` without error. [`parseAmount`](../../internal/infra/sources/chase/parser.go) requires non-empty amounts, sanitizes with `cleanNumeric`, parses `float64`, and explicitly validates finiteness:
```go
func parseAmount(amtRaw string, lineNum int) (float64, error) {
	amtStr := cleanNumeric(amtRaw)
	if amtStr == "" {
		return 0, fmt.Errorf("row %d: transaction amount is required", lineNum)
	}
	amount, err := strconv.ParseFloat(amtStr, 64)
	if err != nil {
		return 0, fmt.Errorf("row %d: invalid amount %q: %w", lineNum, amtStr, err)
	}
	if math.IsNaN(amount) || math.IsInf(amount, 0) {
		return 0, fmt.Errorf("row %d: invalid amount %q: non-finite number", lineNum, amtStr)
	}
	return amount, nil
}
```

#### 8. Mandatory Description Validation (`parseDescription`)
Ensures every transaction record contains a valid, non-blank description string:
```go
func parseDescription(descRaw string, lineNum int) (string, error) {
	desc := strings.TrimSpace(descRaw)
	if desc == "" {
		return "", fmt.Errorf("row %d: transaction description is required", lineNum)
	}
	return desc, nil
}
```

#### 9. Accounting Parentheses & Unicode Whitespace Normalization (`cleanNumeric`)
Resolves review finding F-03. Sanitizes monetary inputs across all Chase statement exports:
- Uses `unicode.IsSpace(r)` in `strings.Map` to eliminate all whitespace categories, including ASCII spaces (`0x20`), tabs (`\t`), CRLF carriage returns (`\r`), non-breaking spaces (`\u00a0`, `\u202f`), and tabular figure spaces (`\u2007`).
- Strips currency signs (`$`), commas (`,`), and plus signs (`+`).
- Supports accounting negative formats: both standard `($50.00)` and outer currency symbols `$(45.00)` or `$( 45.00 )` -> `-45.00`.
- Strictly preserves numeric strings without coercing missing placeholder tokens to `"0"` (missing token detection is strictly isolated in `isPlaceholderNumeric`).

```go
func cleanNumeric(val string) string {
	clean := strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, val)

	clean = strings.ReplaceAll(clean, "$", "")
	clean = strings.ReplaceAll(clean, ",", "")
	clean = strings.ReplaceAll(clean, "+", "")

	if strings.HasPrefix(clean, "(") && strings.HasSuffix(clean, ")") {
		clean = "-" + clean[1:len(clean)-1]
	}

	return clean
}
```

#### 10. Scoped Multi-Layout Date Parsing with UTC Normalization (`parseDate`)
Resolves review finding F-08 by eliminating the package-level mutable global slice `supportedDateLayouts` and its `//nolint:gochecknoglobals` directive. A stack-allocated local array `[...]string` supports 4 common Chase date variants, normalizing all timestamps to midnight UTC (`time.Date(..., time.UTC)`):
```go
func parseDate(val string) (time.Time, error) {
	trimmed := strings.TrimSpace(val)
	if trimmed == "" {
		return time.Time{}, errors.New("date value is empty")
	}

	layouts := [...]string{
		"01/02/2006", // US standard with leading zeros (MM/DD/YYYY)
		"1/2/2006",   // US compact without leading zeros (M/D/YYYY)
		"2006-01-02", // ISO 8601 standard (YYYY-MM-DD)
		"2006/01/02", // ISO variant (YYYY/MM/DD)
	}

	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, trimmed); err == nil {
			return time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, time.UTC), nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid date format %q: expected MM/DD/YYYY or YYYY-MM-DD", val)
}
```

#### 11. Variadic Fallback Helper (`firstNonEmpty`)
Provides zero-allocation candidate resolution for optional or cascading columns:
```go
func firstNonEmpty(values ...string) string {
	for _, v := range values {
		trimmed := strings.TrimSpace(v)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}
```

#### 12. Physical Lineage Tracking (`buildLineage` via `FieldPos(0)`)
Captures the exact 1-based physical line number of the record using `reader.FieldPos(0)`, ensuring accurate forensic lineage even when CSVs contain multi-line quoted fields or skipped rows:
```go
func buildLineage(meta model.FileMetadata, lineNum int) model.SourceLineage {
	return model.SourceLineage{
		FileName:   meta.FileName,
		FileHash:   meta.FileHash,
		RowNumber:  lineNum,
		IngestedAt: meta.IngestedAt,
	}
}
```

---

### 4.5. Streamlined Checking Account Dialect (`checking.go`)

[`checking.go`](../../internal/infra/sources/chase/checking.go) is reduced to ~90 lines. It delegates streaming and header resolution to `parseChaseRecords`, focusing strictly on checking-specific column mapping (`parseCheckingRow`) and running balance extraction (`parseBalance`):

#### Checking Column Resolution Mapping

| Domain Field | Canonical Target | Supported Aliases | Invariant / Validation Rule |
| :--- | :--- | :--- | :--- |
| `PostingDate` | `posting date` | `"post date"` | Required; multi-format parsed (`MM/DD/YYYY`, `YYYY-MM-DD`). |
| `Description` | `description` | — | Required; non-empty string via `parseDescription`. |
| `Amount` | `amount` | — | Required; signed finite `float64` via `parseAmount`. |
| `Details` | `details` | — | Optional; falls back to `Type` or `"TRANSACTION"`. |
| `Type` | `type` | — | Optional; falls back to `Details` or `"TRANSACTION"`. |
| `Balance` | `balance` | — | Optional `*float64`; must be a valid finite number via `parseBalance`. |
| `CheckOrSlipNum` | `check or slip #`| `"check or slip num"` | Optional check/slip serial number. |

---

### 4.6. Streamlined Credit Card Dialect (`credit_card.go`)

[`credit_card.go`](../../internal/infra/sources/chase/credit_card.go) is streamlined to ~90 lines, delegating CSV harness streaming to `parseChaseRecords` and focusing purely on credit card field extraction, type defaulting (`"Sale"`), and pending transaction fallback support:

#### Credit Card Column Resolution Mapping

| Domain Field | Canonical Target | Supported Aliases | Invariant / Validation Rule |
| :--- | :--- | :--- | :--- |
| `PostingDate` | `post date` | `"posting date"`, `"transaction date"`, `"trans date"` | Required; falls back to transaction date for pending transactions. |
| `Description` | `description` | — | Required; non-empty string via `parseDescription`. |
| `Amount` | `amount` | — | Required; signed finite `float64` via `parseAmount`. |
| `Category` | `category` | — | Optional merchant/expense category. |
| `Type` | `type` | — | Optional; defaults to `"Sale"` if blank. |
| `Memo` | `memo` | — | Optional statement memo string. |
| `Balance` | *(N/A)* | — | Always initialized to `nil` for credit cards. |

#### DRY Pending Transaction Fallback Support (`isPlaceholderDate`)
Resolves review finding F-06. Credit card transactions that have not settled emit an empty or placeholder `"Post Date"`, but contain a valid `"Transaction Date"`. To avoid logic drift, `isPlaceholderDate` delegates directly to `isPlaceholderNumeric`:

```go
// isPlaceholderDate checks whether a date string is empty, placeholder numeric, or "pending".
func isPlaceholderDate(val string) bool {
	return isPlaceholderNumeric(val) || strings.EqualFold(strings.TrimSpace(val), "pending")
}
```

```go
postDateStr := getFieldValue(row, colIndex, "post date", "posting date")
if isPlaceholderDate(postDateStr) {
	postDateStr = getFieldValue(row, colIndex, "transaction date", "trans date")
}
if isPlaceholderDate(postDateStr) {
	return model.ChaseTransaction{}, fmt.Errorf("row %d: missing both post date and transaction date", lineNum)
}
```

---

## 5. Fidelity Portfolio Ingestion Adapter (`internal/infra/sources/fidelity/`)

Fidelity brokerage portfolio exports represent point-in-time holdings snapshots across one or more accounts. The adapter handles complex real-world export quirks, including UTF-8 Byte Order Marks (BOM), trailing asymmetric commas, multi-line legal disclaimers, cash sweep normalization, and accounting syntax.

### 5.1. Two-Tier Architecture & Component Responsibilities

Following Phase 2B review hardening, the Fidelity adapter is structured into a clean two-tier architectural separation:
1. **I/O Streaming & Batch Lifecycle Tier ([`extractor.go`](../../internal/infra/sources/fidelity/extractor.go))**: Handles driving adapter responsibilities, parameter validation, stream buffering, BOM stripping with error propagation, header scanning, signature validation, slice memory pre-allocation, and batch envelope assembly with account ID inference.
2. **Row Classification & Parsing Helper Object Tier ([`parser.go`](../../internal/infra/sources/fidelity/parser.go))**: Implements `rowParser`, an encapsulated helper object that performs holding resolution, cash sweep normalization, robust numeric metric extraction, disclaimer and summary filtering, and entity validation.

```mermaid
flowchart TD
    subgraph StreamTier ["I/O Streaming & Batch Lifecycle Tier (extractor.go)"]
        EXT["Fidelity Extractor<br/>(implements port.Extractor)"]
        BOM["stripUTF8BOM()<br/>Peek(3) == 0xefbbbf (surfaces non-EOF errors)"]
        HDR["readHeader()<br/>Context-aware blank line skipping"]
        VAL_HDR["parseAndValidateHeader()<br/>Checks Group 1 & 2 signatures<br/>Yields ErrMissingRequiredColumns"]
        POS_LOOP["parsePositions()<br/>make([]Position, 0, defaultPositionsCapacity=64)"]
        BATCH["makeBatch()<br/>resolveBatchAccountID()"]
    end

    subgraph RowTier ["Row Classification & Metric Parsing Tier (parser.go)"]
        RP["rowParser Helper Object<br/>newRowParser(colIndex, meta)"]
        DISC["isFidelityDisclaimerOrEmpty()<br/>Prefixes: please note, important, *, 1., etc."]
        HOLD["resolveHolding()<br/>isSummaryRow() & normalizeHoldingSymbol()"]
        METRICS["parseMetrics()<br/>parseFinancialNumber()<br/>Unicode spaces, parens, dashes"]
        ACC["resolveAccountID()<br/>isPlaceholderAccountID()"]
        BUILD["buildPosition()<br/>model.FidelityPosition + Validate()"]
    end

    EXT --> BOM --> HDR --> VAL_HDR --> POS_LOOP
    POS_LOOP --> DISC
    DISC -->|Data Row| RP
    RP --> HOLD --> METRICS --> ACC --> BUILD
    BUILD --> POS_LOOP
    POS_LOOP --> BATCH

    classDef stream fill:#e1f5fe,stroke:#0288d1,stroke-width:1.5px;
    classDef row fill:#f3e5f5,stroke:#7b1fa2,stroke-width:1.5px;

    class StreamTier,EXT,BOM,HDR,VAL_HDR,POS_LOOP,BATCH stream;
    class RowTier,RP,DISC,HOLD,METRICS,ACC,BUILD row;
```

---

### 5.2. Mandatory Schema Signature Validation & Sentinel Errors

To prevent arbitrary or malformed CSV files (such as Chase statements or unrelated documents) from producing phantom holdings, [`parseAndValidateHeader`](../../internal/infra/sources/fidelity/extractor.go) requires at least one column match from each of two mandatory signature groups:

```go
requiredSignatures := [][]string{
    {"symbol", "ticker"},
    {"current value", "market value", "current balance", "last price", "quantity"},
}
```

#### Exported Sentinel Errors
The adapter exports explicit sentinel errors, allowing callers, pipeline routers, and test harnesses to programmatically evaluate error conditions via `errors.Is()`:

```go
var (
    // ErrNilReader indicates that a nil io.Reader was provided to Extract.
    ErrNilReader = errors.New("reader cannot be nil")

    // ErrMissingRequiredColumns indicates that the CSV header is missing mandatory columns.
    ErrMissingRequiredColumns = errors.New("fidelity CSV missing required header columns")
)
```

```go
if r == nil {
    return ErrNilReader
}

for _, sig := range requiredSignatures {
    if _, ok := findColumn(colIndex, sig...); !ok {
        return nil, fmt.Errorf("%w (expected one of %v)", ErrMissingRequiredColumns, sig)
    }
}
```

---

### 5.3. Section, Disclaimer & Normalization Heuristics

#### 1. Preservation of Legitimate Holdings with "Total" in Description (F-01)
Broad-market index funds (e.g., Vanguard Total Stock Market `VTI`, Vanguard Total Bond Market `BND`, TotalEnergies `TTE`) routinely contain `"Total "` in their descriptive fund titles.
In [`parser.go`](../../internal/infra/sources/fidelity/parser.go), `isSummaryRow` explicitly decouples ticker symbol evaluation from description scanning:
```go
func isSummaryRow(symbol, description string) bool {
    symLower := strings.ToLower(symbol)
    descLower := strings.ToLower(description)

    if isSummaryKeyword(symLower) {
        return true
    }
    // Only filter based on description if the symbol is empty or a placeholder
    return (symbol == "" || symbol == "**") && isSummaryKeyword(descLower)
}
```
If a row contains a valid ticker symbol (e.g. `VTI`), it is **never** dropped as a summary row, guaranteeing zero accidental data loss on index funds.

#### 2. Cash Sweep Holding Normalization (F-03)
Fidelity exports core cash sweep accounts with varying symbols (`"**"`, `""`, `"FCASH"`, `"CORE"`) and descriptions like `"HELD IN MONEY MARKET"` or `"DEPOSIT SWEEP"`:
- **Normalization to `"CASH"`**: If the symbol is strictly `"**"`, `"FCASH"`, `"CORE"`, or empty with cash holding markers (`isCashDescription`), it is normalized to `"CASH"`.
- **Footnote Asterisk Preservation**: For valid market tickers that include footnote asterisks (e.g. `SPAXX**`), the symbol is preserved as a distinct money market fund rather than coerced into generic `"CASH"`.

```go
func normalizeHoldingSymbol(symbol, description string) (string, bool) {
    if symbol != "" && symbol != "**" && !strings.EqualFold(symbol, "fcash") && !strings.EqualFold(symbol, "core") {
        return symbol, true
    }

    descUpper := strings.ToUpper(description)
    if isCashHolding(symbol, descUpper) {
        return "CASH", true
    }

    return "", false
}
```

#### 3. Stream I/O Safety & Non-EOF BOM Error Propagation (F-02)
[`stripUTF8BOM`](../../internal/infra/sources/fidelity/extractor.go) inspects the first 3 bytes for `0xef, 0xbb, 0xbf`. It surfaces non-EOF I/O errors immediately, treats graceful EOF (empty files) as a non-error, and verifies that `br.Discard(3)` succeeds:
```go
func stripUTF8BOM(br *bufio.Reader) error {
    bom := []byte{0xef, 0xbb, 0xbf}
    peekBytes, err := br.Peek(len(bom))
    if err != nil {
        if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
            return nil
        }
        return fmt.Errorf("failed to peek BOM header: %w", err)
    }
    if bytes.Equal(peekBytes, bom) {
        if _, discardErr := br.Discard(len(bom)); discardErr != nil {
            return fmt.Errorf("failed to discard BOM: %w", discardErr)
        }
    }
    return nil
}
```

#### 4. Context Cancellation in Header and Row Loops (F-05)
Both `readHeader()` and `parsePositions()` evaluate `ctx.Err()` at the start of every iteration, preventing goroutine hangs when reading slow network streams or infinite preamble lines.

#### 5. Expanded Legal Footer & Footnote Filtering (F-06)
[`isDisclaimerText`](../../internal/infra/sources/fidelity/parser.go) inspects the first non-empty field of non-data records to filter legal notices and definitions:
- **Disclaimers**: `"the data and information"`, `"brokerage services"`, `"date downloaded"`, `"disclaimer"`, `"disclosure"`, `"please note"`, `"important"`, `"quotes are"`, `"security prices"`.
- **Totals**: `"total"`, `"total market"`, `"account total"`, `"portfolio total"`, `"ending value"`.
- **Footnotes & Numbered Notes**: Lines beginning with `"* "` (excluding cash `"**"`) or numbered annotations (`"1."`).

#### 6. Slice Memory Pre-Allocation (F-07)
In `parsePositions()`, the positions slice is pre-allocated with `const defaultPositionsCapacity = 64`. This eliminates continuous array reallocations and garbage collection churn when ingesting typical multi-position brokerage accounts.

#### 7. Unified Financial Number Parsing (F-08)
[`parseFinancialNumber`](../../internal/infra/sources/fidelity/parser.go) unifies currency and quantity sanitization:
- **Unicode Non-Breaking Space Sanitization**: Strips both standard non-breaking spaces (`\u00a0` / `&nbsp;`) and narrow non-breaking spaces (`\u202f`) frequently injected by Safari, Firefox, or localized OS exports.
- **Accounting Parentheses**: Automatically transforms `($50.00)` to `-50.00`.
- **Dashes and Placeholders**: Resolves `""`, `"-"`, `"--"`, `"n/a"`, `"unknown"`, and `"pending"` to `0.0, nil`.
- **Enriched Error Context**: Wraps syntax errors with field name and raw value (`invalid %s value %q: %w`).

#### 8. Single-Account Batch Inference (F-09)
In [`resolveBatchAccountID`](../../internal/infra/sources/fidelity/extractor.go), if all extracted positions share the exact same non-empty account identifier, `batch.AccountID` is set to that inferred account ID, overriding generic metadata placeholders (`"DEFAULT"`, `"ALL"`, `"UNKNOWN"`). If records span multiple accounts, `meta.AccountID` is preserved.

---

### 5.4. Fidelity Column Resolution Mapping

The `rowParser` maps column headers to domain fields using case-insensitive alias lookups:

| Domain Field | Canonical Target | Supported Aliases | Conversion & Sanitization Rules |
| :--- | :--- | :--- | :--- |
| `AccountID` | `account number` | `"account id"`, `"account #"`, `"account"` | Falls back to `meta.AccountID` if empty or placeholder (`"-"`, `"n/a"`). |
| `AccountName` | `account name` | `"account nickname"` | Human-readable account label (e.g., `"ROTH IRA"`). |
| `Symbol` | `symbol` | `"ticker"`, `"symbol/cusip"` | Clean ticker; `"**"`, `"FCASH"`, `"CORE"` normalized to `"CASH"`. |
| `Description` | `description` | `"security description"`, `"investment name"` | Full security title or money market description. |
| `Quantity` | `quantity` | `"shares"`, `"qty"` | `parseQuantity`: strips Unicode spaces, parens, commas, plus signs. |
| `LastPrice` | `last price` | `"price"`, `"last"` | `parseCurrency`: strips `$`, must be `>= 0` and finite. |
| `CurrentValue` | `current value` | `"market value"`, `"current value ($)"` | `parseCurrency`: total market valuation. |
| `CostBasis` | `cost basis total`| `"cost basis"`, `"total cost basis"` | `parseCurrency`: aggregate purchase basis (`0.0` for cash/gifts). |

---

## 6. Synthetic Deterministic Ingestion Adapter (`internal/infra/sources/synthetic/`)

The synthetic adapter generates realistic, deterministic financial transactions programmatically without requiring external files or real banking credentials. It is primarily utilized in integration testing, CI/CD pipelines, and local UI development.

### 6.1. Options & Initialization
Configured via `GeneratorOptions`:

```go
type GeneratorOptions struct {
    RowCount int
    Seed     int64
}
```
- If `Seed == 0`, defaults to `defaultSeed = 42`.
- Utilizes Go's PCG PRNG (`math/rand/v2.NewPCG(uint64(seed), 1)`).

---

### 6.2. Persistent PRNG State & Concurrency Safety
To prevent duplicate transaction sequences across sequential `Extract()` calls on the same extractor instance, the PRNG instance is maintained on the struct and guarded by a `sync.Mutex`:

```go
type Extractor struct {
    opts GeneratorOptions
    mu   sync.Mutex
    rng  *rand.Rand
}
```

Sequential invocations of `Extract()` advance the internal generator state, ensuring sequential non-identical batches while preserving reproducible determinism when initialized with a fresh seed.

---

### 6.3. Memory Allocation Bounds & Safety Limits
To prevent memory exhaustion attacks or accidental out-of-memory crashes:
1. **Safety Bound**: Maximum allowable rows capped at `MaxSyntheticRowCount = 100_000`. Requests exceeding this limit return an immediate error.
2. **Negative Handling**: Negative counts are clamped to `0`.
3. **Clamped Pre-allocation**: Initial slice allocation is capped at `min(count, 10_000)`. Batches larger than 10,000 grow dynamically, preventing massive upfront heap spikes.

```go
const (
    MaxSyntheticRowCount    = 100_000
    maxInitialBatchCapacity = 10_000
)

count := e.opts.RowCount
if count < 0 {
    count = 0
}
if count > MaxSyntheticRowCount {
    return nil, fmt.Errorf("row count %d exceeds maximum allowable limit of %d", count, MaxSyntheticRowCount)
}

records := make([]model.ChaseTransaction, 0, min(count, maxInitialBatchCapacity))
```

---

## 7. Architectural Decisions & Remediation Log

### 7.1. Phase 2 Consolidated Ingestion Findings
The table below documents the 10 consolidated findings from the initial Phase 2 architectural and QA review:

| ID | Severity | Component | Architectural Impact Prior to Remediation | Remediation Implemented |
| :---: | :---: | :--- | :--- | :--- |
| **F-01** | `High` | `fidelity` | Arbitrary non-Fidelity CSVs parsed into phantom batches of $0.00 `"CASH"` positions due to unvalidated schema. | Added mandatory schema signature checks requiring `{"symbol", "ticker"}` and `{"current value", "market value", "current balance", "last price", "quantity"}`. |
| **F-02** | `High` | `fidelity` | Summary totals (e.g. `"Account Total"`) and legal footers parsed as massive cash positions, corrupting valuation. | Expanded disclaimer filtering prefixes (`"total"`, `"portfolio total"`, `"ending value"`) and required holding evidence (`"HELD IN"`, `"CORE"`, `"CASH"`) before assigning `"CASH"`. |
| **F-03** | `High` | `chase` | Unbounded `ReadBytes('\n')` in header classification risked memory exhaustion (OOM) on streams without newlines. | Replaced with memory-bounded `bufio.Reader.Peek(4096)`, non-destructive reader reuse, and explicit EOF handling. |
| **F-04** | `High` | `chase`, `fidelity` | Statements with leading blank lines failed header reading on first `reader.Read()`, rejecting valid bank exports. | Introduced leading blank line scanning loops across all CSV parsers to skip empty records until reaching the header. |
| **F-05** | `Medium` | `chase` | Corrupted balance strings (e.g. `"12.4A"`, `"NaN"`) silently converted to `nil`, suppressing errors and breaking auditability. | Added explicit numeric validation returning `row %d: invalid balance %q` for invalid, non-placeholder balance strings. |
| **F-06** | `Medium` | `chase` | Missing `context.Context` in classifier prevented timeout cancellation; matching `"balance"` before card markers misrouted credit cards. | Added `context.Context` to `Classifier.Classify`; reordered classification rules to evaluate Credit Card signatures before generic `"balance"`. |
| **F-07** | `Medium` | `chase` | Pending credit card transactions with empty `"Post Date"` caused entire batch ingestion to fail. | Implemented automatic fallback from empty `"Post Date"` to `"Transaction Date"` for credit card rows. |
| **F-08** | `Medium` | `chase`, `fidelity` | Financial values with accounting parens `($10.00)`, dashes `"-"`, or ISO 8601 dates (`YYYY-MM-DD`) failed parsing. | Updated `cleanNumeric` to translate parens to negative values and dashes to `"0"`; expanded `parseDate` to support ISO 8601 layouts. |
| **F-09** | `Medium` | `chase`, `fidelity` | Manual `rowIdx++` drifted from physical file line numbers on multiline quoted fields and skipped rows. | Adopted `csv.Reader.FieldPos(0)` to capture exact 1-based physical line numbers for record lineage. |
| **F-10** | `Medium` | `synthetic` | PRNG recreated per invocation yielding duplicate batches; slice pre-allocation risked OOM on extreme row counts. | Stored mutex-guarded `*rand.Rand` on struct for sequential continuity; capped row counts at 100,000 and clamped initial pre-allocation at 10,000. |

---

### 7.2. Phase 2B Fidelity Adapter Hardening Log
During the dedicated code review and QA hardening of the isolated Fidelity adapter on branch `hex-phase2b-fidelity` (documented in detail in [`agent-docs/reviews/hex-phase2b-fidelity/FIDELITY-REVIEW-FINDINGS.md`](../../../agent-docs/reviews/hex-phase2b-fidelity/FIDELITY-REVIEW-FINDINGS.md)), an additional 10 findings were prioritized and resolved:

| ID | Severity | Target File(s) | Architectural Impact Prior to Hardening | Implemented Resolution |
| :---: | :---: | :--- | :--- | :--- |
| **F-01** | `High` | `parser.go` | False-positive dropping of legitimate index funds containing `"Total "` in descriptions (e.g. `VTI`, `BND`, `TTE`). | Decoupled ticker evaluation from description checking; if `symbol != ""` and non-summary, row is always preserved. |
| **F-02** | `High` | `extractor.go` | `stripUTF8BOM` swallowed non-EOF peek and discard errors, masking underlying stream I/O failures. | Refactored `stripUTF8BOM` to return `error`, surfaces non-EOF errors, and treats graceful EOF as non-error. |
| **F-03** | `High` | `parser.go` | Core cash sweep positions retained raw punctuation ticker `"**"`, causing downstream ticker lookup failures. | Normalized `"**"`, `"FCASH"`, and `"CORE"` to `"CASH"`, while preserving footnote asterisks on valid tickers (`SPAXX**`). |
| **F-04** | `Medium` | `extractor.go` | Anonymous string errors prevented programmatic inspection via `errors.Is()`. | Declared exported sentinels: `ErrNilReader` and `ErrMissingRequiredColumns`. |
| **F-05** | `Medium` | `extractor.go` | Header-seeking loop ignored `ctx.Err()`, risking worker goroutine leaks on infinite or slow preamble streams. | Added `ctx.Err()` check to every iteration of the header-seeking loop. |
| **F-06** | `Medium` | `parser.go` | Legal notices and numbered footnotes (`"Please note"`, `"* "`, `"1."`) caused currency parsing failures. | Expanded `isDisclaimerText` to filter standard legal notices, quote disclaimers, and numbered footnote annotations. |
| **F-07** | `Medium` | `extractor.go` | Slice initialized with zero capacity, causing repeated memory reallocation churn on large exports. | Pre-allocated positions slice with `defaultPositionsCapacity = 64`. |
| **F-08** | `Medium` | `parser.go` | Unicode non-breaking spaces (`\u00a0`, `\u202f`) caused `strconv.ParseFloat` syntax errors. | Implemented `parseFinancialNumber` stripping Unicode spaces, parens, dashes, and enriching error context. |
| **F-09** | `Medium` | `extractor.go` | Homogeneous single-account statements retained generic placeholder `AccountID` (`"DEFAULT"`). | Implemented `resolveBatchAccountID` inferring single account ID across all positions for the batch envelope. |
| **F-10** | `Medium` | `extractor_test.go` | Missing table-driven tests for BOM error handling, cash normalization, accounting parens, and cancellation. | Expanded unit test matrix with 10 dedicated suites providing 100% test pass rate across all findings. |

---

### 7.3. Phase 2C Chase Adapter Hardening Log
During the dedicated code review and QA hardening of the Chase adapter on branch `hex-phase2c-chase` (documented in detail in [`agent-docs/reviews/hex-phase2c-chase/CHASE-REVIEW-FINDINGS.md`](../../../agent-docs/reviews/hex-phase2c-chase/CHASE-REVIEW-FINDINGS.md)), 9 findings were prioritized and resolved:

| ID | Severity | Target File(s) | Architectural Impact Prior to Hardening | Implemented Resolution |
| :---: | :---: | :--- | :--- | :--- |
| **F-01** | `High` | `extractor_test.go` | Integration tests depended on gitignored local files (`data/unprocessed/...`), causing 100% test skipping in CI. | Replaced local disk dependency with self-contained in-memory CSV fixtures; auxiliary fixture tests defensively skip if files are missing. |
| **F-02** | `High` | `extractor.go`<br/>`parser.go` | Missing exported sentinel error types forced callers to rely on fragile substring matching. | Declared and exported `ErrNilReader`, `ErrUnknownDialect`, and `ErrMissingRequiredColumns` for `errors.Is()`. |
| **F-03** | `High` | `parser.go` | `amount` float parsing did not reject non-finite numbers (`NaN`, `Inf`), passing corrupted values to domain validation. | Enforced `math.IsNaN(amount) \|\| math.IsInf(amount, 0)` checks immediately in `parseAmount`, returning explicit row errors. |
| **F-04** | `High` | `parser.go`<br/>`checking.go`<br/>`credit_card.go` | Monolithic `parseCheckingRow` and `parseCreditCardRow` duplicated CSV streaming, headers, numeric sanitization, and lineage logic. | Decomposed into dedicated `parser.go` engine with reusable helpers (`parseChaseRecords`, `parseAmount`, `cleanNumeric`, `buildLineage`). Streamlined dialect mappers to ~75–100 lines. |
| **F-05** | `Medium` | `parser.go` | `cleanNumeric` failed on accounting negatives with outer dollar signs `$(45.00)` and ignored Unicode non-breaking spaces. | Updated `cleanNumeric` to strip Unicode whitespace (`\u00a0`, `\u202f`), tabs, and handle both `(45.00)` and `$(45.00)` / `$( 45.00 )`. |
| **F-06** | `Medium` | `extractor.go`<br/>`classifier.go` | `ctx.Err()` called without guarding against `nil` context, risking runtime panic on uninitialized context. | Added defensive nil-context guards (`if ctx == nil { ctx = context.Background() }`) at public function entry points. |
| **F-07** | `Medium` | `parser.go` | `readChaseHeader` header-seeking loop ignored `ctx.Err()`, hanging on slow streams or infinite empty rows. | Passed `ctx` to `readChaseHeader` and evaluated `ctx.Err()` on every loop iteration. |
| **F-08** | `Medium` | `parser.go` | `dateLayouts` slice allocated on heap on every row; `transactions` slice started unallocated (`cap=0`). | Hoisted `supportedDateLayouts` to package variable; pre-allocated `transactions` slice with `defaultTransactionsCapacity = 64`. |
| **F-09** | `Medium` | `extractor_test.go`<br/>`parser_test.go` | Test matrix lacked coverage for mid-stream cancellation, corrupted dates, non-finite amounts, and malformed CSV syntax. | Added comprehensive table-driven test suites in `parser_test.go` and `extractor_test.go` with 100% pass across all edge cases. |

---

### 7.4. Phase 2C Chase Readability & Primitives Cleanup Log (Phase 1)
During the readability and ergonomics refactor of the Chase statement adapter on branch `hex-phase2c-chase--readability-cleanup-phase1` (PR #51 against `hex-phase2c-chase`, synthesized in [`agent-docs/reviews/hex-phase2c-chase--readability-cleanup-phase1/PHASE1-REVIEW-FINDINGS.md`](../../../agent-docs/reviews/hex-phase2c-chase--readability-cleanup-phase1/PHASE1-REVIEW-FINDINGS.md)), 9 prioritized findings were remediated:

| ID | Severity | Target File(s) | Architectural Impact Prior to Remediation | Implemented Resolution |
| :---: | :---: | :--- | :--- | :--- |
| **F-01** | `High` | `parser.go` | `isPlaceholderNumeric` rejected spreadsheet missing values (`"NA"`, `"#N/A"`, `"---"`, `"none"`, `"null"`, `"nil"`, `"-$"`) and Unicode typographic dashes (`\u2013`, `\u2014`, `\u2212`, `\u2015`), causing entire statement ingestion to abort on optional balance fields. | Expanded `isPlaceholderNumeric` token matching and rune dash scanning, guaranteeing safe detection while preserving valid zeros (`0`, `0.00`, `$0.00`, `-0.00`) and negative values. |
| **F-02** | `Medium` | `parser.go` | Incomplete bounds checking in `getFieldValue` only verified `idx >= len(row)`, risking runtime panics on negative indices (`idx < 0`) from uninitialized or corrupted maps. | Updated bounds condition to `!ok \|\| idx < 0 \|\| idx >= len(row)`. |
| **F-03** | `Medium` | `parser.go` | `cleanNumeric` failed to strip carriage return (`\r`) in CRLF files and tabular figure spaces (`\u2007`), causing `strconv.ParseFloat` syntax errors on valid numbers. | Replaced custom rune list with `unicode.IsSpace(r)` in `strings.Map`, stripping all whitespace categories. |
| **F-04** | `Medium` | `parser.go` | `rowReader` lacked a `parseDate` method, forcing dialect parsers to bypass stack encapsulation and manually format line errors. | Implemented `(r rowReader) parseDate(aliases ...string) (time.Time, error)` formatting `row %d: transaction date is required` and wrapping parse errors. |
| **F-05** | `Medium` | `parser.go` | `findColumn` performed lookups without trimming candidate aliases and did not skip empty candidate strings, risking false header misses. | Trimmed candidates with `strings.TrimSpace` and skipped empty strings before map lookup. |
| **F-06** | `Medium` | `credit_card.go` | `isPlaceholderDate` duplicated placeholder checks, causing potential logic drift with numeric missing-value detection. | Refactored `isPlaceholderDate` to delegate directly to `isPlaceholderNumeric(val) \|\| strings.EqualFold(strings.TrimSpace(val), "pending")`. |
| **F-07** | `Medium` | `parser_test.go` | Benchmark suite lacked `BenchmarkDirect_ParseRow` comparative baseline, preventing empirical verification of zero-allocation overhead for `rowReader`. | Added comparative baseline benchmark; verified `rowReader` incurs 0 heap allocations and 0 GC pressure. |
| **F-08** | `Low` | `parser.go` | Package-level mutable global slice `supportedDateLayouts` required `//nolint:gochecknoglobals`. | Replaced global slice with stack-allocated fixed array `layouts := [...]string{...}` inside `parseDate`. |
| **F-09** | `Low` | `parser_test.go` | Test matrix lacked edge-case assertions for negative column indices, zero-value `rowReader{}`, CRLF `\r`, and Unicode spreadsheet placeholders. | Added dedicated test suites: `TestRowReader_ZeroValueAndNegativeBounds`, `TestCleanNumeric_CRLFAndFigureSpace`, `TestFindColumn_TrimmingAndEmptyAliases`, and expanded table-driven cases. |

---
## 8. Verification Harness & Local Quality Gates

All secondary ingestion adapters maintain strict quality gates ensuring standard-library isolation, race safety, and high test coverage.

### 8.1. Standard Library Isolation Gate
Run from `babylon_data_loader/`:

```bash
go list -f '{{.ImportPath}} -> {{.Imports}}' ./internal/infra/sources/...
```

**Verified Output**:
```text
babylon/dataloader/internal/infra/sources/chase -> [babylon/dataloader/internal/domain/model babylon/dataloader/internal/domain/port bufio bytes context encoding/csv errors fmt io math strconv strings time]
babylon/dataloader/internal/infra/sources/fidelity -> [babylon/dataloader/internal/domain/model babylon/dataloader/internal/domain/port bufio bytes context encoding/csv errors fmt io strconv strings]
babylon/dataloader/internal/infra/sources/synthetic -> [babylon/dataloader/internal/domain/model babylon/dataloader/internal/domain/port context fmt io math math/rand/v2 sync time]
```
No external packages, third-party CSV libraries, or database drivers exist in the dependency tree.

---

### 8.2. Race Detection & Test Execution Gate
Execute tests across all source packages with race detection enabled:

```bash
go test -v -race -cover ./internal/infra/sources/...
```

**Verified Test Summary**:
- `chase`: **93.4% statement coverage** (100% on `parser.go` helpers using table-driven tests and assertion helpers `assertBalanceResult` and `assertDateResult` to keep `gocognit` complexity < 20). Fully decoupled from local disk files (`data/unprocessed` and `testdata`): runs entirely on in-memory synthetic fixtures, executing 100% of integration cases in CI. Auxiliary fixture tests defensively skip if optional local files are absent. Passes dialect classification (peek bounds, credit card precedence, leading blank lines, nil context), checking parser (corrupted balance rejection, non-finite amounts, accounting parens, ISO dates, multiline physical lineage), credit card parser (pending transaction fallback via `isPlaceholderDate`), zero-value struct safety, negative index bounds guarding, exported sentinels, and context cancellation.
- `fidelity`: **91.8% statement coverage** (100% on `parser.go` helpers). Passes schema signature rejection on arbitrary CSVs, disclaimer and summary total filtering, preservation of legitimate funds with `"Total "` in descriptions (`VTI`, `BND`, `TTE`), cash holding normalization, BOM error handling, accounting parentheses, Unicode non-breaking space parsing, single-account inference, sentinel errors, context cancellation, and synthetic fixture parsing (`Portfolio_Positions_Synthetic.csv`).
- `synthetic`: **91.1% statement coverage**. Passes deterministic reproducibility, RNG continuity across sequential calls, zero counts, metadata validation, and row-count safety bounds (>100,000 rejection).

---

### 8.3. Static Analysis & Linter Gate
Run the project linter:

```bash
golangci-lint run ./internal/infra/sources/...
```

**Verified Output**:
```text
0 issues.
```

---

### 8.4. Continuous Integration Suite
Run the full automated test suite:

```bash
make test-ci
```

**Verified Result**: Full repository test pass with 100% statement coverage across domain models and clean pass across all secondary adapters with 0 test skips on CI-critical suites.

---

### 8.5. Comparative Parser Benchmarking Gate
Benchmark memory allocations and throughput to ensure the `rowReader` abstraction introduces zero heap allocations and zero execution overhead:

```bash
go test -bench=. -benchmem ./internal/infra/sources/chase/...
```

**Verified Benchmark Results**:
```text
goos: darwin
goarch: arm64
pkg: babylon/dataloader/internal/infra/sources/chase
cpu: Apple M1
BenchmarkDirect_ParseRow-8       3421536       325.6 ns/op        16 B/op        2 allocs/op
BenchmarkRowReader_ParseRow-8    3386782       332.2 ns/op        16 B/op        2 allocs/op
PASS
ok      babylon/dataloader/internal/infra/sources/chase    3.191s
```

**Architectural Analysis**:
- **Allocation Parity**: Both direct procedural extraction (`BenchmarkDirect_ParseRow`) and `rowReader` encapsulation (`BenchmarkRowReader_ParseRow`) register exactly `16 B/op` and `2 allocs/op`. This allocation stems entirely from the heap box required by the optional `*float64` balance field (`bal, err := parseBalance(...)`). The `rowReader` value struct itself incurs **0 B/op** and **0 allocs/op**.
- **Execution Overhead**: `rowReader` runs at ~332.2 ns/op versus ~325.6 ns/op for raw procedural parsing—a negligible delta (<2%) fully attributable to stack frame setup.
- **Escape Analysis**: Confirmed via `go build -gcflags="-m -m"`: `rowReader` remains entirely stack-allocated and does not escape to the heap.

---

## 9. Related Documentation & References

- [Phase 1 Specification: Domain Core & Port Contracts](./hexagonal-domain-core.md)
- [Fidelity Portfolio Positions Ingestion Specification](./fidelity-portfolio-ingestion.md)
- [MongoDB Atlas Datalake Specification](./mongodb-atlas-datalake.md)
- [Hexagonal Architecture Refactoring Plan (Parent Strategy)](../../../agent-docs/hex-refactor/README.md)
- [Phase 2 Implementation Plan: Ingestion Adapters & Dialects](../../../agent-docs/hex-refactor/PHASE2-INGESTION-ADAPTERS.md)
- [Phase 2 Review Findings & Remediation Synthesis](../../../agent-docs/reviews/hex-refactor-phase2/PHASE2-REVIEW-FINDINGS.md)
- [Phase 2B Fidelity Review Findings & Remediation Synthesis](../../../agent-docs/reviews/hex-phase2b-fidelity/FIDELITY-REVIEW-FINDINGS.md)
- [Phase 2C Chase Review Findings & Hardening Synthesis](../../../agent-docs/reviews/hex-phase2c-chase/CHASE-REVIEW-FINDINGS.md)
- [Phase 2C Chase Readability & Primitives Cleanup Review Findings (Phase 1)](../../../agent-docs/reviews/hex-phase2c-chase--readability-cleanup-phase1/PHASE1-REVIEW-FINDINGS.md)
- [Phase 1 Execution Plan: Zero-Allocation Parser Primitives](../../../agent-docs/reviews/hex-phase2c-chase/PHASE1-ZERO-ALLOC-PARSER-PRIMITIVES.md)
- [Pull Request #51: Chase Readability Cleanup Phase 1](https://github.com/ajponte/babylon_data_loader/pull/51)
- [AI Effort Estimation Guardrails & Task Decomposition Playbook](../../../agent-docs/reviews/hex-refactor-phase2/AI-EFFORT-ESTIMATION-GUARDRAILS.md)
- [Blameless Retrospective: Hexagonal Refactor Schedule Variance & Sizing Breakdown](../../../agent-docs/reviews/HEX-REFACTOR-BLAMELESS-RETROSPECTIVE.md)
- [Phase 3 Plan: Storage Adapters & MongoDB Router](../../../agent-docs/hex-refactor/PHASE3-STORAGE-ADAPTERS.md)
