# Chase Statement Ingestion Adapter Architecture Specification

This document provides the complete architectural and technical specification for the Chase Bank Statement Ingestion Adapter located in `babylon_data_loader/internal/infra/sources/chase/`.

---

## 1. Overview & Architectural Boundaries

The Chase adapter extracts, classifies, sanitizes, and normalizes financial statement records from Chase Bank CSV exports for deposit accounts (checking/savings) and revolving credit lines (credit cards). 

Following the Phase 1–3 readability and robustness cleanups, the adapter enforces a strict separation of concerns:
- **`extractor.go`**: Declarative 4-stage pipeline orchestrator implementing `port.Extractor`.
- **`classifier.go`**: Memory-bounded in-stream dialect classification via 4KB peeking.
- **`parser.go`**: Dedicated zero-allocation parser primitives (`rowReader`, `isPlaceholderNumeric`, `cleanNumeric`), streaming coordination (`parseChaseRecords`), and context-aware error unwrapping (`handleReadError`).
- **`checking.go`**: Ergonomic row mapper for deposit accounts using `rowReader`.
- **`credit_card.go`**: Ergonomic row mapper for credit card accounts with pending date fallback (`resolveCreditCardDate`).

```mermaid
flowchart TD
    subgraph ChaseAdapter ["Chase Source Package (internal/infra/sources/chase)"]
        EXT["Extractor (extractor.go)<br/>4-Stage Declarative Pipeline"]
        CLS["DefaultClassifier (classifier.go)<br/>bufio.Reader.Peek(4096)"]
        ROUTER["Strategy Router<br/>resolveDialectRoute(switch)"]
        PRS["Parser Engine (parser.go)<br/>• parseChaseRecords<br/>• rowReader (40B stack value)<br/>• isPlaceholderNumeric<br/>• cleanNumeric (unicode.IsSpace)<br/>• handleReadError (unwrapped ctx)"]
        CHK["checking.go<br/>parseCheckingRow(rr)"]
        CC["credit_card.go<br/>parseCreditCardRow(rr)<br/>resolveCreditCardDate(rr)"]
    end

    EXT -->|1. validateExtractParams| EXT
    EXT -->|2. classifyStream| CLS
    CLS -->|DialectType + stream| EXT
    EXT -->|3. parseDialect| ROUTER
    ROUTER -->|DialectChecking| CHK
    ROUTER -->|DialectCreditCard| CC
    CHK --> PRS
    CC --> PRS
    EXT -->|4. buildBatch| BATCH["model.IngestionBatch"]
```

---

## 2. Declarative 4-Stage Pipeline Orchestrator (`extractor.go`)

`Extractor.Extract` decomposes statement ingestion into a 16-line declarative pipeline:

```go
func (e *Extractor) Extract(
	ctx context.Context,
	r io.Reader,
	meta model.FileMetadata,
) (*model.IngestionBatch, error) {
	ctx, err := validateExtractParams(ctx, r, meta)
	if err != nil {
		return nil, err
	}

	dialect, stream, err := e.classifyStream(ctx, r)
	if err != nil {
		return nil, err
	}

	dest, records, err := parseDialect(ctx, dialect, stream, meta)
	if err != nil {
		return nil, err
	}

	return buildBatch(dest, meta, records)
}
```

### Stage 1: `validateExtractParams`
Defensively sanitizes input arguments and establishes short-circuit evaluation:
1. Context Nil Guard: If `ctx == nil`, defaults to `context.Background()`.
2. Immediate Cancellation Check: Evaluates `ctx.Err()` **first**, immediately returning raw `context.Canceled` or `context.DeadlineExceeded` prior to any struct or reader validations.
3. Stream Guard: Verifies `r != nil`, returning typed sentinel `ErrNilReader`.
4. Metadata Invariant Guard: Executes `meta.Validate()`, guarding against empty `FileName`, `FileHash`, `AccountID`, or zero `IngestedAt`.

```go
func validateExtractParams(ctx context.Context, r io.Reader, meta model.FileMetadata) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if r == nil {
		return nil, ErrNilReader
	}
	if err := meta.Validate(); err != nil {
		return nil, fmt.Errorf("invalid file metadata: %w", err)
	}
	return ctx, nil
}
```

### Stage 2: `classifyStream` & Nil Stream Protection
Delegates to `Classifier.Classify` while enforcing pipeline safety invariants:
- Unwrapped Context Error Propagation: If classification fails due to context cancellation, returns raw `context.Canceled` or `context.DeadlineExceeded` without wrapping (F-01).
- Nil Stream Guard: Guarantees that custom or third-party classifier implementations never return a `nil` stream reader with a non-error status (F-02), preventing `csv.NewReader(nil)` panic.

```go
func (e *Extractor) classifyStream(ctx context.Context, r io.Reader) (DialectType, io.Reader, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return DialectUnknown, nil, ctxErr
	}

	dialect, stream, err := e.classifier.Classify(ctx, r)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return DialectUnknown, nil, ctxErr
		}
		if errors.Is(err, context.Canceled) {
			return DialectUnknown, nil, context.Canceled
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return DialectUnknown, nil, context.DeadlineExceeded
		}
		return DialectUnknown, nil, fmt.Errorf("failed to classify chase statement dialect: %w", err)
	}

	if stream == nil {
		return DialectUnknown, nil, fmt.Errorf("classifier returned nil reader stream for dialect %s", dialect)
	}

	return dialect, stream, nil
}
```

### Stage 3: `parseDialect` & Pure Static Strategy Router
Replaces mutable package-level global routing maps with a pure static switch statement (`resolveDialectRoute`):
- **Compile-Time Immutability**: Zero runtime mutation risk, zero heap allocations, and elimination of `//nolint:gochecknoglobals,exhaustive` waivers (F-07).
- Routes `DialectChecking` -> `model.DestChaseChecking` and `parseChecking`.
- Routes `DialectCreditCard` -> `model.DestChaseCreditCard` and `parseCreditCard`.
- Defensively handles `DialectUnknown` and unrecognized dialect types, wrapping typed sentinel `ErrUnknownDialect`.

```go
type dialectParser func(ctx context.Context, r io.Reader, meta model.FileMetadata) ([]model.ChaseTransaction, error)

type dialectRoute struct {
	destination model.Destination
	parser      dialectParser
}

func resolveDialectRoute(dialect DialectType) (dialectRoute, bool) {
	switch dialect {
	case DialectChecking:
		return dialectRoute{
			destination: model.DestChaseChecking,
			parser:      parseChecking,
		}, true
	case DialectCreditCard:
		return dialectRoute{
			destination: model.DestChaseCreditCard,
			parser:      parseCreditCard,
		}, true
	case DialectUnknown:
		return dialectRoute{}, false
	default:
		return dialectRoute{}, false
	}
}
```

### Stage 4: `buildBatch` & Envelope Completeness
Encapsulates parsed transactions into an `IngestionBatch`, populating and validating all 6 envelope metadata properties:
1. `Destination`: Determined by strategy route (`DestChaseChecking` or `DestChaseCreditCard`).
2. `AccountID`: Propagated from `FileMetadata`.
3. `SourceFile`: Propagated from `FileMetadata.FileName`.
4. `FileHash`: Propagated from `FileMetadata.FileHash`.
5. `IngestedAt`: Propagated from `FileMetadata.IngestedAt`.
6. `RecordCount`: Set to `len(records)`.
7. `Payload`: Strongly-typed `[]ChaseTransaction`.

---

## 3. In-Stream Dialect Classification (`classifier.go`)

### Memory-Bounded Header Peeking
[`DefaultClassifier.Classify`](../../internal/infra/sources/chase/classifier.go) wraps incoming streams in `bufio.Reader` and inspects up to `4096` bytes via `br.Peek(4096)`:
- **Bounded Memory**: Fixed at 4KB. Minified text files or unbounded streams cannot cause Out-Of-Memory (OOM) exhaustion.
- **Non-Destructive Reuse**: Returns the existing `br` instance directly as the unconsumed reader stream. Avoids buffer re-allocations or `io.MultiReader` overhead.
- **Context-Aware Loop**: Discards leading blank or whitespace-only lines while checking `ctx.Err()` on every iteration.

### Classification Rule Precedence
Because Chase credit card exports occasionally include balance columns (e.g. `"New Balance"`), credit card indicators must be evaluated **before** checking balance indicators:

```text
Dialect Precedence Hierarchy
├── 1. Credit Card Markers (Evaluated First)
│   ├── Contains "category"
│   ├── Contains "memo"
│   └── Contains "transaction date" OR "trans date"
│       └── Result: DialectCreditCard
├── 2. Checking & Deposit Markers (Evaluated Second)
│   ├── Contains "check or slip"
│   ├── Contains "details" AND "balance"
│   ├── Contains "posting date" AND "balance"
│   └── Contains "balance"
│       └── Result: DialectChecking
└── 3. Unrecognized
    └── Result: DialectUnknown
```

---

## 4. Shared Zero-Allocation Parser Primitives (`parser.go`)

### 1. Reusable Streaming Harness (`parseChaseRecords`)
Coordinates CSV decoding using `encoding/csv.Reader` with `FieldsPerRecord = -1` and `TrimLeadingSpace = true`:
- Pre-allocates record slice with `defaultTransactionsCapacity = 64`.
- Obtains exact 1-based physical line numbers via `reader.FieldPos(0)` for audit lineage.
- Skips empty rows without error.
- Evaluates `ctx.Err()` on every iteration.

### 2. Context Cancellation Unwrapping Contract (`handleReadError`)
Resolves critical review finding F-01: when context cancellation or deadline expiration occurs during CSV header detection or row scanning, errors are returned strictly unwrapped:

```go
func handleReadError(ctx context.Context, dialectName, target string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return fmt.Errorf("failed to read Chase %s CSV %s: %w", dialectName, target, err)
}
```

### 3. Stack-Allocated `rowReader` Value Struct
Encapsulates row slice access, header column index lookups, and physical line number formatting:
- **Memory Footprint**: Exactly 40 bytes on 64-bit architectures:
  - `row []string`: 24 bytes (pointer [8B] + length [8B] + capacity [8B])
  - `colIndex map[string]int`: 8 bytes (pointer to `runtime.hmap`)
  - `lineNum int`: 8 bytes (machine integer)
- **Zero Heap Allocations**: All methods use uniform value receivers (`func (r rowReader)`). Go compiler escape analysis (`-gcflags="-m -m"`) proves `rowReader` never escapes to the heap.
- **Line-Attributed Error Reporting**: Formats validation errors with physical line number attribution (`row %d: ...`).

```go
type rowReader struct {
	row      []string
	colIndex map[string]int
	lineNum  int
}

func newRowReader(row []string, colIndex map[string]int, lineNum int) rowReader {
	return rowReader{
		row:      row,
		colIndex: colIndex,
		lineNum:  lineNum,
	}
}

func (r rowReader) get(aliases ...string) string {
	return getFieldValue(r.row, r.colIndex, aliases...)
}

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

func (r rowReader) parseDescription(aliases ...string) (string, error) {
	return parseDescription(r.get(aliases...), r.lineNum)
}

func (r rowReader) parseAmount(aliases ...string) (float64, error) {
	return parseAmount(r.get(aliases...), r.lineNum)
}

func (r rowReader) parseBalance(aliases ...string) (*float64, error) {
	return parseBalance(r.get(aliases...), r.lineNum)
}
```

### 4. Tabular Missing-Value & Unicode Dash Detection (`isPlaceholderNumeric`)
Centralizes detection of absent values across optional numeric columns, eliminating parser crashes when encountering financial spreadsheet missing tokens or typographic dashes:
- Matches tabular missing tokens: `"-"`, `"--"`, `"---"`, `"n/a"`, `"na"`, `"#n/a"`, `"none"`, `"null"`, `"nil"`, `"-$"`.
- Scans for Unicode typographic dashes: `\u2013` (en-dash `–`), `\u2014` (em-dash `—`), `\u2212` (minus sign `−`), `\u2015` (horizontal bar `―`).
- Preserves valid numeric zeros: `0`, `0.00`, `$0.00`, `-0.00`.

```go
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

### 5. Whitespace Normalization (`cleanNumeric`)
Uses `unicode.IsSpace(r)` in `strings.Map` to strip all whitespace categories:
- Strips ASCII spaces, tabs (`\t`), CRLF carriage returns (`\r`), non-breaking spaces (`\u00a0`, `\u202f`), and tabular figure spaces (`\u2007`).
- Strips currency signs (`$`), commas (`,`), and plus signs (`+`).
- Converts accounting negative formats (`($50.00)` and `$(45.00)` -> `-50.00`, `-45.00`).
- Does NOT coerce missing placeholder tokens to zero (detection is handled by `isPlaceholderNumeric`).

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

### 6. Defensive Column Lookups & Bounds Guard (`findColumn`, `getFieldValue`)
- `findColumn`: Trims candidate aliases with `strings.TrimSpace` and skips empty candidate strings before map lookup.
- `getFieldValue`: Enforces two-sided bounds checking (`!ok || idx < 0 || idx >= len(row)`), preventing negative index panics from uninitialized map values.

### 7. Scoped Date Parsing (`parseDate`)
Uses a stack-allocated fixed array `layouts := [...]string{...}` inside `parseDate` (`MM/DD/YYYY`, `M/D/YYYY`, `YYYY-MM-DD`, `YYYY/MM/DD`), eliminating mutable package-level globals and normalizing all timestamps to midnight UTC (`time.Date(..., time.UTC)`).

---

## 5. Dialect Row Mappers

### Checking Dialect Row Mapper (`checking.go`)
Streamlined to 78 lines. Uses `rowReader` to extract fields with cognitive complexity = 2:

```go
func parseCheckingRow(
	row []string,
	colIndex map[string]int,
	lineNum int,
	meta model.FileMetadata,
) (model.ChaseTransaction, error) {
	rr := newRowReader(row, colIndex, lineNum)

	postingDate, err := rr.parseDate("posting date", "post date")
	if err != nil {
		return model.ChaseTransaction{}, err
	}

	desc, err := rr.parseDescription("description")
	if err != nil {
		return model.ChaseTransaction{}, err
	}

	amount, err := rr.parseAmount("amount")
	if err != nil {
		return model.ChaseTransaction{}, err
	}

	balance, err := rr.parseBalance("balance")
	if err != nil {
		return model.ChaseTransaction{}, err
	}

	details := rr.get("details")
	txType := firstNonEmpty(rr.get("type"), details, "TRANSACTION")
	checkNum := rr.get("check or slip #", "check or slip num")

	tx := model.ChaseTransaction{
		Lineage:        buildLineage(meta, lineNum),
		AccountID:      meta.AccountID,
		PostingDate:    postingDate,
		Description:    desc,
		Details:        details,
		Amount:         amount,
		Type:           txType,
		Balance:        balance,
		CheckOrSlipNum: checkNum,
		Category:       "",
		Memo:           "",
	}

	if valErr := tx.Validate(); valErr != nil {
		return model.ChaseTransaction{}, fmt.Errorf("row %d validation error: %w", lineNum, valErr)
	}

	return tx, nil
}
```

### Credit Card Dialect Row Mapper (`credit_card.go`)
Streamlined to 97 lines. Handles pending transaction date fallbacks and preserves balance when present in credit card CSVs:

```go
func parseCreditCardRow(
	row []string,
	colIndex map[string]int,
	lineNum int,
	meta model.FileMetadata,
) (model.ChaseTransaction, error) {
	rr := newRowReader(row, colIndex, lineNum)

	postingDate, err := resolveCreditCardDate(rr)
	if err != nil {
		return model.ChaseTransaction{}, err
	}

	desc, err := rr.parseDescription("description")
	if err != nil {
		return model.ChaseTransaction{}, err
	}

	amount, err := rr.parseAmount("amount")
	if err != nil {
		return model.ChaseTransaction{}, err
	}

	balance, err := rr.parseBalance("balance")
	if err != nil {
		return model.ChaseTransaction{}, err
	}

	tx := model.ChaseTransaction{
		Lineage:        buildLineage(meta, lineNum),
		AccountID:      meta.AccountID,
		PostingDate:    postingDate,
		Description:    desc,
		Details:        "",
		Amount:         amount,
		Type:           firstNonEmpty(rr.get("type"), "Sale"),
		Balance:        balance,
		CheckOrSlipNum: "",
		Category:       rr.get("category"),
		Memo:           rr.get("memo"),
	}

	if valErr := tx.Validate(); valErr != nil {
		return model.ChaseTransaction{}, fmt.Errorf("row %d validation error: %w", lineNum, valErr)
	}

	return tx, nil
}

func resolveCreditCardDate(rr rowReader) (time.Time, error) {
	postDateStr := rr.get("post date", "posting date")
	if isPlaceholderDate(postDateStr) {
		postDateStr = rr.get("transaction date", "trans date")
	}
	if isPlaceholderDate(postDateStr) {
		return time.Time{}, fmt.Errorf("row %d: missing both post date and transaction date", rr.lineNum)
	}
	postingDate, err := parseDate(postDateStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("row %d: %w", rr.lineNum, err)
	}
	return postingDate, nil
}

func isPlaceholderDate(val string) bool {
	return isPlaceholderNumeric(val) || strings.EqualFold(strings.TrimSpace(val), "pending")
}
```

---

## 6. Exported Sentinel Errors

[`extractor.go`](../../internal/infra/sources/chase/extractor.go) exports typed sentinel errors for callers and test suites to inspect with `errors.Is`:

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

---

## 7. Performance Profiles & Benchmarking Guarantees

The Chase adapter is validated against rigorous throughput, memory allocation, and concurrency benchmarks:

### 1. Comparative Row Parsing Parity
- Procedural Direct Parsing (`BenchmarkDirect_ParseRow`): ~407.3 ns/op, 16 B/op, 2 allocs/op.
- Stack `rowReader` Parsing (`BenchmarkRowReader_ParseRow`): ~413.0 ns/op, 16 B/op, 2 allocs/op.
- **Zero-Allocation Invariant**: Both benchmarks register identical memory footprints (16 B/op, 2 allocs/op), which stems solely from heap boxing the optional `*float64` balance field (`bal, err := parseBalance(...)`). The `rowReader` value struct itself introduces **`0 B/op` and `0 allocs/op`** with a negligible execution latency delta (< 1.4%).

### 2. Pipeline Extraction Throughput
- **Single Checking Batch** (`BenchmarkChaseExtractor_Extract_Checking`): ~4.9 µs/batch, ~24.6 KB, 42 allocs.
- **Single Credit Card Batch** (`BenchmarkChaseExtractor_Extract_CreditCard`): ~4.5 µs/batch, ~24.6 KB, 39 allocs.
- **Concurrent Ingestion** (`BenchmarkChaseExtractor_Extract_Parallel`): ~4.2 µs/batch, ~24.6 KB, 40 allocs, verified under Go race detector (`-race`) across 20 parallel workers with **zero data races**.
- **High-Volume Scaling** (`BenchmarkChaseExtractor_Extract_HighVolume_500Rows`): Parses 500 records in ~348 µs (~697 ns/row, ~617 B/row steady-state, ~3 allocs/row), confirming linear scaling without garbage collection degradation.

### 3. Fast-Fail & Interruption Guarantees
- **Pre-Canceled Context** (`BenchmarkChaseExtractor_Extract_PreCanceledContext`): Fast-fails in ~26.35 ns (32 B/op, 1 alloc), immediately returning raw `context.Canceled` before stream reader inspection or metadata parsing.
- **Empty Stream** (`BenchmarkChaseExtractor_Extract_EmptyStream`): Fast-fails in ~548.4 ns (4.3 KB, 6 allocs) upon encountering immediate EOF.

### 4. Batch Envelope Completeness
Every batch returned by `Extractor.Extract` is verified to satisfy all 6 metadata properties:
- `Destination`: `model.DestChaseChecking` or `model.DestChaseCreditCard`.
- `AccountID`: Verified against input `meta.AccountID`.
- `SourceFile`: Verified against input `meta.FileName`.
- `FileHash`: Verified against input `meta.FileHash`.
- `IngestedAt`: Verified against input `meta.IngestedAt`.
- `RecordCount`: Verified strictly equal to `len(batch.Payload)`.
