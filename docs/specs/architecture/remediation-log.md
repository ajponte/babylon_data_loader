# Architectural Decisions & Ingestion Remediation Log

This document tracks all formal architectural decisions, prioritized code review findings, and QA hardening logs across the lifecycle of the hexagonal ingestion adapters in `babylon_data_loader`.

---

## 1. Phase 2 Consolidated Ingestion Findings

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

## 2. Phase 2B Fidelity Adapter Hardening Log

During the dedicated code review and QA hardening of the isolated Fidelity adapter on branch `hex-phase2b-fidelity` (synthesized in [`agent-docs/reviews/hex-phase2b-fidelity/FIDELITY-REVIEW-FINDINGS.md`](../../../../agent-docs/reviews/hex-phase2b-fidelity/FIDELITY-REVIEW-FINDINGS.md)), 10 findings were prioritized and resolved:

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

## 3. Phase 2C Chase Adapter Hardening Log

During the initial code review and QA hardening of the Chase adapter on branch `hex-phase2c-chase` (synthesized in [`agent-docs/reviews/hex-phase2c-chase/CHASE-REVIEW-FINDINGS.md`](../../../../agent-docs/reviews/hex-phase2c-chase/CHASE-REVIEW-FINDINGS.md)), 9 findings were prioritized and resolved:

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

## 4. Phase 1 Chase Readability & Primitives Cleanup Log

During the readability refactor of Chase parser primitives on branch `hex-phase2c-chase--readability-cleanup-phase1` (PR #51, synthesized in [`agent-docs/reviews/hex-phase2c-chase--readability-cleanup-phase1/PHASE1-REVIEW-FINDINGS.md`](../../../../agent-docs/reviews/hex-phase2c-chase--readability-cleanup-phase1/PHASE1-REVIEW-FINDINGS.md)), 9 findings were remediated:

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

## 5. Phase 2 Chase Row Parser Ergonomics & Date Fallbacks Cleanup Log

During the Phase 2 refactor on branch `hex-phase2c-chase--readability-cleanup-phase2` (PR #52, synthesized in [`agent-docs/reviews/hex-phase2c-chase--readability-cleanup-phase2/PHASE2-REVIEW-FINDINGS.md`](../../../../agent-docs/reviews/hex-phase2c-chase--readability-cleanup-phase2/PHASE2-REVIEW-FINDINGS.md)), 7 findings were remediated:

| ID | Severity | Target File(s) | Architectural Impact Prior to Remediation | Implemented Resolution |
| :---: | :---: | :--- | :--- | :--- |
| **F-01** | `Medium` | `parser_test.go` | Lineage preservation (`RowNumber`, `FileName`, `FileHash`, `AccountID`) and cross-dialect field isolation unverified in row tests. | Added comprehensive assertions for all lineage and envelope fields across checking and credit card tests. |
| **F-02** | `Medium` | `credit_card.go`<br>`parser_test.go` | Incomplete fallback and alias branch coverage in `resolveCreditCardDate` (missing tests for secondary aliases, empty string fallbacks, and pending post date with corrupted transaction date). | Added table tests covering all 5 branching paths in `resolveCreditCardDate`. |
| **F-03** | `Medium` | `parser.go`<br>`parser_test.go` | Missing explicit zero balance (`"0.00"`, `"$0.00"`, `"0"`, `"-0.00"`) assertions in `TestParseBalance`, risking false-positive placeholder regression. | Added explicit valid zero tests in `TestParseBalance` and dialect row tests. |
| **F-04** | `Medium` | `credit_card.go`<br>`parser_test.go` | Standard Chase credit card exports omitting the `"Balance"` header untested in row parser; risked missing column rejection. | Verified credit card schema without balance column safely yields `tx.Balance == nil`. |
| **F-05** | `Medium` | `checking.go`<br>`credit_card.go`<br>`parser_test.go` | Integrated negative row parsing invariants untested in `parseCheckingRow` and `parseCreditCardRow`. | Added negative row tests asserting row-level error attribution for empty description, missing amount, and non-finite balance. |
| **F-06** | `Low` | `parser_test.go` | Incomplete error string assertion in `TestResolveCreditCardDate` neglected row number prefix verification. | Added prefix check verifying `"row 7: invalid date format"`. |
| **F-07** | `Low` | `credit_card.go`<br>`parser_test.go` | Whitespace trimming unverified for optional string fields (`Type`, `Category`, `Memo`) and zero-value `rowReader{}` untested in `resolveCreditCardDate`. | Added assertions verifying trimming behavior and safe error handling on zero-value `rowReader{}`. |

---

## 6. Phase 3 Chase Strategy Router Pipeline & Hardened Quality Remediations Log

During the Phase 3 refactor on branch `hex-phase2c-chase--readability-cleanup-phase3` (synthesized in [`agent-docs/reviews/hex-phase2c-chase--readability-cleanup-phase3/PHASE3-REVIEW-FINDINGS.md`](../../../../agent-docs/reviews/hex-phase2c-chase--readability-cleanup-phase3/PHASE3-REVIEW-FINDINGS.md)), 7 findings were remediated:

| ID | Severity | Target File(s) | Architectural Impact Prior to Remediation | Implemented Resolution |
| :---: | :---: | :--- | :--- | :--- |
| **F-01** | `High` | `extractor.go`<br>`parser.go` | Context cancellation was wrapped into formatted errors instead of returning raw `context.Canceled` or `context.DeadlineExceeded` across classification, header reading, and row parsing. | Implemented strictly unwrapped context error returns across `classifyStream`, `parseChaseRecords`, and `handleReadError`. |
| **F-02** | `Medium` | `extractor.go` | Missing nil stream guard in `classifyStream` allowed custom classifiers returning `(DialectChecking, nil, nil)` to trigger a nil pointer dereference panic in `csv.NewReader(nil)`. | Added explicit nil stream reader check in `classifyStream` returning a descriptive error. |
| **F-03** | `Medium` | `extractor_test.go` | `TestChaseExtractor_ClassifierAndRoutingEdgeCases` asserted `errors.Is(err, context.DeadlineExceeded)`, which gave false confidence by passing even when wrapped; missing `context.Canceled` unwrapped test. | Added strict equality assertions (`err != context.Canceled`, `err != context.DeadlineExceeded`) ensuring unwrapped returns. |
| **F-04** | `Medium` | `extractor_test.go` | `runConcurrentExtract` and inline tests only asserted `RecordCount == 1`, omitting envelope verification across concurrent workers. | Added comprehensive validation of all 6 batch metadata properties across concurrent workers. |
| **F-05** | `Medium` | `extractor_test.go` | Missing explicit boundary test assertions in `TestChaseExtractor_Errors` for `model.ErrMissingAccountID`, `model.ErrMissingFileName`, and `model.ErrMissingIngestedAt`. | Added dedicated subtests for metadata validation error sentinels. |
| **F-06** | `Medium` | `extractor.go` | `validateExtractParams` evaluated `meta.Validate()` and pointer checks before checking `ctx.Err()`, violating short-circuit ordering on canceled contexts. | Reordered checks to evaluate `ctx.Err()` first before any struct validations. |
| **F-07** | `Low` | `extractor.go` | Package-level mutable global map `dialectRoutes` required `//nolint:gochecknoglobals,exhaustive`. | Replaced mutable map with pure static switch statement `resolveDialectRoute`, achieving compile-time immutability and 0 allocations. |

---

## 7. Phase 4 Chase Verification Benchmarks, Concurrency & Quality Gates Remediation Log

During the Phase 4 verification and benchmark hardening on branch `hex-phase2c-chase--readability-cleanup-phase4` (synthesized in [`agent-docs/reviews/hex-phase2c-chase--readability-cleanup-phase4/PHASE4-REVIEW-FINDINGS.md`](../../../../agent-docs/reviews/hex-phase2c-chase--readability-cleanup-phase4/PHASE4-REVIEW-FINDINGS.md)), 6 findings were remediated alongside multi-core parallelism and bandwidth instrumentation:

| ID | Severity | Target File(s) | Architectural Impact Prior to Remediation | Implemented Resolution |
| :---: | :---: | :--- | :--- | :--- |
| **F-01** | `High/Medium` | `extractor_test.go` | Calling `b.Fatalf` inside `b.RunParallel` worker closure violated Go testing invariants (`runtime.Goexit()` in worker goroutine), risking benchmark deadlocks or runner hangs. | Extracted `runParallelExtractWorker`, replaced `b.Fatalf` with `b.Errorf` and early `return` in `BenchmarkChaseExtractor_Extract_Parallel`. |
| **F-02** | `Medium` | `extractor_test.go` | Extraction benchmark suite only profiled 3-row happy paths; missing benchmarks for short-circuit conditions (pre-canceled context and empty stream). | Added `BenchmarkChaseExtractor_Extract_PreCanceledContext` (26.27 ns/op, 1 alloc), `BenchmarkChaseExtractor_Extract_PreCanceledContext_Parallel` (63.91 ns/op, 1 alloc), and `BenchmarkChaseExtractor_Extract_EmptyStream` (543.7 ns/op, 6 allocs). |
| **F-03** | `Medium` | `extractor_test.go` | `TestChaseExtractor_Extract_CheckingInline` and `CreditCardInline` verified record counts and payloads but omitted assertions for batch metadata envelope fields (`SourceFile`, `FileHash`, `IngestedAt`, `AccountID`). | Expanded inline tests to assert all 6 metadata envelope properties against input `meta`. |
| **F-04** | `Medium` | `extractor_test.go` | 3-row fixtures profiled per-call dispatch latency but omitted steady-state streaming throughput, memory reuse, and GC behavior under high volume (500+ rows). | Implemented `generateSyntheticCheckingCSV` helper, `BenchmarkChaseExtractor_Extract_HighVolume_500Rows` (341.1 µs/op, 83.18 MB/s), and `BenchmarkChaseExtractor_Extract_HighVolume_Parallel` (153.8 µs/op, 184.40 MB/s, 2.2x scaling). |
| **F-05** | `Low` | `parser_test.go` | Inconsistent line number variable declaration between comparative benchmarks: `const lineNum = 42` in `BenchmarkDirect` vs `lineNum := 42` in `BenchmarkRowReader`. | Standardized `BenchmarkRowReader_ParseRow` to `const lineNum = 42`. |
| **F-06** | `Low` | `extractor_test.go` | `mockClassifier.Classify` unconditionally returned a nil stream whenever `dialect == chase.DialectChecking`, conflating a valid domain enum with an error simulation condition. | Added dedicated `returnNilStream bool` field to `mockClassifier`, decoupling error injection from valid domain enums. |

### Phase 4 Concurrency & Bandwidth Instrumentation Additions:
1. **Multi-Core Parallel Row Parsing**: Implemented `BenchmarkDirect_ParseRow_Parallel` (82.51 ns/op) and `BenchmarkRowReader_ParseRow_Parallel` (85.63 ns/op). Verified exact zero-allocation parity (16 B/op, 2 allocs/op) under `b.RunParallel`, scaling throughput by ~5.1x across worker goroutines with zero false sharing.
2. **Cancellation Storm Stress**: Added `BenchmarkChaseExtractor_Extract_PreCanceledContext_Parallel` (63.91 ns/op, 32 B/op, 1 alloc) to stress-test concurrent context cancellation short-circuits across multi-goroutine worker pools without lock contention or data races.
3. **High-Volume Parallel Concurrency**: Added `BenchmarkChaseExtractor_Extract_HighVolume_Parallel` (153.8 µs/op, 184.40 MB/s), demonstrating 2.2x parallel scaling over sequential execution (341.1 µs/op, 83.18 MB/s) on 500-record CSV statements.
4. **Bandwidth Throughput Instrumentation**: Configured `b.SetBytes(...)` across all statement benchmarks (`Checking`: 49.76 MB/s, `CreditCard`: 63.57 MB/s, `Parallel`: 62.03 MB/s, `HighVolume_500Rows`: 83.18 MB/s, `HighVolume_Parallel`: 184.40 MB/s) to track I/O ingestion velocity alongside latency and allocations.


---

## 8. Round 3 Chase Adapter Review Remediation Log

During the Round 3 QA and code review of the integrated Chase adapter on branch `hex-phase2c-chase` (PR #43, which includes sub-PRs #51 to #54), 10 consolidated findings were remediated. The detailed review write-ups are kept in the workspace-level `agent-docs/` directory, outside this repository. Chase package coverage rose from 95.0% to 97.9%.

| ID | Severity | Target File(s) | Architectural Impact Prior to Remediation | Implemented Resolution |
| :---: | :---: | :--- | :--- | :--- |
| **R3-01** | `High` | `parser.go`<br>`classifier.go` | A UTF-8 BOM followed by a quoted header failed both dialects with `bare " in non-quoted-field`, because the BOM was stripped only for classification. | `skipUTF8BOM` removes the BOM from the stream before CSV tokenization; the per-column `TrimPrefix` was removed. |
| **R3-02** | `High` | `parser.go` | CR-only (classic Mac) files were read as one header record, so `Extract` returned success with 0 records and the data was silently lost. | `indexHeader` rejects any header column containing `\r` or `\n` with the new `ErrMalformedHeader` sentinel. |
| **R3-03** | `Medium` | `classifier.go`<br>`parser.go` | Classifier and parser chose the header differently: `,,,,,,` rows, more than 4 KB of blank lines, or a valid `Post Date,Description,Amount` header gave `ErrUnknownDialect`; duplicate columns bound last-wins; the parser's EOF branch was unreachable. | One header detector (`newChaseCSVReader` and `readHeaderRecord`, with the same BOM and emptiness rules) serves both stages. Columns are matched exactly, the peek grows from 4 KiB to 64 KiB, and a minimal credit card rule was added. Duplicate required columns return `ErrDuplicateColumn`, and a parser with no header returns `ErrMissingRequiredColumns`. |
| **R3-04** | `Medium` | `parser.go` | `12,34` was stored as 1234, `1+2` as 12 and `0x1p4` as 16 with no error, and a Unicode minus (`−4.50`) aborted the batch. | `cleanNumeric` maps Unicode minus and dashes to `-`, then enforces a strict currency grammar (sign or parentheses, optional `$`, three-digit comma groups, at most two decimals) before `ParseFloat`. |
| **R3-05** | `Medium` | `parser.go`<br>`credit_card.go` | Row errors were plain strings, so `errors.Is` against `model.ErrInvalidAmount`, `ErrMissingDescription` or `ErrMissingPostingDate` was false, and bad data looked like an I/O failure. | Row helpers wrap the domain sentinels (including `model.ErrInvalidBalance`), and `parseChaseRecords` wraps every row rejection in the new `ErrInvalidRow`. |
| **R3-06** | `Medium` | `parser.go`<br>`extractor_test.go` | CSV syntax errors had no sentinel or line context, and the malformed-CSV test only checked that some error came back. | `LazyQuotes` stays `false`. `handleReadError` wraps `*csv.ParseError` with the new `ErrMalformedCSV`, keeping the line and column, and the tests assert the sentinel, `csv.ErrQuote`/`csv.ErrBareQuote` and `StartLine`. |
| **R3-07** | `Medium` | `extractor.go`<br>`classifier.go` | A nil or zero-value `*Extractor` panicked, `NewExtractor` returned an interface, and the nil-context TODO was unresolved. | `classifierOrDefault` makes nil and zero-value extractors usable. The constructors return concrete types with compile-time interface assertions, and a nil context is normalized only in `Extract`; the `Classify` precondition is documented and the TODO removed. |
| **R3-08** | `Medium` | `extractor.go`<br>`extractor_test.go` | `DestChaseChecking == DestChaseCreditCard`, so the 8 destination checks passed even when a checking file was parsed with the credit card parser. | No domain change. `assertDialectPayload` asserts dialect-specific fields at every routing check, and the shared destination is documented in code and spec. |
| **R3-09** | `Medium` | `extractor_test.go`<br>`parser_test.go`<br>`classifier_test.go` | The cancellation test cancelled during the classifier's peek, so the per-row ctx check and the I/O-error paths were never exercised. | A stub `rowParser` cancels after the first row and asserts a strictly unwrapped `context.Canceled` with one call. `cancelingReader` cancels after 8 KiB of a 500-row fixture, and an `errReader` table covers the peek, BOM, header and mid-body I/O failures. |
| **R3-10** | `Medium` | `chase-adapter.md`<br>`hexagonal-ingestion-adapters.md`<br>`verification-and-quality.md` | The specs described a `FileHash` guard, a context-aware classifier loop, a removed `"balance"` rule, distinct destinations and an EOF fast-fail that the code doesn't have, and linked to `agent-docs` outside the repository. | Spec text corrected to match the code. Added the error taxonomy table and the empty-statement contract, fixed broken relative source links, replaced out-of-repo links with this log, and re-measured the extraction benchmarks. |

### Round 3 Decisions
1. **Strict quoting retained**: `LazyQuotes` stays `false`. A loud, line-attributed `ErrMalformedCSV` is safer for financial data than a silent mis-parse.
2. **One header detector**: The classifier reuses the parser's CSV reader, BOM handling and emptiness rule instead of a separate text-line scan. This costs about 13 allocations and under 1 µs per file; per-row cost is unchanged.
3. **Nil context in one place**: Only `Extract` normalizes a nil context. `Classify` documents a non-nil `ctx` as a precondition and never returns a context.
4. **No domain model changes**: Both dialects keep sharing `"transactions_chase"`. Dialect routing is verified through payload fields.
5. **Trailing-minus negatives** (`4.50-`) are rejected with a row-attributed `model.ErrInvalidAmount`, not normalized.

---

## 9. Phase 4G CLI Entrypoint & Code Review Hardening Log

During the implementation and iterative code reviews of the CLI composition root on branch `feat/phase-4g-cli-entrypoint` (PR #89, synthesized in [`agent-docs/reviews/feat/phase-4g-cli-entrypoint/REVIEW-FINDINGS.md`](../../agent-docs/reviews/feat/phase-4g-cli-entrypoint/REVIEW-FINDINGS.md) and [`CODE-REVIEW-FINDINGS.md`](../../agent-docs/reviews/feat/phase-4g-cli-entrypoint/CODE-REVIEW-FINDINGS.md)), 15 findings across QA and code review were remediated:

| ID | Severity | Target File(s) | Architectural Impact Prior to Hardening | Implemented Resolution |
| :---: | :---: | :--- | :--- | :--- |
| **QAF-01** | `Medium` | `cmd/cli/main_test.go` | Fidelity batch record presence checked count only, omitting deserialized payload and field validation. | Added `b.AsFidelityPositions()` assertion verifying `Symbol == "SPAXX**"`, `CurrentValue == 3250.00`, and non-zero `AsOfDate`. |
| **QAF-02** | `Medium` | `cmd/cli/main_test.go` | Zero-file directory ingestion path unverified. | Added `TestRun_IngestEmptyDirectory` verifying 0 files exits 0 and logs `"totalFiles": 0`. |
| **QAF-03** | `Medium` | `cmd/cli/main.go`<br>`cmd/cli/main_test.go` | Pre-canceled or timed-out context returned un-wrapped error string, breaking `errors.Is(err, context.Canceled)`. | Implemented `sanitizedError.Is(target)` method preserving standard sentinel unwrapping while masking credentials. |
| **QAF-04** | `Low` | `cmd/cli/main_test.go` | URI sanitization unverified on percent-encoded characters and complex passwords. | Added table tests asserting URI credential redaction on percent-encoded characters and exact raw URI replacement. |
| **QAF-05** | `Low` | `cmd/cli/main.go`<br>`cmd/cli/main_test.go` | Usage error was unexported string-matching sentinel. | Exported `ErrUsage = errors.New(...)` and asserted via `errors.Is(err, ErrUsage)`. |
| **CRF-01** | `Medium` | `cmd/cli/main.go` | `runIngest` mingled repository connection, extractor registration, and storage traversal. | Decomposed pipeline registration into `newIngestionPipeline(repo, notifier, clock)`. |
| **CRF-02** | `Medium` | `cmd/cli/main.go` | MongoDB connection and error sanitization logic duplicated between `runIngest` and `runPersistSyntheticData`. | Consolidated into reusable `connectRepository(ctx, deps, uri)` helper. |
| **CRF-03** | `Medium` | `cmd/cli/main.go` | Flag declaration and parsing cluttered `runGenerateSyntheticData` control flow. | Extracted `parseSyntheticFlags(args, cfg)` and structured `syntheticOptions` struct with `io.Discard` for clean stderr. |
| **CRF-04** | `Low` | `cmd/cli/main.go` | Synthetic `FileMetadata` construction cluttered persistence runner. | Decomposed into `newSyntheticRequest(now, notifier)` isolating `syntheticAccountID` (`"0000"`). |
| **CRF-05** | `Low` | `cmd/cli/main.go` | Architectural boundaries lacked inline comprehension notes. | Added succinct comments for DI seams, timeout propagation, and custom error unwrapping. |
| **CRF-06** | `Medium` | `cmd/cli/main.go` | `runPersistSyntheticData` lacked pipeline factory symmetry with `runIngest`. | Extracted `newSyntheticPipeline(repo, notifier, clock, rows)` mirroring `newIngestionPipeline`. |
| **CRF-07** | `Medium` | `cmd/cli/main.go` | Store root calculation and key listing procedural boilerplate inside `runIngest`. | Extracted `initStoreAndDiscoverKeys(ctx, d, unprocessedDir)` isolating filesystem storage resolution. |
| **CRF-08** | `Medium` | `cmd/cli/main.go` | Nested `if` conditionals inside `buildObjectRequests` loop. | Extracted `resolveArchiveKey(key, prefix, archivePrefix, moveProcessed)` achieving flat declarative loop. |
| **CRF-09** | `Low` | `cmd/cli/main.go` | JSON formatting entangled with slog logging calls. | Extracted `formatStatsJSON(stats)` separating serialization from logger emission. |
| **CRF-10** | `Low` | `cmd/cli/main.go` | Unexported internal interfaces lacked lifecycle docstrings. | Added succinct docstrings to `repository`, `objectStore`, and `slogNotifier`. |

### Phase 4G Architectural Invariants
1. **Detached Connection Cleanup**: `closeRepoWithTimeout` runs `repo.Close` with `context.WithoutCancel(ctx)` with a 5-second timeout, ensuring clean connection termination even when the main run context timed out or was canceled.
2. **Credential Redaction with Sentinel Preservation**: `sanitizeError` returns a `*sanitizedError` struct implementing `Error() string` (redacted) and `Is(error) bool` (matching underlying errors such as `context.Canceled`), preventing credentials from leaking to logs while preserving caller error inspection.
3. **High-Level Orchestration Symmetry**: Both `runIngest` and `runPersistSyntheticData` are structured into symmetrical 4-step orchestration flows under 25 lines each.
4. **Statement Coverage**: Achieved 86.4% statement coverage on `cmd/cli` with 18 passing tests across all boundary scenarios.

---

## 10. Phase 4H AWS Lambda Worker Cutover & Hardening Log

During the implementation and review passes of the serverless AWS Lambda worker on branch `feat/phase-4h-lambda-cutover` (PR #90, synthesized in [`agent-docs/reviews/feat/phase-4h-lambda-cutover/REVIEW-FINDINGS.md`](../../agent-docs/reviews/feat/phase-4h-lambda-cutover/REVIEW-FINDINGS.md) and [`CODE-REVIEW-FINDINGS.md`](../../agent-docs/reviews/feat/phase-4h-lambda-cutover/CODE-REVIEW-FINDINGS.md)), 15 findings across QA and code review were remediated:

| ID | Severity | Target File(s) | Architectural Impact Prior to Hardening | Implemented Resolution |
| :---: | :---: | :--- | :--- | :--- |
| **CRF-01** | `Medium` | `cmd/lambda/handler.go` | `ensurePipeline` coupled Secrets Manager resolution, client connection, and pipeline registration. | Extracted `connectRepository(ctx)` to isolate MongoDB secret retrieval and client connection logic. |
| **CRF-02** | `Medium` | `cmd/lambda/handler.go` | Extractor registration lacked architectural symmetry with the CLI composition root. | Extracted `newIngestionPipeline(repo, clock)` matching the pattern used in `cmd/cli/main.go`. |
| **CRF-03** | `Medium` | `cmd/lambda/handler.go` | `processRecord` mixed S3 store initialization, stream execution, and error classification. | Extracted `executeIngestion(ctx, p, bucket, key, filename)` separating storage binding from error triage. |
| **CRF-04** | `Low` | `cmd/lambda/handler.go` | Magic string literals used for S3 prefixes. | Standardized on package constants `unprocessedPrefix = "unprocessed/"` and `processedPrefix = "processed/"`. |
| **CRF-05** | `Medium` | `cmd/lambda/handler.go` | Cyclomatic complexity in `isDeterministicError` exceeded linter thresholds (33). | Decomposed into 3 category helpers (`isIngestOrPipelineError`, `isSourceDialectError`, `isModelOrCSVParseError`) dropping complexity to < 12. |
| **QAF-01** | `Medium` | `cmd/lambda/handler.go`<br>`cmd/lambda/handler_test.go` | Pre-canceled context could initiate pipeline work before checking context state. | Added early `ctx.Err() != nil` guard in `processRecord` and added `TestS3Handler_PreCanceledContext`. |
| **QAF-02** | `Medium` | `cmd/lambda/handler_test.go` | Fidelity statement ingestion unverified in Lambda environment. | Added `TestS3Handler_FidelitySuccess` validating destination routing to `positions_fidelity` and `SourceURI`. |
| **QAF-03** | `Medium` | `cmd/lambda/handler_test.go` | Synthetic CSV stream ingestion unverified in Lambda environment. | Added `TestS3Handler_SyntheticSuccess` validating destination routing to `transactions_synthetic` and `SourceURI`. |
| **QAF-04** | `Low` | `cmd/lambda/handler_test.go` | Zero-record S3 event unverified. | Added `TestS3Handler_EmptyRecordsEvent` asserting that 0 records returns `nil` with zero store/repo calls. |
| **QAF-05** | `Low` | `cmd/lambda/handler_test.go` | Complex punctuation passwords unverified in URI credential redaction. | Expanded `TestS3Handler_SanitizedErrorAndKeyValidation` to verify regex masking on complex password strings. |

### Phase 4H Architectural Invariants
1. **Direct Stream Ingestion (Decision P7)**: Streams directly from S3 via `ingest.Ingestor` without `/tmp` disk staging.
2. **Provenance Tracking (Decision P8)**: Sets `source.s3Uri` to canonical `s3://<bucket>/<key>` on all batches and sync logs.
3. **Warm Container Reuse (Decision P9)**: Lazy initialization of repository and pipeline under `sync.Mutex` with safe state reset on failure.
4. **Tri-State Error Taxonomy (Decision U6)**: Transient errors trigger Lambda retry; deterministic errors log and leave objects in `unprocessed/` for operator triage; audit write failures log warnings without aborting successful ingestion.

---

## 11. Phase 4I Desktop Telemetry & Module Path Hardening Log

During the implementation and review passes of the desktop driving adapter on branch `feat/phase-4i-desktop-notifier-and-module-path` (PR #91, synthesized in [`agent-docs/reviews/feat/phase-4i-desktop-notifier-and-module-path/REVIEW-FINDINGS.md`](../../agent-docs/reviews/feat/phase-4i-desktop-notifier-and-module-path/REVIEW-FINDINGS.md)), 10 findings across QA and code review were remediated:

| ID | Severity | Target File(s) | Architectural Impact Prior to Hardening | Implemented Resolution |
| :---: | :---: | :--- | :--- | :--- |
| **CRF-01** | `High` | `desktop/notifier.go` | Calling methods on a nil `(*WailsProgressNotifier)(nil)` dereferenced `n.mu`, causing a panic. | Added `if n == nil { return }` nil-receiver guards to `NotifyProgress`, `SetWailsCtx`, and `SetFileName`. |
| **CRF-02** | `Medium` | `desktop/notifier.go` | Zero-value struct instantiation without constructor resulted in nil function calls. | Added defensive fallbacks defaulting to `DefaultStageMapper` and `emitEvent` when uninitialized. |
| **CRF-03** | `Medium` | `desktop/notifier.go` | Wails context cancellation during application shutdown was unchecked, potentially emitting ghost events. | Added `if targetCtx.Err() != nil { return }` guard prior to event emission. |
| **CRF-04** | `Medium` | `desktop/notifier.go` | Negative `current` or `total` integer values passed through to JSON, causing negative UI percentages. | Clamped negative counts to zero (`if current < 0 { current = 0 }`, `if total < 0 { total = 0 }`). |
| **CRF-05** | `Low` | `desktop/notifier.go` | Package lacked top-level architectural context and concurrency invariants. | Added comprehensive package docstring documenting Decision P11, telemetry port role, and thread safety. |
| **QAF-01** | `Medium` | `desktop/notifier_test.go` | Zero-value struct literal safety was untested. | Added `TestWailsProgressNotifier_ZeroValueSafe` asserting safe no-op on uninitialized structs. |
| **QAF-02** | `Medium` | `desktop/notifier_test.go` | Canceled Wails runtime context behavior was untested. | Added `TestWailsProgressNotifier_CancelledWailsCtx` verifying canceled `wailsCtx` skips event emission. |
| **QAF-03** | `Medium` | `desktop/notifier_test.go` | Negative record count clamping was untested. | Added `TestWailsProgressNotifier_NegativeCountsClamped` validating normalization to 0. |
| **QAF-04** | `Medium` | `desktop/notifier_test.go` | Nil receiver methods were untested. | Added `TestWailsProgressNotifier_NilReceiverSafe` verifying nil receiver safety. |
| **QAF-05** | `Low` | `desktop/frontend/src/types/events.ts` | The `phase` property in `ProgressEvent` was an untyped string, lacking compile-time auto-completion. | Added `ProgressPhase` string union type while preserving open string compatibility. |

### Phase 4I Architectural Invariants
1. **Desktop Module Path Rename (Decision P11)**: Renamed `desktop/go.mod` to `module babylon/dataloader/desktop` to satisfy Go internal package import boundaries while preserving `replace babylon/dataloader => ../`.
2. **Coarse Progress Telemetry (Decision P11 / ADR Divergence 4)**: Maps domain lifecycle stages (`StageParsing`, `StageSaving`, `StageComplete`) to coarse stages (`parsing`, `saving`, `complete`) over the Wails event bus (`ingest:progress`).
3. **Dual Frontend Compatibility**: Additively updated `desktop/frontend/src/components/ProgressPanel.tsx` to handle both hexagonal stages (`saving`, `complete`) and legacy stages (`upserting`, `done`), ensuring 100% backward compatibility with zero test regressions.
4. **Thread-Safe Telemetry Adapter**: `WailsProgressNotifier` guards all mutable context, filename, mapper, and emitter state with `sync.RWMutex`, validated under `go test -race` with 50 concurrent workers.

---

## 12. Phase 4J `port.SyncLogReader` & Desktop History DTO Implementation Log

Phase 4J introduces the driven consumer read port `port.SyncLogReader` (Decision P12) and the desktop history service with legacy JSON DTO mapping in `desktop/history.go` (PR #92, Issue #81).

| Component / File | Role | Architectural Impact | Verification Test |
| :--- | :--- | :--- | :--- |
| `internal/domain/port/sync_log_reader.go` | Consumer Read Port | Segregates query access (`ListSyncLogs`) from command persistence (`port.Repository`). Adheres to interface segregation principles. | `TestPortContracts_SyncLogReader` |
| `desktop/history.go` (`var _ port.SyncLogReader = (*mongo.Repository)(nil)`) | Compile Assertion | Statically verifies that storage adapter satisfies the consumer read port at compile time. | Compile gate & `go test` |
| `desktop/history.go` (`historyEntry`) | Legacy DTO Mapping | Omits database `id` / `_id` (Divergence 10), provides dual legacy aliases (`syncTimestamp`, `recordsUploaded`), and formats UTC timestamps. | `TestToHistoryEntries_LegacyJSONGolden` |
| `desktop/history.go` (`historyService`) | Application Service | Wraps `port.SyncLogReader`, enforces default limit (`DefaultHistoryLimit = 100`) for non-positive values, and guards against nil receivers/readers. | `TestHistoryService_UsesDefaultLimit`, `TestHistoryService_PropagatesError` |

### Phase 4J Architectural Invariants
1. **Interface Segregation (Decision P12)**: Query operations (`ListSyncLogs`) are decoupled from mutation persistence (`SaveBatch`, `LogSync`).
2. **Compile-Time Conformance**: `*mongo.Repository` satisfies `port.SyncLogReader` without circular package dependencies or runtime casting.
3. **Legacy DTO Parity (Divergence 10)**: Omits database `id` / `_id` and populates dual legacy aliases (`syncTimestamp` mirroring `syncedAt`, `recordsUploaded` mirroring `upsertedCount`).
4. **Empty Array Guarantee**: Empty or nil input slices serialize to `[]` (not `null`) to maintain contract consistency with the React frontend.
5. **Quality & Coverage**: Desktop statement coverage reached **90.8%** with 100% test pass rate across `internal/domain/port/...`, `desktop/...`, and `desktop/frontend`.

---

## 13. Phase 4K Desktop App Cutover (`desktop/app.go`) Implementation Log

Phase 4K executes the driving adapter cutover for the Wails desktop application ([Issue #82](https://github.com/ajponte/babylon_data_loader/issues/82), Parent Issue #49). It eliminates legacy packages (`babylon/dataloader/datalake`, `datalake/model`, `storage`, and `csv`) in favor of hexagonal components: `mongo.Repository`, `pipeline.Pipeline`, `ingest.Ingestor` over in-place `localfs.Store` (Decision U3), `WailsProgressNotifier` (Decision P11), and `HistoryService` (Decision P12).

| Component / File | Role | Architectural Impact | Verification Test |
| :--- | :--- | :--- | :--- |
| `desktop/model/sync_log.go` | DTO Model | Defines `model.SyncLog` in package `model` preserving exact Wails TypeScript namespace contracts (`export namespace model { export class SyncLog }`) without database ID (Divergence 10). | `desktop/frontend/src/components/HistoryPanel.test.tsx` (7 Vitest tests) |
| `desktop/history.go` | Type Alias | Aliases `type HistoryEntry = model.SyncLog` to bridge domain sync log mapping to the Wails DTO model. | `TestToHistoryEntries_LegacyJSONGolden`, `TestHistoryService_UsesDefaultLimit` |
| `desktop/app.go` (`IngestFile`) | Driving Ingestion | In-place stream reading via `localfs.New(filepath.Dir(filePath))` and blank `ArchiveKey` (Decision U3). Source overrides support dropdown selection. | `TestApp_ProcessFile_Success`, `TestApp_IngestFile_Error`, `TestApp_IngestFile_Guards` |
| `desktop/app.go` (`GetHistory`) | Driving Query | Exposes `GetHistory() ([]model.SyncLog, error)` using `HistoryService.GetHistory` and `port.SyncLogReader`. | `TestApp_GetHistory_Parity`, `TestApp_GetHistory_Failure` |
| `desktop/app.go` (`OnStartup` / `RetryConnectDB`) | Lifecycle & Resilience | Connects `mongo.Repository`, wires `pipeline.Pipeline` with default extractors, and defensively closes degraded connections on retry. Injected factory hooks (`connectRepo`, `newStore`) enable hermetic testing. | `TestApp_OnStartup_Success`, `TestApp_OnStartup_Failure`, `TestApp_RetryConnectDB` |
| `desktop/frontend/wailsjs/...` | Auto-Generated Bindings | Regenerated via `wails generate module`, producing clean TypeScript definitions for `ingest.RunStats` and `model.SyncLog` with zero handwritten frontend edits. | `cd desktop/frontend && npx tsc --noEmit && npm test` (19 Vitest tests) |

### Phase 4K Architectural Invariants
1. **In-Place File Processing (Decision U3)**: Files are processed directly from their source path on disk via `localfs.New(filepath.Dir(filePath))`. `ArchiveKey` is intentionally empty, ensuring the local filesystem is never mutated or cleaned up.
2. **Zero Handwritten Frontend Alterations**: By placing the DTO in `desktop/model/sync_log.go`, Wails preserves `export namespace model { export class SyncLog }` matching React frontend expectations in `HistoryPanel.tsx` without modifying any TypeScript source files.
3. **Decoupled Persistence & Testability Hooks**: `App` struct accepts injectable factory functions `connectRepo` and `newStore`, defaulting to `mongo.NewRepository` and `localfs.New`. All unit tests execute hermetically without running a live MongoDB instance.
4. **Defensive Reconnection (`RetryConnectDB`)**: Closes any existing degraded repository handle prior to reconnecting and constructing a fresh ingestion pipeline.
5. **Quality & Coverage**: Desktop statement coverage achieved **87.4%** under `go test -race`, with 100% pass rate across the desktop Go test suite and all 19 frontend Vitest tests.

---

## 14. Phase 4L End-to-End Ingestion Integration Verification (`cmd/cli/integration_test.go`) Implementation Log

Phase 4L delivers the end-to-end integration test suite exercising the complete hexagonal architecture through the CLI entrypoint ([Issue #83](https://github.com/ajponte/babylon_data_loader/issues/83), Parent Issue #49). It executes real statement extractors (`chase`, `fidelity`, `synthetic`), real filesystem object storage (`internal/infra/storage/localfs`), and live containerized MongoDB persistence (`internal/infra/storage/mongo`).

| Component / File | Role | Architectural Impact | Verification Test |
| :--- | :--- | :--- | :--- |
| `cmd/cli/integration_test.go` (`validateIntegrationTarget`) | Safety Guard | Rejects `mongodb+srv://` connection strings (preventing Atlas pollution) and `datalake` database names (protecting production datalake). | `TestValidateIntegrationTarget` (9 table-driven test cases) |
| `cmd/cli/integration_test.go` (`setupCLIIntegration`) | Test Harness | Enforces `MONGO_IT_URI` skip guard, provisions ephemeral databases (`babylon_it_cli_<nano>`) with guaranteed `t.Cleanup` drops, and isolates env (`APP_ENVIRONMENT=local`). | `setupCLIIntegration` |
| `cmd/cli/integration_test.go` (`TestE2E_CLIIngest_RealMongo`) | E2E Integration | Ingests Chase, Fidelity, and Synthetic statements via CLI `run(ctx, []string{"ingest"}, d)`. Verifies collection writes, audit logs, and directory archival. | `TestE2E_CLIIngest_RealMongo` |
| `cmd/cli/integration_test.go` (`TestE2E_CLIIngest_RerunIsIdempotent`) | Idempotency & Lineage | Verifies duplicate runs yield 0 new records (`upsertedCount: 0`, `matchedCount: 2`) and preserves pre-existing `source.s3Uri` via dot-notation lineage updates (Decision E6). | `TestE2E_CLIIngest_RerunIsIdempotent` |
| `cmd/cli/integration_test.go` (`TestSyntheticCSV_RoutesToSyntheticDestination`) | Routing Isolation | Verifies synthetic statements route exclusively to `transactions_synthetic` while leaving Chase/Fidelity collections empty, preserving schema fields. | `TestSyntheticCSV_RoutesToSyntheticDestination` |
| `makefile` (`test-integration`) | Automation Seam | Expanded `test-integration` target to execute `./internal/infra/storage/...` and `./cmd/cli/...` under `//go:build integration`. | `make test-integration` |

### Phase 4L Architectural Invariants
1. **Hermetic CI Isolation**: All integration tests use `//go:build integration`. Standard CI and local checks (`make test-ci`, `go test ./...`) remain 100% network- and container-free.
2. **Ephemeral Database Isolation**: Tests never target persistent or shared database instances; each run dynamically provisions and drops `babylon_it_cli_<unixnano>`.
3. **Lineage Preservation (Decision E6)**: Dot-notation update models ensure pre-existing S3 URI lineage (`source.s3Uri`) is never overwritten by local filesystem ingestion.
4. **Target Isolation Protection**: Refuses execution if connection strings contain `mongodb+srv://` or if target database matches `datalake`.
5. **Quality & Pass Rate**: 100% pass rate across all 4 integration tests (4/4 passing, `0.557s`), 100% pass rate in GitHub Actions CI workflow `integration-test` job (`1m25s`), and 0 linter issues under `make check-quality`.





