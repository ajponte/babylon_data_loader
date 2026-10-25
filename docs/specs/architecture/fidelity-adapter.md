# Fidelity Portfolio Ingestion Adapter Architecture Specification

This document specifies the architectural patterns, stream handling, and data normalization invariants of the Fidelity Portfolio Ingestion Adapter located in `babylon_data_loader/internal/infra/sources/fidelity/`.

---

## 1. Overview & Architectural Boundaries

Fidelity brokerage portfolio exports represent point-in-time holdings snapshots across one or more accounts. The adapter transforms heterogeneous, tabular portfolio exports into strongly-typed `model.FidelityPosition` domain records, stamps every position in a non-empty batch with the UTC snapshot instant extracted from the `"Date downloaded ..."` footer, and encapsulates them into an `IngestionBatch`.

```mermaid
flowchart TD
    subgraph FidelityPkg ["Fidelity Ingestion Package (internal/infra/sources/fidelity)"]
        EXT["Extractor (extractor.go)<br/>• validateParams & nil-context guard<br/>• stripUTF8BOM (non-EOF error propagation)<br/>• defaultPositionsCapacity = 64<br/>• Embedded _ &quot;time/tzdata&quot;"]
        HDR["readHeader & validateHeaderSignatures<br/>• Schema signature validation<br/>• Column index resolution"]
        FTR["Date Downloaded Footer Parser (extractor.go)<br/>• matchDateDownloadedField (hasDateDownloadedPrefix, joinNonEmptyFooterFields)<br/>• parseDateDownloaded (stripDateDownloadedPrefix, stripEasternSuffix, normalizeMeridiem)<br/>• resolveDateLocation (America/New_York / EDT / UTC)"]
        PRS["Dedicated Parser Engine (parser.go)<br/>• rowParser.parse<br/>• isDisclaimerText (filtering notices)<br/>• normalizeHoldingSymbol (cash sweep mapping)<br/>• parseFinancialNumber (Unicode space stripping)"]
        FIN["Batch Finalizer (extractor.go)<br/>• finalizePositions (stamps AsOfDate or returns model.ErrMissingAsOfDate)<br/>• resolveBatchAccountID"]
    end

    EXT --> HDR
    HDR --> FTR
    HDR --> PRS
    FTR --> FIN
    PRS --> FIN
    FIN --> BATCH["model.IngestionBatch (model.DestFidelityPosition)"]
```

---

## 2. Real-World Export Quirks & Handling

Fidelity portfolio CSV exports exhibit distinctive formatting characteristics that require specialized parsing defenses:

### 1. `"Date downloaded "` Footer Detection, Parsing & Batch-Level `AsOfDate` Stamping
Fidelity portfolio exports do not include a snapshot date column on individual position rows; instead, the capture timestamp is emitted in a trailing footer line formatted as `"Date downloaded MMM-DD-YYYY H:MM a.m./p.m. ET"` (e.g., `"Date downloaded Oct-01-2025 9:13 p.m ET"`). [`extractor.go`](../../../internal/infra/sources/fidelity/extractor.go) extracts, normalizes, and stamps this timestamp across the batch:

- **Footer Invariants**:
  1. **First Footer Wins**: Only the first `"Date downloaded "` footer encountered in the stream is parsed (`asOfDate.IsZero()`); any subsequent footer lines are ignored.
  2. **Never Emitted as a Position**: When `matchDateDownloadedField` matches a row, `parsePositions` records `asOfDate` and immediately `continue`s before invoking `rowParser.parse`.
  3. **Mandatory on Non-Empty Batches**: In `finalizePositions`, if `len(positions) > 0` and `asOfDate.IsZero()`, extraction fails with `model.ErrMissingAsOfDate`.
  4. **Optional on Empty / Header-Only Streams**: If a stream is empty, header-only, or contains only disclaimers (`len(positions) == 0`), `finalizePositions` succeeds with an empty batch even when no footer is present.
- **Detection (`matchDateDownloadedField`, `hasDateDownloadedPrefix`, `joinNonEmptyFooterFields`)**: Scans row cells, trimming quotes, commas, spaces, tabs, and newlines (`dateTrimCutset = "\", \t\r\n"`), and checks the first non-empty field case-insensitively for the `"date downloaded"` keyword (`dateDownloadedKeyword`) followed by end-of-string or a delimiter (`' '`, `'\t'`, `':'`, `','`). When matched, `joinNonEmptyFooterFields` rejoins any comma-split unquoted trailing cells in the row with `" "`.
- **Parsing & Timezone Resolution (`parseDateDownloaded`, `stripDateDownloadedPrefix`, `stripEasternSuffix`, `normalizeMeridiem`, `resolveDateLocation`)**:
  - Imports `_ "time/tzdata"` to embed the IANA timezone database directly in the binary for container/serverless portability.
  - `stripDateDownloadedPrefix` validates and removes the `"Date downloaded"` prefix; if the remaining date string is empty, returns an error wrapping `ErrInvalidDateDownloaded`.
  - `stripEasternSuffix` strips `"EDT"`, `"EST"`, or `"ET"` suffixes (case-insensitively) and resolves the location via `resolveDateLocation`: loads `"America/New_York"` (falling back to a fixed `"EDT"` `UTC-4` zone if unavailable) when an Eastern suffix is present, or `time.UTC` otherwise.
  - `normalizeMeridiem` normalizes Fidelity's varied meridiem tokens (`"p.m."`, `"a.m."`, `"P.M."`, `"A.M."`, `"p.m"`, `"a.m"`, `"P.M"`, `"A.M"`, `"pm"`, `"am"`) to standard Go `"PM"` / `"AM"`, and internal whitespace runs are collapsed via `strings.Join(strings.Fields(dateStr), " ")`.
  - Parses against `"Jan-02-2006 3:04 PM"` (with fallback to single-digit day `"Jan-2-2006 3:04 PM"`, wrapping `ErrInvalidDateDownloaded` and `errSingle` on failure) and normalizes the resulting instant to `.UTC()`.
- **Batch-Level Stamping (`finalizePositions`)**: Assigns the parsed UTC `asOfDate` to `positions[i].AsOfDate` for every extracted position in the slice.

### 2. UTF-8 Byte Order Mark (BOM) Detection & Propagation
Fidelity CSV files routinely start with a 3-byte UTF-8 BOM (`0xef, 0xbb, 0xbf`). [`stripUTF8BOM`](../../../internal/infra/sources/fidelity/extractor.go) inspects the stream header:
- Surfaces non-EOF I/O peek errors immediately.
- Treats graceful EOF (empty file) as a non-error.
- Discards the BOM bytes without corrupting header detection.

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

### 3. Multi-Line Legal Disclaimers & Summary Totals Filtering
Fidelity statements include multi-line legal notices, quote delays, and summary lines (e.g. `"Account Total"`, `"Ending Value"`) embedded in the CSV body. [`isDisclaimerText`](../../../internal/infra/sources/fidelity/parser.go) identifies and skips these lines:
- Skips legal preamble: `"Please note"`, `"The data and information"`, `"Brokerage services"`.
- Skips summary rows: `"Total"`, `"Account Total"`, `"Ending Value"`, `"Portfolio Total"`.
- Skips numbered footnotes and footnote annotations: `"* "`, `"** "`, `"1. "`, `"2. "`.

### 4. Core Cash Sweep Normalization
Fidelity represents cash sweeps using non-standard ticker markers:
- `"**"` (cash holding indicator)
- `"FCASH"` (Fidelity cash reserve)
- `"CORE"` (Core sweep position)

The parser normalizes these tokens to canonical `"CASH"`, while carefully preserving valid tickers that carry footnote asterisks (e.g. `SPAXX**` -> preserved as `SPAXX**` or resolved to ticker).

```go
func normalizeSymbol(symbol, description string) (string, bool) {
	symUpper := strings.ToUpper(strings.TrimSpace(symbol))
	if symUpper == "**" || symUpper == "FCASH" || symUpper == "CORE" {
		return "CASH", true
	}
	if symUpper != "" {
		return symUpper, true
	}
	descUpper := strings.ToUpper(description)
	if strings.Contains(descUpper, "HELD IN") || strings.Contains(descUpper, "CORE") || strings.Contains(descUpper, "CASH") {
		return "CASH", true
	}
	return "", false
}
```

### 5. Preservation of Mutual Funds with "Total" in Description
Earlier parsers dropped legitimate broad-market index funds (e.g. `VTI` Vanguard Total Stock Market, `BND` Total Bond Market, `TTE` TotalEnergies) because their descriptions contained `"Total"`. The parser decouples symbol evaluation: if `symbol != ""` and the row is not a summary row, the record is unconditionally preserved.

---

## 3. Schema Signature Validation

To prevent arbitrary non-Fidelity CSVs from being ingested as zero-dollar cash positions, `validateHeaderSignatures` validates two mandatory schema signatures:
1. Symbol Column Group: Requires at least one alias matching `{"symbol", "ticker"}`.
2. Valuation Column Group: Requires at least one alias matching `{"current value", "market value", "current balance"}`.

If either group is missing, the adapter rejects the statement immediately with `ErrMissingRequiredColumns`.

---

## 4. Single-Account Batch Inference (`resolveBatchAccountID`)

Fidelity portfolio files frequently represent single-account statements where account numbers appear on every record. The adapter verifies consistency across all rows:
- If all rows reference a single account (e.g. `"Z12345678"`), that account ID is assigned to the `IngestionBatch.AccountID`.
- If accounts are mixed (multi-account portfolio export), the batch retains the caller's `meta.AccountID`.

---

## 5. Exported Sentinel Errors

[`extractor.go`](../../../internal/infra/sources/fidelity/extractor.go) exports adapter-level sentinels and wraps the domain-level `model.ErrMissingAsOfDate` sentinel when a non-empty statement lacks a `"Date downloaded "` footer:

```go
// Adapter-level sentinels in internal/infra/sources/fidelity/extractor.go
var (
	ErrNilReader              = errors.New("reader cannot be nil")
	ErrMissingRequiredColumns = errors.New("fidelity CSV missing required header columns")
	ErrInvalidDateDownloaded  = errors.New("invalid fidelity Date downloaded footer")
)

// Domain-level sentinel in internal/domain/model/fidelity_position.go
// Returned by finalizePositions when len(positions) > 0 and asOfDate.IsZero()
var (
	ErrMissingAsOfDate = errors.New("fidelity snapshot date (asOfDate) is required")
)
```
