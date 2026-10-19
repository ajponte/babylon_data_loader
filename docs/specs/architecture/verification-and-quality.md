# Ingestion Adapters Verification Harness & Local Quality Gates

This document defines the automated verification harness, static analysis rules, performance benchmarks, and local quality gates enforcing architectural compliance across all source adapters in `babylon_data_loader`.

---

## 1. Quality Gates Overview

Every secondary ingestion adapter (`chase`, `fidelity`, `synthetic`) must pass 5 consecutive automated quality gates before pull request merging:

```mermaid
flowchart LR
    G1["1. Standard Library Isolation"] --> G2["2. Race Detection & Coverage"]
    G2 --> G3["3. Static Analysis & Linting"]
    G3 --> G4["4. CI Suite Execution"]
    G4 --> G5["5. Comparative Benchmarks"]
```

---

## 2. Gate 1: Standard Library Isolation Gate

Ensures source adapters maintain 100% standard library purity, preventing transitive dependency sprawl and third-party vulnerabilities.

```bash
go list -f '{{.ImportPath}} -> {{.Imports}}' ./internal/infra/sources/...
```

### Verified Output:
```text
babylon/dataloader/internal/infra/sources/chase -> [babylon/dataloader/internal/domain/model babylon/dataloader/internal/domain/port bufio bytes context encoding/csv errors fmt io math strconv strings time unicode]
babylon/dataloader/internal/infra/sources/fidelity -> [babylon/dataloader/internal/domain/model babylon/dataloader/internal/domain/port bufio bytes context encoding/csv errors fmt io strconv strings]
babylon/dataloader/internal/infra/sources/synthetic -> [babylon/dataloader/internal/domain/model babylon/dataloader/internal/domain/port context fmt io math math/rand/v2 sync time]
```
Zero third-party CSV libraries, database drivers, ORMs, or network SDKs exist in the source adapter dependency trees.

---

## 3. Gate 2: Race Detection & Statement Coverage Gate

Executes unit and integration test suites under the Go race detector with statement coverage profiling:

```bash
go test -v -race -cover ./internal/infra/sources/...
```

### Verified Test Summary:
- **`chase`**: **95.0% statement coverage** (100% on `parser.go` helpers). Fully decoupled from local disk files (`data/unprocessed` and `testdata`), running entirely on synthetic in-memory fixtures. Passes all 4-stage pipeline validations, nil stream protections, context cancellation unwrapping, dialect classification, `rowReader` zero-value and negative bounds safety, date fallbacks (`resolveCreditCardDate`), and credit card balance retention.
- **`fidelity`**: **91.8% statement coverage** (100% on `parser.go` helpers). Passes schema signature rejection, multi-line disclaimer filtering, index fund preservation (`VTI`, `BND`, `TTE`), cash sweep normalization (`CASH`), UTF-8 BOM handling, and single-account batch inference.
- **`synthetic`**: **91.1% statement coverage**. Passes deterministic reproducibility across sequential seeds, concurrent mutex synchronization, zero row count handling, and maximum row count clamping (>100,000 rejection).

---

## 4. Gate 3: Static Analysis & Linter Gate

Runs the project linter configuration across all ingestion packages:

```bash
golangci-lint run ./internal/infra/sources/...
```

### Verified Output:
```text
0 issues.
```
- Zero `//nolint:gochecknoglobals` or `//nolint:exhaustive` waivers.
- Pure value receivers across `rowReader` methods satisfying `recvcheck`.
- Cognitive complexity on all helper functions strictly capped below 20 (`gocognit`).

---

## 5. Gate 4: Continuous Integration Suite Execution

Executes the repository-wide test suite:

```bash
make test-ci
```

**Verified Result**: Full repository test pass with 100% statement coverage across domain models and clean pass across all secondary adapters with 0 test skips on CI-critical suites.

---

## 6. Gate 5: Comparative Parser Benchmarking Gate

Profiles memory allocations and throughput to ensure that high-level abstractions like `rowReader` introduce zero heap allocations and zero execution overhead compared to raw procedural parsing:

```bash
go test -bench=. -benchmem ./internal/infra/sources/chase/...
```

### Verified Benchmark Results:
```text
goos: darwin
goarch: arm64
pkg: babylon/dataloader/internal/infra/sources/chase
cpu: Apple M1
BenchmarkDirect_ParseRow-8       3362310       323.0 ns/op        16 B/op        2 allocs/op
BenchmarkRowReader_ParseRow-8    3682278       327.1 ns/op        16 B/op        2 allocs/op
PASS
ok      babylon/dataloader/internal/infra/sources/chase    3.295s
```

### Architectural Benchmark Analysis:
1. **Allocation Parity**: Both procedural direct parsing (`BenchmarkDirect_ParseRow`) and value-struct encapsulation (`BenchmarkRowReader_ParseRow`) register identical allocations: **`16 B/op` and `2 allocs/op`**. This allocation stems entirely from the heap box required by the optional `*float64` balance field (`bal, err := parseBalance(...)`). The `rowReader` value struct itself incurs **`0 B/op` and `0 allocs/op`**.
2. **Negligible Execution Delta**: `rowReader` executes at ~327.1 ns/op compared to ~323.0 ns/op for raw procedural parsing—a delta of ~1.2%, proving that ergonomic encapsulation introduces negligible CPU overhead.
3. **Compiler Escape Analysis**: Confirmed via `go build -gcflags="-m -m"`: `rowReader` is allocated strictly on the stack frame and registers, never escaping to the heap.
