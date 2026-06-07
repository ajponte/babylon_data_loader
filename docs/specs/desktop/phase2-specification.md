# Phase 2 Specification: Wails Desktop App Setup & Bindings

> [!NOTE]
> **The Lead**: This specification details the Phase 2 implementation on the `wails-phase-two` branch. It covers scaffolding the Wails desktop application framework under the `desktop/` directory, defining persistent MongoDB lifecycle management, configuring Go method bindings for file ingestion (`IngestFile`) and connection retry (`RetryConnectDB`), and setting up safe, PII-compliant progress and log bridges.

---

## 1. Directory Structure & Scaffolding

The Wails desktop application structure was scaffolded using the Wails CLI (React + TypeScript template). The layout integrates with the repository root as follows:

```text
babylon_data_loader/
├── go.mod                        # Root module babylon/dataloader
├── go.work                       # Go multi-module workspace
└── desktop/                      # Wails desktop application root (Phase 2)
    ├── wails.json                # Wails v2 project configuration
    ├── go.mod                    # Sub-module desktop
    ├── main.go                   # Wails entry point & log handler registration
    ├── app.go                    # App struct, startup lifecycle, and UI bindings
    ├── progress.go               # WailsProgressReporter implementation
    ├── logbridge.go              # PII-compliant WailsLogHandler implementation
    ├── app_test.go               # Desktop application unit tests
    └── frontend/                 # React + TS + Vite frontend skeleton
```

A Go workspace file [go.work](../../../go.work) was introduced at the repository root to group both modules (`.` and `./desktop`) for unified package resolution during local building and testing.

---

## 2. Core Go Application & Bindings

### 2.1 Persistent Connection & Degraded State Management
The application establishes a single, persistent MongoDB client on startup:
- **`OnStartup`**: Loads configurations using [config.LoadConfig](../../../config/config.go) and attempts to connect via [ConnectToMongoDBFunc](../../../storage/mongo.go#L89).
- **Degraded Mode**: If the database is unreachable, the application continues to load instead of crashing, emitting a `db:status` event with `connected=false` and the error message to warn the user.
- **`RetryConnectDB`**: Exposes a binding for the UI to attempt reconnection after a startup failure. Emits a `db:status` event with `connected=true` on success.

### 2.2 Ingest File Binding
The `IngestFile` binding maps directly to the UI file picker:
- Receives the absolute path, data source, and account ID.
- Verifies database connection health before beginning.
- Copies the file to the configured `UnprocessedDir` before invoking the core ingestion client's `IngestCSVFile` method.

---

## 3. Communication Bridges & PII Safety

### 3.1 Progress Reporting
The `WailsProgressReporter` (defined in [progress.go](../../../desktop/progress.go)) implements `datalake.ProgressReporter`. It translates backend streaming events (`PhaseValidating`, `PhaseParsing`, `PhaseUpserting`, `PhaseMoving`, `PhaseDone`, and `PhaseFailed`) into Wails event broadcasts (`ingest:progress`) using Wails runtime APIs.

### 3.2 Log Forwarding & PII Scrubbing
A custom `WailsLogHandler` (defined in [logbridge.go](../../../desktop/logbridge.go)) implements `slog.Handler` to pipe structured logs directly to the frontend (`ingest:log` event):
- **Whitelisting**: The handler filters attributes using `isSafeKey` to prevent Personally Identifiable Information (PII) leakage, adhering to the repository's [pii_handling.md](../../pii_handling.md) rules.
- **Allowed Keys**: Only safe structural keys are forwarded (e.g. `file`, `dataSource`, `accountID`, `currentRecord`, `totalRecords`, `upsertedCount`, `duplicateCount`, `error`). Raw transaction descriptions, amounts, and bodies are strictly blocked.

### 3.3 Safe Event Emission in Test Environments
To prevent Wails runtime panics during unit tests where a mock context is used, an `emitEvent` helper (defined in [app.go](../../../desktop/app.go#L157)) checks if it is running in a test execution context (`flag.Lookup("test.v") != nil`) and skips Wails event emissions under test runs.

---

## 4. Reviews & Verifications

### 4.1 Tech Lead Verification
- **SOLID/DDD Compliance**: Clean separation of concerns. The desktop layer (`main.go`, `app.go`) serves as an orchestration shell, keeping business logic (in `datalake/` and `storage/`) decoupled.
- **Quality & Code Health**:
  - Implemented unit tests inside [app_test.go](../../../desktop/app_test.go) covering startup success, startup failure (degraded state), database reconnection retries, and ingestion binding flows under different states.
  - Verified compilation (`go build`) and ran test suites (`go test -v`), ensuring all test runs pass cleanly.

### 4.2 Documentation Engineer Verification
- **Inverted Pyramid Structure**: The document places the lead context at the beginning before exploring directory configurations, technical specifications, log scrubbing safety rules, and reviews.
- **Link Integrity**: Clickable markdown links direct users and agents to specific files and symbols within the repository using relative paths.
- **Ubiquitous Language**: Standardizes terms such as `Transaction`, `DataSource`, `UpsertStats`, `WailsLogHandler`, and `WailsProgressReporter`.

---

## 5. References & Links

- **Wails Integration Plan**: [wails-integration-plan.md](../../../agent-docs/wails-integration-plan.md)
- **Phase 1 Specification**: [phase1-specification.md](phase1-specification.md)
- **PII Handling Guidelines**: [pii_handling.md](../../pii_handling.md)
