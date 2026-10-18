package port

import "context"

// ProgressStage represents the execution phase in the ingestion lifecycle.
type ProgressStage string

const (
	// StageParsing indicates that input streams are being parsed and normalized.
	StageParsing ProgressStage = "PARSING"
	// StageSaving indicates that normalized batches are being persisted to storage.
	StageSaving ProgressStage = "SAVING"
	// StageComplete indicates that ingestion, persistence, and audit logging are completed.
	StageComplete ProgressStage = "COMPLETE"
)

// ProgressNotifier is the telemetry port for non-blocking progress updates.
type ProgressNotifier interface {
	// NotifyProgress emits progress telemetry for the given stage within the request context.
	NotifyProgress(ctx context.Context, stage ProgressStage, current int, total int)
}
