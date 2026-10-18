package port

// ProgressNotifier is the telemetry port for non-blocking progress updates.
type ProgressNotifier interface {
	// NotifyProgress emits progress telemetry for the given stage.
	NotifyProgress(stage string, current int, total int)
}
