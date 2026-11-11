package main

import (
	"context"
	"strings"
	"sync"

	"babylon/dataloader/internal/domain/port"
)

// IngestProgressEvent is the Wails runtime event topic for file ingestion telemetry.
const IngestProgressEvent = "ingest:progress"

// ProgressEvent represents the payload emitted over the Wails event bus for "ingest:progress".
type ProgressEvent struct {
	Phase          string `json:"phase"`
	Message        string `json:"message,omitempty"`
	FileName       string `json:"fileName,omitempty"`
	CurrentRecord  int64  `json:"currentRecord,omitempty"`
	TotalRecords   int64  `json:"totalRecords,omitempty"`
	UpsertedCount  int64  `json:"upsertedCount,omitempty"`
	DuplicateCount int64  `json:"duplicateCount,omitempty"`
}

// StageMapper translates a domain ProgressStage to a frontend phase string and description.
type StageMapper func(stage port.ProgressStage, current, total int) (phase, message string)

// DefaultStageMapper maps domain stages to coarse stages (Decision P11 / ADR Divergence 4).
func DefaultStageMapper(stage port.ProgressStage, _, _ int) (string, string) {
	switch stage {
	case port.StageParsing:
		return "parsing", "Parsing statement..."
	case port.StageSaving:
		return "saving", "Saving records to repository..."
	case port.StageComplete:
		return "complete", "Ingestion completed successfully"
	default:
		return strings.ToLower(string(stage)), ""
	}
}

// NotifierOption configures a WailsProgressNotifier instance.
type NotifierOption func(*WailsProgressNotifier)

// WithStageMapper sets a custom stage mapper on the notifier.
func WithStageMapper(mapper StageMapper) NotifierOption {
	return func(n *WailsProgressNotifier) {
		if mapper != nil {
			n.mapper = mapper
		}
	}
}

// WithFileName sets the initial source filename for the progress notifier.
func WithFileName(fileName string) NotifierOption {
	return func(n *WailsProgressNotifier) {
		n.fileName = fileName
	}
}

// WithEmitter overrides the default event emission function for testing.
func WithEmitter(emitter func(ctx context.Context, name string, optionalData ...interface{})) NotifierOption {
	return func(n *WailsProgressNotifier) {
		if emitter != nil {
			n.emit = emitter
		}
	}
}

// WailsProgressNotifier adapts port.ProgressNotifier to the Wails runtime event bus.
type WailsProgressNotifier struct {
	mu       sync.RWMutex
	wailsCtx context.Context
	fileName string
	mapper   StageMapper
	emit     func(ctx context.Context, name string, optionalData ...interface{})
}

var _ port.ProgressNotifier = (*WailsProgressNotifier)(nil)

// NewWailsProgressNotifier creates a thread-safe progress notifier bound to a Wails context.
func NewWailsProgressNotifier(wailsCtx context.Context, opts ...NotifierOption) *WailsProgressNotifier {
	n := &WailsProgressNotifier{
		wailsCtx: wailsCtx,
		mapper:   DefaultStageMapper,
		emit:     emitEvent,
	}
	for _, opt := range opts {
		opt(n)
	}
	return n
}

// SetWailsCtx updates the Wails runtime context under a mutex.
func (n *WailsProgressNotifier) SetWailsCtx(ctx context.Context) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.wailsCtx = ctx
}

// SetFileName updates the target filename for telemetry events under a mutex.
func (n *WailsProgressNotifier) SetFileName(fileName string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.fileName = fileName
}

// NotifyProgress translates domain stage events and broadcasts them over the Wails event bus.
func (n *WailsProgressNotifier) NotifyProgress(ctx context.Context, stage port.ProgressStage, current int, total int) {
	if ctx != nil && ctx.Err() != nil {
		return
	}

	n.mu.RLock()
	targetCtx := n.wailsCtx
	fileName := n.fileName
	mapper := n.mapper
	emitter := n.emit
	n.mu.RUnlock()

	if targetCtx == nil {
		if ctx == nil {
			return
		}
		targetCtx = ctx
	}

	phase, message := mapper(stage, current, total)
	event := ProgressEvent{
		Phase:         phase,
		Message:       message,
		FileName:      fileName,
		CurrentRecord: int64(current),
		TotalRecords:  int64(total),
	}

	emitter(targetCtx, IngestProgressEvent, event)
}
