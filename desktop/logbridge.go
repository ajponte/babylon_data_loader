package main

import (
	"context"
	"log/slog"
)

// WailsLogHandler implements slog.Handler to pipe log records to the Wails frontend.
type WailsLogHandler struct {
	wailsCtx context.Context
	level    slog.Level
	attrs    []slog.Attr
	groups   []string
}

// NewWailsLogHandler creates a new WailsLogHandler with the specified minimum log level.
func NewWailsLogHandler(level slog.Level) *WailsLogHandler {
	return &WailsLogHandler{
		level: level,
	}
}

// SetWailsCtx updates the Wails context used for emitting events.
func (h *WailsLogHandler) SetWailsCtx(ctx context.Context) {
	h.wailsCtx = ctx
}

// Enabled reports whether the handler handles records at the given level.
func (h *WailsLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.level
}

// FrontendLog represents the JSON structure sent to the Wails frontend.
type FrontendLog struct {
	Time    string         `json:"time"`
	Level   string         `json:"level"`
	Message string         `json:"message"`
	Attrs   map[string]any `json:"attrs,omitempty"`
}

// Handle processes the log record, filters out PII, and emits the ingest:log event.
func (h *WailsLogHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.wailsCtx == nil {
		return nil
	}

	attrs := make(map[string]any)

	// Add pre-configured attributes
	for _, attr := range h.attrs {
		if isSafeKey(attr.Key) {
			attrs[attr.Key] = attr.Value.Any()
		}
	}

	// Add record attributes
	r.Attrs(func(attr slog.Attr) bool {
		if isSafeKey(attr.Key) {
			attrs[attr.Key] = attr.Value.Any()
		}
		return true
	})

	logEvent := FrontendLog{
		Time:    r.Time.Format("2006-01-02T15:04:05.000Z"),
		Level:   r.Level.String(),
		Message: r.Message,
	}
	if len(attrs) > 0 {
		logEvent.Attrs = attrs
	}

	emitEvent(h.wailsCtx, "ingest:log", logEvent)
	return nil
}

// WithAttrs returns a new slog.Handler containing the given attributes.
func (h *WailsLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newAttrs := append([]slog.Attr(nil), h.attrs...)
	newAttrs = append(newAttrs, attrs...)
	return &WailsLogHandler{
		wailsCtx: h.wailsCtx,
		level:    h.level,
		attrs:    newAttrs,
		groups:   h.groups,
	}
}

// WithGroup returns a new slog.Handler with the given group name.
func (h *WailsLogHandler) WithGroup(name string) slog.Handler {
	newGroups := append([]string(nil), h.groups...)
	newGroups = append(newGroups, name)
	return &WailsLogHandler{
		wailsCtx: h.wailsCtx,
		level:    h.level,
		attrs:    h.attrs,
		groups:   newGroups,
	}
}

// isSafeKey returns true if the log metadata key is safe (non-PII).
func isSafeKey(key string) bool {
	switch key {
	case "file", "fileName", "file_name", "sink", "unprocessedDir",
		"dataSource", "data_source", "accountID", "accountId", "account_id",
		"error", "err", "totalFiles", "processedFiles", "failedFiles", "skippedFiles",
		"upsertedCount", "matchedCount", "modifiedCount", "duplicateCount",
		"currentRecord", "totalRecords", "phase", "message", "status", "connected":
		return true
	default:
		return false
	}
}
