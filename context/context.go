// Package context holds methods for pushing new records to the Babylon data lake.
package context

import (
	"context"
	"log/slog"
)

type contextKey struct{}

// WithLogger creates a new context with the provided logger.
func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, contextKey{}, logger)
}

// LoggerFromContext retrieves the logger from the context.
// It returns a default logger if no logger is found.
func LoggerFromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(contextKey{}).(*slog.Logger); ok {
		return logger
	}

	return slog.Default()
}