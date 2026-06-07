package main

import (
	"babylon/dataloader/datalake"
	"context"
)

// WailsProgressReporter implements datalake.ProgressReporter.
type WailsProgressReporter struct {
	ctx context.Context
}

// NewWailsProgressReporter creates a new WailsProgressReporter.
func NewWailsProgressReporter(ctx context.Context) *WailsProgressReporter {
	return &WailsProgressReporter{ctx: ctx}
}

// Report sends a progress event to the Wails frontend.
func (r *WailsProgressReporter) Report(event datalake.ProgressEvent) {
	if r.ctx != nil {
		emitEvent(r.ctx, "ingest:progress", event)
	}
}
