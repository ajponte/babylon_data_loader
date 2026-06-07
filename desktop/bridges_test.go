package main

import (
	"babylon/dataloader/datalake"
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"
)

func TestWailsLogHandler_Enabled(t *testing.T) {
	tests := []struct {
		minLevel slog.Level
		logLevel slog.Level
		expected bool
	}{
		{slog.LevelInfo, slog.LevelDebug, false},
		{slog.LevelInfo, slog.LevelInfo, true},
		{slog.LevelInfo, slog.LevelWarn, true},
		{slog.LevelWarn, slog.LevelInfo, false},
		{slog.LevelWarn, slog.LevelWarn, true},
		{slog.LevelError, slog.LevelWarn, false},
		{slog.LevelError, slog.LevelError, true},
	}

	for _, tc := range tests {
		h := NewWailsLogHandler(tc.minLevel)
		ctx := context.Background()
		got := h.Enabled(ctx, tc.logLevel)
		if got != tc.expected {
			t.Errorf("minLevel=%v, logLevel=%v: expected Enabled=%v, got %v", tc.minLevel, tc.logLevel, tc.expected, got)
		}
	}
}

func TestWailsLogHandler_Handle(t *testing.T) {
	// Setup emitEvent interceptor
	origEmitEvent := emitEvent
	defer func() { emitEvent = origEmitEvent }()

	var emittedEventName string
	var emittedPayload interface{}

	emitEvent = func(ctx context.Context, name string, optionalData ...interface{}) {
		emittedEventName = name
		if len(optionalData) > 0 {
			emittedPayload = optionalData[0]
		}
	}

	// Wails context check: if context is nil, Handle does nothing.
	t.Run("nil wailsCtx should return nil without emitting", func(t *testing.T) {
		h := NewWailsLogHandler(slog.LevelInfo)
		// wailsCtx is nil
		emittedEventName = ""
		emittedPayload = nil

		record := slog.Record{
			Time:    time.Now(),
			Level:   slog.LevelInfo,
			Message: "test message",
		}
		err := h.Handle(context.Background(), record)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if emittedEventName != "" {
			t.Errorf("expected no event emission, got %s", emittedEventName)
		}
	})

	// With a non-nil wailsCtx:
	t.Run("should emit event with filtered attributes", func(t *testing.T) {
		h := NewWailsLogHandler(slog.LevelInfo)
		wailsCtx := context.Background()
		h.SetWailsCtx(wailsCtx)

		emittedEventName = ""
		emittedPayload = nil

		// Record containing some safe and some PII attributes
		record := slog.Record{
			Time:    time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC),
			Level:   slog.LevelInfo,
			Message: "processing transaction",
		}
		record.AddAttrs(
			slog.String("file", "test.csv"),
			slog.String("dataSource", "chase"),
			slog.String("accountID", "12345"),
			slog.String("description", "secret purchase"), // PII - should be filtered
			slog.Float64("balance", 123.45),               // PII - should be filtered
			slog.Float64("amount", 10.0),                  // PII - should be filtered
			slog.String("error", "some db error"),
			slog.Int("currentRecord", 5),
		)

		err := h.Handle(context.Background(), record)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if emittedEventName != "ingest:log" {
			t.Errorf("expected event 'ingest:log', got '%s'", emittedEventName)
		}

		frontendLog, ok := emittedPayload.(FrontendLog)
		if !ok {
			t.Fatalf("expected FrontendLog payload, got %T", emittedPayload)
		}

		if frontendLog.Message != "processing transaction" {
			t.Errorf("expected message 'processing transaction', got '%s'", frontendLog.Message)
		}
		if frontendLog.Level != "INFO" {
			t.Errorf("expected level 'INFO', got '%s'", frontendLog.Level)
		}
		if frontendLog.Time != "2026-06-06T12:00:00.000Z" {
			t.Errorf("expected time '2026-06-06T12:00:00.000Z', got '%s'", frontendLog.Time)
		}

		// Check whitelisted / blacklisted attributes
		if frontendLog.Attrs == nil {
			t.Fatal("expected non-nil attributes map")
		}

		expectedSafe := map[string]any{
			"file":          "test.csv",
			"dataSource":    "chase",
			"accountID":     "12345",
			"error":         "some db error",
			"currentRecord": 5,
		}

		for k, expectedVal := range expectedSafe {
			val, exists := frontendLog.Attrs[k]
			if !exists {
				t.Errorf("expected attribute %q to be present", k)
			} else {
				gotStr := fmt.Sprintf("%v", val)
				expStr := fmt.Sprintf("%v", expectedVal)
				if gotStr != expStr {
					t.Errorf("attribute %q: expected %s, got %s", k, expStr, gotStr)
				}
			}
		}

		// Ensure PII keys are NOT present
		piiKeys := []string{"description", "balance", "amount"}
		for _, k := range piiKeys {
			if _, exists := frontendLog.Attrs[k]; exists {
				t.Errorf("expected PII attribute %q to be filtered out, but it was present", k)
			}
		}
	})
}

func TestWailsLogHandler_WithAttrs_And_WithGroup(t *testing.T) {
	origEmitEvent := emitEvent
	defer func() { emitEvent = origEmitEvent }()

	var emittedPayload interface{}
	emitEvent = func(ctx context.Context, name string, optionalData ...interface{}) {
		if len(optionalData) > 0 {
			emittedPayload = optionalData[0]
		}
	}

	h := NewWailsLogHandler(slog.LevelInfo)
	h.SetWailsCtx(context.Background())

	// Test WithAttrs propagation
	hWithAttrs := h.WithAttrs([]slog.Attr{
		slog.String("dataSource", "synthetic"),
		slog.String("description", "PII description"), // should be filtered out
	})

	// Cast back to check internal state and properties
	h2, ok := hWithAttrs.(*WailsLogHandler)
	if !ok {
		t.Fatalf("expected WithAttrs to return *WailsLogHandler")
	}

	if len(h2.attrs) != 2 {
		t.Errorf("expected 2 pre-configured attributes, got %d", len(h2.attrs))
	}

	// Execute Handle using hWithAttrs to see if they propagate and filter
	record := slog.Record{
		Time:    time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC),
		Level:   slog.LevelInfo,
		Message: "msg with attrs",
	}
	// Add a record-level attribute as well
	record.AddAttrs(slog.String("file", "another.csv"))

	emittedPayload = nil
	err := hWithAttrs.Handle(context.Background(), record)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	frontendLog, ok := emittedPayload.(FrontendLog)
	if !ok {
		t.Fatalf("expected FrontendLog payload, got %T", emittedPayload)
	}

	if frontendLog.Attrs == nil {
		t.Fatal("expected non-nil attributes map")
	}

	if val, exists := frontendLog.Attrs["dataSource"]; !exists || val != "synthetic" {
		t.Errorf("expected 'dataSource' attribute to be 'synthetic', got %v", val)
	}

	if val, exists := frontendLog.Attrs["file"]; !exists || val != "another.csv" {
		t.Errorf("expected 'file' attribute to be 'another.csv', got %v", val)
	}

	if _, exists := frontendLog.Attrs["description"]; exists {
		t.Error("expected pre-configured PII attribute 'description' to be filtered out")
	}

	// Test WithGroup propagation
	hWithGroup := h.WithGroup("testgroup")
	h3, ok := hWithGroup.(*WailsLogHandler)
	if !ok {
		t.Fatalf("expected WithGroup to return *WailsLogHandler")
	}

	if len(h3.groups) != 1 || h3.groups[0] != "testgroup" {
		t.Errorf("expected groups to contain 'testgroup', got %v", h3.groups)
	}
}

func TestWailsProgressReporter_Report(t *testing.T) {
	origEmitEvent := emitEvent
	defer func() { emitEvent = origEmitEvent }()

	var emittedEventName string
	var emittedPayload interface{}

	emitEvent = func(ctx context.Context, name string, optionalData ...interface{}) {
		emittedEventName = name
		if len(optionalData) > 0 {
			emittedPayload = optionalData[0]
		}
	}

	// Test with nil context
	t.Run("nil context should not emit", func(t *testing.T) {
		reporter := NewWailsProgressReporter(nil)
		emittedEventName = ""
		emittedPayload = nil

		event := datalake.ProgressEvent{
			Phase:         "parsing",
			CurrentRecord: 10,
			TotalRecords:  100,
		}

		reporter.Report(event)

		if emittedEventName != "" {
			t.Errorf("expected no event emission for nil context, got %s", emittedEventName)
		}
	})

	// Test with non-nil context
	t.Run("valid context should emit ingest:progress event", func(t *testing.T) {
		ctx := context.Background()
		reporter := NewWailsProgressReporter(ctx)
		emittedEventName = ""
		emittedPayload = nil

		event := datalake.ProgressEvent{
			Phase:         "importing",
			CurrentRecord: 42,
			TotalRecords:  100,
		}

		reporter.Report(event)

		if emittedEventName != "ingest:progress" {
			t.Errorf("expected event name 'ingest:progress', got %q", emittedEventName)
		}

		gotEvent, ok := emittedPayload.(datalake.ProgressEvent)
		if !ok {
			t.Fatalf("expected datalake.ProgressEvent payload, got %T", emittedPayload)
		}

		if gotEvent.Phase != "importing" {
			t.Errorf("expected Phase 'importing', got %q", gotEvent.Phase)
		}
		if gotEvent.CurrentRecord != 42 {
			t.Errorf("expected CurrentRecord 42, got %d", gotEvent.CurrentRecord)
		}
		if gotEvent.TotalRecords != 100 {
			t.Errorf("expected TotalRecords 100, got %d", gotEvent.TotalRecords)
		}
	})
}
