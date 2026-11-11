package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"babylon/dataloader/internal/domain/port"
)

func TestWailsProgressNotifier_MapsStages(t *testing.T) {
	tests := []struct {
		name          string
		stage         port.ProgressStage
		current       int
		total         int
		expectedPhase string
		expectedMsg   string
		expectedCur   int64
		expectedTot   int64
	}{
		{
			name:          "StageParsing maps to parsing",
			stage:         port.StageParsing,
			current:       0,
			total:         0,
			expectedPhase: "parsing",
			expectedMsg:   "Parsing statement...",
			expectedCur:   0,
			expectedTot:   0,
		},
		{
			name:          "StageSaving maps to saving with record counts",
			stage:         port.StageSaving,
			current:       0,
			total:         120,
			expectedPhase: "saving",
			expectedMsg:   "Saving records to repository...",
			expectedCur:   0,
			expectedTot:   120,
		},
		{
			name:          "StageComplete maps to complete with full counts",
			stage:         port.StageComplete,
			current:       120,
			total:         120,
			expectedPhase: "complete",
			expectedMsg:   "Ingestion completed successfully",
			expectedCur:   120,
			expectedTot:   120,
		},
		{
			name:          "Unknown stage falls back to lowercase stage name",
			stage:         port.ProgressStage("VALIDATING"),
			current:       5,
			total:         10,
			expectedPhase: "validating",
			expectedMsg:   "",
			expectedCur:   5,
			expectedTot:   10,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var emittedName string
			var emittedPayload any

			emitter := func(ctx context.Context, name string, optionalData ...any) {
				emittedName = name
				if len(optionalData) > 0 {
					emittedPayload = optionalData[0]
				}
			}

			wailsCtx := context.Background()
			notifier := NewWailsProgressNotifier(
				wailsCtx,
				WithEmitter(emitter),
				WithFileName("Chase1234.csv"),
			)

			notifier.NotifyProgress(context.Background(), tc.stage, tc.current, tc.total)

			if emittedName != IngestProgressEvent {
				t.Fatalf("expected event %q, got %q", IngestProgressEvent, emittedName)
			}

			event, ok := emittedPayload.(ProgressEvent)
			if !ok {
				t.Fatalf("expected payload type ProgressEvent, got %T", emittedPayload)
			}

			if event.Phase != tc.expectedPhase {
				t.Errorf("expected phase %q, got %q", tc.expectedPhase, event.Phase)
			}
			if event.Message != tc.expectedMsg {
				t.Errorf("expected message %q, got %q", tc.expectedMsg, event.Message)
			}
			if event.CurrentRecord != tc.expectedCur {
				t.Errorf("expected currentRecord %d, got %d", tc.expectedCur, event.CurrentRecord)
			}
			if event.TotalRecords != tc.expectedTot {
				t.Errorf("expected totalRecords %d, got %d", tc.expectedTot, event.TotalRecords)
			}
			if event.FileName != "Chase1234.csv" {
				t.Errorf("expected fileName %q, got %q", "Chase1234.csv", event.FileName)
			}
		})
	}
}

func TestWailsProgressNotifier_ThreadSafe(t *testing.T) {
	var mu sync.Mutex
	var callCount int

	emitter := func(ctx context.Context, name string, optionalData ...any) {
		mu.Lock()
		defer mu.Unlock()
		callCount++
	}

	wailsCtx := context.Background()
	notifier := NewWailsProgressNotifier(wailsCtx, WithEmitter(emitter))

	const goroutines = 25
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Goroutines updating context and filename
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				notifier.SetFileName(fmt.Sprintf("file_%d_%d.csv", id, j))
				notifier.SetWailsCtx(context.Background())
			}
		}(i)
	}

	// Goroutines sending progress notifications
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			stages := []port.ProgressStage{port.StageParsing, port.StageSaving, port.StageComplete}
			for j := 0; j < iterations; j++ {
				stage := stages[j%len(stages)]
				notifier.NotifyProgress(context.Background(), stage, j, iterations)
			}
		}(i)
	}

	wg.Wait()

	mu.Lock()
	finalCount := callCount
	mu.Unlock()

	expectedCalls := goroutines * iterations
	if finalCount != expectedCalls {
		t.Fatalf("expected %d emitted events, got %d", expectedCalls, finalCount)
	}
}

func TestWailsProgressNotifier_ContextHandling(t *testing.T) {
	t.Run("nil wailsCtx and nil call ctx does not emit or panic", func(t *testing.T) {
		var emitted bool
		emitter := func(ctx context.Context, name string, optionalData ...any) {
			emitted = true
		}

		notifier := NewWailsProgressNotifier(nil, WithEmitter(emitter))
		notifier.NotifyProgress(nil, port.StageParsing, 0, 0)

		if emitted {
			t.Fatal("expected no emission with nil contexts")
		}
	})

	t.Run("pre-canceled context does not emit", func(t *testing.T) {
		var emitted bool
		emitter := func(ctx context.Context, name string, optionalData ...any) {
			emitted = true
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		notifier := NewWailsProgressNotifier(context.Background(), WithEmitter(emitter))
		notifier.NotifyProgress(ctx, port.StageParsing, 0, 0)

		if emitted {
			t.Fatal("expected no emission with canceled context")
		}
	})

	t.Run("fallback to call ctx when wailsCtx is nil", func(t *testing.T) {
		var capturedCtx context.Context
		emitter := func(ctx context.Context, name string, optionalData ...any) {
			capturedCtx = ctx
		}

		type keyType string
		const testKey keyType = "test"
		callCtx := context.WithValue(context.Background(), testKey, "val")

		notifier := NewWailsProgressNotifier(nil, WithEmitter(emitter))
		notifier.NotifyProgress(callCtx, port.StageParsing, 0, 0)

		if capturedCtx == nil || capturedCtx.Value(testKey) != "val" {
			t.Fatalf("expected fallback to call context, got %v", capturedCtx)
		}
	})
}

func TestWailsProgressNotifier_CustomStageMapper(t *testing.T) {
	var emittedProgress ProgressEvent
	emitter := func(ctx context.Context, name string, optionalData ...any) {
		if len(optionalData) > 0 {
			emittedProgress, _ = optionalData[0].(ProgressEvent)
		}
	}

	customMapper := func(stage port.ProgressStage, cur, tot int) (string, string) {
		return "custom_" + strings.ToLower(string(stage)), "custom description"
	}

	notifier := NewWailsProgressNotifier(
		context.Background(),
		WithEmitter(emitter),
		WithStageMapper(customMapper),
	)

	notifier.NotifyProgress(context.Background(), port.StageSaving, 10, 50)

	if emittedProgress.Phase != "custom_saving" {
		t.Errorf("expected phase custom_saving, got %q", emittedProgress.Phase)
	}
	if emittedProgress.Message != "custom description" {
		t.Errorf("expected message custom description, got %q", emittedProgress.Message)
	}
}

func TestWailsProgressNotifier_DefaultEmitter(t *testing.T) {
	// Tests that the constructor defaults emit to emitEvent without panic
	notifier := NewWailsProgressNotifier(context.Background())
	if notifier.emit == nil {
		t.Fatal("expected default emit function to be non-nil")
	}
}

func TestWailsProgressNotifier_ZeroValueSafe(t *testing.T) {
	// Assert zero-value struct with nil emitter and mapper does not panic
	var notifier WailsProgressNotifier
	// With nil contexts, should safely return without panic
	notifier.NotifyProgress(nil, port.StageParsing, 0, 0)

	// With context, should fallback to defaults without panic
	ctx := context.Background()
	notifier.NotifyProgress(ctx, port.StageParsing, 0, 0)
}

func TestWailsProgressNotifier_CancelledWailsCtx(t *testing.T) {
	var emitted bool
	emitter := func(ctx context.Context, name string, optionalData ...any) {
		emitted = true
	}

	wailsCtx, cancel := context.WithCancel(context.Background())
	cancel() // canceled Wails runtime context

	notifier := NewWailsProgressNotifier(wailsCtx, WithEmitter(emitter))
	notifier.NotifyProgress(context.Background(), port.StageParsing, 0, 0)

	if emitted {
		t.Fatal("expected no emission when wailsCtx is canceled")
	}
}

func TestWailsProgressNotifier_NegativeCountsClamped(t *testing.T) {
	var emittedProgress ProgressEvent
	emitter := func(ctx context.Context, name string, optionalData ...any) {
		if len(optionalData) > 0 {
			emittedProgress, _ = optionalData[0].(ProgressEvent)
		}
	}

	notifier := NewWailsProgressNotifier(context.Background(), WithEmitter(emitter))
	notifier.NotifyProgress(context.Background(), port.StageParsing, -10, -50)

	if emittedProgress.CurrentRecord != 0 {
		t.Errorf("expected currentRecord clamped to 0, got %d", emittedProgress.CurrentRecord)
	}
	if emittedProgress.TotalRecords != 0 {
		t.Errorf("expected totalRecords clamped to 0, got %d", emittedProgress.TotalRecords)
	}
}

func TestWailsProgressNotifier_NilReceiverSafe(t *testing.T) {
	var notifier *WailsProgressNotifier
	// None of these should panic on a nil receiver
	notifier.NotifyProgress(context.Background(), port.StageParsing, 0, 0)
	notifier.SetWailsCtx(context.Background())
	notifier.SetFileName("test.csv")
}
