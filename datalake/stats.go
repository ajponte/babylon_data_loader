package datalake

import (
	"fmt"
	"log/slog"
)

// Stats holds statistics about the file processing.
type Stats struct {
	TotalFiles     int
	ProcessedFiles int
	FailedFiles    int
	Failures       map[string]string
}

// NewStats creates and initializes a new Stats object.
func NewStats() *Stats {
	return &Stats{
		Failures: make(map[string]string),
	}
}

// AddFailure records a failed file and its reason.
func (s *Stats) AddFailure(file, reason string) {
	s.FailedFiles++
	s.Failures[file] = reason
}

// IncrementProcessed increments the count of successfully processed files.
func (s *Stats) IncrementProcessed() {
	s.ProcessedFiles++
}

// Log prints the final statistics to the provided logger.
func (s *Stats) Log(logger *slog.Logger) {
	logger.Info("--- Ingestion Stats ---")
	logger.Info(fmt.Sprintf("Total files found: %d", s.TotalFiles))
	logger.Info(fmt.Sprintf("Files processed: %d", s.ProcessedFiles))
	logger.Info(fmt.Sprintf("Files failed/skipped: %d", s.FailedFiles))
	if s.FailedFiles > 0 {
		logger.Info("Failed files:")
		for file, reason := range s.Failures {
			logger.Info(fmt.Sprintf("- %s: %s", file, reason))
		}
	}
	logger.Info("-----------------------")
}
