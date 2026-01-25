package datalake

import (
	"encoding/json"
	"log/slog"
)

// Stats holds statistics about the file processing.
type Stats struct {
	TotalFiles     int               `json:"totalFiles"`
	ProcessedFiles int               `json:"processedFiles"`
	FailedFiles    int               `json:"failedFiles"`
	Failures       map[string]string `json:"failures"`
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

// Log prints the final statistics to the provided logger in JSON format.
func (s *Stats) Log(logger *slog.Logger) {
	jsonData, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		logger.Error("Failed to marshal stats to JSON", "error", err)
		return
	}
	logger.Info("--- Ingestion Stats (JSON) ---")
	logger.Info(string(jsonData))
	logger.Info("------------------------------")
}
