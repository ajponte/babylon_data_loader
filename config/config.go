package config

import (
	"time"
)

// Config holds the application configuration.
type Config struct {
	MongoURI           string
	UnprocessedDir     string
	ProcessedDir       string
	MoveProcessedFiles bool
	SyntheticDataDir   string
	SyntheticDataRows  int
	Timeout            time.Duration
}
