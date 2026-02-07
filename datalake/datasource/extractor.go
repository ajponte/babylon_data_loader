package datasource

import (
	"errors"
)

// SourceInfo holds the extracted data source and account ID.
type SourceInfo struct {
	DataSource string
	AccountID  string
}

// InfoExtractor defines the interface for extracting source information from a filename.
type InfoExtractor interface {
	ExtractInfo(filename string) (*SourceInfo, error)
}

// ErrUnableToExtractInfo is returned when the extractor cannot parse the filename.
var ErrUnableToExtractInfo = errors.New("unable to extract source info from filename")
