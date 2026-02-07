package datasource

import (
	"strings"
)

// GenericExtractor provides a fallback for generic filenames.
type GenericExtractor struct{}

// NewGenericExtractor creates a new GenericExtractor.
func NewGenericExtractor() *GenericExtractor {
	return &GenericExtractor{}
}

// ExtractInfo extracts data source and account ID from generic filenames.
func (e *GenericExtractor) ExtractInfo(filename string) (*SourceInfo, error) {
	lowerFileName := strings.ToLower(filename)

	if strings.Contains(lowerFileName, "test") {
		return &SourceInfo{
			DataSource: "test",
			AccountID:  "0000", // Assign a default account ID for test files
		}, nil
	}

	return nil, ErrUnableToExtractInfo
}
