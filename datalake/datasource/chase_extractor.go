package datasource

import (
	"regexp"
	"strings"
)

// ChaseExtractor extracts info for Chase bank files.
type ChaseExtractor struct{}

// NewChaseExtractor creates a new ChaseExtractor.
func NewChaseExtractor() *ChaseExtractor {
	return &ChaseExtractor{}
}

// ExtractInfo extracts data source and account ID from Chase filenames.
func (e *ChaseExtractor) ExtractInfo(filename string) (*SourceInfo, error) {
	lowerFileName := strings.ToLower(filename)

	// Regex to match "chase" and then a 4-digit number
	re := regexp.MustCompile(`chase(\d{4})`)
	matches := re.FindStringSubmatch(lowerFileName)

	if len(matches) > 1 {
		return &SourceInfo{
			DataSource: "chase",
			AccountID:  matches[1],
		}, nil
	}

	return nil, ErrUnableToExtractInfo
}
