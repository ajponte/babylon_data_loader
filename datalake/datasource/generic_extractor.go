package datasource

import (
	"regexp"
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

	if strings.Contains(lowerFileName, "chase") {
		// Use a regular expression to find the 4-digit account number after "chase"
		re := regexp.MustCompile(`chase(\d{4})`)
		matches := re.FindStringSubmatch(lowerFileName)

		if len(matches) > 1 {
			return &SourceInfo{
				DataSource: string(Chase),
				AccountID:  matches[1],
			}, nil
		}
	}

	if strings.Contains(lowerFileName, "synthetic") {
		return &SourceInfo{
			DataSource: string(Synthetic),
			AccountID:  "0000", // Assign a default account ID for synthetic files
		}, nil
	}

	return nil, ErrUnableToExtractInfo
}
