package datasource

import (
	"path/filepath"
	"regexp"
	"strings"
)

// GenericExtractor provides a fallback for generic filenames.
type GenericExtractor struct {
	chaseRegex *regexp.Regexp
}

// NewGenericExtractor creates a new GenericExtractor.
func NewGenericExtractor() *GenericExtractor {
	return &GenericExtractor{
		chaseRegex: regexp.MustCompile(`chase(\d{4})`),
	}
}

// ExtractInfo extracts data source and account ID from generic filenames.
func (e *GenericExtractor) ExtractInfo(filename string) (*SourceInfo, error) {
	baseName := filepath.Base(filename)
	lowerBaseName := strings.ToLower(baseName)
	lowerFileName := strings.ToLower(filename)

	re := e.chaseRegex
	if re == nil {
		re = regexp.MustCompile(`chase(\d{4})`)
	}

	if strings.Contains(lowerBaseName, "chase") {
		matches := re.FindStringSubmatch(lowerBaseName)
		if len(matches) > 1 {
			return &SourceInfo{
				DataSource: string(Chase),
				AccountID:  matches[1],
			}, nil
		}
	}

	if strings.Contains(lowerFileName, "fidelity") || strings.HasPrefix(lowerBaseName, "portfolio_positions") {
		return &SourceInfo{
			DataSource: string(Fidelity),
			AccountID:  "portfolio",
		}, nil
	}

	if strings.Contains(lowerBaseName, "synthetic") {
		return &SourceInfo{
			DataSource: string(Synthetic),
			AccountID:  "0000", // Assign a default account ID for synthetic files
		}, nil
	}

	return nil, ErrUnableToExtractInfo
}
