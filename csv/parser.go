package csv

import "context"

// Parser defines the interface for parsing CSV data.
type Parser interface {
	Parse(ctx context.Context, filePath string, dataSource string, accountID string) ([]map[string]string, int64, error)
}
