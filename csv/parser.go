package csvparser

import "context"

// Parser defines the interface for parsing CSV data.
type Parser interface {
	Parse(
		ctx context.Context,
		filePath string,
		dataSource string,
		accountID string,
	) ([]map[string]string, int64, error)
}

type RowProgress struct {
	CurrentRecord int64
	TotalRecords  int64
}

type RowProgressCallback func(progress RowProgress)

type StreamingParser interface {
	Parser
	ParseStream(
		ctx context.Context,
		filePath string,
		dataSource string,
		accountID string,
		onProgress RowProgressCallback,
	) ([]map[string]string, int64, error)
}
