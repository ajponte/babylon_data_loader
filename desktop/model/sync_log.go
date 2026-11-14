// Package model provides UI-facing Data Transfer Objects (DTOs) for the desktop application.
//
// These structs define the serialization shapes exposed to the Wails frontend runtime,
// preserving exact TypeScript namespace contracts (Divergence 10) without legacy datalake coupling.
package model

import "time"

// SyncLog defines the legacy-compatible JSON Data Transfer Object for desktop UI history rendering.
// It matches the contract expected by desktop/frontend/src/components/HistoryPanel.tsx,
// omitting database IDs while populating both canonical and legacy dual-aliased fields (Divergence 10).
type SyncLog struct {
	FileName        string    `json:"fileName"`
	FileHash        string    `json:"fileHash,omitempty"`
	CollectionName  string    `json:"collectionName"`
	RecordCount     int64     `json:"recordCount"`
	UpsertedCount   int64     `json:"upsertedCount"`
	MatchedCount    int64     `json:"matchedCount"`
	Status          string    `json:"status"`
	ErrorMessage    string    `json:"errorMessage,omitempty"`
	SyncedAt        time.Time `json:"syncedAt"`
	DurationMs      int64     `json:"durationMs"`
	SyncTimestamp   time.Time `json:"syncTimestamp"`
	RecordsUploaded int64     `json:"recordsUploaded"`
}
