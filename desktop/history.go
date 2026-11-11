// Package main provides the Wails desktop driving adapter for the Babylon data loader.
//
// This file defines the legacy-compatible history DTO, mapping functions, and historyService
// adhering to Decision P12 (port.SyncLogReader consumer read port) and Divergence 10 (omitted database ID).
package main

import (
	"context"
	"errors"
	"time"

	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
	"babylon/dataloader/internal/infra/storage/mongo"
)

// Ensure mongo.Repository satisfies port.SyncLogReader at compile time.
var _ port.SyncLogReader = (*mongo.Repository)(nil)

// DefaultHistoryLimit defines the default maximum number of historical sync entries returned.
const DefaultHistoryLimit int64 = 100

// historyEntry defines the legacy-compatible JSON Data Transfer Object for desktop UI history rendering.
// It matches the contract expected by desktop/frontend/src/components/HistoryPanel.tsx,
// omitting database IDs while populating both canonical and legacy dual-aliased fields (Divergence 10).
type historyEntry struct {
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

// toHistoryEntry maps a domain model.SyncLog to a legacy-compatible desktop historyEntry DTO.
func toHistoryEntry(log model.SyncLog) historyEntry {
	status := string(log.Status)
	if status == "" {
		status = "SUCCESS"
	}
	return historyEntry{
		FileName:        log.FileName,
		FileHash:        log.FileHash,
		CollectionName:  log.CollectionName,
		RecordCount:     log.RecordCount,
		UpsertedCount:   log.UpsertedCount,
		MatchedCount:    log.MatchedCount,
		Status:          status,
		ErrorMessage:    log.ErrorMessage,
		SyncedAt:        log.SyncedAt,
		DurationMs:      log.DurationMs,
		SyncTimestamp:   log.SyncedAt,
		RecordsUploaded: log.UpsertedCount,
	}
}

// toHistoryEntries maps a slice of domain model.SyncLog to a slice of historyEntry DTOs.
// Returns an empty slice (non-nil) when input is nil or empty, ensuring valid JSON array serialization ([]).
func toHistoryEntries(logs []model.SyncLog) []historyEntry {
	if len(logs) == 0 {
		return []historyEntry{}
	}
	entries := make([]historyEntry, len(logs))
	for i, l := range logs {
		entries[i] = toHistoryEntry(l)
	}
	return entries
}

// historyService retrieves sync audit history via port.SyncLogReader and maps domain models to desktop DTOs.
type historyService struct {
	reader       port.SyncLogReader
	defaultLimit int64
}

// newHistoryService constructs a historyService wrapping a port.SyncLogReader.
func newHistoryService(reader port.SyncLogReader) *historyService {
	return &historyService{
		reader:       reader,
		defaultLimit: DefaultHistoryLimit,
	}
}

// GetHistory retrieves up to limit history entries, falling back to DefaultHistoryLimit if limit <= 0.
// When called without limit arguments or with a non-positive value, it uses the default limit.
func (s *historyService) GetHistory(ctx context.Context, limit ...int64) ([]historyEntry, error) {
	if s == nil || s.reader == nil {
		return nil, errors.New("history service: uninitialized or nil sync log reader")
	}
	lim := s.defaultLimit
	if len(limit) > 0 && limit[0] > 0 {
		lim = limit[0]
	}
	logs, err := s.reader.ListSyncLogs(ctx, lim)
	if err != nil {
		return nil, err
	}
	return toHistoryEntries(logs), nil
}
