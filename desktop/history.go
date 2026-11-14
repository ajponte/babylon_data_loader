// Package main provides the Wails desktop driving adapter for the Babylon data loader.
//
// This file defines the legacy-compatible history DTO, mapping functions, and HistoryService
// adhering to Decision P12 (port.SyncLogReader consumer read port) and Divergence 10 (omitted database ID).
package main

import (
	"context"
	"errors"

	desktopmodel "babylon/dataloader/desktop/model"
	"babylon/dataloader/internal/domain/model"
	"babylon/dataloader/internal/domain/port"
	"babylon/dataloader/internal/infra/storage/mongo"
)

// Ensure mongo.Repository satisfies port.SyncLogReader at compile time.
var _ port.SyncLogReader = (*mongo.Repository)(nil)

// DefaultHistoryLimit defines the default maximum number of historical sync entries returned.
const DefaultHistoryLimit int64 = port.DefaultSyncLogLimit

// HistoryEntry is a type alias for desktopmodel.SyncLog, defining the legacy-compatible
// JSON Data Transfer Object for desktop UI history rendering.
// It matches the contract expected by desktop/frontend/src/components/HistoryPanel.tsx,
// omitting database IDs while populating both canonical and legacy dual-aliased fields (Divergence 10).
type HistoryEntry = desktopmodel.SyncLog

// ToHistoryEntry maps a domain model.SyncLog to a legacy-compatible desktop HistoryEntry DTO.
// Timestamps are strictly normalized to UTC.
func ToHistoryEntry(log model.SyncLog) HistoryEntry {
	status := string(log.Status)
	if status == "" {
		status = "SUCCESS"
	}
	syncedAt := log.SyncedAt.UTC()
	return HistoryEntry{
		FileName:        log.FileName,
		FileHash:        log.FileHash,
		CollectionName:  log.CollectionName,
		RecordCount:     log.RecordCount,
		UpsertedCount:   log.UpsertedCount,
		MatchedCount:    log.MatchedCount,
		Status:          status,
		ErrorMessage:    log.ErrorMessage,
		SyncedAt:        syncedAt,
		DurationMs:      log.DurationMs,
		SyncTimestamp:   syncedAt,
		RecordsUploaded: log.UpsertedCount,
	}
}

// ToHistoryEntries maps a slice of domain model.SyncLog to a slice of HistoryEntry DTOs.
// Returns an empty slice (non-nil) when input is nil or empty, ensuring valid JSON array serialization ([]).
func ToHistoryEntries(logs []model.SyncLog) []HistoryEntry {
	if len(logs) == 0 {
		return []HistoryEntry{}
	}
	entries := make([]HistoryEntry, len(logs))
	for i, l := range logs {
		entries[i] = ToHistoryEntry(l)
	}
	return entries
}

// HistoryService retrieves sync audit history via port.SyncLogReader and maps domain models to desktop DTOs.
type HistoryService struct {
	reader       port.SyncLogReader
	defaultLimit int64
}

// NewHistoryService constructs a HistoryService wrapping a port.SyncLogReader.
func NewHistoryService(reader port.SyncLogReader) *HistoryService {
	return &HistoryService{
		reader:       reader,
		defaultLimit: DefaultHistoryLimit,
	}
}

// GetHistory retrieves up to limit history entries, falling back to DefaultHistoryLimit if limit <= 0.
// When called without limit arguments or with a non-positive value, it uses the default limit.
func (s *HistoryService) GetHistory(ctx context.Context, limit ...int64) ([]HistoryEntry, error) {
	if s == nil || s.reader == nil {
		return nil, errors.New("history service: uninitialized or nil sync log reader")
	}
	if ctx == nil {
		return nil, errors.New("history service: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	defaultLimit := s.defaultLimit
	if defaultLimit <= 0 {
		defaultLimit = DefaultHistoryLimit
	}
	lim := defaultLimit
	if len(limit) > 0 && limit[0] > 0 {
		lim = limit[0]
	}

	logs, err := s.reader.ListSyncLogs(ctx, lim)
	if err != nil {
		return nil, err
	}
	return ToHistoryEntries(logs), nil
}
