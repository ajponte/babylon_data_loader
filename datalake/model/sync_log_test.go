package model_test

import (
	"testing"
	"time"

	"babylon/dataloader/datalake/model"

	"go.mongodb.org/mongo-driver/bson"
)

// TC-SL-01: NewSuccessSyncLog asserts all 11 fields populated, canonical keys set, legacy aliases synchronized.
func TestSyncLog_NewSuccessSyncLog(t *testing.T) {
	syncedAt := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	log := model.NewSuccessSyncLog(
		"chase_statement.csv",
		"abc123hash",
		"transactions_chase",
		150,
		140,
		10,
		syncedAt,
		250,
	)

	if log.FileName != "chase_statement.csv" {
		t.Errorf("expected FileName 'chase_statement.csv', got %q", log.FileName)
	}
	if log.FileHash != "abc123hash" {
		t.Errorf("expected FileHash 'abc123hash', got %q", log.FileHash)
	}
	if log.CollectionName != "transactions_chase" {
		t.Errorf("expected CollectionName 'transactions_chase', got %q", log.CollectionName)
	}
	if log.RecordCount != 150 {
		t.Errorf("expected RecordCount 150, got %d", log.RecordCount)
	}
	if log.UpsertedCount != 140 {
		t.Errorf("expected UpsertedCount 140, got %d", log.UpsertedCount)
	}
	if log.MatchedCount != 10 {
		t.Errorf("expected MatchedCount 10, got %d", log.MatchedCount)
	}
	if log.Status != string(model.SyncStatusSuccess) {
		t.Errorf("expected Status %q, got %q", model.SyncStatusSuccess, log.Status)
	}
	if log.ErrorMessage != "" {
		t.Errorf("expected empty ErrorMessage, got %q", log.ErrorMessage)
	}
	if !log.SyncedAt.Equal(syncedAt) {
		t.Errorf("expected SyncedAt %v, got %v", syncedAt, log.SyncedAt)
	}
	if log.DurationMs != 250 {
		t.Errorf("expected DurationMs 250, got %d", log.DurationMs)
	}

	// Verify legacy aliases
	if log.LegacyCollectionName != "transactions_chase" {
		t.Errorf("expected LegacyCollectionName 'transactions_chase', got %q", log.LegacyCollectionName)
	}
	if !log.LegacySyncTimestamp.Equal(syncedAt) {
		t.Errorf("expected LegacySyncTimestamp %v, got %v", syncedAt, log.LegacySyncTimestamp)
	}
	if log.LegacyRecordsUploaded != 140 {
		t.Errorf("expected LegacyRecordsUploaded 140, got %d", log.LegacyRecordsUploaded)
	}
}

// TC-SL-02: NewFailedSyncLog asserts Status="FAILED", ErrorMessage set, zero upsert/matched counts, legacy aliases synchronized.
func TestSyncLog_NewFailedSyncLog(t *testing.T) {
	syncedAt := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	log := model.NewFailedSyncLog(
		"corrupt.csv",
		"hash456",
		"transactions_chase",
		"invalid csv format on row 5",
		20,
		syncedAt,
		55,
	)

	if log.FileName != "corrupt.csv" {
		t.Errorf("expected FileName 'corrupt.csv', got %q", log.FileName)
	}
	if log.FileHash != "hash456" {
		t.Errorf("expected FileHash 'hash456', got %q", log.FileHash)
	}
	if log.CollectionName != "transactions_chase" {
		t.Errorf("expected CollectionName 'transactions_chase', got %q", log.CollectionName)
	}
	if log.RecordCount != 20 {
		t.Errorf("expected RecordCount 20, got %d", log.RecordCount)
	}
	if log.UpsertedCount != 0 {
		t.Errorf("expected UpsertedCount 0, got %d", log.UpsertedCount)
	}
	if log.MatchedCount != 0 {
		t.Errorf("expected MatchedCount 0, got %d", log.MatchedCount)
	}
	if log.Status != string(model.SyncStatusFailed) {
		t.Errorf("expected Status %q, got %q", model.SyncStatusFailed, log.Status)
	}
	if log.ErrorMessage != "invalid csv format on row 5" {
		t.Errorf("expected ErrorMessage 'invalid csv format on row 5', got %q", log.ErrorMessage)
	}
	if !log.SyncedAt.Equal(syncedAt) {
		t.Errorf("expected SyncedAt %v, got %v", syncedAt, log.SyncedAt)
	}
	if log.DurationMs != 55 {
		t.Errorf("expected DurationMs 55, got %d", log.DurationMs)
	}

	// Verify legacy aliases
	if log.LegacyCollectionName != "transactions_chase" {
		t.Errorf("expected LegacyCollectionName 'transactions_chase', got %q", log.LegacyCollectionName)
	}
	if !log.LegacySyncTimestamp.Equal(syncedAt) {
		t.Errorf("expected LegacySyncTimestamp %v, got %v", syncedAt, log.LegacySyncTimestamp)
	}
	if log.LegacyRecordsUploaded != 0 {
		t.Errorf("expected LegacyRecordsUploaded 0, got %d", log.LegacyRecordsUploaded)
	}
}

// TC-SL-03: Normalize simulates reading an existing MongoDB document with only legacy fields.
func TestSyncLog_Normalize_LegacyRecord(t *testing.T) {
	legacyTime := time.Date(2025, 5, 20, 8, 30, 0, 0, time.UTC)
	log := model.SyncLog{
		LegacyCollectionName:  "positions_fidelity",
		LegacySyncTimestamp:   legacyTime,
		LegacyRecordsUploaded: 42,
	}

	log.Normalize()

	if log.CollectionName != "positions_fidelity" {
		t.Errorf("expected CollectionName backfilled to 'positions_fidelity', got %q", log.CollectionName)
	}
	if !log.SyncedAt.Equal(legacyTime) {
		t.Errorf("expected SyncedAt backfilled to %v, got %v", legacyTime, log.SyncedAt)
	}
	if log.RecordCount != 42 {
		t.Errorf("expected RecordCount backfilled to 42, got %d", log.RecordCount)
	}
	if log.UpsertedCount != 42 {
		t.Errorf("expected UpsertedCount backfilled to 42, got %d", log.UpsertedCount)
	}
	if log.Status != string(model.SyncStatusSuccess) {
		t.Errorf("expected Status defaulted to %q, got %q", model.SyncStatusSuccess, log.Status)
	}

	// Reverse check: canonical populated, legacy empty
	canonicalTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	canonicalLog := model.SyncLog{
		CollectionName: "transactions_chase",
		SyncedAt:       canonicalTime,
		UpsertedCount:  100,
	}
	canonicalLog.Normalize()

	if canonicalLog.LegacyCollectionName != "transactions_chase" {
		t.Errorf("expected LegacyCollectionName populated from CollectionName, got %q", canonicalLog.LegacyCollectionName)
	}
	if !canonicalLog.LegacySyncTimestamp.Equal(canonicalTime) {
		t.Errorf("expected LegacySyncTimestamp populated from SyncedAt, got %v", canonicalLog.LegacySyncTimestamp)
	}
	if canonicalLog.LegacyRecordsUploaded != 100 {
		t.Errorf("expected LegacyRecordsUploaded populated from UpsertedCount, got %d", canonicalLog.LegacyRecordsUploaded)
	}
}

// TC-SL-04: Normalize asserts safe execution on nil receiver with zero panics.
func TestSyncLog_Normalize_NilReceiver(t *testing.T) {
	var nilLog *model.SyncLog
	// Should not panic
	nilLog.Normalize()
}

// TC-SL-05: BSON Serialization asserts canonical camelCase keys.
func TestSyncLog_BSONSerialization(t *testing.T) {
	syncedAt := time.Date(2026, 10, 17, 14, 0, 0, 0, time.UTC)
	original := model.NewSuccessSyncLog(
		"fidelity_data.csv",
		"sha256hex",
		"positions_fidelity",
		50,
		45,
		5,
		syncedAt,
		120,
	)

	data, err := bson.Marshal(original)
	if err != nil {
		t.Fatalf("failed to marshal SyncLog to BSON: %v", err)
	}

	var rawDoc bson.M
	if err = bson.Unmarshal(data, &rawDoc); err != nil {
		t.Fatalf("failed to unmarshal BSON to bson.M: %v", err)
	}

	// Assert canonical camelCase keys exist
	expectedKeys := []string{
		"fileName", "fileHash", "collectionName", "recordCount",
		"upsertedCount", "matchedCount", "status", "syncedAt", "durationMs",
	}
	for _, key := range expectedKeys {
		if _, exists := rawDoc[key]; !exists {
			t.Errorf("expected BSON key %q to exist in marshaled document", key)
		}
	}

	// Round-trip unmarshal into typed SyncLog
	var decoded model.SyncLog
	if err = bson.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal BSON into typed SyncLog: %v", err)
	}

	if decoded.FileName != original.FileName {
		t.Errorf("expected decoded FileName %q, got %q", original.FileName, decoded.FileName)
	}
	if decoded.RecordCount != original.RecordCount {
		t.Errorf("expected decoded RecordCount %d, got %d", original.RecordCount, decoded.RecordCount)
	}
	if !decoded.SyncedAt.Equal(original.SyncedAt) {
		t.Errorf("expected decoded SyncedAt %v, got %v", original.SyncedAt, decoded.SyncedAt)
	}
}
