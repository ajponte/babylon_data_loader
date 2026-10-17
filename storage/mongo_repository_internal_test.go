package storage

import (
	"testing"
	"time"

	"babylon/dataloader/datalake/model"

	"go.mongodb.org/mongo-driver/mongo"
)

func TestToUpsertStats(t *testing.T) {
	t.Run("nil result returns empty stats", func(t *testing.T) {
		stats := toUpsertStats(nil)
		if stats.UpsertedCount != 0 || stats.MatchedCount != 0 || stats.ModifiedCount != 0 {
			t.Errorf("expected empty stats for nil, got %+v", stats)
		}
	})

	t.Run("populates counts from BulkWriteResult", func(t *testing.T) {
		res := &mongo.BulkWriteResult{
			UpsertedCount: 10,
			MatchedCount:  5,
			ModifiedCount: 3,
		}
		stats := toUpsertStats(res)
		if stats.UpsertedCount != 10 || stats.MatchedCount != 5 || stats.ModifiedCount != 3 {
			t.Errorf("expected stats {10, 5, 3}, got %+v", stats)
		}
	})
}

func TestBuildChaseFilter(t *testing.T) {
	postingDate := time.Date(2026, time.January, 15, 0, 0, 0, 0, time.UTC)
	doc := model.ChaseTransaction{
		AccountID:   "CHASE-1234",
		PostingDate: postingDate,
		Description: "Grocery store",
		Amount:      -42.50,
		Source: model.SourceLineage{
			RowNumber: 12,
		},
	}

	filter := buildChaseFilter(doc)

	if filter[fieldAccountID] != "CHASE-1234" {
		t.Errorf("expected %s to be CHASE-1234, got %v", fieldAccountID, filter[fieldAccountID])
	}
	if filter[fieldPostingDate] != postingDate {
		t.Errorf("expected %s to be %v, got %v", fieldPostingDate, postingDate, filter[fieldPostingDate])
	}
	if filter["description"] != "Grocery store" {
		t.Errorf("expected description to be 'Grocery store', got %v", filter["description"])
	}
	if filter["amount"] != -42.50 {
		t.Errorf("expected amount to be -42.50, got %v", filter["amount"])
	}
	if filter["source.rowNumber"] != int64(12) {
		t.Errorf("expected source.rowNumber to be 12, got %v", filter["source.rowNumber"])
	}
}
