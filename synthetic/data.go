package synthetic

import (
	"context"
	"encoding/csv"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"

	bcontext "babylon/dataloader/appcontext"

	"go.mongodb.org/mongo-driver/mongo"
)

const (
	maxAmount  = 1000
	maxBalance = 10000
)

// Data represents a single row from the CSV file.
type Data struct {
	Details        string
	PostingDate    string
	Description    string
	Amount         float64
	Type           string
	Balance        float64
	CheckOrSlipNum string
}

// GenerateSyntheticDocuments generates a slice of synthetic data documents.
func GenerateSyntheticDocuments(rows int) []Data {
	documents := make([]Data, rows)
	for i := range rows {
		//nolint:gosec // G404: Use of weak random number generator is acceptable for non-sensitive test data.
		amount := rand.Float64() * maxAmount
		//nolint:gosec // G404: Use of weak random number generator is acceptable for non-sensitive test data.
		balance := rand.Float64() * maxBalance
		documents[i] = Data{
			Details:        "SALE",
			PostingDate:    time.Now().Format("01/02/2006"),
			Description:    fmt.Sprintf("Synthetic transaction %d", i),
			Amount:         amount,
			Type:           "DEBIT",
			Balance:        balance,
			CheckOrSlipNum: "",
		}
	}
	return documents
}

// PersistSyntheticData persists a slice of synthetic data to MongoDB.
func PersistSyntheticData(
	ctx context.Context,
	client *mongo.Client,
	collectionName string,
	documents []Data,
) error {
	logger := bcontext.LoggerFromContext(ctx)
	if len(documents) == 0 {
		logger.InfoContext(ctx, "No documents to insert into MongoDB.")
		return nil
	}

	docsToInsert := make([]interface{}, len(documents))
	for i, doc := range documents {
		docsToInsert[i] = doc
	}

	collection := client.Database("datalake").Collection(collectionName)
	res, err := collection.InsertMany(ctx, docsToInsert)
	if err != nil {
		return fmt.Errorf("failed to insert synthetic data into MongoDB: %w", err)
	}

	logger.InfoContext(ctx, "Successfully inserted synthetic data into MongoDB", "count", len(res.InsertedIDs))
	return nil
}

// GenerateAndPersistSyntheticData generates synthetic data and persists it directly to MongoDB.
func GenerateAndPersistSyntheticData(
	ctx context.Context,
	client *mongo.Client,
	collectionName string,
	rows int,
) error {
	logger := bcontext.LoggerFromContext(ctx)
	logger.InfoContext(ctx, "Generating and persisting synthetic data to MongoDB",
		"collection", collectionName, "rows", rows)
	documents := GenerateSyntheticDocuments(rows)
	return PersistSyntheticData(ctx, client, collectionName, documents)
}

// GenerateSyntheticData creates a CSV file with synthetic data.
func GenerateSyntheticData(rows int, dir string) error {
	var err error
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, 0750); err != nil {
			return fmt.Errorf("failed to create directory '%s': %w", dir, err)
		}
	}

	filePath := filepath.Join(dir, "test-synthetic-data.csv")
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file '%s': %w", filePath, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{"Details", "Posting Date", "Description", "Amount", "Type", "Balance", "Check or Slip #"}
	if err = writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	documents := GenerateSyntheticDocuments(rows)
	// Write data rows
	for _, record := range documents {
		row := []string{
			record.Details,
			record.PostingDate,
			record.Description,
			fmt.Sprintf("%.2f", record.Amount),
			record.Type,
			fmt.Sprintf("%.2f", record.Balance),
			record.CheckOrSlipNum,
		}
		if err = writer.Write(row); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
	}

	return nil
}
