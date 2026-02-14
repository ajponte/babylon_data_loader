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
	"babylon/dataloader/storage"
)

const (
	// Max amount for random account ID generation.
	maxAmount = 1000
	// Max balance for random account ID generation.
	maxBalance = 10000
	// Max value for random account ID generation.
	maxAccountID = 10000
)

// Data represents a single row from the CSV file.
type Data struct {
	Details        string  `bson:"Details"`
	PostingDate    string  `bson:"PostingDate"`
	Description    string  `bson:"Description"`
	Amount         float64 `bson:"Amount"`
	Category       string  `bson:"category"` // New field
	Type           string  `bson:"Type"`
	Balance        float64 `bson:"Balance"`
	CheckOrSlipNum string  `bson:"CheckOrSlipNum"`
	DataSource     string  `bson:"dataSource"` // New field
	AccountID      string  `bson:"accountID"`  // New field
}

// GenerateSyntheticDocuments generates a slice of synthetic data documents.
func GenerateSyntheticDocuments(rows int) []Data {
	documents := make([]Data, rows)
	for i := range rows {
		//nolint:gosec // G404: Use of weak random number generator is acceptable for non-sensitive test data.
		amount := rand.Float64() * maxAmount
		//nolint:gosec // G404: Use of weak random number generator is acceptable for non-sensitive test data.
		balance := rand.Float64() * maxBalance
		//nolint:gosec // G404: Use of weak random number generator is acceptable for non-sensitive test data.
		accountID := fmt.Sprintf("%04d", rand.IntN(maxAccountID)) // Random 4-digit account ID
		documents[i] = Data{
			Details:        "SALE",
			PostingDate:    time.Now().Format("01/02/2006"),
			Description:    fmt.Sprintf("Synthetic transaction %d", i),
			Amount:         amount,
			Category:       "synthetic",
			Type:           "DEBIT",
			Balance:        balance,
			CheckOrSlipNum: "",
			DataSource:     "synthetic",
			AccountID:      accountID,
		}
	}
	return documents
}

// PersistSyntheticData persists a slice of synthetic data to MongoDB.
func PersistSyntheticData(
	ctx context.Context,
	client storage.MongoClient,
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
	client storage.MongoClient,
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
		if err = os.MkdirAll(dir, 0o750); err != nil {
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
	header := []string{
		"Details",
		"Posting Date",
		"Description",
		"Category",
		"Amount",
		"Type",
		"Balance",
		"Check or Slip #",
	}
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
			record.Category,
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
