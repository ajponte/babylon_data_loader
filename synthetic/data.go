package synthetic

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"
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

// GenerateSyntheticData creates a CSV file with synthetic data.
func GenerateSyntheticData(rows int, dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0750); err != nil {
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
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write data rows
	for i := 0; i < rows; i++ {
		amount := rand.Float64() * 1000
		balance := rand.Float64() * 10000
		record := Data{
			Details:        "SALE",
			PostingDate:    time.Now().Format("01/02/2006"),
			Description:    fmt.Sprintf("Synthetic transaction %d", i),
			Amount:         amount,
			Type:           "DEBIT",
			Balance:        balance,
			CheckOrSlipNum: "",
		}
		row := []string{
			record.Details,
			record.PostingDate,
			record.Description,
			fmt.Sprintf("%.2f", record.Amount),
			record.Type,
			fmt.Sprintf("%.2f", record.Balance),
			record.CheckOrSlipNum,
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
	}

	return nil
}
