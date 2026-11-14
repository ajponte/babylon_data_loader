package synthetic

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"strconv"
	"time"
)

// ErrNilWriter indicates that a nil io.Writer was supplied to WriteCSV.
var ErrNilWriter = errors.New("synthetic csv: writer cannot be nil")

func legacyCSVHeader() []string {
	return []string{
		"Details",
		"Posting Date",
		"Description",
		"Category",
		"Amount",
		"Type",
		"Balance",
		"Check or Slip #",
	}
}

// WriteCSV generates deterministic synthetic transaction rows and streams them as CSV.
func WriteCSV(w io.Writer, opts GeneratorOptions) error {
	if w == nil {
		return ErrNilWriter
	}
	if opts.RowCount < 0 {
		opts.RowCount = 0
	}
	if opts.RowCount > MaxSyntheticRowCount {
		return fmt.Errorf("row count %d exceeds maximum allowable limit of %d", opts.RowCount, MaxSyntheticRowCount)
	}

	seed := opts.Seed
	if seed == 0 {
		seed = defaultSeed
	}

	//nolint:gosec // G404: Use of math/rand/v2 is acceptable for non-cryptographic synthetic test data.
	rng := rand.New(rand.NewPCG(uint64(seed), 1))

	csvWriter := csv.NewWriter(w)

	if err := csvWriter.Write(legacyCSVHeader()); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	baseDate := time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)
	currentBalance := baseBalance

	for i := range opts.RowCount {
		rowNumber := i + 1
		amt := math.Round((rng.Float64()*maxAmount-maxAmount/2.0)*centsFactor) / centsFactor
		if amt == 0 {
			amt = 10.00
		}
		currentBalance += amt
		balanceCopy := math.Round(currentBalance*centsFactor) / centsFactor

		txType := typeDebit
		details := typeDebit
		checkOrSlipNum := ""
		if amt > 0 {
			txType = typeCredit
			details = typeCredit
		} else if i%checkCadence == 0 {
			details = typeCheck
			checkOrSlipNum = strconv.Itoa(baseCheckNum + i)
		}

		postingDate := baseDate.AddDate(0, 0, -i).Format("01/02/2006")
		description := fmt.Sprintf("Synthetic transaction %d", i)

		record := []string{
			details,
			postingDate,
			description,
			sourceSynthetic,
			fmt.Sprintf("%.2f", amt),
			txType,
			fmt.Sprintf("%.2f", balanceCopy),
			checkOrSlipNum,
		}

		if err := csvWriter.Write(record); err != nil {
			return fmt.Errorf("failed to write CSV record %d: %w", rowNumber, err)
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return fmt.Errorf("failed to flush CSV writer: %w", err)
	}

	return nil
}
