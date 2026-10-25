package mongo

import (
	"time"

	"babylon/dataloader/internal/domain/model"
)

const (
	dataSourceChase     = "chase"
	dataSourceSynthetic = "synthetic"
	dataSourceFidelity  = "fidelity"
)

type lineageDoc struct {
	// S3URI is a Phase 4 placeholder: batch/file metadata supplied by the Lambda entrypoint.
	// Always empty in Phase 3, so omitempty keeps it out of inserts and $set (E6 preserves
	// existing values). Field order matches legacy datalake/model.SourceLineage.
	S3URI      string    `bson:"s3Uri,omitempty"`
	FileName   string    `bson:"fileName"`
	RowNumber  int64     `bson:"rowNumber"` // int64, NOT int: Go int encodes as BSON int32.
	IngestedAt time.Time `bson:"ingestedAt"`
	SHA256     string    `bson:"sha256,omitempty"` // from Lineage.FileHash.
}

type chaseDoc struct {
	AccountID       string     `bson:"accountId"`
	PostingDate     time.Time  `bson:"postingDate"`
	TransactionDate *time.Time `bson:"transactionDate,omitempty"` // 2F; nil for checking/synthetic.
	Description     string     `bson:"description"`
	Amount          float64    `bson:"amount"`
	TransactionType string     `bson:"transactionType"`
	Balance         *float64   `bson:"balance,omitempty"`
	Category        string     `bson:"category,omitempty"`
	Details         string     `bson:"details,omitempty"`
	CheckOrSlipNum  string     `bson:"checkOrSlipNum,omitempty"`
	Memo            string     `bson:"memo,omitempty"`
	DataSource      string     `bson:"dataSource"`
	Source          lineageDoc `bson:"source"`
}

// Field order matches legacy datalake/model.FidelityPosition. No omitempty on the 2E
// fields: legacy always emits them (placeholders stored as 0.0 / "").
type fidelityDoc struct {
	AccountNumber        string     `bson:"accountNumber"`
	AccountName          string     `bson:"accountName"`
	Symbol               string     `bson:"symbol"`
	Description          string     `bson:"description"`
	Quantity             float64    `bson:"quantity"`
	LastPrice            float64    `bson:"lastPrice"`
	LastPriceChange      float64    `bson:"lastPriceChange"` // 2E.
	CurrentValue         float64    `bson:"currentValue"`
	TodaysGainLossDollar float64    `bson:"todaysGainLossDollar"` // 2E.
	TodaysGainLossPct    float64    `bson:"todaysGainLossPct"`    // 2E, percentage points.
	TotalGainLossDollar  float64    `bson:"totalGainLossDollar"`  // 2E.
	TotalGainLossPct     float64    `bson:"totalGainLossPct"`     // 2E, percentage points.
	PercentOfAccount     float64    `bson:"percentOfAccount"`     // 2E, percentage points.
	CostBasisTotal       float64    `bson:"costBasisTotal"`
	AverageCostBasis     float64    `bson:"averageCostBasis"` // 2E.
	Type                 string     `bson:"type"`             // 2E, emitted even when "".
	AsOfDate             time.Time  `bson:"asOfDate"`
	DataSource           string     `bson:"dataSource"`
	Source               lineageDoc `bson:"source"`
}

func toLineageDoc(l model.SourceLineage) lineageDoc {
	return lineageDoc{
		S3URI:      "",
		FileName:   l.FileName,
		RowNumber:  int64(l.RowNumber),
		IngestedAt: l.IngestedAt.UTC(),
		SHA256:     l.FileHash,
	}
}

func toChaseDoc(tx model.ChaseTransaction, dataSource string) chaseDoc {
	var txDate *time.Time
	if tx.TransactionDate != nil {
		t := tx.TransactionDate.UTC()
		txDate = &t
	}

	var balance *float64
	if tx.Balance != nil {
		b := *tx.Balance
		balance = &b
	}

	return chaseDoc{
		AccountID:       tx.AccountID,
		PostingDate:     tx.PostingDate.UTC(),
		TransactionDate: txDate,
		Description:     tx.Description,
		Amount:          tx.Amount,
		TransactionType: tx.Type,
		Balance:         balance,
		Category:        tx.Category,
		Details:         tx.Details,
		CheckOrSlipNum:  tx.CheckOrSlipNum,
		Memo:            tx.Memo,
		DataSource:      dataSource,
		Source:          toLineageDoc(tx.Lineage),
	}
}

func toFidelityDoc(p model.FidelityPosition) fidelityDoc {
	return fidelityDoc{
		AccountNumber:        p.AccountID,
		AccountName:          p.AccountName,
		Symbol:               p.Symbol,
		Description:          p.Description,
		Quantity:             p.Quantity,
		LastPrice:            p.LastPrice,
		LastPriceChange:      p.LastPriceChange,
		CurrentValue:         p.CurrentValue,
		TodaysGainLossDollar: p.TodaysGainLossDollar,
		TodaysGainLossPct:    p.TodaysGainLossPct,
		TotalGainLossDollar:  p.TotalGainLossDollar,
		TotalGainLossPct:     p.TotalGainLossPct,
		PercentOfAccount:     p.PercentOfAccount,
		CostBasisTotal:       p.CostBasis,
		AverageCostBasis:     p.AverageCostBasis,
		Type:                 p.Type,
		AsOfDate:             p.AsOfDate.UTC(),
		DataSource:           dataSourceFidelity,
		Source:               toLineageDoc(p.Lineage),
	}
}
