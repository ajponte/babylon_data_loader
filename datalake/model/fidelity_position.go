package model

import "time"

// SourceLineage captures immutable provenance coordinates back to the source file.
type SourceLineage struct {
	S3URI      string    `bson:"s3Uri,omitempty"`
	FileName   string    `bson:"fileName"`
	RowNumber  int64     `bson:"rowNumber"`
	IngestedAt time.Time `bson:"ingestedAt"`
	SHA256     string    `bson:"sha256,omitempty"`
}

// FidelityPosition represents a point-in-time portfolio holding snapshot.
type FidelityPosition struct {
	AccountNumber        string        `bson:"accountNumber"`
	AccountName          string        `bson:"accountName"`
	Symbol               string        `bson:"symbol"`
	Description          string        `bson:"description"`
	Quantity             float64       `bson:"quantity"`
	LastPrice            float64       `bson:"lastPrice"`
	LastPriceChange      float64       `bson:"lastPriceChange"`
	CurrentValue         float64       `bson:"currentValue"`
	TodaysGainLossDollar float64       `bson:"todaysGainLossDollar"`
	TodaysGainLossPct    float64       `bson:"todaysGainLossPct"`
	TotalGainLossDollar  float64       `bson:"totalGainLossDollar"`
	TotalGainLossPct     float64       `bson:"totalGainLossPct"`
	PercentOfAccount     float64       `bson:"percentOfAccount"`
	CostBasisTotal       float64       `bson:"costBasisTotal"`
	AverageCostBasis     float64       `bson:"averageCostBasis"`
	Type                 string        `bson:"type"`
	AsOfDate             time.Time     `bson:"asOfDate"`
	DataSource           string        `bson:"dataSource"`
	Source               SourceLineage `bson:"source"`
}
