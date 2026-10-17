package model

import "time"

// ChaseTransaction represents a normalized financial transaction from Chase Bank
// (supporting both Banking and Credit Card statement formats) persisted to MongoDB Atlas.
type ChaseTransaction struct {
	AccountID       string        `bson:"accountId"`
	PostingDate     time.Time     `bson:"postingDate"`
	TransactionDate *time.Time    `bson:"transactionDate,omitempty"`
	Description     string        `bson:"description"`
	Amount          float64       `bson:"amount"`
	TransactionType string        `bson:"transactionType"`
	Balance         *float64      `bson:"balance,omitempty"`
	Category        string        `bson:"category,omitempty"`
	Details         string        `bson:"details,omitempty"`
	CheckOrSlipNum  string        `bson:"checkOrSlipNum,omitempty"`
	Memo            string        `bson:"memo,omitempty"`
	DataSource      string        `bson:"dataSource"`
	Source          SourceLineage `bson:"source"`
}
