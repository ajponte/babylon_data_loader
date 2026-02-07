package model

// Transaction represents a single row from the CSV file, mapped for storage.
type Transaction struct {
	Details        string  `bson:"Details"`
	PostingDate    string  `bson:"PostingDate"`
	Description    string  `bson:"Description"`
	Amount         float64 `bson:"Amount"`
	Category       string  `bson:"category"`
	Type           string  `bson:"Type"`
	Balance        float64 `bson:"Balance"`
	CheckOrSlipNum string  `bson:"CheckOrSlipNum"`
	DataSource     string  `bson:"dataSource"`
	AccountID      string  `bson:"accountID"`
}
