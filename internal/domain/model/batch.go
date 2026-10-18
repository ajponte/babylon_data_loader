package model

import (
	"errors"
	"time"
)

var (
	// ErrNilBatch indicates that an IngestionBatch is nil.
	ErrNilBatch = errors.New("ingestion batch is nil")
	// ErrInvalidDestination indicates an unsupported or unknown Destination.
	ErrInvalidDestination = errors.New("invalid batch destination")
	// ErrNegativeRecordCount indicates that the batch record count is negative.
	ErrNegativeRecordCount = errors.New("batch record count cannot be negative")
	// ErrNilPayload indicates that the batch payload is nil when record count is positive.
	ErrNilPayload = errors.New("batch payload cannot be nil when record count is positive")
	// ErrMissingIngestedAt indicates that the ingested-at timestamp is zero.
	ErrMissingIngestedAt = errors.New("ingested at timestamp is required")
	// ErrPayloadTypeMismatch indicates that the batch payload does not match the expected type.
	ErrPayloadTypeMismatch = errors.New("batch payload type mismatch")
	// ErrMissingAccountID indicates that the account ID is empty.
	ErrMissingAccountID = errors.New("account ID is required")
	// ErrNilLineage indicates that a SourceLineage is nil.
	ErrNilLineage = errors.New("source lineage is nil")
	// ErrMissingFileName indicates that the file name is empty.
	ErrMissingFileName = errors.New("file name is required")
	// ErrNegativeRowNumber indicates that the lineage row number is negative.
	ErrNegativeRowNumber = errors.New("lineage row number cannot be negative")
	// ErrNilFileMetadata indicates that a FileMetadata is nil.
	ErrNilFileMetadata = errors.New("file metadata is nil")
)

// Destination represents the target collection or storage destination for ingested data.
type Destination string

const (
	// DestChaseChecking routes data to the Chase transactions collection.
	DestChaseChecking Destination = "transactions_chase"
	// DestChaseCreditCard routes data to the Chase transactions collection.
	// Both checking and credit card statements share the same datalake target collection.
	DestChaseCreditCard Destination = "transactions_chase"
	// DestFidelityPosition routes data to the Fidelity positions collection.
	DestFidelityPosition Destination = "positions_fidelity"
	// DestSynthetic routes data to the synthetic transactions collection.
	DestSynthetic Destination = "transactions_synthetic"
)

// IsValid reports whether the destination matches a recognized target destination.
// DestChaseChecking and DestChaseCreditCard share the same underlying collection routing ("transactions_chase").
func (d Destination) IsValid() bool {
	switch d {
	case DestChaseChecking, DestFidelityPosition, DestSynthetic:
		// Note: DestChaseCreditCard evaluates to the same string constant ("transactions_chase")
		// as DestChaseChecking and is therefore covered by DestChaseChecking.
		return true
	default:
		return false
	}
}

// IngestionBatch wraps extracted records and metadata for datalake routing.
type IngestionBatch struct {
	Destination Destination
	AccountID   string
	SourceFile  string
	FileHash    string
	IngestedAt  time.Time
	RecordCount int
	Payload     any
}

// Validate validates the core integrity of the IngestionBatch.
func (b *IngestionBatch) Validate() error {
	if b == nil {
		return ErrNilBatch
	}
	if !b.Destination.IsValid() {
		return ErrInvalidDestination
	}
	if b.AccountID == "" {
		return ErrMissingAccountID
	}
	if b.SourceFile == "" {
		return ErrMissingFileName
	}
	if b.IngestedAt.IsZero() {
		return ErrMissingIngestedAt
	}
	if b.RecordCount < 0 {
		return ErrNegativeRecordCount
	}
	if b.RecordCount > 0 && b.Payload == nil {
		return ErrNilPayload
	}
	return nil
}

// AsChaseTransactions type-asserts the payload to a slice of ChaseTransaction.
// Returns ErrNilBatch if the receiver is nil.
// If RecordCount is zero and Payload is nil, an empty slice is returned.
// If RecordCount is positive and Payload is nil, ErrNilPayload is returned.
// If Payload is not []ChaseTransaction, ErrPayloadTypeMismatch is returned.
func (b *IngestionBatch) AsChaseTransactions() ([]ChaseTransaction, error) {
	if b == nil {
		return nil, ErrNilBatch
	}
	if b.Payload == nil {
		if b.RecordCount == 0 {
			return []ChaseTransaction{}, nil
		}
		return nil, ErrNilPayload
	}
	txs, ok := b.Payload.([]ChaseTransaction)
	if !ok {
		return nil, ErrPayloadTypeMismatch
	}
	return txs, nil
}

// AsFidelityPositions type-asserts the payload to a slice of FidelityPosition.
// Returns ErrNilBatch if the receiver is nil.
// If RecordCount is zero and Payload is nil, an empty slice is returned.
// If RecordCount is positive and Payload is nil, ErrNilPayload is returned.
// If Payload is not []FidelityPosition, ErrPayloadTypeMismatch is returned.
func (b *IngestionBatch) AsFidelityPositions() ([]FidelityPosition, error) {
	if b == nil {
		return nil, ErrNilBatch
	}
	if b.Payload == nil {
		if b.RecordCount == 0 {
			return []FidelityPosition{}, nil
		}
		return nil, ErrNilPayload
	}
	pos, ok := b.Payload.([]FidelityPosition)
	if !ok {
		return nil, ErrPayloadTypeMismatch
	}
	return pos, nil
}

// SourceLineage tracks the origin and audit metadata of an individual ingested record.
type SourceLineage struct {
	FileName   string
	FileHash   string
	RowNumber  int
	IngestedAt time.Time
}

// Validate validates the core integrity of the SourceLineage.
func (l *SourceLineage) Validate() error {
	if l == nil {
		return ErrNilLineage
	}
	if l.FileName == "" {
		return ErrMissingFileName
	}
	if l.RowNumber < 0 {
		return ErrNegativeRowNumber
	}
	return nil
}

// FileMetadata encapsulates file identification, ownership, and timestamp metadata for extraction.
type FileMetadata struct {
	FileName   string
	FileHash   string
	AccountID  string
	IngestedAt time.Time
}

// Validate validates the core fields of FileMetadata.
func (m *FileMetadata) Validate() error {
	if m == nil {
		return ErrNilFileMetadata
	}
	if m.FileName == "" {
		return ErrMissingFileName
	}
	if m.AccountID == "" {
		return ErrMissingAccountID
	}
	if m.IngestedAt.IsZero() {
		return ErrMissingIngestedAt
	}
	return nil
}
