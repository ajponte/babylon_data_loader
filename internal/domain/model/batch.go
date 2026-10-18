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
	// ErrNilLineage indicates that a SourceLineage is nil.
	ErrNilLineage = errors.New("source lineage is nil")
	// ErrMissingFileName indicates that the lineage file name is empty.
	ErrMissingFileName = errors.New("lineage file name is required")
	// ErrNegativeRowNumber indicates that the lineage row number is negative.
	ErrNegativeRowNumber = errors.New("lineage row number cannot be negative")
)

// Destination represents the target collection or storage destination for ingested data.
type Destination string

const (
	// DestChaseChecking routes data to the Chase transactions collection.
	DestChaseChecking Destination = "transactions_chase"
	// DestChaseCreditCard routes data to the Chase transactions collection.
	DestChaseCreditCard Destination = "transactions_chase"
	// DestFidelityPosition routes data to the Fidelity positions collection.
	DestFidelityPosition Destination = "positions_fidelity"
	// DestSynthetic routes data to the synthetic transactions collection.
	DestSynthetic Destination = "transactions_synthetic"
)

// IsValid reports whether the destination matches a recognized target destination.
func (d Destination) IsValid() bool {
	switch d {
	case DestChaseChecking, DestFidelityPosition, DestSynthetic:
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
	if b.RecordCount < 0 {
		return ErrNegativeRecordCount
	}
	return nil
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
