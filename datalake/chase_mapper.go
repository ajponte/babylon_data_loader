package datalake

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"babylon/dataloader/datalake/model"
)

// ChaseMapper encapsulates source file provenance coordinates and provides
// pure in-memory transformation from raw CSV maps to typed model.ChaseTransaction entities.
type ChaseMapper struct {
	fileName   string
	s3URI      string
	fileHash   string
	accountID  string
	ingestedAt time.Time
}

// NewChaseMapper constructs a ChaseMapper by computing the SHA-256 digest of the source file.
func NewChaseMapper(
	ctx context.Context,
	filePath string,
	fileName string,
	s3URI string,
	accountID string,
) (*ChaseMapper, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}

	if strings.TrimSpace(accountID) == "" {
		return nil, errors.New("cannot create ChaseMapper: accountID is required")
	}

	fileHash, err := ComputeFileSHA256(ctx, filePath)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("failed to compute sha256 for file %s: %w", filePath, err)
	}

	return NewChaseMapperWithMetadata(
		fileName,
		s3URI,
		fileHash,
		accountID,
		time.Now().UTC(),
	), nil
}

// NewChaseMapperWithMetadata creates a ChaseMapper with explicit metadata parameters.
// This factory enables hermetic unit testing without requiring physical files on disk.
func NewChaseMapperWithMetadata(
	fileName string,
	s3URI string,
	fileHash string,
	accountID string,
	ingestedAt time.Time,
) *ChaseMapper {
	return &ChaseMapper{
		fileName:   fileName,
		s3URI:      s3URI,
		fileHash:   fileHash,
		accountID:  accountID,
		ingestedAt: ingestedAt,
	}
}

// FileName returns the source file name.
func (m *ChaseMapper) FileName() string {
	if m == nil {
		return ""
	}
	return m.fileName
}

// S3URI returns the destination or source S3 URI for datalake storage.
func (m *ChaseMapper) S3URI() string {
	if m == nil {
		return ""
	}
	return m.s3URI
}

// FileHash returns the SHA-256 hex digest of the raw source file.
func (m *ChaseMapper) FileHash() string {
	if m == nil {
		return ""
	}
	return m.fileHash
}

// AccountID returns the account identifier associated with this mapper.
func (m *ChaseMapper) AccountID() string {
	if m == nil {
		return ""
	}
	return m.accountID
}

// IngestedAt returns the ingestion timestamp applied to mapped transactions.
func (m *ChaseMapper) IngestedAt() time.Time {
	if m == nil {
		return time.Time{}
	}
	return m.ingestedAt
}

// MapRecords transforms raw record maps into typed ChaseTransaction slices using background context.
func (m *ChaseMapper) MapRecords(rawRecords []map[string]string) ([]model.ChaseTransaction, error) {
	if m == nil {
		return nil, errors.New("cannot map records: ChaseMapper instance is nil")
	}
	return m.MapRecordsWithContext(context.Background(), rawRecords)
}

// MapRecordsWithContext transforms raw records while respecting context cancellation.
func (m *ChaseMapper) MapRecordsWithContext(
	ctx context.Context,
	rawRecords []map[string]string,
) ([]model.ChaseTransaction, error) {
	if m == nil {
		return nil, errors.New("cannot map records: ChaseMapper instance is nil")
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if len(rawRecords) == 0 {
		return []model.ChaseTransaction{}, nil
	}

	transactions := make([]model.ChaseTransaction, 0, len(rawRecords))
	for _, record := range rawRecords {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		tx, err := m.MapRecord(record)
		if err != nil {
			return nil, err
		}
		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// MapRecord transforms a single raw record map into a model.ChaseTransaction with lineage.
func (m *ChaseMapper) MapRecord(record map[string]string) (model.ChaseTransaction, error) {
	if m == nil {
		return model.ChaseTransaction{}, errors.New("cannot map record: ChaseMapper instance is nil")
	}

	rowNumber := parseRowNumber(record)

	postingDate, err := m.resolvePostingDate(record)
	if err != nil {
		return model.ChaseTransaction{}, formatChaseMappingError(rowNumber, err)
	}

	amount, err := m.resolveAmount(record)
	if err != nil {
		return model.ChaseTransaction{}, formatChaseMappingError(rowNumber, err)
	}

	desc, err := m.resolveDescription(record)
	if err != nil {
		return model.ChaseTransaction{}, formatChaseMappingError(rowNumber, err)
	}

	txType, details := m.resolveTransactionTypeAndDetails(record)

	return model.ChaseTransaction{
		AccountID:       m.resolveAccountID(record),
		PostingDate:     postingDate,
		TransactionDate: m.resolveTransactionDate(record),
		Description:     desc,
		Amount:          amount,
		TransactionType: txType,
		Balance:         m.resolveBalance(record),
		Category:        m.resolveCategory(record),
		Details:         details,
		CheckOrSlipNum:  m.resolveCheckNumber(record),
		Memo:            m.resolveMemo(record),
		DataSource:      "chase",
		Source:          m.createSourceLineage(rowNumber),
	}, nil
}

func (m *ChaseMapper) createSourceLineage(rowNumber int64) model.SourceLineage {
	if m == nil {
		return model.SourceLineage{RowNumber: rowNumber}
	}
	return model.SourceLineage{
		S3URI:      m.s3URI,
		FileName:   m.fileName,
		RowNumber:  rowNumber,
		IngestedAt: m.ingestedAt,
		SHA256:     m.fileHash,
	}
}

func (m *ChaseMapper) resolveDescription(record map[string]string) (string, error) {
	desc := getFirstNonEmpty(record, "description", "Description")
	if strings.TrimSpace(desc) == "" {
		return "", errors.New("description is required")
	}
	return desc, nil
}

func (m *ChaseMapper) resolveTransactionTypeAndDetails(record map[string]string) (string, string) {
	txType := getFirstNonEmpty(record, "type", "Type")
	details := getFirstNonEmpty(record, "details", "Details")
	if txType == "" {
		txType = details
	}
	if txType == "" {
		txType = "TRANSACTION"
	}
	return txType, details
}

func (m *ChaseMapper) resolveAccountID(record map[string]string) string {
	if explicitAcc := getFirstNonEmpty(record, "account id", "accountId"); explicitAcc != "" {
		return explicitAcc
	}
	if m == nil {
		return ""
	}
	return m.accountID
}

func (m *ChaseMapper) resolveCategory(record map[string]string) string {
	return getFirstNonEmpty(record, "category", "Category")
}

func (m *ChaseMapper) resolveCheckNumber(record map[string]string) string {
	return getFirstNonEmpty(record, "check or slip #", "check or slip num", "Check or Slip #")
}

func (m *ChaseMapper) resolveMemo(record map[string]string) string {
	return getFirstNonEmpty(record, "memo", "Memo")
}

func (m *ChaseMapper) resolvePostingDate(record map[string]string) (time.Time, error) {
	val := getFirstNonEmpty(record, "posting date", "Posting Date", "post date", "Post Date")
	if val == "" {
		return time.Time{}, errors.New("posting date is required")
	}

	parsed, err := time.Parse("01/02/2006", val)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid posting date format %q: %w", val, err)
	}

	return time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, time.UTC), nil
}

func (m *ChaseMapper) resolveTransactionDate(record map[string]string) *time.Time {
	val := getFirstNonEmpty(record, "transaction date", "Transaction Date", "trans date", "Trans Date")
	if val == "" {
		return nil
	}

	parsed, err := time.Parse("01/02/2006", val)
	if err != nil {
		return nil
	}

	t := time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, time.UTC)
	return &t
}

func (m *ChaseMapper) resolveAmount(record map[string]string) (float64, error) {
	val := getFirstNonEmpty(record, "amount", "Amount")
	if val == "" {
		return 0, errors.New("amount is required")
	}

	clean := cleanNumericString(val)
	if clean == "" {
		return 0, errors.New("amount is required")
	}

	amount, err := strconv.ParseFloat(clean, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid amount value %q: %w", val, err)
	}

	return amount, nil
}

func (m *ChaseMapper) resolveBalance(record map[string]string) *float64 {
	val := getFirstNonEmpty(record, "balance", "Balance")
	if val == "" {
		return nil
	}

	clean := cleanNumericString(val)
	if clean == "" {
		return nil
	}

	bal, err := strconv.ParseFloat(clean, 64)
	if err != nil {
		return nil
	}

	return &bal
}

func cleanNumericString(val string) string {
	clean := strings.TrimSpace(val)
	clean = strings.ReplaceAll(clean, "$", "")
	clean = strings.ReplaceAll(clean, ",", "")
	clean = strings.ReplaceAll(clean, "+", "")
	return strings.TrimSpace(clean)
}

func getFirstNonEmpty(record map[string]string, keys ...string) string {
	if record == nil {
		return ""
	}
	for _, key := range keys {
		if val, ok := record[key]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func formatChaseMappingError(rowNumber int64, err error) error {
	if err == nil {
		return nil
	}
	if rowNumber > 0 {
		return fmt.Errorf("failed to map raw record to Chase transaction (row %d): %w", rowNumber, err)
	}
	return fmt.Errorf("failed to map raw record to Chase transaction: %w", err)
}
