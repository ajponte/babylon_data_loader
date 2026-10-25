package mongo

import (
	"fmt"

	"babylon/dataloader/internal/domain/model"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
)

const (
	chaseCollection     = string(model.DestChaseChecking)    // "transactions_chase"
	fidelityCollection  = string(model.DestFidelityPosition) // "positions_fidelity"
	syntheticCollection = string(model.DestSynthetic)        // "transactions_synthetic"

	// Chase BSON field names used across compound unique index keys and upsert filters.
	fieldAccountID       = "accountId"
	fieldPostingDate     = "postingDate"
	fieldDescription     = "description"
	fieldAmount          = "amount"
	fieldSourceRowNumber = "source.rowNumber"

	// Fidelity BSON field names used across compound unique index keys and upsert filters.
	fieldAccountNumber = "accountNumber"
	fieldSymbol        = "symbol"
	fieldAsOfDate      = "asOfDate"

	// flattenedSourceFieldsCount estimates the extra fields produced when flattening the lineage subdocument.
	flattenedSourceFieldsCount = 4
)

// chaseIndexes defines legacy-compatible indexes for transactions_chase.
// The compound unique index is sparse to preserve legacy behavior where documents
// missing index fields are excluded from uniqueness enforcement.
func chaseIndexes() []indexSpec {
	return []indexSpec{
		{
			name: "idx_chase_account_posting_date",
			keys: bson.D{
				{Key: fieldAccountID, Value: 1},
				{Key: fieldPostingDate, Value: -1},
			},
			unique: false,
			sparse: false,
		},
		{
			name: "idx_chase_account_post_desc_amount_row",
			keys: bson.D{
				{Key: fieldAccountID, Value: 1},
				{Key: fieldPostingDate, Value: 1},
				{Key: fieldDescription, Value: 1},
				{Key: fieldAmount, Value: 1},
				{Key: fieldSourceRowNumber, Value: 1},
			},
			unique: true,
			sparse: true,
		},
	}
}

// fidelityIndexes defines legacy-compatible indexes for positions_fidelity.
// The compound unique index is non-sparse because every position document
// strictly requires accountNumber, symbol, and asOfDate for identification.
func fidelityIndexes() []indexSpec {
	return []indexSpec{
		{
			name: "idx_account_symbol_asof",
			keys: bson.D{
				{Key: fieldAccountNumber, Value: 1},
				{Key: fieldSymbol, Value: 1},
				{Key: fieldAsOfDate, Value: 1},
			},
			unique: true,
			sparse: false,
		},
	}
}

// chaseFilter builds an upsert filter for Chase transactions matching the unique compound index keys byte-for-byte.
// Invariant: filter keys strictly equal unique index keys to ensure atomic upsert idempotency.
func chaseFilter(d chaseDoc) bson.D {
	return bson.D{
		{Key: fieldAccountID, Value: d.AccountID},
		{Key: fieldPostingDate, Value: d.PostingDate.UTC()},
		{Key: fieldDescription, Value: d.Description},
		{Key: fieldAmount, Value: d.Amount},
		{Key: fieldSourceRowNumber, Value: d.Source.RowNumber},
	}
}

// fidelityFilter builds an upsert filter for Fidelity positions matching the unique compound index keys byte-for-byte.
// Invariant: filter keys strictly equal unique index keys to ensure atomic upsert idempotency.
func fidelityFilter(d fidelityDoc) bson.D {
	return bson.D{
		{Key: fieldAccountNumber, Value: d.AccountNumber},
		{Key: fieldSymbol, Value: d.Symbol},
		{Key: fieldAsOfDate, Value: d.AsOfDate.UTC()},
	}
}

// setDocument marshals a document and transforms nested lineage into dotted notation.
// Dotted lineage preservation (decision E6): flattening "source" ensures existing disk fields
// (such as historical s3Uri) are preserved rather than overwritten by subdocument replacement.
func setDocument(doc any) (bson.D, error) {
	data, err := bson.Marshal(doc)
	if err != nil {
		return nil, err
	}
	var raw bson.D
	if unmarshalErr := bson.Unmarshal(data, &raw); unmarshalErr != nil {
		return nil, unmarshalErr
	}

	return bson.D{{Key: "$set", Value: flattenSourceLineage(raw)}}, nil
}

// flattenSourceLineage transforms the top-level "source" subdocument into dotted "source.<key>" entries.
// Flattening preserves historical lineage fields on disk (decision E6).
func flattenSourceLineage(raw bson.D) bson.D {
	flattened := make(bson.D, 0, len(raw)+flattenedSourceFieldsCount)
	for _, elem := range raw {
		if elem.Key == "source" {
			flattened = appendDottedLineage(flattened, elem.Value)
			continue
		}
		flattened = append(flattened, elem)
	}
	return flattened
}

// appendDottedLineage unpacks a lineage subdocument into dotted "source.<key>" elements.
func appendDottedLineage(dst bson.D, val any) bson.D {
	sub, ok := val.(bson.D)
	if !ok {
		return dst
	}
	for _, elem := range sub {
		dst = append(dst, bson.E{
			Key:   "source." + elem.Key,
			Value: elem.Value,
		})
	}
	return dst
}

// collectionFor maps a destination to its collection or returns model.ErrInvalidDestination.
// Per decision D1, syntheticCollection ("transactions_synthetic") defines zero catalog indexes,
// but maps validly to receive Chase-shaped transaction models.
func collectionFor(dest model.Destination) (string, error) {
	switch dest {
	// DestChaseChecking and DestChaseCreditCard share the exact same string value ("transactions_chase").
	// In Go, listing both in a switch statement causes a duplicate case compile-time error.
	// Matching DestChaseChecking covers both checking and credit card destinations.
	case model.DestChaseChecking:
		return chaseCollection, nil
	case model.DestFidelityPosition:
		return fidelityCollection, nil
	case model.DestSynthetic:
		return syntheticCollection, nil
	default:
		return "", model.ErrInvalidDestination
	}
}

type writePlan struct {
	collection string
	models     []mongodrv.WriteModel
}

// buildWritePlan coordinates collection resolution and batch write model planning.
func buildWritePlan(batch *model.IngestionBatch) (writePlan, error) {
	if batch == nil {
		return writePlan{}, model.ErrNilBatch
	}
	if batch.RecordCount < 0 {
		return writePlan{}, model.ErrNegativeRecordCount
	}

	coll, err := collectionFor(batch.Destination)
	if err != nil {
		return writePlan{}, err
	}

	models, err := routeWriteModels(coll, batch)
	if err != nil {
		return writePlan{}, err
	}

	return writePlan{collection: coll, models: models}, nil
}

// routeWriteModels routes write model generation by collection target and data source.
func routeWriteModels(coll string, batch *model.IngestionBatch) ([]mongodrv.WriteModel, error) {
	switch coll {
	case chaseCollection:
		return planChaseBatch(batch, dataSourceChase)
	case syntheticCollection:
		return planChaseBatch(batch, dataSourceSynthetic)
	case fidelityCollection:
		return planFidelityBatch(batch)
	default:
		return nil, model.ErrInvalidDestination
	}
}

func planChaseBatch(batch *model.IngestionBatch, dataSource string) ([]mongodrv.WriteModel, error) {
	txs, err := batch.AsChaseTransactions()
	if err != nil {
		return nil, err
	}
	return buildChaseWriteModels(txs, dataSource)
}

func planFidelityBatch(batch *model.IngestionBatch) ([]mongodrv.WriteModel, error) {
	positions, err := batch.AsFidelityPositions()
	if err != nil {
		return nil, err
	}
	return buildFidelityWriteModels(positions)
}

func buildChaseWriteModels(txs []model.ChaseTransaction, dataSource string) ([]mongodrv.WriteModel, error) {
	models := make([]mongodrv.WriteModel, 0, len(txs))
	for _, tx := range txs {
		m, err := buildChaseUpdateModel(tx, dataSource)
		if err != nil {
			return nil, err
		}
		models = append(models, m)
	}
	return models, nil
}

func buildChaseUpdateModel(tx model.ChaseTransaction, dataSource string) (*mongodrv.UpdateOneModel, error) {
	d := toChaseDoc(tx, dataSource)
	u, err := setDocument(d)
	if err != nil {
		return nil, fmt.Errorf("mongo: build chase update model: %w", err)
	}
	return mongodrv.NewUpdateOneModel().
		SetFilter(chaseFilter(d)).
		SetUpdate(u).
		SetUpsert(true), nil
}

func buildFidelityWriteModels(positions []model.FidelityPosition) ([]mongodrv.WriteModel, error) {
	models := make([]mongodrv.WriteModel, 0, len(positions))
	for _, p := range positions {
		m, err := buildFidelityUpdateModel(p)
		if err != nil {
			return nil, err
		}
		models = append(models, m)
	}
	return models, nil
}

func buildFidelityUpdateModel(p model.FidelityPosition) (*mongodrv.UpdateOneModel, error) {
	if p.AsOfDate.IsZero() {
		return nil, model.ErrMissingAsOfDate
	}
	d := toFidelityDoc(p)
	u, err := setDocument(d)
	if err != nil {
		return nil, fmt.Errorf("mongo: build fidelity update model: %w", err)
	}
	return mongodrv.NewUpdateOneModel().
		SetFilter(fidelityFilter(d)).
		SetUpdate(u).
		SetUpsert(true), nil
}
