package mongo

import (
	"babylon/dataloader/internal/domain/model"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
)

const (
	chaseCollection     = string(model.DestChaseChecking)    // "transactions_chase"
	fidelityCollection  = string(model.DestFidelityPosition) // "positions_fidelity"
	syntheticCollection = string(model.DestSynthetic)        // "transactions_synthetic"

	fieldAccountID   = "accountId"
	fieldPostingDate = "postingDate"

	// flattenedSourceFieldsCount estimates the extra fields produced when flattening the lineage subdocument.
	flattenedSourceFieldsCount = 4
)

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
				{Key: "description", Value: 1},
				{Key: "amount", Value: 1},
				{Key: "source.rowNumber", Value: 1},
			},
			unique: true,
			sparse: true,
		},
	}
}

func fidelityIndexes() []indexSpec {
	return []indexSpec{
		{
			name: "idx_account_symbol_asof",
			keys: bson.D{
				{Key: "accountNumber", Value: 1},
				{Key: "symbol", Value: 1},
				{Key: "asOfDate", Value: 1},
			},
			unique: true,
			sparse: false,
		},
	}
}

func chaseFilter(d chaseDoc) bson.D {
	return bson.D{
		{Key: fieldAccountID, Value: d.AccountID},
		{Key: fieldPostingDate, Value: d.PostingDate.UTC()},
		{Key: "description", Value: d.Description},
		{Key: "amount", Value: d.Amount},
		{Key: "source.rowNumber", Value: d.Source.RowNumber},
	}
}

func fidelityFilter(d fidelityDoc) bson.D {
	return bson.D{
		{Key: "accountNumber", Value: d.AccountNumber},
		{Key: "symbol", Value: d.Symbol},
		{Key: "asOfDate", Value: d.AsOfDate.UTC()},
	}
}

func setDocument(doc any) (bson.D, error) {
	data, err := bson.Marshal(doc)
	if err != nil {
		return nil, err
	}
	var raw bson.D
	if unmarshalErr := bson.Unmarshal(data, &raw); unmarshalErr != nil {
		return nil, unmarshalErr
	}

	flattened := make(bson.D, 0, len(raw)+flattenedSourceFieldsCount)
	for _, elem := range raw {
		if elem.Key == "source" {
			if sub, ok := elem.Value.(bson.D); ok {
				for _, subElem := range sub {
					flattened = append(flattened, bson.E{
						Key:   "source." + subElem.Key,
						Value: subElem.Value,
					})
				}
			}
			continue
		}
		flattened = append(flattened, elem)
	}

	return bson.D{{Key: "$set", Value: flattened}}, nil
}

// collectionFor maps a destination to its collection or returns model.ErrInvalidDestination.
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

func buildWritePlan(batch *model.IngestionBatch) (writePlan, error) {
	if batch == nil {
		return writePlan{}, model.ErrNilBatch
	}

	coll, err := collectionFor(batch.Destination)
	if err != nil {
		return writePlan{}, err
	}

	var models []mongodrv.WriteModel
	switch coll {
	case chaseCollection:
		var txs []model.ChaseTransaction
		txs, err = batch.AsChaseTransactions()
		if err != nil {
			return writePlan{}, err
		}
		models, err = buildChaseWriteModels(txs, dataSourceChase)
		if err != nil {
			return writePlan{}, err
		}
	case syntheticCollection:
		var txs []model.ChaseTransaction
		txs, err = batch.AsChaseTransactions()
		if err != nil {
			return writePlan{}, err
		}
		models, err = buildChaseWriteModels(txs, dataSourceSynthetic)
		if err != nil {
			return writePlan{}, err
		}
	case fidelityCollection:
		var positions []model.FidelityPosition
		positions, err = batch.AsFidelityPositions()
		if err != nil {
			return writePlan{}, err
		}
		models, err = buildFidelityWriteModels(positions)
		if err != nil {
			return writePlan{}, err
		}
	default:
		return writePlan{}, model.ErrInvalidDestination
	}

	return writePlan{collection: coll, models: models}, nil
}

func buildChaseWriteModels(txs []model.ChaseTransaction, dataSource string) ([]mongodrv.WriteModel, error) {
	models := make([]mongodrv.WriteModel, 0, len(txs))
	for _, tx := range txs {
		d := toChaseDoc(tx, dataSource)
		u, err := setDocument(d)
		if err != nil {
			return nil, err
		}
		model := mongodrv.NewUpdateOneModel().
			SetFilter(chaseFilter(d)).
			SetUpdate(u).
			SetUpsert(true)
		models = append(models, model)
	}
	return models, nil
}

func buildFidelityWriteModels(positions []model.FidelityPosition) ([]mongodrv.WriteModel, error) {
	models := make([]mongodrv.WriteModel, 0, len(positions))
	for _, p := range positions {
		if p.AsOfDate.IsZero() {
			return nil, model.ErrMissingAsOfDate
		}
		d := toFidelityDoc(p)
		u, err := setDocument(d)
		if err != nil {
			return nil, err
		}
		model := mongodrv.NewUpdateOneModel().
			SetFilter(fidelityFilter(d)).
			SetUpdate(u).
			SetUpsert(true)
		models = append(models, model)
	}
	return models, nil
}
