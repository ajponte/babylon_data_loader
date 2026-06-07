package model

import "time"

// SyncLog represents a record in the dataSync collection.
type SyncLog struct {
	CollectionName  string    `bson:"collection_name"  json:"collectionName"`
	SyncTimestamp   time.Time `bson:"sync_timestamp"   json:"syncTimestamp"`
	RecordsUploaded int64     `bson:"records_uploaded" json:"recordsUploaded"`
}
