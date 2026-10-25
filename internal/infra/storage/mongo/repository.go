package mongo

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"babylon/dataloader/internal/domain/port"
)

var _ port.Repository = (*Repository)(nil)

const (
	// DefaultDatabase defines the default datalake database name.
	DefaultDatabase = "datalake"
	// DefaultTimeout defines the default connect and ping timeout.
	DefaultTimeout = 10 * time.Second
	// dataSyncCollection defines the audit log collection name.
	dataSyncCollection = "dataSync"
)

// ErrMissingURI indicates that no connection URI was provided.
var ErrMissingURI = errors.New("mongo: connection URI is required")

// Config contains MongoDB connection settings.
type Config struct {
	URI      string        // required; never logged or included in errors
	Database string        // default DefaultDatabase
	Timeout  time.Duration // connect + ping budget; default DefaultTimeout
}

func (c Config) withDefaults() Config {
	if c.Database == "" {
		c.Database = DefaultDatabase
	}
	if c.Timeout <= 0 {
		c.Timeout = DefaultTimeout
	}
	return c
}

type database interface {
	Collection(name string) collection
}

type collection interface {
	InsertOne(ctx context.Context, doc any, opts ...*options.InsertOneOptions) (*mongodrv.InsertOneResult, error)
	BulkWrite(
		ctx context.Context,
		models []mongodrv.WriteModel,
		opts ...*options.BulkWriteOptions,
	) (*mongodrv.BulkWriteResult, error)
	CreateIndexes(ctx context.Context, models []mongodrv.IndexModel) error
}

type realDatabase struct {
	db *mongodrv.Database
}

func (d *realDatabase) Collection(name string) collection {
	return &realCollection{coll: d.db.Collection(name)}
}

type realCollection struct {
	coll *mongodrv.Collection
}

func (c *realCollection) InsertOne(
	ctx context.Context,
	doc any,
	opts ...*options.InsertOneOptions,
) (*mongodrv.InsertOneResult, error) {
	return c.coll.InsertOne(ctx, doc, opts...)
}

func (c *realCollection) BulkWrite(
	ctx context.Context,
	models []mongodrv.WriteModel,
	opts ...*options.BulkWriteOptions,
) (*mongodrv.BulkWriteResult, error) {
	return c.coll.BulkWrite(ctx, models, opts...)
}

func (c *realCollection) CreateIndexes(ctx context.Context, models []mongodrv.IndexModel) error {
	_, err := c.coll.Indexes().CreateMany(ctx, models)
	return err
}

// Repository manages persistence in MongoDB.
type Repository struct {
	client  *mongodrv.Client
	db      database
	mu      sync.Mutex
	ensured map[string]bool
	now     func() time.Time
}

// NewRepository connects to MongoDB with the given configuration.
// It verifies connectivity via Ping and performs zero index creation.
func NewRepository(ctx context.Context, cfg Config) (*Repository, error) {
	if cfg.URI == "" {
		return nil, ErrMissingURI
	}
	cfg = cfg.withDefaults()

	timeoutCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	client, err := mongodrv.Connect(timeoutCtx, options.Client().ApplyURI(cfg.URI))
	if err != nil {
		return nil, fmt.Errorf("mongo: connect: %w", sanitizeURIError(err, cfg.URI))
	}
	if pingErr := client.Ping(timeoutCtx, nil); pingErr != nil {
		_ = client.Disconnect(timeoutCtx)
		return nil, fmt.Errorf("mongo: ping: %w", sanitizeURIError(pingErr, cfg.URI))
	}

	return &Repository{
		client:  client,
		db:      &realDatabase{db: client.Database(cfg.Database)},
		ensured: make(map[string]bool),
		now:     time.Now,
	}, nil
}

func newRepository(db database) *Repository {
	return &Repository{
		db:      db,
		ensured: make(map[string]bool),
		now:     time.Now,
	}
}

// timeNow returns the current timestamp using the injected clock function r.now, falling back
// to time.Now if unconfigured. This enables deterministic elapsed duration calculations in unit tests.
func (r *Repository) timeNow() time.Time {
	if r != nil && r.now != nil {
		return r.now()
	}
	return time.Now()
}

// Close disconnects the MongoDB client. It is nil-safe.
func (r *Repository) Close(ctx context.Context) error {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.Disconnect(ctx)
}

type indexSpec struct {
	name   string
	keys   bson.D
	unique bool
	sparse bool
}

type collectionIndexes struct {
	collection string
	indexes    []indexSpec
}

// indexCatalog defines the deterministic legacy index catalog for datalake collections.
// Invariant (Decision D1): transactions_synthetic has zero registered index specifications
// to avoid IndexOptionsConflict on live Atlas clusters and preserve legacy parity.
func indexCatalog() []collectionIndexes {
	return []collectionIndexes{
		{collection: dataSyncCollection, indexes: dataSyncIndexes()},
		{collection: chaseCollection, indexes: chaseIndexes()},
		{collection: fidelityCollection, indexes: fidelityIndexes()},
	}
}

func toIndexModel(spec indexSpec) mongodrv.IndexModel {
	opts := options.Index().SetName(spec.name)
	if spec.unique {
		opts.SetUnique(true)
	}
	if spec.sparse {
		opts.SetSparse(true)
	}
	return mongodrv.IndexModel{Keys: spec.keys, Options: opts}
}

func indexSpecsFor(coll string) []indexSpec {
	for _, ci := range indexCatalog() {
		if ci.collection == coll {
			return ci.indexes
		}
	}
	return nil
}

func toIndexModels(specs []indexSpec) []mongodrv.IndexModel {
	models := make([]mongodrv.IndexModel, len(specs))
	for i, spec := range specs {
		models[i] = toIndexModel(spec)
	}
	return models
}

func (r *Repository) ensureIndexes(ctx context.Context, coll string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.ensured == nil {
		r.ensured = make(map[string]bool)
	}
	if r.ensured[coll] {
		return nil
	}

	specs := indexSpecsFor(coll)
	if len(specs) == 0 {
		return nil
	}

	models := toIndexModels(specs)
	if err := r.db.Collection(coll).CreateIndexes(ctx, models); err != nil {
		return fmt.Errorf("mongo: create indexes on %s: %w", coll, err)
	}

	r.ensured[coll] = true
	return nil
}

// EnsureIndexes creates all indexes declared in the index catalog in deterministic order.
func (r *Repository) EnsureIndexes(ctx context.Context) error {
	for _, ci := range indexCatalog() {
		if err := r.ensureIndexes(ctx, ci.collection); err != nil {
			return err
		}
	}
	return nil
}

func sanitizeURIError(err error, rawURI string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if rawURI != "" {
		msg = strings.ReplaceAll(msg, rawURI, "[REDACTED_URI]")
	}
	if u, parseErr := url.Parse(rawURI); parseErr == nil && u != nil && u.User != nil {
		if pass, hasPass := u.User.Password(); hasPass && pass != "" {
			msg = strings.ReplaceAll(msg, pass, "[REDACTED]")
		}
		if username := u.User.Username(); username != "" {
			msg = strings.ReplaceAll(msg, username, "[REDACTED]")
		}
	}
	re := regexp.MustCompile(`://([^:@\s]+):([^@\s]+)@`)
	return errors.New(re.ReplaceAllString(msg, "://[REDACTED]:[REDACTED]@"))
}
