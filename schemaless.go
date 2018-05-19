package schemaless

import (
	"context"
	"github.com/dgryski/go-metro"
	jh "github.com/dgryski/go-shardedkv/choosers/jump"
	"github.com/rbastic/go-schemaless/core"
	"github.com/rbastic/go-schemaless/models"
	"sync"
)

// Storage is a key-value storage backend
type Storage interface {
	// GetCell the cell designated (row key, column key, ref key)
	GetCell(ctx context.Context, rowKey string, columnKey string, refKey int64) (cell models.Cell, found bool, err error)

	// GetCellLatest returns the latest value for a given rowKey and columnKey, and a bool indicating if the key was present
	GetCellLatest(ctx context.Context, rowKey string, columnKey string) (cell models.Cell, found bool, err error)

	// GetCellsForShard returns 'limit' cells after 'location' from shard 'shard_no'
	GetCellsForShard(ctx context.Context, shardNumber int, location string, value interface{}, limit int) (cells []models.Cell, found bool, err error)

	// PutCell inits a cell with given row key, column key, and ref key
	PutCell(ctx context.Context, rowKey string, columnKey string, refKey int64, cell models.Cell) (err error)

	// ResetConnection reinitializes the connection for the shard responsible for a key
	ResetConnection(ctx context.Context, key string) error

	// Cleans up any resources, etc.
	Destroy(ctx context.Context) error
}

// DataStore is our overall datastore structure, backed by at least one
// KVStore. Flexible double-writing migration strategies could require more
// than one being listed in this structure below.
type DataStore struct {
	active *core.KVStore
	// we avoid holding the lock during a call to a storage engine, which may block
	mu sync.Mutex
}

// Chooser maps keys to shards
type Chooser interface {
	// SetBuckets sets the list of known buckets from which the chooser should select
	SetBuckets([]string) error
	// Choose returns a bucket for a given key
	Choose(key string) string
	// Buckets returns the list of known buckets
	Buckets() []string
}

// Shard is a named storage backend
type Shard struct {
	Name    string
	Backend Storage
}

func hash64(b []byte) uint64 { return metro.Hash64(b, 0) }

// New returns a DatStore structure
func New(shards []core.Shard) *DataStore {
	chooser := jh.New(hash64)
	kv := core.New(chooser, shards)
	return &DataStore{active: kv}
}

func (ds *DataStore) GetCell(ctx context.Context, rowKey string, columnKey string, refKey int64) (cell models.Cell, found bool, err error) {
	return ds.active.GetCell(ctx, rowKey, columnKey, refKey)
}

func (ds *DataStore) GetCellLatest(ctx context.Context, rowKey string, columnKey string) (cell models.Cell, found bool, err error) {
	return ds.active.GetCellLatest(ctx, rowKey, columnKey)
}

func (ds *DataStore) GetCellsForShard(ctx context.Context, shardNumber int, location string, value interface{}, limit int) (cells []models.Cell, found bool, err error) {
	return ds.active.GetCellsForShard(ctx, shardNumber, location, value, limit)
}

// PutCell
func (ds *DataStore) PutCell(ctx context.Context, rowKey string, columnKey string, refKey int64, cell models.Cell) error {
	return ds.active.PutCell(ctx, rowKey, columnKey, refKey, cell)
}

// ResetConnection implements Storage.ResetConnection()
func (ds *DataStore) ResetConnection(ctx context.Context, key string) error {
	return ds.active.ResetConnection(ctx, key)
}

// Destroy implements Storage.Destroy()
func (ds *DataStore) Destroy(ctx context.Context) error {
	return ds.active.Destroy(ctx)
}
