package schemaless

import (
	"context"
	"fmt"

	"github.com/dgryski/go-metro"
	jh "github.com/dgryski/go-shardedkv/choosers/jump"
	"github.com/rbastic/go-schemaless/core"
	"github.com/rbastic/go-schemaless/models"
)

// Storage is a key-value storage backend
type Storage interface {
	// Get the cell designated (row key, column key, ref key)
	Get(ctx context.Context, tblName, rowKey, columnKey string, refKey int64) (cell models.Cell, found bool, err error)

	// GetLatest returns the latest value for a given rowKey and columnKey, and a bool indicating if the key was present
	GetLatest(ctx context.Context, tblName, rowKey, columnKey string) (cell models.Cell, found bool, err error)

	// PartitionRead returns 'limit' cells after 'location' from shard 'shard_no'
	PartitionRead(ctx context.Context, tblName string, partitionNumber int, location string, value int64, limit int) (cells []models.Cell, found bool, err error)

	// Put inits a cell with given row key, column key, and ref key
	Put(ctx context.Context, tblName, rowKey, columnKey string, refKey int64, body string) (err error)

	// FindPartition returns the partition number for a specific rowKey
	FindPartition(tblName, rowKey string) int

	// ResetConnection reinitializes the connection for the shard responsible for a key
	ResetConnection(ctx context.Context, key string) error

	// Destroy cleans up any resources, etc.
	Destroy(ctx context.Context) error
}

// DataStore is our overall datastore structure, backed by at least one
// KVStore.
type DataStore struct {
	sources map[string]*core.KVStore
	// no mutex is required at this level -- only in core
	// mu sync.Mutex
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

func (ds *DataStore) WithSources(tblName string, shards []core.Shard) *DataStore {
	chooser := jh.New(hash64)
	kv := core.New(chooser, shards)
	ds.sources[tblName] = kv
	return ds
}

func (ds *DataStore) WithName(tblName string, bucketName string) *DataStore {
	tbl, err := ds.getTable(tblName)
	if err != nil {
		panic(err)
	}

	tbl = tbl.WithName(tblName)
	return ds
}

// New is an empty constructor for DataStore.
func New() *DataStore {
	return &DataStore{sources: make(map[string]*core.KVStore)}
}

func (ds *DataStore) getTable(tblName string) (*core.KVStore, error) {
	if tbl, ok := ds.sources[tblName]; ok {
		return tbl, nil
	}

	// Return any core.KVStore if we haven't heard of the table yet (it
	// could be an index table)
	for _, tbl := range ds.sources {
		return tbl, nil
	}

	return nil, fmt.Errorf("table %s does not exist\n", tblName)
}

// Get implements Storage.Get()
func (ds *DataStore) Get(ctx context.Context, tblName, rowKey, columnKey string, refKey int64) (cell models.Cell, found bool, err error) {
	source, err := ds.getTable(tblName)
	if err != nil {
		return models.Cell{}, false, err
	}

	return source.Get(ctx, tblName, rowKey, columnKey, refKey)
}

// GetLatest implements Storage.GetLatest()
func (ds *DataStore) GetLatest(ctx context.Context, tblName, rowKey, columnKey string) (cell models.Cell, found bool, err error) {
	source, err := ds.getTable(tblName)
	if err != nil {
		return models.Cell{}, false, err
	}

	return source.GetLatest(ctx, tblName, rowKey, columnKey)
}

// PartitionRead implements Storage.PartitionRead()
func (ds *DataStore) PartitionRead(ctx context.Context, tblName string, partitionNumber int, location string, value int64, limit int) (cells []models.Cell, found bool, err error) {

	source, err := ds.getTable(tblName)
	if err != nil {
		return nil, false, err
	}

	return source.PartitionRead(ctx, tblName, partitionNumber, location, value, limit)
}

// Put implements Storage.Put()
func (ds *DataStore) Put(ctx context.Context, tblName, rowKey, columnKey string, refKey int64, body string) error {
	source, err := ds.getTable(tblName)
	if err != nil {
		return err
	}

	return source.Put(ctx, tblName, rowKey, columnKey, refKey, body)
}

// FindPartition implements Storage.FindPartition()
func (ds *DataStore) FindPartition(tblName, rowKey string) (int, error) {
	source, err := ds.getTable(tblName)
	if err != nil {
		return -1, err
	}

	return source.FindPartition(tblName, rowKey)
}

// ResetConnection implements Storage.ResetConnection()
func (ds *DataStore) ResetConnection(ctx context.Context, tblName, rowKey string) error {
	source, err := ds.getTable(tblName)
	if err != nil {
		return err
	}

	return source.ResetConnection(ctx, rowKey)
}

// Destroy implements Storage.Destroy()
func (ds *DataStore) Destroy(ctx context.Context) error {
	for _, source := range ds.sources {
		err := source.Destroy(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
