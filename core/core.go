package core

import (
	"context"
	"github.com/rbastic/go-schemaless/models"
	"sync"
)

// Storage is a key-value storage backend
type Storage interface {
	// GetCell the cell designated (row key, column key, ref key)
	GetCell(ctx context.Context, rowKey string, columnKey string, refKey int64) (cell models.Cell, found bool, err error)

	// GetCellLatest returns the latest value for a given rowKey and columnKey, and a bool indicating if the key was present
	GetCellLatest(ctx context.Context, rowKey string, columnKey string) (cell models.Cell, found bool, err error)

	// PartitionRead returns 'limit' cells after 'location' from shard 'shard_no'
	PartitionRead(ctx context.Context, partitionNumber int, location string, value uint64, limit int) (cells []models.Cell, found bool, err error)

	// PutCell inits a cell with given row key, column key, and ref key
	PutCell(ctx context.Context, rowKey string, columnKey string, refKey int64, cell models.Cell) (err error)

	// ResetConnection reinitializes the connection for the shard responsible for a key
	ResetConnection(ctx context.Context, key string) error

	// Cleans up any resources, etc.
	Destroy(ctx context.Context) error
}

// KVStore is a sharded key-value store
type KVStore struct {
	continuum Chooser
	storages  map[string]Storage

	migration Chooser
	mstorages map[string]Storage

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

// New returns a KVStore that uses chooser to shard the keys across the provided shards
func New(chooser Chooser, shards []Shard) *KVStore {
	var buckets []string
	kv := &KVStore{
		continuum: chooser,
		storages:  make(map[string]Storage),
		// what about migration?
	}
	for _, shard := range shards {
		buckets = append(buckets, shard.Name)
		kv.AddShard(shard.Name, shard.Backend)
	}
	chooser.SetBuckets(buckets)
	return kv
}

func (kv *KVStore) GetCell(ctx context.Context, rowKey string, columnKey string, refKey int64) (cell models.Cell, found bool, err error) {
	var storage Storage
	var migStorage Storage

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.migration != nil {
		shard := kv.migration.Choose(rowKey)
		migStorage = kv.mstorages[shard]
		if migStorage != nil {
			val, ok, err := migStorage.GetCell(ctx, rowKey, columnKey, refKey)
			if err != nil {
				return val, false, err
			}
			if ok {
				return val, ok, err
			}
		}
	}

	shard := kv.continuum.Choose(rowKey)
	storage = kv.storages[shard]

	return storage.GetCell(ctx, rowKey, columnKey, refKey)
}

func (kv *KVStore) GetCellLatest(ctx context.Context, rowKey string, columnKey string) (cell models.Cell, found bool, err error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.migration != nil {
		shard := kv.migration.Choose(rowKey)
		migStorage := kv.mstorages[shard]

		if migStorage != nil {
			vals, ok, err := migStorage.GetCellLatest(ctx, rowKey, columnKey)
			if err != nil {
				return vals, ok, err
			}
			if ok {
				return vals, ok, nil
			}
		}
	}

	shard := kv.continuum.Choose(rowKey)
	storage := kv.storages[shard]
	return storage.GetCellLatest(ctx, rowKey, columnKey)
}

// PutCell
func (kv *KVStore) PutCell(ctx context.Context, rowKey string, columnKey string, refKey int64, cell models.Cell) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.migration != nil {
		shard := kv.migration.Choose(rowKey)
		storage := kv.mstorages[shard]
		if storage != nil {
			return storage.PutCell(ctx, rowKey, columnKey, refKey, cell)
		}
	}

	shard := kv.continuum.Choose(rowKey)
	storage := kv.storages[shard]
	return storage.PutCell(ctx, rowKey, columnKey, refKey, cell)
}

func (kv *KVStore) PartitionRead(ctx context.Context, partitionNumber int, location string, value uint64, limit int) (cells []models.Cell, found bool, err error) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.migration != nil {
		buckets := kv.migration.Buckets()
		shard := buckets[partitionNumber]
		migStorage := kv.mstorages[shard]

		if migStorage != nil {
			return migStorage.PartitionRead(ctx, partitionNumber, location, value, limit)
		}
	}

	buckets := kv.continuum.Buckets()
	shard := buckets[partitionNumber]
	storage := kv.storages[shard]

	return storage.PartitionRead(ctx, partitionNumber, location, value, limit)
}

// ResetConnection implements Storage.ResetConnection()
func (kv *KVStore) ResetConnection(ctx context.Context, key string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.migration != nil {
		shard := kv.migration.Choose(key)
		migStorage := kv.mstorages[shard]

		if migStorage != nil {
			err := migStorage.ResetConnection(ctx, key)
			if err != nil {
				return err
			}
		}
	}

	shard := kv.continuum.Choose(key)
	storage := kv.storages[shard]
	return storage.ResetConnection(ctx, key)
}

// Destroy implements Storage.Destroy()
func (kv *KVStore) Destroy(ctx context.Context) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.migration != nil {
		for _, migStorage := range kv.mstorages {
			err := migStorage.Destroy(ctx)
			if err != nil {
				return err
			}
		}
		return nil
	}
	for _, store := range kv.storages {
		err := store.Destroy(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// AddShard adds a shard from the list of known shards
func (kv *KVStore) AddShard(shard string, storage Storage) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.storages[shard] = storage
}

// DeleteShard removes a shard from the list of known shards
func (kv *KVStore) DeleteShard(shard string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.storages, shard)
}

// BeginMigration begins a continuum migration.  All the shards in the new
// continuum must already be known to the KVStore via AddShard().
func (kv *KVStore) BeginMigration(continuum Chooser) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.migration = continuum
	kv.mstorages = kv.storages
}

// BeginMigrationWithShards begins a continuum migration using the new set of shards.
func (kv *KVStore) BeginMigrationWithShards(continuum Chooser, shards []Shard) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	var buckets []string
	mstorages := make(map[string]Storage)
	for _, shard := range shards {
		buckets = append(buckets, shard.Name)
		mstorages[shard.Name] = shard.Backend
	}

	continuum.SetBuckets(buckets)

	kv.migration = continuum
	kv.mstorages = mstorages
}

// EndMigration ends a continuum migration and marks the migration continuum
// as the new primary
func (kv *KVStore) EndMigration() {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.continuum = kv.migration
	kv.migration = nil

	kv.storages = kv.mstorages
	kv.mstorages = nil
}
