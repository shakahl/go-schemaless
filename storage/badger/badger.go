package badger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/rbastic/go-schemaless/models"
	"go.uber.org/zap"
)

type Storage struct {
	rowKeyDelim string
	opened      bool
	dir         string
	db          *badger.DB
	sugar       *zap.SugaredLogger
	mu          sync.Mutex
	immutable   bool
}

func New(dir string) (*Storage, error) {
	// TODO(rbastic): need to be able to tune/externalize badger options,
	// possibly?
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir

	var m sync.Mutex

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	if db == nil {
		// shouldn't happen, but just in case
		return nil, errors.New("badger db is nil")
	}

	// TODO(rbastic): externalize zap logger and/or just use global logger,
	// since that can be redirected.
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}

	return &Storage{
		rowKeyDelim: ":",
		dir:         dir,
		db:          db,
		opened:      true,
		mu:          m,
		sugar:       logger.Sugar(),
		immutable:   true,
	}, nil
}

func (s *Storage) get(key string) ([]byte, bool, error) {
	s.mu.Lock()
	if !s.opened {
		return nil, false, errors.New("database is closed")
	}
	s.mu.Unlock()

	var valCopy []byte

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			// This func with val would only be called if
			// item.Value encounters no error.
			valCopy = make([]byte, len(val))
			copy(valCopy, val)
			return nil
		})
	})

	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}

	return valCopy, true, nil
}

func (s *Storage) GetCell(ctx context.Context, rowKey string, columnKey string, refKey int64) (cell models.Cell, found bool, err error) {
	var (
		data []byte
	)

	queryKey := strings.Join([]string{"r", rowKey, columnKey, fmt.Sprintf("%d", refKey)}, s.rowKeyDelim)
	data, found, err = s.get(queryKey)
	if err != nil {
		return cell, false, err
	}

	if found {
		err = json.Unmarshal(data, &cell)
		if err != nil {
			return cell, false, err
		}
	}

	cell.RowKey = rowKey
	cell.ColumnName = columnKey
	// FIXME(rbastic): RefKey is doubly encoded in both the object and the key

	return cell, found, err
}

func (s *Storage) GetCellLatest(ctx context.Context, rowKey, columnKey string) (cell models.Cell, found bool, err error) {

	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		// We use the key-only scan because of what the Badger
		// documentation claims, regarding how it often requires a
		// memory-only scan due to the indexes being held almost
		// entirely in memory.
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte("r" + s.rowKeyDelim + rowKey + s.rowKeyDelim + columnKey)

		maxRefKey := 0
		var maxRowKey []byte
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)

			cmps := strings.Split(string(k), s.rowKeyDelim)
			refKey, err := strconv.Atoi(cmps[3])
			if err != nil {
				return err
			}

			if refKey > maxRefKey {
				maxRefKey = refKey
				maxRowKey = k
			}
		}

		if maxRefKey == 0 {
			return badger.ErrKeyNotFound
		}

		item, err := txn.Get(maxRowKey)
		if err != nil {
			return err
		}

		var valCopy []byte
		errn := item.Value(func(val []byte) error {
			// This func with val would only be called if
			// item.Value encounters no error.
			valCopy = make([]byte, len(val))
			copy(valCopy, val)
			return nil
		})

		if errn != nil {
			return errn
		}

		errn = json.Unmarshal(valCopy, &cell)
		if errn != nil {
			return errn
		}

		found = true
		return nil
	})

	if err != nil && err == badger.ErrKeyNotFound {
		return cell, false, nil
	}

	return cell, found, err
}

func (s *Storage) PartitionRead(ctx context.Context, partitionNumber int, location string, value uint64, limit int) (cells []models.Cell, found bool, err error) {
	// partitionNumber is ignored, since we already do that from the top-level
	var queryKeys []string

	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions

		// We use the key-only scan because of what the Badger
		// documentation claims, regarding how it often requires a
		// memory-only scan due to the indexes being held almost
		// entirely in memory.

		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		var locPrefix string
		switch location {
		case "added_at":
			locPrefix = "a"
		case "created_at", "timestamp":
			locPrefix = "t"
		default:
			return errors.New("unrecognized location " + location)
		}

		prefix := []byte(locPrefix + s.rowKeyDelim)

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)

			cmps := strings.Split(string(k), s.rowKeyDelim)

			intVal, err := strconv.Atoi(cmps[1])
			if err != nil {
				return err
			}

			if uint64(intVal) > value {
				pk := strings.Join(cmps[2:], s.rowKeyDelim)
				queryKeys = append(queryKeys, pk)
			}
		}

		return nil
	})

	for _, k := range queryKeys {
		var cellb []byte
		cellb, found, err = s.get("r" + s.rowKeyDelim + k)
		if err != nil {
			return nil, false, err
		}

		if found {
			var cell models.Cell
			err = json.Unmarshal(cellb, &cell)
			if err != nil {
				return nil, false, err
			}

			cells = append(cells, cell)
		}
	}

	// ensure found is true if cells is nonzero
	if len(cells) > 0 {
		found = true
	}

	return cells, found, err
}

func (s *Storage) setCell(key string, val []byte, rowKey string, columnKey string, refKey int64, cell models.Cell) error {
	s.mu.Lock()
	if !s.opened {
		return errors.New("database is closed")
	}
	s.mu.Unlock()

	return s.db.Update(func(txn *badger.Txn) error {
		storeKey := []byte("r" + s.rowKeyDelim + key)

		// NOTE(rbastic): immutable mode is not yet externally
		// configurable, and is on by default.
		if s.immutable {
			_, err := txn.Get(storeKey)
			if err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}

		err := txn.Set(storeKey, val)
		if err != nil {
			return err
		}

		// Write added_at key 'index'
		idx1 := strings.Join([]string{"a", fmt.Sprintf("%d", cell.AddedAt), rowKey, columnKey, fmt.Sprintf("%d", refKey)}, s.rowKeyDelim)
		err = txn.Set([]byte(idx1), nil)
		if err != nil {
			return err
		}

		// Write created_at key 'index'
		ca := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
		idx2 := strings.Join([]string{"t", ca, rowKey, columnKey, fmt.Sprintf("%d", refKey)}, s.rowKeyDelim)
		err = txn.Set([]byte(idx2), nil)
		if err != nil {
			return err
		}

		return nil
	})
}

func (s *Storage) PutCell(ctx context.Context, rowKey, columnKey string, refKey int64, cell models.Cell) (err error) {
	refKeyTs := strconv.FormatInt(refKey, 10)

	queryKey := strings.Join([]string{rowKey, columnKey, refKeyTs}, s.rowKeyDelim)
	cell.RefKey = refKey

	if cell.CreatedAt == 0 {
		cell.CreatedAt = uint64(time.Now().UTC().UnixNano())
	}

	var cellData []byte
	cellData, err = json.Marshal(cell)
	if err != nil {
		return
	}

	return s.setCell(queryKey, cellData, rowKey, columnKey, refKey, cell)
}

func (s *Storage) ResetConnection(ctx context.Context, key string) error {
	return nil
}

func (s *Storage) Destroy(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db != nil {
		db := s.db
		s.db = nil
		return db.Close()
	}
	return nil
}
