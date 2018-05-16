// Package memory is a memory-backed Schemaless store, using a SQLite in-memory database.  This is useful mostly for testing.
package memory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rbastic/go-schemaless/models"
	"go.uber.org/zap"
	"sync"
	"time"
)

// Storage is a simple memory-backed storage (RowKeyMap) with a global mutex.
type Storage struct {
	store *sql.DB
	mu    sync.Mutex
	sugar *zap.SugaredLogger
}

const (
	driver    = "sqlite3"
	memoryDSN = "file::memory:"
)

func exec(db *sql.DB, sqlStr string) error {
	_, err := db.Exec(sqlStr)
	if err != nil {
		return err
	}
	return nil
}

func createTable(ctx context.Context, db *sql.DB) error {
	return exec(db, " CREATE TABLE cell ( added_at      INTEGER PRIMARY KEY AUTOINCREMENT, row_key		  VARCHAR(36) NOT NULL, column_name	  VARCHAR(64) NOT NULL, ref_key		  INTEGER NOT NULL, body		  JSON, created_at    DATETIME DEFAULT CURRENT_TIMESTAMP) ")
}

func createIndex(ctx context.Context, db *sql.DB) error {
	return exec(db, "CREATE UNIQUE INDEX IF NOT EXISTS uniqcell_idx ON cell ( row_key, column_name, ref_key )")
}

// New returns a new memory-backed Storage
func New() *Storage {
	db, err := sql.Open(driver, memoryDSN)
	if err != nil {
		panic(err)
	}

	err = createTable(context.TODO(), db)
	if err != nil {
		panic(err)
	}

	err = createIndex(context.TODO(), db)
	if err != nil {
		panic(err)
	}

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	s := logger.Sugar()

	return &Storage{
		// initialize top-level
		store: db,
		sugar: s,
	}
}

func (s *Storage) GetCell(rowKey string, columnKey string, refKey int64) (cell models.Cell, found bool, err error) {
	var (
		resAddedAt   int64
		resRowKey    string
		resColName   string
		resRefKey    int64
		resBody      string
		resCreatedAt *time.Time
		rows *sql.Rows
	)
	rows, err = s.store.Query("SELECT added_at, row_key, column_name, ref_key, body,created_at FROM cell WHERE row_key = ? AND column_name = ? AND ref_key = ? ", rowKey, columnKey, refKey)
	if err != nil {
		return
	}
	defer rows.Close()

	found = false
	for rows.Next() {
		err = rows.Scan(&resAddedAt, &resRowKey, &resColName, &resRefKey, &resBody, &resCreatedAt)
		if err != nil {
			return
		}
		s.sugar.Infow("scanned data", "AddedAt", resAddedAt, "RowKey", resRowKey, "ColName", resColName, "RefKey", resRefKey, "Body", resBody, "CreatedAt", resCreatedAt)

		cell.AddedAt = resAddedAt
		cell.RowKey = resRowKey
		cell.ColumnName = resColName
		cell.RefKey = resRefKey
		cell.Body = []byte(resBody)
		cell.CreatedAt = resCreatedAt
		found = true
	}

	err = rows.Err()
	if err != nil {
		return
	}

	return cell, found, nil
}

func (s *Storage) GetCellLatest(rowKey, columnKey string) (cell models.Cell, found bool, err error) {
	var (
		resAddedAt   int64
		resRowKey    string
		resColName   string
		resRefKey    int64
		resBody      string
		resCreatedAt *time.Time
		rows *sql.Rows
	)
	rows, err = s.store.Query("SELECT added_at, row_key, column_name, ref_key, body, created_at FROM cell WHERE row_key = ? AND column_name = ? ORDER BY ref_key DESC LIMIT 1", rowKey, columnKey)
	if err != nil {
		return
	}
	defer rows.Close()

	found = false
	for rows.Next() {
		err = rows.Scan(&resAddedAt, &resRowKey, &resColName, &resRefKey, &resBody, &resCreatedAt)
		if err != nil {
			return
		}
		s.sugar.Infow("scanned data", "AddedAt", resAddedAt, "RowKey", resRowKey, "ColName", resColName, "RefKey", resRefKey, "Body", resBody, "CreatedAt", resCreatedAt)

		cell.AddedAt = resAddedAt
		cell.RowKey = resRowKey
		cell.ColumnName = resColName
		cell.RefKey = resRefKey
		cell.Body = []byte(resBody)
		cell.CreatedAt = resCreatedAt
		found = true
	}

	err = rows.Err()
	if err != nil {
		return
	}

	return cell, found, nil
}

func (s *Storage) GetCellsForShard(shardNumber int, location string, value interface{}, limit int) (cells []models.Cell, found bool, err error) {

	var (
		resAddedAt   int64
		resRowKey    string
		resColName   string
		resRefKey    int64
		resBody      string
		resCreatedAt *time.Time
	)

	// TODO: shardNumber

	var locationColumn string

	switch location {
	case "timestamp":
		locationColumn = "created_at"
	case "added_at":
		locationColumn = "added_at"
	default:
		err = errors.New("Unrecognized location " + location)
		return
	}

	sqlStr := fmt.Sprintf("SELECT added_at, row_key, column_name, ref_key, body, created_at FROM cell WHERE %s > ?", locationColumn)

	var rows *sql.Rows
	rows, err = s.store.Query(sqlStr, value)
	if err != nil {
		return
	}
	defer rows.Close()

	found = false
	for rows.Next() {
		err = rows.Scan(&resAddedAt, &resRowKey, &resColName, &resRefKey, &resBody, &resCreatedAt)
		if err != nil {
			return
		}
		s.sugar.Infow("scanned data", "AddedAt", resAddedAt, "RowKey", resRowKey, "ColName", resColName, "RefKey", resRefKey, "Body", resBody, "CreatedAt", resCreatedAt)

		var cell models.Cell
		cell.AddedAt = resAddedAt
		cell.RowKey = resRowKey
		cell.ColumnName = resColName
		cell.RefKey = resRefKey
		cell.Body = []byte(resBody)
		cell.CreatedAt = resCreatedAt
		cells = append(cells, cell)
		found = true
	}

	err = rows.Err()
	if err != nil {
		return
	}

	return cells, found, nil
}

func (s *Storage) PutCell(rowKey, columnKey string, refKey int64, cell models.Cell) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var stmt *sql.Stmt
	stmt, err = s.store.Prepare("INSERT INTO cell ( row_key, column_name, ref_key, body ) VALUES(?, ?, ?, ?)")
	if err != nil {
		return
	}
	var res sql.Result
	res, err = stmt.Exec(rowKey, columnKey, refKey, cell.Body)
	if err != nil {
		return
	}
	var lastId int64
	lastId, err = res.LastInsertId()
	if err != nil {
		return
	}
	var rowCnt int64
	rowCnt, err = res.RowsAffected()
	if err != nil {
		return
	}
	s.sugar.Infof("ID = %d, affected = %d\n", lastId, rowCnt)
	return
}

func (s *Storage) ResetConnection(key string) error {
	return nil
}

func (s *Storage) Destroy(ctx context.Context) error {
	return s.store.Close()
}
