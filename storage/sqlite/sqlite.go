package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rbastic/go-schemaless/models"
	"go.uber.org/zap"
	"time"
)

type Storage struct {
	store *sql.DB
	sugar *zap.SugaredLogger
}

const (
	driver = "sqlite3"

	createTableSQL      = "CREATE TABLE cell ( added_at INTEGER PRIMARY KEY AUTOINCREMENT, row_key VARCHAR(36) NOT NULL, column_name VARCHAR(64) NOT NULL, ref_key INTEGER NOT NULL, body TEXT, created_at UNSIGNED INTEGER DEFAULT 0)"
	createIndexSQL      = "CREATE UNIQUE INDEX IF NOT EXISTS uniqcell_idx ON cell ( row_key, column_name, ref_key )"
	getCellSQL          = "SELECT added_at, row_key, column_name, ref_key, body, created_at FROM cell WHERE row_key = ? AND column_name = ? AND ref_key = ? LIMIT 1"
	getCellLatestSQL    = "SELECT added_at, row_key, column_name, ref_key, body, created_at FROM cell WHERE row_key = ? AND column_name = ? ORDER BY ref_key DESC LIMIT 1"
	getCellsForShardSQL = "SELECT added_at, row_key, column_name, ref_key, body, created_at FROM cell WHERE %s > ? LIMIT %d"
	putCellSQL          = "INSERT INTO cell ( row_key, column_name, ref_key, body, created_at ) VALUES(?, ?, ?, ?, ?)"
)

func exec(db *sql.DB, sqlStr string) error {
	_, err := db.Exec(sqlStr)
	if err != nil {
		return err
	}
	return nil
}

func createTable(ctx context.Context, db *sql.DB) error {
	return exec(db, createTableSQL)
}

func createIndex(ctx context.Context, db *sql.DB) error {
	return exec(db, createIndexSQL)
}

// New returns a new sqlite file-backed Storage
func New(path string) (*Storage, error) {
	db, err := sql.Open(driver, path+"_cell.db")
	if err != nil {
		return nil, err
	}

	err = createTable(context.TODO(), db)
	if err != nil {
		return nil, err
	}

	err = createIndex(context.TODO(), db)
	if err != nil {
		return nil, err
	}

	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}
	s := logger.Sugar()

	return &Storage{
		// initialize top-level
		store: db,
		sugar: s,
	}, nil
}

func (s *Storage) Get(ctx context.Context, rowKey string, columnKey string, refKey int64) (cell models.Cell, found bool, err error) {
	var (
		resAddedAt   uint64
		resRowKey    string
		resColName   string
		resRefKey    int64
		resBody      string
		resCreatedAt int64
		rows         *sql.Rows
	)
	s.sugar.Infow("Get", "query", getCellSQL, "rowKey", rowKey, "columnKey", columnKey, "refKey", refKey)
	rows, err = s.store.Query(getCellSQL, rowKey, columnKey, refKey)
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
		s.sugar.Infow("Get scanned data", "AddedAt", resAddedAt, "RowKey", resRowKey, "ColName", resColName, "RefKey", resRefKey, "Body", resBody, "CreatedAt", resCreatedAt)

		cell.AddedAt = resAddedAt
		cell.RowKey = resRowKey
		cell.ColumnName = resColName
		cell.RefKey = resRefKey
		cell.Body = resBody
		cell.CreatedAt = uint64(resCreatedAt)
		found = true
	}

	err = rows.Err()
	if err != nil {
		return
	}

	return cell, found, nil
}

func (s *Storage) GetLatest(ctx context.Context, rowKey, columnKey string) (cell models.Cell, found bool, err error) {
	var (
		resAddedAt   uint64
		resRowKey    string
		resColName   string
		resRefKey    int64
		resBody      string
		resCreatedAt int64
		rows         *sql.Rows
	)
	s.sugar.Infow("GetLatest", "query", getCellSQL, "rowKey", rowKey, "columnKey", columnKey)
	rows, err = s.store.Query(getCellLatestSQL, rowKey, columnKey)
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
		s.sugar.Infow("GetLatest scanned data", "AddedAt", resAddedAt, "RowKey", resRowKey, "ColName", resColName, "RefKey", resRefKey, "Body", resBody, "CreatedAt", resCreatedAt)

		cell.AddedAt = resAddedAt
		cell.RowKey = resRowKey
		cell.ColumnName = resColName
		cell.RefKey = resRefKey
		cell.Body = resBody
		cell.CreatedAt = uint64(resCreatedAt)
		found = true
	}

	err = rows.Err()
	if err != nil {
		return
	}

	return cell, found, nil
}

func (s *Storage) PartitionRead(ctx context.Context, partitionNumber int, location string, value uint64, limit int) (cells []models.Cell, found bool, err error) {

	var (
		resAddedAt   uint64
		resRowKey    string
		resColName   string
		resRefKey    int64
		resBody      string
		resCreatedAt int64

		locationColumn string
	)

	switch location {
	case "timestamp":
		fallthrough
	case "created_at":
		locationColumn = "created_at"
	case "added_at":
		locationColumn = "added_at"
	default:
		err = errors.New("Unrecognized location " + location)
		return
	}

	sqlStr := fmt.Sprintf(getCellsForShardSQL, locationColumn, limit)

	var rows *sql.Rows
	s.sugar.Infow("PartitionRead", "query", sqlStr, "value", value)
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
		s.sugar.Infow("PartitionRead: scanned data", "AddedAt", resAddedAt, "RowKey", resRowKey, "ColName", resColName, "RefKey", resRefKey, "Body", resBody, "CreatedAt", resCreatedAt)

		var cell models.Cell
		cell.AddedAt = resAddedAt
		cell.RowKey = resRowKey
		cell.ColumnName = resColName
		cell.RefKey = resRefKey
		cell.Body = resBody
		cell.CreatedAt = uint64(resCreatedAt)
		cells = append(cells, cell)
		found = true
	}

	err = rows.Err()
	if err != nil {
		return
	}

	return cells, found, nil
}

func (s *Storage) Put(ctx context.Context, rowKey, columnKey string, refKey int64, cell models.Cell) (err error) {
	if cell.CreatedAt == 0 {
		cell.CreatedAt = uint64(time.Now().UTC().UnixNano())
	}
	var stmt *sql.Stmt
	stmt, err = s.store.Prepare(putCellSQL)
	if err != nil {
		return
	}
	var res sql.Result
	s.sugar.Infow("Put", "rowKey", rowKey, "columnKey", columnKey, "refKey", refKey, "Body", cell.Body)
	// TODO(rbastic): fix CreatedAt here
	res, err = stmt.Exec(rowKey, columnKey, refKey, cell.Body, cell.CreatedAt)
	if err != nil {
		return
	}
	var rowCnt int64
	rowCnt, err = res.RowsAffected()
	if err != nil {
		return
	}
	s.sugar.Infof("ID = %s, affected = %d\n", rowKey, rowCnt)
	return
}

func (s *Storage) ResetConnection(ctx context.Context, key string) error {
	return s.store.Close()
}

func (s *Storage) Destroy(ctx context.Context) error {
	s.sugar.Sync()
	return s.store.Close()
}
