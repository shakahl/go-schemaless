package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rbastic/go-schemaless/models"
	"go.uber.org/zap"
)

// ErrNotAtStorageLevel marks methods that are not to be called at storage level
var ErrNotAtStorageLevel = errors.New("not implemented at storage level")

type Storage struct {
	store *sql.DB
	sugar *zap.SugaredLogger
}

const (
	driver = "sqlite3"

	createTableSQL      = "CREATE TABLE IF NOT EXISTS %s ( added_at INTEGER PRIMARY KEY AUTOINCREMENT, row_key VARCHAR(36) NOT NULL, column_name VARCHAR(64) NOT NULL, ref_key INTEGER NOT NULL, body TEXT, created_at INTEGER DEFAULT 0)"
	createIndexSQL      = "CREATE UNIQUE INDEX IF NOT EXISTS uniq%s_idx ON %s ( row_key, column_name, ref_key )"
	getCellSQL          = "SELECT added_at, row_key, column_name, ref_key, body, created_at FROM %s WHERE row_key = ? AND column_name = ? AND ref_key = ? LIMIT 1"
	getCellLatestSQL    = "SELECT added_at, row_key, column_name, ref_key, body, created_at FROM %s WHERE row_key = ? AND column_name = ? ORDER BY ref_key DESC LIMIT 1"
	getCellsForShardSQL = "SELECT added_at, row_key, column_name, ref_key, body, created_at FROM %s WHERE %s >= ? LIMIT %d"
	putCellSQL          = "INSERT INTO %s ( row_key, column_name, ref_key, body, created_at ) VALUES(?, ?, ?, ?, ?)"
)

func exec(db *sql.DB, sqlStr string) error {
	_, err := db.Exec(sqlStr)
	if err != nil {
		return err
	}
	return nil
}

func CreateTable(ctx context.Context, db *sql.DB, tblName string) error {
	return exec(db, fmt.Sprintf(createTableSQL, tblName))
}

func CreateIndex(ctx context.Context, db *sql.DB, tblName string) error {
	return exec(db, fmt.Sprintf(createIndexSQL, tblName, tblName))
}

// New returns a new sqlite file-backed Storage
func New(tblName, path string) (*Storage, error) {
	db, err := sql.Open(driver, path+"_"+tblName+".db")
	if err != nil {
		return nil, err
	}

	err = CreateTable(context.TODO(), db, tblName)
	if err != nil {
		return nil, err
	}

	err = CreateIndex(context.TODO(), db, tblName)
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

func (s *Storage) GetDB() *sql.DB {
	return s.store
}

func (s *Storage) Get(ctx context.Context, tblName, rowKey, columnKey string, refKey int64) (cell models.Cell, found bool, err error) {
	var (
		resAddedAt   int64
		resRowKey    string
		resColName   string
		resRefKey    int64
		resBody      string
		resCreatedAt int64
		rows         *sql.Rows
	)
	sqlQuery := fmt.Sprintf(getCellSQL, tblName)

	//s.sugar.Infow("Get", "query", sqlQuery, "rowKey", rowKey, "columnKey", columnKey, "refKey", refKey)

	rows, err = s.store.Query(sqlQuery, rowKey, columnKey, refKey)
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
		//s.sugar.Infow("Get scanned data", "AddedAt", resAddedAt, "RowKey", resRowKey, "ColName", resColName, "RefKey", resRefKey, "Body", resBody, "CreatedAt", resCreatedAt)

		cell.AddedAt = resAddedAt
		cell.RowKey = resRowKey
		cell.ColumnName = resColName
		cell.RefKey = resRefKey
		cell.Body = resBody
		cell.CreatedAt = resCreatedAt
		found = true
	}

	err = rows.Err()
	if err != nil {
		return
	}

	return cell, found, nil
}

func (s *Storage) GetLatest(ctx context.Context, tblName, rowKey, columnKey string) (cell models.Cell, found bool, err error) {
	var (
		resAddedAt   int64
		resRowKey    string
		resColName   string
		resRefKey    int64
		resBody      string
		resCreatedAt int64
		rows         *sql.Rows
	)
	//s.sugar.Infow("GetLatest", "query", getCellSQL, "rowKey", rowKey, "columnKey", columnKey)

	sqlQuery := fmt.Sprintf(getCellLatestSQL, tblName)
	rows, err = s.store.Query(sqlQuery, rowKey, columnKey)
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
		//s.sugar.Infow("GetLatest scanned data", "AddedAt", resAddedAt, "RowKey", resRowKey, "ColName", resColName, "RefKey", resRefKey, "Body", resBody, "CreatedAt", resCreatedAt)

		cell.AddedAt = resAddedAt
		cell.RowKey = resRowKey
		cell.ColumnName = resColName
		cell.RefKey = resRefKey
		cell.Body = resBody
		cell.CreatedAt = resCreatedAt
		found = true
	}

	err = rows.Err()
	if err != nil {
		return
	}

	return cell, found, nil
}

func (s *Storage) FindPartition(tblName, rowKey string) int {
	panic("FindPartition not implemented at storage level")
}

func (s *Storage) PartitionRead(ctx context.Context, tblName string, partitionNumber int, location string, value int64, limit int) (cells []models.Cell, found bool, err error) {

	var (
		resAddedAt   int64
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
	case "ref_key":
		locationColumn = "ref_key"
	default:
		err = errors.New("unrecognized location " + location)
		return
	}

	sqlStr := fmt.Sprintf(getCellsForShardSQL, tblName, locationColumn, limit)

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
		//s.sugar.Infow("PartitionRead: scanned data", "AddedAt", resAddedAt, "RowKey", resRowKey, "ColName", resColName, "RefKey", resRefKey, "Body", resBody, "CreatedAt", resCreatedAt)

		var cell models.Cell
		cell.AddedAt = resAddedAt
		cell.RowKey = resRowKey
		cell.ColumnName = resColName
		cell.RefKey = resRefKey
		cell.Body = resBody
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

func (s *Storage) Put(ctx context.Context, tblName, rowKey, columnKey string, refKey int64, body string) (err error) {
	createdAt := time.Now().UTC().UnixNano()
	//s.sugar.Infow("STORING", "createdAt", createdAt)
	var stmt *sql.Stmt
	stmt, err = s.store.Prepare(fmt.Sprintf(putCellSQL, tblName))
	if err != nil {
		return err
	}
	var res sql.Result

	//s.sugar.Infow("Put: wrote data", "RowKey", rowKey, "ColumnName", columnKey, "refKey", refKey, "Body", body, "createdAt", createdAt)

	res, err = stmt.Exec(rowKey, columnKey, refKey, body, createdAt)
	if err != nil {
		return err
	}
	var rowCnt int64
	rowCnt, err = res.RowsAffected()
	if err != nil {
		return err
	}
	if rowCnt == 0 {
		return errors.New("row-count was zero for put")
	}
	//s.sugar.Infof("ID = %s, affected = %d\n", rowKey, rowCnt)
	return nil
}

// ResetConnection closes the store.
func (s *Storage) ResetConnection(ctx context.Context, key string) error {
	return s.store.Close()
}

// Destroy closes the store
func (s *Storage) Destroy(ctx context.Context) error {
	s.sugar.Sync()
	return s.store.Close()
}
