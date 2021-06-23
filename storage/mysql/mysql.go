// Package mysql is a mysql-backed Schemaless store.
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/rbastic/go-schemaless/models"
	"go.uber.org/zap"
	"time"
)

// Storage is a MySQL-backed storage.
type Storage struct {
	user     string
	pass     string
	host     string
	port     string
	database string

	store *sql.DB
	sugar *zap.SugaredLogger
}

const (
	driver          = "mysql"
	timeParseString = "2006-01-02 15:04:05"
	// dsnFormat string parameters: username, password, host, port, database.
	// parseTime is for parsing and handling *time.Time properly
	dsnFormat = "%s:%s@tcp(%s:%s)/%s?parseTime=true"

	getCellSQL          = "SELECT added_at, row_key, column_name, ref_key, body,created_at FROM %s WHERE row_key = ? AND column_name = ? AND ref_key = ? LIMIT 1"
	getCellLatestSQL    = "SELECT added_at, row_key, column_name, ref_key, body, created_at FROM %s WHERE row_key = ? AND column_name = ? ORDER BY ref_key DESC LIMIT 1"
	getCellsForShardSQL = "SELECT added_at, row_key, column_name, ref_key, body, created_at FROM %s WHERE %s >= ? LIMIT %d"
	putCellSQL          = "INSERT INTO %s ( row_key, column_name, ref_key, body ) VALUES(?, ?, ?, ?)"
)

func exec(db *sql.DB, sqlStr string) error {
	_, err := db.Exec(sqlStr)
	if err != nil {
		return err
	}
	return nil
}

// New returns a new mysql-backed Storage
func New() *Storage {
	return &Storage{}
}

func (s *Storage) WithZap() error {
	logger, err := zap.NewProduction()
	if err != nil {
		return err
	}
	s.sugar = logger.Sugar()
	return nil
}

func (s *Storage) Open() error {
	if s.sugar != nil {
		s.sugar.Infof("Open dsnFormat:%s dsn:%s", dsnFormat, fmt.Sprintf(dsnFormat, s.user, s.pass, s.host, s.port, s.database) )
	}
	db, err := sql.Open(driver, fmt.Sprintf(dsnFormat, s.user, s.pass, s.host, s.port, s.database))
	if err != nil {
		return err
	}
	s.store = db
	return nil
}

func (s *Storage) WithUser(user string) *Storage {
	s.user = user
	return s
}

func (s *Storage) WithPass(pass string) *Storage {
	s.pass = pass
	return s
}

func (s *Storage) WithHost(host string) *Storage {
	s.host = host
	return s
}

func (s *Storage) WithPort(port string) *Storage {
	s.port = port
	return s
}

func (s *Storage) WithDatabase(database string) *Storage {
	s.database = database
	return s
}

func (s *Storage) Get(ctx context.Context, tblName, rowKey, columnKey string, refKey int64) (cell models.Cell, found bool, err error) {
	var (
		resAddedAt   int64
		resRowKey    string
		resColName   string
		resRefKey    int64
		resBody      string
		resCreatedAt time.Time
		rows         *sql.Rows
	)
	s.sugar.Infow("Get", "query", getCellSQL, "rowKey", rowKey, "columnKey", columnKey, "refKey", refKey)

	sqlQuery := fmt.Sprintf(getCellSQL, tblName)

	rows, err = s.store.QueryContext(ctx, sqlQuery, rowKey, columnKey, refKey)
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
		cell.CreatedAt = resCreatedAt.UnixNano()
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
		resCreatedAt time.Time
		rows         *sql.Rows
	)
	s.sugar.Infow("GetLatest", "query before", getCellLatestSQL, "rowKey", rowKey, "columnKey", columnKey)

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
		s.sugar.Infow("GetLatest scanned data", "AddedAt", resAddedAt, "RowKey", resRowKey, "ColName", resColName, "RefKey", resRefKey, "Body", resBody, "CreatedAt", resCreatedAt)

		cell.AddedAt = resAddedAt
		cell.RowKey = resRowKey
		cell.ColumnName = resColName
		cell.RefKey = resRefKey
		cell.Body = resBody
		cell.CreatedAt = resCreatedAt.UnixNano()
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
		resCreatedAt time.Time

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
		err = errors.New("unrecognized location " + location)
		return
	}

	sqlStr := fmt.Sprintf(getCellsForShardSQL, tblName, locationColumn, limit)

	var rows *sql.Rows
	s.sugar.Infow("PartitionRead", "query", sqlStr, "value", value)
	rows, err = s.store.QueryContext(ctx, sqlStr, value)
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
		cell.CreatedAt = resCreatedAt.UnixNano()
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

	var stmt *sql.Stmt
	stmt, err = s.store.PrepareContext(ctx, fmt.Sprintf(putCellSQL, tblName))
	if err != nil {
		return
	}
	var res sql.Result
	s.sugar.Infow("Put", "rowKey", rowKey, "columnKey", columnKey, "refKey", refKey, "Body", body)
	res, err = stmt.Exec(rowKey, columnKey, refKey, body)
	if err != nil {
		return
	}
	var rowCnt int64
	rowCnt, err = res.RowsAffected()
	if err != nil {
		return
	}
	s.sugar.Infof("affected = %d\n", rowCnt)
	return
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
