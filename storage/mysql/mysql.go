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
	"reflect"
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
	Sugar *zap.SugaredLogger
}

const (
	//timeParseString = "2006-01-02T15:04:05Z"
	timeParseString = "2006-01-02 15:04:05"
	driver          = "mysql"
	// dsnFormat string parameters: username, password, host, port, database.
	// parseTime is for parsing and handling *time.Time properly
	dsnFormat = "%s:%s@tcp(%s:%s)/%s?parseTime=true"
	// This space intentionally left blank for facilitating vimdiff
	// acrosss storages.

	getCellSQL          = "SELECT added_at, row_key, column_name, ref_key, body,created_at FROM cell WHERE row_key = ? AND column_name = ? AND ref_key = ? LIMIT 1"
	getCellLatestSQL    = "SELECT added_at, row_key, column_name, ref_key, body, created_at FROM cell WHERE row_key = ? AND column_name = ? ORDER BY ref_key DESC LIMIT 1"
	getCellsForShardSQL = "SELECT added_at, row_key, column_name, ref_key, body, created_at FROM cell WHERE %s > %s LIMIT %d"
	putCellSQL          = "INSERT INTO cell ( row_key, column_name, ref_key, body ) VALUES(?, ?, ?, ?)"
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
	s.Sugar = logger.Sugar()
	return nil
}

func (s *Storage) Open() error {
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

func (s *Storage) GetCell(ctx context.Context, rowKey string, columnKey string, refKey int64) (cell models.Cell, found bool, err error) {
	var (
		resAddedAt   int64
		resRowKey    string
		resColName   string
		resRefKey    int64
		resBody      string
		resCreatedAt *time.Time
		rows         *sql.Rows
	)
	s.Sugar.Infow("GetCell", "query", getCellSQL, "rowKey", rowKey, "columnKey", columnKey, "refKey", refKey)
	rows, err = s.store.QueryContext(ctx, getCellSQL, rowKey, columnKey, refKey)
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
		s.Sugar.Infow("GetCell scanned data", "AddedAt", resAddedAt, "RowKey", resRowKey, "ColName", resColName, "RefKey", resRefKey, "Body", resBody, "CreatedAt", resCreatedAt)

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

func (s *Storage) GetCellLatest(ctx context.Context, rowKey, columnKey string) (cell models.Cell, found bool, err error) {
	var (
		resAddedAt   int64
		resRowKey    string
		resColName   string
		resRefKey    int64
		resBody      string
		resCreatedAt *time.Time
		rows         *sql.Rows
	)
	s.Sugar.Infow("GetCellLatest", "query before", getCellLatestSQL, "rowKey", rowKey, "columnKey", columnKey)
	rows, err = s.store.QueryContext(ctx, getCellLatestSQL, rowKey, columnKey)
	s.Sugar.Infow("GetCellLatest", "query after", getCellLatestSQL, "rowKey", rowKey, "columnKey", columnKey, "rows", rows, "error", err)
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
		s.Sugar.Infow("GetCellLatest scanned data", "AddedAt", resAddedAt, "RowKey", resRowKey, "ColName", resColName, "RefKey", resRefKey, "Body", resBody, "CreatedAt", resCreatedAt)

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

func (s *Storage) PartitionRead(ctx context.Context, partitionNumber int, location string, value interface{}, limit int) (cells []models.Cell, found bool, err error) {

	var (
		resAddedAt   int64
		resRowKey    string
		resColName   string
		resRefKey    int64
		resBody      string
		resCreatedAt *time.Time

		locationColumn string
		valueStr       string
	)

	switch location {
	case "timestamp":
		fallthrough
	case "created_at":
		locationColumn = "created_at"
		switch value.(type) {
		case *time.Time:
			t := value.(*time.Time)
			valueStr = t.Format(timeParseString)
			if valueStr == "" {
				err = fmt.Errorf("PartitionRead had empty value after formatting *time.Time:'%v'", t)
				return
			}
		case time.Time:
			t := value.(time.Time)
			valueStr = t.Format(timeParseString)
			if valueStr == "" {
				err = fmt.Errorf("PartitionRead had empty value after formatting time.Time:'%v'", t)
				return
			}
		case string:
			s.Sugar.Infow("let me guess, it's a string")
			t := value.(string)
			valueStr = t
			if valueStr == "" {
				err = fmt.Errorf("PartitionRead had empty value after formatting string:'%v'", t)
				return
			}
		default:
			err = fmt.Errorf("PartitionRead had unrecognized type %v", reflect.TypeOf(value))
			return
		}
		valueStr = "'" + valueStr + "'"
	case "added_at":
		locationColumn = "added_at"
		switch value.(type) {
		case int:
			t := value.(int)
			valueStr = fmt.Sprintf("%d", t)
		case int64:
			t := value.(int64)
			valueStr = fmt.Sprintf("%d", t)
		case string:
			t := value.(string)
			valueStr = fmt.Sprintf("%s", t)
			return
		default:
			err = fmt.Errorf("PartitionRead had unrecognized type %v", reflect.TypeOf(value))
			return
		}
	default:
		err = errors.New("PartitionRead had unrecognized location " + location)
		return
	}

	sqlStr := fmt.Sprintf(getCellsForShardSQL, locationColumn, valueStr, limit)

	var rows *sql.Rows
	s.Sugar.Infow("PartitionRead", "query", sqlStr, "valueStr", valueStr)
	rows, err = s.store.QueryContext(ctx, sqlStr)
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
		s.Sugar.Infow("PartitionRead: scanned data", "AddedAt", resAddedAt, "RowKey", resRowKey, "ColName", resColName, "RefKey", resRefKey, "Body", resBody, "CreatedAt", resCreatedAt)

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

func (s *Storage) PutCell(ctx context.Context, rowKey, columnKey string, refKey int64, cell models.Cell) (err error) {
	var stmt *sql.Stmt
	stmt, err = s.store.PrepareContext(ctx, putCellSQL)
	if err != nil {
		return
	}
	var res sql.Result
	s.Sugar.Infow("PutCell", "rowKey", rowKey, "columnKey", columnKey, "refKey", refKey, "Body", cell.Body)
	res, err = stmt.Exec(rowKey, columnKey, refKey, cell.Body)
	if err != nil {
		return
	}
	var lastID int64
	lastID, err = res.LastInsertId()
	if err != nil {
		return
	}
	var rowCnt int64
	rowCnt, err = res.RowsAffected()
	if err != nil {
		return
	}
	// TODO(rbastic): Should we side-affect the cell and record the AddedAt?
	s.Sugar.Infof("ID = %d, affected = %d\n", lastID, rowCnt)
	return
}

// ResetConnection does not destroy the store for in-memory stores.
func (s *Storage) ResetConnection(ctx context.Context, key string) error {
	return nil
}

// Destroy closes the in-memory store, and is a completely destructive operation.
func (s *Storage) Destroy(ctx context.Context) error {
	// TODO(rbastic): What do if there's an error in Sync()?
	s.Sugar.Sync()
	return s.store.Close()
}
