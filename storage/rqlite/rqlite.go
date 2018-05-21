// Package rqlite is a rqlite-backed Schemaless store.
package rqlite

import (
	"context"
	"errors"
	"fmt"
	"github.com/rqlite/gorqlite"
	"github.com/rbastic/go-schemaless/models"
	"go.uber.org/zap"
	"reflect"
	"strings"
	"time"
)

type rqliteDB struct {
	conn *gorqlite.Connection
	sugar *zap.SugaredLogger
}

func newRqlite() *rqliteDB {
	return &rqliteDB{}
}

func (r*rqliteDB) WithOpen(url string) *rqliteDB {
	store, err := gorqlite.Open(url)
	if err !=nil {
		panic(err)
	}
	r.conn = &store
	return r
}

func (r*rqliteDB) WithSugar(z *zap.SugaredLogger) *rqliteDB {
	r.sugar = z
	return r
}

// Storage is a rqlite-backed storage.
type Storage struct {
	store *rqliteDB
	sugar *zap.SugaredLogger
}

const (
	// dsnFormat string parameters: username, password, host, port, database.
	DSNFormat = "%s://%s:%d/?level=%s&timeout=%d"
	// This space intentionally left blank for facilitating vimdiff
	// acrosss storages.
	getCellSQL          = "SELECT added_at, row_key, column_name, ref_key, body,created_at FROM cell WHERE row_key = '%s' AND column_name = '%s' AND ref_key = %d LIMIT 1"
	getCellLatestSQL    = "SELECT added_at, row_key, column_name, ref_key, body, created_at FROM cell WHERE row_key = '%s' AND column_name = '%s' ORDER BY ref_key DESC LIMIT 1"
	getCellsForShardSQL = "SELECT added_at, row_key, column_name, ref_key, body, created_at FROM cell WHERE %s > '%s' LIMIT %d"
	putCellSQL          = "INSERT INTO cell ( row_key, column_name, ref_key, body ) VALUES('%s', '%s', %d, '%s')"
)

// New returns a new rqlite--backed Storage. scheme is http/https. level is
// "none", "weak", or "strong".  timeout is in seconds.
func New() *Storage {
	return &Storage{}
}

func (s *Storage) WithZap() *Storage {
	// TODO(rbastic): Hmmm.. Should I ping the db?
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	sug := logger.Sugar()
	s.sugar = sug
	return s
}

func (s *Storage) WithURL( url string) *Storage {
	s.store = newRqlite().WithOpen(url)
	return s
}

func quoteString(s string) string {
	quoted := strings.Replace(s, "'", "\\'", -1)
	return quoted
}

func (s *Storage) GetCell(ctx context.Context, rowKey string, columnKey string, refKey int64) (cell models.Cell, found bool, err error) {
	var (
		resAddedAt   int64
		resRowKey    string
		resColName   string
		resRefKey    int64
		resBody      string
		resCreatedAt string
	)

	s.sugar.Infow("GetCell", "querySQL before", getCellSQL, "rowKey", rowKey, "columnKey", columnKey, "refKey", refKey)
	querySQL := fmt.Sprintf(getCellSQL, quoteString(rowKey), quoteString(columnKey), refKey)
	s.sugar.Infow("GetCell", "querySQL after", querySQL)

	rows, err := s.store.conn.QueryOne(querySQL)
	if err != nil {
		return
	}

	found = false
	for rows.Next() {
		err = rows.Scan(&resAddedAt, &resRowKey, &resColName, &resRefKey, &resBody, &resCreatedAt)
		if err != nil {
			return
		}
		s.sugar.Infow("GetCell scanned data", "AddedAt", resAddedAt, "RowKey", resRowKey, "ColName", resColName, "RefKey", resRefKey, "Body", resBody, "CreatedAt", resCreatedAt)

		cell.AddedAt = resAddedAt
		cell.RowKey = resRowKey
		cell.ColumnName = resColName
		cell.RefKey = resRefKey
		cell.Body = []byte(resBody)
		var t time.Time
		t, err = time.Parse(time.RFC3339, resCreatedAt)
		if err != nil {
			return
		}
		cell.CreatedAt = &t
		found = true
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
		resCreatedAt string
		rows gorqlite.QueryResult
	)

	s.sugar.Infow("GetCellLatest", "querySQL before", getCellSQL, "rowKey", rowKey, "columnKey", columnKey)
	querySQL := fmt.Sprintf(getCellLatestSQL, quoteString(rowKey), quoteString(columnKey))
	s.sugar.Infow("GetCellLatest", "querySQL after", querySQL)
	rows, err = s.store.conn.QueryOne(querySQL)
	if err != nil {
		return
	}
	found = false
	for rows.Next() {
		err = rows.Scan(&resAddedAt, &resRowKey, &resColName, &resRefKey, &resBody, &resCreatedAt)
		if err != nil {
			return
		}
		s.sugar.Infow("GetCellLatest scanned data", "AddedAt", resAddedAt, "RowKey", resRowKey, "ColName", resColName, "RefKey", resRefKey, "Body", resBody, "CreatedAt", resCreatedAt)

		cell.AddedAt = resAddedAt
		cell.RowKey = resRowKey
		cell.ColumnName = resColName
		cell.RefKey = resRefKey
		cell.Body = []byte(resBody)
		var t time.Time
		t, err = time.Parse(time.RFC3339, resCreatedAt)
		if err != nil {
			return
		}
		cell.CreatedAt = &t
		found = true
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
		resCreatedAt string
		locationColumn string
		valueStr string
	)

	switch location {
	case "timestamp":
		fallthrough
	case "created_at":
		locationColumn = "created_at"
		switch value.(type) {
			case *time.Time:
				t := value.(*time.Time)
				valueStr = t.Format(time.RFC3339)
			case time.Time:
				t := value.(time.Time)
				valueStr = t.Format(time.RFC3339)
			default:
				err = fmt.Errorf("PartitionRead unrecognized type %v", reflect.TypeOf(value))

			}
	case "added_at":
		locationColumn = "added_at"
		valueStr = fmt.Sprintf("%d", value)
	default:
		err = errors.New("Unrecognized location " + location)
		return
	}

	sqlStr := fmt.Sprintf(getCellsForShardSQL, locationColumn, valueStr, limit)

	var rows []gorqlite.QueryResult
	s.sugar.Infow("PartitionRead", "query", sqlStr, "value", value)
	stmts := make([]string, 1)
	stmts[0] = sqlStr
	rows, err = s.store.conn.Query(stmts)
	if err != nil {
		return
	}

	found = false
	for _, row := range rows {
		row.Next()
		err = row.Scan(&resAddedAt, &resRowKey, &resColName, &resRefKey, &resBody, &resCreatedAt)
		if err != nil {
			return
		}
		s.sugar.Infow("PartitionRead: scanned data", "AddedAt", resAddedAt, "RowKey", resRowKey, "ColName", resColName, "RefKey", resRefKey, "Body", resBody, "CreatedAt", resCreatedAt)

		var cell models.Cell
		cell.AddedAt = resAddedAt
		cell.RowKey = resRowKey
		cell.ColumnName = resColName
		cell.RefKey = resRefKey
		cell.Body = []byte(resBody)

		var t time.Time
		t, err = time.Parse(time.RFC3339, resCreatedAt)
		if err != nil {
			return
		}
		cell.CreatedAt = &t
		cells = append(cells, cell)
		found = true
	}

	return cells, found, nil
}

func (s *Storage) PutCell(ctx context.Context, rowKey, columnKey string, refKey int64, cell models.Cell) (err error) {
	s.sugar.Infow("PutCell", "rowKey", rowKey, "columnKey", columnKey, "refKey", refKey, "Body", cell.Body)

	insertSQL := fmt.Sprintf(putCellSQL, quoteString(rowKey), quoteString(columnKey), refKey, quoteString(string(cell.Body)))

	s.sugar.Infow("PutCell", "insertSQL", insertSQL)

	stmts := make([]string, 1)
	stmts[0] = insertSQL

	var results []gorqlite.WriteResult
	results, err = s.store.conn.Write(stmts)
	if err != nil {
		return
	}

	for _, v := range results {
		//fmt.Printf("for result %d, %d rows were affected\n",n,v.RowsAffected)
		//fmt.Printf("last insert id was %d\n", v.LastInsertID)
		if v.Err != nil {
			//fmt.Printf("   we have this error: %s\n",v.Err.Error())
			return v.Err
		}
	}
	return
}

// ResetConnection does not destroy the store for in-memory stores.
func (s *Storage) ResetConnection(ctx context.Context, key string) error {
	return nil
}

// Destroy closes the in-memory store, and is a completely destructive operation.
func (s *Storage) Destroy(ctx context.Context) error {
	// TODO(rbastic): What do if there's an error in Sync()?
	s.sugar.Sync()

	s.store.conn.Close()
	return nil
}
