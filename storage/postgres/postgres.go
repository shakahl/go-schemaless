// Package postgres is a postgres-backed Schemaless store.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/rbastic/go-schemaless/models"
	"go.uber.org/zap"
)

// Storage is a Postgres-backed storage.
type Storage struct {
	store *sql.DB
	sugar *zap.SugaredLogger
}

const (
	driver = "postgres"
	// dsnFormat string parameters: username, password, host, port, database.
	dsnFormat           = "postgres://%s:%s@%s/%s?sslmode=disable"
	getCellSQL          = "SELECT added_at, row_key, column_name, ref_key, body,created_at FROM cell WHERE row_key = $1 AND column_name = $2 AND ref_key = $3 LIMIT 1"
	getCellLatestSQL    = "SELECT added_at, row_key, column_name, ref_key, body, created_at FROM cell WHERE row_key = $1 AND column_name = $2 ORDER BY ref_key DESC LIMIT 1"
	getCellsForShardSQL = "SELECT added_at, row_key, column_name, ref_key, body, created_at FROM cell WHERE %s > $1 LIMIT %d"
	putCellSQL          = "INSERT INTO cell ( row_key, column_name, ref_key, body ) VALUES($1, $2, $3, $4)"
)

func exec(db *sql.DB, sqlStr string) error {
	_, err := db.Exec(sqlStr)
	if err != nil {
		return err
	}
	return nil
}

// New returns a new postgres-backed Storage
func New(user, pass, host, port, database string) *Storage {
	// TODO(rbastic): We do not Sprintf() the port.
	// TODO(rbastic): Hmmm.. Should I ping the db?
	db, err := sql.Open(driver, fmt.Sprintf(dsnFormat, user, pass, host, database))
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

func (s *Storage) GetCell(ctx context.Context, rowKey string, columnKey string, refKey int64) (cell models.Cell, found bool, err error) {
	var (
		resAddedAt   uint64
		resRowKey    string
		resColName   string
		resRefKey    int64
		resBody      string
		resCreatedAt uint64
		rows         *sql.Rows
	)
	s.sugar.Infow("GetCell", "query", getCellSQL, "rowKey", rowKey, "columnKey", columnKey, "refKey", refKey)
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
		s.sugar.Infow("GetCell scanned data", "AddedAt", resAddedAt, "RowKey", resRowKey, "ColName", resColName, "RefKey", resRefKey, "Body", resBody, "CreatedAt", resCreatedAt)

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
		resAddedAt   uint64
		resRowKey    string
		resColName   string
		resRefKey    int64
		resBody      string
		resCreatedAt uint64
		rows         *sql.Rows
	)
	s.sugar.Infow("GetCellLatest", "query", getCellSQL, "rowKey", rowKey, "columnKey", columnKey)
	rows, err = s.store.QueryContext(ctx, getCellLatestSQL, rowKey, columnKey)
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
		s.sugar.Infow("GetCellLatest scanned data", "AddedAt", resAddedAt, "RowKey", resRowKey, "ColName", resColName, "RefKey", resRefKey, "Body", resBody, "CreatedAt", resCreatedAt)

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

func (s *Storage) PartitionRead(ctx context.Context, partitionNumber int, location string, value uint64, limit int) (cells []models.Cell, found bool, err error) {

	var (
		resAddedAt   uint64
		resRowKey    string
		resColName   string
		resRefKey    int64
		resBody      string
		resCreatedAt uint64

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
	s.sugar.Infow("PutCell", "rowKey", rowKey, "columnKey", columnKey, "refKey", refKey, "Body", cell.Body)
	res, err = stmt.Exec(rowKey, columnKey, refKey, cell.Body)
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
	// TODO(rbastic): What do if there's an error in Sync()?
	s.sugar.Sync()
	return s.store.Close()
}
