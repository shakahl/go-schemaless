package httpapi

import (
	"context"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"

	"github.com/rbastic/go-schemaless/core"

	"github.com/rbastic/go-schemaless/examples/schemalessd/pkg/config"

	"strconv"

	stsqlite "github.com/rbastic/go-schemaless/storage/sqlite"
)

func (hs *HTTPAPI) getSqliteShards(prefix string, datastore *config.DatastoreConfig) ([]core.Shard, error) {
	var shards []core.Shard
	nShards := len(datastore.Shards)

	// Iterate every shard (represented as a 'store')
	for i := 0; i < nShards; i++ {
		label := prefix + strconv.Itoa(i)

		store, err := stsqlite.New(prefix, label)
		if err != nil {
			return nil, err
		}

		// Create any necessary secondary index tables on each individual shard
		for j := 0; j < len(datastore.Indexes); j++ {
			for _, idx := range datastore.Indexes {

				sourceField := idx.ColumnDefs[0].IndexData.SourceField
				indexColumn := strings.ToLower(idx.ColumnDefs[0].ColumnName)
				indexTableName := prefix + "_" + indexColumn + "_" + sourceField
				indexKey := prefix + "_" + indexColumn

				err := stsqlite.CreateTable(context.TODO(), store.GetDB(), indexTableName)
				if err != nil {
					return nil, err
				}

				err = stsqlite.CreateIndex(context.TODO(), store.GetDB(), indexTableName)
				if err != nil {
					return nil, err
				}

				hs.registerIndex(indexKey, &AsyncIndex{
					SourceField:    sourceField,
					IndexColumn:    indexColumn,
					IndexTableName: indexTableName,
				})
			}
		}

		shards = append(shards, core.Shard{Name: label, Backend: store})
	}

	return shards, nil
}

