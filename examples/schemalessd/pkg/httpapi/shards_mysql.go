package httpapi

import (
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"

	"github.com/rbastic/go-schemaless/core"

	"github.com/rbastic/go-schemaless/examples/schemalessd/pkg/config"

	"strconv"

	stmysql "github.com/rbastic/go-schemaless/storage/mysql"
)

func (hs *HTTPAPI) getMysqlShards(prefix string, datastore *config.DatastoreConfig) ([]core.Shard, error) {
	var shards []core.Shard
	nShards := len(datastore.Shards)

	// Iterate every shard (represented as a 'store')
	for i := 0; i < nShards; i++ {
		label := prefix + strconv.Itoa(i)

		host := datastore.Shards[i].Host
		port := datastore.Shards[i].Port
		user := datastore.Shards[i].Username
		pass := datastore.Shards[i].Password
		dbname := datastore.Shards[i].Database

		store := stmysql.New().
			WithHost(host).
			WithPort(port).
			WithUser(user).
			WithPass(pass).
			WithDatabase(dbname)

		err := store.WithZap()
		if err != nil {
			return nil, err
		}
		err = store.Open()
		if err != nil {
			return nil, err
		}

		// Create any necessary secondary index tables on each individual shard
		for j := 0; j < len(datastore.Indexes); j++ {
			for _, idx := range datastore.Indexes {

				idxData := idx.ColumnDefs[0].IndexData

				sourceField := idxData.SourceField
				indexColumn := strings.ToLower(idx.ColumnDefs[0].ColumnName)
				indexTableName := prefix + "_" + indexColumn + "_" + sourceField
				indexKey := prefix + "_" + indexColumn

				var fields []string

				for k := range idxData.Fields {
					fields = append(fields, k)
				}

				hs.registerIndex(indexKey, &AsyncIndex{
					SourceField:    sourceField,
					IndexColumn:    indexColumn,
					IndexTableName: indexTableName,
					Fields:         fields,
				})
			}
		}
		shards = append(shards, core.Shard{Name: label, Backend: store})
	}

	return shards, nil
}


