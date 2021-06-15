package config

import (
	"encoding/json"
	"io/ioutil"
)

type Shard struct {
	Label    string `json:"label"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type Index struct {
	Table string `json:"table"`
	DataStore string `json:"datastore"`
	ColumnDefs []ColumnDef `json:"column_defs"`
}

type ColumnDef struct {
	ColumnKey string `json:"column_key"`
	Fields []Field `json:"fields"`
}

type Field struct {
	Field string `json:"field"`
	Type string `json:"type"`
}

type ShardConfig struct {
	Driver     string            `json:"driver"`
	Datastores []DatastoreConfig `json:"datastores"`
}

type DatastoreConfig struct {
	Name   string  `json:"name"`
	Shards []Shard `json:"shards"`
	Indexes
}

func LoadConfig(file string) (*ShardConfig, error) {
	contents, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	var shardConfig ShardConfig
	err = json.Unmarshal(contents, &shardConfig)
	if err != nil {
		return nil, err
	}

	return &shardConfig, nil
}
