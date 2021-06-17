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
	Table      string      `json:"table"`
	ColumnDefs map[string]*ColumnDef `json:"column_defs"`
}

type ColumnDef struct {
	SourceField string  `json:"source_field"`
	Fields    map[string]FieldType `json:"fields"`
}

type FieldType string

type ShardConfig struct {
	Driver     string            `json:"driver"`
	Datastores []DatastoreConfig `json:"datastores"`
}

type DatastoreConfig struct {
	Name    string  `json:"name"`
	Shards  []Shard `json:"shards"`
	Indexes []Index `json:"indexes"`
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
