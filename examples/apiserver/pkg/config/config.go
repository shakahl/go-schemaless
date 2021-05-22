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

type ShardConfig struct {
	Driver string  `json:"driver"`
	Shards []Shard `json:"shards"`
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
