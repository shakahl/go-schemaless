package api

import "github.com/rbastic/go-schemaless/models"

type StatusResponse struct {
	Error   string `json:"error,omitempty"`
	Success bool   `json:"success"`
}

type PutRequest struct {
	Store     string `json:"store"`
	Table     string `json:"table"`
	RowKey    string `json:"rowKey"`
	ColumnKey string `json:"columnKey"`
	RefKey    int64  `json:"refKey"`
	Body      string `json:"body"`
}

type PutResponse struct {
	Error   string `json:"error,omitempty"`
	Success bool   `json:"success"`
}

type GetRequest struct {
	Store     string `json:"store"`
	Table     string `json:"table"`
	RowKey    string `json:"rowKey"`
	ColumnKey string `json:"columnKey"`
	RefKey    int64  `json:"refKey"`
}

type GetResponse struct {
	Error   string `json:"error,omitempty"`
	Success bool   `json:"success"`
	Found   bool   `json:"found"`

	Cell models.Cell `json:"cell"`
}

type GetLatestRequest struct {
	Store     string `json:"store"`
	Table     string `json:"table"`
	RowKey    string `json:"rowKey"`
	ColumnKey string `json:"columnKey"`
}

type GetLatestResponse struct {
	Error   string `json:"error,omitempty"`
	Success bool   `json:"success"`
	Found   bool   `json:"found"`

	Cell models.Cell `json:"cell"`
}

type PartitionReadRequest struct {
	Store           string `json:"store"`
	Table           string `json:"table"`
	PartitionNumber int    `json:"partitionNumber"`
	Location        string `json:"location"`
	Value           uint64 `json:"value"`
	Limit           int    `json:"limit"`
}

type PartitionReadResponse struct {
	Error   string `json:"error,omitempty"`
	Success bool   `json:"success"`
	Found   bool   `json:"found"`

	Cells []models.Cell `json:"cells"`
}
