package api

import "github.com/rbastic/go-schemaless/models"

// StatusResponse contains a simplified health response.
type StatusResponse struct {
	Error   string `json:"error,omitempty"`
	Success bool   `json:"success"`
}

// PutRequest is for issuing Put() calls to the Schemaless data store
type PutRequest struct {
	Store     string `json:"store"`
	Table     string `json:"table"`
	RowKey    string `json:"rowKey"`
	ColumnKey string `json:"columnKey"`
	RefKey    int64  `json:"refKey"`
	Body      string `json:"body"`
}

// PutResponse specifies the response for a Put operation
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
	Value           string `json:"value"`
	Limit           int    `json:"limit"`
}

type PartitionReadResponse struct {
	Error   string `json:"error,omitempty"`
	Success bool   `json:"success"`
	Found   bool   `json:"found"`

	Cells []models.Cell `json:"cells"`
}

type FindPartitionRequest struct {
	Store  string `json:"store"`
	Table  string `json:"table"`
	RowKey string `json:"rowKey"`
}

type FindPartitionResponse struct {
	PartitionNumber int `json:"partitionNumber"`

	Error   string `json:"error,omitempty"`
	Success bool   `json:"success"`
}
