package api

type ErrorResponse struct {
	ErrorText       string
	Error           string
}

type StatusResponse struct {
	Error   string `json:"error,omitempty"`
	Success bool   `json:"success"`
}

type PutRequest struct {
	Table string `json:"table"`
	RowKey string `json:"rowKey"`
	ColumnKey string `json:"columnKey"`
	RefKey int64 `json:"refKey"`
	Body string `json:"body"`
}

type PutResponse struct {
	Error   string `json:"error,omitempty"`
	Success bool   `json:"success"`
}
