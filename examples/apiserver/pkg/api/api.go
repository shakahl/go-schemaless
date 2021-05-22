package api

type Base struct {
	SiteTitle string
}

type ErrorResponse struct {
	SiteTitle       string
	ErrorText       string
	Error           string
	BackToDashboard string
}

type StatusResponse struct {
	Base
	Error   string `json:"error,omitempty"`
	Success bool   `json:"success"`
}
