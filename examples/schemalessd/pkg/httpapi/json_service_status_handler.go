package httpapi

import (
	"encoding/json"
	"github.com/rbastic/go-schemaless/examples/schemalessd/pkg/api"
	"net/http"
)

func (hs *HTTPAPI) jsonServiceStatusHandler(w http.ResponseWriter, r *http.Request) {
	var resp api.StatusResponse
	resp.Success = true

	respText, err := json.Marshal(resp)
	if err != nil {
		hs.writeError(hs.l, w, err)
		return
	}

	_, err = w.Write([]byte(respText))
	if err != nil {
		hs.writeError(hs.l, w, err)
		return
	}
}
