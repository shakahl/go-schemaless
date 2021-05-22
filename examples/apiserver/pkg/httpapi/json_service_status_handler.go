package httpapi

import (
	"encoding/json"
	"github.com/rbastic/go-schemaless/examples/apiserver/pkg/api"
	"net/http"
)

func (hs *HTTPAPI) jsonServiceStatusHandler(w http.ResponseWriter, r *http.Request) {
	// TODO
	//l, err := go-schemaless/examples/apiservergit.GetLogger(r.Context(), hs.l)
	//if err != nil {
	//	hs.writeError(nil, w, err)
	//	return
	//}

	l := hs.l

	var resp api.StatusResponse
	resp.Success = true

	respText, err := json.Marshal(resp)
	if err != nil {
		hs.writeError(l, w, err)
		return
	}

	_, err = w.Write([]byte(respText))
	if err != nil {
		hs.writeError(l, w, err)
		return
	}
}
