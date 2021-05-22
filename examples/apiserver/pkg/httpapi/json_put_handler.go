package httpapi

import (
	"encoding/json"
	"github.com/rbastic/go-schemaless/examples/apiserver/pkg/api"
	"net/http"
)

func (hs *HTTPAPI) jsonServiceStatusHandler(w http.ResponseWriter, r *http.Request) {
	var resp api.PutResponse
	resp.Success = true

	var request api.PutRequest
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err != nil {
		panic(err)
	}
	if err := r.Body.Close(); err != nil {
		panic(err)
	}
	if err := json.Unmarshal(body, &request); err != nil {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(422) // unprocessable entity
		if err := json.NewEncoder(w).Encode(err); err != nil {
			panic(err)
		}
	}

	respText, err := json.Marshal(resp)
	if err != nil {
		hs.writeError(hs.l, w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(todos); err != nil {
		panic(err)
	}

	_, err = w.Write([]byte(respText))
	if err != nil {
		hs.writeError(hs.l, w, err)
		return
	}
}
