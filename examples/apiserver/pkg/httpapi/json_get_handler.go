package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/rbastic/go-schemaless/examples/apiserver/pkg/api"
	"github.com/rbastic/go-schemaless/models"
)

func (hs *HTTPAPI) jsonGetHandler(w http.ResponseWriter, r *http.Request) {

	var request api.GetRequest
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err != nil {
		hs.writeError(hs.l, w, err)
		return
	}
	if err := r.Body.Close(); err != nil {
		hs.writeError(hs.l, w, err)
		return
	}

	if err := json.Unmarshal(body, &request); err != nil {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusUnprocessableEntity)
		if err := json.NewEncoder(w).Encode(err); err != nil {
			panic(err)
		}
	}

	var resp api.GetResponse
	resp.Success = true

	var cell models.Cell
	var found bool

	cell, found, err = hs.kv.Get(context.TODO(), request.Table, request.RowKey, request.ColumnKey, request.RefKey)
	if err != nil {
		resp.Success = false
		resp.Error = err.Error()
	}

	resp.Cell = cell
	resp.Found = found

	respText, err := json.Marshal(resp)
	if err != nil {
		hs.writeError(hs.l, w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write([]byte(respText))
	if err != nil {
		hs.writeError(hs.l, w, err)
		return
	}
}
