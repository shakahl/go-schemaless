package httpapi

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/rbastic/go-schemaless/examples/schemalessd/pkg/api"
)

func (hs *HTTPAPI) jsonFindPartitionHandler(w http.ResponseWriter, r *http.Request) {

	var request api.FindPartitionRequest
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
			hs.writeError(hs.l, w, err)
			return
		}
	}

	var resp api.FindPartitionResponse

	if request.Store == "" {
		resp.Error = ErrMissingStore.Error()
	}

	store, err := hs.getStore(request.Store)
	if err != nil {
		resp.Success = false
		resp.Error = err.Error()
	}

	if resp.Error == "" {
		response, err := store.FindPartition(request.Table, request.RowKey)
		if err != nil {
			resp.Success = false
			resp.Error = err.Error()
		} else {
			resp.PartitionNumber = response
			resp.Success = true
		}

	}

	respBytes, err := json.Marshal(resp)
	if err != nil {
		hs.writeError(hs.l, w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_, err = w.Write(respBytes)
	if err != nil {
		hs.writeError(hs.l, w, err)
		return
	}
}
