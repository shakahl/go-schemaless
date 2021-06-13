package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/rbastic/go-schemaless/examples/apiserver/pkg/api"
	"github.com/rbastic/go-schemaless/models"

	"strconv"
)

func (hs *HTTPAPI) jsonPartitionReadHandler(w http.ResponseWriter, r *http.Request) {

	var request api.PartitionReadRequest
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
		hs.writeError(hs.l, w, err)
		return
	}

	var resp api.PartitionReadResponse

	var cells []models.Cell
	var found bool

	if request.Store == "" {
		resp.Error = ErrMissingStore.Error()
	}

	intValue, err := strconv.ParseInt(request.Value, 10, 64)
	if err != nil {
		hs.writeError(hs.l, w, err)
		return
	}

	store, err := hs.getStore(request.Store)
	if err != nil {
		resp.Success = false
		resp.Error = err.Error()
	} else {
		resp.Success = true

		cells, found, err = store.PartitionRead(context.TODO(), request.Table, request.PartitionNumber, request.Location, intValue, request.Limit)
		if err != nil {
			resp.Success = false
			resp.Error = err.Error()
		}

	}

	resp.Cells = cells
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
