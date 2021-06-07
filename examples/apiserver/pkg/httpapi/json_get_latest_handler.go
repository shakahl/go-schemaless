package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/tidwall/sjson"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/rbastic/go-schemaless/examples/apiserver/pkg/api"
)

func (hs *HTTPAPI) jsonGetLatestHandler(w http.ResponseWriter, r *http.Request) {

	var request api.GetLatestRequest
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

	var resp api.GetLatestResponse

	reqid := uuid.New().String()

	cell, found, err := hs.kv.GetLatest(context.TODO(), request.Table, request.RowKey, request.ColumnKey)
	if err != nil {
		resp.Success = false
		resp.Error = err.Error()
		resp.Found = false
	} else {
		fmt.Printf("%s (server) GetCellLatest localzz: %+v %s %s \n", reqid, cell, found, err)
		resp.Success = true
		resp.Cell = &cell
		resp.Found = found
	}

	cellText, err := json.Marshal(cell)
	if err != nil {
		hs.writeError(hs.l, w, err)
		return
	}

	respBytes, err := json.Marshal(resp)
	if err != nil {
		hs.writeError(hs.l, w, err)
		return
	}

	fmt.Printf("%s cellText->%s\n", reqid, cellText)

	fmt.Printf("%s respTExt before->%s\n", reqid, respBytes)
	respText, err := sjson.SetRaw(string(respBytes), "cell", string(cellText))
	if err != nil {
		hs.writeError(hs.l, w, err)
		return
	}
	fmt.Printf("%s respTExt before->%s\n", reqid, respText)

	fmt.Printf("%s (server) GetCellLatest response: %s\n", reqid, respText)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_, err = w.Write([]byte(respText))
	if err != nil {
		hs.writeError(hs.l, w, err)
		return
	}
}
