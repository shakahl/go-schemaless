package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/rbastic/go-schemaless/examples/apiserver/pkg/api"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

var ErrMissingStore = errors.New("store not specified in request")

func (hs *HTTPAPI) jsonPutHandler(w http.ResponseWriter, r *http.Request) {

	var request api.PutRequest
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

	var resp api.PutResponse
	resp.Success = true

	if request.Store == "" {
		resp.Error = ErrMissingStore.Error()
	}

	store, err := hs.getStore(request.Store)
	if err != nil {
		resp.Success = false
		resp.Error = err.Error()
	}

	if resp.Error == "" {
		err = store.Put(context.TODO(), request.Table, request.RowKey, request.ColumnKey, request.RefKey, request.Body)
		if err != nil {
			resp.Success = false
			resp.Error = err.Error()
		}

		// Very basic async denormalized secondary index tables
		if request.Store == "trips" && request.Table == "trips" && request.ColumnKey == "BASE" {
			hs.l.Info("inside async put handler")
			indexTable := "trips_base_driver_partner_uuid"
			columnKey := "BASE"
			rowKey := gjson.Get(string(request.Body), "driver_partner_uuid").String()
			if rowKey == "" {
				hs.l.Info("error with async index write: driver_partner_uuid missing")
				return
			}

			refKey := time.Now().UTC().UnixNano()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
			defer cancel()
			fmt.Printf("store:%+v indexTable:%s rowKey:%s columnkey:%s refKey:%d body:%s\n", store, indexTable, rowKey, columnKey, refKey, request.Body)
			err := store.Put(ctx, indexTable, rowKey, columnKey, refKey, request.Body)
			if err != nil {
				hs.l.Info("error with async index write: Put()", zap.Error(err))
				return
			}
		}
	}
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
