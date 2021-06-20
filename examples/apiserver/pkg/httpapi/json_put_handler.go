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

	"strings"

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

		indexTableName, jsonIndexField, ok, err := hs.getIndexIfExists(request.Store, request.Table, request.ColumnKey)
		if err != nil {
			hs.l.Error("error getting index", zap.Error(err))
			return
		}
		if ok {
			go func() {
				rowKey := gjson.Get(string(request.Body), jsonIndexField).String() // jsonIndexValue
				if rowKey == "" {
					hs.l.Error(fmt.Sprintf("error with async index write: %s missing", jsonIndexField))
					return
				}

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
				defer cancel()

				err := store.Put(ctx, indexTableName, rowKey, jsonIndexField, request.RefKey, request.Body)
				if err != nil {
					hs.l.Error("error with async index write: Put()", zap.Error(err))
					return
				}

			}()
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

func (hs *HTTPAPI) getIndexIfExists(store, table, columnKey string) (string, string, bool, error) {
	indexKey := table + "_" + strings.ToLower(columnKey)
	asyncIndex, ok := hs.indexMap[indexKey]

	if ok {
		return asyncIndex.IndexTableName, asyncIndex.SourceField, true, nil
	}

	return "", "", false, nil
}
