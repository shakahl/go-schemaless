package httpapi

import (
	"github.com/rbastic/go-schemaless/examples/apiserver/pkg/api"

	"encoding/json"
	"fmt"
	"go.uber.org/zap"

	"net/http"
)

const badMarshal = "failed to marshal response when attempting to write error"

// We reuse the api.StatusResponse type to write an error.
func (hs *HTTPAPI) writeError(l *zap.Logger, w http.ResponseWriter, callerErr error) {
	// Log the error
	if l == nil {
		l = hs.l
	}

	l.Error("error", zap.Error(callerErr))

	// write the error to the user, not very friendly, but we'll get there.
	var resp api.StatusResponse
	resp.Error = callerErr.Error()

	marshaledError, err := json.Marshal(resp)
	if err != nil {
		l.Error(badMarshal, zap.Error(err), zap.Error(callerErr))
		return
	}

	// write server error header + write error as JSON
	w.WriteHeader(http.StatusInternalServerError)
	fmt.Fprintf(w, string(marshaledError))
}
