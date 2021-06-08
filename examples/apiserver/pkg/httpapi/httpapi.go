package httpapi

import (
	"context"
	"fmt"

	_ "github.com/mattn/go-sqlite3"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/render"
	"github.com/kelseyhightower/envconfig"

	"go.uber.org/zap"

	"github.com/rbastic/go-schemaless"
	"github.com/rbastic/go-schemaless/core"

	loggerMiddleware "github.com/rbastic/go-schemaless/examples/apiserver/pkg/middleware/zap"

	"net/http"

	"github.com/rbastic/go-schemaless/examples/apiserver/pkg/config"

	"strconv"
	"time"

	st "github.com/rbastic/go-schemaless/storage/sqlite"
)

type Specification struct {
	Address  string
	Protocol string
	Site     string

	ShardConfigFile string
}

// HTTPAPI encapsulates everything we need to run a webserver.
type HTTPAPI struct {
	Address  string
	Protocol string
	Site     string

	hs *http.Server
	l  *zap.Logger

	kv *schemaless.DataStore

	shardConfig *config.ShardConfig
}

// New requires a zap logger (see pkg/log, and/or
// go.uber.org/zap) and a suitable config object.  It returns
// an HTTPAPI or nil and an error.  Properly configured, the
// HTTPAPI should be ready to have Start() called.
func New(l *zap.Logger) (*HTTPAPI, error) {

	var s Specification
	err := envconfig.Process("app", &s)
	if err != nil {
		return nil, err
	}

	var hs HTTPAPI
	hs.Protocol = s.Protocol
	hs.Address = s.Address

	if hs.Address == "" {
		l.Info("consider setting ADDRESS=...")
	}

	if hs.Protocol == "" {
		l.Info("consider setting PROTOCOL=... [to https for production use - defaulted to http]")
		hs.Protocol = "http"
	}

	if hs.Site == "" {
		l.Info("consider setting SITE=... [defaulted to localhost]")
		hs.Site = "localhost"
	}

	// Set our logger
	hs.l = l

	if s.ShardConfigFile == "" {
		panic("please set APP_SHARDCONFIGFILE")
	}

	hs.shardConfig, err = config.LoadConfig(s.ShardConfigFile)
	if err != nil {
		panic(err)
	}

	err = hs.loadShards()
	if err != nil {
		panic(err)
	}

	mux := chi.NewRouter()
	mux.NotFound(hs.notFoundHandler)

	mux.Use(middleware.RequestID)
	mux.Use(middleware.RealIP)
	mux.Use(loggerMiddleware.Logger(hs.l))
	mux.Use(middleware.Recoverer)

	mux.Route("/service", func(r chi.Router) {
		render.SetContentType(render.ContentTypeJSON)

		r.Get("/status", hs.jsonServiceStatusHandler)
	})

	mux.Route("/api", func(r chi.Router) {
		render.SetContentType(render.ContentTypeJSON)

		r.Post("/put", hs.jsonPutHandler)
		r.Post("/get", hs.jsonGetHandler)
		r.Post("/getLatest", hs.jsonGetLatestHandler)
		r.Post("/partitionRead", hs.jsonPartitionReadHandler)
	})

	server := &http.Server{
		Addr:    hs.Address,
		Handler: mux,

		ReadTimeout:  720 * time.Second, // TODO externalize constants
		WriteTimeout: 720 * time.Second,
		IdleTimeout:  720 * time.Second,
	}

	hs.hs = server

	return &hs, nil
}

// Start attempts to run the HTTPAPI, optionally returning an error.
// If no error is returned, the HTTPAPI should be running.
func (hs *HTTPAPI) Start() error {
	hs.l.Debug("Starting server", zap.String("address", hs.Address))

	if err := hs.hs.ListenAndServe(); err != nil {
		return err
	}

	return nil
}

// Stop attempts to shut down a webserver.  An error will be returned if the
// shutdown is unsuccessful or the timeout exceeded.
func (hs *HTTPAPI) Stop(ctx context.Context) error {
	return hs.hs.Shutdown(ctx)
}

func (hs *HTTPAPI) notFoundHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
	fmt.Fprintf(w, "We didn't recognize the page you're trying to access.")
}

func (hs *HTTPAPI) loadShards() error {
	label := hs.shardConfig.Shards[0].Label
	driver := hs.shardConfig.Driver

	switch driver {
	case "sqlite3":
		shards, err := hs.getSqliteShards(label)
		if err != nil {
			return err
		}
		hs.kv = schemaless.New().WithSource(shards)
	default:
		return fmt.Errorf("unrecognized driver: %s", driver)
	}

	return nil
}

func (hs *HTTPAPI) getSqliteShards(prefix string) ([]core.Shard, error) {
	var shards []core.Shard
	nShards := len(hs.shardConfig.Shards)

	for i := 0; i < nShards; i++ {
		label := prefix + strconv.Itoa(i)
		// NOTE: sqlite implementation supports only a single table per shard created at start time 
		st, err := st.New("cell", label)
		if err != nil {
			return nil, err
		}
		shards = append(shards, core.Shard{Name: label, Backend: st})
	}

	return shards, nil
}
