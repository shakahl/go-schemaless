package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/render"
	"github.com/kelseyhightower/envconfig"

	"go.uber.org/zap"

	"github.com/rbastic/go-schemaless"
	"github.com/rbastic/go-schemaless/core"

	loggerMiddleware "github.com/rbastic/go-schemaless/examples/schemalessd/pkg/middleware/zap"

	"net/http"

	"github.com/rbastic/go-schemaless/examples/schemalessd/pkg/config"

	"time"
)

type Specification struct {
	Address  string
	Protocol string
	Site     string

	ShardConfigFile string
}

type AsyncIndex struct {
	SourceField    string
	IndexColumn    string
	IndexTableName string
	Fields         []string
}

// HTTPAPI encapsulates everything we need to run a webserver.
type HTTPAPI struct {
	Address  string
	Protocol string
	Site     string

	hs *http.Server
	l  *zap.Logger

	Stores map[string]*schemaless.DataStore

	shardConfig *config.ShardConfig

	indexMap map[string]*AsyncIndex
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
		log.Fatal("please set APP_SHARDCONFIGFILE")
	}

	hs.indexMap = make(map[string]*AsyncIndex)

	hs.shardConfig, err = config.LoadConfig(s.ShardConfigFile)
	if err != nil {
		log.Fatal(err)
	}

	err = hs.loadShards()
	if err != nil {
		log.Fatal(err)
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
		r.Post("/findPartition", hs.jsonFindPartitionHandler)
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
	driver := hs.shardConfig.Driver

	hs.Stores = make(map[string]*schemaless.DataStore)

	for _, datastore := range hs.shardConfig.Datastores {
		label := datastore.Name

		var shards []core.Shard
		var err error
		switch driver {
		case "mysql":
			shards, err = hs.getMysqlShards(label, &datastore)
			if err != nil {
				return err
			}
		case "sqlite3":
			shards, err = hs.getSqliteShards(label, &datastore)
			if err != nil {
				return err
			}
		case "postgres":
			shards, err = hs.getPostgresShards(label, &datastore)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognized driver: '%s'", driver)

		}

		if _, ok := hs.Stores[datastore.Name]; !ok {
			store, ok := hs.Stores[datastore.Name]
			if !ok {
				store = schemaless.New()
			}
			fmt.Printf("with sources name: %s datastore.Name %s\n", label, datastore.Name)
			hs.Stores[datastore.Name] = store.WithSources(datastore.Name, shards).WithName(label, label)
		}
	}

	return nil
}

func (hs *HTTPAPI) getStore(storeName string) (*schemaless.DataStore, error) {
	store, ok := hs.Stores[storeName]
	if !ok {
		return nil, errors.New("store not found")
	}

	return store, nil
}

func (hs *HTTPAPI) registerIndex(key string, ai *AsyncIndex) {
	hs.indexMap[key] = ai
}
