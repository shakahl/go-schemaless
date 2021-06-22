package main

import (
	"github.com/rbastic/go-schemaless/examples/apiserver/pkg/httpapi"
	"github.com/rbastic/go-schemaless/examples/apiserver/pkg/log"

	"context"
	"errors"
	"fmt"

	"github.com/oklog/run"

	"go.uber.org/zap"

	"os"
	"os/signal"

	"syscall"

	"time"
)

func main() {
	var useJSON bool
	if os.Getenv("JSON") != "" {
		useJSON = true
	}

	logger, err := log.New(useJSON)
	if err != nil {
		logger.Error("couldn't initialize logger", zap.Error(err))
		os.Exit(-1)
	}

	logger.Info("Booting go-schemaless/examples/apiserver")
	hs, err := httpapi.New(logger)
	if err != nil {
		logger.Error("couldn't create http server", zap.Error(err))
		os.Exit(-1)
	}

	var g run.Group
	{
		g.Add(func() error {
			logger.Info("starting http server run.Group")
			return hs.Start()
		}, func(error) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
			defer cancel()
			logger.Error("httpserver shutting down", zap.Error(hs.Stop(ctx)))
		})
	}

	{
		cancel := make(chan struct{})
		g.Add(func() error {
			return interrupt(cancel)
		}, func(error) {
			close(cancel)
		})
	}

	err = g.Run()
	if err != nil {
		logger.Error("error", zap.Error(err))
		os.Exit(-1)
	}

	logger.Info("server gracefully shutdown")
}

func interrupt(cancel <-chan struct{}) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-c:
		return fmt.Errorf("received signal %s", sig)
	case <-cancel:
		return errors.New("canceled")
	}
}
