package pprofx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
)

const (
	contextCanceledErrMsg     = "context canceled"
	DefaultPprofServerPort    = "7001"
	DefaultPprofServerNameKey = "internal-pprof-server"
)

type (
	Server struct {
		ctx    context.Context
		server *http.Server

		name            string
		pprofServerPort string
		defaultTimeouts time.Duration

		logger slogx.SLogger
	}
)

func NewPProfServer(
	ctx context.Context,
	opts ...options.Option,
) *Server {
	s := &Server{
		ctx:             ctx,
		name:            DefaultPprofServerNameKey,
		pprofServerPort: DefaultPprofServerPort,
		defaultTimeouts: time.Second * 30,

		logger: slogx.New(),
	}
	options.ApplyOptions(s, opts...)

	s.handle()

	return s
}

func (svr *Server) handle() {
	mux := http.NewServeMux()
	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", svr.pprofServerPort),
		Handler: mux,

		ReadTimeout:       svr.defaultTimeouts,
		ReadHeaderTimeout: svr.defaultTimeouts,
		WriteTimeout:      svr.defaultTimeouts,
		IdleTimeout:       svr.defaultTimeouts,

		MaxHeaderBytes: 0,
		TLSConfig:      nil,
		TLSNextProto:   nil,
		ConnState:      nil,
		ErrorLog:       nil,
		BaseContext:    nil,
		ConnContext:    nil,
	}

	// TODO: remove health check routes from pprof server
	mux.HandleFunc("/debug/health", healthHandler)
	mux.HandleFunc("/debug/readiness", readinessHandler)
	mux.HandleFunc("/debug/liveness", livenessHandler)

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/vars", http.DefaultServeMux)

	svr.server = server
}

func (svr *Server) Start() error {
	svr.logger.Debug(svr.ctx, "starting pprof's HTTP server", "name", svr.name, "port", svr.pprofServerPort)

	if err := svr.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

func (svr *Server) Stop() {
	defer func() {
		if r := recover(); r != nil {
			svr.logger.Warn(svr.ctx, "recovering from panic", "panic", r)
		}
	}()

	if err := svr.server.Shutdown(svr.ctx); err != nil &&
		!errors.Is(err, http.ErrServerClosed) &&
		err.Error() != contextCanceledErrMsg {
		svr.logger.Error(svr.ctx, "error shutting down pprof's http server", "error", err)

		return
	}

	svr.logger.Debug(svr.ctx, "pprof's http server stopped", "name", svr.name, "port", svr.pprofServerPort)
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func readinessHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func livenessHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}
