package factories

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"
)

const (
	contextCanceledErrMsg     = "context canceled"
	DefaultPprofServerPort    = "7001"
	DefaultPprofServerNameKey = "internal-pprof-server"
)

type (
	PProfServer struct {
		ctx    context.Context
		server *http.Server

		name            string
		pprofServerPort string
		defaultTimeouts time.Duration

		logger go_logger.Logger
	}
)

func NewPProfServer(
	ctx context.Context,
	opts ...options.Option,
) *PProfServer {
	s := &PProfServer{
		ctx:             ctx,
		name:            DefaultPprofServerNameKey,
		pprofServerPort: DefaultPprofServerPort,
		defaultTimeouts: time.Second * 30,

		logger: go_logger.NewGoLogger(ctx,
			&go_logger.Publishers{},
			&go_logger.Opts{
				Debug:   true,
				Publish: false,
			},
		),
	}
	options.ApplyOptions(s, opts...)

	s.handle()

	return s
}

func (svr *PProfServer) handle() {
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

func (svr *PProfServer) Start() error {
	svr.logger.Infof(
		"starting pprof's HTTP server",
		go_logger.Field{"port": svr.pprofServerPort},
	)

	return svr.server.ListenAndServe()
}

func (svr *PProfServer) Stop() {
	defer func() {
		if r := recover(); r != nil {
			svr.logger.Warningf(
				"recovering from panic",
				go_logger.Field{"recover": fmt.Sprintf("%v", r)},
			)
		}
	}()

	if err := svr.server.Shutdown(svr.ctx); err != nil &&
		err != http.ErrServerClosed &&
		err.Error() != contextCanceledErrMsg {
		svr.logger.Errorf(
			"pprof's HTTP server ListenAndServe shutdown error",
			go_logger.Field{"error": err.Error()},
		)

		return
	}

	svr.logger.Infof(
		"HTTP server ListenAndServe shutdown ok",
		go_logger.Field{"name": svr.name},
	)
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
