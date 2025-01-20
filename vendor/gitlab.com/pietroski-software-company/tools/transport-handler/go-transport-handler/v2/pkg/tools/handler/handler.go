//go:build (darwin && cgo) || linux

package transporthandler

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"gitlab.com/pietroski-software-company/devex/golang/concurrent"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	handlers_model "gitlab.com/pietroski-software-company/tools/transport-handler/go-transport-handler/v2/pkg/models/handlers"
	tracer_models "gitlab.com/pietroski-software-company/tools/transport-handler/go-transport-handler/v2/pkg/models/tracer"
	stack_tracer "gitlab.com/pietroski-software-company/tools/transport-handler/go-transport-handler/v2/pkg/tools/tracer/stack"
)

type (
	Handler interface {
		StartServers(servers ServerMapping)
		Cancel()
	}

	Profiler interface {
		Stop()
	}

	profiler struct {
		pprof Profiler
	}

	srvChan struct {
		stopSig         chan os.Signal
		isStopSigClosed bool
		errSig          chan error
		isErrSigClosed  bool

		mtx *sync.Mutex
	}

	ServerMapping map[string]handlers_model.Server

	Opts struct {
		Debug        bool
		ProfilerPort string
	}

	handler struct {
		ctx      context.Context
		cancelFn context.CancelFunc
		osExit   func(code int)

		serverMapping ServerMapping
		offThread     concurrent.Operator
		tracing       tracer_models.Tracer

		srvChan  srvChan
		profiler profiler

		options *Opts

		logger go_logger.Logger

		serverCount *atomic.Uint32
		mtx         *sync.Mutex
	}
)

var (
	OsExit = os.Exit
)

func NewHandler(
	ctx context.Context,
	cancelFn context.CancelFunc,
	exiter func(int),
	prof Profiler,
	logger go_logger.Logger,
	options *Opts,
) Handler {
	ctx, cancelFn = handleCtxGen(ctx, cancelFn)
	logger = handleLogger(ctx, logger)

	return &handler{
		ctx:       ctx,
		cancelFn:  cancelFn,
		osExit:    exiter,
		offThread: concurrent.New("transport-handler", concurrent.WithThreadLimit(1<<6)),

		serverCount: &atomic.Uint32{},

		profiler: profiler{
			pprof: prof,
		},
		logger:  logger,
		options: options,

		tracing: stack_tracer.NewGST(),
		mtx:     &sync.Mutex{},
	}
}

func NewDefaultHandler() Handler {
	ctx, cancelFn := handleCtxGen(nil, nil)
	logger := handleLogger(ctx, nil)

	return &handler{
		ctx:       ctx,
		cancelFn:  cancelFn,
		osExit:    OsExit,
		offThread: concurrent.New("transport-handler", concurrent.WithThreadLimit(1<<6)),

		// TODO: remove profiler field?
		profiler: profiler{
			pprof: nil,
		},
		options: &Opts{
			// With debug set to true, it will, for now:
			// - start pprof server.
			Debug: true,
		},
		logger: logger,

		tracing:     stack_tracer.NewGST(),
		serverCount: &atomic.Uint32{},
		mtx:         &sync.Mutex{},
	}
}

// StartServers starts all the variadic given servers and blocks the main thread.
func (h *handler) StartServers(servers ServerMapping) {
	h.serverMapping = servers
	servers = h.handlePprof(servers)
	h.makeSrvChan(len(servers))

	signal.Notify(h.srvChan.stopSig, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	for sName, s := range servers {
		h.logger.Infof(
			"starting server",
			go_logger.Field{"server_name": sName},
		)
		h.offThread.Op(func() {
			defer h.handleStartPanic()
			if err := s.Start(); err != nil {
				h.handleErr(fmt.Errorf("error starting server %s: %w", sName, err))
			}
		})
		h.logger.Infof(
			"started server",
			go_logger.Field{"server_name": sName},
		)
	}

	h.handleServer()
}

func (h *handler) handleServer() {
	for {
		select {
		case stopSig := <-h.srvChan.stopSig:
			h.logger.Warningf(
				"stop server sig!!",
				go_logger.Field{"signal": stopSig.String()},
			)
			h.handleShutdown()
			h.osExit(0)
			return
		case err := <-h.srvChan.errSig:
			h.logger.Warningf(
				"err server sig!!",
				go_logger.Field{"error": err},
			)
			h.handleShutdown()
			if err != nil {
				h.osExit(1)
			} else {
				h.osExit(0)
			}

			return
		case ctxSig := <-h.ctx.Done():
			h.logger.Warningf(
				"ctx server sig!!",
				go_logger.Field{"context": ctxSig},
				go_logger.Field{"error": h.ctx.Err()},
			)

			h.srvChan.handleErr(nil)
		}
	}
}

func (h *handler) handleShutdown() {
	h.cancel()
	h.stopServers()
	h.stopProfiler()

	h.handleWaiting()

	h.closeChannels()
	h.tracing.Trace()
}

func (h *handler) cancel() {
	h.logger.Infof("cancelling all contexts")
	h.cancelFn()
	h.logger.Infof("all contexts cancelled")
}

func (h *handler) sigKill() {
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGQUIT)
}

func (h *handler) closeChannels() {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	h.closeSrvSigChan()
	h.closeErrChan()
}

func (h *handler) Cancel() {
	h.cancel()
}
