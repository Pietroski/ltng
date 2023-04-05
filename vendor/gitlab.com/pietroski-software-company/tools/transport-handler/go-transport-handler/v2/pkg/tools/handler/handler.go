//go:build (darwin && cgo) || linux

package transporthandler

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

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

	goPool struct {
		wg  *sync.WaitGroup
		gst tracer_models.Tracer
	}

	srvChan struct {
		stopSig  chan os.Signal
		errSig   chan error
		panicSig chan bool
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

		srvMap        *sync.Map
		serverMapping ServerMapping

		goPool   goPool
		srvChan  srvChan
		profiler profiler

		options *Opts

		logger go_logger.Logger
	}
)

var (
	makeStopSrvSig = func() chan os.Signal {
		return make(chan os.Signal)
	}

	makeErrSrvSig = func(n int) chan error {
		return make(chan error, n)
	}
	makePanicSrvSig = func(n int) chan bool {
		return make(chan bool, n)
	}

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
		ctx:      ctx,
		cancelFn: cancelFn,
		osExit:   exiter,

		goPool: goPool{
			wg:  &sync.WaitGroup{},
			gst: stack_tracer.NewGST(),
		},

		srvMap: &sync.Map{},

		profiler: profiler{
			pprof: prof,
		},

		logger: logger,

		options: options,
	}
}

func NewDefaultHandler() Handler {
	ctx, cancelFn := handleCtxGen(nil, nil)
	logger := handleLogger(ctx, nil)

	return &handler{
		ctx:      ctx,
		cancelFn: cancelFn,
		osExit:   OsExit,

		// TODO: remove profiler field?
		profiler: profiler{
			pprof: nil,
		},

		goPool: goPool{
			wg:  &sync.WaitGroup{},
			gst: stack_tracer.NewGST(),
		},

		options: &Opts{
			// With debug set to true, it will, for now:
			// - start pprof server.
			Debug: true,
		},

		logger: logger,
	}
}

// StartServers starts all the variadic given servers and blocks the main thread.
func (h *handler) StartServers(servers ServerMapping) {
	servers = h.handlePprof(servers)
	h.makeSrvChan(len(servers))
	h.serverMapping = servers

	signal.Notify(h.srvChan.stopSig, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	for sName, s := range servers {
		h.logger.Infof(
			"starting server",
			go_logger.Field{"server_name": sName},
		)
		h.goPool.wg.Add(1)
		go func(sName string, s handlers_model.Server) {
			defer h.handlePanic()
			defer h.goPool.wg.Done()

			h.logger.Infof(
				"started server",
				go_logger.Field{"server_name": sName},
			)
			if err := s.Start(); err != nil {
				h.handleErr(err)
			}
		}(sName, s)
		//time.Sleep(time.Millisecond * 50)
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
		case errSig := <-h.srvChan.errSig:
			h.logger.Warningf(
				"err server sig!!",
				go_logger.Field{"error": errSig.Error()},
			)
			h.handleShutdown()
			h.osExit(1)
			return
		case panicSig := <-h.srvChan.panicSig:
			h.logger.Warningf(
				"panic server sig!!",
				go_logger.Field{"hasPanicked": panicSig},
			)
			h.handleShutdown()
			h.osExit(2)
			return
		case ctxSig := <-h.ctx.Done():
			h.logger.Warningf(
				"ctx server sig!!",
				go_logger.Field{"context": ctxSig},
			)
			h.srvChan.errSig <- h.ctx.Err()
		}
	}
}

func (h *handler) handleShutdown() {
	h.stopServers()
	h.cancel()
	h.sigKill()
	h.handleWaiting()
	h.closeSrvSigChan()
	h.closeErrChan()
	h.closePanicChan()
	h.goPool.gst.Trace()
	h.stopProfiler()
}

func (h *handler) cancel() {
	h.logger.Infof("cancelling all contexts")
	h.cancelFn()
	h.logger.Infof("all contexts cancelled")
}

func (h *handler) Cancel() {
	h.cancel()
}
