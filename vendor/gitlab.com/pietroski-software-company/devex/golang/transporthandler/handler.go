package transporthandler

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"gitlab.com/pietroski-software-company/devex/golang/concurrent"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	stack_tracer "gitlab.com/pietroski-software-company/devex/golang/transporthandler/internal/tools/stacktracer"
	"gitlab.com/pietroski-software-company/devex/golang/transporthandler/models"
)

type (
	handler struct {
		ctx        context.Context
		cancelFunc context.CancelFunc
		osExit     func(code int)

		serverMapping ServerMapping
		offThread     concurrent.Operator
		tracing       models.Tracer
		logger        go_logger.Logger

		srvChan *srvChan
	}

	srvChan struct {
		stopSig         chan os.Signal
		isStopSigClosed bool
		errSig          chan error
		isErrSigClosed  bool
		errLimit        int
		errCount        int

		mtx *sync.Mutex
	}
)

func New(ctx context.Context, cancel context.CancelFunc, opts ...options.Option) Handler {
	h := &handler{
		ctx:           ctx,
		cancelFunc:    cancel,
		osExit:        OsExit,
		serverMapping: ServerMapping{},
		offThread:     concurrent.New("TransportHandler"),
		tracing:       stack_tracer.NewGST(),
		logger:        go_logger.NewGoLogger(ctx, nil, &go_logger.Opts{Debug: true}),
	}
	options.ApplyOptions(h, opts...)

	serverLimit := len(h.serverMapping)
	h.srvChan = &srvChan{
		stopSig: make(chan os.Signal, 1),
		errSig:  make(chan error, serverLimit),

		errLimit: serverLimit,
		mtx:      &sync.Mutex{},
	}
	signal.Notify(h.srvChan.stopSig, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGHUP)

	return h
}

func (h *handler) StartServers() {
	h.start()
	h.handle()
}

func (h *handler) StopServers() {
	h.cancel()
}
