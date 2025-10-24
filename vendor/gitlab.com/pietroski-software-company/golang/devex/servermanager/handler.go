package servermanager

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"
)

type (
	handler struct {
		ctx        context.Context
		cancelFunc context.CancelFunc
		osExit     func(code int)

		serverMapping ServerMapping
		offThread     syncx.Operator
		logger        slogx.SLogger

		srvChan *srvChan
	}

	srvChan struct {
		ctx    context.Context
		logger slogx.SLogger

		errSig  *syncx.Channel[error]
		stopSig *syncx.Channel[os.Signal]
	}
)

func New(ctx context.Context, cancel context.CancelFunc, opts ...options.Option) Handler {
	h := &handler{
		ctx:           ctx,
		cancelFunc:    cancel,
		osExit:        OsExit,
		serverMapping: ServerMapping{},
		offThread:     syncx.NewThreadOperator("ServerManager"),
		logger:        slogx.New(),

		srvChan: nil,
	}
	options.ApplyOptions(h, opts...)

	serverLimit := len(h.serverMapping)
	h.srvChan = &srvChan{
		ctx:    ctx,
		logger: h.logger,

		errSig:  syncx.NewChannel[error](syncx.WithChannelSize[error](serverLimit)),
		stopSig: syncx.NewChannel[os.Signal](syncx.WithChannelSize[os.Signal](serverLimit)),
	}

	signal.Notify(h.srvChan.stopSig.Ch, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGHUP)

	return h
}

func (h *handler) StartServers() {
	h.start()
	h.handle()
}

func (h *handler) StopServers() {
	h.cancel()
}
