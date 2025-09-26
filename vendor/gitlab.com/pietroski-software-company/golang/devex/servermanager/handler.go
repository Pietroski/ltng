package servermanager

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"gitlab.com/pietroski-software-company/golang/devex/concurrent"
	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/safe"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
)

type (
	handler struct {
		ctx        context.Context
		cancelFunc context.CancelFunc
		osExit     func(code int)

		serverMapping ServerMapping
		offThread     concurrent.Operator
		logger        slogx.SLogger

		srvChan *srvChan
	}

	srvChan struct {
		ctx    context.Context
		logger slogx.SLogger

		errSig  *safe.Channel[error]
		stopSig *safe.Channel[os.Signal]
	}
)

func New(ctx context.Context, cancel context.CancelFunc, opts ...options.Option) Handler {
	h := &handler{
		ctx:           ctx,
		cancelFunc:    cancel,
		osExit:        OsExit,
		serverMapping: ServerMapping{},
		offThread:     concurrent.New("ServerManager"),
		logger:        slogx.New(),

		srvChan: nil,
	}
	options.ApplyOptions(h, opts...)

	serverLimit := len(h.serverMapping)
	h.srvChan = &srvChan{
		ctx:    ctx,
		logger: h.logger,

		errSig:  safe.NewChannel[error](safe.WithChannelSize[error](serverLimit)),
		stopSig: safe.NewChannel[os.Signal](safe.WithChannelSize[os.Signal](serverLimit)),
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
