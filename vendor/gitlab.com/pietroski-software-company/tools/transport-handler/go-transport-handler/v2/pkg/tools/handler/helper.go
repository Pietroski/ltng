package transporthandler

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	handlers_model "gitlab.com/pietroski-software-company/tools/transport-handler/go-transport-handler/v2/pkg/models/handlers"
	pprof_server "gitlab.com/pietroski-software-company/tools/transport-handler/go-transport-handler/v2/pkg/tools/profiler/pprof-server"
)

func (h *handler) stopServers() {
	for srvName, srv := range h.serverMapping {
		//if srv == nil {
		//	continue
		//}

		h.goPool.wg.Add(1)
		go func(srvName string, srv handlers_model.Server) {
			defer h.handleStopPanic()
			defer h.goPool.wg.Done()

			h.stopCall(srvName, srv)
		}(srvName, srv)
	}
}

func (h *handler) stopCall(srvName string, srv handlers_model.Server) {
	h.logger.Infof(
		"stopping server",
		go_logger.Field{"server_name": srvName},
	)
	srv.Stop()
	h.logger.Infof(
		"stopped server",
		go_logger.Field{"server_name": srvName},
	)
}

func (h *handler) makeSrvChan(srvLen int) {
	srvLen++
	h.srvChan = srvChan{
		stopSig:  makeStopSrvSig(),
		errSig:   makeErrSrvSig(srvLen),
		panicSig: makePanicSrvSig(srvLen),
	}
}

func (h *handler) closeSrvSigChan() {
	defer h.handleCloseChanPanic()

	h.logger.Infof("closing srv sig channel...")
	if !isChanClosed(h.srvChan.stopSig) {
		signal.Stop(h.srvChan.stopSig)
		close(h.srvChan.stopSig)
	}
	h.logger.Infof("srv sig channel successfully closed.")
}

func (h *handler) closeErrChan() {
	defer h.handleCloseChanPanic()

	h.logger.Infof("closing error channel...")
	if !isChanClosed(h.srvChan.errSig) {
		close(h.srvChan.errSig)
	}
	h.logger.Infof("error channel successfully closed.")
}

func (h *handler) closePanicChan() {
	defer h.handleCloseChanPanic()

	h.logger.Infof("closing panic channel...")
	if !isChanClosed(h.srvChan.panicSig) {
		close(h.srvChan.panicSig)
	}
	h.logger.Infof("panic channel successfully closed.")
}

func (h *handler) handleCloseChanPanic() {
	if r := recover(); r != nil {
		h.logger.Warningf(
			"recovering from close channel panic",
			go_logger.Field{"panic": fmt.Sprintf("%v", r)},
		)
		h.goPool.gst.Trace()
		h.osExit(2)
		return
	}
}

func (h *handler) handleWaiting() {
	h.logger.Infof("waiting for goroutines to stop")
	h.goPool.wg.Wait()
	h.logger.Infof("goroutines stopped")
}

func (h *handler) handleErr(err error) {
	h.logger.Errorf(
		"error from server",
		go_logger.Field{"error": err},
	)
	if !isChanClosed(h.srvChan.errSig) {
		h.srvChan.errSig <- err
		h.logger.Errorf(
			"post-err",
			go_logger.Field{"error": err},
		)
	}
}

func (h *handler) handlePanic() {
	if r := recover(); r != nil {
		h.logger.Warningf(
			"recovering from runtime panic",
			go_logger.Field{"panic": fmt.Sprintf("%v", r)},
		)
		if !isChanClosed(h.srvChan.panicSig) {
			h.srvChan.panicSig <- true
			h.logger.Warningf(
				"post-panic",
				go_logger.Field{"panic": fmt.Sprintf("%v", r)},
			)
		}
	}
}

func (h *handler) handleStopPanic() {
	if r := recover(); r != nil {
		h.logger.Warningf(
			"recovering from stopping runtime panic",
			go_logger.Field{"panic": fmt.Sprintf("%v", r)},
		)
	}
}

func (h *handler) sigKill() {
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGQUIT)
}

func handleCtxGen(
	ctx context.Context,
	cancelFn context.CancelFunc,
) (
	context.Context,
	context.CancelFunc,
) {
	if ctx == nil && cancelFn == nil {
		ctx, cancelFn = context.WithCancel(context.Background())
	} else if ctx != nil && cancelFn == nil {
		ctx, cancelFn = context.WithCancel(ctx)
	}

	return ctx, cancelFn
}

func handleLogger(
	ctx context.Context,
	logger go_logger.Logger,
) go_logger.Logger {
	if logger != nil {
		return logger
	}

	return go_logger.NewGoLogger(
		ctx,
		&go_logger.Publishers{},
		&go_logger.Opts{
			Debug:   true,
			Publish: false,
		},
	)
}

func (h *handler) handlePprof(servers ServerMapping) ServerMapping {
	if !h.options.Debug {
		return servers
	}

	const (
		internalPprofServerNameKey = "internal-pprof-server"
		internalPprofServerTimeout = time.Second * 30
	)

	servers[internalPprofServerNameKey] =
		pprof_server.NewPProfServer(
			h.ctx,
			h.logger,
			internalPprofServerNameKey,
			h.setProfilerPort(),
			internalPprofServerTimeout,
		)

	return servers
}

func (h *handler) setProfilerPort() string {
	const internalPprofServerPort = ":7001"

	profilerPort := h.options.ProfilerPort
	if profilerPort == "" {
		profilerPort = internalPprofServerPort
	}

	return profilerPort
}

func (h *handler) stopProfiler() {
	if h.profiler.pprof == nil {
		return
	}

	h.profiler.pprof.Stop()
}

func isChanClosed[T any](ch chan T) bool {
	log.Println("analysing channel")
	select {
	case _, ok := <-ch:
		log.Println("channel is:", ok)
		return !ok
	default:
		log.Println("nothing from the channel")
		return false
	}
}
