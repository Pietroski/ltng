package transporthandler

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"

	pprof_server "gitlab.com/pietroski-software-company/tools/transport-handler/go-transport-handler/v2/pkg/tools/profiler/pprof-server"
)

func (h *handler) stopServers() {
	for srvName, srv := range h.serverMapping {
		h.logger.Infof(
			"stopping server",
			go_logger.Field{"server_name": srvName},
		)
		if srv != nil {
			srv.Stop()
		}
		h.serverCount.Add(1)
		h.logger.Infof(
			"stopped server",
			go_logger.Field{"server_name": srvName},
		)
	}
}

func (h *handler) makeSrvChan(srvLen int) {
	//srvLen++
	h.srvChan = srvChan{
		stopSig: make(chan os.Signal),
		errSig:  make(chan error, srvLen),

		mtx: &sync.Mutex{},
	}
}

func (h *handler) closeSrvSigChan() {
	//defer h.handleCloseChanPanic()

	h.logger.Infof("closing srv sig channel...")
	h.srvChan.closeStopSigChannel()
	h.srvChan.cleanStopSigChannel()
	h.logger.Infof("srv sig channel successfully closed.")
}

func (h *handler) closeErrChan() {
	//defer h.handleCloseChanPanic()

	h.logger.Infof("closing error channel...")
	h.srvChan.closeErrChannel()
	h.srvChan.cleanErrorChannel()
	h.logger.Infof("error channel successfully closed.")
}

func (h *handler) handleCloseChanPanic() {
	if r := recover(); r != nil {
		h.logger.Warningf(
			"recovering from close channel panic",
			go_logger.Field{"panic": fmt.Sprintf("%v", r)},
		)

		h.tracing.Trace()
	}
}

func (h *handler) handleWaiting() {
	h.logger.Infof("waiting for goroutines to stop")
	for h.serverCount.Load() != uint32(len(h.serverMapping)) {
		runtime.Gosched()
	}
	h.offThread.Wait()
	h.logger.Infof("goroutines stopped")
	h.tracing.Trace()
}

func (h *handler) handleErr(err error) {
	defer h.handlePanic()

	h.logger.Errorf(
		"error from server",
		go_logger.Field{"error": err},
	)
	h.srvChan.handleErr(err)
	h.logger.Errorf(
		"post-err",
		go_logger.Field{"error": err},
	)
}

func (h *handler) handlePanic() {
	if r := recover(); r != nil {
		h.logger.Warningf("recovering from runtime panic",
			go_logger.Field{"panic": fmt.Sprintf("%v", r)},
		)
		h.srvChan.handleErr(fmt.Errorf("recovering from runtime panic: %v", r))
		h.logger.Warningf("recovered from runtime panic",
			go_logger.Field{"panic": fmt.Sprintf("%v", r)},
		)
	}
}

func (h *handler) handleStartPanic() {
	if r := recover(); r != nil {
		h.logger.Warningf("recovering from runtime panic",
			go_logger.Field{"panic": fmt.Sprintf("%v", r)},
		)
		h.srvChan.handleErr(fmt.Errorf("recovering from runtime panic: %v", r))
		h.logger.Warningf("recovered from runtime panic",
			go_logger.Field{"panic": fmt.Sprintf("%v", r)},
		)
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

// profiling

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

// srvChan methods

func (s *srvChan) handleErr(err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.isErrSigClosed {
		return
	}

	s.errSig <- err
}

func (s *srvChan) closeErrChannel() {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.isErrSigClosed {
		return
	}

	close(s.errSig)
}

func (s *srvChan) cleanErrorChannel() {
	for _ = range s.errSig {
		//
	}
}

func (s *srvChan) closeStopSigChannel() {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.isStopSigClosed {
		return
	}

	close(s.stopSig)
}

func (s *srvChan) cleanStopSigChannel() {
	for _ = range s.stopSig {
		//
	}
}
