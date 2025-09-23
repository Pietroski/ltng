package servermanager

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"syscall"
)

func (h *handler) start() {
	h.logger.Debug(h.ctx, "server list", "name_list", h.serverMapping.NameList())

	for serverName, server := range h.serverMapping {
		h.logger.Debug(h.ctx, "starting server", "server_name", serverName)
		h.offThread.Op(func() {
			defer h.handlePanic()
			if err := server.Start(); err != nil {
				h.handleErr(fmt.Errorf("error starting server %s: %w", serverName, err))
			}
		})
		h.logger.Debug(h.ctx, "started server", "server_name", serverName)
	}

	h.logger.Info(h.ctx, "servers spawn up", "server_list", h.serverMapping.NameList())
}

func (h *handler) handle() {
	select {
	case stopSig := <-h.srvChan.stopSig.Ch:
		h.logger.Info(h.ctx, "stop server signal", "signal", stopSig.String())
		h.shutdown()
		h.osExit(0)

	case err := <-h.srvChan.errSig.Ch:
		h.logger.Error(h.ctx, "error stop server signal", "error", err)
		h.shutdown()
		h.osExit(1)

	case <-h.ctx.Done():
		h.logger.Info(h.ctx, "ctx stop server signal", "context", h.ctx.Err())
		h.osExit(0)
	}
}

func (h *handler) stop() {
	for srvName, srv := range h.serverMapping {
		h.logger.Debug(h.ctx, "stopping server", "server_name", srvName)
		if srv != nil {
			srv.Stop()
		}
		h.logger.Debug(h.ctx, "stopped server", "server_name", srvName)
	}
}

func (h *handler) shutdown() {
	h.cancel()
	h.sigKill()

	h.logger.Debug(h.ctx, "shutting down servers")
	h.stop()
	h.logger.Debug(h.ctx, "waiting for servers to shut down")
	h.wait()
	h.logger.Debug(h.ctx, "all servers shut down")

	h.logger.Debug(h.ctx, "closing channels")
	h.closeChannels()
	h.logger.Debug(h.ctx, "all channels closed")

	h.logger.Info(h.ctx, "server manager shut down complete: all resources shut down")
}

func (h *handler) cancel() {
	h.logger.Debug(h.ctx, "cancelling all contexts")
	h.cancelFunc()
	h.logger.Debug(h.ctx, "all contexts cancelled")
}

func (h *handler) sigKill() {
	h.logger.Debug(h.ctx, "sending SIGKILL")
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGQUIT)
	h.logger.Debug(h.ctx, "SIGQUIT sent")
}

func (h *handler) handleErr(err error) {
	defer h.handlePanic()

	h.logger.Error(h.ctx, "error from server", "error", err)
	h.srvChan.handleErr(err)
	h.logger.Debug(h.ctx, "post error handling", "error", err)
}

func (h *handler) wait() {
	h.logger.Debug(h.ctx, "waiting for goroutines to stop")
	h.offThread.Wait()
	h.logger.Debug(h.ctx, "all goroutines stopped")

	h.logger.Debug(h.ctx, "goroutine count", "count", runtime.NumGoroutine())
}

func (h *handler) closeChannels() {
	defer h.handlePanic()

	h.logger.Debug(h.ctx, "closing error channel")
	h.srvChan.closeErrChannel()
	h.logger.Debug(h.ctx, "error channel successfully closed")
	h.logger.Debug(h.ctx, "cleaning error channel")
	h.srvChan.cleanErrorChannel()
	h.logger.Debug(h.ctx, "error channel successfully cleaned")
}

// panic handling

func (h *handler) handlePanic() {
	if r := recover(); r != nil {
		h.logger.Warn(h.ctx, "recovering from runtime panic",
			"panic", r, "stack", string(debug.Stack()))

		h.srvChan.handleErr(fmt.Errorf("recovering from runtime panic: %v", r))

		h.logger.Warn(h.ctx, "recovered from runtime panic",
			"panic", r, "stack", string(debug.Stack()))
	}
}

// srvChan methods

func (s *srvChan) handleErr(err error) {
	if s.errSig.IsClosed() {
		s.logger.Debug(s.ctx, "error channel already closed, nothing to send")
		return
	}

	s.errSig.Send(err)
}

func (s *srvChan) closeErrChannel() {
	if s.errSig.IsClosed() {
		return
	}

	s.errSig.Close()
}

func (s *srvChan) cleanErrorChannel() {
	s.logger.Debug(s.ctx, "cleaning error channel")
	for err := range s.errSig.Ch {
		s.logger.Error(s.ctx, "error from error channel cleaning", "error", err)
	}
}

func (s *srvChan) closeStopSigChannel() {
	if s.stopSig.IsClosed() {
		return
	}

	s.stopSig.Close()
}

func (s *srvChan) cleanStopSigChannel() {
	s.logger.Debug(s.ctx, "cleaning signal channel")
	for signal := range s.stopSig.Ch {
		s.logger.Debug(s.ctx, "signal from signal channel cleaning", "signal", signal.String())
	}
}
