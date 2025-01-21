package transporthandler

import (
	"fmt"
	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"syscall"
)

func (h *handler) start() {
	for serverName, server := range h.serverMapping {
		h.logger.Infof(
			"starting server",
			go_logger.Field{"server_name": serverName},
		)
		h.offThread.Op(func() {
			defer h.handlePanic()
			if err := server.Start(); err != nil {
				h.handleErr(fmt.Errorf("error starting server %s: %w", serverName, err))
			}
		})
		h.logger.Infof(
			"started server",
			go_logger.Field{"server_name": serverName},
		)
	}
}

func (h *handler) handle() {
	for {
		select {
		case stopSig := <-h.srvChan.stopSig:
			h.logger.Warningf("stop server sig!!", go_logger.Field{"signal": stopSig.String()})
			h.shutdown()
			h.osExit(0)
			return
		case err := <-h.srvChan.errSig:
			h.logger.Warningf("err server sig!!", go_logger.Field{"error": err})
			h.sigKill()
			h.shutdown()
			if err != nil {
				h.osExit(1)
			} else {
				h.osExit(0)
			}
			return
		case ctxSig := <-h.ctx.Done():
			h.logger.Warningf("ctx server sig!!",
				go_logger.Field{"context": ctxSig},
				go_logger.Field{"error": h.ctx.Err()})

			h.srvChan.handleErr(nil)
			//case <-time.After(time.Millisecond * 100):
		}
	}
}

func (h *handler) stop() {
	for srvName, srv := range h.serverMapping {
		h.logger.Infof(
			"stopping server",
			go_logger.Field{"server_name": srvName},
		)
		if srv != nil {
			srv.Stop()
		}
		h.logger.Infof(
			"stopped server",
			go_logger.Field{"server_name": srvName},
		)
	}
}

func (h *handler) shutdown() {
	h.cancel()
	h.sigKill()

	h.logger.Infof("shutting down servers")
	h.stop()
	h.logger.Infof("waiting for servers to shut down")
	h.wait()
	h.logger.Infof("all servers shut down")

	h.logger.Infof("closing channels")
	h.closeChannels()
	h.logger.Infof("all channels closed")

	h.tracing.Trace()
}

func (h *handler) cancel() {
	h.logger.Infof("cancelling all contexts")
	h.cancelFunc()
	h.logger.Infof("all contexts cancelled")
}

func (h *handler) sigKill() {
	h.logger.Infof("sending SIGKILL")
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGQUIT)
	h.logger.Infof("SIGKILL sent")
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

func (h *handler) wait() {
	h.logger.Infof("waiting for goroutines to stop")
	h.offThread.Wait()
	h.logger.Infof("goroutines stopped")
	h.tracing.Trace()
}

func (h *handler) closeChannels() {
	defer h.handlePanic()

	//h.logger.Infof("closing srv sig channel...")
	//h.srvChan.closeStopSigChannel()
	// h.srvChan.cleanStopSigChannel()
	//h.logger.Infof("srv sig channel successfully closed")

	h.logger.Infof("closing error channel...")
	h.srvChan.closeErrChannel()
	h.logger.Infof("error channel successfully closed")
	h.logger.Infof("cleaning error channel...")
	h.srvChan.cleanErrorChannel()
	h.logger.Infof("error channel successfully cleaned")
}

// panic handling

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

// srvChan methods

func (s *srvChan) handleErr(err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.isErrSigClosed || s.errCount == s.errLimit {
		fmt.Printf("error print because error channel is already closed: %v\n", err)
		return
	}

	s.errCount++
	s.errSig <- err
}

func (s *srvChan) closeErrChannel() {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.isErrSigClosed {
		return
	}

	close(s.errSig)
	s.isErrSigClosed = true
}

func (s *srvChan) cleanErrorChannel() {
	fmt.Println("cleaning error channel")
	for err := range s.errSig {
		fmt.Printf("error from cleaning error channel: %v\n", err)
	}
}

func (s *srvChan) closeStopSigChannel() {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.isStopSigClosed {
		// TODO: log sig
		return
	}

	close(s.stopSig)
	s.isStopSigClosed = true
}

func (s *srvChan) cleanStopSigChannel() {
	for _ = range s.stopSig {
		// TODO: log sig
	}
}
