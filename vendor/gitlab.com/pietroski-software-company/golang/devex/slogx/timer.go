package slogx

import (
	"sync/atomic"
	"time"
)

func (l *Slog) setDefaultTimer() {
	l.logLevelTTL = new(atomic.Uint64)
	l.logLevelTTL.Store(86_400) // 86_400 - 24h
	l.hasSetLogLevel = new(atomic.Bool)
	l.hasSetLogLevel.Store(false)
}

func (l *Slog) setLogLevelReset() {
	if l.hasSetLogLevel.CompareAndSwap(false, true) {
		go func() {
			ttl := time.Duration(l.logLevelTTL.Load())
			time.Sleep(time.Second * ttl)

			l.logLevel.Set(l.initialLogLevel)
			l.setDefaultTimer()
		}()
	}
}
