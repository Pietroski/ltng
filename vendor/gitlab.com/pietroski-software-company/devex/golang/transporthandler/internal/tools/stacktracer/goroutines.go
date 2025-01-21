package stacktracer

import (
	"log"
	"runtime"

	"gitlab.com/pietroski-software-company/devex/golang/transporthandler/models"
)

type goroutineStackTracer struct{}

func NewGoroutineStackTracer() models.Tracer {
	tracer := &goroutineStackTracer{}
	return tracer
}

func NewGST() models.Tracer {
	tracer := &goroutineStackTracer{}
	tracer.Trace()
	return tracer
}

func (goroutineStackTracer) Trace() {
	log.Printf("tracer - goroutine count: %v\n", runtime.NumGoroutine())
}
