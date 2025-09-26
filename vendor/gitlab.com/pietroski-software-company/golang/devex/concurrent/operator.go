package concurrent

import (
	"fmt"
	"math"
	"sync"

	"gitlab.com/pietroski-software-company/golang/devex/options"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate -o ./fakes . Operator

const (
	defaultLimit       = 20
	defaultMaxChanSize = math.MaxInt16 // math.MaxInt32
)

type ListType interface {
	[]any
}

type (
	Operator interface {
		Op(fn func())
		OpX(fn func() (any, error))

		WaitAndWrapErr() (err error)
		Collect() chan any
		Wait()
	}

	OffThread struct {
		name       string
		wg         *sync.WaitGroup
		limiter    chan struct{}
		errChan    chan error
		resultChan chan any
	}
)

func New(name string, opts ...options.Option) *OffThread {
	op := &OffThread{
		name:       name,
		wg:         &sync.WaitGroup{},
		limiter:    make(chan struct{}, defaultLimit),
		errChan:    make(chan error, defaultMaxChanSize),
		resultChan: make(chan any, defaultMaxChanSize),
	}
	options.ApplyOptions(op, opts...)

	return op
}

func (op *OffThread) Op(fn func()) {
	op.wg.Add(1)
	op.limiter <- struct{}{}
	go func() {
		defer func() {
			<-op.limiter
			op.wg.Done()
		}()
		fn()
	}()
}

func (op *OffThread) OpX(fn func() (any, error)) {
	op.wg.Add(1)
	op.limiter <- struct{}{}
	go func() {
		defer func() {
			<-op.limiter
			op.wg.Done()
		}()
		result, err := fn()
		if err != nil {
			op.errChan <- err
			return
		}

		if result != nil {
			op.resultChan <- result
		}
	}()
}

func (op *OffThread) WaitAndWrapErr() (err error) {
	op.wg.Wait()

	close(op.limiter)
	close(op.errChan)
	close(op.resultChan)

	for v := range op.errChan {
		if v != nil {
			if err == nil {
				err = v
				continue
			}

			err = fmt.Errorf("%v: %w", err, v)
		}
	}

	return
}

func (op *OffThread) Collect() chan any {
	return op.resultChan
}

func (op *OffThread) Wait() {
	op.wg.Wait()

	close(op.limiter)
	close(op.errChan)
	close(op.resultChan)
}
