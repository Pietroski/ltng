package syncx

import (
	"fmt"
	"math"
	"sync"

	"gitlab.com/pietroski-software-company/golang/devex/options"
)

const (
	defaultLimit       = 20
	defaultMaxChanSize = math.MaxInt16 // math.MaxInt32
)

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
		limiter    *Channel[struct{}]
		errChan    *Channel[error]
		resultChan *Channel[any]
	}
)

func WithThreadLimit(threadLimit int) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*OffThread); ok {
			c.limiter = NewChannel[struct{}](WithChannelSize[struct{}](threadLimit))
		}
	}
}

func WithErrChanLimit(threadLimit int) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*OffThread); ok {
			c.errChan = NewChannel[error](WithChannelSize[error](threadLimit))
		}
	}
}

func WithResultChanLimit(threadLimit int) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*OffThread); ok {
			c.resultChan = NewChannel[any](WithChannelSize[any](threadLimit))
		}
	}
}

func NewThreadOperator(name string, opts ...options.Option) *OffThread {
	op := &OffThread{
		name:       name,
		wg:         &sync.WaitGroup{},
		limiter:    NewChannel[struct{}](WithChannelSize[struct{}](defaultLimit)),
		errChan:    NewChannel[error](WithChannelSize[error](defaultMaxChanSize)),
		resultChan: NewChannel[any](WithChannelSize[any](defaultMaxChanSize)),
	}
	options.ApplyOptions(op, opts...)

	return op
}

func (op *OffThread) Op(fn func()) {
	op.wg.Add(1)
	op.limiter.Send(struct{}{})
	go func() {
		defer func() {
			<-op.limiter.Ch
			op.wg.Done()
		}()
		fn()
	}()
}

func (op *OffThread) OpX(fn func() (any, error)) {
	op.wg.Add(1)
	op.limiter.Send(struct{}{})
	go func() {
		defer func() {
			<-op.limiter.Ch
			op.wg.Done()
		}()
		result, err := fn()
		if err != nil {
			op.errChan.Send(err)
			return
		}

		if result != nil {
			op.resultChan.Send(result)
		}
	}()
}

func (op *OffThread) WaitAndWrapErr() (err error) {
	op.wg.Wait()

	op.limiter.Close()
	op.errChan.Close()
	op.resultChan.Close()

	for v := range op.errChan.Ch {
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
	return op.resultChan.Ch
}

func (op *OffThread) Wait() {
	op.wg.Wait()

	op.limiter.Close()
	op.errChan.Close()
	op.resultChan.Close()
}
