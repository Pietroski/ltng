package loop

import (
	"time"

	"gitlab.com/pietroski-software-company/golang/devex/options"
)

type BackoffType int

const (
	Linear BackoffType = iota
	Exponential
)

type (
	Retrier interface {
		Do(func() error) error
	}

	Retry struct {
		maxRetryCount  int
		initialBackoff time.Duration
		backoffFactor  time.Duration
		backoffType    BackoffType
	}
)

func WithMaxRetryCount(maxRetryCount int) options.Option {
	return func(i interface{}) {
		if r, ok := i.(*Retry); ok {
			r.maxRetryCount = maxRetryCount
		}
	}
}

func WithInitialBackoff(duration time.Duration) options.Option {
	return func(i interface{}) {
		if r, ok := i.(*Retry); ok {
			r.initialBackoff = duration
		}
	}
}

func WithBackoffFactor(backoffFactor time.Duration) options.Option {
	return func(i interface{}) {
		if r, ok := i.(*Retry); ok {
			r.backoffFactor = backoffFactor
		}
	}
}

func WithBackoffType(backoffType BackoffType) options.Option {
	return func(i interface{}) {
		if r, ok := i.(*Retry); ok {
			r.backoffType = backoffType
		}
	}
}

func WithLinearBackoff() options.Option {
	return func(i interface{}) {
		if r, ok := i.(*Retry); ok {
			r.backoffType = Linear
		}
	}
}

func WithExponentialBackoff() options.Option {
	return func(i interface{}) {
		if r, ok := i.(*Retry); ok {
			r.backoffType = Exponential
		}
	}
}

func New(opts ...options.Option) *Retry {
	r := &Retry{
		maxRetryCount:  2,
		initialBackoff: time.Second,
		backoffFactor:  2,
	}
	options.ApplyOptions(r, opts...)

	return r
}

// DoRetry retries calling a callback function for n amount of time with exponential backoff.
func (rwb *Retry) DoRetry(fn func() error) error {
	count := 1
	wait := rwb.initialBackoff

	err := fn()
	for err != nil && count <= rwb.maxRetryCount {
		time.Sleep(wait)

		count++
		err = fn()
		if err != nil {
			switch rwb.backoffType {
			case Exponential:
				wait *= rwb.backoffFactor
			case Linear:
				fallthrough
			default:
				wait += rwb.backoffFactor
			}
		}
	}

	return err
}
