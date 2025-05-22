package ltngqueue_engine

import (
	"time"

	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"
)

func WithTimeout(duration time.Duration) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Queue); ok {
			c.awaitTimeout = duration
		}
	}
}

func WithRetryCountLimit(amount uint64) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Queue); ok {
			c.retryCountLimit = amount
		}
	}
}
