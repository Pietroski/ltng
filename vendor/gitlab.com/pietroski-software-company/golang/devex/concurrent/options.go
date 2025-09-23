package concurrent

import "gitlab.com/pietroski-software-company/golang/devex/options"

func WithThreadLimit(threadLimit int) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*OffThread); ok {
			c.limiter = make(chan struct{}, threadLimit)
		}
	}
}

func WithErrChanLimit(threadLimit int) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*OffThread); ok {
			c.errChan = make(chan error, threadLimit)
		}
	}
}

func WithResultChanLimit(threadLimit int) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*OffThread); ok {
			c.resultChan = make(chan any, threadLimit)
		}
	}
}
