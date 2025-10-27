package pprofx

import (
	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
)

func WithPprofPort(port string) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Server); ok {
			c.pprofServerPort = port
		}
	}
}

func WithPprofLogger(logger slogx.SLogger) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Server); ok {
			c.logger = logger
		}
	}
}
