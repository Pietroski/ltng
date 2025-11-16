package v3

import (
	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
)

func WithLogger(l slogx.SLogger) options.Option {
	return func(i interface{}) {
		o, ok := i.(*LTNGEngine)
		if ok {
			o.logger = l
		}
	}
}
