package servermanager

import (
	"context"

	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/servermanager/internal/pprof"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
)

func WithContextCancellation(ctx context.Context, cancel context.CancelFunc) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*handler); ok {
			if ctx == nil && cancel == nil {
				ctx, cancel = context.WithCancel(context.Background())
			} else if ctx != nil && cancel == nil {
				ctx, cancel = context.WithCancel(ctx)
			}

			c.ctx = ctx
			c.cancelFunc = cancel
		}
	}
}

func WithPprofServer(ctx context.Context, opts ...options.Option) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*handler); ok {
			c.serverMapping[pprof.DefaultPprofServerNameKey] =
				pprof.NewPProfServer(ctx, opts...)
		}
	}
}

func WithCustomProfilingServer(servername string, server Server) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*handler); ok {
			c.serverMapping[servername] = server
		}
	}
}

func WithServers(servers ServerMapping) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*handler); ok {
			// in order to avoid to rewrite WithPprofServer,
			// traverse the full mapping and re-assign it to
			// the existing server mapping.
			for k, v := range servers {
				c.serverMapping[k] = v
			}
		}
	}
}

func WithExiter(exiter func(int)) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*handler); ok {
			c.osExit = exiter
		}
	}
}

func WithLogger(logger slogx.SLogger) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*handler); ok {
			c.logger = logger
		}
	}
}
