package transporthandler

import (
	"context"
	"gitlab.com/pietroski-software-company/devex/golang/transporthandler/internal/factories"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"
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
			c.serverMapping[factories.DefaultPprofServerNameKey] =
				factories.NewPProfServer(ctx, opts...)
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
