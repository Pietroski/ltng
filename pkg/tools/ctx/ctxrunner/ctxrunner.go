package ctxrunner

import (
	"context"

	"gitlab.com/pietroski-software-company/golang/devex/slogx"
)

func WithCancellation[T any](
	ctx context.Context,
	channel chan T,
	fn func(args T),
) {
	thread, ok := ctx.Value("thread").(string)
	if !ok || thread == "" {
		thread = "thread"
	}

	for {
		select {
		case <-ctx.Done():
			slogx.New().Debug(ctx, "context is done", "thread", thread, "error", ctx.Err())

			// TODO: try closing it on the publishing side
			// or use the concurrent lib
			close(channel)
			for v := range channel {
				fn(v)
			}

			return
		case v := <-channel:
			fn(v)
		}
	}
}
