package ctxrunner

import (
	"context"
	"log"
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
			log.Printf("context done for %v: %v\n", thread, ctx.Err())
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
