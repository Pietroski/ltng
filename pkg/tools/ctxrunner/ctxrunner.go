package ctxrunner

import (
	"context"
	"log"
)

func RunWithCancellation[T any](
	ctx context.Context,
	channel chan T,
	fn func(args T),
) {
	for {
		select {
		case <-ctx.Done():
			// TODO: add func name param
			log.Printf("context done: %v\n", ctx.Err())
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
