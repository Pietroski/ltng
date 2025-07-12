package ctxhandler

import (
	"context"
	"io"
	"log"
	"runtime"
)

func WithCancellation(
	ctx context.Context,
	fn func() error,
) {
	thread, ok := ctx.Value("thread").(string)
	if !ok || thread == "" {
		thread = "thread"
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("context done for %v: %v\n", thread, ctx.Err())
			for err := fn(); err != io.EOF; err = fn() {
				runtime.Gosched()
			}

			return
		default:
			_ = fn()
		}
	}
}
