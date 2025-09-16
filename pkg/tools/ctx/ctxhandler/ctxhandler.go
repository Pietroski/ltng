package ctxhandler

import (
	"context"
	"errors"
	"log"
	"runtime"
)

var ErrEnded = errors.New("context has ended")

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
			for err := fn(); !errors.Is(err, ErrEnded); err = fn() {
				runtime.Gosched()
			}

			return
		default:
			_ = fn()
		}
	}
}

func WithCancelLimit(
	ctx context.Context,
	limit uint32,
	fn func() error,
) {
	thread, ok := ctx.Value("thread").(string)
	if !ok || thread == "" {
		thread = "thread"
	}

	limiter := make(chan struct{}, limit)
	for {
		limiter <- struct{}{}

		select {
		case <-ctx.Done():
			log.Printf("context done for %v: %v\n", thread, ctx.Err())
			for err := fn(); !errors.Is(err, ErrEnded); err = fn() {
				runtime.Gosched()
			}
			<-limiter

			return
		default:
			go func() {
				_ = fn()
				<-limiter
			}()
		}
	}
}

func WithCancelOrDone(
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
			for err := fn(); !errors.Is(err, ErrEnded); err = fn() {
				runtime.Gosched()
			}

			return
		default:
			if err := fn(); errors.Is(err, ErrEnded) {
				return
			}
			//go func() {
			//	if err := fn(); errors.Is(err, ErrEnded) {
			//		return
			//	}
			//}()
		}
	}
}
