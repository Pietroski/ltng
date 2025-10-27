package loop

import (
	"context"
	"runtime"
	"sync"

	"github.com/google/uuid"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
)

var ErrEnded = errorsx.New("context has ended")

const (
	ThreadKey   = "thread"
	ErrLimitKey = "err_limit"
	sep         = "-"

	defaultErrLimit = 10
)

func WithThread(
	ctx context.Context,
	thread string,
) context.Context {
	return context.WithValue(ctx, ThreadKey, thread)
}

func WithErrorLimit(
	ctx context.Context,
	limit uint32,
) context.Context {
	return context.WithValue(ctx, ErrLimitKey, limit)
}

func threadFromCtx(ctx context.Context) string {
	thread, ok := ctx.Value(ThreadKey).(string)
	if !ok || thread == "" {
		newUUID, err := uuid.NewV7()
		if err != nil {
			slogx.New().Error(ctx, "failed to generate uuid for loop thread", "error", err)
		}

		thread = ThreadKey + sep + newUUID.String()
	}

	return thread
}

func handleCancelWithLimit(
	ctx context.Context,
	fn func() error,
) {
	thread := threadFromCtx(ctx)

	errLimit, ok := ctx.Value(ErrLimitKey).(uint32)
	if !ok {
		errLimit = defaultErrLimit
	}

	errCount := uint32(0)
	slogx.New().Trace(ctx, "context is done for loop thread", ThreadKey, thread, "error", ctx.Err())
	for err := fn(); !errorsx.Is(err, ErrEnded); err = fn() {
		errCount++
		if errCount >= errLimit {
			slogx.New().Trace(ctx, "loop thread reached error limit", ThreadKey, thread, "error", err)

			return
		}

		runtime.Gosched()
	}
}

func Run(
	ctx context.Context,
	fn func() error,
) {
	for {
		select {
		case <-ctx.Done():
			handleCancelWithLimit(ctx, fn)

			return
		default:
			_ = fn()
		}
	}
}

func RunUntilDone(
	ctx context.Context,
	fn func() error,
) {
	for {
		select {
		case <-ctx.Done():
			handleCancelWithLimit(ctx, fn)

			return
		default:
			if err := fn(); errorsx.Is(err, ErrEnded) {
				return
			}
		}
	}
}

func RunWithLimit(
	ctx context.Context,
	limit uint32,
	fn func() error,
) {
	var wg sync.WaitGroup
	limiter := make(chan struct{}, limit)

	for {
		limiter <- struct{}{}

		select {
		case <-ctx.Done():
			wg.Wait()
			handleCancelWithLimit(ctx, fn)

			return
		default:
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-limiter }()

				_ = fn()
			}()
		}
	}
}

func RunFromChannel[T any](
	ctx context.Context,
	channel chan T,
	fn func(args T),
) {
	for {
		select {
		case <-ctx.Done():
			thread := threadFromCtx(ctx)
			slogx.New().Trace(ctx, "context is done for loop thread", ThreadKey, thread, "error", ctx.Err())

			for v := range channel {
				fn(v)
			}

			return
		case v, ok := <-channel:
			if !ok {
				thread := threadFromCtx(ctx)
				slogx.New().Trace(ctx, "channel closed for loop thread", ThreadKey, thread)

				return
			}

			fn(v)
		}
	}
}
