package syncx

import (
	"runtime"
	"sync/atomic"

	"gitlab.com/pietroski-software-company/golang/devex/options"
)

type Channel[T any] struct {
	lock     *atomic.Bool
	isClosed *atomic.Bool
	Ch       chan T
}

func WithChannelSize[T any](size int) options.Option {
	return func(i interface{}) {
		if c, ok := i.(*Channel[T]); ok {
			c.Ch = make(chan T, size)
		}
	}
}

func NewChannel[T any](opts ...options.Option) *Channel[T] {
	channel := &Channel[T]{
		isClosed: &atomic.Bool{},
		lock:     &atomic.Bool{},
		Ch:       make(chan T),
	}
	options.ApplyOptions(channel, opts...)

	return channel
}

func (ch *Channel[T]) Send(v T) (sent bool) {
	if ch.isClosed.Load() {
		return false
	}

	ch.Ch <- v
	return true
}

func (ch *Channel[T]) IsClosed() bool {
	return ch.isClosed.Load()
}

func (ch *Channel[T]) Close() {
	if ch.isClosed.Load() {
		return
	}

	for !ch.lock.CompareAndSwap(false, true) {
		runtime.Gosched()
	}
	// Release the lock before returning
	defer ch.lock.Store(false)

	if ch.isClosed.Load() {
		return
	}

	ch.isClosed.Store(true)
	close(ch.Ch)
}
