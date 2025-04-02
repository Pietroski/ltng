package safe

import (
	"sync/atomic"

	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"
)

type TicketStorageLoop[T any] struct {
	*TicketStorage[T]
	nextIndex *atomic.Uint64
}

func WithTicketStorage[T any](ts *TicketStorage[T]) options.Option {
	return func(i interface{}) {
		if tsl, ok := i.(*TicketStorageLoop[T]); ok {
			tsl.TicketStorage = ts
		}
	}
}

func WithTicketStorageSize[T any](size int) options.Option {
	return func(i interface{}) {
		if tsl, ok := i.(*TicketStorageLoop[T]); ok {
			tsl.TicketStorage = NewTicketStorage[T](size)
		}
	}
}

func NewTicketStorageLoop[T any](opts ...options.Option) *TicketStorageLoop[T] {
	tsl := &TicketStorageLoop[T]{
		TicketStorage: NewTicketStorage[T](0),
		nextIndex:     new(atomic.Uint64),
	}
	options.ApplyOptions(tsl, opts...)
	tsl.nextIndex.Store(uint64(0))

	return tsl
}

func (it *TicketStorageLoop[T]) Next() T {
	idx := it.nextIndex.Add(1) - 1
	if idx < it.done.Load() {
		return it.slots[idx]
	}

	it.nextIndex.Store(1)
	return it.slots[0]
}
