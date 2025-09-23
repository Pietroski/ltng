package safe

import (
	"runtime"
	"sync/atomic"
)

type TicketStorage[T any] struct {
	ticket    *atomic.Uint64
	done      *atomic.Uint64
	appending *atomic.Uint64
	slots     []T
}

func NewTicketStorage[T any](size int) *TicketStorage[T] { // , capacity int
	return &TicketStorage[T]{
		ticket:    new(atomic.Uint64),
		done:      new(atomic.Uint64),
		appending: new(atomic.Uint64),
		slots:     make([]T, size),
	}
}

func (ts *TicketStorage[T]) Put(item T) {
	t := ts.ticket.Add(1) - 1
	ts.slots[t] = item
	for !ts.done.CompareAndSwap(t, t+1) {
		runtime.Gosched()
	}
}

func (ts *TicketStorage[T]) Append(item T) {
	for !ts.appending.CompareAndSwap(0, 1) {
		runtime.Gosched()
	}

	ts.slots = append(ts.slots, item)
	ts.done.Add(1)
	ts.appending.Store(0)
}

func (ts *TicketStorage[T]) Get() []T {
	// TODO: evaluate whether it is necessary
	for !ts.appending.CompareAndSwap(0, 1) {
		runtime.Gosched()
	}
	defer ts.appending.Store(0)

	return ts.slots[:ts.done.Load()]
}

func (ts *TicketStorage[T]) Slice(i, j int) []T {
	for !ts.appending.CompareAndSwap(0, 1) {
		runtime.Gosched()
	}

	ts.slots = ts.slots[i:j]
	ts.done.Store(uint64(len(ts.slots)))
	ts.appending.Store(0)

	return ts.slots
}

func (ts *TicketStorage[T]) FindAndDelete(fn func(item T) bool) {
	for !ts.appending.CompareAndSwap(0, 1) {
		runtime.Gosched()
	}

	for idx, item := range ts.slots {
		if fn(item) {
			if idx == len(ts.slots)-1 {
				ts.slots = ts.slots[:idx]
			} else {
				ts.slots = append(ts.slots[:idx], ts.slots[idx+1:]...)
			}

			//fmt.Println(item, idx, ts.slots)
			break
		}
	}

	ts.done.Store(ts.done.Load() - 1)
	ts.appending.Store(0)
}
