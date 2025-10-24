package syncx

import (
	"runtime"
	"sync/atomic"
)

type (
	ThreadLocker interface {
		Lock()
		Unlock()
	}

	SpinLock struct {
		state  *int64
		locker int64
	}
)

const (
	free = int64(0)
)

func NewThreadLocker() ThreadLocker {
	state := int64(0)
	return &SpinLock{
		state:  &state,
		locker: 42,
	}
}

func (sl *SpinLock) Lock() {
	for !atomic.CompareAndSwapInt64(sl.state, free, sl.locker) {
		runtime.Gosched()
	}
}

func (sl *SpinLock) Unlock() {
	atomic.StoreInt64(sl.state, free)
}
