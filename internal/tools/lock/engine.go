package lock

import (
	"runtime"
	"sync"
)

type EngineLock struct {
	opTracker *sync.Map
}

func (mtx *EngineLock) Lock(key, value any) {
	_, ok := mtx.opTracker.LoadOrStore(key, value)
	if !ok {
		_, ok = mtx.opTracker.LoadOrStore(key, value)
		runtime.Gosched()
	}
}

func (mtx *EngineLock) Unlock(key any) {
	mtx.opTracker.Delete(key)
}
