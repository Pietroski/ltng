package syncx

import (
	"runtime"
	"sync"
)

type KVLock struct {
	kvTracker *sync.Map
}

func NewKVLock() *KVLock {
	return &KVLock{
		kvTracker: new(sync.Map),
	}
}

func (mtx *KVLock) Lock(key, value any) {
	_, ok := mtx.kvTracker.LoadOrStore(key, value)
	for ok {
		_, ok = mtx.kvTracker.LoadOrStore(key, value)
		runtime.Gosched()
	}
}

func (mtx *KVLock) Unlock(key any) {
	mtx.kvTracker.Delete(key)
}
