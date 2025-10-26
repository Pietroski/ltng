package process

import (
	"context"
	"sync"

	"gitlab.com/pietroski-software-company/golang/devex/syncx"
)

const (
	initialPidStrMapSize = 1 << 8
	generalLockKey       = "general"
	counterLockKey       = "counter"
)

type Register struct {
	kvLock        *syncx.KVLock
	mtx           *sync.Mutex
	pidStrMapping map[string]struct{}
	pidCounter    uint64
}

func New(ctx context.Context) *Register {
	return &Register{
		kvLock:        syncx.NewKVLock(),
		mtx:           &sync.Mutex{},
		pidStrMapping: make(map[string]struct{}, initialPidStrMapSize),
	}
}

func (reg *Register) Register(pidStr string) {
	//reg.kvLock.Lock(pidStr, struct{}{})
	//defer reg.kvLock.Unlock(pidStr)
	reg.mtx.Lock()
	defer reg.mtx.Unlock()
	reg.pidStrMapping[pidStr] = struct{}{}
}

func (reg *Register) UnRegister(pidStr string) {
	//reg.kvLock.Lock(pidStr, struct{}{})
	//defer reg.kvLock.Unlock(pidStr)
	reg.mtx.Lock()
	defer reg.mtx.Unlock()
	delete(reg.pidStrMapping, pidStr)
}

func (reg *Register) PidCount() int {
	//reg.kvLock.Lock(generalLockKey, struct{}{})
	//defer reg.kvLock.Unlock(generalLockKey)
	reg.mtx.Lock()
	defer reg.mtx.Unlock()
	return len(reg.pidStrMapping)
}

func (reg *Register) Count() {
	reg.kvLock.Lock(counterLockKey, struct{}{})
	defer reg.kvLock.Unlock(counterLockKey)
	reg.pidCounter++
}

func (reg *Register) CountEnd() {
	reg.kvLock.Lock(counterLockKey, struct{}{})
	defer reg.kvLock.Unlock(counterLockKey)
	reg.pidCounter--
}

func (reg *Register) CountNumber() uint64 {
	reg.kvLock.Lock(counterLockKey, struct{}{})
	defer reg.kvLock.Unlock(counterLockKey)
	return reg.pidCounter
}

// TODO: generate a pid and store that
// add the pid to the struct itemInfoData so it can be able to check its later process later
