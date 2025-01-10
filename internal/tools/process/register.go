package process

import (
	"context"
	"sync"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/lock"
)

const (
	initialPidStrMapSize = 1 << 8
	generalLockKey       = "general"
	counterLockKey       = "counter"
)

type Register struct {
	opLock        *lock.EngineLock
	mtx           *sync.Mutex
	pidStrMapping map[string]struct{}
	pidCounter    uint64
}

func New(ctx context.Context) *Register {
	return &Register{
		opLock:        lock.NewEngineLock(),
		mtx:           &sync.Mutex{},
		pidStrMapping: make(map[string]struct{}, initialPidStrMapSize),
	}
}

func (reg *Register) Register(pidStr string) {
	//reg.opLock.Lock(pidStr, struct{}{})
	//defer reg.opLock.Unlock(pidStr)
	reg.mtx.Lock()
	defer reg.mtx.Unlock()
	reg.pidStrMapping[pidStr] = struct{}{}
}

func (reg *Register) UnRegister(pidStr string) {
	//reg.opLock.Lock(pidStr, struct{}{})
	//defer reg.opLock.Unlock(pidStr)
	reg.mtx.Lock()
	defer reg.mtx.Unlock()
	delete(reg.pidStrMapping, pidStr)
}

func (reg *Register) PidCount() int {
	//reg.opLock.Lock(generalLockKey, struct{}{})
	//defer reg.opLock.Unlock(generalLockKey)
	reg.mtx.Lock()
	defer reg.mtx.Unlock()
	return len(reg.pidStrMapping)
}

func (reg *Register) Count() {
	reg.opLock.Lock(counterLockKey, struct{}{})
	defer reg.opLock.Unlock(counterLockKey)
	reg.pidCounter++
}

func (reg *Register) CountEnd() {
	reg.opLock.Lock(counterLockKey, struct{}{})
	defer reg.opLock.Unlock(counterLockKey)
	reg.pidCounter--
}

func (reg *Register) CountNumber() uint64 {
	reg.opLock.Lock(counterLockKey, struct{}{})
	defer reg.opLock.Unlock(counterLockKey)
	return reg.pidCounter
}

// TODO: generate a pid and store that
// add the pid to the struct itemInfoData so it can be able to check its later process later
