package badger_pool

import (
	"github.com/dgraph-io/badger/v3"
)

type (
	StorePath string

	BadgerDBPoolMapping struct {
		StorePath    StorePath
		badgerDriver *badger.DB
	}

	BadgerDBPool map[StorePath]BadgerDBPoolMapping
)

func NewBadgerPool() *BadgerDBPool {
	return &BadgerDBPool{}
}
