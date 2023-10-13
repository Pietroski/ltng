package manager

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/dgraph-io/badger/v3"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"

	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	co "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator"
)

const (
	InternalLocalManagement = ".db/internal/local/management"

	IndexedSuffixPath = "/indexed"
	IndexedSuffixName = "-indexed"

	IndexedListSuffixPath = "/indexed-list"
	IndexedListSuffixName = "-indexed-list"

	SoftDeleteSuffixPath = "/soft-delete"
	SoftDeleteSuffixName = "-soft-delete"

	SoftDeleteListSuffixPath = "/soft-delete-list"
	SoftDeleteListSuffixName = "-soft-delete-list"

	SoftDeleteCounterSuffixPath = "/soft-delete-counter"
	SoftDeleteCounterSuffixName = "-soft-delete-counter"

	ErrCorruptedStoreDataCastFailure = "corrupted stored memory - failed to cast into DBMemInfo model"
	ErrKeyNotFound                   = "key not found"
)

var (
	ErrUnimplemented = fmt.Errorf("unimplemented method")
)

type (
	Manager interface {
		CreateStore(
			ctx context.Context,
			info *management_models.DBInfo,
		) error
		DeleteStore(
			ctx context.Context,
			name string,
		) error

		GetDBInfo(
			ctx context.Context,
			name string,
		) (*management_models.DBInfo, error)
		GetDBMemoryInfo(
			ctx context.Context,
			name string,
		) (*management_models.DBMemoryInfo, error)

		ListStoreInfo(
			ctx context.Context,
			size, page int,
		) ([]*management_models.DBInfo, error)
		ListStoreMemoryInfo(
			ctx context.Context,
			size, page int,
		) ([]*management_models.DBMemoryInfo, error)

		ValidatePagination(size, page int) (bool, error)
		Paginate(
			it *badger.Iterator,
			size, page int,
		) *badger.Iterator

		Start() error
		Shutdown()
		ShutdownStores()
		Restart() error
		// MigrateStore
		// BackupStore
		// (Purge|Truncate)Store
	}

	BadgerLocalManager struct {
		db *badger.DB
		//TODO: remove logger and instead user error wrapper for returning
		logger        go_logger.Logger
		serializer    go_serializer.Serializer
		badgerMapping *sync.Map

		chainedOperator co.ChainOperator
	}
)

func NewBadgerLocalManager(
	db *badger.DB,
	serializer go_serializer.Serializer,
	logger go_logger.Logger,
) Manager {
	m := &BadgerLocalManager{
		db: db,

		serializer:    serializer,
		logger:        logger,
		badgerMapping: &sync.Map{},
	}

	return m
}

// CreateStore creates a new store.
func (m *BadgerLocalManager) CreateStore(
	ctx context.Context,
	info *management_models.DBInfo,
) error {
	var createStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(info)
	}

	indexedInfoPath := info.Path + IndexedSuffixPath
	indexedInfoName := info.Name + IndexedSuffixName
	indexedInfo := management_models.NewDBInfo(indexedInfoName, indexedInfoPath)
	var createIndexedStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(indexedInfo)
	}
	var deleteStore = func() error {
		return m.deleteFromMemoryAndDisk(ctx, info.Name)
	}

	indexedListInfoPath := info.Path + IndexedListSuffixPath
	indexedListInfoName := info.Name + IndexedListSuffixName
	indexedListInfo := management_models.NewDBInfo(indexedListInfoName, indexedListInfoPath)
	var createIndexedListStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(indexedListInfo)
	}
	var deleteIndexedStore = func() error {
		return m.deleteFromMemoryAndDisk(ctx, indexedInfoName)
	}

	softDeleteInfoPath := info.Path + SoftDeleteSuffixPath
	softDeleteInfoName := info.Name + SoftDeleteSuffixName
	softDeleteInfo := management_models.NewDBInfo(softDeleteInfoName, softDeleteInfoPath)
	var createSoftDeleteStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(softDeleteInfo)
	}
	var deleteIndexedListStore = func() error {
		return m.deleteFromMemoryAndDisk(ctx, indexedListInfoName)
	}

	softDeleteListInfoPath := info.Path + SoftDeleteListSuffixPath
	softDeleteListInfoName := info.Name + SoftDeleteListSuffixName
	softDeleteListInfo := management_models.NewDBInfo(softDeleteListInfoName, softDeleteListInfoPath)
	var createSoftDeleteListStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(softDeleteListInfo)
	}
	var deleteSoftDeleteStore = func() error {
		return m.deleteFromMemoryAndDisk(ctx, softDeleteInfoName)
	}

	softDeleteCounterInfoPath := info.Path + SoftDeleteCounterSuffixPath
	softDeleteCounterInfoName := info.Name + SoftDeleteCounterSuffixName
	softDeleteCounterInfo := management_models.NewDBInfo(softDeleteCounterInfoName, softDeleteCounterInfoPath)
	var createSoftDeleteCounterStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(softDeleteCounterInfo)
	}
	var deleteSoftDeleteListStore = func() error {
		return m.deleteFromMemoryAndDisk(ctx, softDeleteListInfoName)
	}

	createSoftDeleteCounterOps := &co.Ops{
		Action: &co.Action{
			Act:         createSoftDeleteCounterStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        nil,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: deleteSoftDeleteListStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        nil,
		},
	}

	createSoftDeleteListOps := &co.Ops{
		Action: &co.Action{
			Act:         createSoftDeleteListStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        createSoftDeleteCounterOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: deleteSoftDeleteStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        nil,
		},
	}
	createSoftDeleteCounterOps.RollbackAction.Next = createSoftDeleteListOps

	createSoftDeleteOps := &co.Ops{
		Action: &co.Action{
			Act:         createSoftDeleteStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        createSoftDeleteListOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: deleteIndexedListStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        nil,
		},
	}
	createSoftDeleteListOps.RollbackAction.Next = createSoftDeleteOps

	createIndexedListOps := &co.Ops{
		Action: &co.Action{
			Act:         createIndexedListStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        createSoftDeleteOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: deleteIndexedStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        nil,
		},
	}
	createSoftDeleteOps.RollbackAction.Next = createIndexedListOps

	createIndexedOps := &co.Ops{
		Action: &co.Action{
			Act:         createIndexedStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        createIndexedListOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: deleteStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        nil,
		},
	}
	createIndexedListOps.RollbackAction.Next = createIndexedOps

	createOps := &co.Ops{
		Action: &co.Action{
			Act:         createStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        createIndexedOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: nil,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        nil,
		},
	}
	createIndexedOps.RollbackAction.Next = createOps

	err := m.chainedOperator.Operate(createOps)

	return err
}

// DeleteStore creates a new store.
func (m *BadgerLocalManager) DeleteStore(
	ctx context.Context,
	name string,
) error {
	var deleteStore = func() error {
		return m.deleteFromMemoryAndDisk(ctx, name)
	}

	info, err := m.getStoreInfoFromMemoryOrFromDisk(ctx, name)
	if err != nil {
		return err
	}

	indexedInfoPath := info.Path + IndexedSuffixPath
	indexedInfoName := info.Name + IndexedSuffixName
	indexedInfo := management_models.NewDBInfo(indexedInfoName, indexedInfoPath)
	var deleteIndexedStore = func() error {
		return m.deleteFromMemoryAndDisk(ctx, indexedInfoName)
	}
	var createStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(info)
	}

	indexedListInfoPath := info.Path + IndexedListSuffixPath
	indexedListInfoName := info.Name + IndexedListSuffixName
	indexedListInfo := management_models.NewDBInfo(indexedListInfoName, indexedListInfoPath)
	var deleteIndexedListStore = func() error {
		return m.deleteFromMemoryAndDisk(ctx, indexedListInfoName)
	}
	var createIndexedStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(indexedInfo)
	}

	softDeleteInfoPath := info.Path + SoftDeleteSuffixPath
	softDeleteInfoName := info.Name + SoftDeleteSuffixName
	softDeleteInfo := management_models.NewDBInfo(softDeleteInfoName, softDeleteInfoPath)
	var deleteSoftDeleteStore = func() error {
		return m.deleteFromMemoryAndDisk(ctx, softDeleteInfoName)
	}
	var createIndexedListStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(indexedListInfo)
	}

	softDeleteListInfoPath := info.Path + SoftDeleteListSuffixPath
	softDeleteListInfoName := info.Name + SoftDeleteListSuffixName
	softDeleteListInfo := management_models.NewDBInfo(softDeleteListInfoName, softDeleteListInfoPath)
	var deleteSoftDeleteListStore = func() error {
		return m.deleteFromMemoryAndDisk(ctx, softDeleteListInfoName)
	}
	var createSoftDeleteStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(softDeleteInfo)
	}

	//softDeleteCounterInfoPath := info.Path + SoftDeleteCounterSuffixPath
	softDeleteCounterInfoName := info.Name + SoftDeleteCounterSuffixName
	//softDeleteCounterInfo := management_models.NewDBInfo(softDeleteCounterInfoName, softDeleteCounterInfoPath)
	var deleteSoftDeleteCounterStore = func() error {
		return m.deleteFromMemoryAndDisk(ctx, softDeleteCounterInfoName)
	}
	var createSoftDeleteListStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(softDeleteListInfo)
	}

	deleteSoftDeleteCounterStoreOps := &co.Ops{
		Action: &co.Action{
			Act:         deleteSoftDeleteCounterStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        nil,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: createSoftDeleteListStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        nil,
		},
	}

	deleteSoftDeleteListStoreOps := &co.Ops{
		Action: &co.Action{
			Act:         deleteSoftDeleteListStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        deleteSoftDeleteCounterStoreOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: createSoftDeleteStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        nil,
		},
	}
	deleteSoftDeleteCounterStoreOps.RollbackAction.Next = deleteSoftDeleteListStoreOps

	deleteSoftDeleteStoreOps := &co.Ops{
		Action: &co.Action{
			Act:         deleteSoftDeleteStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        deleteSoftDeleteListStoreOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: createIndexedListStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        nil,
		},
	}
	deleteSoftDeleteListStoreOps.RollbackAction.Next = deleteSoftDeleteStoreOps

	deleteIndexedListStoreOps := &co.Ops{
		Action: &co.Action{
			Act:         deleteIndexedListStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        deleteSoftDeleteStoreOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: createIndexedStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        nil,
		},
	}
	deleteSoftDeleteStoreOps.RollbackAction.Next = deleteIndexedListStoreOps

	deleteIndexedStoreOps := &co.Ops{
		Action: &co.Action{
			Act:         deleteIndexedStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        deleteIndexedListStoreOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: createStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        nil,
		},
	}
	deleteIndexedListStoreOps.RollbackAction.Next = deleteIndexedStoreOps

	deleteStoreOps := &co.Ops{
		Action: &co.Action{
			Act:         deleteStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        deleteIndexedStoreOps,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: nil,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        nil,
		},
	}
	deleteIndexedStoreOps.RollbackAction.Next = deleteStoreOps

	err = m.chainedOperator.Operate(deleteStoreOps)

	return err
}

func (m *BadgerLocalManager) GetDBInfo(
	ctx context.Context,
	name string,
) (info *management_models.DBInfo, err error) {
	return m.getStoreInfoFromMemoryOrFromDisk(ctx, name)
}

func (m *BadgerLocalManager) GetDBMemoryInfo(
	ctx context.Context,
	name string,
) (info *management_models.DBMemoryInfo, err error) {
	return m.getStoreMemoryInfoFromMemoryOrDisk(ctx, name)
}

func (m *BadgerLocalManager) ListStoreInfo(
	ctx context.Context,
	size, page int,
) ([]*management_models.DBInfo, error) {
	if ok, err := m.ValidatePagination(size, page); !ok {
		if err != nil {
			return []*management_models.DBInfo{}, err
		}

		return m.listAllStoresInfoFromMemoryOrDisk(ctx)
	}

	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = size

	memoryInfoList := make([]*management_models.DBInfo, size)
	err := m.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()
		it = m.Paginate(it, size, page)

		for idx := 0; idx < size; idx++ {
			if !it.Valid() {
				break
			}

			item := it.Item()

			_, value, err := m.deserializeItem(item)
			if err != nil {
				log.Printf("failed to deserialize item: %v", err)
				continue
			}

			memoryInfoList[idx] = &value

			it.Next()
		}

		return nil
	})
	if err != nil {
		return memoryInfoList, fmt.Errorf("failed to obtain memory info list - err: %v", err)
	}

	return memoryInfoList, nil
}

func (m *BadgerLocalManager) ListStoreMemoryInfo(
	ctx context.Context,
	size, page int,
) ([]*management_models.DBMemoryInfo, error) {
	if ok, err := m.ValidatePagination(size, page); !ok {
		if err != nil {
			return []*management_models.DBMemoryInfo{}, err
		}

		return m.listAllStoresMemoryInfoFromMemoryOrDisk(ctx)
	}

	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = size * page

	memoryInfoList := make([]*management_models.DBMemoryInfo, size)
	err := m.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()
		it = m.Paginate(it, size, page)

		for idx := 0; idx < size; idx++ {
			if !it.Valid() {
				break
			}

			item := it.Item()

			key, value, err := m.deserializeItem(item)
			if err != nil {
				log.Printf("failed to deserialize item: %v", err)
				continue
			}

			if memoryInfoItem, err := m.getStoreFromMemory(key); err == nil {
				memoryInfoList[idx] = memoryInfoItem

				continue
			}

			db, err := m.openLoadAndReturn(&value)
			if err != nil {
				log.Printf("failed to open and load item - %v: %v", key, err)
				continue
			}
			memoryInfoItem := value.InfoToMemoryInfo(db)
			memoryInfoList[idx] = memoryInfoItem

			it.Next()
		}

		return nil
	})
	if err != nil {
		return memoryInfoList, fmt.Errorf("failed to obtain memory info list - err: %v", err)
	}

	return memoryInfoList, nil
}

func (m *BadgerLocalManager) listAllStoresInfoFromMemoryOrDisk(
	ctx context.Context,
) ([]*management_models.DBInfo, error) {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 50

	var memoryInfoList []*management_models.DBInfo
	err := m.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			_, value, err := m.deserializeItem(item)
			if err != nil {
				log.Printf("failed to deserialize item: %v", err)
				continue
			}

			memoryInfoList = append(memoryInfoList, &value)
		}

		return nil
	})
	if err != nil {
		return memoryInfoList, fmt.Errorf("failed to obtain memory info list - err: %v", err)
	}

	return memoryInfoList, nil
}

func (m *BadgerLocalManager) listAllStoresMemoryInfoFromMemoryOrDisk(
	ctx context.Context,
) ([]*management_models.DBMemoryInfo, error) {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 50

	var memoryInfoList []*management_models.DBMemoryInfo
	err := m.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			key, value, err := m.deserializeItem(item)
			if err != nil {
				log.Printf("failed to deserialize item: %v", err)
				continue
			}

			if memoryInfoItem, err := m.getStoreFromMemory(key); err == nil {
				memoryInfoList = append(memoryInfoList, memoryInfoItem)

				continue
			}

			db, err := m.openLoadAndReturn(&value)
			if err != nil {
				log.Printf("failed to open and load item - %v: %v", key, err)
				continue
			}
			memoryInfoItem := value.InfoToMemoryInfo(db)
			memoryInfoList = append(memoryInfoList, memoryInfoItem)
		}

		return nil
	})
	if err != nil {
		return memoryInfoList, fmt.Errorf("failed to obtain memory info list - err: %v", err)
	}

	return memoryInfoList, nil
}
