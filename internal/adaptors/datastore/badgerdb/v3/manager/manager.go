package badgerdb_manager_adaptor_v3

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/dgraph-io/badger/v3"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"

	badgerdb_management_models_v3 "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/badgerdb/v3/management"
	co "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/chained-operator"
)

const (
	InternalLocalManagement        = ".db/internal/local/management/" + InternalLocalManagementVersion
	InternalLocalManagementVersion = "v3"

	IndexedSuffixPath = "/indexed"
	IndexedSuffixName = "-indexed"

	IndexedListSuffixPath = "/indexed-list"
	IndexedListSuffixName = "-indexed-list"

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
			info *badgerdb_management_models_v3.DBInfo,
		) error
		DeleteStore(
			ctx context.Context,
			name string,
		) error

		GetDBInfo(
			ctx context.Context,
			name string,
		) (*badgerdb_management_models_v3.DBInfo, error)
		GetDBMemoryInfo(
			ctx context.Context,
			name string,
		) (*badgerdb_management_models_v3.DBMemoryInfo, error)

		ListStoreInfo(
			ctx context.Context,
			size, page int,
		) ([]*badgerdb_management_models_v3.DBInfo, error)
		ListStoreMemoryInfo(
			ctx context.Context,
			size, page int,
		) ([]*badgerdb_management_models_v3.DBMemoryInfo, error)

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

	BadgerLocalManagerV3Params struct {
		DB         *badger.DB
		Logger     go_logger.Logger
		Serializer go_serializer.Serializer
	}

	BadgerLocalManagerV3 struct {
		db *badger.DB
		//TODO: remove logger and instead user error wrapper for returning
		logger        go_logger.Logger
		serializer    go_serializer.Serializer
		badgerMapping *sync.Map

		chainedOperator co.ChainOperator
	}
)

func NewBadgerLocalManagerV3(
	params *BadgerLocalManagerV3Params,
) (*BadgerLocalManagerV3, error) {
	m := &BadgerLocalManagerV3{
		db: params.DB,

		serializer:    params.Serializer,
		logger:        params.Logger,
		badgerMapping: &sync.Map{},
	}

	return m, nil
}

// CreateStore creates a new store.
func (m *BadgerLocalManagerV3) CreateStore(
	ctx context.Context,
	info *badgerdb_management_models_v3.DBInfo,
) error {
	var createStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(info)
	}

	indexedInfoPath := info.Path + IndexedSuffixPath
	indexedInfoName := info.Name + IndexedSuffixName
	indexedInfo := badgerdb_management_models_v3.NewDBInfo(indexedInfoName, indexedInfoPath)
	var createIndexedStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(indexedInfo)
	}
	var deleteStore = func() error {
		return m.deleteFromMemoryAndDisk(ctx, info.Name)
	}

	indexedListInfoPath := info.Path + IndexedListSuffixPath
	indexedListInfoName := info.Name + IndexedListSuffixName
	indexedListInfo := badgerdb_management_models_v3.NewDBInfo(indexedListInfoName, indexedListInfoPath)
	var createIndexedListStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(indexedListInfo)
	}
	var deleteIndexedStore = func() error {
		return m.deleteFromMemoryAndDisk(ctx, indexedInfoName)
	}

	createIndexedListOps := &co.Ops{
		Action: &co.Action{
			Act:         createIndexedListStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        nil,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: deleteIndexedStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        nil,
		},
	}

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
func (m *BadgerLocalManagerV3) DeleteStore(
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
	var createStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(info)
	}

	indexedInfoPath := info.Path + IndexedSuffixPath
	indexedInfoName := info.Name + IndexedSuffixName
	indexedInfo := badgerdb_management_models_v3.NewDBInfo(indexedInfoName, indexedInfoPath)
	var deleteIndexedStore = func() error {
		return m.deleteFromMemoryAndDisk(ctx, indexedInfoName)
	}
	var createIndexedStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(indexedInfo)
	}

	indexedListInfoName := info.Name + IndexedListSuffixName
	var deleteIndexedListStore = func() error {
		return m.deleteFromMemoryAndDisk(ctx, indexedListInfoName)
	}

	deleteIndexedListStoreOps := &co.Ops{
		Action: &co.Action{
			Act:         deleteIndexedListStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        nil,
		},
		RollbackAction: &co.RollbackAction{
			RollbackAct: createIndexedStore,
			RetrialOpts: co.DefaultRetrialOps,
			Next:        nil,
		},
	}

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

func (m *BadgerLocalManagerV3) GetDBInfo(
	ctx context.Context,
	name string,
) (info *badgerdb_management_models_v3.DBInfo, err error) {
	return m.getStoreInfoFromMemoryOrFromDisk(ctx, name)
}

func (m *BadgerLocalManagerV3) GetDBMemoryInfo(
	ctx context.Context,
	name string,
) (info *badgerdb_management_models_v3.DBMemoryInfo, err error) {
	return m.getStoreMemoryInfoFromMemoryOrDisk(ctx, name)
}

func (m *BadgerLocalManagerV3) ListStoreInfo(
	ctx context.Context,
	size, page int,
) ([]*badgerdb_management_models_v3.DBInfo, error) {
	if ok, err := m.ValidatePagination(size, page); !ok {
		if err != nil {
			return []*badgerdb_management_models_v3.DBInfo{}, err
		}

		return m.listAllStoresInfoFromMemoryOrDisk(ctx)
	}

	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = size

	memoryInfoList := make([]*badgerdb_management_models_v3.DBInfo, size)
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

func (m *BadgerLocalManagerV3) ListStoreMemoryInfo(
	ctx context.Context,
	size, page int,
) ([]*badgerdb_management_models_v3.DBMemoryInfo, error) {
	if ok, err := m.ValidatePagination(size, page); !ok {
		if err != nil {
			return []*badgerdb_management_models_v3.DBMemoryInfo{}, err
		}

		return m.listAllStoresMemoryInfoFromMemoryOrDisk(ctx)
	}

	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = size * page

	memoryInfoList := make([]*badgerdb_management_models_v3.DBMemoryInfo, size)
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

func (m *BadgerLocalManagerV3) listAllStoresInfoFromMemoryOrDisk(
	_ context.Context,
) ([]*badgerdb_management_models_v3.DBInfo, error) {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 50

	var memoryInfoList []*badgerdb_management_models_v3.DBInfo
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

func (m *BadgerLocalManagerV3) listAllStoresMemoryInfoFromMemoryOrDisk(
	_ context.Context,
) ([]*badgerdb_management_models_v3.DBMemoryInfo, error) {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 50

	var memoryInfoList []*badgerdb_management_models_v3.DBMemoryInfo
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

func (m *BadgerLocalManagerV3) Backup(_ context.Context, filePath string, backupSince uint64) error {
	buffer := bytes.NewBuffer([]byte{})
	_, err := m.db.Backup(buffer, backupSince)
	if err != nil {
		return fmt.Errorf("failed to backup: %v", err)
	}

	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %v", err)
	}
	defer func() {
		_ = f.Close()
	}()

	_, err = f.Write(buffer.Bytes())
	if err != nil {
		return fmt.Errorf("failed to write backup file: %v", err)
	}

	return nil
}

func (m *BadgerLocalManagerV3) RestoreBackup(_ context.Context, filePath string, maxPendingWrites int) error {
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %v", err)
	}
	defer func() {
		_ = f.Close()
	}()

	err = m.db.Load(f, maxPendingWrites)
	if err != nil {
		return fmt.Errorf("failed to restore backup: %v", err)
	}

	return nil
}
