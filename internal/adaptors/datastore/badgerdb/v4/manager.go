package v4

import (
	"bytes"
	"context"
	"os"
	"sync"

	"github.com/dgraph-io/badger/v4"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/serializer"
	serializer_models "gitlab.com/pietroski-software-company/golang/devex/serializer/models"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"

	badgerdb_management_models_v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/badgerdb/v4/management"
	lo "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/list-operator"
)

const (
	InternalLocalManagement        = ".db/internal/local/management/" + InternalLocalManagementVersion
	InternalLocalManagementVersion = "v4"

	IndexedSuffixPath = "/indexed"
	IndexedSuffixName = "-indexed"

	IndexedListSuffixPath = "/indexed-list"
	IndexedListSuffixName = "-indexed-list"

	ErrCorruptedStoreDataCastFailure = "corrupted stored memory - failed to cast into DBMemInfo model"
	ErrKeyNotFound                   = "key not found"
)

var (
	ErrUnimplemented = errorsx.New("unimplemented method")
)

type (
	Manager interface {
		CreateStore(
			ctx context.Context,
			info *badgerdb_management_models_v4.DBInfo,
		) error
		DeleteStore(
			ctx context.Context,
			name string,
		) error

		GetDBInfo(
			ctx context.Context,
			name string,
		) (*badgerdb_management_models_v4.DBInfo, error)
		GetDBMemoryInfo(
			ctx context.Context,
			name string,
		) (*badgerdb_management_models_v4.DBMemoryInfo, error)

		ListStoreInfo(
			ctx context.Context,
			size, page int,
		) ([]*badgerdb_management_models_v4.DBInfo, error)
		ListStoreMemoryInfo(
			ctx context.Context,
			size, page int,
		) ([]*badgerdb_management_models_v4.DBMemoryInfo, error)

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

	BadgerLocalManagerV4 struct {
		ctx           context.Context
		db            *badger.DB
		logger        slogx.SLogger
		serializer    serializer_models.Serializer
		badgerMapping *sync.Map
		reqMapping    *sync.Map
		mtx           *sync.Mutex
	}
)

func NewBadgerLocalManagerV4(
	ctx context.Context, opts ...options.Option,
) (*BadgerLocalManagerV4, error) {
	m := &BadgerLocalManagerV4{
		ctx:        ctx,
		serializer: serializer.NewJsonSerializer(),
		logger:     slogx.New(),

		badgerMapping: &sync.Map{},
		reqMapping:    &sync.Map{},
		mtx:           &sync.Mutex{},
	}
	options.ApplyOptions(m, opts...)

	return m, nil
}

// CreateStore creates a new store.
func (m *BadgerLocalManagerV4) CreateStore(
	ctx context.Context,
	info *badgerdb_management_models_v4.DBInfo,
) error {
	var createStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(info)
	}

	indexedInfoPath := info.Path + IndexedSuffixPath
	indexedInfoName := info.Name + IndexedSuffixName
	indexedInfo := badgerdb_management_models_v4.NewDBInfo(indexedInfoName, indexedInfoPath)
	var createIndexedStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(indexedInfo)
	}
	var deleteStore = func() error {
		return m.deleteFromMemoryAndDisk(ctx, info.Name)
	}

	indexedListInfoPath := info.Path + IndexedListSuffixPath
	indexedListInfoName := info.Name + IndexedListSuffixName
	indexedListInfo := badgerdb_management_models_v4.NewDBInfo(indexedListInfoName, indexedListInfoPath)
	var createIndexedListStore = func() error {
		return m.createOpenStoreAndLoadIntoMemory(indexedListInfo)
	}
	var deleteIndexedStore = func() error {
		return m.deleteFromMemoryAndDisk(ctx, indexedInfoName)
	}

	operations := []*lo.Operation{
		{
			Action: &lo.Action{
				Act:         createStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         createIndexedStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: deleteStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         createIndexedListStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: deleteIndexedStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
	}

	if err := lo.New(operations...).Operate(); err != nil {
		return err
	}

	return nil
}

// DeleteStore creates a new store.
func (m *BadgerLocalManagerV4) DeleteStore(
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
	indexedInfo := badgerdb_management_models_v4.NewDBInfo(indexedInfoName, indexedInfoPath)
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

	operations := []*lo.Operation{
		{
			Action: &lo.Action{
				Act:         deleteStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         deleteIndexedStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: createStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         deleteIndexedListStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: createIndexedStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
	}

	if err = lo.New(operations...).Operate(); err != nil {
		return err
	}

	return nil
}

func (m *BadgerLocalManagerV4) GetDBInfo(
	ctx context.Context,
	name string,
) (info *badgerdb_management_models_v4.DBInfo, err error) {
	return m.getStoreInfoFromMemoryOrFromDisk(ctx, name)
}

func (m *BadgerLocalManagerV4) GetDBMemoryInfo(
	ctx context.Context,
	name string,
) (info *badgerdb_management_models_v4.DBMemoryInfo, err error) {
	return m.getStoreMemoryInfoFromMemoryOrDisk(ctx, name)
}

func (m *BadgerLocalManagerV4) ListStoreInfo(
	ctx context.Context,
	size, page int,
) ([]*badgerdb_management_models_v4.DBInfo, error) {
	if ok, err := m.ValidatePagination(size, page); !ok {
		if err != nil {
			return []*badgerdb_management_models_v4.DBInfo{}, err
		}

		return m.listAllStoresInfoFromMemoryOrDisk(ctx)
	}

	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = size

	memoryInfoList := make([]*badgerdb_management_models_v4.DBInfo, size)
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
				m.logger.Error(ctx, "failed to deserialize item", "err", err)
				continue
			}

			memoryInfoList[idx] = &value

			it.Next()
		}

		return nil
	})
	if err != nil {
		return memoryInfoList, errorsx.Wrap(err, "failed to obtain memory info list")
	}

	return memoryInfoList, nil
}

func (m *BadgerLocalManagerV4) ListStoreMemoryInfo(
	ctx context.Context,
	size, page int,
) ([]*badgerdb_management_models_v4.DBMemoryInfo, error) {
	if ok, err := m.ValidatePagination(size, page); !ok {
		if err != nil {
			return []*badgerdb_management_models_v4.DBMemoryInfo{}, err
		}

		return m.listAllStoresMemoryInfoFromMemoryOrDisk(ctx)
	}

	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = size * page

	memoryInfoList := make([]*badgerdb_management_models_v4.DBMemoryInfo, size)
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
				m.logger.Error(ctx, "failed to deserialize item", "err", err)
				continue
			}

			if memoryInfoItem, err := m.getStoreFromMemory(key); err == nil {
				memoryInfoList[idx] = memoryInfoItem

				continue
			}

			db, err := m.openLoadAndReturn(&value)
			if err != nil {
				m.logger.Error(ctx, "failed to open and load item", "key", key, "err", err)
				continue
			}
			memoryInfoItem := value.InfoToMemoryInfo(db)
			memoryInfoList[idx] = memoryInfoItem

			it.Next()
		}

		return nil
	})
	if err != nil {
		return memoryInfoList, errorsx.Wrap(err, "failed to obtain memory info list")
	}

	return memoryInfoList, nil
}

func (m *BadgerLocalManagerV4) listAllStoresInfoFromMemoryOrDisk(
	ctx context.Context,
) ([]*badgerdb_management_models_v4.DBInfo, error) {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 50

	var memoryInfoList []*badgerdb_management_models_v4.DBInfo
	err := m.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			_, value, err := m.deserializeItem(item)
			if err != nil {
				m.logger.Error(ctx, "failed to deserialize item", "err", err)
				continue
			}

			memoryInfoList = append(memoryInfoList, &value)
		}

		return nil
	})
	if err != nil {
		return memoryInfoList, errorsx.Wrap(err, "failed to obtain memory info list")
	}

	return memoryInfoList, nil
}

func (m *BadgerLocalManagerV4) listAllStoresMemoryInfoFromMemoryOrDisk(
	ctx context.Context,
) ([]*badgerdb_management_models_v4.DBMemoryInfo, error) {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 50

	var memoryInfoList []*badgerdb_management_models_v4.DBMemoryInfo
	err := m.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			key, value, err := m.deserializeItem(item)
			if err != nil {
				m.logger.Error(ctx, "failed to deserialize item", "err", err)
				continue
			}

			if memoryInfoItem, err := m.getStoreFromMemory(key); err == nil {
				memoryInfoList = append(memoryInfoList, memoryInfoItem)

				continue
			}

			db, err := m.openLoadAndReturn(&value)
			if err != nil {
				m.logger.Error(ctx, "failed to open and load item", "key", key, "err", err)
				continue
			}
			memoryInfoItem := value.InfoToMemoryInfo(db)
			memoryInfoList = append(memoryInfoList, memoryInfoItem)
		}

		return nil
	})
	if err != nil {
		return memoryInfoList, errorsx.Wrap(err, "failed to obtain memory info list")
	}

	return memoryInfoList, nil
}

func (m *BadgerLocalManagerV4) Backup(_ context.Context, filePath string, backupSince uint64) error {
	buffer := bytes.NewBuffer([]byte{})
	_, err := m.db.Backup(buffer, backupSince)
	if err != nil {
		return errorsx.Wrap(err, "failed to backup")
	}

	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return errorsx.Wrap(err, "failed to create backup file")
	}
	defer func() {
		_ = f.Close()
	}()

	_, err = f.Write(buffer.Bytes())
	if err != nil {
		return errorsx.Wrap(err, "failed to write backup file")
	}

	return nil
}

func (m *BadgerLocalManagerV4) RestoreBackup(_ context.Context, filePath string, maxPendingWrites int) error {
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_RDONLY, 0644)
	if err != nil {
		return errorsx.Wrap(err, "failed to open backup file")
	}
	defer func() {
		_ = f.Close()
	}()

	err = m.db.Load(f, maxPendingWrites)
	if err != nil {
		return errorsx.Wrap(err, "failed to restore backup")
	}

	return nil
}
