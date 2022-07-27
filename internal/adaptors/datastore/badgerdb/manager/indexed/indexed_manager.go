package indexed_manager

import (
	"context"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/adaptors/datastore/badgerdb/manager"
	management_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/management"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
)

const (
	InternalLocalManagement = ".db/internal/local/management"
	IndexerSuffixPath       = "/indexes"
	IndexerSuffixName       = "-indexed"
)

var (
	ErrUnimplemented = fmt.Errorf("unimplemented method")
)

type (
	IndexerManager interface {
		CreateIndexedStore(
			ctx context.Context,
			info *management_models.DBInfo,
		) error
		DeleteIndexedStore(
			ctx context.Context,
			name string,
		) error

		GetDBInfo(
			ctx context.Context,
			name string,
		) (*management_models.DBInfo, error)
		GetIndexedDBInfo(
			ctx context.Context,
			name string,
		) (*management_models.DBInfo, error)
		GetDBAndIndexedDBInfo(
			ctx context.Context,
			name string,
		) (
			*management_models.DBInfo,
			*management_models.DBInfo,
			error,
		)

		GetDBMemoryInfo(
			ctx context.Context,
			name string,
		) (*management_models.DBMemoryInfo, error)
		GetIndexedDBMemoryInfo(
			ctx context.Context,
			name string,
		) (*management_models.DBMemoryInfo, error)
		GetDBAndIndexedDBMemoryInfo(
			ctx context.Context,
			name string,
		) (
			*management_models.DBMemoryInfo,
			*management_models.DBMemoryInfo,
			error,
		)

		ListStoreInfo(
			ctx context.Context,
			size, page int,
		) ([]*management_models.DBInfo, error)
		ListIndexedStoreInfo(
			ctx context.Context,
			size, page int,
		) ([]*management_models.DBInfo, error)
		ListStoreAndIndexedStoreInfo(
			ctx context.Context,
			size, page int,
		) (
			[]*management_models.DBInfo,
			[]*management_models.DBInfo,
			error,
		)

		ListStoreMemoryInfo(
			ctx context.Context,
			size, page int,
		) ([]*management_models.DBMemoryInfo, error)
		ListIndexedStoreMemoryInfo(
			ctx context.Context,
			size, page int,
		) ([]*management_models.DBMemoryInfo, error)
		ListStoreAndIndexedStoreMemoryInfo(
			ctx context.Context,
			size, page int,
		) (
			[]*management_models.DBMemoryInfo,
			[]*management_models.DBMemoryInfo,
			error,
		)
	}

	BadgerLocalIndexerManager struct {
		serializer go_serializer.Serializer

		db      *badger.DB
		manager manager.Manager
	}
)

func NewBadgerLocalIndexerManager(
	db *badger.DB,
	manager manager.Manager,
	serializer go_serializer.Serializer,
) IndexerManager {
	m := &BadgerLocalIndexerManager{
		serializer: serializer,

		db:      db,
		manager: manager,
	}

	return m
}

// CreateIndexedStore (CreateOpenIndexedStoreAndLoadIntoMemory) checks if the give into key already exists in database,
// if it does not, it opens a db path and stores it in the db manager and loads it into memory for caching.
// Then, it will try to create an index sub-store for the given store.
// This method consists of simple retrial policies which will try to revert in its best action
// the failed indexing store creation.
func (m *BadgerLocalIndexerManager) CreateIndexedStore(
	ctx context.Context,
	info *management_models.DBInfo,
) error {
	err := m.db.Update(func(txn *badger.Txn) error {
		err := m.manager.CreateOpenStoreAndLoadIntoMemory(info)
		if err != nil {
			return fmt.Errorf("failed to create indexed store - err: %v", err)
		}

		indexedInfoPath := info.Path + IndexerSuffixPath
		indexedInfoName := info.Name + IndexerSuffixName
		indexedInfo := management_models.NewDBInfo(indexedInfoName, indexedInfoPath)
		err = m.manager.CreateOpenStoreAndLoadIntoMemory(indexedInfo)
		if err != nil {
			return m.manager.DeleteFromMemoryAndDisk(ctx, info.Name)
		}

		return nil
	})

	return err
}

// DeleteIndexedStore (DeleteFromMemoryAndDisk) first checks the on memory sync map and
// if present, closes the running instance to the given database.
// It will then serialize the given key and search for it in the database,
// if found, it will delete it from the records.
// If the data is corrupted or the opened pointer is not able to be closed,
// it wil return an error.
// Also, if it fails to serialize, it will return an error.
// If by any case it does not find the key or if it fails by any reason to delete
// it will return an error.
func (m *BadgerLocalIndexerManager) DeleteIndexedStore(
	ctx context.Context,
	name string,
) error {
	err := m.db.Update(func(txn *badger.Txn) error {
		indexedName := name + IndexerSuffixName
		err := m.manager.DeleteFromMemoryAndDisk(ctx, indexedName)
		if err != nil {
			return fmt.Errorf("failed to delete indexed store: %v", err)
		}

		err = m.manager.DeleteFromMemoryAndDisk(ctx, indexedName)
		if err != nil {
			return fmt.Errorf("failed to delete store: %v", err)
		}

		return nil
	})

	return err
}

func (m *BadgerLocalIndexerManager) GetDBInfo(
	ctx context.Context,
	name string,
) (*management_models.DBInfo, error) {
	return m.manager.GetDBInfo(ctx, name)
}

func (m *BadgerLocalIndexerManager) GetIndexedDBInfo(
	ctx context.Context,
	name string,
) (*management_models.DBInfo, error) {
	indexedName := name + IndexerSuffixName
	return m.manager.GetDBInfo(ctx, indexedName)
}

func (m *BadgerLocalIndexerManager) GetDBAndIndexedDBInfo(
	ctx context.Context,
	name string,
) (
	*management_models.DBInfo,
	*management_models.DBInfo,
	error,
) {
	dbInfo, err := m.GetDBInfo(ctx, name)
	if err != nil {
		return nil, nil, err
	}

	indexedDBInfo, err := m.GetIndexedDBInfo(ctx, name)
	if err != nil {
		return dbInfo, nil, err
	}

	return dbInfo, indexedDBInfo, nil
}

func (m *BadgerLocalIndexerManager) GetDBMemoryInfo(
	ctx context.Context,
	name string,
) (*management_models.DBMemoryInfo, error) {
	return m.manager.GetDBMemoryInfo(ctx, name)
}

func (m *BadgerLocalIndexerManager) GetIndexedDBMemoryInfo(
	ctx context.Context,
	name string,
) (*management_models.DBMemoryInfo, error) {
	indexedName := name + IndexerSuffixName
	return m.manager.GetDBMemoryInfo(ctx, indexedName)
}

func (m *BadgerLocalIndexerManager) GetDBAndIndexedDBMemoryInfo(
	ctx context.Context,
	name string,
) (
	*management_models.DBMemoryInfo,
	*management_models.DBMemoryInfo,
	error,
) {
	dbMemoryInfo, err := m.GetDBMemoryInfo(ctx, name)
	if err != nil {
		return nil, nil, err
	}

	indexDBMemoryInfo, err := m.GetIndexedDBMemoryInfo(ctx, name)
	if err != nil {
		return dbMemoryInfo, nil, err
	}

	return dbMemoryInfo, indexDBMemoryInfo, nil
}

func (m *BadgerLocalIndexerManager) ListStoreInfo(
	ctx context.Context,
	size, page int,
) ([]*management_models.DBInfo, error) {
	return m.manager.ListStoreInfoFromMemoryOrDisk(ctx, size, page)
}

func (m *BadgerLocalIndexerManager) ListIndexedStoreInfo(
	ctx context.Context,
	size, page int,
) ([]*management_models.DBInfo, error) {
	var indexedDBInfoList []*management_models.DBInfo
	dbInfoList, err := m.ListStoreInfo(ctx, size, page)
	if err != nil {
		return nil, err
	}

	for _, dbInfo := range dbInfoList {
		if dbInfo == nil {
			continue
		}

		if strings.Contains(dbInfo.Name, IndexerSuffixName) {
			indexedDBInfoList = append(indexedDBInfoList, dbInfo)
		}
	}

	return indexedDBInfoList, nil
}

func (m *BadgerLocalIndexerManager) ListStoreAndIndexedStoreInfo(
	ctx context.Context,
	size, page int,
) (
	[]*management_models.DBInfo,
	[]*management_models.DBInfo,
	error,
) {
	dbInfoList, err := m.ListStoreInfo(ctx, size, page)
	if err != nil {
		return nil, nil, err
	}

	indexDBInfoList, err := m.ListIndexedStoreInfo(ctx, size, page)
	if err != nil {
		return dbInfoList, nil, err
	}

	return dbInfoList, indexDBInfoList, nil
}

func (m *BadgerLocalIndexerManager) ListStoreMemoryInfo(
	ctx context.Context,
	size, page int,
) ([]*management_models.DBMemoryInfo, error) {
	return m.manager.ListStoreMemoryInfoFromMemoryOrDisk(ctx, size, page)
}

func (m *BadgerLocalIndexerManager) ListIndexedStoreMemoryInfo(
	ctx context.Context,
	size, page int,
) ([]*management_models.DBMemoryInfo, error) {
	var indexedDBMemoryInfoList []*management_models.DBMemoryInfo
	dbMemoryInfoList, err := m.ListStoreMemoryInfo(ctx, size, page)
	if err != nil {
		return nil, err
	}

	for _, dbMemoryInfo := range dbMemoryInfoList {
		if dbMemoryInfo == nil {
			continue
		}

		if strings.Contains(dbMemoryInfo.Name, IndexerSuffixName) {
			indexedDBMemoryInfoList = append(indexedDBMemoryInfoList, dbMemoryInfo)
		}
	}

	return indexedDBMemoryInfoList, nil
}

func (m *BadgerLocalIndexerManager) ListStoreAndIndexedStoreMemoryInfo(
	ctx context.Context,
	size, page int,
) (
	[]*management_models.DBMemoryInfo,
	[]*management_models.DBMemoryInfo,
	error,
) {
	dbMemoryInfoList, err := m.ListStoreMemoryInfo(ctx, size, page)
	if err != nil {
		return nil, nil, err
	}

	indexDBMemoryInfoList, err := m.ListIndexedStoreMemoryInfo(ctx, size, page)
	if err != nil {
		return dbMemoryInfoList, nil, err
	}

	return dbMemoryInfoList, indexDBMemoryInfoList, nil
}
