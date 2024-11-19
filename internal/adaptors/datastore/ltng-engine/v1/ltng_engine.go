package v1

import (
	"context"
	"fmt"
	ltng_engine_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/ltng-engine/v1"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"
	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"

	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/tools/lock"
	lo "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/list-operator"
)

type (
	LTNGEngine struct {
		opMtx            *lock.EngineLock
		serializer       go_serializer.Serializer
		fileStoreMapping map[string]*fileInfo
	}

	fileInfo struct {
		DBInfo *DBInfo
		File   *os.File
	}

	DBInfo struct {
		Name         string `json:"name,omitempty"`
		Path         string `json:"path,omitempty"`
		CreatedAt    int64  `json:"createdAt"`
		LastOpenedAt int64  `json:"lastOpenedAt"`
	}
)

func (dbInfo *DBInfo) IndexInfo() *DBInfo {
	return &DBInfo{
		Name:         dbInfo.Name + suffixSep + dbIndexStoreSuffixName,
		Path:         dbInfo.Path + dbIndexStoreSuffixPath,
		CreatedAt:    dbInfo.CreatedAt,
		LastOpenedAt: dbInfo.LastOpenedAt,
	}
}

func (dbInfo *DBInfo) IndexListInfo() *DBInfo {
	return &DBInfo{
		Name:         dbInfo.Name + suffixSep + dbIndexListStoreSuffixName,
		Path:         dbInfo.Path + dbIndexListStoreSuffixPath,
		CreatedAt:    dbInfo.CreatedAt,
		LastOpenedAt: dbInfo.LastOpenedAt,
	}
}

func (dbInfo *DBInfo) RelationalInfo() *DBInfo {
	return &DBInfo{
		Name:         dbInfo.Name + suffixSep + dbRelationalName,
		Path:         dbInfo.Path + dbRelationalPath,
		CreatedAt:    dbInfo.CreatedAt,
		LastOpenedAt: dbInfo.LastOpenedAt,
	}
}

func New(opts ...options.Option) *LTNGEngine {
	engine := &LTNGEngine{
		opMtx:            new(lock.EngineLock),
		fileStoreMapping: make(map[string]*fileInfo),
		serializer:       go_serializer.NewRawBinarySerializer(),
	}
	options.ApplyOptions(engine, opts...)

	return engine
}

const (
	dbBasePath    = ".db"
	dbBaseVersion = "/v1"
	dbStoreList   = "/stores"
	dbSep         = "/"
	dbTmpDeletion = "/del-tmp"
	dbFileExt     = ".bin"
	dbFileExtSP   = "*" + dbFileExt
	everything    = "*"

	basePath = dbBasePath + dbBaseVersion + dbStoreList

	suffixSep                  = "-"
	dbIndexStoreSuffixName     = "indexed"
	dbIndexStoreSuffixPath     = "/indexed"
	dbIndexListStoreSuffixName = "indexed-list"
	dbIndexListStoreSuffixPath = "/indexed-list"
	dbRelationalName           = "relational"
	dbRelationalPath           = "/relational"

	dbManagerName = "ltng-engine-manager"
	dbManagerPath = "internal/ltng-engine/manager"

	dbFilePerm = 0750
	dbFileOp   = 0644
	dbFileRead = 0444
)

var (
	dbManagerInfo = &DBInfo{
		Name: dbManagerName,
		Path: dbManagerPath,
	}
)

func (e *LTNGEngine) Close() {
	for k, v := range e.fileStoreMapping {
		e.opMtx.Lock(k, v.DBInfo)
		if err := v.File.Close(); err != nil {
			log.Printf("error closing file from %s store: %v\n", k, err)
		}
		delete(e.fileStoreMapping, k)
		e.opMtx.Unlock(k)
	}
}

func (e *LTNGEngine) openManager(ctx context.Context) (*DBInfo, error) {
	e.opMtx.Lock(dbManagerName, dbManagerInfo)
	defer e.opMtx.Unlock(dbManagerName)

	loadedInfo, err := e.loadStore(ctx, dbManagerInfo)
	if err == nil && loadedInfo != nil {
		return loadedInfo, nil
	}

	return e.createStoreOnDisk(ctx, dbManagerInfo)
}

func (e *LTNGEngine) createStoreOnDisk(
	_ context.Context,
	info *DBInfo,
) (*DBInfo, error) {
	if err := os.MkdirAll(dbBasePath+dbBaseVersion+dbSep+info.Path, dbFilePerm); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(
		dbBasePath+dbBaseVersion+dbStoreList+
			dbSep+info.Path+dbSep+info.Name+dbFileExt,
		os.O_CREATE|os.O_RDWR, dbFileOp,
	)
	if err != nil {
		return nil, err
	}

	info.CreatedAt = time.Now().UTC().Unix()
	info.LastOpenedAt = info.CreatedAt

	bs, err := e.serializer.Serialize(info)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize db manager info - %s | err: %v", info.Name, err)
	}

	_, err = f.Write(bs)
	if err != nil {
		return nil, fmt.Errorf("failed to write db manager info - %s | err: %v", info.Name, err)
	}

	e.fileStoreMapping[info.Name] = &fileInfo{
		DBInfo: info,
		File:   f,
	}

	return info, nil
}

func (e *LTNGEngine) loadStore(
	ctx context.Context,
	info *DBInfo,
) (*DBInfo, error) {
	e.opMtx.Lock(info.Name, info)
	defer e.opMtx.Unlock(info.Name)

	value, ok := e.fileStoreMapping[info.Name]
	if !ok {
		var store DBInfo
		loadStore := func() error {
			storeInfo, err := e.loadStoreFromDisk(ctx, info)
			if err != nil {
				return fmt.Errorf("failed to load %s store info from disk: %w", info.Name, err)
			}

			store = *storeInfo

			return nil
		}
		unloadStore := func() error {
			fileStore, ok := e.fileStoreMapping[info.Name]
			if ok {
				if err := fileStore.File.Close(); err != nil {
					return fmt.Errorf("failed to close file from %s store: %v", info.Name, err)
				}
			}

			return nil
		}

		indexInfo := info.IndexInfo()
		loadIndexStore := func() error {
			_, err := e.loadStoreFromDisk(ctx, indexInfo)
			if err != nil {
				return fmt.Errorf("failed to load %s index store info from disk: %w", info.Name, err)
			}

			return nil
		}
		unloadIndexStore := func() error {
			fileStore, ok := e.fileStoreMapping[indexInfo.Name]
			if ok {
				if err := fileStore.File.Close(); err != nil {
					return fmt.Errorf("failed to close file from %s index store: %v", info.Name, err)
				}
			}

			return nil
		}

		indexListInfo := info.IndexListInfo()
		loadIndexListStore := func() error {
			_, err := e.loadStoreFromDisk(ctx, indexListInfo)
			if err != nil {
				return fmt.Errorf("failed to load %s index-list store info from disk: %w", info.Name, err)
			}

			return nil
		}
		unloadIndexListStore := func() error {
			fileStore, ok := e.fileStoreMapping[indexListInfo.Name]
			if ok {
				if err := fileStore.File.Close(); err != nil {
					return fmt.Errorf("failed to close file from %s index-list store: %v", info.Name, err)
				}
			}

			return nil
		}

		relationalInfo := info.RelationalInfo()
		relationalStore := func() error {
			_, err := e.loadStoreFromDisk(ctx, relationalInfo)
			if err != nil {
				return fmt.Errorf("failed to load %s relational store info from disk: %w", info.Name, err)
			}

			return nil
		}

		operations := []*lo.Operation{
			{
				Action: &lo.Action{
					Act:         loadStore,
					RetrialOpts: lo.DefaultRetrialOps,
				},
			},
			{
				Action: &lo.Action{
					Act:         loadIndexStore,
					RetrialOpts: lo.DefaultRetrialOps,
				},
				Rollback: &lo.RollbackAction{
					RollbackAct: unloadStore,
					RetrialOpts: lo.DefaultRetrialOps,
				},
			},
			{
				Action: &lo.Action{
					Act:         loadIndexListStore,
					RetrialOpts: lo.DefaultRetrialOps,
				},
				Rollback: &lo.RollbackAction{
					RollbackAct: unloadIndexStore,
					RetrialOpts: lo.DefaultRetrialOps,
				},
			},
			{
				Action: &lo.Action{
					Act:         relationalStore,
					RetrialOpts: lo.DefaultRetrialOps,
				},
				Rollback: &lo.RollbackAction{
					RollbackAct: unloadIndexListStore,
					RetrialOpts: lo.DefaultRetrialOps,
				},
			},
		}

		if err := lo.New(operations...); err != nil {
			return nil, fmt.Errorf("failed to load store info from disk - %s | err: %v", info.Name, err)
		}

		return &store, nil
	}

	return value.DBInfo, nil
}

func (e *LTNGEngine) loadStoreFromDisk(
	_ context.Context,
	info *DBInfo,
) (*DBInfo, error) {
	f, err := os.OpenFile(
		basePath+dbSep+info.Path,
		os.O_RDONLY, dbFileRead,
	)
	if err != nil {
		return nil, fmt.Errorf("error opening %s store: %v", info.Name, err)
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("error getting file %s store stats: %v", info.Name, err)
	}

	bs := make([]byte, stat.Size())
	_, err = f.Read(bs)
	if err != nil {
		return nil, err
	}

	var store DBInfo
	if err = e.serializer.Deserialize(bs, &store); err != nil {
		return nil, fmt.Errorf("error deserializing %s store info: %v", info.Name, err)
	}

	store.LastOpenedAt = time.Now().UTC().Unix()
	bs, err = e.serializer.Serialize(store)
	if err != nil {
		return nil, fmt.Errorf("error serializing last %s store info: %v", info.Name, err)
	}

	if _, err = f.Write(bs); err != nil {
		return nil, fmt.Errorf("error writing last %s store info: %v", info.Name, err)
	}

	e.fileStoreMapping[info.Name] = &fileInfo{
		DBInfo: &store,
		File:   f,
	}

	return &store, nil
}

func (e *LTNGEngine) openFile(
	_ context.Context,
	filePath string,
) (*os.File, error) {
	f, err := os.OpenFile(filePath, os.O_RDONLY, dbFileRead)
	if err != nil {
		return nil, fmt.Errorf("error opening %s store: %v", filePath, err)
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("error getting file %s store stats: %v", filePath, err)
	}

	bs := make([]byte, stat.Size())
	_, err = f.Read(bs)
	if err != nil {
		return nil, fmt.Errorf("error reading raw file from %s store: %v", filePath, err)
	}

	return f, nil
}

func (e *LTNGEngine) CreateStore(
	ctx context.Context,
	info *DBInfo,
) (store *DBInfo, err error) {
	e.opMtx.Lock(info.Name, info)
	defer e.opMtx.Unlock(info.Name)

	loadedInfo, err := e.LoadStore(ctx, info)
	if err == nil {
		return loadedInfo, nil
	}

	mainStore := func() error {
		store, err = e.createStoreOnDisk(ctx, info)
		if err != nil {
			return fmt.Errorf("error creating %s store: %v", info.Name, err)
		}

		return nil
	}
	deleteMainStore := func() error {
		err = e.DeleteStore(ctx, info)
		if err != nil {
			return fmt.Errorf("error deleting %s store: %v", info.Name, err)
		}

		return nil
	}

	indexInfo := info.IndexInfo()
	indexStore := func() error {
		_, err = e.createStoreOnDisk(ctx, indexInfo)
		if err != nil {
			return fmt.Errorf("error creating %s index store: %v", info.Name, err)
		}

		return nil
	}
	deleteIndexStore := func() error {
		err = e.DeleteStore(ctx, indexInfo)
		if err != nil {
			return fmt.Errorf("error deleting %s index store: %v", info.Name, err)
		}

		return nil
	}

	indexListInfo := info.IndexListInfo()
	indexListStore := func() error {
		_, err = e.createStoreOnDisk(ctx, indexListInfo)
		if err != nil {
			return fmt.Errorf("error creating %s index-list store: %v", info.Name, err)
		}

		return nil
	}
	deleteIndexListStore := func() error {
		err = e.DeleteStore(ctx, indexListInfo)
		if err != nil {
			return fmt.Errorf("error deleting %s index-list store: %v", info.Name, err)
		}

		return nil
	}

	relationalStore := func() error {
		_, err = e.createStoreOnDisk(ctx, info.RelationalInfo())
		if err != nil {
			return fmt.Errorf("error creating %s relational store: %v", info.Name, err)
		}

		return nil
	}

	operations := []*lo.Operation{
		{
			Action: &lo.Action{
				Act:         mainStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         indexStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: deleteMainStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         indexListStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: deleteIndexStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         relationalStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: deleteIndexListStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
	}

	if err = lo.New(operations...).Operate(); err != nil {
		return nil, err
	}

	return store, nil
}

func (e *LTNGEngine) LoadStore(
	ctx context.Context,
	info *DBInfo,
) (*DBInfo, error) {
	return e.loadStore(ctx, info)
}

func (e *LTNGEngine) DeleteStore(
	ctx context.Context,
	info *DBInfo,
) error {
	e.opMtx.Unlock(info.Name)
	defer e.opMtx.Unlock(info.Name)

	delete(e.fileStoreMapping, info.Name)
	delete(e.fileStoreMapping, info.IndexInfo().Name)
	delete(e.fileStoreMapping, info.IndexListInfo().Name)
	delete(e.fileStoreMapping, info.RelationalInfo().Name)

	if err := cpExec(ctx,
		basePath+info.Path+dbSep+everything,
		basePath+dbTmpDeletion+dbSep+info.Path+dbSep); err != nil {
		return fmt.Errorf("error copying %s store: %v", info.Name, err)
	}

	if err := os.RemoveAll(basePath + dbSep + info.Path); err != nil {
		if err := cpExec(ctx,
			basePath+dbTmpDeletion+dbSep+info.Path+dbSep+everything,
			basePath+info.Path+dbSep); err != nil {
			return fmt.Errorf("error de-copying %s store: %v", info.Name, err)
		}

		return fmt.Errorf("failed to remove %s store - %s | err: %v",
			info.Name, info.Name, err)
	}

	if err := os.RemoveAll(basePath + dbTmpDeletion + dbSep + info.Path); err != nil {
		return fmt.Errorf("failed to remove %s store - %s | err: %v",
			info.Name, info.Name, err)
	}

	return nil
}

func cpExec(_ context.Context, filePath string, dstFilePath string) error {
	return exec.Command("cp", "-R", filePath, dstFilePath).Run()
}

func (e *LTNGEngine) ListStores(
	ctx context.Context,
	pagination *ltng_engine_models.Pagination,
) ([]*DBInfo, error) {
	matches, err := filepath.Glob(basePath + dbSep + dbFileExtSP)
	if err != nil {
		return nil, fmt.Errorf("error listing stores: %v", err)
	}

	if pagination.IsValid() {
		pRef := pagination.PageID * pagination.PageSize
		pLimit := pRef + pagination.PageSize
		if pLimit > uint64(len(matches)) {
			pLimit = uint64(len(matches))
		}

		matches = matches[pRef : pRef+pagination.PageSize]
	}

	for _, file := range matches {
		filePath := strings.ReplaceAll(file, basePath+dbSep, "")
		splitPath := strings.Split(filePath, dbSep)
		name := splitPath[len(splitPath)-1]
		path := strings.Join(splitPath[:len(splitPath)-1], "/")
		info := &DBInfo{
			Name: name,
			Path: path,
		}

		e.opMtx.Lock(info.Name, info)
		if _, err = e.LoadStore(ctx, info); err != nil {
			if err = e.DeleteStore(ctx, info); err != nil {
				log.Printf("error deleting %s store: %v\n", info.Name, err)
			}
		}
		e.opMtx.Unlock(info.Name)
	}

	return nil, nil
}

type (
	Item struct {
		Key   []byte
		Value []byte
		Error error
	}

	Items []*Item

	IndexOpts struct {
		HasIdx          bool
		ParentKey       []byte
		IndexingKeys    [][]byte
		IndexProperties IndexProperties
	}

	IndexProperties struct {
		IndexDeletionBehaviour IndexDeletionBehaviour
		IndexSearchPattern     IndexSearchPattern
		ListSearchPattern      ListSearchPattern
	}

	CreateOpts struct {
		IndexOpts IndexOpts
	}
)

type IndexDeletionBehaviour int

const (
	None IndexDeletionBehaviour = iota
	Cascade
	IndexOnly
	CascadeByIdx
)

type IndexSearchPattern int

const (
	One IndexSearchPattern = iota
	AndComputational
	OrComputational
	IndexingList
)

type ListSearchPattern int

const (
	Default ListSearchPattern = iota
	All
)

func (e *LTNGEngine) loadItemFromDisk(
	ctx context.Context,
	item *Item,
	opts *IndexOpts,
) {
	//
}
