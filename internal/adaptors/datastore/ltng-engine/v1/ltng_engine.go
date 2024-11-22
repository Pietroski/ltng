package v1

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gitlab.com/pietroski-software-company/devex/golang/serializer"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	ltng_engine_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/ltng-engine/v1"
	"gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/tools/lock"
	lo "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/list-operator"
)

func New(ctx context.Context, opts ...options.Option) (*LTNGEngine, error) {
	engine := &LTNGEngine{
		opMtx:            lock.NewEngineLock(),
		mtx:              new(sync.Mutex),
		fileStoreMapping: make(map[string]*fileInfo),
		serializer:       serializer.NewRawBinarySerializer(),
	}
	options.ApplyOptions(engine, opts...)

	if err := engine.createStatsPathOnDisk(ctx); err != nil {
		return nil, err
	}
	if _, _, err := engine.createStoreStatsOnDisk(
		ctx, dbManagerInfo.RelationalInfo(),
	); err != nil {
		return nil, fmt.Errorf("failed creating store stats manager on-disk: %w", err)
	}
	if err := os.MkdirAll(
		dbTmpDelDataPath, dbFilePerm,
	); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(
		dbTmpDelStatsPath, dbFilePerm,
	); err != nil {
		return nil, err
	}

	return engine, nil
}

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

func (e *LTNGEngine) CreateStore(
	ctx context.Context,
	info *DBInfo,
) (store *DBInfo, err error) {
	e.opMtx.Lock(info.Name, struct{}{})
	defer e.opMtx.Unlock(info.Name)

	if info.Path == "" {
		return nil, fmt.Errorf("missing path")
	}

	if loadedInfo, _, err := e.loadStoreFromMemoryOrDisk(ctx, info); err == nil {
		return loadedInfo, nil
	}

	statsFileCreation := func() error {
		store, _, err = e.createStoreStatsOnDisk(ctx, info)
		if err != nil {
			return fmt.Errorf("error creating %s store stats: %v", info.Name, err)
		}

		return nil
	}
	statsFileDeletion := func() error {
		err = e.DeleteStore(ctx, info)
		if err != nil {
			return fmt.Errorf("error deleting %s store stats: %v", info.Name, err)
		}

		return nil
	}

	mainStoreCreation := func() error {
		err = e.createDataPathOnDisk(ctx, info)
		if err != nil {
			return fmt.Errorf("error creating %s store: %v", info.Name, err)
		}

		return nil
	}
	mainStoreDeletion := func() error {
		err = e.DeleteStore(ctx, info)
		if err != nil {
			return fmt.Errorf("error creating %s store: %v", info.Name, err)
		}

		return nil
	}

	indexInfo := info.IndexInfo()
	indexStoreCreation := func() error {
		err = e.createDataPathOnDisk(ctx, indexInfo)
		if err != nil {
			return fmt.Errorf("error creating %s index store: %v", info.Name, err)
		}

		return nil
	}
	indexStoreDeletion := func() error {
		err = e.DeleteStore(ctx, indexInfo)
		if err != nil {
			return fmt.Errorf("error deleting %s index store: %v", info.Name, err)
		}

		return nil
	}

	indexListInfo := info.IndexListInfo()
	indexListStoreCreation := func() error {
		err = e.createDataPathOnDisk(ctx, indexListInfo)
		if err != nil {
			return fmt.Errorf("error creating %s index-list store: %v", info.Name, err)
		}

		return nil
	}
	indexListStoreDeletion := func() error {
		err = e.DeleteStore(ctx, indexListInfo)
		if err != nil {
			return fmt.Errorf("error deleting %s index-list store: %v", info.Name, err)
		}

		return nil
	}

	relationalInfo := info.RelationalInfo()
	relationalStoreCreation := func() error {
		err = e.createDataPathOnDisk(ctx, relationalInfo)
		if err != nil {
			return fmt.Errorf("error creating %s index-list store: %v", info.Name, err)
		}

		return nil
	}
	relationalStoreDeletion := func() error {
		err = e.DeleteStore(ctx, relationalInfo)
		if err != nil {
			return fmt.Errorf("error deleting %s index-list store: %v", info.Name, err)
		}

		return nil
	}

	relationalStoreStatsUpdate := func() error {
		relationalInfoManager := dbManagerInfo.RelationalInfo()
		_, file, err := e.loadStoreFromMemoryOrDisk(ctx, relationalInfoManager)
		if err != nil {
			return fmt.Errorf("error creating %s relational store: %w",
				relationalInfoManager.Name, err)
		}

		if _, err = file.Seek(0, 2); err != nil {
			return fmt.Errorf("error seeking to the end of %s manager relational store: %w",
				relationalInfoManager.Name, err)
		}

		if _, err := file.Write([]byte(lineBreak)); err != nil {
			return fmt.Errorf("error writing line break to the end of %s manager relational store: %w",
				relationalInfoManager.Name, err)
		}

		loadedInfo, _, err := e.loadStoreFromMemoryOrDisk(ctx, info)
		if err != nil {
			return fmt.Errorf("error loading created store from memory or disk %s relational store: %w",
				info.Name, err)
		}

		bs, err := e.serializer.Serialize(loadedInfo)
		if err != nil {
			return fmt.Errorf("error serializing loadedInfo %s from created store: %w",
				info.Name, err)
		}

		if _, err = file.Write(bs); err != nil {
			return fmt.Errorf("error writing created store %s to the relational file manager: %w",
				info.Name, err)
		}

		return nil
	}

	operations := []*lo.Operation{
		{
			Action: &lo.Action{
				Act:         statsFileCreation,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: statsFileDeletion,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         mainStoreCreation,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: mainStoreDeletion,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         indexStoreCreation,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: indexStoreDeletion,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         indexListStoreCreation,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: indexListStoreDeletion,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         relationalStoreCreation,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: relationalStoreDeletion,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         relationalStoreStatsUpdate,
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
	dbInfo, _, err := e.loadStoreFromMemoryOrDisk(ctx, info)
	return dbInfo, err
}

func (e *LTNGEngine) DeleteStore(
	ctx context.Context,
	info *DBInfo,
) error {
	e.opMtx.Lock(info.Name, struct{}{})
	defer e.opMtx.Unlock(info.Name)

	_ = e.fileStoreMapping[info.Name].File.Close()

	delete(e.fileStoreMapping, info.Name)
	delete(e.fileStoreMapping, info.RelationalInfo().Name)

	// create temporary deletion data directory
	if err := os.MkdirAll(
		getTmpDelDataPathWithSep(info.Name), dbFilePerm,
	); err != nil {
		return err
	}

	// create temporary deletion stats directory
	if err := os.MkdirAll(
		getTmpDelStatsPathWithSep(info.Name), dbFilePerm,
	); err != nil {
		return err
	}

	// copy data store to tmp del path
	if bs, err := cpExec(ctx, getDataPathWithSep(info.Path), getTmpDelDataPathWithSep(info.Name)); err != nil {
		return fmt.Errorf("error copying %s store data - output: %s: %w", info.Name, bs, err)
	}

	// copy stats file to tmp del stats path
	if bs, err := cpExec(ctx, getStatsFilepath(info.Name), getTmpDelStatsPathWithSep(info.Name)); err != nil {
		return fmt.Errorf("error copying %s store stats - output: %s: %w", info.Name, bs, err)
	}

	// delete all files from store directory
	if bs, err := delExec(ctx, getDataPathWithSep(info.Path)); err != nil {
		return fmt.Errorf("failed to remove %s store - output: %s: %w", info.Name, bs, err)
	}

	// delete stats file
	if bs, err := delFileExec(ctx, getStatsFilepath(info.Name)); err != nil {
		return fmt.Errorf("failed to remove tmp del path %s store - output: %s: %w", info.Name, bs, err)
	}

	//if bs, err := cpExec(ctx, getTmpDelDataPathWithSep(info.Name), getDataPathWithSep(info.Path)); err != nil {
	//	return fmt.Errorf("error de-copying %s store - output: %s: %w", info.Name, bs, err)
	//}
	//
	//if bs, err := cpExec(ctx, getTmpDelStatsPathWithSep(info.Name), getStatsFilepath(info.Name)); err != nil {
	//	return fmt.Errorf("error de-copying %s store - output: %s: %w", info.Name, bs, err)
	//}

	// delete tmp stats path
	if bs, err := delHardExec(ctx, getTmpDelDataPathWithSep(info.Name)); err != nil {
		return fmt.Errorf("failed to remove tmp del path %s store - output: %s: %w", info.Name, bs, err)
	}

	// delete tmp stats path
	if bs, err := delHardExec(ctx, getTmpDelStatsPathWithSep(info.Name)); err != nil {
		return fmt.Errorf("failed to remove tmp del path %s store - output: %s: %w", info.Name, bs, err)
	}

	return nil
}

func (e *LTNGEngine) ListStores(
	ctx context.Context,
	pagination *ltng_engine_models.Pagination,
) ([]*DBInfo, error) {
	matches, err := filepath.Glob(baseStatsPath + sep + allExt)
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
		filePath := strings.ReplaceAll(file, baseStatsPath+sep, "")
		splitPath := strings.Split(filePath, sep)
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
