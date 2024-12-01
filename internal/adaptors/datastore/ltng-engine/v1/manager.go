package v1

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	ltng_engine_models "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/internal/models/ltng-engine/v1"
	lo "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/list-operator"
)

const (
	StoreAlreadyExists = "store already exists"
)

func (e *LTNGEngine) createStore(
	ctx context.Context,
	info *StoreInfo,
) (store *StoreInfo, err error) {
	e.opMtx.Lock(info.Name, struct{}{})
	defer e.opMtx.Unlock(info.Name)

	if info.Path == "" {
		return nil, fmt.Errorf("missing path")
	}

	if fi, err := e.loadStoreFromMemoryOrDisk(ctx, info); err == nil {
		return fi.FileData.Header.StoreInfo, nil
	}

	statsFileCreation := func() error {
		fi, err := e.createStatsStoreOnDisk(ctx, info)
		if err != nil {
			return fmt.Errorf("error creating %s store stats: %v", info.Name, err)
		}
		store = fi.FileData.Header.StoreInfo
		e.storeFileMapping[info.Name] = fi

		return nil
	}
	statsFileDeletion := func() error {
		err = e.deleteStore(ctx, info)
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
		return e.insertRelationalStats(ctx, store)
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

func (e *LTNGEngine) loadStore(
	ctx context.Context,
	info *StoreInfo,
) (*StoreInfo, error) {
	e.opMtx.Lock(info.Name, struct{}{})
	defer e.opMtx.Unlock(info.Name)

	fi, err := e.loadStoreFromMemoryOrDisk(ctx, info)
	if err != nil {
		return nil, err
	}

	return fi.FileData.Header.StoreInfo, err
}

func (e *LTNGEngine) deleteStore(
	ctx context.Context,
	info *StoreInfo,
) error {
	e.opMtx.Lock(info.Name, struct{}{})
	defer e.opMtx.Unlock(info.Name)

	fileStats, ok := e.storeFileMapping[info.Name]
	if ok {
		_ = fileStats.File.Close()
	}

	delete(e.storeFileMapping, info.Name)
	delete(e.storeFileMapping, info.RelationalInfo().Name)

	// create temporary deletion stats directory
	if err := os.MkdirAll(
		getTmpDelStatsPathWithSep(info.Name), dbFilePerm,
	); err != nil {
		return err
	}

	// create temporary deletion data directory
	if err := os.MkdirAll(
		getTmpDelDataPathWithSep(info.Name), dbFilePerm,
	); err != nil {
		return err
	}

	deleteRowFromRelationalStore := func() error {
		fi, err := e.loadRelationalStoreFromMemoryOrDisk(ctx)
		if err != nil {
			return fmt.Errorf("error opening %s manager relational store: %w", info.Name, err)
		}

		if err = e.deleteFromRelationalStats(ctx, fi, []byte(info.Name)); err != nil {
			return fmt.Errorf("error deleting manager store stats from relation store: %w", err)
		}

		return nil
	}
	deleteRowFromRelationalStoreRollback := func() error {
		if bs, err := cpExec(ctx, getTmpDelStatsPathWithSep(info.Name), getStatsFilepath(info.Name)); err != nil {
			return fmt.Errorf("failed to re-copy tmp stats file %s - output: %s: %w", info.Name, bs, err)
		}

		if bs, err := delHardExec(ctx, getTmpDelStatsPath(info.Name)); err != nil {
			log.Printf("failed to delete tmp stats dir %s - output: %s: %v", info.Name, bs, err)
		}

		if bs, err := delHardExec(ctx, getTmpDelDataPath(info.Name)); err != nil {
			log.Printf("failed to delete tmp data dir %s - output: %s: %v", info.Name, bs, err)
		}

		return nil
	}

	copyStatsFile := func() error {
		// copy stats file to tmp del stats path
		if bs, err := cpExec(ctx, getStatsFilepath(info.Name), getTmpDelStatsPathWithSep(info.Name)); err != nil {
			return fmt.Errorf("error copying %s store stats - output: %s: %w", info.Name, bs, err)
		}

		return nil
	}
	copyStatsFileRollback := func() error {
		// delete stats file from tmp del stats path
		if bs, err := delHardExec(ctx, getTmpDelStatsPathWithSep(info.Name)); err != nil {
			return fmt.Errorf("error deleting %s tmp store stats - output: %s: %w", info.Name, bs, err)
		}

		return nil
	}

	copyDataPath := func() error {
		// copy data store to tmp del path
		if bs, err := cpExec(ctx, getDataPathWithSep(info.Path), getTmpDelDataPathWithSep(info.Name)); err != nil {
			return fmt.Errorf("error copying %s store data - output: %s: %w", info.Name, bs, err)
		}

		return nil
	}
	copyDataPathRollback := func() error {
		// delete all files from store directory
		if bs, err := delHardExec(ctx, getTmpDelDataPathWithSep(info.Name)); err != nil {
			return fmt.Errorf("failed deleting %s tmp data store - output: %s: %w", info.Name, bs, err)
		}

		return nil
	}

	deleteStatsPath := func() error {
		// delete stats file
		if bs, err := delFileExec(ctx, getStatsFilepath(info.Name)); err != nil {
			return fmt.Errorf("failed to relete %s stats file - output: %s: %w", info.Name, bs, err)
		}

		return nil
	}
	deleteStatsPathRollback := func() error {
		if bs, err := cpExec(ctx, getTmpDelStatsPathWithSep(info.Name), getStatsFilepath(info.Name)); err != nil {
			return fmt.Errorf("error de-copying %s store stats file - output: %s: %w", info.Name, bs, err)
		}

		return nil
	}

	deleteDataPath := func() error {
		// delete all files from store directory // delExec
		if bs, err := delStoreDirsExec(ctx, getDataPath(info.Path)); err != nil {
			return fmt.Errorf("failed to delete %s data store - output: %s: %w", info.Name, bs, err)
		}

		return nil
	}
	deleteDataPathRollback := func() error {
		if bs, err := cpExec(ctx, getTmpDelDataPathWithSep(info.Name), getDataPathWithSep(info.Path+sep+info.Name)); err != nil {
			return fmt.Errorf("error de-copying %s data store - output: %s: %w", info.Name, bs, err)
		}

		return nil
	}

	deleteTmpDataAndStatsPath := func() error {
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

	operations := []*lo.Operation{
		{
			Action: &lo.Action{
				Act:         deleteRowFromRelationalStore,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: deleteRowFromRelationalStoreRollback,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         copyStatsFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: copyStatsFileRollback,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         copyDataPath,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: copyDataPathRollback,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         deleteStatsPath,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: deleteStatsPathRollback,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         deleteDataPath,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: deleteDataPathRollback,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         deleteTmpDataAndStatsPath,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
	}

	if err := lo.New(operations...).Operate(); err != nil {
		return err
	}

	return nil
}

func (e *LTNGEngine) listStores(
	ctx context.Context,
	pagination *ltng_engine_models.Pagination,
) ([]*StoreInfo, error) {
	var matchBox []*StoreInfo
	if pagination.IsValid() {
		pRef := (pagination.PageID - 1) * pagination.PageSize
		pLimit := pRef + pagination.PageSize
		//if pLimit > uint64(len(matches)) {
		//	pLimit = uint64(len(matches))
		//}

		matchBox = make([]*StoreInfo, pLimit)
	} else {
		return nil, fmt.Errorf("invalid pagination")
	}

	relationalInfoManager := dbManagerStoreInfo.RelationalInfo()

	e.opMtx.Lock(relationalInfoManager.Name, struct{}{})
	defer e.opMtx.Unlock(relationalInfoManager.Name)

	fi, err := e.loadRelationalStoreFromMemoryOrDisk(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating %s relational store: %w",
			relationalInfoManager.Name, err)
	}

	reader, err := newFileReader(ctx, fi, true)
	if err != nil {
		return nil, fmt.Errorf("error creating %s file reader: %w",
			relationalInfoManager.Name, err)
	}

	var count int
	for idx := range matchBox {
		var bs []byte
		bs, err = reader.read(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, fmt.Errorf("error reading lines from %s file: %w",
				relationalInfoManager.Name, err)
		}
		if bs == nil {
			continue
		}

		var match FileData
		if err = e.serializer.Deserialize(bs, &match); err != nil {
			return nil, err
		}

		matchBox[idx] = match.Header.StoreInfo
		count++
	}

	return matchBox[:count], nil
}
