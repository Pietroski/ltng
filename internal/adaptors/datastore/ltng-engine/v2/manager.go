package v2

import (
	"context"
	"fmt"
	"io"
	"os"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/saga"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/rw"
)

const (
	StoreAlreadyExists = "store already exists"
)

func (e *LTNGEngine) createStore(
	ctx context.Context,
	info *ltngenginemodels.StoreInfo,
) (store *ltngenginemodels.StoreInfo, err error) {
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
			return fmt.Errorf("error creating %s store stats: %w", info.Name, err)
		}
		store = fi.FileData.Header.StoreInfo
		e.storeFileMapping.Set(info.Name, fi)

		return nil
	}
	statsFileDeletion := func() error {
		err = e.deleteStore(ctx, info)
		if err != nil {
			return fmt.Errorf("error deleting %s store stats: %w", info.Name, err)
		}

		return nil
	}

	mainStoreCreation := func() error {
		err = e.createDataPathOnDisk(ctx, info)
		if err != nil {
			return fmt.Errorf("error creating %s store: %w", info.Name, err)
		}

		return nil
	}
	mainStoreDeletion := func() error {
		err = e.DeleteStore(ctx, info)
		if err != nil {
			return fmt.Errorf("error creating %s store: %w", info.Name, err)
		}

		return nil
	}

	indexInfo := info.IndexInfo()
	indexStoreCreation := func() error {
		err = e.createDataPathOnDisk(ctx, indexInfo)
		if err != nil {
			return fmt.Errorf("error creating %s index store: %w", info.Name, err)
		}

		return nil
	}
	indexStoreDeletion := func() error {
		err = e.DeleteStore(ctx, indexInfo)
		if err != nil {
			return fmt.Errorf("error deleting %s index store: %w", info.Name, err)
		}

		return nil
	}

	indexListInfo := info.IndexListInfo()
	indexListStoreCreation := func() error {
		err = e.createDataPathOnDisk(ctx, indexListInfo)
		if err != nil {
			return fmt.Errorf("error creating %s index-list store: %w", info.Name, err)
		}

		return nil
	}
	indexListStoreDeletion := func() error {
		err = e.DeleteStore(ctx, indexListInfo)
		if err != nil {
			return fmt.Errorf("error deleting %s index-list store: %w", info.Name, err)
		}

		return nil
	}

	relationalInfo := info.RelationalInfo()
	relationalStoreCreation := func() error {
		if err = e.createDataPathOnDisk(ctx, relationalInfo); err != nil {
			return fmt.Errorf("error creating %s relational store: %w", relationalInfo.Name, err)
		}

		if _, err = e.createRelationalItemStore(ctx, info); err != nil {
			return fmt.Errorf("error creating %s relational store header item: %w", relationalInfo.Name, err)
		}

		return nil
	}
	relationalStoreDeletion := func() error {
		err = e.DeleteStore(ctx, relationalInfo)
		if err != nil {
			return fmt.Errorf("error deleting %s index-list store: %w", relationalInfo.Name, err)
		}

		return nil
	}

	relationalStoreStatsUpdate := func() error {
		return e.insertRelationalStats(ctx, store)
	}

	operations := []*saga.Operation{
		{
			Action: &saga.Action{
				Do:          statsFileCreation,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Do:          statsFileDeletion,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Do:          mainStoreCreation,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Do:          mainStoreDeletion,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Do:          indexStoreCreation,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Do:          indexStoreDeletion,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Do:          indexListStoreCreation,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Do:          indexListStoreDeletion,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Do:          relationalStoreCreation,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Do:          relationalStoreDeletion,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Do:          relationalStoreStatsUpdate,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
	}
	if err = saga.NewListOperator(operations...).Operate(); err != nil {
		return nil, err
	}

	return store, nil
}

func (e *LTNGEngine) loadStore(
	ctx context.Context,
	info *ltngenginemodels.StoreInfo,
) (*ltngenginemodels.StoreInfo, error) {
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
	info *ltngenginemodels.StoreInfo,
) error {
	e.opMtx.Lock(info.Name, struct{}{})
	defer e.opMtx.Unlock(info.Name)

	fileStats, ok := e.storeFileMapping.Get(info.Name)
	if ok {
		_ = fileStats.File.Close()
	}

	e.storeFileMapping.Delete(info.Name)
	e.storeFileMapping.Delete(info.RelationalInfo().Name)

	// create temporary deletion stats directory
	if err := os.MkdirAll(
		ltngenginemodels.GetTmpDelStatsPathWithSep(info.Name), ltngenginemodels.DBFilePerm,
	); err != nil {
		return err
	}

	// create temporary deletion data directory
	if err := os.MkdirAll(
		ltngenginemodels.GetTmpDelDataPathWithSep(info.Name), ltngenginemodels.DBFilePerm,
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
		if err := osx.CpExec(ctx,
			ltngenginemodels.GetTmpDelStatsPathWithSep(info.Name),
			ltngenginemodels.GetStatsFilepath(info.Name),
		); err != nil {
			return errorsx.Errorf("failed to re-copy tmp stats file %s: %v", info.Name, err)
		}

		if err := osx.DelHardExec(ctx, ltngenginemodels.GetTmpDelStatsPath(info.Name)); err != nil {
			e.logger.Error(ctx, "failed to delete tmp stats dir",
				"store_name", info.Name, "err", err)
		}

		if err := osx.DelHardExec(ctx, ltngenginemodels.GetTmpDelDataPath(info.Name)); err != nil {
			e.logger.Error(ctx, "failed to delete tmp data dir",
				"store_name", info.Name, "err", err)
		}

		return nil
	}

	copyStatsFile := func() error {
		// copy stats file to tmp del stats path
		if err := osx.CpExec(ctx,
			ltngenginemodels.GetStatsFilepath(info.Name),
			ltngenginemodels.GetTmpDelStatsPathWithSep(info.Name),
		); err != nil {
			return errorsx.Errorf("failed to copy %s store stats: %v", info.Name, err)
		}

		return nil
	}
	copyStatsFileRollback := func() error {
		// delete stats file from tmp del stats path
		if err := osx.DelHardExec(ctx, ltngenginemodels.GetTmpDelStatsPathWithSep(info.Name)); err != nil {
			return errorsx.Errorf("failed to delete %s tmp store stats: %v", info.Name, err)
		}

		return nil
	}

	copyDataPath := func() error {
		// copy data store to tmp del path
		if err := osx.CpExec(ctx,
			ltngenginemodels.GetDataPathWithSep(info.Path),
			ltngenginemodels.GetTmpDelDataPathWithSep(info.Name),
		); err != nil {
			return errorsx.Errorf("failed to copy %s store data: %v", info.Name, err)
		}

		return nil
	}
	copyDataPathRollback := func() error {
		// delete all files from store directory
		if err := osx.DelHardExec(ctx, ltngenginemodels.GetTmpDelDataPathWithSep(info.Name)); err != nil {
			return errorsx.Errorf("failed to delete %s tmp data store: %v", info.Name, err)
		}

		return nil
	}

	deleteStatsPath := func() error {
		// delete stats file
		if err := osx.DelFileExec(ctx, ltngenginemodels.GetStatsFilepath(info.Name)); err != nil {
			return errorsx.Errorf("failed to delete %s stats file: %v", info.Name, err)
		}

		return nil
	}
	deleteStatsPathRollback := func() error {
		if err := osx.CpExec(ctx,
			ltngenginemodels.GetTmpDelStatsPathWithSep(info.Name),
			ltngenginemodels.GetStatsFilepath(info.Name),
		); err != nil {
			return errorsx.Errorf("failed to re-copy tmp stats file %s: %v", info.Name, err)
		}

		return nil
	}

	deleteDataPath := func() error {
		// delete all files from store directory // delExec // DelDirsWithoutSepBothOSExec <-> DelStoreDirsExec
		if err := osx.DelDirsBothOSExec(ctx, ltngenginemodels.GetDataPath(info.Path)); err != nil {
			return errorsx.Errorf("failed to delete %s data store: %v", info.Name, err)
		}

		return nil
	}
	deleteDataPathRollback := func() error {
		if err := osx.CpExec(ctx,
			ltngenginemodels.GetTmpDelDataPathWithSep(info.Name),
			ltngenginemodels.GetDataPathWithSep(info.Path+ltngenginemodels.Sep+info.Name),
		); err != nil {
			return errorsx.Errorf("failed to de-copy tmp data file %s: %v", info.Name, err)
		}

		return nil
	}

	deleteTmpDataAndStatsPath := func() error {
		// delete tmp stats path
		if err := osx.DelHardExec(ctx, ltngenginemodels.GetTmpDelDataPathWithSep(info.Name)); err != nil {
			return errorsx.Errorf("failed to remove tmp del path %s store: %v", info.Name, err)
		}

		// delete tmp stats path
		if err := osx.DelHardExec(ctx, ltngenginemodels.GetTmpDelStatsPathWithSep(info.Name)); err != nil {
			return errorsx.Errorf("failed to remove tmp del path %s store: %v", info.Name, err)
		}

		return nil
	}

	operations := []*saga.Operation{
		{
			Action: &saga.Action{
				Do:          deleteRowFromRelationalStore,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Do:          deleteRowFromRelationalStoreRollback,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Do:          copyStatsFile,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Do:          copyStatsFileRollback,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Do:          copyDataPath,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Do:          copyDataPathRollback,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Do:          deleteStatsPath,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Do:          deleteStatsPathRollback,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Do:          deleteDataPath,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Do:          deleteDataPathRollback,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Do:          deleteTmpDataAndStatsPath,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
	}

	if err := saga.NewListOperator(operations...).Operate(); err != nil {
		return err
	}

	return nil
}

func (e *LTNGEngine) listStores(
	ctx context.Context,
	pagination *ltngenginemodels.Pagination,
) ([]*ltngenginemodels.StoreInfo, error) {
	var matchBox []*ltngenginemodels.StoreInfo
	if pagination.IsValid() {
		pRef := (pagination.PageID - 1) * pagination.PageSize
		pLimit := pRef + pagination.PageSize
		//if pLimit > uint64(len(matches)) {
		//	pLimit = uint64(len(matches))
		//}

		matchBox = make([]*ltngenginemodels.StoreInfo, pLimit)
	} else {
		return nil, fmt.Errorf("invalid pagination")
	}

	relationalInfoManager := ltngenginemodels.DBManagerStoreInfo.RelationalInfo()

	e.opMtx.Lock(relationalInfoManager.Name, struct{}{})
	defer e.opMtx.Unlock(relationalInfoManager.Name)

	fi, err := e.loadRelationalStoreFromMemoryOrDisk(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating %s relational store: %w",
			relationalInfoManager.Name, err)
	}

	reader, err := rw.NewFileReader(ctx, fi, true)
	if err != nil {
		return nil, fmt.Errorf("error creating %s file reader: %w",
			relationalInfoManager.Name, err)
	}

	var count int
	for idx := range matchBox {
		var bs []byte
		bs, err = reader.Read(ctx)
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

		var match ltngenginemodels.FileData
		if err = e.serializer.Deserialize(bs, &match); err != nil {
			return nil, err
		}

		matchBox[idx] = match.Header.StoreInfo
		count++
	}

	return matchBox[:count], nil
}
