package ltngdbenginev3

import (
	"context"
	"os"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/saga"

	ltngdbenginemodelsv3 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdbengine/v3"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"
)

func (e *LTNGEngine) createManagerStatsPathsOnDisk(
	ctx context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) error {
	return saga.NewListOperator(e.buildStatsPathOperations(ctx, info)...).Operate()
}

func (e *LTNGEngine) createStatsPathsOnDisk(
	ctx context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) error {
	return saga.NewListOperator(e.buildStatsPathOperations(ctx, info)[0:2]...).Operate()
}

func (e *LTNGEngine) buildStatsPathOperations(
	ctx context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) []*saga.Operation {
	createStatsPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetStatsPath(info.Path)
		if err := os.MkdirAll(path, osx.FileRW); err != nil {
			return errorsx.Wrapf(err, "error creating stats path: %s", path)
		}

		return nil
	}
	deleteStatsPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetStatsPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting stats path: %s", path)
		}

		return nil
	}
	createTemporaryStatsPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetStatsPath(info.TemporaryInfo().Path)
		if err := os.MkdirAll(path, osx.FileRW); err != nil {
			return errorsx.Wrapf(err, "error creating temporary stats path: %s", path)
		}

		return nil
	}
	deleteTemporaryStatsPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetStatsPath(info.TemporaryInfo().Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting temporary stats path: %s", path)
		}

		return nil
	}

	createRelationalStatsPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetStatsPath(info.RelationalInfo().Path)
		if err := os.MkdirAll(path, osx.FileRW); err != nil {
			return errorsx.Wrapf(err, "error creating relational stats path: %s", path)
		}

		return nil
	}
	deleteRelationalStatsPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetStatsPath(info.RelationalInfo().Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting relational stats path: %s", path)
		}

		return nil
	}
	createTemporaryRelationalStatsPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetStatsPath(info.TemporaryInfo().RelationalInfo().Path)
		if err := os.MkdirAll(path, osx.FileRW); err != nil {
			return errorsx.Wrapf(err, "error creating temporary relational stats path: %s", path)
		}

		return nil
	}

	return []*saga.Operation{
		{
			Action: &saga.Action{
				Name:        "createStatsPathOnDisk",
				Do:          createStatsPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteStatsPathOnDisk",
				Do:          deleteStatsPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "createTemporaryStatsPathOnDisk",
				Do:          createTemporaryStatsPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteTemporaryStatsPathOnDisk",
				Do:          deleteTemporaryStatsPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "createRelationalStatsPathOnDisk",
				Do:          createRelationalStatsPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteRelationalStatsPathOnDisk",
				Do:          deleteRelationalStatsPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "createTemporaryRelationalStatsPathOnDisk",
				Do:          createTemporaryRelationalStatsPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
	}
}

func (e *LTNGEngine) createDataPathsOnDisk(
	ctx context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) error {
	return saga.NewListOperator(e.buildDataPathOperations(ctx, info)...).Operate()
}

func (e *LTNGEngine) buildDataPathOperations(
	ctx context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) []*saga.Operation {
	createDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.Path)
		if err := os.MkdirAll(path, osx.FileRW); err != nil {
			return errorsx.Wrapf(err, "error creating data path: %s", path)
		}

		return nil
	}
	deleteDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting data path: %s", path)
		}

		return nil
	}
	createTemporaryDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.TemporaryInfo().Path)
		if err := os.MkdirAll(path, osx.FileRW); err != nil {
			return errorsx.Wrapf(err, "error creating temporary data path: %s", path)
		}

		return nil
	}
	deleteTemporaryDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.TemporaryInfo().Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting temporary data path: %s", path)
		}

		return nil
	}

	createRelationalDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.RelationalInfo().Path)
		if err := os.MkdirAll(path, osx.FileRW); err != nil {
			return errorsx.Wrapf(err, "error creating relational data path: %s", path)
		}

		return nil
	}
	deleteRelationalDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.RelationalInfo().Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting relational data path: %s", path)
		}

		return nil
	}
	createTemporaryRelationalDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.TemporaryInfo().RelationalInfo().Path)
		if err := os.MkdirAll(path, osx.FileRW); err != nil {
			return errorsx.Wrapf(err, "error creating temporary relational data path: %s", path)
		}

		return nil
	}
	deleteTemporaryRelationalDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.TemporaryInfo().RelationalInfo().Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting temporary relational data path: %s", path)
		}

		return nil
	}

	createIndexedDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.IndexInfo().Path)
		if err := os.MkdirAll(path, osx.FileRW); err != nil {
			return errorsx.Wrapf(err, "error creating indexed data path: %s", path)
		}

		return nil
	}
	deleteIndexedDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.IndexInfo().Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting indexed data path: %s", path)
		}

		return nil
	}
	createTemporaryIndexedDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.TemporaryInfo().IndexInfo().Path)
		if err := os.MkdirAll(path, osx.FileRW); err != nil {
			return errorsx.Wrapf(err, "error creating temporary indexed data path: %s", path)
		}

		return nil
	}
	deleteTemporaryIndexedDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.TemporaryInfo().IndexInfo().Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting temporary indexed data path: %s", path)
		}

		return nil
	}

	createIndexedListDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.IndexListInfo().Path)
		if err := os.MkdirAll(path, osx.FileRW); err != nil {
			return errorsx.Wrapf(err, "error creating indexed list data path: %s", path)
		}

		return nil
	}
	deleteIndexedListDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.IndexListInfo().Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting indexed list data path: %s", path)
		}

		return nil
	}
	createTemporaryIndexedListDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.TemporaryInfo().IndexListInfo().Path)
		if err := os.MkdirAll(path, osx.FileRW); err != nil {
			return errorsx.Wrapf(err, "error creating temporary indexed list data path: %s", path)
		}

		return nil
	}

	return []*saga.Operation{
		{
			Action: &saga.Action{
				Name:        "createDataPathOnDisk",
				Do:          createDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteDataPathOnDisk",
				Do:          deleteDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "createTemporaryDataPathOnDisk",
				Do:          createTemporaryDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteTemporaryDataPathOnDisk",
				Do:          deleteTemporaryDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "createRelationalDataPathOnDisk",
				Do:          createRelationalDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteRelationalDataPathOnDisk",
				Do:          deleteRelationalDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "createTemporaryRelationalDataPathOnDisk",
				Do:          createTemporaryRelationalDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteTemporaryRelationalDataPathOnDisk",
				Do:          deleteTemporaryRelationalDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "createIndexedDataPathOnDisk",
				Do:          createIndexedDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteIndexedDataPathOnDisk",
				Do:          deleteIndexedDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "createTemporaryIndexedDataPathOnDisk",
				Do:          createTemporaryIndexedDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteTemporaryIndexedDataPathOnDisk",
				Do:          deleteTemporaryIndexedDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "createIndexedListDataPathOnDisk",
				Do:          createIndexedListDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteIndexedListDataPathOnDisk",
				Do:          deleteIndexedListDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "createTemporaryIndexedListDataPathOnDisk",
				Do:          createTemporaryIndexedListDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
	}
}

// ################################################################################################################## \\

func (e *LTNGEngine) removeEmptyManagerStatsPathsFromDisk(
	ctx context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) error {
	if err := e.removeEmptyStatsPathsFromDisk(ctx, info); err != nil {
		e.logger.Error(ctx, "error cleaning up stats paths from disk",
			"path", info.Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdbenginemodelsv3.GetStatsPath(
		info.RelationalInfo().Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up relational stats path",
			"path", info.Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdbenginemodelsv3.GetStatsPath(
		info.TemporaryInfo().RelationalInfo().Path),
	); err != nil {
		e.logger.Error(ctx, "error cleaning up temporary relational stats path",
			"path", info.Path, "error", err)
	}

	return nil
}

func (e *LTNGEngine) removeEmptyStatsPathsFromDisk(
	ctx context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) error {
	if err := osx.CleanupEmptyDirs(ctx, ltngdbenginemodelsv3.GetStatsPath(info.Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up stats path",
			"path", info.Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdbenginemodelsv3.GetStatsPath(info.TemporaryInfo().Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up temporary stats path",
			"path", info.Path, "error", err)
	}

	return nil
}

func (e *LTNGEngine) removeEmptyDataPathsFromDisk(
	ctx context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) error {
	if err := osx.CleanupEmptyDirs(ctx, ltngdbenginemodelsv3.GetDataPath(info.Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up data path",
			"path", info.Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdbenginemodelsv3.GetDataPath(
		info.TemporaryInfo().Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up tmp data path",
			"path", info.Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdbenginemodelsv3.GetDataPath(
		info.IndexInfo().Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up indexed data path",
			"path", info.IndexInfo().Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdbenginemodelsv3.GetDataPath(
		info.TemporaryInfo().IndexInfo().Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up tmp data indexed path",
			"path", info.TemporaryInfo().IndexInfo().Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdbenginemodelsv3.GetDataPath(
		info.IndexListInfo().Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up indexed list data path",
			"path", info.IndexListInfo().Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdbenginemodelsv3.GetDataPath(
		info.TemporaryInfo().IndexListInfo().Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up tmp data indexed list path",
			"path", info.TemporaryInfo().IndexListInfo().Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdbenginemodelsv3.GetDataPath(
		info.RelationalInfo().Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up relational data path",
			"path", info.RelationalInfo().Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdbenginemodelsv3.GetDataPath(
		info.TemporaryInfo().RelationalInfo().Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up tmp relational data path",
			"path", info.TemporaryInfo().RelationalInfo().Path, "error", err)
	}

	return nil
}

// ################################################################################################################## \\

func (e *LTNGEngine) createStoreOnDisk(
	ctx context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) error {
	return saga.NewListOperator(e.buildCreateStoreOperations(ctx, info)...).Operate()
}

func (e *LTNGEngine) buildCreateStoreOperations(
	ctx context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) []*saga.Operation {
	createStatsPathsOnDisk := func() error {
		return e.createStatsPathsOnDisk(ctx, info)
	}
	deleteStatsPathsFromDisk := func() error {
		return e.removeEmptyStatsPathsFromDisk(ctx, info)
	}

	createDataPathsOnDisk := func() error {
		return e.createDataPathsOnDisk(ctx, info)
	}
	deleteDataPathsFromDisk := func() error {
		return e.removeEmptyDataPathsFromDisk(ctx, info)
	}

	createStatsStoreOnDisk := func() error {
		if _, err := e.createStatsStoreOnDisk(ctx, info); err != nil {
			return err
		}

		return nil
	}
	deleteStatsStoreFromDisk := func() error {
		if err := e.deleteStatsStoreFromDisk(ctx, info); err != nil {
			return err
		}

		return nil
	}

	createRelationalStoreOnDisk := func() error {
		if _, err := e.createRelationalDataStore(ctx, info); err != nil {
			return err
		}

		return nil
	}
	deleteRelationalStoreFromDisk := func() error {
		if err := e.deleteRelationalDataStore(ctx, info); err != nil {
			return err
		}

		return nil
	}

	updateRelationalStatsStoreOnDisk := func() error {
		if err := e.upsertRelationalStats(ctx, info); err != nil {
			return err
		}

		return nil
	}

	operations := []*saga.Operation{
		{
			Action: &saga.Action{
				Name:        "createStatsPathsOnDisk",
				Do:          createStatsPathsOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteStatsPathsFromDisk",
				Do:          deleteStatsPathsFromDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "createDataPathsOnDisk",
				Do:          createDataPathsOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteDataPathsFromDisk",
				Do:          deleteDataPathsFromDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "createStatsStoreOnDisk",
				Do:          createStatsStoreOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteStatsStoreFromDisk",
				Do:          deleteStatsStoreFromDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "createRelationalStoreOnDisk",
				Do:          createRelationalStoreOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteRelationalStoreFromDisk",
				Do:          deleteRelationalStoreFromDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "updateRelationalStatsStoreOnDisk",
				Do:          updateRelationalStatsStoreOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
	}
	return operations
}

func (e *LTNGEngine) deleteFStoreFromDisk(
	ctx context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) error {
	return saga.NewListOperator(e.buildDeletionStoreOperations(ctx, info)...).Operate()
}

func (e *LTNGEngine) buildDeletionStoreOperations(
	ctx context.Context,
	info *ltngdbenginemodelsv3.StoreInfo,
) []*saga.Operation {
	moveDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.Path)
		tmpPath := ltngdbenginemodelsv3.GetDataPath(info.TemporaryInfo().Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, path, tmpPath); err != nil {
			return errorsx.Wrapf(err, "error moving data files to tmp path: %s", path)
		}

		return nil
	}
	moveBackDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.Path)
		tmpPath := ltngdbenginemodelsv3.GetDataPath(info.TemporaryInfo().Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, tmpPath, path); err != nil {
			return errorsx.Wrapf(err, "error moving back data files from tmp path: %s", path)
		}

		return nil
	}

	moveIndexDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.IndexInfo().Path)
		tmpPath := ltngdbenginemodelsv3.GetDataPath(info.TemporaryIndexInfo().Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, path, tmpPath); err != nil {
			return errorsx.Wrapf(err, "error moving indexed data files to tmp path: %s", path)
		}

		return nil
	}
	moveBackIndexedDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.IndexInfo().Path)
		tmpPath := ltngdbenginemodelsv3.GetDataPath(info.TemporaryIndexInfo().Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, tmpPath, path); err != nil {
			return errorsx.Wrapf(err, "error moving back indexed data files to tmp path: %s", path)
		}

		return nil
	}

	moveIndexListDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.IndexListInfo().Path)
		tmpPath := ltngdbenginemodelsv3.GetDataPath(info.TemporaryIndexListInfo().Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, path, tmpPath); err != nil {
			return errorsx.Wrapf(err, "error moving indexed list data files to tmp path: %s", path)
		}

		return nil
	}
	moveBackIndexedListDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.IndexListInfo().Path)
		tmpPath := ltngdbenginemodelsv3.GetDataPath(info.TemporaryIndexListInfo().Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, tmpPath, path); err != nil {
			return errorsx.Wrapf(err, "error moving back indexed list data files to tmp path: %s", path)
		}

		return nil
	}

	moveRelationalDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.RelationalInfo().Path)
		tmpPath := ltngdbenginemodelsv3.GetDataPath(info.TemporaryRelationalInfo().Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, path, tmpPath); err != nil {
			return errorsx.Wrapf(err, "error moving indexed list data files to tmp path: %s", path)
		}

		return nil
	}
	moveBackRelationalDataPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.RelationalInfo().Path)
		tmpPath := ltngdbenginemodelsv3.GetDataPath(info.TemporaryRelationalInfo().Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, tmpPath, path); err != nil {
			return errorsx.Wrapf(err, "error moving back indexed list data files to tmp path: %s", path)
		}

		return nil
	}

	moveStatsPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetStatsPath(info.Path)
		tmpPath := ltngdbenginemodelsv3.GetStatsPath(info.TemporaryInfo().Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, path, tmpPath); err != nil {
			return errorsx.Wrapf(err, "error moving stats files to tmp path: %s", path)
		}

		return nil
	}
	moveBackStatsPathOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetStatsPath(info.Path)
		tmpPath := ltngdbenginemodelsv3.GetStatsPath(info.TemporaryInfo().Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, tmpPath, path); err != nil {
			return errorsx.Wrapf(err, "error moving back stats files from tmp path: %s", path)
		}

		return nil
	}

	//moveRelationalStatsPathOnDisk := func() error {
	//	path := ltngdbenginemodelsv3.GetStatsPath(info.RelationalInfo().Path)
	//	tmpPath := ltngdbenginemodelsv3.GetStatsPath(info.TemporaryRelationalInfo().Path)
	//	if _, err := osx.MvOnlyFilesFromDirAsync(ctx, path, tmpPath); err != nil {
	//		return errorsx.Wrapf(err, "error moving relational stats files to tmp path: %s", path)
	//	}
	//
	//	return nil
	//}
	//moveBackRelationalStatsPathOnDisk := func() error {
	//	path := ltngdbenginemodelsv3.GetStatsPath(info.RelationalInfo().Path)
	//	tmpPath := ltngdbenginemodelsv3.GetStatsPath(info.TemporaryRelationalInfo().Path)
	//	if _, err := osx.MvOnlyFilesFromDirAsync(ctx, tmpPath, path); err != nil {
	//		return errorsx.Wrapf(err, "error moving back relational stats files from tmp path: %s", path)
	//	}
	//
	//	return nil
	//}

	cleanupDataPathsOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error cleaning up data path: %s", path)
		}

		path = ltngdbenginemodelsv3.GetDataPath(info.IndexInfo().Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error cleaning up indexed data path: %s", path)
		}

		path = ltngdbenginemodelsv3.GetDataPath(info.IndexListInfo().Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error cleaning up indexed list data path: %s", path)
		}

		path = ltngdbenginemodelsv3.GetDataPath(info.RelationalInfo().Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error cleaning up relational data path: %s", path)
		}

		return nil
	}
	restoreDataPathsOnDisk := func() error {
		return e.createDataPathsOnDisk(ctx, info)
	}

	cleanupStatsPathsOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetStatsPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error cleaning up stats path: %s", path)
		}

		//path = ltngdbenginemodelsv3.GetStatsPath(info.RelationalInfo().Path)
		//if err := osx.CleanupDirs(ctx, path); err != nil {
		//	return errorsx.Wrapf(err, "error cleaning up relational stats path: %s", path)
		//}

		return nil
	}
	restoreStatsPathsOnDisk := func() error {
		return e.createStatsPathsOnDisk(ctx, info)
	}

	removeFromRelationalStatsFile := func() error {
		rfi, err := e.loadRelationalStatsStoreFromMemoryOrDisk(ctx)
		if err != nil {
			return errorsx.Wrapf(err, "error loading %s relational store",
				ltngdbenginemodelsv3.DBManagerStoreInfo.RelationalInfo().Name)
		}

		_, err = rfi.RelationalFileManager.DeleteByKey(ctx, rfi.FileData.Key)
		if err != nil {
			return err
		}

		return nil
	}
	restoreRelationalStatsFile := func() error {
		return nil
	}

	cleanupTemporaryDataPathsOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetDataPath(info.TemporaryInfo().Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			e.logger.Error(ctx, "error cleaning up temporary data path", "path", path)
		}

		path = ltngdbenginemodelsv3.GetDataPath(info.TemporaryIndexInfo().Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			e.logger.Error(ctx, "error cleaning up temporary indexed data path", "path", path)
		}

		path = ltngdbenginemodelsv3.GetDataPath(info.TemporaryIndexListInfo().Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			e.logger.Error(ctx, "error cleaning up temporary indexed list data path", "path", path)
		}

		path = ltngdbenginemodelsv3.GetDataPath(info.TemporaryRelationalInfo().Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			e.logger.Error(ctx, "error cleaning up temporary relational data path", "path", path)
		}

		return nil
	}
	restoreTemporaryDataPathsOnDisk := func() error {
		return nil
	}

	cleanupTemporaryStatsPathsOnDisk := func() error {
		path := ltngdbenginemodelsv3.GetStatsPath(info.TemporaryInfo().Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			e.logger.Error(ctx, "error cleaning up temporary data path", "path", path)
		}

		//path = ltngdbenginemodelsv3.GetStatsPath(info.TemporaryRelationalInfo().Path)
		//if err := osx.CleanupDirs(ctx, path); err != nil {
		//	e.logger.Error(ctx, "error cleaning up temporary indexed data path", "path", path)
		//}

		return nil
	}

	return []*saga.Operation{
		{
			Action: &saga.Action{
				Name:        "moveDataPathOnDisk",
				Do:          moveDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "moveBackDataPathOnDisk",
				Do:          moveBackDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "moveIndexDataPathOnDisk",
				Do:          moveIndexDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "moveBackIndexedDataPathOnDisk",
				Do:          moveBackIndexedDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "moveIndexListDataPathOnDisk",
				Do:          moveIndexListDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "moveBackIndexedListDataPathOnDisk",
				Do:          moveBackIndexedListDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "moveRelationalDataPathOnDisk",
				Do:          moveRelationalDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "moveBackRelationalDataPathOnDisk",
				Do:          moveBackRelationalDataPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "moveStatsPathOnDisk",
				Do:          moveStatsPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "moveBackStatsPathOnDisk",
				Do:          moveBackStatsPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		//{
		//	Action: &saga.Action{
		//		Name:        "moveRelationalStatsPathOnDisk",
		//		Do:          moveRelationalStatsPathOnDisk,
		//		RetrialOpts: saga.DefaultRetrialOps,
		//	},
		//	Rollback: &saga.Rollback{
		//		Name:        "moveBackRelationalStatsPathOnDisk",
		//		Do:          moveBackRelationalStatsPathOnDisk,
		//		RetrialOpts: saga.DefaultRetrialOps,
		//	},
		//},
		{
			Action: &saga.Action{
				Name:        "cleanupDataPathsOnDisk",
				Do:          cleanupDataPathsOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "restoreDataPathsOnDisk",
				Do:          restoreDataPathsOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "cleanupStatsPathsOnDisk",
				Do:          cleanupStatsPathsOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "restoreStatsPathsOnDisk",
				Do:          restoreStatsPathsOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "removeFromRelationalStatsFile",
				Do:          removeFromRelationalStatsFile,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "restoreRelationalStatsFile",
				Do:          restoreRelationalStatsFile,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "cleanupTemporaryDataPathsOnDisk",
				Do:          cleanupTemporaryDataPathsOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "restoreTemporaryDataPathsOnDisk",
				Do:          restoreTemporaryDataPathsOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "cleanupTemporaryStatsPathsOnDisk",
				Do:          cleanupTemporaryStatsPathsOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
	}
}

// ################################################################################################################## \\
