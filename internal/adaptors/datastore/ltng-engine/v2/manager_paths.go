package v2

import (
	"context"
	"os"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/saga"
	"gitlab.com/pietroski-software-company/lightning-db/pkg/tools/osx"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
)

func (e *LTNGEngine) createStatsPathsOnDisk(
	ctx context.Context,
	info *ltngdata.StoreInfo,
) error {
	return saga.NewListOperator(e.buildStatsPathOperations(ctx, info)...).Operate()
}

func (e *LTNGEngine) buildStatsPathOperations(
	ctx context.Context,
	info *ltngdata.StoreInfo,
) []*saga.Operation {
	createStatsPathOnDisk := func() error {
		path := ltngdata.GetStatsPath(info.Path)
		if err := os.MkdirAll(path, ltngdata.DBFileRW); err != nil {
			return errorsx.Wrapf(err, "error creating stats path: %s", path)
		}

		return nil
	}
	deleteStatsPathOnDisk := func() error {
		path := ltngdata.GetStatsPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting stats path: %s", path)
		}

		return nil
	}
	createTemporaryStatsPathOnDisk := func() error {
		path := ltngdata.GetTemporaryStatsPath(info.Path)
		if err := os.MkdirAll(path, ltngdata.DBFileRW); err != nil {
			return errorsx.Wrapf(err, "error creating temporary stats path: %s", path)
		}

		return nil
	}
	deleteTemporaryStatsPathOnDisk := func() error {
		path := ltngdata.GetTemporaryStatsPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting temporary stats path: %s", path)
		}

		return nil
	}

	createRelationalStatsPathOnDisk := func() error {
		path := ltngdata.GetRelationalStatsPath(info.Path)
		if err := os.MkdirAll(path, ltngdata.DBFileRW); err != nil {
			return errorsx.Wrapf(err, "error creating stats path: %s", path)
		}

		return nil
	}
	deleteRelationalStatsPathOnDisk := func() error {
		path := ltngdata.GetRelationalStatsPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting stats path: %s", path)
		}

		return nil
	}
	createTemporaryRelationalStatsPathOnDisk := func() error {
		path := ltngdata.GetTemporaryRelationalStatsPath(info.Path)
		if err := os.MkdirAll(path, ltngdata.DBFileRW); err != nil {
			return errorsx.Wrapf(err, "error creating stats path: %s", path)
		}

		return nil
	}
	//deleteTemporaryRelationalStatsPathOnDisk := func() error {
	//	path := ltngdata.GetTemporaryRelationalStatsPath(info.Path)
	//	if err := osx.CleanupDirs(ctx, path); err != nil {
	//		return errorsx.Wrapf(err, "error deleting stats path: %s", path)
	//	}
	//
	//	return nil
	//}

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
	info *ltngdata.StoreInfo,
) error {
	return saga.NewListOperator(e.buildDataPathOperations(ctx, info)...).Operate()
}

func (e *LTNGEngine) buildDataPathOperations(
	ctx context.Context,
	info *ltngdata.StoreInfo,
) []*saga.Operation {
	createDataPathOnDisk := func() error {
		path := ltngdata.GetDataPath(info.Path)
		if err := os.MkdirAll(path, ltngdata.DBFileRW); err != nil {
			return errorsx.Wrapf(err, "error creating data path: %s", path)
		}

		return nil
	}
	deleteDataPathOnDisk := func() error {
		path := ltngdata.GetDataPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting data path: %s", path)
		}

		return nil
	}
	createTemporaryDataPathOnDisk := func() error {
		path := ltngdata.GetTemporaryDataPath(info.Path)
		if err := os.MkdirAll(path, ltngdata.DBFileRW); err != nil {
			return errorsx.Wrapf(err, "error creating temporary data path: %s", path)
		}

		return nil
	}
	deleteTemporaryDataPathOnDisk := func() error {
		path := ltngdata.GetTemporaryDataPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting temporary data path: %s", path)
		}

		return nil
	}

	//createRubishDataPathOnDisk := func() error {
	//	path := ltngdata.GetRubishDataPath(info.Path)
	//	if err := os.MkdirAll(path, ltngdata.DBFileRW); err != nil {
	//		return errorsx.Wrapf(err, "error creating rubish data path: %s", path)
	//	}
	//
	//	return nil
	//}
	//deleteRubishDataPathOnDisk := func() error {
	//	path := ltngdata.GetRubishDataPath(info.Path)
	//	if err := osx.CleanupDirs(ctx, path); err != nil {
	//		return errorsx.Wrapf(err, "error deleting rubish data path: %s", path)
	//	}
	//
	//	return nil
	//}
	//createTemporaryRubishDataPathOnDisk := func() error {
	//	path := ltngdata.GetTemporaryRubishDataPath(info.Path)
	//	if err := os.MkdirAll(path, ltngdata.DBFileRW); err != nil {
	//		return errorsx.Wrapf(err, "error creating temporary rubish data path: %s", path)
	//	}
	//
	//	return nil
	//}
	//deleteTemporaryRubishDataPathOnDisk := func() error {
	//	path := ltngdata.GetTemporaryRubishDataPath(info.Path)
	//	if err := osx.CleanupDirs(ctx, path); err != nil {
	//		return errorsx.Wrapf(err, "error deleting temporary rubish data path: %s", path)
	//	}
	//
	//	return nil
	//}

	createRelationalDataPathOnDisk := func() error {
		path := ltngdata.GetRelationalDataPath(info.Path)
		if err := os.MkdirAll(path, ltngdata.DBFileRW); err != nil {
			return errorsx.Wrapf(err, "error creating relational data path: %s", path)
		}

		return nil
	}
	deleteRelationalDataPathOnDisk := func() error {
		path := ltngdata.GetRelationalDataPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting relational data path: %s", path)
		}

		return nil
	}
	createTemporaryRelationalDataPathOnDisk := func() error {
		path := ltngdata.GetTemporaryRelationalDataPath(info.Path)
		if err := os.MkdirAll(path, ltngdata.DBFileRW); err != nil {
			return errorsx.Wrapf(err, "error creating temporary relational data path: %s", path)
		}

		return nil
	}
	deleteTemporaryRelationalDataPathOnDisk := func() error {
		path := ltngdata.GetTemporaryRelationalDataPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting temporary relational data path: %s", path)
		}

		return nil
	}

	createIndexedDataPathOnDisk := func() error {
		path := ltngdata.GetIndexedDataPath(info.Path)
		if err := os.MkdirAll(path, ltngdata.DBFileRW); err != nil {
			return errorsx.Wrapf(err, "error creating indexed data path: %s", path)
		}

		return nil
	}
	deleteIndexedDataPathOnDisk := func() error {
		path := ltngdata.GetIndexedDataPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting indexed data path: %s", path)
		}

		return nil
	}
	createTemporaryIndexedDataPathOnDisk := func() error {
		path := ltngdata.GetTemporaryIndexedDataPath(info.Path)
		if err := os.MkdirAll(path, ltngdata.DBFileRW); err != nil {
			return errorsx.Wrapf(err, "error creating temporary indexed data path: %s", path)
		}

		return nil
	}
	deleteTemporaryIndexedDataPathOnDisk := func() error {
		path := ltngdata.GetTemporaryIndexedDataPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting temporary indexed data path: %s", path)
		}

		return nil
	}

	createIndexedListDataPathOnDisk := func() error {
		path := ltngdata.GetIndexedListDataPath(info.Path)
		if err := os.MkdirAll(path, ltngdata.DBFileRW); err != nil {
			return errorsx.Wrapf(err, "error creating indexed list data path: %s", path)
		}

		return nil
	}
	deleteIndexedListDataPathOnDisk := func() error {
		path := ltngdata.GetIndexedListDataPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error deleting indexed list data path: %s", path)
		}

		return nil
	}
	createTemporaryIndexedListDataPathOnDisk := func() error {
		path := ltngdata.GetTemporaryIndexedListDataPath(info.Path)
		if err := os.MkdirAll(path, ltngdata.DBFileRW); err != nil {
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
		//{
		//	Action: &saga.Action{
		//		Name:        "createRubishDataPathOnDisk",
		//		Do:          createRubishDataPathOnDisk,
		//		RetrialOpts: saga.DefaultRetrialOps,
		//	},
		//	Rollback: &saga.Rollback{
		//		Name:        "deleteRubishDataPathOnDisk",
		//		Do:          deleteRubishDataPathOnDisk,
		//		RetrialOpts: saga.DefaultRetrialOps,
		//	},
		//},
		//{
		//	Action: &saga.Action{
		//		Name:        "createTemporaryRubishDataPathOnDisk",
		//		Do:          createTemporaryRubishDataPathOnDisk,
		//		RetrialOpts: saga.DefaultRetrialOps,
		//	},
		//	Rollback: &saga.Rollback{
		//		Name:        "deleteTemporaryRubishDataPathOnDisk",
		//		Do:          deleteTemporaryRubishDataPathOnDisk,
		//		RetrialOpts: saga.DefaultRetrialOps,
		//	},
		//},
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

func (e *LTNGEngine) removeEmptyStatsPathsFromDisk(
	ctx context.Context,
	info *ltngdata.StoreInfo,
) error {
	if err := osx.CleanupEmptyDirs(ctx, ltngdata.GetStatsPath(info.Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up stats path", "path", info.Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdata.GetTemporaryStatsPath(info.Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up stats path", "path", info.Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdata.GetRelationalStatsPath(info.Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up stats path", "path", info.Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdata.GetTemporaryRelationalStatsPath(info.Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up stats path", "path", info.Path, "error", err)
	}

	return nil
}

func (e *LTNGEngine) removeEmptyDataPathsFromDisk(
	ctx context.Context,
	info *ltngdata.StoreInfo,
) error {
	if err := osx.CleanupEmptyDirs(ctx, ltngdata.GetDataPath(info.Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up data path", info.Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdata.GetTemporaryDataPath(info.Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up tmp data path", info.Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdata.GetIndexedDataPath(info.Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up indexed data path", info.Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdata.GetTemporaryIndexedDataPath(info.Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up tmp data indexed path", info.Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdata.GetIndexedListDataPath(info.Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up indexed list data path", info.Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdata.GetTemporaryIndexedListDataPath(info.Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up tmp data indexed list path", info.Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdata.GetRelationalDataPath(info.Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up relational data path", info.Path, "error", err)
	}

	if err := osx.CleanupEmptyDirs(ctx, ltngdata.GetTemporaryRelationalDataPath(info.Path)); err != nil {
		e.logger.Error(ctx, "error cleaning up tmp relational data path", info.Path, "error", err)
	}

	return nil
}

// ################################################################################################################## \\

func (e *LTNGEngine) createFullStoreOnDisk(
	ctx context.Context,
	info *ltngdata.StoreInfo,
) error {
	return saga.NewListOperator(e.buildCreateStoreOperations(ctx, info)...).Operate()
}

func (e *LTNGEngine) buildCreateStoreOperations(
	ctx context.Context,
	info *ltngdata.StoreInfo,
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

	createStoreOnDisk := func() error {
		_, err := e.createStoreOnDisk(ctx, info)
		if err != nil {
			return err
		}

		return nil
	}
	deleteStoreFromDisk := func() error {
		return os.Remove(ltngdata.GetStatsFilepath(info.Path, info.Name))
	}

	relationalStoreStatsUpdate := func() error {
		fi, err := e.createRelationalItemStore(ctx, info)
		if err != nil {
			return err
		}
		info = fi.FileData.Header.StoreInfo

		return e.insertRelationalStats(ctx, info)
	}
	relationalStoreDeletion := func() error {
		if err := os.Remove(ltngdata.GetRelationalStatsFilepath(info.Path, info.Name)); err != nil {
			e.logger.Error(ctx, "error cleaning up relational stats path", "path", info.Path, "error", err)
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
				Name:        "createStoreOnDisk",
				Do:          createStoreOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "deleteStoreFromDisk",
				Do:          deleteStoreFromDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
		{
			Action: &saga.Action{
				Name:        "relationalStoreStatsUpdate",
				Do:          relationalStoreStatsUpdate,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "relationalStoreDeletion",
				Do:          relationalStoreDeletion,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
	}
	return operations
}

func (e *LTNGEngine) deleteFullStoreFromDisk(
	ctx context.Context,
	info *ltngdata.StoreInfo,
) error {
	return saga.NewListOperator(e.buildDeletionStoreOperations(ctx, info)...).Operate()
}

func (e *LTNGEngine) buildDeletionStoreOperations(
	ctx context.Context,
	info *ltngdata.StoreInfo,
) []*saga.Operation {
	moveDataPathOnDisk := func() error {
		path := ltngdata.GetDataPath(info.Path)
		tmpPath := ltngdata.GetTemporaryDataPath(info.Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, path, tmpPath); err != nil {
			return errorsx.Wrapf(err, "error moving data files to tmp path: %s", path)
		}

		return nil
	}
	moveBackDataPathOnDisk := func() error {
		path := ltngdata.GetDataPath(info.Path)
		tmpPath := ltngdata.GetTemporaryDataPath(info.Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, tmpPath, path); err != nil {
			return errorsx.Wrapf(err, "error moving back data files from tmp path: %s", path)
		}

		return nil
	}

	moveIndexDataPathOnDisk := func() error {
		path := ltngdata.GetIndexedDataPath(info.Path)
		tmpPath := ltngdata.GetTemporaryIndexedDataPath(info.Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, path, tmpPath); err != nil {
			return errorsx.Wrapf(err, "error moving indexed data files to tmp path: %s", path)
		}

		return nil
	}
	moveBackIndexedDataPathOnDisk := func() error {
		path := ltngdata.GetIndexedDataPath(info.Path)
		tmpPath := ltngdata.GetTemporaryIndexedDataPath(info.Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, tmpPath, path); err != nil {
			return errorsx.Wrapf(err, "error moving back indexed data files to tmp path: %s", path)
		}

		return nil
	}

	moveIndexListDataPathOnDisk := func() error {
		path := ltngdata.GetIndexedListDataPath(info.Path)
		tmpPath := ltngdata.GetTemporaryIndexedListDataPath(info.Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, path, tmpPath); err != nil {
			return errorsx.Wrapf(err, "error moving indexed list data files to tmp path: %s", path)
		}

		return nil
	}
	moveBackIndexedListDataPathOnDisk := func() error {
		path := ltngdata.GetIndexedListDataPath(info.Path)
		tmpPath := ltngdata.GetTemporaryIndexedListDataPath(info.Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, tmpPath, path); err != nil {
			return errorsx.Wrapf(err, "error moving back indexed list data files to tmp path: %s", path)
		}

		return nil
	}

	moveRelationalDataPathOnDisk := func() error {
		path := ltngdata.GetRelationalDataPath(info.Path)
		tmpPath := ltngdata.GetTemporaryRelationalDataPath(info.Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, path, tmpPath); err != nil {
			return errorsx.Wrapf(err, "error moving indexed list data files to tmp path: %s", path)
		}

		return nil
	}
	moveBackRelationalDataPathOnDisk := func() error {
		path := ltngdata.GetRelationalDataPath(info.Path)
		tmpPath := ltngdata.GetTemporaryRelationalDataPath(info.Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, tmpPath, path); err != nil {
			return errorsx.Wrapf(err, "error moving back indexed list data files to tmp path: %s", path)
		}

		return nil
	}

	moveStatsPathOnDisk := func() error {
		path := ltngdata.GetStatsPath(info.Path)
		tmpPath := ltngdata.GetTemporaryStatsPath(info.Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, path, tmpPath); err != nil {
			return errorsx.Wrapf(err, "error moving stats files to tmp path: %s", path)
		}

		return nil
	}
	moveBackStatsPathOnDisk := func() error {
		path := ltngdata.GetStatsPath(info.Path)
		tmpPath := ltngdata.GetTemporaryStatsPath(info.Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, tmpPath, path); err != nil {
			return errorsx.Wrapf(err, "error moving back stats files from tmp path: %s", path)
		}

		return nil
	}

	moveRelationalStatsPathOnDisk := func() error {
		path := ltngdata.GetRelationalStatsPath(info.Path)
		tmpPath := ltngdata.GetTemporaryRelationalStatsPath(info.Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, path, tmpPath); err != nil {
			return errorsx.Wrapf(err, "error moving relational stats files to tmp path: %s", path)
		}

		return nil
	}
	moveBackRelationalStatsPathOnDisk := func() error {
		path := ltngdata.GetRelationalStatsPath(info.Path)
		tmpPath := ltngdata.GetTemporaryRelationalStatsPath(info.Path)
		if _, err := osx.MvOnlyFilesFromDirAsync(ctx, tmpPath, path); err != nil {
			return errorsx.Wrapf(err, "error moving back relational stats files from tmp path: %s", path)
		}

		return nil
	}

	cleanupDataPathsOnDisk := func() error {
		path := ltngdata.GetDataPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error cleaning up data path: %s", path)
		}

		path = ltngdata.GetIndexedDataPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error cleaning up indexed data path: %s", path)
		}

		path = ltngdata.GetIndexedListDataPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error cleaning up indexed list data path: %s", path)
		}

		path = ltngdata.GetRelationalDataPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error cleaning up relational data path: %s", path)
		}

		return nil
	}
	restoreDataPathsOnDisk := func() error {
		return e.createDataPathsOnDisk(ctx, info)
	}

	cleanupStatsPathsOnDisk := func() error {
		path := ltngdata.GetStatsPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error cleaning up stats path: %s", path)
		}

		path = ltngdata.GetRelationalStatsPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			return errorsx.Wrapf(err, "error cleaning up relational stats path: %s", path)
		}

		return nil
	}
	restoreStatsPathsOnDisk := func() error {
		return e.createStatsPathsOnDisk(ctx, info)
	}

	removeFromRelationalStatsFile := func() error {
		fi, err := e.loadRelationalStatsStoreFromMemoryOrDisk(ctx)
		if err != nil {
			return errorsx.Wrapf(err, "error creating %s relational store",
				ltngdata.DBManagerStoreInfo.RelationalInfo().Name)
		}

		return e.deleteFromRelationalStats(ctx, fi, fi.FileData, fi.FileData.Key)
	}
	restoreRelationalStatsFile := func() error {
		return nil
	}

	cleanupTemporaryDataPathsOnDisk := func() error {
		path := ltngdata.GetTemporaryDataPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			e.logger.Error(ctx, "error cleaning up temporary data path", "path", path)
		}

		path = ltngdata.GetTemporaryIndexedDataPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			e.logger.Error(ctx, "error cleaning up temporary indexed data path", "path", path)
		}

		path = ltngdata.GetTemporaryIndexedListDataPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			e.logger.Error(ctx, "error cleaning up temporary indexed list data path", "path", path)
		}

		path = ltngdata.GetTemporaryRelationalDataPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			e.logger.Error(ctx, "error cleaning up temporary relational data path", "path", path)
		}

		return nil
	}
	restoreTemporaryDataPathsOnDisk := func() error {
		return nil
	}

	cleanupTemporaryStatsPathsOnDisk := func() error {
		path := ltngdata.GetTemporaryStatsPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			e.logger.Error(ctx, "error cleaning up temporary data path", "path", path)
		}

		path = ltngdata.GetTemporaryRelationalStatsPath(info.Path)
		if err := osx.CleanupDirs(ctx, path); err != nil {
			e.logger.Error(ctx, "error cleaning up temporary indexed data path", "path", path)
		}

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
		{
			Action: &saga.Action{
				Name:        "moveRelationalStatsPathOnDisk",
				Do:          moveRelationalStatsPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
			Rollback: &saga.Rollback{
				Name:        "moveBackRelationalStatsPathOnDisk",
				Do:          moveBackRelationalStatsPathOnDisk,
				RetrialOpts: saga.DefaultRetrialOps,
			},
		},
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
