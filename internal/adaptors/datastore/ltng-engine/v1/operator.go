package v1

import (
	"bytes"
	"context"
	"fmt"
	"os"

	lo "gitlab.com/pietroski-software-company/lightning-db/lightning-node/go-lightning-node/pkg/tools/list-operator"
)

func (e *LTNGEngine) loadItem(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
	opts *IndexOpts,
) ([]byte, error) {
	lockKey := dbMetaInfo.LockName(string(item.Key))

	e.opMtx.Lock(lockKey, struct{}{})
	defer e.opMtx.Unlock(lockKey)

	if opts == nil || len(opts.IndexingKeys) != 1 {
		err := fmt.Errorf("load item requires index key list with length of 1")
		return []byte{}, fmt.Errorf("invalid index payload size for giving option: %w", err)
	}

	if !opts.HasIdx {
		return e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, item)
	}

	switch opts.IndexProperties.IndexSearchPattern {
	case AndComputational:
		return e.andComputationalSearch(ctx, opts, dbMetaInfo)
	case OrComputational:
		return e.orComputationalSearch(ctx, opts, dbMetaInfo)
	case IndexingList:
		indexingList, err := e.loadIndexingList(ctx, dbMetaInfo, opts)
		if err != nil {
			return nil, err
		}

		return bytes.Join(indexingList, []byte(bytesSep)), nil
	case One:
		fallthrough
	default:
		return e.straightSearch(ctx, opts, dbMetaInfo)
	}
}

func (e *LTNGEngine) createItem(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
	opts *IndexOpts,
) ([]byte, error) {
	lockKey := dbMetaInfo.LockName(string(item.Key))
	e.opMtx.Lock(lockKey, struct{}{})
	defer e.opMtx.Unlock(lockKey)

	if _, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, item); err == nil {
		return nil, fmt.Errorf("error item already exists")
	}

	if !opts.HasIdx {
		return nil, e.createItemOnDisk(ctx, dbMetaInfo, item)
	}

	strKey := string(item.Key)

	createItemOnDisk := func() error {
		return e.createItemOnDisk(ctx, dbMetaInfo, item)
	}
	deleteItemOnDisk := func() error {
		return os.Remove(getDataFilepath(dbMetaInfo.Path, strKey))
	}

	createIndexedItemOnDisk := func() error {
		for _, indexKey := range opts.IndexingKeys {
			if err := e.createIndexItemOnDisk(ctx, dbMetaInfo.IndexInfo(), &Item{
				Key:   indexKey,
				Value: opts.ParentKey,
			}); err != nil {
				return err
			}
		}

		return nil
	}
	deleteIndexedItemOnDisk := func() error {
		for _, indexKey := range opts.IndexingKeys {
			if err := os.Remove(getDataFilepath(dbMetaInfo.IndexInfo().Path, string(indexKey))); err != nil {
				return fmt.Errorf("error deleting item on database: %w", err)
			}
		}

		return nil
	}

	createIndexedItemListOnDisk := func() error {
		return e.createIndexItemOnDisk(ctx,
			dbMetaInfo.IndexListInfo(),
			&Item{
				Key:   opts.ParentKey,
				Value: bytes.Join(opts.IndexingKeys, []byte(bytesSep)),
			},
		)
	}
	//deleteIndexedItemListOnDisk := func() error {
	//	return os.Remove(getDataFilepath(dbMetaInfo.IndexListInfo().Path, strKey))
	//}

	operations := []*lo.Operation{
		{
			Action: &lo.Action{
				Act:         createItemOnDisk,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         createIndexedItemOnDisk,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: deleteItemOnDisk,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         createIndexedItemListOnDisk,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: deleteIndexedItemOnDisk,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
	}
	if err := lo.New(operations...).Operate(); err != nil {
		return nil, err
	}

	return item.Value, nil
}

func (e *LTNGEngine) upsertItem(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
	opts *IndexOpts,
) ([]byte, error) {
	lockKey := dbMetaInfo.LockName(string(item.Key))
	e.opMtx.Lock(lockKey, struct{}{})
	defer e.opMtx.Unlock(lockKey)

	if _, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, item); err == nil {
		return nil, fmt.Errorf("error item already exists")
	}

	if !opts.HasIdx {
		return nil, e.upsertItemOnDisk(ctx, dbMetaInfo, item)
	}

	strKey := string(item.Key)

	createItemOnDisk := func() error {
		return e.upsertItemOnDisk(ctx, dbMetaInfo, item)
	}
	deleteItemOnDisk := func() error {
		return os.Remove(getDataFilepath(dbMetaInfo.Path, strKey))
	}

	createIndexedItemOnDisk := func() error {
		for _, indexKey := range opts.IndexingKeys {
			if err := e.upsertIndexItemOnDisk(ctx, dbMetaInfo.IndexInfo(), &Item{
				Key:   indexKey,
				Value: opts.ParentKey,
			}); err != nil {
				return err
			}
		}

		return nil
	}
	deleteIndexedItemOnDisk := func() error {
		for _, indexKey := range opts.IndexingKeys {
			if err := os.Remove(getDataFilepath(dbMetaInfo.IndexInfo().Path, string(indexKey))); err != nil {
				return fmt.Errorf("error deleting item on database: %w", err)
			}
		}

		return nil
	}

	createIndexedItemListOnDisk := func() error {
		return e.upsertIndexItemOnDisk(ctx,
			dbMetaInfo.IndexListInfo(),
			&Item{
				Key:   opts.ParentKey,
				Value: bytes.Join(opts.IndexingKeys, []byte(bytesSep)),
			},
		)
	}

	operations := []*lo.Operation{
		{
			Action: &lo.Action{
				Act:         createItemOnDisk,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         createIndexedItemOnDisk,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: deleteItemOnDisk,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         createIndexedItemListOnDisk,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: deleteIndexedItemOnDisk,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
	}
	if err := lo.New(operations...).Operate(); err != nil {
		return nil, err
	}

	return item.Value, nil
}

func (e *LTNGEngine) deleteItem(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	key []byte,
) error {
	strItemKey := string(key)
	filePath := getDataFilepath(dbMetaInfo.Path, strItemKey)

	delPaths, err := e.createTmpDeletionPaths(ctx, dbMetaInfo)
	if err != nil {
		return err
	}

	moveItemForDeletion := func() error {
		if _, err = mvFileExec(ctx, filePath, getTmpDelDataFilePath(delPaths.tmpDelPath, strItemKey)); err != nil {
			return err
		}

		return nil
	}
	recreateDeletedItem := func() error {
		if _, err = mvFileExec(ctx, getTmpDelDataFilePath(delPaths.tmpDelPath, strItemKey), filePath); err != nil {
			return err
		}

		return nil
	}

	indexList, err := e.loadIndexingList(ctx, dbMetaInfo, &IndexOpts{ParentKey: key})
	if err != nil {
		return err
	}
	moveIndexesToTmpFile := func() error {
		for _, index := range indexList {
			strItemKey = string(index)

			if _, err = mvFileExec(ctx,
				getDataFilepath(dbMetaInfo.IndexInfo().Path, strItemKey),
				getTmpDelDataFilePath(delPaths.indexTmpDelPath, strItemKey),
			); err != nil {
				return err
			}
		}

		return nil
	}
	recreateIndexes := func() error {
		for _, index := range indexList {
			if err = e.createIndexItemOnDisk(ctx, dbMetaInfo, &Item{
				Key:   index,
				Value: key,
			}); err != nil {
				return err
			}
		}

		return nil
	}

	moveIndexListToTmpFile := func() error {
		if _, err = mvFileExec(ctx,
			getDataFilepath(dbMetaInfo.IndexListInfo().Path, strItemKey),
			getTmpDelDataFilePath(delPaths.indexListTmpDelPath, strItemKey),
		); err != nil {
			return err
		}

		return nil
	}
	recreateIndexListFromTmpFile := func() error {
		idxList := bytes.Join(indexList, []byte(bytesSep))
		return e.createItemOnDisk(ctx, dbMetaInfo, &Item{
			Key:   key,
			Value: idxList,
		})
	}

	deleteFromRelationalData := func() error {
		return e.deleteRelationalData(ctx, dbMetaInfo, key)
	}

	operations := []*lo.Operation{
		{
			Action: &lo.Action{
				Act:         moveItemForDeletion,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         moveIndexesToTmpFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: recreateDeletedItem,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         moveIndexListToTmpFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: recreateIndexes,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         deleteFromRelationalData,
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: recreateIndexListFromTmpFile,
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
	}
	if err = lo.New(operations...).Operate(); err != nil {
		return err
	}

	// deleteTmpFiles
	if _, err = delStoreDirsExec(ctx, dbTmpDelDataPath+sep+delPaths.tmpDelPath); err != nil {
		return err
	}

	return nil
}
