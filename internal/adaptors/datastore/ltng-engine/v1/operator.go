package v1

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"

	ltng_engine_models "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	lo "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/list-operator"
)

func (e *LTNGEngine) loadItem(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
	opts *IndexOpts,
) (*Item, error) {
	lockKey := dbMetaInfo.LockName(string(item.Key))

	e.opMtx.Lock(lockKey, struct{}{})
	defer e.opMtx.Unlock(lockKey)

	if opts == nil {
		return nil, nil
	}

	if !opts.HasIdx {
		return e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, item, true)
	}

	switch opts.IndexProperties.IndexSearchPattern {
	case AndComputational:
		return e.andComputationalSearch(ctx, opts, dbMetaInfo)
	case OrComputational:
		return e.orComputationalSearch(ctx, opts, dbMetaInfo)
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
) (*Item, error) {
	strItemKey := hex.EncodeToString(item.Key)

	lockKey := dbMetaInfo.LockName(strItemKey)
	e.opMtx.Lock(lockKey, struct{}{})
	defer e.opMtx.Unlock(lockKey)

	if _, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, item, true); err == nil {
		return nil, fmt.Errorf("error item already exists")
	}

	if !opts.HasIdx {
		return nil, e.createItemOnDisk(ctx, dbMetaInfo, item)
	}

	//createItemOnDisk := func() error {
	//	return e.createItemOnDisk(ctx, dbMetaInfo, item)
	//}
	//deleteItemOnDisk := func() error {
	//	return os.Remove(getDataFilepath(dbMetaInfo.Path, strItemKey))
	//}
	//
	//createIndexedItemOnDisk := func() error {
	//	for _, indexKey := range opts.IndexingKeys {
	//		if err := e.createIndexItemOnDisk(ctx, dbMetaInfo.IndexInfo(), &Item{
	//			Key:   indexKey,
	//			Value: opts.ParentKey,
	//		}); err != nil {
	//			return err
	//		}
	//	}
	//
	//	return nil
	//}
	//deleteIndexedItemOnDisk := func() error {
	//	for _, indexKey := range opts.IndexingKeys {
	//		if err := os.Remove(getDataFilepath(dbMetaInfo.IndexInfo().Path, string(indexKey))); err != nil {
	//			return fmt.Errorf("error deleting item on database: %w", err)
	//		}
	//	}
	//
	//	return nil
	//}
	//
	//createIndexedItemListOnDisk := func() error {
	//	return e.createIndexItemOnDisk(ctx,
	//		dbMetaInfo.IndexListInfo(),
	//		&Item{
	//			Key:   opts.ParentKey,
	//			Value: bytes.Join(opts.IndexingKeys, []byte(bytesSep)),
	//		},
	//	)
	//}
	//
	//operations := []*lo.Operation{
	//	{
	//		Action: &lo.Action{
	//			Act:         createItemOnDisk,
	//			RetrialOpts: lo.DefaultRetrialOps,
	//		},
	//	},
	//	{
	//		Action: &lo.Action{
	//			Act:         createIndexedItemOnDisk,
	//			RetrialOpts: lo.DefaultRetrialOps,
	//		},
	//		Rollback: &lo.RollbackAction{
	//			RollbackAct: deleteItemOnDisk,
	//			RetrialOpts: lo.DefaultRetrialOps,
	//		},
	//	},
	//	{
	//		Action: &lo.Action{
	//			Act:         createIndexedItemListOnDisk,
	//			RetrialOpts: lo.DefaultRetrialOps,
	//		},
	//		Rollback: &lo.RollbackAction{
	//			RollbackAct: deleteIndexedItemOnDisk,
	//			RetrialOpts: lo.DefaultRetrialOps,
	//		},
	//	},
	//}
	//if err := lo.New(operations...).Operate(); err != nil {
	//	return nil, err
	//}

	cs := newCreateSaga(e)
	if err := lo.New([]*lo.Operation{
		{
			Action: &lo.Action{
				Act:         cs.createItemOnDisk(ctx, dbMetaInfo, item),
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         cs.createIndexedItemOnDisk(ctx, dbMetaInfo, opts),
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: cs.deleteItemOnDisk(dbMetaInfo.Path, strItemKey),
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
		{
			Action: &lo.Action{
				Act:         cs.createIndexedItemListOnDisk(ctx, dbMetaInfo, opts),
				RetrialOpts: lo.DefaultRetrialOps,
			},
			Rollback: &lo.RollbackAction{
				RollbackAct: cs.deleteIndexedItemOnDisk(dbMetaInfo, opts),
				RetrialOpts: lo.DefaultRetrialOps,
			},
		},
	}...).Operate(); err != nil {
		return nil, err
	}

	//err := e.fq.Write(ctx, item)
	//if err != nil {
	//	return nil, err
	//}

	return item, nil
}

func (e *LTNGEngine) upsertItem(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
	opts *IndexOpts,
) (*Item, error) {
	strItemKey := hex.EncodeToString(item.Key)

	lockKey := dbMetaInfo.LockName(strItemKey)
	e.opMtx.Lock(lockKey, struct{}{})
	defer e.opMtx.Unlock(lockKey)

	//if _, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, item, true); err == nil {
	//	return nil, fmt.Errorf("error item already exists")
	//}

	if !opts.HasIdx {
		return nil, e.upsertItemOnDisk(ctx, dbMetaInfo, item)
	}

	createItemOnDisk := func() error {
		return e.upsertItemOnDisk(ctx, dbMetaInfo, item)
	}
	deleteItemOnDisk := func() error {
		return os.Remove(getDataFilepath(dbMetaInfo.Path, strItemKey))
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

	return item, nil
}

func (e *LTNGEngine) deleteItem(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
	opts *IndexOpts,
) (*Item, error) {
	strItemKey := hex.EncodeToString(item.Key)

	lockKey := dbMetaInfo.LockName(strItemKey)
	e.opMtx.Lock(lockKey, struct{}{})
	defer e.opMtx.Unlock(lockKey)

	switch opts.IndexProperties.IndexDeletionBehaviour {
	case Cascade:
		return nil, e.deleteCascade(ctx, dbMetaInfo, item.Key)
	case CascadeByIdx:
		return nil, e.deleteCascadeByIdx(ctx, dbMetaInfo, item.Key)
	case IndexOnly:
		return nil, e.deleteIndexOnly(ctx, dbMetaInfo, item.Key)
	case None:
		fallthrough
	default:
		return nil, fmt.Errorf("invalid index deletion behaviour")
	}
}

func (e *LTNGEngine) listItems(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	pagination *ltng_engine_models.Pagination,
	opts *IndexOpts,
) (*ListItemsResult, error) {
	relationalItemStore := dbMetaInfo.RelationalInfo()
	lockKey := relationalItemStore.LockName(listingItemsFromStore)

	e.opMtx.Lock(lockKey, struct{}{})
	defer e.opMtx.Unlock(lockKey)

	if opts == nil {
		return nil, nil
	}

	switch opts.IndexProperties.ListSearchPattern {
	case All:
		return e.listAllItems(ctx, dbMetaInfo)
	case IndexingList:
		itemList, err := e.loadIndexingList(ctx, dbMetaInfo, opts)
		if err != nil {
			return nil, err
		}

		return &ListItemsResult{Items: itemList}, nil
	case Default:
		fallthrough
	default:
		return e.listPaginatedItems(ctx, dbMetaInfo, pagination)
	}
}
