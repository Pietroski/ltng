package v2

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
)

func (e *LTNGEngine) loadItem(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	//return e.caching.LoadItem(ctx, dbMetaInfo, item, opts)

	strItemKey := hex.EncodeToString(item.Key)
	lockKey := dbMetaInfo.LockName(strItemKey)
	e.opMtx.Lock(lockKey, struct{}{})
	defer e.opMtx.Unlock(lockKey)

	if i, err := e.memoryStore.LoadItem(
		ctx, dbMetaInfo, item, opts,
	); err == nil && i != nil {
		return i, nil
	}

	return nil, fmt.Errorf("error item not found")

	//strItemKey := hex.EncodeToString(item.Key)
	//lockKey := dbMetaInfo.LockName(strItemKey)
	//e.opMtx.Lock(lockKey, struct{}{})
	//defer e.opMtx.Unlock(lockKey)
	//
	//if opts == nil {
	//	return nil, nil
	//}
	//
	//if i, err := e.memoryStore.LoadItem(ctx, dbMetaInfo, item, opts); err == nil && i != nil {
	//	return i, nil
	//}
	//
	//if !opts.HasIdx {
	//	return e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, item, true)
	//}
	//
	//switch opts.IndexProperties.IndexSearchPattern {
	//case ltngenginemodels.AndComputational:
	//	return e.andComputationalSearch(ctx, opts, dbMetaInfo)
	//case ltngenginemodels.OrComputational:
	//	return e.orComputationalSearch(ctx, opts, dbMetaInfo)
	//case ltngenginemodels.One:
	//	fallthrough
	//default:
	//	return e.straightSearch(ctx, opts, dbMetaInfo)
	//}
}

func (e *LTNGEngine) createItem(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	strItemKey := hex.EncodeToString(item.Key)

	if _, err := e.memoryStore.LoadItem(ctx, dbMetaInfo, item, opts); err != nil {
		if _, err = os.Stat(ltngenginemodels.GetDataFilepath(dbMetaInfo.Path, strItemKey)); !os.IsNotExist(err) {
			return nil, fmt.Errorf("file already exist: %s: %v", dbMetaInfo.Path, err)
		}
	} else {
		return nil, fmt.Errorf("error item already exists")
	}

	itemInfoData := &ltngenginemodels.ItemInfoData{
		OpNatureType: ltngenginemodels.OpNatureTypeItem,
		OpType:       ltngenginemodels.OpTypeCreate,
		DBMetaInfo:   dbMetaInfo,
		Item:         item,
		Opts:         opts,
	}
	if err := e.fq.WriteOnCursor(ctx, itemInfoData); err != nil {
		return nil, err
	}
	e.opSaga.crudChannels.OpSagaChannel.QueueChannel <- struct{}{}
	_, _ = e.memoryStore.CreateItem(ctx, dbMetaInfo, item, opts)

	return item, nil
}

func (e *LTNGEngine) upsertItem(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	itemInfoData := &ltngenginemodels.ItemInfoData{
		OpNatureType: ltngenginemodels.OpNatureTypeItem,
		OpType:       ltngenginemodels.OpTypeUpsert,
		DBMetaInfo:   dbMetaInfo,
		Item:         item,
		Opts:         opts,
	}
	if err := e.fq.WriteOnCursor(ctx, itemInfoData); err != nil {
		return nil, err
	}
	e.opSaga.crudChannels.OpSagaChannel.QueueChannel <- struct{}{}
	_, _ = e.memoryStore.UpsertItem(ctx, dbMetaInfo, item, opts)

	return item, nil
}

func (e *LTNGEngine) deleteItem(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	//strItemKey := hex.EncodeToString(item.Key)
	//
	//lockKey := dbMetaInfo.LockName(strItemKey)
	//e.opMtx.Lock(lockKey, struct{}{})
	//defer e.opMtx.Unlock(lockKey)
	//
	//switch opts.IndexProperties.IndexDeletionBehaviour {
	//case ltngenginemodels.Cascade:
	//	return nil, e.deleteCascade(ctx, dbMetaInfo, item.Key)
	//case ltngenginemodels.CascadeByIdx:
	//	return nil, e.deleteCascadeByIdx(ctx, dbMetaInfo, item.Key)
	//case ltngenginemodels.IndexOnly:
	//	return nil, e.deleteIndexOnly(ctx, dbMetaInfo, item.Key)
	//case ltngenginemodels.None:
	//	fallthrough
	//default:
	//	return nil, fmt.Errorf("invalid index deletion behaviour")
	//}

	strItemKey := hex.EncodeToString(item.Key)

	if _, err := e.memoryStore.LoadItem(ctx, dbMetaInfo, item, opts); err != nil {
		if _, err = os.Stat(ltngenginemodels.GetDataFilepath(dbMetaInfo.Path, strItemKey)); os.IsNotExist(err) {
			return nil, fmt.Errorf("file does not exist: %s: %v", dbMetaInfo.Path, err)
		}
	}

	itemInfoData := &ltngenginemodels.ItemInfoData{
		OpNatureType: ltngenginemodels.OpNatureTypeItem,
		OpType:       ltngenginemodels.OpTypeDelete,
		DBMetaInfo:   dbMetaInfo,
		Item:         item,
		Opts:         opts,
	}
	if err := e.fq.WriteOnCursor(ctx, itemInfoData); err != nil {
		return nil, err
	}
	e.opSaga.crudChannels.OpSagaChannel.QueueChannel <- struct{}{}
	_, _ = e.memoryStore.DeleteItem(ctx, dbMetaInfo, item, opts)

	return item, nil
}

func (e *LTNGEngine) listItems(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	pagination *ltngenginemodels.Pagination,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.ListItemsResult, error) {
	relationalItemStore := dbMetaInfo.RelationalInfo()
	lockKey := relationalItemStore.LockName(ltngenginemodels.RelationalDataStore)

	e.opMtx.Lock(lockKey, struct{}{})
	defer e.opMtx.Unlock(lockKey)

	if opts == nil {
		return nil, nil
	}

	switch opts.IndexProperties.ListSearchPattern {
	case ltngenginemodels.All:
		return e.listAllItems(ctx, dbMetaInfo)
	case ltngenginemodels.IndexingList:
		itemList, err := e.loadIndexingList(ctx, dbMetaInfo, opts)
		if err != nil {
			return nil, err
		}

		return &ltngenginemodels.ListItemsResult{Items: itemList}, nil
	case ltngenginemodels.Default:
		fallthrough
	default:
		return e.listPaginatedItems(ctx, dbMetaInfo, pagination)
	}
}

// LWCSU
// LOCK
// WRITE
// CACHE
// SIGNAL
// UNLOCK
