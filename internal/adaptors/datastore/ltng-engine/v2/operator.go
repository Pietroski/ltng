package v2

import (
	"context"
	"encoding/hex"
	"errors"
	"os"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
)

func (e *LTNGEngine) loadItem(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
	i, err := e.searchMemoryFirst(ctx, dbMetaInfo, item, opts)
	if err == nil {
		return i, nil
	} else if errors.Is(err, itemMarkedAsDeletedErr) {
		return nil, err
	}

	if !opts.HasIdx {
		return e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, item)
	}

	switch opts.IndexProperties.IndexSearchPattern {
	case ltngdata.AndComputational:
		return e.andComputationalSearch(ctx, opts, dbMetaInfo)
	case ltngdata.OrComputational:
		return e.orComputationalSearch(ctx, opts, dbMetaInfo)
	case ltngdata.One:
		fallthrough
	default:
		return e.straightSearch(ctx, opts, dbMetaInfo)
	}
}

func (e *LTNGEngine) createItem(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
	strItemKey := hex.EncodeToString(item.Key)

	if _, err := e.memoryStore.LoadItem(ctx, dbMetaInfo, item, opts); err != nil {
		if _, err = os.Stat(ltngdata.GetDataFilepath(dbMetaInfo.Path, strItemKey)); !os.IsNotExist(err) {
			return nil, errorsx.Wrapf(err, "file already exist: %s", dbMetaInfo.Path)
		}
	} else {
		return nil, errorsx.New("error item already exists")
	}

	itemInfoData := &ltngdata.ItemInfoData{
		OpNatureType: ltngdata.OpNatureTypeItem,
		OpType:       ltngdata.OpTypeCreate,
		DBMetaInfo:   dbMetaInfo,
		Item:         item,
		Opts:         opts,
	}
	if err := e.fq.WriteOnCursor(ctx, itemInfoData); err != nil {
		return nil, err
	}
	e.opSaga.crudChannels.OpSagaChannel.QueueChannel.Send(struct{}{})
	_, _ = e.memoryStore.CreateItem(ctx, dbMetaInfo, item, opts)

	return item, nil
}

func (e *LTNGEngine) upsertItem(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
	itemInfoData := &ltngdata.ItemInfoData{
		OpNatureType: ltngdata.OpNatureTypeItem,
		OpType:       ltngdata.OpTypeUpsert,
		DBMetaInfo:   dbMetaInfo,
		Item:         item,
		Opts:         opts,
	}
	if err := e.fq.WriteOnCursor(ctx, itemInfoData); err != nil {
		return nil, err
	}
	e.opSaga.crudChannels.OpSagaChannel.QueueChannel.Send(struct{}{})
	_, _ = e.memoryStore.UpsertItem(ctx, dbMetaInfo, item, opts)

	return item, nil
}

func (e *LTNGEngine) deleteItem(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
	itemInfoData := &ltngdata.ItemInfoData{
		OpNatureType: ltngdata.OpNatureTypeItem,
		OpType:       ltngdata.OpTypeDelete,
		DBMetaInfo:   dbMetaInfo,
		Item:         item,
		Opts:         opts,
	}
	if err := e.fq.WriteOnCursor(ctx, itemInfoData); err != nil {
		return nil, err
	}
	e.opSaga.crudChannels.OpSagaChannel.QueueChannel.Send(struct{}{})
	if opts.IndexProperties.IndexDeletionBehaviour != ltngdata.IndexOnly {
		e.markedAsDeletedMapping.Set(dbMetaInfo.LockName(hex.EncodeToString(item.Key)), struct{}{})
	} else if opts.IndexProperties.IndexDeletionBehaviour == ltngdata.IndexOnly {
		for _, indexKey := range opts.IndexingKeys {
			e.markedAsDeletedMapping.Set(dbMetaInfo.IndexInfo().LockName(hex.EncodeToString(indexKey)), struct{}{})
		}
	}
	_, _ = e.memoryStore.DeleteItem(ctx, dbMetaInfo, item, opts)

	return item, nil
}

func (e *LTNGEngine) listItems(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	pagination *ltngdata.Pagination,
	opts *ltngdata.IndexOpts,
) (*ltngdata.ListItemsResult, error) {
	relationalItemStore := dbMetaInfo.RelationalInfo()
	lockKey := relationalItemStore.LockName(ltngdata.RelationalDataStoreKey)
	e.kvLock.Lock(lockKey, struct{}{})
	defer e.kvLock.Unlock(lockKey)

	if i, err := e.memoryStore.ListItems(
		ctx, dbMetaInfo, pagination, opts,
	); err == nil && i != nil {
		return i, nil
	}

	switch opts.IndexProperties.ListSearchPattern {
	case ltngdata.All:
		return e.listAllItems(ctx, dbMetaInfo)
	case ltngdata.IndexingList:
		itemList, err := e.loadIndexingList(ctx, dbMetaInfo, opts)
		if err != nil {
			return nil, err
		}

		return &ltngdata.ListItemsResult{Items: itemList}, nil
	case ltngdata.Default:
		fallthrough
	default:
		return e.listPaginatedItems(ctx, dbMetaInfo, pagination)
	}
}

// LWSCU
// LOCK
// WRITE -- persist to process later
// SIGNAL
// CACHE
// UNLOCK
