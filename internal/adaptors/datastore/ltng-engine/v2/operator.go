package v2

import (
	"context"
	"encoding/hex"
	"errors"
	"os"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
)

func (e *LTNGEngine) loadItem(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	i, err := e.searchMemoryFirst(ctx, dbMetaInfo, item, opts)
	if err == nil {
		return i, nil
	} else if errors.Is(err, itemMarkedAsDeletedErr) {
		return nil, err
	}

	if !opts.HasIdx {
		return e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, item, true)
	}

	switch opts.IndexProperties.IndexSearchPattern {
	case ltngenginemodels.AndComputational:
		return e.andComputationalSearch(ctx, opts, dbMetaInfo)
	case ltngenginemodels.OrComputational:
		return e.orComputationalSearch(ctx, opts, dbMetaInfo)
	case ltngenginemodels.One:
		fallthrough
	default:
		return e.straightSearch(ctx, opts, dbMetaInfo)
	}
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
			return nil, errorsx.Wrapf(err, "file already exist: %s", dbMetaInfo.Path)
		}
	} else {
		return nil, errorsx.New("error item already exists")
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
	if opts.IndexProperties.IndexDeletionBehaviour != ltngenginemodels.IndexOnly {
		e.markedAsDeletedMapping.Set(dbMetaInfo.LockName(hex.EncodeToString(item.Key)), struct{}{})
	} else if opts.IndexProperties.IndexDeletionBehaviour == ltngenginemodels.IndexOnly {
		for _, indexKey := range opts.IndexingKeys {
			e.markedAsDeletedMapping.Set(dbMetaInfo.IndexInfo().LockName(hex.EncodeToString(indexKey)), struct{}{})
		}
	}
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
	e.kvLock.Lock(lockKey, struct{}{})
	defer e.kvLock.Unlock(lockKey)

	if i, err := e.memoryStore.ListItems(
		ctx, dbMetaInfo, pagination, opts,
	); err == nil && i != nil {
		return i, nil
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

// LWSCU
// LOCK
// WRITE -- persist to process later
// SIGNAL
// CACHE
// UNLOCK
