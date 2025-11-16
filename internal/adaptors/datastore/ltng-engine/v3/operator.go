package v3

import (
	"context"
	"encoding/hex"
	"errors"
	"os"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"

	v4 "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngdb/v3"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
)

func (e *LTNGEngine) loadItem(
	ctx context.Context,
	dbMetaInfo *v4.ManagerStoreMetaInfo,
	item *v4.Item,
	opts *v4.IndexOpts,
) (*v4.Item, error) {
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
	case v4.AndComputational:
		return e.andComputationalSearch(ctx, opts, dbMetaInfo)
	case v4.OrComputational:
		return e.orComputationalSearch(ctx, opts, dbMetaInfo)
	case v4.One:
		fallthrough
	default:
		return e.straightSearch(ctx, opts, dbMetaInfo)
	}
}

func (e *LTNGEngine) createItem(
	ctx context.Context,
	dbMetaInfo *v4.ManagerStoreMetaInfo,
	item *v4.Item,
	opts *v4.IndexOpts,
) (*v4.Item, error) {
	strItemKey := hex.EncodeToString(item.Key)

	if _, err := e.memoryStore.LoadItem(ctx, dbMetaInfo, item, opts); err != nil {
		if _, err = os.Stat(v4.GetDataFilepath(dbMetaInfo.Path, strItemKey)); !os.IsNotExist(err) {
			return nil, errorsx.Wrapf(err, "file already exist: %s", dbMetaInfo.Path)
		}
	} else {
		return nil, errorsx.New("error item already exists")
	}

	traceID, err := e.getTracerID(ctx)
	if err != nil {
		return nil, errorsx.Wrap(err, "error getting tracer ID")
	}

	itemInfoData := &v4.ItemInfoData{
		TraceID: traceID,

		OpNatureType: v4.OpNatureTypeItem,
		OpType:       v4.OpTypeCreate,
		DBMetaInfo:   dbMetaInfo,
		Item:         item,
		Opts:         opts,
	}

	if _, err = e.fq.Write(itemInfoData); err != nil {
		return nil, err
	}
	_, _ = e.memoryStore.CreateItem(ctx, dbMetaInfo, item, opts)

	return item, nil
}

func (e *LTNGEngine) upsertItem(
	ctx context.Context,
	dbMetaInfo *v4.ManagerStoreMetaInfo,
	item *v4.Item,
	opts *v4.IndexOpts,
) (*v4.Item, error) {
	traceID, err := e.getTracerID(ctx)
	if err != nil {
		return nil, errorsx.Wrap(err, "error getting tracer ID")
	}

	itemInfoData := &v4.ItemInfoData{
		TraceID: traceID,

		OpNatureType: v4.OpNatureTypeItem,
		OpType:       v4.OpTypeUpsert,
		DBMetaInfo:   dbMetaInfo,
		Item:         item,
		Opts:         opts,
	}

	if _, err = e.fq.Write(itemInfoData); err != nil {
		return nil, err
	}
	_, _ = e.memoryStore.UpsertItem(ctx, dbMetaInfo, item, opts)

	return item, nil
}

func (e *LTNGEngine) deleteItem(
	ctx context.Context,
	dbMetaInfo *v4.ManagerStoreMetaInfo,
	item *v4.Item,
	opts *v4.IndexOpts,
) (*v4.Item, error) {
	traceID, err := e.getTracerID(ctx)
	if err != nil {
		return nil, errorsx.Wrap(err, "error getting tracer ID")
	}

	itemInfoData := &v4.ItemInfoData{
		TraceID: traceID,

		OpNatureType: v4.OpNatureTypeItem,
		OpType:       v4.OpTypeDelete,
		DBMetaInfo:   dbMetaInfo,
		Item:         item,
		Opts:         opts,
	}

	if _, err = e.fq.Write(itemInfoData); err != nil {
		return nil, err
	}

	if opts.IndexProperties.IndexDeletionBehaviour != v4.IndexOnly {
		e.markedAsDeletedMapping.Set(dbMetaInfo.LockName(hex.EncodeToString(item.Key)), struct{}{})
	} else if opts.IndexProperties.IndexDeletionBehaviour == v4.IndexOnly {
		for _, indexKey := range opts.IndexingKeys {
			e.markedAsDeletedMapping.Set(dbMetaInfo.IndexInfo().LockName(hex.EncodeToString(indexKey)), struct{}{})
		}
	}
	_, _ = e.memoryStore.DeleteItem(ctx, dbMetaInfo, item, opts)

	return item, nil
}

func (e *LTNGEngine) listItems(
	ctx context.Context,
	dbMetaInfo *v4.ManagerStoreMetaInfo,
	pagination *ltngdata.Pagination,
	opts *v4.IndexOpts,
) (*v4.ListItemsResult, error) {
	lockKey := dbMetaInfo.RelationalInfo().LockName(v4.RelationalDataStoreKey)
	e.kvLock.Lock(lockKey, struct{}{})
	defer e.kvLock.Unlock(lockKey)

	if i, err := e.memoryStore.ListItems(
		ctx, dbMetaInfo, pagination, opts,
	); err == nil && i != nil {
		return i, nil
	}

	switch opts.IndexProperties.ListSearchPattern {
	case v4.All:
		return e.listAllItems(ctx, dbMetaInfo)
	case v4.IndexingList:
		itemList, err := e.loadIndexingList(ctx, dbMetaInfo, opts)
		if err != nil {
			return nil, err
		}

		return &v4.ListItemsResult{Items: itemList}, nil
	case v4.Default:
		fallthrough
	default:
		return e.listPaginatedItems(ctx, dbMetaInfo, pagination)
	}
}

// LWSCU
// LOCK
// WRITE -- persist to process later (write first, process later - WAL (write ahead logs))
// SIGNAL
// CACHE
// UNLOCK
//
// LWCU
// LOCK
// WRITE -- persist to process later (write first, process later - WAL (write ahead logs))
// CACHE
// UNLOCK
//
