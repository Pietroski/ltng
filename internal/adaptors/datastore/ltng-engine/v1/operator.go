package v1

import (
	"context"
	"fmt"
	"os"
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

	if opts == nil || len(opts.IndexingKeys) != 1 {
		err := fmt.Errorf("straightSearch requires index key list with length of 1")
		return nil, fmt.Errorf("invalid index payload size for giving option: %v", err)
	}

	if _, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, item); err == nil {
		return nil, fmt.Errorf("error item already exists")
	}

	if !opts.HasIdx {
		return nil, e.createItemOnDisk(ctx, dbMetaInfo, item, opts)
	}

	{
		// TODO:
	}

	return nil, nil
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

	if opts == nil || len(opts.IndexingKeys) != 1 {
		err := fmt.Errorf("straightSearch requires index key list with length of 1")
		return []byte{}, fmt.Errorf("invalid index payload size for giving option: %v", err)
	}

	if bs, err := e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, item); err == nil {
		return bs, nil
		//return nil, fmt.Errorf("error item already exists")
	}

	if !opts.HasIdx {
		return e.loadItemFromMemoryOrDisk(ctx, dbMetaInfo, item)
	}

	{
	}

	return nil, nil
}

func (e *LTNGEngine) upsertItemOnDisk(
	ctx context.Context,
	dbMetaInfo *ManagerStoreMetaInfo,
	item *Item,
	opts *IndexOpts,
) error {
	if err := os.MkdirAll(
		getDataPath(dbMetaInfo.Path), dbFilePerm,
	); err != nil {
		return err
	}

	return nil
}
