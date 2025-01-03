package memorystorev1

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/bytesop"
)

func (ltng *LTNGCacheEngine) createItem(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	key := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.Name), item.Key},
		[]byte(ltngenginemodels.BytesSliceSep),
	)
	strKey := hex.EncodeToString(key)
	if err := ltng.cache.Set(ctx, strKey, item.Value); err != nil {
		return nil, err
	}

	if opts == nil || !opts.HasIdx {
		return item, nil
	}

	for _, itemKey := range opts.IndexingKeys {
		indexKey := bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), itemKey},
			[]byte(ltngenginemodels.BytesSliceSep),
		)
		strIndexKey := hex.EncodeToString(indexKey)
		if err := ltng.cache.Set(ctx, strIndexKey, opts.ParentKey); err != nil {
			return nil, err
		}
	}

	indexListKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.IndexListInfo().Name), opts.ParentKey},
		[]byte(ltngenginemodels.BytesSliceSep),
	)
	strIndexListKey := hex.EncodeToString(indexListKey)
	if err := ltng.cache.Set(ctx, strIndexListKey, opts.IndexingKeys); err != nil {
		return nil, err
	}

	relationalKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.RelationalInfo().Name)},
		[]byte(ltngenginemodels.BytesSliceSep),
	)
	strRelationalKey := hex.EncodeToString(relationalKey)
	var value []*ltngenginemodels.Item
	if err := ltng.cache.Get(ctx, strRelationalKey, &value, func() (interface{}, error) {
		return []*ltngenginemodels.Item{}, nil
	}); err != nil {
		return nil, err
	}
	value = append(value, &ltngenginemodels.Item{
		Key:   item.Key,
		Value: item.Value,
	})
	if err := ltng.cache.Set(ctx, strRelationalKey, value); err != nil {
		return nil, err
	}

	return item, nil
}

func (ltng *LTNGCacheEngine) upsertItem(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	key := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.Name), item.Key},
		[]byte(ltngenginemodels.BytesSliceSep),
	)
	strKey := hex.EncodeToString(key)
	if err := ltng.cache.Set(ctx, strKey, item.Value); err != nil {
		return nil, err
	}

	if opts == nil || !opts.HasIdx {
		return item, nil
	}

	indexListKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.IndexListInfo().Name), opts.ParentKey},
		[]byte(ltngenginemodels.BytesSliceSep),
	)
	strIndexListKey := hex.EncodeToString(indexListKey)
	if err := ltng.cache.Set(ctx, strIndexListKey, opts.IndexingKeys); err != nil {
		return nil, err
	}

	var indexingKeys [][]byte
	_ = ltng.cache.Get(ctx, strIndexListKey, &indexingKeys, func() (interface{}, error) {
		return []*ltngenginemodels.Item{}, nil
	})

	keysToSave := bytesop.CalRightDiff(indexingKeys, opts.IndexingKeys)
	for _, itemKey := range keysToSave {
		indexKey := bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), itemKey},
			[]byte(ltngenginemodels.BytesSliceSep),
		)
		strIndexKey := hex.EncodeToString(indexKey)
		if err := ltng.cache.Set(ctx, strIndexKey, opts.ParentKey); err != nil {
			return nil, err
		}
	}

	keysToDelete := bytesop.CalRightDiff(opts.IndexingKeys, indexingKeys)
	for _, itemKey := range keysToDelete {
		indexKey := bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), itemKey},
			[]byte(ltngenginemodels.BytesSliceSep),
		)
		strIndexKey := hex.EncodeToString(indexKey)
		if err := ltng.cache.Del(ctx, strIndexKey); err != nil {
			return nil, err
		}
	}

	relationalKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.RelationalInfo().Name)},
		[]byte(ltngenginemodels.BytesSliceSep),
	)
	strRelationalKey := hex.EncodeToString(relationalKey)
	var value []*ltngenginemodels.Item
	_ = ltng.cache.Get(ctx, strRelationalKey, &value, func() (interface{}, error) {
		return []*ltngenginemodels.Item{}, nil
	})
	value = append(value, &ltngenginemodels.Item{
		Key:   key,
		Value: item.Value,
	})
	if err := ltng.cache.Set(ctx, strRelationalKey, value); err != nil {
		return nil, err
	}

	return item, nil
}

func (ltng *LTNGCacheEngine) deleteItem(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	switch opts.IndexProperties.IndexDeletionBehaviour {
	case ltngenginemodels.Cascade:
		return ltng.deleteOnCascade(ctx, dbMetaInfo, item, opts)
	case ltngenginemodels.CascadeByIdx:
		return ltng.deleteOnCascadeByIdx(ctx, dbMetaInfo, item, opts)
	case ltngenginemodels.IndexOnly:
		return ltng.deleteIdxOnly(ctx, dbMetaInfo, item, opts)
	case ltngenginemodels.None:
		fallthrough
	default:
		return nil, fmt.Errorf("invalid index deletion behaviour")
	}
}

func (ltng *LTNGCacheEngine) loadItem(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	if opts == nil {
		return nil, fmt.Errorf("invalid index deletion behaviour")
	}

	if !opts.HasIdx {
		key := bytes.Join(
			[][]byte{[]byte(dbMetaInfo.Name), item.Key},
			[]byte(ltngenginemodels.BytesSliceSep),
		)
		strKey := hex.EncodeToString(key)

		var value []byte
		if err := ltng.cache.Get(ctx, strKey, &value, nil); err != nil {
			return nil, err
		}

		return &ltngenginemodels.Item{
			Key:   item.Key,
			Value: value,
		}, nil
	}

	switch opts.IndexProperties.IndexSearchPattern {
	case ltngenginemodels.AndComputational:
		return ltng.andComputationalSearch(ctx, dbMetaInfo, item, opts)
	case ltngenginemodels.OrComputational:
		return ltng.orComputationalSearch(ctx, dbMetaInfo, item, opts)
	case ltngenginemodels.One:
		fallthrough
	default:
		return ltng.straightSearch(ctx, dbMetaInfo, item, opts)
	}
}

func (ltng *LTNGCacheEngine) listItems(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	pagination *ltngenginemodels.Pagination,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.ListItemsResult, error) {
	if !pagination.IsValid() {
		return nil, fmt.Errorf("invalid pagination")
	}

	relationalKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.RelationalInfo().Name)},
		[]byte(ltngenginemodels.BytesSliceSep),
	)
	strRelationalKey := hex.EncodeToString(relationalKey)
	var value []*ltngenginemodels.Item
	if err := ltng.cache.Get(ctx, strRelationalKey, &value, func() (interface{}, error) {
		return []*ltngenginemodels.Item{}, nil
	}); err != nil {
		return nil, err
	}

	return &ltngenginemodels.ListItemsResult{
		Pagination: pagination.CalcNextPage(uint64(len(value))),
		Items: (*ltngenginemodels.ItemList)(&value).
			GetItemsFromPagination(pagination),
	}, nil
}
