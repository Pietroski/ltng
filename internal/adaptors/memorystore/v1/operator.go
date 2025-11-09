package memorystorev1

import (
	"bytes"
	"context"
	"encoding/hex"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/bytesop"
	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
)

func (ltng *LTNGCacheEngine) createItem(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
	key := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.Name), item.Key},
		[]byte(ltngdata.BsSep),
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
			[]byte(ltngdata.BsSep),
		)
		strIndexKey := hex.EncodeToString(indexKey)
		if err := ltng.cache.Set(ctx, strIndexKey, opts.ParentKey); err != nil {
			return nil, err
		}
	}

	indexListKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.IndexListInfo().Name), opts.ParentKey},
		[]byte(ltngdata.BsSep),
	)
	strIndexListKey := hex.EncodeToString(indexListKey)
	if err := ltng.cache.Set(ctx, strIndexListKey, opts.IndexingKeys); err != nil {
		return nil, err
	}

	relationalKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.RelationalInfo().Name)},
		[]byte(ltngdata.BsSep),
	)
	strRelationalKey := hex.EncodeToString(relationalKey)
	var value []*ltngdata.Item
	if err := ltng.cache.Get(ctx, strRelationalKey, &value, func() (interface{}, error) {
		return []*ltngdata.Item{}, nil
	}); err != nil {
		return nil, err
	}
	value = append(value, &ltngdata.Item{
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
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
	key := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.Name), item.Key},
		[]byte(ltngdata.BsSep),
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
		[]byte(ltngdata.BsSep),
	)
	strIndexListKey := hex.EncodeToString(indexListKey)
	var indexingKeys [][]byte
	_ = ltng.cache.Get(ctx, strIndexListKey, &indexingKeys, func() (interface{}, error) {
		return [][]byte{}, nil
	})

	keysToSave := bytesop.CalRightDiff(indexingKeys, opts.IndexingKeys)
	for _, itemKey := range keysToSave {
		indexKey := bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), itemKey},
			[]byte(ltngdata.BsSep),
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
			[]byte(ltngdata.BsSep),
		)
		strIndexKey := hex.EncodeToString(indexKey)
		if err := ltng.cache.Del(ctx, strIndexKey); err != nil {
			return nil, err
		}
	}

	if err := ltng.cache.Set(ctx, strIndexListKey, opts.IndexingKeys); err != nil {
		return nil, err
	}

	relationalKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.RelationalInfo().Name)},
		[]byte(ltngdata.BsSep),
	)
	strRelationalKey := hex.EncodeToString(relationalKey)
	var value []*ltngdata.Item
	_ = ltng.cache.Get(ctx, strRelationalKey, &value, func() (interface{}, error) {
		return []*ltngdata.Item{}, nil
	})
	var found bool
	for idx, itemValue := range value {
		if bytes.Equal(item.Key, itemValue.Key) {
			found = true
			value[idx] = &ltngdata.Item{
				Key:   item.Key,
				Value: item.Value,
			}
		}
	}
	if !found {
		value = append(value, &ltngdata.Item{
			Key:   item.Key,
			Value: item.Value,
		})
	}
	if err := ltng.cache.Set(ctx, strRelationalKey, value); err != nil {
		return nil, err
	}

	return item, nil
}

func (ltng *LTNGCacheEngine) deleteItem(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
	switch opts.IndexProperties.IndexDeletionBehaviour {
	case ltngdata.Cascade:
		return ltng.deleteOnCascade(ctx, dbMetaInfo, item, opts)
	case ltngdata.CascadeByIdx:
		return ltng.deleteOnCascadeByIdx(ctx, dbMetaInfo, item, opts)
	case ltngdata.IndexOnly:
		return ltng.deleteIdxOnly(ctx, dbMetaInfo, item, opts)
	case ltngdata.None:
		fallthrough
	default:
		return nil, errorsx.New("invalid index deletion behaviour")
	}
}

func (ltng *LTNGCacheEngine) loadItem(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
	if opts == nil {
		return nil, errorsx.New("invalid index deletion behaviour")
	}

	if !opts.HasIdx {
		key := bytes.Join(
			[][]byte{[]byte(dbMetaInfo.Name), item.Key},
			[]byte(ltngdata.BsSep),
		)
		strKey := hex.EncodeToString(key)

		var value []byte
		if err := ltng.cache.Get(ctx, strKey, &value, nil); err != nil {
			return nil, err
		}

		return &ltngdata.Item{
			Key:   item.Key,
			Value: value,
		}, nil
	}

	switch opts.IndexProperties.IndexSearchPattern {
	case ltngdata.AndComputational:
		return ltng.andComputationalSearch(ctx, dbMetaInfo, item, opts)
	case ltngdata.OrComputational:
		return ltng.orComputationalSearch(ctx, dbMetaInfo, item, opts)
	case ltngdata.One:
		fallthrough
	default:
		return ltng.straightSearch(ctx, dbMetaInfo, item, opts)
	}
}

func (ltng *LTNGCacheEngine) listItems(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	pagination *ltngdata.Pagination,
	opts *ltngdata.IndexOpts,
) (*ltngdata.ListItemsResult, error) {
	if !pagination.IsValid() {
		return nil, errorsx.New("invalid pagination")
	}

	switch opts.IndexProperties.ListSearchPattern {
	case ltngdata.IndexingList:
		return ltng.indexingListItems(ctx, dbMetaInfo, pagination, opts)
	case ltngdata.All:
		return ltng.allListItems(ctx, dbMetaInfo, pagination, opts)
	case ltngdata.Default:
		fallthrough
	default:
		return ltng.defaultListItems(ctx, dbMetaInfo, pagination, opts)
	}
}
