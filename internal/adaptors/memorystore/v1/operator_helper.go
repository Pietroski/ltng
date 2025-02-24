package memorystorev1

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
)

func (ltng *LTNGCacheEngine) deleteOnCascade(
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
	if err := ltng.cache.Del(ctx, strKey); err != nil {
		return nil, err
	}

	if opts == nil || !opts.HasIdx {
		return item, nil
	}

	indexListKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.IndexListInfo().Name), item.Key},
		[]byte(ltngenginemodels.BytesSliceSep),
	)
	strIndexListKey := hex.EncodeToString(indexListKey)
	var indexingKeys [][]byte
	_ = ltng.cache.Get(ctx, strIndexListKey, &indexingKeys, func() (interface{}, error) {
		return [][]byte{}, nil
	})

	for _, itemKey := range indexingKeys {
		indexKey := bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), itemKey},
			[]byte(ltngenginemodels.BytesSliceSep),
		)
		strIndexKey := hex.EncodeToString(indexKey)
		_ = ltng.cache.Del(ctx, strIndexKey)
	}

	_ = ltng.cache.Del(ctx, strIndexListKey)

	relationalKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.RelationalInfo().Name)},
		[]byte(ltngenginemodels.BytesSliceSep),
	)
	strRelationalKey := hex.EncodeToString(relationalKey)
	var value []*ltngenginemodels.Item
	if err := ltng.cache.Get(ctx, strRelationalKey, &value, func() (interface{}, error) {
		return []*ltngenginemodels.Item{}, fmt.Errorf("item not found")
	}); err != nil {
		return nil, nil
	}
	var remainingItemFromRelational []*ltngenginemodels.Item
	for _, v := range value {
		if bytes.Equal(v.Key, item.Key) {
			continue
		}

		remainingItemFromRelational = append(remainingItemFromRelational, v)
	}
	if err := ltng.cache.Set(ctx, strRelationalKey, remainingItemFromRelational); err != nil {
		return nil, err
	}

	return nil, nil
}

func (ltng *LTNGCacheEngine) deleteOnCascadeByIdx(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	var key []byte
	if opts.ParentKey != nil {
		key = bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), opts.ParentKey},
			[]byte(ltngenginemodels.BytesSliceSep),
		)
	}
	if key == nil && (opts.IndexingKeys == nil || len(opts.IndexingKeys) == 0) {
		return nil, fmt.Errorf("invalid indexing key")
	} else if key == nil {
		key = bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), opts.IndexingKeys[0]},
			[]byte(ltngenginemodels.BytesSliceSep),
		)
	}
	strKey := hex.EncodeToString(key)

	var keyValue []byte
	if err := ltng.cache.Get(ctx, strKey, &keyValue, nil); err != nil {
		return nil, err
	}

	item.Key = keyValue
	return ltng.deleteOnCascade(ctx, dbMetaInfo, item, opts)
}

func (ltng *LTNGCacheEngine) deleteIdxOnly(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	_ *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	if opts.IndexingKeys == nil || len(opts.IndexingKeys) == 0 {
		return nil, fmt.Errorf("invalid indexing key relation")
	}

	secondaryKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), opts.IndexingKeys[0]},
		[]byte(ltngenginemodels.BytesSliceSep),
	)
	secondaryStrKey := hex.EncodeToString(secondaryKey)

	var mainKey []byte
	if err := ltng.cache.Get(ctx, secondaryStrKey, &mainKey, nil); err != nil {
		return nil, err
	}

	keyValue := mainKey
	indexingKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.IndexListInfo().Name), keyValue},
		[]byte(ltngenginemodels.BytesSliceSep),
	)
	indexingStrKey := hex.EncodeToString(indexingKey)

	var indexingList [][]byte
	if err := ltng.cache.Get(ctx, indexingStrKey, &indexingList, nil); err != nil {
		return nil, err
	}

	var newIndexingList [][]byte
	for _, v := range indexingList {
		addToIndexingList := true
		for _, indexItem := range opts.IndexingKeys {
			if bytes.Equal(v, indexItem) {
				addToIndexingList = false
			}
		}

		if !addToIndexingList {
			continue
		}
		newIndexingList = append(newIndexingList, v)
	}

	for _, indexKey := range opts.IndexingKeys {
		key := bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), indexKey},
			[]byte(ltngenginemodels.BytesSliceSep),
		)
		strKey := hex.EncodeToString(key)
		_ = ltng.cache.Del(ctx, strKey)
	}
	_ = ltng.cache.Set(ctx, indexingStrKey, newIndexingList)

	return nil, nil
}

// #####################################################################################################################

func (ltng *LTNGCacheEngine) straightSearch(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	var key []byte
	var strKey string
	if opts.ParentKey != nil {
		key = bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), opts.ParentKey},
			[]byte(ltngenginemodels.BytesSliceSep),
		)
		strKey = hex.EncodeToString(key)
	} else if opts.IndexingKeys == nil || len(opts.IndexingKeys) != 1 {
		return nil, fmt.Errorf("invalid indexing key")
	} else {
		key = bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), opts.IndexingKeys[0]},
			[]byte(ltngenginemodels.BytesSliceSep),
		)
		strKey = hex.EncodeToString(key)
	}

	var keyValue []byte
	if err := ltng.cache.Get(ctx, strKey, &keyValue, nil); err != nil {
		return nil, err
	}

	key = bytes.Join(
		[][]byte{[]byte(dbMetaInfo.Name), keyValue},
		[]byte(ltngenginemodels.BytesSliceSep),
	)
	strKey = hex.EncodeToString(key)
	var valueValue []byte
	if err := ltng.cache.Get(ctx, strKey, &valueValue, nil); err != nil {
		return nil, err
	}

	return &ltngenginemodels.Item{
		Key:   keyValue,
		Value: valueValue,
	}, nil
}

func (ltng *LTNGCacheEngine) andComputationalSearch(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	var value []byte
	var indexKey []byte
	for _, itemKey := range opts.IndexingKeys {
		indexKey = bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), itemKey},
			[]byte(ltngenginemodels.BytesSliceSep),
		)
		strIndexKey := hex.EncodeToString(indexKey)
		if err := ltng.cache.Get(ctx, strIndexKey, &value, nil); err != nil {
			return nil, fmt.Errorf("not found - index list inconsistency")
		}
	}

	key := bytes.Split(indexKey, []byte(ltngenginemodels.BytesSliceSep))
	return &ltngenginemodels.Item{
		Key:   key[1],
		Value: value,
	}, nil
}

func (ltng *LTNGCacheEngine) orComputationalSearch(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	var value []byte
	for _, itemKey := range opts.IndexingKeys {
		indexKey := bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), itemKey},
			[]byte(ltngenginemodels.BytesSliceSep),
		)
		strIndexKey := hex.EncodeToString(indexKey)
		if err := ltng.cache.Get(ctx, strIndexKey, &value, nil); err != nil {
			continue
		} else {
			key := bytes.Split(indexKey, []byte(ltngenginemodels.BytesSliceSep))
			return &ltngenginemodels.Item{
				Key:   key[0],
				Value: value,
			}, nil
		}
	}

	return nil, fmt.Errorf("not found")
}

// #####################################################################################################################

func (ltng *LTNGCacheEngine) defaultListItems(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	pagination *ltngenginemodels.Pagination,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.ListItemsResult, error) {
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

func (ltng *LTNGCacheEngine) allListItems(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	pagination *ltngenginemodels.Pagination,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.ListItemsResult, error) {
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
		Pagination: pagination,
		Items:      value,
	}, nil
}

func (ltng *LTNGCacheEngine) indexingListItems(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	pagination *ltngenginemodels.Pagination,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.ListItemsResult, error) {
	indexListKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.IndexListInfo().Name), opts.ParentKey},
		[]byte(ltngenginemodels.BytesSliceSep),
	)
	strIndexListKey := hex.EncodeToString(indexListKey)
	var indexingKeys [][]byte
	_ = ltng.cache.Get(ctx, strIndexListKey, &indexingKeys, func() (interface{}, error) {
		return []*ltngenginemodels.Item{}, nil
	})

	items := make([]*ltngenginemodels.Item, len(indexingKeys))
	for idx, indexingKey := range indexingKeys {
		items[idx] = &ltngenginemodels.Item{
			Key:   opts.ParentKey,
			Value: indexingKey,
		}
	}

	return &ltngenginemodels.ListItemsResult{
		Pagination: pagination,
		Items:      items,
	}, nil
}

// #####################################################################################################################
