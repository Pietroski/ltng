package memorystorev1

import (
	"bytes"
	"context"
	"encoding/hex"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"

	"gitlab.com/pietroski-software-company/lightning-db/internal/tools/ltngdata"
)

func (ltng *LTNGCacheEngine) deleteOnCascade(
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
	if err := ltng.cache.Del(ctx, strKey); err != nil {
		return nil, err
	}

	if opts == nil || !opts.HasIdx {
		return item, nil
	}

	indexListKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.IndexListInfo().Name), item.Key},
		[]byte(ltngdata.BsSep),
	)
	strIndexListKey := hex.EncodeToString(indexListKey)
	var indexingKeys [][]byte
	_ = ltng.cache.Get(ctx, strIndexListKey, &indexingKeys, func() (interface{}, error) {
		return [][]byte{}, nil
	})

	for _, itemKey := range indexingKeys {
		indexKey := bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), itemKey},
			[]byte(ltngdata.BsSep),
		)
		strIndexKey := hex.EncodeToString(indexKey)
		_ = ltng.cache.Del(ctx, strIndexKey)
	}

	_ = ltng.cache.Del(ctx, strIndexListKey)

	relationalKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.RelationalInfo().Name)},
		[]byte(ltngdata.BsSep),
	)
	strRelationalKey := hex.EncodeToString(relationalKey)
	var value []*ltngdata.Item
	if err := ltng.cache.Get(ctx, strRelationalKey, &value, func() (interface{}, error) {
		return []*ltngdata.Item{}, errorsx.New("item not found")
	}); err != nil {
		return nil, nil
	}
	var remainingItemFromRelational []*ltngdata.Item
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
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
	var key []byte
	if opts.ParentKey != nil {
		key = bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), opts.ParentKey},
			[]byte(ltngdata.BsSep),
		)
	}
	if key == nil && (opts.IndexingKeys == nil || len(opts.IndexingKeys) == 0) {
		return nil, errorsx.New("invalid indexing key")
	} else if key == nil {
		key = bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), opts.IndexingKeys[0]},
			[]byte(ltngdata.BsSep),
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
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	_ *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
	if opts.IndexingKeys == nil || len(opts.IndexingKeys) == 0 {
		return nil, errorsx.New("invalid indexing key relation")
	}

	secondaryKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), opts.IndexingKeys[0]},
		[]byte(ltngdata.BsSep),
	)
	secondaryStrKey := hex.EncodeToString(secondaryKey)

	var mainKey []byte
	if err := ltng.cache.Get(ctx, secondaryStrKey, &mainKey, nil); err != nil {
		return nil, err
	}

	keyValue := mainKey
	indexingKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.IndexListInfo().Name), keyValue},
		[]byte(ltngdata.BsSep),
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
			[]byte(ltngdata.BsSep),
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
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
	var key []byte
	var strKey string
	if opts.ParentKey != nil {
		key = bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), opts.ParentKey},
			[]byte(ltngdata.BsSep),
		)
		strKey = hex.EncodeToString(key)
	} else if opts.IndexingKeys == nil || len(opts.IndexingKeys) != 1 {
		return nil, errorsx.New("invalid indexing key")
	} else {
		key = bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), opts.IndexingKeys[0]},
			[]byte(ltngdata.BsSep),
		)
		strKey = hex.EncodeToString(key)
	}

	var keyValue []byte
	if err := ltng.cache.Get(ctx, strKey, &keyValue, nil); err != nil {
		return nil, err
	}

	key = bytes.Join(
		[][]byte{[]byte(dbMetaInfo.Name), keyValue},
		[]byte(ltngdata.BsSep),
	)
	strKey = hex.EncodeToString(key)
	var valueValue []byte
	if err := ltng.cache.Get(ctx, strKey, &valueValue, nil); err != nil {
		return nil, err
	}

	return &ltngdata.Item{
		Key:   keyValue,
		Value: valueValue,
	}, nil
}

func (ltng *LTNGCacheEngine) andComputationalSearch(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
	var value []byte
	var indexKey []byte
	for _, itemKey := range opts.IndexingKeys {
		indexKey = bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), itemKey},
			[]byte(ltngdata.BsSep),
		)
		strIndexKey := hex.EncodeToString(indexKey)
		if err := ltng.cache.Get(ctx, strIndexKey, &value, nil); err != nil {
			return nil, errorsx.New("not found - index list inconsistency")
		}
	}

	key := bytes.Split(indexKey, []byte(ltngdata.BsSep))
	return &ltngdata.Item{
		Key:   key[1],
		Value: value,
	}, nil
}

func (ltng *LTNGCacheEngine) orComputationalSearch(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	item *ltngdata.Item,
	opts *ltngdata.IndexOpts,
) (*ltngdata.Item, error) {
	var value []byte
	for _, itemKey := range opts.IndexingKeys {
		indexKey := bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), itemKey},
			[]byte(ltngdata.BsSep),
		)
		strIndexKey := hex.EncodeToString(indexKey)
		if err := ltng.cache.Get(ctx, strIndexKey, &value, nil); err != nil {
			continue
		} else {
			key := bytes.Split(indexKey, []byte(ltngdata.BsSep))
			return &ltngdata.Item{
				Key:   key[0],
				Value: value,
			}, nil
		}
	}

	return nil, errorsx.New("not found")
}

// #####################################################################################################################

func (ltng *LTNGCacheEngine) defaultListItems(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	pagination *ltngdata.Pagination,
	opts *ltngdata.IndexOpts,
) (*ltngdata.ListItemsResult, error) {
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

	return &ltngdata.ListItemsResult{
		Pagination: pagination.CalcNextPage(uint64(len(value))),
		Items: (*ltngdata.ItemList)(&value).
			GetItemsFromPagination(pagination),
	}, nil
}

func (ltng *LTNGCacheEngine) allListItems(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	pagination *ltngdata.Pagination,
	opts *ltngdata.IndexOpts,
) (*ltngdata.ListItemsResult, error) {
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

	return &ltngdata.ListItemsResult{
		Pagination: pagination,
		Items:      value,
	}, nil
}

func (ltng *LTNGCacheEngine) indexingListItems(
	ctx context.Context,
	dbMetaInfo *ltngdata.ManagerStoreMetaInfo,
	pagination *ltngdata.Pagination,
	opts *ltngdata.IndexOpts,
) (*ltngdata.ListItemsResult, error) {
	indexListKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.IndexListInfo().Name), opts.ParentKey},
		[]byte(ltngdata.BsSep),
	)
	strIndexListKey := hex.EncodeToString(indexListKey)
	var indexingKeys [][]byte
	_ = ltng.cache.Get(ctx, strIndexListKey, &indexingKeys, func() (interface{}, error) {
		return []*ltngdata.Item{}, nil
	})

	items := make([]*ltngdata.Item, len(indexingKeys))
	for idx, indexingKey := range indexingKeys {
		items[idx] = &ltngdata.Item{
			Key:   opts.ParentKey,
			Value: indexingKey,
		}
	}

	return &ltngdata.ListItemsResult{
		Pagination: pagination,
		Items:      items,
	}, nil
}

// #####################################################################################################################
