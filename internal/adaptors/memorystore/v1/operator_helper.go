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
		[][]byte{[]byte(dbMetaInfo.IndexListInfo().Name), key},
		[]byte(ltngenginemodels.BytesSliceSep),
	)
	strIndexListKey := hex.EncodeToString(indexListKey)
	var indexingKeys [][]byte
	_ = ltng.cache.Get(ctx, strIndexListKey, &indexingKeys, func() (interface{}, error) {
		return []*ltngenginemodels.Item{}, nil
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
	var deletedItemFromRelational []*ltngenginemodels.Item
	for _, v := range value {
		if bytes.Equal(v.Key, item.Key) {
			continue
		}

		deletedItemFromRelational = append(deletedItemFromRelational, v)
	}
	if err := ltng.cache.Set(ctx, strRelationalKey, deletedItemFromRelational); err != nil {
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
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	if opts.IndexingKeys == nil || len(opts.IndexingKeys) == 0 {
		return nil, fmt.Errorf("invalid indexing key")
	}

	key := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), opts.IndexingKeys[0]},
		[]byte(ltngenginemodels.BytesSliceSep),
	)
	strKey := hex.EncodeToString(key)

	var keyValue []byte
	if err := ltng.cache.Get(ctx, strKey, &keyValue, nil); err != nil {
		return nil, err
	}

	indexingKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.IndexListInfo().Name), keyValue},
		[]byte(ltngenginemodels.BytesSliceSep),
	)
	var indexingList [][]byte
	indexingStrKey := hex.EncodeToString(indexingKey)
	if err := ltng.cache.Get(ctx, indexingStrKey, &indexingList, nil); err != nil {
		return nil, err
	}

	var newIndexingList [][]byte
	for _, v := range indexingList {
		if bytes.Equal(v, item.Key) {
			continue
		}
		newIndexingList = append(newIndexingList, v)
	}

	_ = ltng.cache.Set(ctx, indexingStrKey, newIndexingList)
	_ = ltng.cache.Del(ctx, strKey)

	return nil, nil
}

// #####################################################################################################################

func (ltng *LTNGCacheEngine) straightSearch(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	key := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), opts.ParentKey},
		[]byte(ltngenginemodels.BytesSliceSep),
	)
	strKey := hex.EncodeToString(key)
	if key == nil && (opts.IndexingKeys == nil || len(opts.IndexingKeys) == 0) {
		return nil, fmt.Errorf("invalid indexing key")
	} else if key == nil {
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
		Key:   key[0],
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
