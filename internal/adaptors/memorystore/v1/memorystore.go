package memorystorev1

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	ltngenginemodels "gitlab.com/pietroski-software-company/lightning-db/internal/models/ltngengine"
	go_cache "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/cache"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"
)

type LTNGCacheEngine struct {
	cache go_cache.Cacher
}

func New(ctx context.Context, opts ...options.Option) *LTNGCacheEngine {
	return &LTNGCacheEngine{
		cache: go_cache.New(),
	}
}

func (ltng *LTNGCacheEngine) CreateItem(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	key := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.Name), item.Key},
		[]byte(ltngenginemodels.BytesSep),
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
			[]byte(ltngenginemodels.BytesSep),
		)
		strIndexKey := hex.EncodeToString(indexKey)
		if err := ltng.cache.Set(ctx, strIndexKey, opts.ParentKey); err != nil {
			return nil, err
		}
	}

	indexListKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.IndexListInfo().Name), opts.ParentKey},
		[]byte(ltngenginemodels.BytesSep),
	)
	strIndexListKey := hex.EncodeToString(indexListKey)
	if err := ltng.cache.Set(ctx, strIndexListKey, opts.IndexingKeys); err != nil {
		return nil, err
	}

	relationalKey := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.RelationalInfo().Name)},
		[]byte(ltngenginemodels.BytesSep),
	)
	strRelationalKey := hex.EncodeToString(relationalKey)
	var value []*ltngenginemodels.Item
	if err := ltng.cache.Get(ctx, strRelationalKey, &value, func() (interface{}, error) {
		return append(value, &ltngenginemodels.Item{
			Key:   key,
			Value: item.Value,
		}), nil
	}); err != nil {
		return nil, err
	}

	return item, nil
}

func (ltng *LTNGCacheEngine) LoadItem(
	ctx context.Context,
	dbMetaInfo *ltngenginemodels.ManagerStoreMetaInfo,
	item *ltngenginemodels.Item,
	opts *ltngenginemodels.IndexOpts,
) (*ltngenginemodels.Item, error) {
	if opts == nil {
		return nil, nil
	}

	key := bytes.Join(
		[][]byte{[]byte(dbMetaInfo.Name), item.Key},
		[]byte(ltngenginemodels.BytesSep),
	)
	strKey := hex.EncodeToString(key)
	if !opts.HasIdx {
		var value []byte
		if err := ltng.cache.Get(ctx, strKey, &value, nil); err != nil {
			return nil, err
		}

		return &ltngenginemodels.Item{
			Key:   key,
			Value: value,
		}, nil
	}

	switch opts.IndexProperties.IndexSearchPattern {
	case ltngenginemodels.AndComputational:
		var value []byte
		for _, itemKey := range opts.IndexingKeys {
			indexKey := bytes.Join(
				[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), itemKey},
				[]byte(ltngenginemodels.BytesSep),
			)
			strIndexKey := hex.EncodeToString(indexKey)
			if err := ltng.cache.Get(ctx, strIndexKey, &value, nil); err != nil {
				return nil, err
			}
		}

		return &ltngenginemodels.Item{
			Key:   key,
			Value: value,
		}, nil
	case ltngenginemodels.OrComputational:
		var value []byte
		for _, itemKey := range opts.IndexingKeys {
			indexKey := bytes.Join(
				[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), itemKey},
				[]byte(ltngenginemodels.BytesSep),
			)
			strIndexKey := hex.EncodeToString(indexKey)
			if err := ltng.cache.Get(ctx, strIndexKey, &value, nil); err != nil {
				return nil, err
			} else {
				return &ltngenginemodels.Item{
					Key:   indexKey,
					Value: value,
				}, nil
			}
		}

		return nil, fmt.Errorf("not found")
	case ltngenginemodels.One:
		fallthrough
	default:
		key := bytes.Join(
			[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), opts.ParentKey},
			[]byte(ltngenginemodels.BytesSep),
		)
		strKey := hex.EncodeToString(key)
		if key == nil && (opts.IndexingKeys == nil || len(opts.IndexingKeys) == 0) {
			return nil, fmt.Errorf("invalid indexing key")
		} else if key == nil {
			key = bytes.Join(
				[][]byte{[]byte(dbMetaInfo.IndexInfo().Name), opts.IndexingKeys[0]},
				[]byte(ltngenginemodels.BytesSep),
			)
			strKey = hex.EncodeToString(key)
		}

		var value []byte
		if err := ltng.cache.Get(ctx, strKey, &value, nil); err != nil {
			return nil, err
		}

		key = bytes.Join(
			[][]byte{[]byte(dbMetaInfo.Name), value},
			[]byte(ltngenginemodels.BytesSep),
		)
		strKey = hex.EncodeToString(key)
		if err := ltng.cache.Get(ctx, strKey, &value, nil); err != nil {
			return nil, err
		}

		return &ltngenginemodels.Item{
			Key:   key,
			Value: value,
		}, nil
	}
}
