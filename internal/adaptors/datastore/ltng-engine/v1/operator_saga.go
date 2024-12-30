package v1

import (
	"bytes"
	"context"
	"fmt"
	"os"
)

type createSaga struct {
	e *LTNGEngine
}

func newCreateSaga(e *LTNGEngine) *createSaga {
	return &createSaga{
		e: e,
	}
}

func (s *createSaga) createItemOnDisk(
	ctx context.Context, dbMetaInfo *ManagerStoreMetaInfo, item *Item,
) func() error {
	return func() error {
		return s.e.createItemOnDisk(ctx, dbMetaInfo, item)
	}
}

func (s *createSaga) deleteItemOnDisk(
	path string, key string,
) func() error {
	return func() error {
		return os.Remove(getDataFilepath(path, key))
	}
}

func (s *createSaga) createIndexedItemOnDisk(
	ctx context.Context, dbMetaInfo *ManagerStoreMetaInfo, opts *IndexOpts,
) func() error {
	return func() error {
		for _, indexKey := range opts.IndexingKeys {
			if err := s.e.createIndexItemOnDisk(ctx, dbMetaInfo.IndexInfo(), &Item{
				Key:   indexKey,
				Value: opts.ParentKey,
			}); err != nil {
				return err
			}
		}

		return nil
	}
}

func (s *createSaga) deleteIndexedItemOnDisk(
	dbMetaInfo *ManagerStoreMetaInfo,
	opts *IndexOpts,
) func() error {
	return func() error {
		for _, indexKey := range opts.IndexingKeys {
			if err := os.Remove(getDataFilepath(dbMetaInfo.IndexInfo().Path, string(indexKey))); err != nil {
				return fmt.Errorf("error deleting item on database: %w", err)
			}
		}

		return nil
	}
}

func (s *createSaga) createIndexedItemListOnDisk(
	ctx context.Context, dbMetaInfo *ManagerStoreMetaInfo, opts *IndexOpts,
) func() error {
	return func() error {
		return s.e.createIndexItemOnDisk(ctx,
			dbMetaInfo.IndexListInfo(),
			&Item{
				Key:   opts.ParentKey,
				Value: bytes.Join(opts.IndexingKeys, []byte(bytesSep)),
			},
		)
	}
}
