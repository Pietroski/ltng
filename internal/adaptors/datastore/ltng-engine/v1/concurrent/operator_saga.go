package v1

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"

	off_thread "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/op/off-thread"
)

func ResponseAccumulator(respSigChan ...chan error) error {
	var err error
	for _, sigChan := range respSigChan {
		sigErr := <-sigChan
		if sigErr != nil {
			if err == nil {
				err = sigErr
			} else {
				err = fmt.Errorf("%s: %w", sigErr, err)
			}
		}
	}

	return err
}

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

// cidOnThread stands for createItemOnDisk on thread.
func (s *createSaga) cidOnThread(
	ctx context.Context,
) {
	for v := range s.e.mainThreadOperatorChan {
		err := s.e.createItemOnDisk(v.ctx, v.dbMetaInfo, v.item)
		v.respSignal <- err
		close(v.respSignal)
	}
}

func (s *createSaga) deleteItemOnDisk(
	path string, key string,
) func() error {
	return func() error {
		return os.Remove(getDataFilepath(path, key))
	}
}

func (s *createSaga) cidRollbackOnThread(
	ctx context.Context,
) {
	for v := range s.e.mainThreadOperatorChan {
		strKey := hex.EncodeToString(v.item.Key)
		err := os.Remove(getDataFilepath(v.dbMetaInfo.Path, strKey))
		v.respSignal <- err
		close(v.respSignal)
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

func (s *createSaga) ciidOnThread(
	ctx context.Context,
) {
	for v := range s.e.indexThreadOperatorChan {
		op := off_thread.New("createIndexItemOnDisk")
		for _, indexKey := range v.opts.IndexingKeys {
			op.OpX(func() (any, error) {
				if err := s.e.createIndexItemOnDisk(v.ctx, v.dbMetaInfo.IndexInfo(), &Item{
					Key:   indexKey,
					Value: v.opts.ParentKey,
				}); err != nil {
					return nil, err
				}

				return nil, nil
			})
		}
		err := op.WaitAndWrapErr()
		v.respSignal <- err
		close(v.respSignal)
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

func (s *createSaga) ciildOnThread(
	ctx context.Context,
) {
	for v := range s.e.indexListThreadOperatorChan {
		err := s.e.createIndexItemOnDisk(ctx,
			v.dbMetaInfo.IndexListInfo(),
			&Item{
				Key:   v.opts.ParentKey,
				Value: bytes.Join(v.opts.IndexingKeys, []byte(bytesSep)),
			},
		)
		v.respSignal <- err
		close(v.respSignal)
	}
}
